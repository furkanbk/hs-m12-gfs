"""SQLite metadata store for the Naranja naming server.

Holds **metadata only** — file → chunk → replica/leader mapping. Chunk *bytes*
never live here; they live on the storage servers' filesystem at
``/data/{chunk_id}``.

Connection rules that matter:
  * ``PRAGMA foreign_keys = ON`` is per-connection, so every connection we hand
    out sets it — otherwise ``ON DELETE CASCADE`` silently no-ops.
  * The DB file lives on a named volume (``DB_PATH``) so the naming server can
    restart with its state intact (our SPOF mitigation).
"""
from __future__ import annotations

import os
import sqlite3
from contextlib import contextmanager
from pathlib import Path
from typing import Iterator

DB_PATH = os.environ.get("DB_PATH", "/data/naranja.db")

_SCHEMA = """
CREATE TABLE IF NOT EXISTS files (
    file_id    TEXT PRIMARY KEY,
    filename   TEXT NOT NULL UNIQUE,
    size_bytes INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS chunks (
    chunk_id   TEXT PRIMARY KEY,
    file_id    TEXT NOT NULL REFERENCES files(file_id) ON DELETE CASCADE,
    idx        INTEGER NOT NULL,
    size_bytes INTEGER NOT NULL,
    leader     TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_chunks_file ON chunks(file_id);

CREATE TABLE IF NOT EXISTS replicas (
    id       INTEGER PRIMARY KEY AUTOINCREMENT,
    chunk_id TEXT NOT NULL REFERENCES chunks(chunk_id) ON DELETE CASCADE,
    addr     TEXT NOT NULL,
    position INTEGER NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_replicas_chunk ON replicas(chunk_id);
"""


@contextmanager
def connect() -> Iterator[sqlite3.Connection]:
    """Open a connection with FK enforcement and row access by name.

    A fresh connection per unit of work keeps us clear of sqlite3's
    same-thread restriction under FastAPI's threadpool.
    """
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    # Per-connection: without this, ON DELETE CASCADE does nothing.
    conn.execute("PRAGMA foreign_keys = ON")
    try:
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def init_db() -> None:
    """Create the parent dir, the schema, and switch the DB to WAL mode."""
    Path(DB_PATH).parent.mkdir(parents=True, exist_ok=True)
    with connect() as conn:
        # WAL persists once set; better read/write concurrency for the metadata.
        conn.execute("PRAGMA journal_mode = WAL")
        conn.executescript(_SCHEMA)


def healthcheck() -> bool:
    """Return True iff the DB answers a trivial query."""
    try:
        with connect() as conn:
            conn.execute("SELECT 1").fetchone()
        return True
    except Exception:
        return False


def insert_file(file_id: str, filename: str, size_bytes: int, chunks: list[dict]) -> None:
    """Persist a committed file's full layout: file → chunks → replicas.

    Raises ``sqlite3.IntegrityError`` if the filename already exists (UNIQUE).
    """
    with connect() as conn:
        conn.execute(
            "INSERT INTO files (file_id, filename, size_bytes) VALUES (?, ?, ?)",
            (file_id, filename, size_bytes),
        )
        for chunk in chunks:
            conn.execute(
                "INSERT INTO chunks (chunk_id, file_id, idx, size_bytes, leader) "
                "VALUES (?, ?, ?, ?, ?)",
                (
                    chunk["chunk_id"],
                    file_id,
                    chunk["index"],
                    chunk["size_bytes"],
                    chunk["leader"],
                ),
            )
            for position, addr in enumerate(chunk["replicas"]):
                conn.execute(
                    "INSERT INTO replicas (chunk_id, addr, position) VALUES (?, ?, ?)",
                    (chunk["chunk_id"], addr, position),
                )


def get_file(filename: str) -> dict | None:
    """Return the file's metadata + chunk locations, chunks ordered by index.

    Shape mirrors the commit layout so the client can reassemble by index:
    ``{file_id, filename, size_bytes, chunks: [{chunk_id, index, replicas,
    leader, size_bytes}]}``. Returns None if the file is unknown.
    """
    with connect() as conn:
        row = conn.execute(
            "SELECT file_id, filename, size_bytes FROM files WHERE filename = ?",
            (filename,),
        ).fetchone()
        if row is None:
            return None

        chunk_rows = conn.execute(
            "SELECT chunk_id, idx, size_bytes, leader FROM chunks "
            "WHERE file_id = ? ORDER BY idx",
            (row["file_id"],),
        ).fetchall()

        chunks = []
        for c in chunk_rows:
            replica_rows = conn.execute(
                "SELECT addr FROM replicas WHERE chunk_id = ? ORDER BY position",
                (c["chunk_id"],),
            ).fetchall()
            chunks.append(
                {
                    "chunk_id": c["chunk_id"],
                    "index": c["idx"],
                    "replicas": [r["addr"] for r in replica_rows],
                    "leader": c["leader"],
                    "size_bytes": c["size_bytes"],
                }
            )

        return {
            "file_id": row["file_id"],
            "filename": row["filename"],
            "size_bytes": row["size_bytes"],
            "chunks": chunks,
        }


def get_size(filename: str) -> int | None:
    """Return the file size from metadata (no chunk transfer). None if unknown."""
    with connect() as conn:
        row = conn.execute(
            "SELECT size_bytes FROM files WHERE filename = ?", (filename,)
        ).fetchone()
        return None if row is None else row["size_bytes"]


def delete_file(filename: str) -> dict | None:
    """Gather chunk ids + their replicas, THEN cascade-delete the metadata.

    Returns ``{chunk_ids: [...], replicas: {chunk_id: [addr, ...]}}`` so the
    caller knows which chunk files to purge from storage, or None if the file
    is unknown. The gather MUST happen before the delete — once the cascade
    fires the rows are gone.
    """
    with connect() as conn:
        file_row = conn.execute(
            "SELECT file_id FROM files WHERE filename = ?", (filename,)
        ).fetchone()
        if file_row is None:
            return None

        chunk_rows = conn.execute(
            "SELECT chunk_id FROM chunks WHERE file_id = ? ORDER BY idx",
            (file_row["file_id"],),
        ).fetchall()

        chunk_ids = [c["chunk_id"] for c in chunk_rows]
        replicas: dict[str, list[str]] = {}
        for chunk_id in chunk_ids:
            replica_rows = conn.execute(
                "SELECT addr FROM replicas WHERE chunk_id = ? ORDER BY position",
                (chunk_id,),
            ).fetchall()
            replicas[chunk_id] = [r["addr"] for r in replica_rows]

        # Now it is safe to delete; FK cascade removes chunks + replicas.
        conn.execute("DELETE FROM files WHERE file_id = ?", (file_row["file_id"],))

        return {"chunk_ids": chunk_ids, "replicas": replicas}
