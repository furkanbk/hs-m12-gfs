"""Naranja DFS — Naming server (metadata authority).

The single source of truth for file → chunk → replica/leader metadata. Chunk
*bytes* never live here; they live on the storage servers' filesystem under
``/data/{chunk_id}``. Metadata is persisted to SQLite on a named volume so the
naming server can restart with its state intact (our SPOF mitigation).

Implements the canonical control-plane contract from docs/ARCHITECTURE.md:
  * POST   /allocate           — stateless: ids + replica placement + leader.
  * POST   /commit             — persist the committed file layout.
  * GET    /files/{filename}   — chunk locations for reads (ordered by index).
  * DELETE /files/{filename}   — chunks/replicas to purge, then cascade-delete.
  * GET    /files/{filename}/size — file size from metadata.
  * GET    /healthz            — liveness incl. a real DB probe.

Owner: Daryna. Do not change the API shapes — they are the team's integration
boundary.
"""
from __future__ import annotations

import os
import sqlite3
import uuid
from contextlib import asynccontextmanager

from fastapi import FastAPI, HTTPException
from fastapi.responses import JSONResponse
from pydantic import BaseModel

from . import db

# Placement config comes from env so compose wires the pool / factor — never
# hardcode. With 3 servers / RF 3 every chunk lands on all three.
STORAGE_SERVERS = [
    s.strip()
    for s in os.environ.get(
        "STORAGE_SERVERS", "storage-1:8000,storage-2:8000,storage-3:8000"
    ).split(",")
    if s.strip()
]
REPLICATION_FACTOR = int(os.environ.get("REPLICATION_FACTOR", "3"))


@asynccontextmanager
async def lifespan(app: FastAPI):
    # Init the DB on startup (create dir + schema + WAL). Not @app.on_event.
    db.init_db()
    yield


app = FastAPI(title="Naranja DFS — Naming server", lifespan=lifespan)


class AllocateRequest(BaseModel):
    filename: str
    size_bytes: int
    num_chunks: int


class CommitRequest(BaseModel):
    file_id: str
    filename: str
    size_bytes: int
    chunks: list[dict]


@app.get("/healthz")
async def healthz():
    """Liveness + a real DB probe (SELECT 1). 503 if the DB is unreachable."""
    if not db.healthcheck():
        raise HTTPException(status_code=503, detail="database unreachable")
    return {"ok": True}


def _place_chunk(index: int) -> tuple[list[str], str]:
    """Pick replicas + leader for a chunk index.

    Replicas are ``REPLICATION_FACTOR`` servers from the pool starting at a
    rotating offset; the leader rotates round-robin by chunk index (so we don't
    make one server leader for everything).
    """
    pool = STORAGE_SERVERS
    factor = min(REPLICATION_FACTOR, len(pool))
    start = index % len(pool)
    replicas = [pool[(start + j) % len(pool)] for j in range(factor)]
    leader = replicas[0]
    return replicas, leader


@app.post("/allocate")
async def allocate(req: AllocateRequest) -> dict:
    """Stateless: generate ids + compute placement. Persists nothing.

    The DB is written only at commit time. ``num_chunks == 0`` (empty file)
    yields an empty chunk list, which is valid.
    """
    if not STORAGE_SERVERS:
        raise HTTPException(status_code=503, detail="no storage servers configured")

    file_id = str(uuid.uuid4())
    chunks = []
    for index in range(req.num_chunks):
        replicas, leader = _place_chunk(index)
        chunks.append(
            {
                "chunk_id": str(uuid.uuid4()),
                "index": index,
                "replicas": replicas,
                "leader": leader,
            }
        )
    return {"file_id": file_id, "chunks": chunks}


@app.post("/commit")
async def commit(req: CommitRequest) -> dict:
    """Persist the committed file layout. Trust the body — it carries the
    layout the client allocated and pushed. Duplicate filename → 409."""
    try:
        db.insert_file(req.file_id, req.filename, req.size_bytes, req.chunks)
    except sqlite3.IntegrityError as exc:
        raise HTTPException(
            status_code=409, detail=f"file already exists: {req.filename}"
        ) from exc
    return {"ok": True}


@app.get("/files/{filename}")
async def get_file(filename: str) -> dict:
    """Metadata + chunk locations for the Read flow (chunks ordered by index,
    each carrying size_bytes)."""
    meta = db.get_file(filename)
    if meta is None:
        raise HTTPException(status_code=404, detail=f"file not found: {filename}")
    return meta


@app.delete("/files/{filename}")
async def delete_file(filename: str) -> dict:
    """Return chunk ids + their replicas to purge, then cascade-delete the
    metadata. Gather happens before the delete inside the DB layer."""
    result = db.delete_file(filename)
    if result is None:
        raise HTTPException(status_code=404, detail=f"file not found: {filename}")
    return result


@app.get("/files/{filename}/size")
async def get_size(filename: str) -> dict:
    """File size from metadata — no chunk transfer."""
    size = db.get_size(filename)
    if size is None:
        raise HTTPException(status_code=404, detail=f"file not found: {filename}")
    return {"size_bytes": size}
