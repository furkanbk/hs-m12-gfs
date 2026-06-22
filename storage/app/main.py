"""Naranja DFS — Storage server.

One image, run x3 via docker-compose. Each container stores only its own chunks
as individual files under /data (a named docker volume) — chunk content lives on
the Linux filesystem, never in a database.

Disk persistence (read/delete) is owned by Shafeen; the leader -> secondary
commit/ack protocol is owned by Ivan and now lives in ``replication.py``. See
the TODO markers and docs/TASKS.md.
"""
from __future__ import annotations

import logging
import os
import re
import tempfile
from pathlib import Path

from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import FileResponse
from pydantic import BaseModel

from . import replication

log = logging.getLogger("storage")

DATA_DIR = Path(os.environ.get("DATA_DIR", "/data"))
DATA_DIR.mkdir(parents=True, exist_ok=True)
SELF_ADDR = os.environ.get("SELF_ADDR", "storage:8000")
CHUNK_ID_PATTERN = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_.-]{0,127}$")

app = FastAPI(title="Naranja DFS — Storage server")


class CommitRequest(BaseModel):
    secondaries: list[str] = []


@app.get("/healthz")
async def healthz() -> dict:
    return {"ok": True, "self": SELF_ADDR, "data_dir": str(DATA_DIR)}


def chunk_path(chunk_id: str) -> Path:
    """Return the safe on-disk path for a chunk id.

    Chunk ids are expected to be opaque ids from the naming server, but storage
    still validates them so a client cannot escape DATA_DIR with path segments.
    """
    if not CHUNK_ID_PATTERN.fullmatch(chunk_id):
        raise HTTPException(status_code=400, detail="Invalid chunk id.")
    return DATA_DIR / chunk_id


def fsync_directory(path: Path) -> None:
    """Flush directory metadata after create/rename/delete operations."""
    dir_fd = os.open(path, os.O_RDONLY)
    try:
        os.fsync(dir_fd)
    finally:
        os.close(dir_fd)


def write_chunk_durably(path: Path, data: bytes) -> None:
    """Write chunk bytes using temp file + fsync + atomic rename."""
    temp_name: str | None = None
    try:
        with tempfile.NamedTemporaryFile(
            mode="wb",
            dir=DATA_DIR,
            prefix=f".{path.name}.",
            suffix=".tmp",
            delete=False,
        ) as temp_file:
            temp_name = temp_file.name
            temp_file.write(data)
            temp_file.flush()
            os.fsync(temp_file.fileno())

        os.replace(temp_name, path)
        fsync_directory(DATA_DIR)
    finally:
        if temp_name is not None:
            temp_path = Path(temp_name)
            if temp_path.exists():
                temp_path.unlink()


@app.put("/chunks/{chunk_id}/data")
async def put_data(chunk_id: str, request: Request) -> dict:
    """Persist raw chunk bytes to /data/{chunk_id}."""
    data = await request.body()
    path = chunk_path(chunk_id)
    try:
        write_chunk_durably(path, data)
    except OSError as exc:
        raise HTTPException(status_code=500, detail=f"Could not store chunk: {exc}") from exc
    return {"ok": True, "chunk_id": chunk_id, "size_bytes": len(data)}


@app.post("/chunks/{chunk_id}/commit")
async def commit(chunk_id: str, req: CommitRequest) -> dict:
    """Leader commit (owner: Ivan). Sent ONLY to the leader by the client.

    Finalizes the chunk locally, drives every secondary to finalize via
    ``commit-replica``, waits for their acks, and enforces the write policy
    (see ``replication.WRITE_MIN_REPLICAS``). Returns the contract shape
    ``{"ok": true, "acked": [...]}`` on success; on insufficient acks it fails
    with HTTP 503 so the client surfaces a clear, retryable error rather than
    treating an under-replicated chunk as durable.
    """
    if not replication.is_valid_chunk_id(chunk_id):
        raise HTTPException(status_code=400, detail=f"invalid chunk_id: {chunk_id!r}")
    outcome = await replication.leader_commit(
        self_addr=SELF_ADDR,
        chunk_id=chunk_id,
        secondaries=req.secondaries,
        data_dir=DATA_DIR,
    )
    if not outcome.ok:
        log.error("commit refused for chunk %s: %s", chunk_id, outcome.reason())
        raise HTTPException(status_code=503, detail=outcome.reason())
    return {"ok": True, "acked": list(outcome.acked)}


@app.post("/chunks/{chunk_id}/commit-replica")
async def commit_replica(chunk_id: str) -> dict:
    """Secondary finalize + ack (owner: Ivan). Internal: leader -> secondary.

    A secondary can only ack a chunk whose bytes it actually holds (pushed by
    the data plane). If the data is missing, refuse with HTTP 409 so the leader
    counts this replica as failed instead of recording a phantom ack.
    """
    if not replication.is_valid_chunk_id(chunk_id):
        raise HTTPException(status_code=400, detail=f"invalid chunk_id: {chunk_id!r}")
    if not replication.chunk_present(DATA_DIR, chunk_id):
        log.warning("secondary %s missing data for chunk %s; declining", SELF_ADDR, chunk_id)
        raise HTTPException(status_code=409, detail=f"chunk {chunk_id} not present on {SELF_ADDR}")
    return {"ok": True, "acked": SELF_ADDR}


@app.get("/chunks/{chunk_id}")
async def get_chunk(chunk_id: str) -> FileResponse:
    """Return raw chunk bytes from local disk."""
    path = chunk_path(chunk_id)
    if not path.exists() or not path.is_file():
        raise HTTPException(status_code=404, detail="Chunk not found.")
    return FileResponse(path, media_type="application/octet-stream")


@app.delete("/chunks/{chunk_id}")
async def delete_chunk(chunk_id: str) -> dict:
    """Remove a chunk replica from local disk.

    Delete is idempotent: removing an already-missing replica still returns ok,
    which keeps distributed delete retries simple.
    """
    path = chunk_path(chunk_id)
    existed = path.exists()
    if existed:
        try:
            path.unlink()
            fsync_directory(DATA_DIR)
        except OSError as exc:
            raise HTTPException(status_code=500, detail=f"Could not delete chunk: {exc}") from exc
    return {"ok": True, "chunk_id": chunk_id, "deleted": existed}
