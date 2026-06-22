"""Naranja DFS — Storage server (STUB).

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
from pathlib import Path

from fastapi import FastAPI, HTTPException, Request
from pydantic import BaseModel

from . import replication

log = logging.getLogger("storage")

DATA_DIR = Path(os.environ.get("DATA_DIR", "/data"))
DATA_DIR.mkdir(parents=True, exist_ok=True)
SELF_ADDR = os.environ.get("SELF_ADDR", "storage:8000")

app = FastAPI(title="Naranja DFS — Storage server")


class CommitRequest(BaseModel):
    secondaries: list[str] = []


@app.get("/healthz")
async def healthz() -> dict:
    return {"ok": True}


@app.put("/chunks/{chunk_id}/data")
async def put_data(chunk_id: str, request: Request) -> dict:
    """Stub: accept the bytes. Writes to /data/{chunk_id} to demonstrate the
    on-filesystem storage model, but keeps it trivial (no fsync/durability)."""
    # TODO(Shafeen): real durable write (atomic write + fsync) to /data/{chunk_id}.
    data = await request.body()
    (DATA_DIR / chunk_id).write_bytes(data)
    return {"ok": True}


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


# --- Scaffold (owner: Shafeen) ---------------------------------------------

@app.get("/chunks/{chunk_id}")
async def get_chunk(chunk_id: str):
    # TODO(Shafeen): stream raw chunk bytes from /data/{chunk_id}.
    raise NotImplementedError


@app.delete("/chunks/{chunk_id}")
async def delete_chunk(chunk_id: str):
    # TODO(Shafeen): remove the chunk file from /data/{chunk_id}.
    raise NotImplementedError
