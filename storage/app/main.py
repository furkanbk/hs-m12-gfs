"""Naranja DFS — Storage server (STUB).

One image, run x3 via docker-compose. Each container stores only its own chunks
as individual files under /data (a named docker volume) — chunk content lives on
the Linux filesystem, never in a database.

This stub accepts requests and returns success immediately so Berat's client
works end-to-end today. Real logic is owned by Shafeen (disk persistence,
read/delete) and Ivan (leader -> secondary ack coordination). See the
TODO markers and docs/TASKS.md.
"""
from __future__ import annotations

import os
from pathlib import Path

from fastapi import FastAPI, Request
from pydantic import BaseModel

DATA_DIR = Path(os.environ.get("DATA_DIR", "/data"))
DATA_DIR.mkdir(parents=True, exist_ok=True)
SELF_ADDR = os.environ.get("SELF_ADDR", "storage:8000")

app = FastAPI(title="Naranja DFS — Storage server (stub)")


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
    """Stub: leader commit. Returns success immediately.

    TODO(Ivan): real leader logic — finalize locally, then call
    POST /chunks/{id}/commit-replica on every secondary, wait for ALL acks,
    and only then return success. Define how many simultaneous failures we
    survive and enforce it here.
    """
    return {"ok": True, "acked": [SELF_ADDR, *req.secondaries]}


@app.post("/chunks/{chunk_id}/commit-replica")
async def commit_replica(chunk_id: str) -> dict:
    """Stub: secondary finalize + ack. Returns success immediately.

    TODO(Ivan/Shafeen): real secondary finalize, then ack.
    """
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
