"""Naranja DFS — Naming server (STUB).

Metadata authority. This is a minimal, runnable stub so Berat's client works
end-to-end today. It implements POST /allocate, POST /commit and GET /healthz
with trivial in-memory behavior. The real logic is owned by Daryna — see the
TODO(Daryna) markers and docs/TASKS.md.

Locked decisions this stub respects:
  * Replication factor 3 — every chunk is placed on all 3 storage servers.
  * leader = storage-1:8000 for every chunk.
  * Metadata only — chunk BYTES never live here (they go to the filesystem on
    the storage servers).
"""
from __future__ import annotations

import os
import uuid

from fastapi import FastAPI
from pydantic import BaseModel

# Replica set comes from env so compose can wire the storage hostnames.
STORAGE_REPLICAS = os.environ.get(
    "STORAGE_REPLICAS", "storage-1:8000,storage-2:8000,storage-3:8000"
).split(",")
LEADER = os.environ.get("LEADER", STORAGE_REPLICAS[0])

app = FastAPI(title="Naranja DFS — Naming server (stub)")

# In-memory metadata store. TODO(Daryna): replace with SQLite (metadata only).
FILES: dict[str, dict] = {}


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
async def healthz() -> dict:
    return {"ok": True}


@app.post("/allocate")
async def allocate(req: AllocateRequest) -> dict:
    """Stub: generate ids, place every chunk on all replicas, leader = first."""
    # TODO(Daryna): real replica placement + leader selection (spread chunks
    # so each storage server holds only a fraction; pick leaders per chunk).
    file_id = str(uuid.uuid4())
    chunks = []
    for index in range(req.num_chunks):
        chunks.append(
            {
                "chunk_id": str(uuid.uuid4()),
                "index": index,
                "replicas": list(STORAGE_REPLICAS),
                "leader": LEADER,
            }
        )
    return {"file_id": file_id, "chunks": chunks}


@app.post("/commit")
async def commit(req: CommitRequest) -> dict:
    """Stub: store the file -> chunk -> locations mapping in memory."""
    # TODO(Daryna): persist to SQLite; never store chunk bytes here.
    FILES[req.filename] = req.model_dump()
    return {"ok": True}


# --- Scaffold (owner: Daryna) ----------------------------------------------

@app.get("/files/{filename}")
async def get_file(filename: str):
    # TODO(Daryna): return metadata + chunk locations for the Read flow.
    raise NotImplementedError


@app.delete("/files/{filename}")
async def delete_file(filename: str):
    # TODO(Daryna): return chunk_ids + replicas to purge, cascade delete metadata.
    raise NotImplementedError


@app.get("/files/{filename}/size")
async def get_size(filename: str):
    # TODO(Daryna): return size from metadata, no chunk transfer.
    raise NotImplementedError
