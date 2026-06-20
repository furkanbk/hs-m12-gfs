"""Naranja DFS — Client service (Berat's part, fully implemented).

A FastAPI app that:
  * serves a minimal web UI at /  (upload a .txt + Send),
  * runs the full Create/Write flow against the naming + storage servers,
  * scaffolds the Read / Delete / Get-size client flows (TODO markers).

Config comes entirely from environment variables (see nameserver_client and
storage_client) so docker-compose can wire hostnames — no hardcoded hosts here.
"""
from __future__ import annotations

import logging
import os
from pathlib import Path

from fastapi import FastAPI, HTTPException, UploadFile
from fastapi.responses import FileResponse, JSONResponse
from fastapi.staticfiles import StaticFiles

from . import chunker, nameserver_client, storage_client

logging.basicConfig(level=logging.INFO)
log = logging.getLogger("client")

CHUNK_SIZE = int(os.environ.get("CHUNK_SIZE", "1024"))
STATIC_DIR = Path(__file__).parent / "static"

app = FastAPI(title="Naranja DFS — Client")


@app.get("/healthz")
async def healthz() -> dict:
    return {"ok": True}


@app.get("/")
async def index() -> FileResponse:
    return FileResponse(STATIC_DIR / "index.html")


@app.post("/api/files")
async def create_file(file: UploadFile) -> JSONResponse:
    """Full Create/Write flow: chunk -> allocate -> push -> commit -> register.

    Returns a summary the UI can display (file_id, chunk count, replicas hit).
    """
    if not file.filename or not file.filename.lower().endswith(".txt"):
        raise HTTPException(status_code=400, detail="Only .txt files are accepted.")

    data = await file.read()
    chunks = chunker.split(data, CHUNK_SIZE)
    size_bytes = len(data)

    # 1. Ask the naming server for chunk ids + replica placement.
    try:
        allocation = await nameserver_client.allocate(
            filename=file.filename, size_bytes=size_bytes, num_chunks=len(chunks)
        )
    except Exception as exc:  # noqa: BLE001 — surface any allocation failure
        log.exception("allocate failed")
        raise HTTPException(status_code=502, detail=f"allocate failed: {exc}") from exc

    file_id = allocation["file_id"]
    allocated = allocation["chunks"]
    if len(allocated) != len(chunks):
        raise HTTPException(
            status_code=502,
            detail=(
                f"naming server allocated {len(allocated)} chunks "
                f"but file has {len(chunks)}"
            ),
        )

    replicas_hit: set[str] = set()
    committed_chunks: list[dict] = []

    # 2 + 3. For each chunk: push bytes to ALL replicas, then commit to leader.
    for local_chunk, alloc in zip(chunks, allocated):
        chunk_id = alloc["chunk_id"]
        replicas = alloc["replicas"]
        leader = alloc["leader"]
        secondaries = [r for r in replicas if r != leader]

        # Data flow: push to all replicas in parallel.
        try:
            await storage_client.push_to_all_replicas(replicas, chunk_id, local_chunk.data)
        except Exception as exc:  # noqa: BLE001
            log.exception("data push failed for chunk %s", chunk_id)
            raise HTTPException(
                status_code=502,
                detail=f"chunk {alloc['index']} data push failed: {exc}",
            ) from exc

        # Control flow: single commit to the leader; leader handles acks.
        try:
            await storage_client.commit_to_leader(leader, chunk_id, secondaries)
        except Exception as exc:  # noqa: BLE001
            log.exception("leader commit failed for chunk %s", chunk_id)
            raise HTTPException(
                status_code=502,
                detail=f"chunk {alloc['index']} leader commit failed: {exc}",
            ) from exc

        replicas_hit.update(replicas)
        committed_chunks.append(
            {
                "chunk_id": chunk_id,
                "index": alloc["index"],
                "replicas": replicas,
                "leader": leader,
                "size_bytes": local_chunk.size_bytes,
            }
        )

    # 4. Register the file with the naming server.
    try:
        await nameserver_client.commit(
            file_id=file_id,
            filename=file.filename,
            size_bytes=size_bytes,
            chunks=committed_chunks,
        )
    except Exception as exc:  # noqa: BLE001
        log.exception("naming server commit failed")
        raise HTTPException(status_code=502, detail=f"register failed: {exc}") from exc

    return JSONResponse(
        {
            "ok": True,
            "file_id": file_id,
            "filename": file.filename,
            "size_bytes": size_bytes,
            "num_chunks": len(committed_chunks),
            "replicas_hit": sorted(replicas_hit),
        }
    )


# --- Scaffold: Read / Delete / Get-size client flows ------------------------
# These depend on the real naming server (Daryna) and storage read/delete
# (Shafeen). The UI this session only needs upload + Send.

@app.get("/api/files/{filename}")
async def read_file(filename: str):
    """Read flow: look up chunk locations, fetch each chunk, reassemble."""
    # TODO(Berat): after Daryna's GET /files/{filename} and Shafeen's
    # GET /chunks/{id} exist: lookup -> fetch each chunk from any replica ->
    # reassemble in index order using the stored size_bytes.
    raise HTTPException(status_code=501, detail="Read not implemented yet (Berat, after naming server).")


@app.delete("/api/files/{filename}")
async def delete_file(filename: str):
    """Delete flow: ask naming server for chunk ids, then purge replicas."""
    # TODO(Berat): after Daryna's DELETE /files/{filename} and Shafeen's
    # DELETE /chunks/{id} exist: delete metadata -> instruct each replica.
    raise HTTPException(status_code=501, detail="Delete not implemented yet (Berat, after naming server).")


@app.get("/api/files/{filename}/size")
async def file_size(filename: str):
    """Get-size flow: return size from metadata, no chunk transfer."""
    # TODO(Berat): after Daryna's GET /files/{filename}/size exists.
    raise HTTPException(status_code=501, detail="Get-size not implemented yet (Berat, after naming server).")
