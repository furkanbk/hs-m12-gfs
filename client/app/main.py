"""Naranja DFS — Client service (Berat's part, fully implemented).

A FastAPI app that:
  * serves a minimal web UI at /  (upload a .txt + Send, then read/size/delete),
  * runs the full Create/Write flow against the naming + storage servers,
  * runs the Read / Delete / Get-size client flows against the real naming
    server, tolerating an unreachable replica (retry, then fall back).

Config comes entirely from environment variables (see nameserver_client and
storage_client) so docker-compose can wire hostnames — no hardcoded hosts here.
"""
from __future__ import annotations

import logging
import os
from pathlib import Path

import httpx
from fastapi import FastAPI, HTTPException, UploadFile
from fastapi.responses import FileResponse, JSONResponse, Response

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


# --- Read / Delete / Get-size client flows ----------------------------------
# These run against the real naming server (Daryna) and storage read/delete
# (Shafeen/Ivan). Reads and deletes tolerate an unreachable replica.

def _nameserver_http_error(exc: httpx.HTTPStatusError, *, what: str) -> HTTPException:
    """Translate an upstream naming-server error into a client-facing one.

    A 404 (unknown file) is passed straight through; anything else is reported
    as a 502 so the UI can tell "no such file" apart from "the cluster is sick".
    """
    status = exc.response.status_code
    if status == 404:
        return HTTPException(status_code=404, detail="File not found.")
    return HTTPException(status_code=502, detail=f"{what} failed upstream: HTTP {status}")


@app.get("/api/files/{filename}")
async def read_file(filename: str):
    """Read flow: look up chunk locations, fetch each chunk from any reachable
    replica, reassemble in index order. Returns the file as plain text."""
    try:
        meta = await nameserver_client.lookup(filename)
    except httpx.HTTPStatusError as exc:
        raise _nameserver_http_error(exc, what="lookup") from exc
    except Exception as exc:  # noqa: BLE001 — naming server unreachable
        log.exception("lookup failed")
        raise HTTPException(status_code=502, detail=f"lookup failed: {exc}") from exc

    chunks = sorted(meta.get("chunks", []), key=lambda c: c["index"])
    pieces: list[bytes] = []
    for chunk in chunks:
        try:
            data = await storage_client.fetch_chunk_any(chunk["replicas"], chunk["chunk_id"])
        except storage_client.ReplicaUnavailable as exc:
            log.error("read failed: %s", exc)
            raise HTTPException(
                status_code=503,
                detail=f"chunk {chunk['index']} unavailable: {exc}",
            ) from exc
        # Trust the stored size_bytes as the source of truth for chunk length.
        pieces.append(data[: chunk["size_bytes"]])

    body = b"".join(pieces)
    return Response(
        content=body,
        media_type="text/plain; charset=utf-8",
        headers={"Content-Disposition": f'inline; filename="{filename}"'},
    )


@app.delete("/api/files/{filename}")
async def delete_file(filename: str) -> JSONResponse:
    """Delete flow: drop the metadata (naming server), then purge every replica.

    Storage delete is idempotent and best-effort: if a replica is unreachable we
    still report success for the file (metadata is gone) but list the replicas
    that could not be purged so an operator can reclaim the orphaned chunks.
    """
    try:
        result = await nameserver_client.delete(filename)
    except httpx.HTTPStatusError as exc:
        raise _nameserver_http_error(exc, what="delete") from exc
    except Exception as exc:  # noqa: BLE001 — naming server unreachable
        log.exception("metadata delete failed")
        raise HTTPException(status_code=502, detail=f"delete failed: {exc}") from exc

    replicas_by_chunk: dict[str, list[str]] = result.get("replicas", {})
    purged = 0
    failed: list[dict] = []
    for chunk_id, replicas in replicas_by_chunk.items():
        for replica in replicas:
            if await storage_client.delete_chunk(replica, chunk_id):
                purged += 1
            else:
                failed.append({"chunk_id": chunk_id, "replica": replica})

    return JSONResponse(
        {
            "ok": True,
            "filename": filename,
            "num_chunks": len(result.get("chunk_ids", [])),
            "replicas_purged": purged,
            "replicas_failed": failed,
        }
    )


@app.get("/api/files/{filename}/size")
async def file_size(filename: str) -> JSONResponse:
    """Get-size flow: return size from metadata, no chunk transfer."""
    try:
        result = await nameserver_client.size(filename)
    except httpx.HTTPStatusError as exc:
        raise _nameserver_http_error(exc, what="size") from exc
    except Exception as exc:  # noqa: BLE001 — naming server unreachable
        log.exception("size lookup failed")
        raise HTTPException(status_code=502, detail=f"size lookup failed: {exc}") from exc

    return JSONResponse({"filename": filename, "size_bytes": result["size_bytes"]})
