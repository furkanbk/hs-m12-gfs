"""Helpers for talking to the naming server (control plane).

Part of Berat's client. Conforms to the API contract in docs/ARCHITECTURE.md.
``allocate`` and ``commit`` are fully implemented against the stub naming
server. ``lookup``/``delete``/``size`` are scaffolded for the read/delete/size
flows that depend on Daryna's real naming server.
"""
from __future__ import annotations

import os

import httpx

NAMESERVER_ADDR = os.environ.get("NAMESERVER_ADDR", "nameserver:8000")
TIMEOUT = httpx.Timeout(float(os.environ.get("REQUEST_TIMEOUT", "10")))


def _base_url() -> str:
    return f"http://{NAMESERVER_ADDR}"


async def allocate(filename: str, size_bytes: int, num_chunks: int) -> dict:
    """Ask the naming server to allocate chunk ids + replica placement.

    Returns the parsed JSON: { "file_id", "chunks": [ { chunk_id, index,
    replicas, leader }, ... ] }.
    """
    payload = {
        "filename": filename,
        "size_bytes": size_bytes,
        "num_chunks": num_chunks,
    }
    async with httpx.AsyncClient(timeout=TIMEOUT) as client:
        resp = await client.post(f"{_base_url()}/allocate", json=payload)
        resp.raise_for_status()
        return resp.json()


async def commit(file_id: str, filename: str, size_bytes: int, chunks: list[dict]) -> dict:
    """Register the file -> chunk -> locations mapping with the naming server."""
    payload = {
        "file_id": file_id,
        "filename": filename,
        "size_bytes": size_bytes,
        "chunks": chunks,
    }
    async with httpx.AsyncClient(timeout=TIMEOUT) as client:
        resp = await client.post(f"{_base_url()}/commit", json=payload)
        resp.raise_for_status()
        return resp.json()


# --- Scaffold: depend on Daryna's real naming server ------------------------

async def lookup(filename: str) -> dict:
    """Fetch metadata + chunk locations for a file (used by the Read flow)."""
    # TODO(Daryna): implement GET /files/{filename} on the naming server,
    # then wire this up. Returns chunk locations in index order.
    raise NotImplementedError(
        "lookup() needs the real naming server GET /files/{filename} (owner: Daryna)"
    )


async def delete(filename: str) -> dict:
    """Delete a file's metadata; returns chunk_ids + replicas to purge."""
    # TODO(Daryna): implement DELETE /files/{filename} on the naming server.
    raise NotImplementedError(
        "delete() needs the real naming server DELETE /files/{filename} (owner: Daryna)"
    )


async def size(filename: str) -> dict:
    """Return the file size from metadata (no chunk transfer)."""
    # TODO(Daryna): implement GET /files/{filename}/size on the naming server.
    raise NotImplementedError(
        "size() needs the real naming server GET /files/{filename}/size (owner: Daryna)"
    )
