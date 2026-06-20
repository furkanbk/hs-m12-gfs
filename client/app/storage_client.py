"""Helpers for talking to storage servers (data plane).

Part of Berat's client. Implements the GFS-style decoupling of data flow and
control flow:

  1. push_data(...)        -> push chunk bytes to ALL replicas, in parallel.
  2. commit_to_leader(...) -> send a single commit to the leader, which is
                              responsible for finalizing on the secondaries and
                              waiting for their acks. The client treats the
                              leader's response as authoritative.

The client does NOT collect secondary acks itself — that is the leader's job
(owner: Ivan).
"""
from __future__ import annotations

import asyncio
import os

import httpx

TIMEOUT = httpx.Timeout(float(os.environ.get("REQUEST_TIMEOUT", "10")))


async def push_data(replica: str, chunk_id: str, data: bytes) -> None:
    """PUT raw chunk bytes to a single replica. Raises on failure."""
    url = f"http://{replica}/chunks/{chunk_id}/data"
    async with httpx.AsyncClient(timeout=TIMEOUT) as client:
        resp = await client.put(
            url, content=data, headers={"Content-Type": "application/octet-stream"}
        )
        resp.raise_for_status()


async def push_to_all_replicas(replicas: list[str], chunk_id: str, data: bytes) -> None:
    """Push the chunk bytes to every replica in parallel (data flow).

    Raises the first error if any replica fails so the caller can surface it.
    """
    results = await asyncio.gather(
        *(push_data(replica, chunk_id, data) for replica in replicas),
        return_exceptions=True,
    )
    errors = [
        (replica, exc)
        for replica, exc in zip(replicas, results)
        if isinstance(exc, Exception)
    ]
    if errors:
        replica, exc = errors[0]
        raise RuntimeError(f"push to replica {replica} failed: {exc}") from exc


async def commit_to_leader(leader: str, chunk_id: str, secondaries: list[str]) -> dict:
    """Send a single commit to the leader (control flow).

    The leader finalizes locally, instructs the secondaries, waits for all acks,
    and returns success. We treat the leader's response as the authoritative
    success signal for this chunk.
    """
    url = f"http://{leader}/chunks/{chunk_id}/commit"
    async with httpx.AsyncClient(timeout=TIMEOUT) as client:
        resp = await client.post(url, json={"secondaries": secondaries})
        resp.raise_for_status()
        return resp.json()


# --- Scaffold: depend on Shafeen's real storage read/delete -----------------

async def fetch_chunk(replica: str, chunk_id: str) -> bytes:
    """Fetch raw chunk bytes from any replica (used by the Read flow)."""
    # TODO(Shafeen): implement GET /chunks/{chunk_id} on the storage server.
    raise NotImplementedError(
        "fetch_chunk() needs the real storage GET /chunks/{chunk_id} (owner: Shafeen)"
    )


async def delete_chunk(replica: str, chunk_id: str) -> None:
    """Delete a chunk replica (used by the Delete flow)."""
    # TODO(Shafeen): implement DELETE /chunks/{chunk_id} on the storage server.
    raise NotImplementedError(
        "delete_chunk() needs the real storage DELETE /chunks/{chunk_id} (owner: Shafeen)"
    )
