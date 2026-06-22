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
import logging
import os

import httpx

log = logging.getLogger("client.storage")

TIMEOUT = httpx.Timeout(float(os.environ.get("REQUEST_TIMEOUT", "10")))

# Replica unreachability is expected (a storage server can be down). We retry a
# transient failure a few times against the same replica, then — for reads —
# fall back to the next replica. Tunable via env so compose/tests can shorten it.
RETRY_ATTEMPTS = int(os.environ.get("STORAGE_RETRY_ATTEMPTS", "3"))
RETRY_BACKOFF = float(os.environ.get("STORAGE_RETRY_BACKOFF", "0.2"))


class ReplicaUnavailable(Exception):
    """Raised when every replica we tried for a chunk failed.

    Carries the per-replica errors so the caller can report exactly what went
    wrong (which replicas, which failure) rather than a single opaque message.
    """

    def __init__(self, chunk_id: str, attempts: dict[str, str]) -> None:
        self.chunk_id = chunk_id
        self.attempts = attempts
        detail = "; ".join(f"{addr}: {err}" for addr, err in attempts.items())
        super().__init__(f"chunk {chunk_id}: no replica reachable ({detail})")


def _is_transient(exc: Exception) -> bool:
    """A failure worth retrying against the *same* replica.

    Connection/timeout errors and 5xx responses are transient (server busy,
    restarting). A 4xx (e.g. 404 missing, 400 bad id) is not — for those we move
    on to the next replica instead of hammering this one.
    """
    if isinstance(exc, httpx.TransportError):
        return True
    if isinstance(exc, httpx.HTTPStatusError):
        return exc.response.status_code >= 500
    return False


async def push_data(replica: str, chunk_id: str, data: bytes) -> None:
    """PUT raw chunk bytes to a single replica, retrying transient failures.

    A write must reach ALL replicas (RF 3), so we do not fall back here — we
    retry the same replica on a transient error and re-raise if it stays down,
    letting the caller fail the write with a clear message.
    """
    url = f"http://{replica}/chunks/{chunk_id}/data"
    last_exc: Exception | None = None
    for attempt in range(1, RETRY_ATTEMPTS + 1):
        try:
            async with httpx.AsyncClient(timeout=TIMEOUT) as client:
                resp = await client.put(
                    url, content=data, headers={"Content-Type": "application/octet-stream"}
                )
                resp.raise_for_status()
            return
        except Exception as exc:  # noqa: BLE001 — classify below
            last_exc = exc
            if _is_transient(exc) and attempt < RETRY_ATTEMPTS:
                await asyncio.sleep(RETRY_BACKOFF * attempt)
                continue
            raise
    raise last_exc  # pragma: no cover — loop either returns or raises


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


# --- Read / Delete data plane (storage GET / DELETE /chunks/{id}) -----------

async def fetch_chunk(replica: str, chunk_id: str) -> bytes:
    """Fetch raw chunk bytes from a single replica. Raises on failure."""
    url = f"http://{replica}/chunks/{chunk_id}"
    async with httpx.AsyncClient(timeout=TIMEOUT) as client:
        resp = await client.get(url)
        resp.raise_for_status()
        return resp.content


async def fetch_chunk_any(replicas: list[str], chunk_id: str) -> bytes:
    """Fetch a chunk from whichever replica answers first (Read flow).

    Tries each replica in order, retrying transient failures a few times before
    falling back to the next replica. With replication factor 3 a chunk is
    readable as long as ≥1 replica is up. If every replica fails, raises
    ``ReplicaUnavailable`` carrying the per-replica errors.
    """
    attempts: dict[str, str] = {}
    for replica in replicas:
        last_exc: Exception | None = None
        for attempt in range(1, RETRY_ATTEMPTS + 1):
            try:
                return await fetch_chunk(replica, chunk_id)
            except Exception as exc:  # noqa: BLE001 — classify below
                last_exc = exc
                if _is_transient(exc) and attempt < RETRY_ATTEMPTS:
                    await asyncio.sleep(RETRY_BACKOFF * attempt)
                    continue
                break
        attempts[replica] = str(last_exc)
        log.warning("read fallback: chunk %s unreadable from %s (%s)", chunk_id, replica, last_exc)
    raise ReplicaUnavailable(chunk_id, attempts)


async def delete_chunk(replica: str, chunk_id: str) -> bool:
    """Delete a chunk replica. Returns False (instead of raising) if the replica
    is unreachable so the Delete flow can purge the survivors and report the
    rest. Storage delete is idempotent, so retrying a transient failure is safe.
    """
    url = f"http://{replica}/chunks/{chunk_id}"
    last_exc: Exception | None = None
    for attempt in range(1, RETRY_ATTEMPTS + 1):
        try:
            async with httpx.AsyncClient(timeout=TIMEOUT) as client:
                resp = await client.delete(url)
                resp.raise_for_status()
            return True
        except Exception as exc:  # noqa: BLE001 — classify below
            last_exc = exc
            if _is_transient(exc) and attempt < RETRY_ATTEMPTS:
                await asyncio.sleep(RETRY_BACKOFF * attempt)
                continue
            break
    log.warning("delete: chunk %s not purged from %s (%s)", chunk_id, replica, last_exc)
    return False
