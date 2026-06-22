"""Naranja DFS — replication & leader/primary commit protocol (owner: Ivan).

This module implements the *control plane* of a chunk write. By the time a
commit arrives, the chunk bytes have already been pushed to every replica
out-of-band by the client (``PUT /chunks/{id}/data``). GFS-style, data flow and
control flow are decoupled: the commit is where the **leader** finalizes the
chunk locally, drives every **secondary** to finalize via
``POST /chunks/{id}/commit-replica``, and waits for their acks before declaring
the chunk durable.

Failure tolerance is governed by one explicit, enforced policy:
``WRITE_MIN_REPLICAS`` — the minimum number of replicas (counting the leader)
that must finalize for a commit to succeed.

* Default (``0`` → "all replicas"): a commit succeeds only when **every** replica
  acks. This keeps every committed chunk at the full replication factor, which
  is what lets the read path lose up to ``factor - 1`` replicas and still serve
  the chunk. The trade-off: a single storage server being down blocks *new*
  writes to chunks it hosts (the system stays readable, not writable).
* Lowering it (e.g. ``2`` with factor 3) lets writes proceed while one replica
  is down, at the cost of committing under-replicated chunks. We do **not**
  implement background re-replication/repair, so the strict default is the safe
  choice for this project and is what the architecture's "survive 2 failures on
  read" guarantee depends on.

Pure of FastAPI: handlers in ``main.py`` translate :class:`CommitOutcome` into
HTTP responses. This module only knows about chunks, replicas, and httpx.
"""
from __future__ import annotations

import asyncio
import logging
import os
import re
from dataclasses import dataclass
from pathlib import Path

import httpx

log = logging.getLogger("storage.replication")

# --- Tunables (env-overridable; safe defaults need no docker-compose change) --
REPLICA_TIMEOUT = httpx.Timeout(float(os.environ.get("REPLICA_TIMEOUT", "10")))
# Attempts per secondary = 1 initial try + COMMIT_RETRIES retries.
COMMIT_RETRIES = int(os.environ.get("COMMIT_RETRIES", "2"))
COMMIT_BACKOFF = float(os.environ.get("COMMIT_BACKOFF", "0.2"))
# Backstop deadline for the whole secondary fan-out, so a degraded cluster can
# never pin a commit handler for longer than this.
LEADER_COMMIT_TIMEOUT = float(os.environ.get("LEADER_COMMIT_TIMEOUT", "30"))
# Minimum replicas (incl. leader) that must finalize. <= 0 == "all replicas".
WRITE_MIN_REPLICAS = int(os.environ.get("WRITE_MIN_REPLICAS", "0"))

# Chunk ids are UUIDs minted by the naming server. Pin the charset so a hostile
# or buggy caller can never use one to escape DATA_DIR (e.g. "../../etc/passwd").
_CHUNK_ID_RE = re.compile(r"^[A-Za-z0-9_.-]+$")


def is_valid_chunk_id(chunk_id: str) -> bool:
    """Reject ids that could traverse outside ``DATA_DIR`` when used as a path."""
    return (
        bool(chunk_id)
        and chunk_id not in (".", "..")
        and _CHUNK_ID_RE.fullmatch(chunk_id) is not None
    )


@dataclass(frozen=True)
class CommitOutcome:
    """Result of a leader commit. Immutable — built once and returned."""

    ok: bool
    acked: tuple[str, ...]
    failed: tuple[str, ...]
    required: int

    @property
    def total(self) -> int:
        return len(self.acked) + len(self.failed)

    def reason(self) -> str:
        return (
            f"commit needs >= {self.required} of {self.total} replicas to finalize, "
            f"got {len(self.acked)} ack(s); failed: {', '.join(self.failed) or 'none'}"
        )


def required_replicas(total: int) -> int:
    """Resolve the enforced write policy for a chunk with ``total`` replicas.

    ``WRITE_MIN_REPLICAS <= 0`` means "all replicas". Any explicit value is
    clamped to ``[1, total]`` so a misconfiguration can never silently demand
    more replicas than exist or fewer than one (the leader always counts).
    """
    if WRITE_MIN_REPLICAS <= 0:
        return total
    return max(1, min(WRITE_MIN_REPLICAS, total))


def chunk_present(data_dir: Path, chunk_id: str) -> bool:
    """Has the chunk's data been pushed here yet?

    A replica can only finalize a chunk it actually holds — committing data you
    do not have would be a phantom ack. The bytes are written by the data-plane
    ``PUT /chunks/{id}/data`` (owned by Shafeen); we only check for their
    presence as the commit pre-condition.
    """
    return (data_dir / chunk_id).is_file()


async def finalize_secondary(secondary: str, chunk_id: str) -> bool:
    """Drive one secondary to finalize the chunk; return True iff it acks.

    Retries transient failures with linear backoff. A non-2xx response or a
    falsy ``ok`` body counts as a failed ack (the secondary does not hold the
    chunk, or could not finalize it).
    """
    url = f"http://{secondary}/chunks/{chunk_id}/commit-replica"
    last_err: Exception | None = None
    async with httpx.AsyncClient(timeout=REPLICA_TIMEOUT) as client:
        for attempt in range(COMMIT_RETRIES + 1):
            try:
                resp = await client.post(url)
                resp.raise_for_status()
                if resp.json().get("ok", False):
                    return True
                # 2xx but ok=false: a soft decline (e.g. data still landing) —
                # treat like any other non-ack and retry rather than give up.
                last_err = RuntimeError(f"{secondary} declined commit (ok=false)")
            except Exception as exc:  # noqa: BLE001 — any failure is a non-ack
                last_err = exc
            if attempt < COMMIT_RETRIES:
                await asyncio.sleep(COMMIT_BACKOFF * (attempt + 1))
    log.warning(
        "secondary %s failed to finalize chunk %s after %d attempts: %s",
        secondary,
        chunk_id,
        COMMIT_RETRIES + 1,
        last_err,
    )
    return False


async def leader_commit(
    *,
    self_addr: str,
    chunk_id: str,
    secondaries: list[str],
    data_dir: Path,
) -> CommitOutcome:
    """Run the leader side of the two-phase-ish commit for one chunk.

    1. Finalize locally — the leader must hold the chunk, else there is nothing
       to commit (the commit fails outright; the leader is mandatory).
    2. Fan out ``commit-replica`` to every secondary in parallel and gather acks.
    3. Enforce :func:`required_replicas`: succeed iff enough replicas finalized,
       *and* the leader is among them.

    The returned :class:`CommitOutcome` lists exactly which replicas acked, so
    the HTTP layer can report durability honestly.
    """
    total = 1 + len(secondaries)
    required = required_replicas(total)

    if not chunk_present(data_dir, chunk_id):
        # Leader has no data — refuse rather than fan out a doomed commit.
        log.error("leader %s missing data for chunk %s; refusing commit", self_addr, chunk_id)
        return CommitOutcome(
            ok=False, acked=(), failed=(self_addr, *secondaries), required=required
        )

    try:
        results = await asyncio.wait_for(
            asyncio.gather(*(finalize_secondary(s, chunk_id) for s in secondaries)),
            timeout=LEADER_COMMIT_TIMEOUT,
        )
    except asyncio.TimeoutError:
        log.error(
            "commit for chunk %s exceeded %.0fs fan-out deadline", chunk_id, LEADER_COMMIT_TIMEOUT
        )
        return CommitOutcome(
            ok=False, acked=(self_addr,), failed=tuple(secondaries), required=required
        )

    acked = [self_addr]
    failed: list[str] = []
    for secondary, ok in zip(secondaries, results):
        (acked if ok else failed).append(secondary)

    # Leader is always present in `acked` here, so this enforces both the count
    # and the "leader must finalize" rule.
    satisfied = len(acked) >= required
    return CommitOutcome(
        ok=satisfied, acked=tuple(acked), failed=tuple(failed), required=required
    )
