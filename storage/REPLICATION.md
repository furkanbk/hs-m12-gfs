# Replication & leader/primary commit protocol

> Owner: **Ivan Zhukau**. Scope: the chunk **commit path** on the storage
> server (`app/replication.py` + the `commit` / `commit-replica` handlers in
> `app/main.py`). This is the control-plane half of a write; chunk-byte
> persistence and read/delete are Shafeen's. The canonical request/response
> shapes live in [`docs/ARCHITECTURE.md`](../docs/ARCHITECTURE.md) and are
> unchanged by this work.

## What a write actually does

GFS decouples **data flow** from **control flow**, and so do we:

1. **Data flow (already done before commit):** the client pushes the chunk
   bytes to *all* replicas in parallel via `PUT /chunks/{id}/data`.
2. **Control flow (this module):** the client sends a **single commit to the
   leader** — `POST /chunks/{id}/commit` with the secondary list. The leader:
   1. **finalizes locally** — it must already hold the chunk bytes, else there
      is nothing to commit and it refuses (the leader is mandatory);
   2. **fans out** `POST /chunks/{id}/commit-replica` to every secondary in
      parallel, retrying transient failures (`COMMIT_RETRIES`, linear backoff);
   3. **gathers acks** and **enforces the write policy** (below);
   4. returns `{"ok": true, "acked": [...]}` on success, or **HTTP 503** with a
      human-readable reason on failure.

A secondary only acks a chunk whose bytes it actually holds; if the data never
arrived it replies **409**, which the leader records as a failed replica rather
than a phantom ack. This makes the `acked` list an honest record of durability.

The client treats the leader's response as **authoritative** — it never counts
secondary acks itself.

## Write policy: how many failures we survive — and we enforce it

One env-tunable knob, `WRITE_MIN_REPLICAS` = the minimum replicas (counting the
leader) that must finalize for a commit to succeed.

| `WRITE_MIN_REPLICAS` | Write succeeds when… | Writes survive | Committed chunks |
| --- | --- | --- | --- |
| `0` → **all** (default) | every replica acks | **0** storage failures | always at full factor (3) |
| `2` (factor 3) | leader + ≥1 secondary ack | **1** storage failure | may be under-replicated |
| `1` | leader acks | 2 (but leader-only) | likely under-replicated |

The default is **strict (all replicas)** on purpose. We do **not** implement
background re-replication/repair, so the only way to *guarantee* every committed
chunk has all 3 replicas — which is what lets the **read** path lose up to 2
replicas and still serve every chunk — is to refuse a write that can't reach
all 3. The trade-off is explicit: while a storage server is down the system
stays **readable but not writable** for chunks it hosts. Lowering the knob trades
that durability guarantee for write availability.

The value is **clamped to `[1, total]`**, so a misconfiguration can never demand
more replicas than exist or fewer than the (always-counted) leader.

### Failure cases, concretely

- **A secondary is down during a write** — under the default policy the leader
  cannot collect all acks, so the commit fails with 503 and the client can
  retry (e.g. once the server is back). No half-committed, under-replicated
  chunk is ever reported as durable.
- **The leader is down** — the client's commit never reaches it and fails fast.
  (Leader *re-election* is out of scope; the naming server's allocation fixes a
  per-chunk leader.)
- **A read with replicas down** — out of scope for this module, but the strict
  write policy guarantees 3 live copies at commit time, so a committed chunk
  survives up to **2** simultaneous storage failures on the read path.

## Configuration (safe defaults; no docker-compose change required)

| Env var | Default | Meaning |
| --- | --- | --- |
| `WRITE_MIN_REPLICAS` | `0` (all) | min replicas incl. leader to commit |
| `COMMIT_RETRIES` | `2` | retries per secondary (3 attempts total) |
| `COMMIT_BACKOFF` | `0.2` | linear backoff base, seconds |
| `REPLICA_TIMEOUT` | `10` | per-request timeout to a secondary, seconds |

> Note for **Mikita**: defaults need no compose edits. If we ever want writes to
> tolerate a down replica in the demo, set `WRITE_MIN_REPLICAS: "2"` on the
> storage services — happy to pair on it.

## Running the tests

```bash
cd storage
pip install -r requirements-dev.txt
python -m pytest
```

`tests/test_replication.py` covers the policy/quorum logic and leader fan-out;
`tests/test_endpoints.py` drives the FastAPI handlers (ack, 409 decline, 503 on
insufficient replicas) end-to-end with the secondary calls faked.
