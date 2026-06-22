# Naranja — Architecture

> Seeded by Berat; **finalized by Mikita** (integration + fault-tolerance
> analysis, §5). The **API contracts** below are the canonical integration
> boundary — do not change them silently.

## 1. Overview

Naranja is a GFS-inspired distributed file system for **text files only**.
There are three roles:

| Role            | Count | Responsibility |
| --------------- | ----- | -------------- |
| Naming server   | 1     | Metadata authority. Indexes all files, knows where every chunk lives. |
| Storage servers | 3     | Each stores only a fraction of all chunks, as files on the Linux FS. |
| Client          | 1     | Tool/UI that hides the distribution from the user. |

**Core rules**
- Files are split into fixed **1024-byte** chunks (last chunk may be smaller).
- Each chunk is replicated across **3** storage servers (replication factor 3).
- **Metadata** lives in the naming server (in-memory in the stub → SQLite).
- **Chunk content** lives on the **filesystem** of the storage servers under
  `/data/{chunk_id}`, never inside a database.

## 2. Tech stack & protocol decisions

- **Python 3.11 + FastAPI + uvicorn** for every service — one uniform stack the
  whole team knows.
- **httpx** for inter-service calls.
- Client UI is **plain HTML + vanilla JS** served by the client's FastAPI app —
  no framework, no build step.
- One `Dockerfile` per component (`python:3.11-slim`); everything runs via
  `docker compose up`.
- **Protocol:** HTTP/JSON for the control plane (client ↔ naming server) and
  HTTP raw bytes for the data plane (client ↔ storage, leader ↔ secondary).
  - *Why HTTP and not gRPC?* gRPC would be the more GFS-authentic choice
    (streaming, tighter contracts). We chose REST-ish HTTP for simplicity and
    debuggability in a student project; it is trivial to inspect with `curl`.

## 3. API contracts (canonical)

### Naming server (control plane)

**`POST /allocate`**
```json
// request
{ "filename": "notes.txt", "size_bytes": 5120, "num_chunks": 5 }
// response
{ "file_id": "<uuid>",
  "chunks": [
    { "chunk_id": "<uuid>", "index": 0,
      "replicas": ["storage-1:8000","storage-2:8000","storage-3:8000"],
      "leader": "storage-1:8000" }
  ] }
```
*(implemented — Daryna)* Stateless: generates ids and computes placement,
persisting nothing. With 3 servers / RF 3 every chunk lands on all 3 distinct
storage servers; the leader rotates round-robin by chunk index (not always
`storage-1`).

**`POST /commit`**
```json
// request
{ "file_id": "<uuid>", "filename": "notes.txt", "size_bytes": 5120,
  "chunks": [ { "chunk_id": "...", "index": 0, "replicas": ["..."],
               "leader": "...", "size_bytes": 1024 } ] }
// response
{ "ok": true }
```
*(implemented — Daryna)* Persists the file → chunk → replica/leader layout to
SQLite. Duplicate filename → `409`.

**`GET /files/{filename}`** → metadata + chunk locations (chunks ordered by
index, each with `size_bytes`); `404` if unknown. *(implemented — Daryna)*
**`DELETE /files/{filename}`** → `{ "chunk_ids": [...], "replicas": {chunk_id: [addr, ...]} }`;
cascade-deletes metadata; `404` if unknown. *(implemented — Daryna)*
**`GET /files/{filename}/size`** → `{ "size_bytes": 5120 }`; `404` if unknown. *(implemented — Daryna)*
**`GET /healthz`** → `{ "ok": true }`

### Storage server (data plane)

**`PUT /chunks/{chunk_id}/data`** — body = raw chunk bytes. Any replica stores
the bytes durably as `/data/{chunk_id}` using atomic temp-file replacement and
fsync. → `{ "ok": true, "chunk_id": "...", "size_bytes": 1024 }`.

**`POST /chunks/{chunk_id}/commit`** — sent **only to the leader** by the client.
```json
// request
{ "secondaries": ["storage-2:8000","storage-3:8000"] }
// response
{ "ok": true, "acked": ["storage-1:8000","storage-2:8000","storage-3:8000"] }
```
The leader finalizes locally, instructs each secondary via
`POST /chunks/{id}/commit-replica`, **waits for ALL acks**, then returns success.
On insufficient acks it fails with `503` so the client surfaces a retryable
error rather than treating an under-replicated chunk as durable.
*(implemented — Ivan; see `storage/REPLICATION.md`)*

**`POST /chunks/{chunk_id}/commit-replica`** — leader → secondary internal
finalize. A secondary only acks a chunk whose bytes it actually holds, else
`409`. → ack. *(implemented — Ivan/Shafeen)*.

**`GET /chunks/{chunk_id}`** → raw bytes from the local chunk file.
**`DELETE /chunks/{chunk_id}`** → `{ "ok": true, "chunk_id": "...", "deleted": true }`.
Delete is idempotent, so a missing chunk returns `deleted: false`.
**`GET /healthz`** → `{ "ok": true }` (storage also returns diagnostic
`self` and `data_dir` fields; consumers should rely only on `ok`).

## 4. Flows

### Write / Create (implemented in the client)

GFS decouples **data flow** from **control flow**:

1. Client splits the file into 1024-byte chunks.
2. Client → naming server `POST /allocate` → per-chunk replica list + leader.
3. Client **pushes chunk bytes to ALL replicas** (`PUT /chunks/{id}/data`) in
   parallel. *(data flow)*
4. Client sends a **single commit to the leader** (`POST /chunks/{id}/commit`)
   listing the secondaries. *(control flow)*
5. The **leader** finalizes on the secondaries and **waits for all acks**, then
   returns success. The client treats the chunk as durable **only when the
   leader returns success** — it does not collect secondary acks itself.
6. After all chunks commit, client → naming server `POST /commit` registers the
   file → chunk → locations mapping.

### Read *(implemented in the client)*
Client → naming server `GET /files/{filename}` for chunk locations → fetch each
chunk from any **reachable** replica → reassemble in index order using each
chunk's `size_bytes`. For every chunk the client tries the replicas in order,
retrying transient failures, and **falls back to the next replica** if one is
down; a chunk only fails the read when *all* its replicas are unreachable. The
reassembled file is returned as `text/plain`.

### Delete *(implemented in the client)*
Client → naming server `DELETE /files/{filename}` (returns chunk ids + replicas)
→ client instructs each storage server to delete its replica. Delete is
**best-effort and idempotent**: the metadata is gone once the naming server
responds, so unreachable replicas are reported back to the caller
(`replicas_failed`, i.e. orphaned chunks to reclaim later) rather than failing
the operation.

### Get size *(implemented in the client)*
Client → naming server `GET /files/{filename}/size` → size from metadata, no
chunk transfer.

### Client failure handling (retry / report)
Inter-service calls retry transient failures (connection errors, timeouts, 5xx)
with backoff before giving up; 4xx responses are not retried. **Writes** retry
each replica but never fall back — a write must reach all 3 replicas (RF 3).
**Reads** fall back across replicas as above. **Deletes** report unreachable
replicas instead of failing. Retry counts/backoff are env-tunable
(`STORAGE_RETRY_ATTEMPTS`, `STORAGE_RETRY_BACKOFF`).

## 5. Fault tolerance

Naranja's guarantee in one line: **with replication factor 3 and a strict write
policy, every committed file stays fully readable while up to 2 of the 3 storage
servers are down.** Writes, by deliberate design, are the opposite — they require
*all* replicas, trading write availability for a durability guarantee we can keep
without a repair daemon. This section makes that precise. Each claim below was
verified end-to-end against the running compose stack (see
[`tests/e2e/`](../tests/e2e) and the failure-matrix test).

### 5.1 Storage server down during a **write** → write fails (strict, by design)

A write has two phases (§4): the client first **pushes the chunk bytes to all 3
replicas in parallel** (data plane), then sends **one commit to the leader**,
which waits for every ack (control plane). Two independent guards both demand
full replication:

1. **Client data-push** (`client/app/storage_client.py`): `push_to_all_replicas`
   retries each replica but **never falls back** — if any replica cannot receive
   the bytes, the whole write fails (`502`). This is the canonical contract in
   §4: *"a write must reach all 3 replicas."*
2. **Leader commit policy** (`storage/app/replication.py`): `WRITE_MIN_REPLICAS`
   governs how many replicas (incl. the leader) must finalize. We run the
   **strict default (`0` → all)**, so the leader only returns success when every
   replica acks; otherwise it fails with `503`.

> **Order matters.** A fully-down storage node trips guard #1 first — the write
> dies in the data-push phase and never reaches the commit logic. So
> `WRITE_MIN_REPLICAS` only changes behaviour for a node that *received* the
> bytes but then fails to ack the commit (e.g. it crashes between push and
> commit). Relaxing the knob alone does **not** make Naranja write-available with
> a node down; the client's all-replicas push would have to change too, and that
> contract change is out of scope. We chose the strict, consistent story.

**Consequence:** while any storage server is down, the system is **readable but
not writable** for the chunks that node hosts (with RF 3 and 3 servers, that is
every chunk). No half-committed, under-replicated chunk is ever reported durable.
The strict policy is what *guarantees* 3 live copies at commit time — which is
the precondition for the read guarantee below. We accept "no writes during a
degradation" precisely because we do **not** run background re-replication, so a
relaxed policy would silently leave under-replicated chunks with no path back to
full health.

### 5.2 Storage server down during a **read** → survives 2 of 3 failures

Reads are the resilient path. The client asks the naming server for each chunk's
replica list, then tries the replicas **in order, falling back to the next on
failure** (`fetch_chunk_any`); a chunk only fails to read when *all* of its
replicas are unreachable. Because the strict write policy guarantees 3 live
copies at commit time, **a committed chunk survives up to 2 simultaneous storage
failures** and the file still reassembles. *(Verified: stopped 2 of 3 storage
servers, the file still read back byte-identical.)*

| Storage servers up (of 3) | Read | Write |
| ------------------------- | ---- | ----- |
| 3 | ✅ | ✅ |
| 2 | ✅ | ❌ (needs all 3) |
| 1 | ✅ | ❌ |
| 0 | ❌ | ❌ |

### 5.3 The naming server is the **single point of failure**

All metadata lives in one naming server. If it is down, **no allocate, lookup,
delete, or size** is possible: the chunk *bytes* are intact on storage, but they
are unreachable *by name* (the client has no other way to learn a file's chunk
layout). This is the system's defining SPOF.

**Mitigations in place:**
- **Persistence** — metadata is in **SQLite on a named Docker volume**
  (`nameserver-data:/data`, `DB_PATH=/data/naranja.db`) in **WAL mode**, so the
  naming server restarts with its full state intact. Nothing is held only in
  memory across the commit boundary (`allocate` is stateless; state is written at
  `commit`).
- **Auto-restart** — `restart: unless-stopped` (compose) brings the process back
  after a crash, and a real DB probe in `/healthz` (`SELECT 1`) reports it
  unhealthy until the database is actually reachable.

**Not done (honest scope):** the naming server is **not replicated** — there is
no standby/failover. A permanent loss of its volume *with no backup* is
unrecoverable (see §5.5). Replicating the naming server (e.g. Raft, or a
read-replica + backup of the SQLite file) is the obvious next step.

### 5.4 Replication math & placement

- **Factor 3, 3 servers:** placement (`nameserver/app/main.py:_place_chunk`) uses
  `factor = min(REPLICATION_FACTOR, len(pool)) = 3`, selecting 3 distinct servers
  from a rotating offset — so **every chunk lands on all three distinct storage
  servers** (replicas are spread, never doubled on one node). The leader rotates
  round-robin by chunk index, so no single node is leader for everything.
- **Availability:** a chunk is readable iff **≥ 1** of its 3 replicas is up ⇒ we
  tolerate **2 simultaneous** storage failures on the read path.
- **Durability at commit:** the strict write policy means a chunk is only ever
  *recorded* as committed when **all 3** copies exist, so the "tolerate 2" math
  holds for every committed chunk (no chunk is silently born under-replicated).

### 5.5 Recoverable vs. unrecoverable failures

| Failure | Recoverable? | Why |
| ------- | ------------ | --- |
| Storage server crash/restart, **volume intact** | ✅ Recoverable | Chunk files persist on the named volume; `restart: unless-stopped` brings it back; reads fall back to other replicas meanwhile. |
| Naming server crash/restart, **volume intact** | ✅ Recoverable | SQLite (WAL) on the named volume reloads full metadata on restart. |
| Client crash | ✅ Recoverable | Client is stateless; restart and retry. In-flight operations are retried by design (retry/backoff, §4). |
| **1–2** storage volumes lost permanently | ✅ Recoverable (no data loss) | Remaining replica(s) still serve every chunk; files stay readable. (Re-replication to restore factor 3 is manual — not automated.) |
| **All 3** replicas of a chunk lost permanently | ❌ **Data loss** | No copy remains; the chunk (and its file) is gone. |
| Naming server volume lost with **no backup** | ❌ **Metadata loss** | Bytes survive on storage but are orphaned/unreachable by name; effectively lost without the index. |

### 5.6 Explicitly out of scope (and why it's safe for this project)

- **Leader re-election** — a chunk's leader is fixed by allocation; if the leader
  is down, that chunk's *commit* fails fast (the write already needs all 3 up
  anyway, §5.1). Reads don't use the leader.
- **Background re-replication / repair** — none. This is *why* the strict write
  policy is correct: it is the only way to keep the "every committed chunk has 3
  copies" invariant the read guarantee depends on.
- **Network partition / split-brain** — single naming server ⇒ one authority,
  so there is no split-brain to resolve; a partitioned client simply sees
  failures and retries.
- **Naming-server replication / backups** — the known SPOF (§5.3); the
  documented next step.
