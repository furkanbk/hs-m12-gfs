# Naranja — Architecture

> Seeded by Berat this session. **Mikita finalizes**, especially the
> fault-tolerance section. The **API contracts** below are the canonical
> integration boundary — do not change them silently.

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
*Stub:* generate ids, place every chunk on all 3 storage servers,
`leader = storage-1:8000`.

**`POST /commit`**
```json
// request
{ "file_id": "<uuid>", "filename": "notes.txt", "size_bytes": 5120,
  "chunks": [ { "chunk_id": "...", "index": 0, "replicas": ["..."],
               "leader": "...", "size_bytes": 1024 } ] }
// response
{ "ok": true }
```
*Stub:* store in an in-memory dict.

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
**`GET /healthz`** → `{ "ok": true }`

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

## 5. Fault tolerance — to be completed (owner: Mikita)

Answer these explicitly:

- **Storage server down during a write:** what does the leader do when a
  secondary does not ack? Do we fail the write, or commit with fewer replicas
  and repair later?
- **Storage server down during a read:** reads can use any replica — how many
  replicas must survive to still serve every chunk?
- **Naming server is a single point of failure:** if it dies, no allocate /
  lookup / delete is possible (data on storage is intact but unreachable by
  name). Mitigations: persistence (SQLite on a volume) so it can restart with
  state; future: replicate the naming server.
- **Replication math:** with replication factor 3, a chunk is available as long
  as ≥1 of its 3 replicas is up. State how many *simultaneous* storage failures
  we survive (target: 2) and confirm the placement actually spreads replicas.
- **Recoverable vs unrecoverable failures:** process restart with intact volume
  (recoverable) vs. loss of all replicas of a chunk / loss of the naming-server
  metadata with no backup (data loss).
