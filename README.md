# Naranja — Distributed File System

A GFS-inspired distributed file system for text files: files are split into
1024-byte chunks, replicated across 3 storage servers, indexed by a single
naming server, and accessed through a client with a web UI.

> Skeleton by Berat; finalized by **Mikita** (run/use docs, fault-tolerance
> summary, testing & CI).

## Quick start

```bash
docker compose up --build
```

Then open **http://localhost:8080**, pick a `.txt` file, and click **Send**.
The client splits it into 1024-byte chunks, pushes each chunk to all 3 storage
replicas, commits via the leader, and registers the file with the naming server.
The same page can **Read**, **Get size**, and **Delete** a stored file by name.

> **Port 8080 already in use?** Only the client port is published; override the
> host port without touching the compose file:
> `NARANJA_CLIENT_PORT=8088 docker compose up --build` → open `http://localhost:8088`.

### Architecture at a glance

```
                 ┌──────────────┐   metadata (control plane)   ┌───────────────────┐
   browser  ───▶ │    client    │ ───────────────────────────▶ │    nameserver     │
   :8080         │  (web UI +   │   allocate / commit / lookup  │  SQLite metadata  │
                 │   backend)   │                               │   (the only SPOF) │
                 └──────┬───────┘                               └───────────────────┘
                        │ chunk bytes (data plane): push to ALL 3, commit to leader
                        ▼
            ┌───────────────┬───────────────┬───────────────┐
            │   storage-1   │   storage-2   │   storage-3   │   chunk files on /data
            └───────────────┴───────────────┴───────────────┘   (replication factor 3)
```

Data flow (moving bytes) is decoupled from control flow (deciding durability):
the client pushes bytes to all replicas, then sends a single commit to the
chunk's **leader**, which finalizes the secondaries and waits for their acks.
See [`docs/ARCHITECTURE.md`](docs/ARCHITECTURE.md) for the full flows and the
canonical API contracts.

## Components

| Service       | Port (host)          | Description |
| ------------- | -------------------- | ----------- |
| `client`      | 8080 → 8000          | Web UI + client backend (Create/Read/Delete/Size). |
| `nameserver`  | internal             | Metadata authority (allocate/commit/lookup). |
| `storage-1/2/3` | internal           | Chunk storage on the Linux FS (`/data`, named volumes). |

Internal services talk over the compose network by service name
(e.g. `storage-1:8000`). Only the client UI is exposed to the host.

Each storage container writes chunk bytes to its own named Docker volume:
`storage1-data`, `storage2-data`, and `storage3-data`. Chunk files are stored
under `/data/{chunk_id}` inside the container and are never stored in a database.
The storage server writes via temp file + atomic replace + fsync, streams chunks
back with `GET /chunks/{chunk_id}`, and supports idempotent replica deletion with
`DELETE /chunks/{chunk_id}`.

## Usage (client operations)

All client operations are implemented end-to-end against the real naming and
storage servers:

- **Create / Write** — upload a `.txt` via the UI, or `POST /api/files`
  (multipart `file`). Returns `file_id`, chunk count, and replicas hit.
- **Read** — `GET /api/files/{filename}`: looks up chunk locations, fetches each
  chunk from any reachable replica, and reassembles the file in index order.
- **Delete** — `DELETE /api/files/{filename}`: drops the metadata, then purges
  every replica. Reports `replicas_purged` and any `replicas_failed`.
- **Get size** — `GET /api/files/{filename}/size`: size from metadata only, no
  chunk transfer.

```bash
# Create
curl -F "file=@notes.txt" http://localhost:8080/api/files
# Read (reassembled file to stdout)
curl http://localhost:8080/api/files/notes.txt
# Get size
curl http://localhost:8080/api/files/notes.txt/size
# Delete
curl -X DELETE http://localhost:8080/api/files/notes.txt
```

**Replica unreachable (retry / report).** Transient failures
(connection/timeout/5xx) are retried with backoff. A **read** then falls back to
the next replica, so a file stays readable as long as ≥1 of its 3 replicas is
up. A **write** retries but does not fall back — it must reach all 3 replicas. A
**delete** is best-effort and idempotent: unreachable replicas are reported as
`replicas_failed` rather than failing the whole operation.

## Fault tolerance (summary)

Replication factor 3 with a **strict write policy** buys a clear guarantee, all
verified end-to-end in [`tests/e2e/`](tests/e2e):

| Scenario | Behaviour |
| -------- | --------- |
| 1–2 of 3 storage servers down | **Reads still work** — the client falls back across replicas; a chunk is readable while ≥1 of its 3 replicas is up. |
| Any storage server down | **Writes fail (by design).** A write must reach all 3 replicas, so no chunk is ever silently committed under-replicated. The system is *readable but not writable* during a degradation. |
| Naming server restart | **Metadata survives** — it lives in SQLite (WAL) on a named volume, so the server restarts with full state. |
| Naming server permanently lost (no backup) | **Unrecoverable** — bytes survive on storage but are unreachable by name. This is the system's single point of failure. |

Full analysis (recoverable vs. data-loss failures, replication math, what's out
of scope and why) is in [`docs/ARCHITECTURE.md` §5](docs/ARCHITECTURE.md#5-fault-tolerance).

## Testing & CI

```bash
# Storage commit-path unit tests (no Docker)
cd storage && pip install -r requirements-dev.txt && python -m pytest

# Full end-to-end suite across all 5 services (brings the stack up & down)
pip install -r tests/e2e/requirements.txt
NARANJA_CLIENT_PORT=8088 python -m pytest tests/e2e -v   # drop the env var if 8080 is free
```

The e2e suite covers create/read/size/delete, chunk boundaries, empty + UTF-8
files, and fault injection (storage nodes down, naming-server restart). Both
suites run on every pull request and on pushes to `main` via
[GitHub Actions](.github/workflows/ci.yml). See
[`tests/e2e/README.md`](tests/e2e/README.md) for options (e.g. reusing a running
stack).

## Documentation

- [Architecture & API contracts](docs/ARCHITECTURE.md)
- [Team tasks](docs/TASKS.md)
- [Agent / contributor guidance](CLAUDE.md)
