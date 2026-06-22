# Naranja — Distributed File System

A GFS-inspired distributed file system for text files: files are split into
1024-byte chunks, replicated across 3 storage servers, indexed by a single
naming server, and accessed through a client with a web UI.

> Skeleton by Berat. **Mikita finalizes** the README.

## Quick start

```bash
docker compose up --build
```

Then open **http://localhost:8080**, pick a `.txt` file, and click **Send**.
The client splits it into 1024-byte chunks, pushes each chunk to all 3 storage
replicas, commits via the leader, and registers the file with the naming server.
The same page can **Read**, **Get size**, and **Delete** a stored file by name.

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

## Documentation

- [Architecture & API contracts](docs/ARCHITECTURE.md)
- [Team tasks](docs/TASKS.md)
- [Agent / contributor guidance](CLAUDE.md)
