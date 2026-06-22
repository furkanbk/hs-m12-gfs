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

- **Create / Write** — implemented: upload a `.txt` via the UI, or
  `POST /api/files` (multipart `file`).
- **Read** — *stub* (`GET /api/files/{filename}`), pending the real naming server.
- **Delete** — *stub* (`DELETE /api/files/{filename}`), pending the real naming server.
- **Get size** — *stub* (`GET /api/files/{filename}/size`), pending the real naming server.

## Documentation

- [Architecture & API contracts](docs/ARCHITECTURE.md)
- [Team tasks](docs/TASKS.md)
- [Agent / contributor guidance](CLAUDE.md)
