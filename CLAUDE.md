# CLAUDE.md — Naranja DFS

Guidance for future Claude Code sessions working on this repository.

## Project summary

Naranja is a **GFS-inspired distributed file system for text files**. A file is
split into fixed **1024-byte chunks**, each replicated across **3 storage
servers** (replication factor 3). A single **naming server** is the metadata
authority; **storage servers** hold chunk content as files on the Linux
filesystem; a **client** with a web UI hides the distribution from the user.

**Roles:** 1 naming server, 3 storage servers, 1 client.

**Locked tech stack (uniform):** Python 3.11, FastAPI + uvicorn, httpx for
inter-service calls, plain HTML + vanilla JS for the UI (no build step), one
`Dockerfile` per component (`python:3.11-slim`), orchestrated by docker-compose.

## Ownership map

| Owner             | Component / directory                     |
| ----------------- | ----------------------------------------- |
| Berat Furkan Koçak| Client (`client/`) + scaffold & docs      |
| Daryna Karpenko   | Naming server (`nameserver/`)             |
| Shafeen Noor      | Storage server persistence (`storage/`)   |
| Ivan Zhukau       | Replication & leader protocol (`storage/` commit path) |
| Mikita Voitsik    | Integration, DevOps, docs (`docker-compose.yml`, `README.md`, `docs/`) |

## The golden rule

When working on a task, **modify only the component the current developer
owns**, unless explicitly told otherwise. Do **not** "helpfully" rewrite a
teammate's stub into a real implementation. Stubs are intentional integration
points; turning them into real logic steps on someone else's task.

## API contracts are sacred

The request/response shapes in [`docs/ARCHITECTURE.md`](docs/ARCHITECTURE.md)
are the integration boundary. **Never change them silently.** If a change is
truly needed, stop and flag it for the group.

## Definition of done per component

- **Client (Berat):** UI uploads a `.txt`, runs chunk → allocate → push to all
  replicas → commit to leader → register with naming server; shows success
  (file_id, chunk count, replicas hit) or a clear error. Read/Delete/Get-size
  client flows completed once the naming server is real.
- **Naming server (Daryna):** SQLite-backed metadata (never chunk bytes); real
  allocate/commit; files lookup/delete/size endpoints.
- **Storage server (Shafeen):** durable chunk writes to `/data/{chunk_id}`;
  read/delete chunk endpoints.
- **Replication (Ivan):** leader forwards to secondaries, waits for ALL acks;
  defined and enforced failure tolerance.
- **Integration (Mikita):** compose finalized; README + architecture finalized
  with fault-tolerance analysis; end-to-end tests.

## How to test locally

```bash
docker compose up --build
# open http://localhost:8080
# pick a .txt file, click Send, expect a success result against the stubs
```

## Reminders

- **Text files only.**
- **Chunk size is exactly 1024 bytes** (last chunk may be smaller).
- **Chunk content lives on the filesystem**, metadata in the DB — never chunk
  bytes in the database.
- **Replication factor 3.**
- Track progress in [`docs/TASKS.md`](docs/TASKS.md) — check off completed items.
