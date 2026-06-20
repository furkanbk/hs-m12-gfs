# Naranja — Team Tasks

> **Check off your own items** as you complete them. **Do not edit the API
> contract in `docs/ARCHITECTURE.md` without telling the group.**

## Berat Furkan Koçak — Client + scaffold (mostly done this session)
- [x] Repo structure, docker-compose, .gitignore
- [x] CLAUDE.md, ARCHITECTURE.md skeleton, TASKS.md, README skeleton
- [x] Client UI (upload .txt + Send) and client FastAPI backend
- [x] 1024-byte chunker
- [x] Create/Write flow: push data to all replicas → commit to leader → register with nameserver
- [ ] Client side of Read (reassemble from replicas) — after naming server is real
- [ ] Client side of Delete and Get-size — after naming server is real
- [ ] Client-side handling for a replica being unreachable (retry/report)

## Daryna Karpenko — Naming server (metadata authority)
- [ ] Replace in-memory store with SQLite (metadata only; never store chunk bytes)
- [ ] Real `POST /allocate`: replica placement + leader selection
- [ ] `POST /commit`: persist file → chunk → replica/leader mapping
- [ ] `GET /files/{filename}` (chunk locations for reads)
- [ ] `DELETE /files/{filename}` (return chunks to purge, cascade delete metadata)
- [ ] `GET /files/{filename}/size` (from metadata, no chunk transfer)
- [ ] Dockerfile + healthz hardening

## Shafeen Noor — Storage server (chunk persistence)
- [ ] Real `PUT /chunks/{id}/data`: write bytes to `/data/{chunk_id}` on disk
- [ ] `GET /chunks/{id}`: stream chunk bytes back
- [ ] `DELETE /chunks/{id}`: remove chunk file
- [ ] Disk layout, fsync/durability, healthz
- [ ] Dockerfile + volume documentation

## Ivan Zhukau — Replication & leader/primary protocol
- [ ] Real `POST /chunks/{id}/commit` on leader: forward to secondaries, wait for ALL acks
- [ ] `POST /chunks/{id}/commit-replica`: secondary finalize + ack
- [ ] Failure handling: what happens when a secondary is down during a write
- [ ] Define "how many simultaneous failures we survive" and enforce it
- [ ] Coordinate the client↔storage contract with Berat and Shafeen

## Mikita Voitsik — Integration, DevOps & documentation
- [ ] Finalize docker-compose (volumes, network, ports, env, healthchecks)
- [ ] README: how to run (compose, ports) + how to use (client ops with examples)
- [ ] Architecture document: finalize, including full **fault-tolerance analysis**
      (storage server down? naming server = single point of failure? recoverable
      vs data-loss failures? how many simultaneous failures survivable?)
- [ ] End-to-end integration tests across all 5 services
- [ ] Optional CI
