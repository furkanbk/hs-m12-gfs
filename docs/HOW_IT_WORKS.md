# How Naranja DFS Works — End to End

This document explains how the system actually behaves during a Write, Read, and Delete, including how consistency is kept and how the commit acknowledgement protocol works. Written for the team to study and present.

---

## The Big Picture

```
Browser / User
      │
      ▼
  [Client :8080]          ← web UI + orchestration
      │
      ├──────────────────────────────────────►  [Naming Server :8001]
      │                                          SQLite DB
      │                                          (metadata only)
      │
      ├──► [Storage-1 :8002]
      ├──► [Storage-2 :8003]
      └──► [Storage-3 :8004]
                /data/{chunk_id}   (raw bytes on disk)
```

- The **naming server** is the metadata brain. It knows where every chunk lives but never touches chunk bytes.
- The **storage servers** hold the actual bytes. Three identical containers, each with its own `/data` volume.
- The **client** is the orchestrator. It talks to both planes and hides all of this from the user's browser.

---

## The Two Planes (GFS-style)

Everything flows along two separate paths:

| Plane | What it carries | Who drives it |
|---|---|---|
| **Data plane** | raw chunk bytes | Client → Storage servers (in parallel) |
| **Control plane** | commit/ack signals | Client → Leader → Secondaries |

These two happen one after the other for every chunk. Data goes first, control second. This separation is the core GFS design idea.

---

## WRITE — Step by Step

**Trigger:** User picks a `.txt` file and clicks Send.

### Step 1 — Chunking (Client)

The client reads the file into memory and splits it into 1024-byte pieces.

```
"hello world, this is..." (2500 bytes)
  → Chunk 0: bytes   0–1023  (1024 bytes)
  → Chunk 1: bytes 1024–2047 (1024 bytes)
  → Chunk 2: bytes 2048–2499  (452 bytes, last chunk is smaller)
```

### Step 2 — Allocate (Client → Naming Server)

The client sends one request to the naming server:

```
POST /allocate
{ "filename": "notes.txt", "size_bytes": 2500, "num_chunks": 3 }
```

The naming server generates UUIDs and decides replica placement using a round-robin formula — chunk 0 starts at server 0, chunk 1 starts at server 1, etc. With RF=3 and 3 servers, every chunk lands on all three servers.

```
Response:
{
  "file_id": "uuid-A",
  "chunks": [
    { "chunk_id": "uuid-1", "index": 0, "replicas": ["storage-1", "storage-2", "storage-3"], "leader": "storage-1" },
    { "chunk_id": "uuid-2", "index": 1, "replicas": ["storage-2", "storage-3", "storage-1"], "leader": "storage-2" },
    { "chunk_id": "uuid-3", "index": 2, "replicas": ["storage-3", "storage-1", "storage-2"], "leader": "storage-3" }
  ]
}
```

**Nothing is written to the database yet.** Allocate is stateless — it just mints IDs.

### Step 3 — Push Data (Client → All 3 Storage Servers, in parallel)

For each chunk, the client pushes the raw bytes to **all 3 replicas at the same time**:

```
PUT storage-1/chunks/uuid-1/data  ─┐
PUT storage-2/chunks/uuid-1/data  ─┼─ parallel, all must succeed
PUT storage-3/chunks/uuid-1/data  ─┘
```

Each storage server writes the bytes durably:
1. Writes to a temp file first (`.{chunk_id}.tmp`)
2. `fsync` — flushes the temp file to disk
3. Atomic `rename` — replaces the real file path in one kernel operation
4. `fsync` on the directory — makes the rename itself durable

This means a crash mid-write can never leave a half-written chunk visible. Either the old file is there, or the new complete one — never a partial.

**If any replica fails to accept the bytes, the whole write fails here.** We do not proceed to commit.

### Step 4 — Commit to Leader (Client → Leader Only)

Now the client sends a single commit request to the leader only:

```
POST storage-1/chunks/uuid-1/commit
{ "secondaries": ["storage-2", "storage-3"] }
```

The leader then runs the ack protocol internally (see below).

### Step 5 — Register with Naming Server (Client → Naming Server)

After every chunk has been committed, the client sends the full file layout to the naming server to be stored permanently:

```
POST /commit
{
  "file_id": "uuid-A",
  "filename": "notes.txt",
  "size_bytes": 2500,
  "chunks": [ { chunk_id, index, replicas, leader, size_bytes }, ... ]
}
```

The naming server writes this into SQLite atomically (single transaction): first the `files` row, then the `chunks` rows, then the `replicas` rows. If anything fails, the whole transaction rolls back.

**Only after this step does the file exist as far as the system is concerned.**

---

## THE COMMIT ACK PROTOCOL (How consistency is enforced)

This is what happens inside Step 4 — the heart of how the system guarantees durability.

```
Client
  │
  └──► Leader (storage-1)
          │
          │  1. Check: do I have the bytes? (chunk_present)
          │     If NO → refuse immediately (no phantom ack)
          │
          ├──► Secondary (storage-2)  POST /commit-replica  ──┐
          ├──► Secondary (storage-3)  POST /commit-replica  ──┤ parallel fan-out
          │                                                    │
          │◄──── ack (ok=true) or failure ────────────────────┘
          │
          │  2. Count: how many replicas acked (including myself)?
          │     Default policy: ALL 3 must ack (WRITE_MIN_REPLICAS = 0 means "all")
          │
          │  3. If enough acks: return {"ok": true, "acked": [...]}
          │     If not enough:  return HTTP 503 (commit refused)
          ▼
        Client decides if chunk write succeeded
```

**What a secondary does when it gets `/commit-replica`:**
1. Check if it actually has the bytes (from Step 3). If NO → return HTTP 409 (not 200). It refuses to ack data it doesn't hold.
2. If YES → return `{"ok": true}`.

**Retry behavior:** The leader retries each secondary up to 3 times with 200ms back-off before counting it as failed.

**Overall deadline:** The whole fan-out has a 30-second timeout. A hung secondary cannot block the leader forever.

**Write policy:** By default (`WRITE_MIN_REPLICAS=0` = "all"), all 3 replicas must ack. If even one fails, the commit is rejected with HTTP 503. This guarantees every committed chunk exists on all 3 servers, which is what makes reads fault-tolerant.

---

## READ — Step by Step

**Trigger:** User enters a filename and clicks Read.

### Step 1 — Lookup (Client → Naming Server)

```
GET /files/notes.txt

Response:
{
  "file_id": "uuid-A",
  "filename": "notes.txt",
  "size_bytes": 2500,
  "chunks": [
    { "chunk_id": "uuid-1", "index": 0, "replicas": ["storage-1", "storage-2", "storage-3"], "size_bytes": 1024 },
    { "chunk_id": "uuid-2", "index": 1, "replicas": [...], "size_bytes": 1024 },
    { "chunk_id": "uuid-3", "index": 2, "replicas": [...], "size_bytes": 452 }
  ]
}
```

### Step 2 — Fetch Each Chunk (Client → Any Replica)

For each chunk (in index order), the client tries replicas one at a time:

```
Try storage-1 for chunk-0 → success → use it
Try storage-1 for chunk-1 → fails (server down) → try storage-2 → success
Try storage-1 for chunk-2 → success
```

Because all 3 replicas hold the same bytes, the client only needs **1 out of 3** to answer. The system can survive 2 storage servers being down and still serve a read.

Transient failures (network hiccup, 5xx) are retried up to 3 times per replica. Non-transient failures (404, 400) skip straight to the next replica.

### Step 3 — Reassemble

The client concatenates the pieces in index order, trimming each piece to its recorded `size_bytes` (so the last chunk doesn't include padding). The full file is returned as plain text.

---

## DELETE — Step by Step

**Trigger:** User enters a filename and clicks Delete.

### Step 1 — Delete Metadata First (Client → Naming Server)

```
DELETE /files/notes.txt

Response:
{
  "chunk_ids": ["uuid-1", "uuid-2", "uuid-3"],
  "replicas": {
    "uuid-1": ["storage-1", "storage-2", "storage-3"],
    "uuid-2": [...],
    "uuid-3": [...]
  }
}
```

The naming server first reads all chunk/replica info, then deletes the `files` row. SQLite's `ON DELETE CASCADE` automatically removes the `chunks` and `replicas` rows in the same transaction.

**The metadata is gone before the storage bytes are purged.** This means the file becomes immediately invisible to new reads — even if the storage purge is still in progress or partially fails.

### Step 2 — Purge Storage (Client → Each Replica, Best Effort)

```
DELETE storage-1/chunks/uuid-1  ─┐
DELETE storage-2/chunks/uuid-1  ─┤ sequential per replica, per chunk
DELETE storage-3/chunks/uuid-1  ─┘
```

Storage delete is **idempotent** — deleting an already-gone chunk returns ok. This makes retries safe.

If a storage server is unreachable, the client logs the failure and moves on. The file metadata is already gone, so the file is permanently deleted from the user's perspective. The orphaned chunk bytes on the unreachable server are reported back to the caller (e.g. "2 replicas purged, 1 failed"). There is no background garbage collector in this version.

---

## How Consistency is Ensured — Summary

| Concern | How it is handled |
|---|---|
| Half-written chunk on disk | Write-then-fsync-then-rename: the file path is only visible after a complete, synced write |
| Chunk exists on some but not all replicas | Commit is rejected unless all required replicas ack. The file is never registered. |
| File registered before chunks are safely written | Naming server `/commit` is called **last**, after all chunk commits succeed |
| File partially visible during upload | Not possible — the file only appears in the naming server DB after the full commit |
| Naming server restart loses metadata | SQLite DB lives on a named Docker volume; WAL mode survives crashes |
| Naming server loses a commit mid-write | SQLite transaction: all-or-nothing insert of file + chunks + replicas |
| Read sees a chunk that was never committed | Secondaries refuse to ack unless they have the bytes; the write fails if acks are missing |
| Duplicate filename | Naming server enforces a UNIQUE constraint on filename; returns HTTP 409 |

---

## Fault Tolerance At a Glance

```
Can 1 storage server go down?
  - Reads:   YES  (need only 1 of 3)
  - Writes:  NO   (default policy requires all 3 to ack)
             → Can be relaxed: set WRITE_MIN_REPLICAS=2 to allow writes with 1 server down

Can 2 storage servers go down?
  - Reads:   YES  (1 of 3 still answers)
  - Writes:  NO

Can the naming server restart?
  - YES — SQLite + WAL on a named volume; state survives restarts

Can the naming server crash mid-commit?
  - SQLite transaction rolls back; the file is simply not registered.
    Chunk bytes on storage are orphaned (no garbage collection in this version).
```

---

## The Naming Server Database (SQLite)

The naming server is the only place with a database. It has three tables:

```
┌─────────────────────────────────┐
│             files               │
├─────────────┬───────────────────┤
│ file_id     │ TEXT  PRIMARY KEY │  UUID generated at allocate time
│ filename    │ TEXT  UNIQUE      │  enforces no duplicate filenames
│ size_bytes  │ INTEGER           │  total file size
└─────────────┴───────────────────┘
        │
        │ 1 file → many chunks
        ▼
┌─────────────────────────────────┐
│             chunks              │
├─────────────┬───────────────────┤
│ chunk_id    │ TEXT  PRIMARY KEY │  UUID, also the filename on storage disk
│ file_id     │ TEXT  FK → files  │  which file this chunk belongs to
│ idx         │ INTEGER           │  position in the file (0, 1, 2 ...)
│ size_bytes  │ INTEGER           │  exact byte length of this chunk
│ leader      │ TEXT              │  which storage server is the leader
└─────────────┴───────────────────┘
        │
        │ 1 chunk → many replicas (3 rows per chunk)
        ▼
┌─────────────────────────────────┐
│            replicas             │
├─────────────┬───────────────────┤
│ id          │ INTEGER  PK       │  auto-increment
│ chunk_id    │ TEXT  FK → chunks │  which chunk this replica belongs to
│ addr        │ TEXT              │  storage server address e.g. "storage-1:8000"
│ position    │ INTEGER           │  order (0 = leader, 1, 2 = secondaries)
└─────────────┴───────────────────┘
```

**Example — a 2500-byte file with 3 chunks:**

```
files:
  file_id="uuid-A"  filename="notes.txt"  size_bytes=2500

chunks:
  chunk_id="uuid-1"  file_id="uuid-A"  idx=0  size_bytes=1024  leader="storage-1:8000"
  chunk_id="uuid-2"  file_id="uuid-A"  idx=1  size_bytes=1024  leader="storage-2:8000"
  chunk_id="uuid-3"  file_id="uuid-A"  idx=2  size_bytes=452   leader="storage-3:8000"

replicas:
  chunk_id="uuid-1"  addr="storage-1:8000"  position=0   ← leader
  chunk_id="uuid-1"  addr="storage-2:8000"  position=1
  chunk_id="uuid-1"  addr="storage-3:8000"  position=2
  chunk_id="uuid-2"  addr="storage-2:8000"  position=0   ← leader
  chunk_id="uuid-2"  addr="storage-3:8000"  position=1
  chunk_id="uuid-2"  addr="storage-1:8000"  position=2
  chunk_id="uuid-3"  addr="storage-3:8000"  position=0   ← leader
  chunk_id="uuid-3"  addr="storage-1:8000"  position=1
  chunk_id="uuid-3"  addr="storage-2:8000"  position=2
```

**Key design decisions:**

- `ON DELETE CASCADE` — deleting a row from `files` automatically removes all its `chunks` rows, and deleting a `chunks` row removes all its `replicas` rows. One SQL delete cleans up everything.
- `filename UNIQUE` — the database itself enforces no duplicate filenames, returning a 409 if you try.
- The `chunk_id` UUID is also the literal filename on each storage server's disk (`/data/{chunk_id}`). The DB and the filesystem use the same ID.
- Chunk bytes are **never** stored here. The DB only stores addresses — it tells you *where* to find the bytes, not the bytes themselves.
- WAL mode (Write-Ahead Logging) is enabled so the DB can handle a read and a write at the same time without locking.

---

## Sequence Diagram — Write

```
Browser    Client     Naming Server    storage-1    storage-2    storage-3
  │           │              │              │            │            │
  │──upload──►│              │              │            │            │
  │           │──/allocate──►│              │            │            │
  │           │◄─────────────│ (ids+placement, no DB write yet)
  │           │              │              │            │            │
  │           │──PUT chunk───────────────►  │            │            │
  │           │──PUT chunk──────────────────────────►    │            │
  │           │──PUT chunk─────────────────────────────────────────►  │
  │           │              │  (all parallel, all must succeed)
  │           │              │              │            │            │
  │           │──POST commit──────────────► │            │            │
  │           │              │         leader checks disk
  │           │              │         ├──POST commit-replica──────►  │
  │           │              │         ├──POST commit-replica──────────────────► │
  │           │              │         ◄── ack ──────────────────────│
  │           │              │         ◄── ack ──────────────────────────────── │
  │           │◄─────────────────────────── ok (all acked)
  │           │              │              │            │            │
  │           │──/commit─────►│              │            │            │
  │           │              │ (write SQLite — now the file exists)
  │           │◄─────────────│              │            │            │
  │◄─success──│              │              │            │            │
```
