"""End-to-end fault-tolerance tests (owner: Mikita).

These inject real failures into the running cluster (stopping/starting
containers) and assert the guarantees documented in docs/ARCHITECTURE.md §5:

  * reads survive up to 2 of 3 storage servers being down (RF 3);
  * writes are strict — they fail while any storage server is down;
  * naming-server metadata survives a restart (SQLite on a volume = SPOF
    mitigation).

They require the managed stack (they call ``docker compose stop/start``), so
they are skipped when NARANJA_MANAGE_STACK=0.
"""
from __future__ import annotations

import pytest

from conftest import MANAGE_STACK, compose, wait_until_serving

pytestmark = pytest.mark.skipif(
    not MANAGE_STACK,
    reason="fault-injection needs to control containers (NARANJA_MANAGE_STACK=1)",
)


def _create(client, name, content):
    return client.post("/api/files", files={"file": (name, content, "text/plain")})


def test_read_survives_one_storage_down(client, unique_name, restore_storage):
    name = unique_name("ft1")
    content = b"survives one node down. " * 100
    assert _create(client, name, content).status_code == 200

    compose("stop", "storage-3")
    # No wait needed: the client falls back across replicas on its own.
    read = client.get(f"/api/files/{name}")
    assert read.status_code == 200
    assert read.content == content


def test_read_survives_two_storage_down(client, unique_name, restore_storage):
    name = unique_name("ft2")
    content = b"survives two nodes down. " * 100
    assert _create(client, name, content).status_code == 200

    compose("stop", "storage-2", "storage-3")  # only storage-1 left
    read = client.get(f"/api/files/{name}")
    assert read.status_code == 200
    assert read.content == content


def test_write_fails_while_storage_down(client, unique_name, restore_storage):
    # Strict write policy: a write must reach all 3 replicas, so with a node
    # down the create fails in the DATA-PUSH phase (not the commit phase) rather
    # than committing an under-replicated chunk (docs/ARCHITECTURE.md §5.1).
    compose("stop", "storage-3")
    r = _create(client, unique_name("ft-write"), b"should not commit")
    # Assert the specific, attributable failure so an unrelated 4xx can't pass.
    assert r.status_code == 502
    assert "push" in r.json().get("detail", "").lower()


def test_delete_succeeds_with_replica_down(client, unique_name, restore_storage):
    # Delete is best-effort + idempotent: the metadata is dropped (file gone)
    # and reachable replicas are purged, while an unreachable replica is reported
    # in replicas_failed rather than failing the operation (docs/ARCHITECTURE.md §4).
    name = unique_name("ft-del")
    assert _create(client, name, b"delete me while a node is down. " * 30).status_code == 200

    compose("stop", "storage-2")
    d = client.delete(f"/api/files/{name}")
    assert d.status_code == 200
    body = d.json()
    assert body["ok"] is True
    assert len(body["replicas_failed"]) >= 1          # storage-2 could not be purged
    # Metadata is gone regardless, so the file no longer reads.
    assert client.get(f"/api/files/{name}").status_code == 404


def test_chunk_survives_node_restart(client, unique_name, restore_storage):
    # Recoverable failure (docs/ARCHITECTURE.md §5.5): a chunk on all 3 replicas
    # survives a node restart with its volume intact. Prove it by restarting
    # storage-3, then serving the read from storage-3 ALONE (the other two
    # stopped) — only possible if its /data volume persisted across the restart.
    name = unique_name("ft-recover")
    content = b"must survive a storage restart. " * 60
    assert _create(client, name, content).status_code == 200

    compose("restart", "storage-3")
    wait_until_serving()                          # cluster (incl. storage-3) back
    compose("stop", "storage-1", "storage-2")     # force the read onto storage-3
    assert client.get(f"/api/files/{name}").content == content


def test_nameserver_restart_preserves_metadata(client, unique_name):
    # SPOF mitigation: metadata is SQLite on a named volume, so a naming-server
    # restart must come back with the file still known.
    name = unique_name("ft-ns")
    content = b"metadata must survive a naming-server restart. " * 20
    assert _create(client, name, content).status_code == 200

    compose("restart", "nameserver")
    # The client stays "healthy" through the restart; wait until the full path
    # (which needs the naming server) actually serves again before asserting.
    wait_until_serving()

    assert client.get(f"/api/files/{name}/size").json()["size_bytes"] == len(content)
    assert client.get(f"/api/files/{name}").content == content
