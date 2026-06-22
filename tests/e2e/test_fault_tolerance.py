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
    # down the create fails rather than committing an under-replicated chunk.
    compose("stop", "storage-3")
    r = _create(client, unique_name("ft-write"), b"should not commit")
    assert r.status_code >= 400


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
