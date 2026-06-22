"""Endpoint-level tests for the storage commit path (owner: Ivan).

Exercises the FastAPI handlers via TestClient. The leader's fan-out to
secondaries is redirected to in-test fakes so no real network is needed.
"""
from __future__ import annotations

import asyncio

from fastapi.testclient import TestClient

from app import main, replication
from app.main import DATA_DIR, app

client = TestClient(app)


def _write_local_chunk(chunk_id: str) -> None:
    (DATA_DIR / chunk_id).write_bytes(b"hello chunk")


# --- commit-replica (secondary side) ---------------------------------------

def test_commit_replica_acks_when_present():
    _write_local_chunk("present-1")
    resp = client.post("/chunks/present-1/commit-replica")
    assert resp.status_code == 200
    assert resp.json()["ok"] is True


def test_commit_replica_declines_when_missing():
    resp = client.post("/chunks/never-pushed/commit-replica")
    assert resp.status_code == 409


# --- commit (leader side) ---------------------------------------------------

def test_leader_commit_success(monkeypatch):
    monkeypatch.setattr(replication, "WRITE_MIN_REPLICAS", 0)

    async def all_ack(secondary, chunk_id):
        return True

    monkeypatch.setattr(replication, "finalize_secondary", all_ack)
    _write_local_chunk("leader-ok")

    resp = client.post(
        "/chunks/leader-ok/commit",
        json={"secondaries": ["storage-2:8000", "storage-3:8000"]},
    )
    assert resp.status_code == 200
    body = resp.json()
    assert body["ok"] is True
    assert set(body["acked"]) == {"storage-1:8000", "storage-2:8000", "storage-3:8000"}


def test_leader_commit_503_when_secondary_down(monkeypatch):
    monkeypatch.setattr(replication, "WRITE_MIN_REPLICAS", 0)  # strict: all required

    async def one_down(secondary, chunk_id):
        return secondary != "storage-3:8000"

    monkeypatch.setattr(replication, "finalize_secondary", one_down)
    _write_local_chunk("leader-degraded")

    resp = client.post(
        "/chunks/leader-degraded/commit",
        json={"secondaries": ["storage-2:8000", "storage-3:8000"]},
    )
    assert resp.status_code == 503
    assert "storage-3:8000" in resp.json()["detail"]


def test_commit_rejects_path_traversal_chunk_id():
    resp = client.post(
        "/chunks/..%2F..%2Fetc%2Fpasswd/commit", json={"secondaries": []}
    )
    assert resp.status_code in (400, 404)  # 404 if the router rejects the path first


def test_leader_commit_503_when_leader_missing_data(monkeypatch):
    async def all_ack(secondary, chunk_id):
        return True

    monkeypatch.setattr(replication, "finalize_secondary", all_ack)
    # chunk 'no-data' is never written locally
    resp = client.post(
        "/chunks/no-data/commit",
        json={"secondaries": ["storage-2:8000"]},
    )
    assert resp.status_code == 503
