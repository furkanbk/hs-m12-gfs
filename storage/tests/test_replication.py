"""Tests for the leader/secondary commit protocol (owner: Ivan).

Run from the ``storage/`` dir:  python -m pytest
"""
from __future__ import annotations

import asyncio
from pathlib import Path

import pytest

from app import replication
from app.replication import (
    CommitOutcome,
    is_valid_chunk_id,
    leader_commit,
    required_replicas,
)


def run(coro):
    return asyncio.run(coro)


# --- chunk_id validation (path-traversal guard) -----------------------------

def test_valid_chunk_ids_accepted():
    assert is_valid_chunk_id("3f2a9c1e-0b6d-4a8e-9f12-abcdef012345")
    assert is_valid_chunk_id("chunk_0")


def test_invalid_chunk_ids_rejected():
    for bad in ("", ".", "..", "../etc/passwd", "a/b", "a\\b", "foo/../bar", "a b"):
        assert not is_valid_chunk_id(bad), bad


# --- policy resolution ------------------------------------------------------

def test_required_replicas_all_by_default(monkeypatch):
    monkeypatch.setattr(replication, "WRITE_MIN_REPLICAS", 0)
    assert required_replicas(3) == 3  # "all replicas"
    assert required_replicas(1) == 1


def test_required_replicas_quorum_is_clamped(monkeypatch):
    monkeypatch.setattr(replication, "WRITE_MIN_REPLICAS", 2)
    assert required_replicas(3) == 2
    # never demand more replicas than exist, never fewer than one
    monkeypatch.setattr(replication, "WRITE_MIN_REPLICAS", 9)
    assert required_replicas(3) == 3
    monkeypatch.setattr(replication, "WRITE_MIN_REPLICAS", -5)
    assert required_replicas(3) == 3


# --- chunk presence ---------------------------------------------------------

def test_chunk_present(tmp_path: Path):
    assert replication.chunk_present(tmp_path, "abc") is False
    (tmp_path / "abc").write_bytes(b"data")
    assert replication.chunk_present(tmp_path, "abc") is True


# --- leader_commit happy path ----------------------------------------------

def _present(tmp_path: Path, chunk_id: str) -> None:
    (tmp_path / chunk_id).write_bytes(b"chunk-bytes")


def test_leader_commit_all_ack(tmp_path, monkeypatch):
    monkeypatch.setattr(replication, "WRITE_MIN_REPLICAS", 0)

    async def fake_finalize(secondary, chunk_id):
        return True

    monkeypatch.setattr(replication, "finalize_secondary", fake_finalize)
    _present(tmp_path, "c1")

    out = run(leader_commit(
        self_addr="storage-1:8000",
        chunk_id="c1",
        secondaries=["storage-2:8000", "storage-3:8000"],
        data_dir=tmp_path,
    ))
    assert isinstance(out, CommitOutcome)
    assert out.ok is True
    assert set(out.acked) == {"storage-1:8000", "storage-2:8000", "storage-3:8000"}
    assert out.failed == ()


def test_leader_commit_strict_fails_when_secondary_down(tmp_path, monkeypatch):
    """Default policy = all replicas: one down secondary fails the whole commit."""
    monkeypatch.setattr(replication, "WRITE_MIN_REPLICAS", 0)

    async def fake_finalize(secondary, chunk_id):
        return secondary != "storage-3:8000"  # storage-3 is down

    monkeypatch.setattr(replication, "finalize_secondary", fake_finalize)
    _present(tmp_path, "c1")

    out = run(leader_commit(
        self_addr="storage-1:8000",
        chunk_id="c1",
        secondaries=["storage-2:8000", "storage-3:8000"],
        data_dir=tmp_path,
    ))
    assert out.ok is False
    assert "storage-3:8000" in out.failed
    assert out.required == 3


def test_leader_commit_quorum_tolerates_one_failure(tmp_path, monkeypatch):
    """With WRITE_MIN_REPLICAS=2, a write survives one down secondary."""
    monkeypatch.setattr(replication, "WRITE_MIN_REPLICAS", 2)

    async def fake_finalize(secondary, chunk_id):
        return secondary != "storage-3:8000"

    monkeypatch.setattr(replication, "finalize_secondary", fake_finalize)
    _present(tmp_path, "c1")

    out = run(leader_commit(
        self_addr="storage-1:8000",
        chunk_id="c1",
        secondaries=["storage-2:8000", "storage-3:8000"],
        data_dir=tmp_path,
    ))
    assert out.ok is True  # leader + storage-2 == 2 >= 2
    assert "storage-3:8000" in out.failed


def test_leader_commit_fails_when_leader_missing_data(tmp_path, monkeypatch):
    monkeypatch.setattr(replication, "WRITE_MIN_REPLICAS", 1)

    async def fake_finalize(secondary, chunk_id):  # would ack, but never called
        return True

    monkeypatch.setattr(replication, "finalize_secondary", fake_finalize)
    # NOTE: chunk file intentionally not created on the leader.

    out = run(leader_commit(
        self_addr="storage-1:8000",
        chunk_id="missing",
        secondaries=["storage-2:8000", "storage-3:8000"],
        data_dir=tmp_path,
    ))
    assert out.ok is False
    assert out.acked == ()
