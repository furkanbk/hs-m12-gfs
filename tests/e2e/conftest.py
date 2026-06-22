"""Shared fixtures for the Naranja end-to-end suite (owner: Mikita).

These tests exercise **all five services together** through the only public
surface — the client's HTTP API on the host — exactly as a real user would.

Two ways to run (see tests/e2e/README.md):

  * **Managed (default, what CI uses):** the suite brings the whole stack up with
    ``docker compose up -d --build --wait`` and tears it down afterwards. Set the
    host port with ``NARANJA_CLIENT_PORT`` if 8080 is taken.
  * **Reuse a running stack:** ``NARANJA_MANAGE_STACK=0`` — the suite assumes the
    stack is already up at ``NARANJA_BASE_URL`` and only checks it is reachable.

Env knobs:
  NARANJA_CLIENT_PORT     host port the client is published on (default 8080)
  NARANJA_BASE_URL        full base URL (default http://localhost:<port>)
  NARANJA_COMPOSE_PROJECT compose project name (default "naranja-e2e")
  NARANJA_MANAGE_STACK    "1" (default) bring stack up/down here; "0" reuse
"""
from __future__ import annotations

import os
import subprocess
import time
from pathlib import Path

import httpx
import pytest

REPO_ROOT = Path(__file__).resolve().parents[2]
COMPOSE_FILE = REPO_ROOT / "docker-compose.yml"

CLIENT_PORT = os.environ.get("NARANJA_CLIENT_PORT", "8080")
BASE_URL = os.environ.get("NARANJA_BASE_URL", f"http://localhost:{CLIENT_PORT}")
PROJECT = os.environ.get("NARANJA_COMPOSE_PROJECT", "naranja-e2e")
MANAGE_STACK = os.environ.get("NARANJA_MANAGE_STACK", "1") == "1"

STORAGE_SERVICES = ["storage-1", "storage-2", "storage-3"]
ALL_SERVICES = ["nameserver", *STORAGE_SERVICES, "client"]


def compose(*args: str, check: bool = True, capture: bool = False) -> subprocess.CompletedProcess:
    """Run a ``docker compose`` subcommand against this project/compose file."""
    cmd = [
        "docker", "compose",
        "-f", str(COMPOSE_FILE),
        "-p", PROJECT,
        *args,
    ]
    env = {**os.environ, "NARANJA_CLIENT_PORT": CLIENT_PORT}
    return subprocess.run(
        cmd,
        check=check,
        env=env,
        text=True,
        capture_output=capture,
    )


def wait_for_health(url: str = BASE_URL, timeout: float = 90.0) -> None:
    """Block until the client answers /healthz, or fail the run."""
    deadline = time.monotonic() + timeout
    last = None
    while time.monotonic() < deadline:
        try:
            r = httpx.get(f"{url}/healthz", timeout=3.0)
            if r.status_code == 200 and r.json().get("ok"):
                return
        except Exception as exc:  # noqa: BLE001 — still starting up
            last = exc
        time.sleep(1.0)
    raise RuntimeError(f"client never became healthy at {url} ({last})")


def wait_until_serving(url: str = BASE_URL, timeout: float = 60.0) -> None:
    """Gate the suite on the *whole write path* being live, not just /healthz.

    On a cold start ``/healthz`` can answer before every storage node is ready to
    accept a chunk push/commit, which races the first few tests. We retry a real
    create → read → delete round-trip of a probe file until it fully succeeds.
    """
    deadline = time.monotonic() + timeout
    last = None
    with httpx.Client(base_url=url, timeout=10.0) as c:
        while time.monotonic() < deadline:
            probe = f"e2e-warmup-{int(time.time()*1000)}.txt"
            try:
                created = c.post(
                    "/api/files",
                    files={"file": (probe, b"warmup", "text/plain")},
                )
                if created.status_code == 200:
                    read = c.get(f"/api/files/{probe}")
                    c.delete(f"/api/files/{probe}")
                    if read.status_code == 200 and read.content == b"warmup":
                        return
                last = f"create={created.status_code} {created.text[:120]}"
            except Exception as exc:  # noqa: BLE001 — cluster still warming up
                last = exc
            time.sleep(1.5)
    raise RuntimeError(f"stack never served a full round-trip at {url} ({last})")


@pytest.fixture(scope="session", autouse=True)
def stack():
    """Ensure the 5-service stack is up for the whole session.

    Managed mode brings it up with ``--wait`` (compose blocks on healthchecks)
    and tears it down with its volumes at the end so runs are reproducible.
    """
    if MANAGE_STACK:
        compose("up", "-d", "--build", "--wait")
    wait_for_health()
    wait_until_serving()
    yield
    if MANAGE_STACK:
        compose("down", "-v", check=False)


@pytest.fixture
def base_url() -> str:
    return BASE_URL


@pytest.fixture
def client(base_url):
    with httpx.Client(base_url=base_url, timeout=30.0) as c:
        yield c


@pytest.fixture
def unique_name():
    """A fresh .txt filename per test so tests never collide on the shared
    namespace (filenames are unique in the naming server)."""
    counter = {"n": 0}

    def _make(stem: str = "file") -> str:
        counter["n"] += 1
        # time-based + counter keeps names unique across re-runs without a teardown.
        return f"e2e-{stem}-{int(time.time()*1000)}-{counter['n']}.txt"

    return _make


@pytest.fixture
def restore_storage():
    """Guarantee every storage node is back up after a fault-injection test,
    so a failure mid-test can't poison the rest of the session."""
    yield
    if not MANAGE_STACK:
        return
    compose("start", *STORAGE_SERVICES, check=False)
    # Wait until a full create→read→delete round-trip works again, so a
    # fault-injection test can never leave the cluster degraded for the next one.
    wait_until_serving()
