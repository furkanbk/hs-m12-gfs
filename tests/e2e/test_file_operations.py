"""End-to-end file operations across all 5 services (owner: Mikita).

Every test drives the real client API on the host, which in turn talks to the
real naming server and the 3 real storage servers — a full integration path:
chunk → allocate → push to all replicas → commit to leader → register, and the
read/size/delete flows back out.
"""
from __future__ import annotations


def _create(client, name: str, content: bytes):
    return client.post(
        "/api/files",
        files={"file": (name, content, "text/plain")},
    )


def test_client_healthz(client):
    r = client.get("/healthz")
    assert r.status_code == 200
    assert r.json()["ok"] is True


def test_create_read_roundtrip_multichunk(client, unique_name):
    name = unique_name("multi")
    # ~2.5 chunks at 1024 bytes/chunk → exercises chunk splitting + reassembly.
    content = b"Naranja distributed file system. " * 80
    r = _create(client, name, content)
    assert r.status_code == 200, r.text
    body = r.json()
    assert body["ok"] is True
    assert body["num_chunks"] == (len(content) + 1023) // 1024
    assert set(body["replicas_hit"]) == {
        "storage-1:8000", "storage-2:8000", "storage-3:8000",
    }

    read = client.get(f"/api/files/{name}")
    assert read.status_code == 200
    assert read.content == content  # byte-identical round trip


def test_size_reports_byte_length(client, unique_name):
    name = unique_name("size")
    content = b"x" * 1500
    assert _create(client, name, content).status_code == 200
    r = client.get(f"/api/files/{name}/size")
    assert r.status_code == 200
    assert r.json()["size_bytes"] == 1500


def test_delete_then_read_is_404(client, unique_name):
    name = unique_name("del")
    assert _create(client, name, b"to be deleted").status_code == 200

    d = client.delete(f"/api/files/{name}")
    assert d.status_code == 200
    body = d.json()
    assert body["ok"] is True
    assert body["replicas_purged"] >= 1
    assert body["replicas_failed"] == []

    assert client.get(f"/api/files/{name}").status_code == 404
    assert client.get(f"/api/files/{name}/size").status_code == 404


def test_non_txt_rejected(client):
    r = client.post(
        "/api/files",
        files={"file": ("evil.bin", b"\x00\x01\x02", "application/octet-stream")},
    )
    assert r.status_code == 400


def test_read_unknown_is_404(client):
    assert client.get("/api/files/does-not-exist.txt").status_code == 404


def test_size_unknown_is_404(client):
    assert client.get("/api/files/does-not-exist.txt/size").status_code == 404


def test_duplicate_filename_rejected(client, unique_name):
    name = unique_name("dup")
    assert _create(client, name, b"first").status_code == 200
    # Naming server enforces a UNIQUE filename → the second register fails;
    # the client surfaces it as an upstream error rather than ok.
    second = _create(client, name, b"second")
    assert second.status_code >= 400


def test_exact_chunk_boundary_roundtrip(client, unique_name):
    # Exactly 2 full chunks: the last-chunk-smaller path must NOT trigger here.
    name = unique_name("boundary")
    content = b"A" * 2048
    r = _create(client, name, content)
    assert r.status_code == 200
    assert r.json()["num_chunks"] == 2
    assert client.get(f"/api/files/{name}").content == content


def test_empty_file_roundtrips(client, unique_name):
    name = unique_name("empty")
    r = _create(client, name, b"")
    assert r.status_code == 200
    assert r.json()["size_bytes"] == 0
    read = client.get(f"/api/files/{name}")
    assert read.status_code == 200
    assert read.content == b""
    assert client.get(f"/api/files/{name}/size").json()["size_bytes"] == 0


def test_utf8_multibyte_roundtrip(client, unique_name):
    # size_bytes is the BYTE length, not character count — verify multibyte
    # content reassembles exactly and size matches the encoded length.
    name = unique_name("utf8")
    content = "naranja 🍊 café — ünïcödé ✓\n".encode("utf-8") * 50
    r = _create(client, name, content)
    assert r.status_code == 200
    assert client.get(f"/api/files/{name}").content == content
    assert client.get(f"/api/files/{name}/size").json()["size_bytes"] == len(content)
