"""Split raw bytes into fixed-size chunks (1024 bytes by default).

This is part of Berat's client. The chunk size is a locked architectural
decision: exactly 1024 bytes per chunk, last chunk may be smaller.
"""
from __future__ import annotations

import os
from dataclasses import dataclass

CHUNK_SIZE = int(os.environ.get("CHUNK_SIZE", "1024"))


@dataclass
class Chunk:
    """A single chunk of a file, before it has been assigned a chunk_id."""

    index: int          # 0-based position of this chunk within the file
    data: bytes         # the raw bytes (<= CHUNK_SIZE)
    size_bytes: int     # exact byte length, so reassembly is exact


def split(data: bytes, chunk_size: int = CHUNK_SIZE) -> list[Chunk]:
    """Split ``data`` into a list of Chunk objects of ``chunk_size`` bytes.

    The final chunk may be smaller than ``chunk_size``. An empty input still
    produces a single empty chunk so that zero-byte files round-trip cleanly.
    """
    if chunk_size <= 0:
        raise ValueError("chunk_size must be positive")

    if not data:
        return [Chunk(index=0, data=b"", size_bytes=0)]

    chunks: list[Chunk] = []
    for index, start in enumerate(range(0, len(data), chunk_size)):
        piece = data[start:start + chunk_size]
        chunks.append(Chunk(index=index, data=piece, size_bytes=len(piece)))
    return chunks
