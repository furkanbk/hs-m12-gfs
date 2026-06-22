"""Test fixtures for the storage commit-path tests.

``app.main`` creates ``DATA_DIR`` at import time, defaulting to ``/data`` (only
writable inside the container). Point it at a temp dir *before* the app module
is imported so the suite runs anywhere.
"""
from __future__ import annotations

import os
import tempfile

# Must run before any `from app import main` — conftest is imported first.
_TMP = tempfile.mkdtemp(prefix="naranja-storage-test-")
os.environ.setdefault("DATA_DIR", _TMP)
os.environ.setdefault("SELF_ADDR", "storage-1:8000")
