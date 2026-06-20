"""Offline tests for the process-shared IdentityCache used by the live search
path. The constructor re-reads and re-parses the whole cache JSON from disk;
_search_core builds one on every live search, so the live path must reuse a
single shared instance per resolved path (mirroring the api.py loader memo).

Run: python test_shared_identity_cache.py
"""

from __future__ import annotations

import json
import tempfile
from pathlib import Path

import fragrance_parser_full_rewrite_fixed as fp


_SEED_URL = "https://www.fragrantica.com/perfume/Acme/Widget-1.html"


def _write_cache(tmpdir: Path) -> Path:
    path = tmpdir / "ident.json"
    key = fp.IdentityTools.cache_key("Acme", "Widget", "")
    path.write_text(json.dumps({key: {"url": _SEED_URL}}), encoding="utf-8")
    return path


def test_same_path_returns_same_instance():
    """Two lookups for the same path return the identical object (one disk
    read), so concurrent/repeat searches share accumulated hits."""
    fp._SHARED_IDENTITY_CACHES.clear()
    with tempfile.TemporaryDirectory() as d:
        path = _write_cache(Path(d))
        a = fp._shared_identity_cache(str(path))
        b = fp._shared_identity_cache(str(path))
        assert a is b, "same path must return the same shared instance"
        # Path normalization: a trailing form resolving to the same path reuses it.
        c = fp._shared_identity_cache(Path(str(path)))
        assert c is a, "equivalent Path/str inputs must resolve to one instance"
    print("PASS same_path_returns_same_instance")


def test_distinct_paths_distinct_instances():
    fp._SHARED_IDENTITY_CACHES.clear()
    with tempfile.TemporaryDirectory() as d:
        p1 = Path(d) / "a.json"
        p2 = Path(d) / "b.json"
        p1.write_text("{}", encoding="utf-8")
        p2.write_text("{}", encoding="utf-8")
        a = fp._shared_identity_cache(str(p1))
        b = fp._shared_identity_cache(str(p2))
        assert a is not b, "distinct paths must not share an instance"
    print("PASS distinct_paths_distinct_instances")


def test_shared_instance_is_a_real_identity_cache():
    """The shared object is a fully-functional IdentityCache (get/put work), so
    the live path's read-through and write-back are unchanged."""
    fp._SHARED_IDENTITY_CACHES.clear()
    with tempfile.TemporaryDirectory() as d:
        path = _write_cache(Path(d))
        cache = fp._shared_identity_cache(str(path))
        assert isinstance(cache, fp.IdentityCache)
        # The seeded row is loaded (proves the file was actually parsed).
        assert cache.get("Acme", "Widget", "") == _SEED_URL
    print("PASS shared_instance_is_a_real_identity_cache")


if __name__ == "__main__":
    test_same_path_returns_same_instance()
    test_distinct_paths_distinct_instances()
    test_shared_instance_is_a_real_identity_cache()
    print("\nALL TESTS PASSED")
