#!/usr/bin/env python3
"""Guards for readiness gap E3: DATABASE_SSL_CA -> CA-verified TLS.

Fully offline; pytest-style, run via ``python -m pytest``.
"""
from __future__ import annotations

import sys
from contextlib import contextmanager
from pathlib import Path

_REPO_ROOT = Path(__file__).resolve().parent
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

import db

_URL = "postgresql://user:pass@host.example:5432/engine"


def test_no_ca_passes_url_through_unchanged(monkeypatch):
    monkeypatch.delenv("DATABASE_SSL_CA", raising=False)
    assert db._tls_hardened_conninfo(_URL) == _URL


def test_ca_file_path_appends_verify_full(monkeypatch, tmp_path):
    ca = tmp_path / "ca.pem"
    ca.write_text("-----BEGIN CERTIFICATE-----\nabc\n-----END CERTIFICATE-----\n")
    monkeypatch.setenv("DATABASE_SSL_CA", str(ca))
    out = db._tls_hardened_conninfo(_URL)
    assert "sslmode=verify-full" in out
    assert "sslrootcert=" in out
    assert out.startswith(_URL + "?"), "params must extend the URL, not replace it"


def test_verify_ca_mode_is_explicitly_supported(monkeypatch, tmp_path):
    ca = tmp_path / "ca.pem"
    ca.write_text("x")
    monkeypatch.setenv("DATABASE_SSL_CA", str(ca))
    monkeypatch.setenv("DATABASE_SSL_MODE", "verify-ca")
    out = db._tls_hardened_conninfo(_URL)
    assert "sslmode=verify-ca" in out
    assert "sslrootcert=" in out


def test_invalid_ssl_mode_fails_closed(monkeypatch, tmp_path):
    ca = tmp_path / "ca.pem"
    ca.write_text("x")
    monkeypatch.setenv("DATABASE_SSL_CA", str(ca))
    monkeypatch.setenv("DATABASE_SSL_MODE", "require")
    import pytest

    with pytest.raises(ValueError, match="DATABASE_SSL_MODE"):
        db._tls_hardened_conninfo(_URL)


def test_inline_pem_is_materialized_to_a_file(monkeypatch):
    pem = "-----BEGIN CERTIFICATE-----\ninline\n-----END CERTIFICATE-----\n"
    monkeypatch.setenv("DATABASE_SSL_CA", pem)
    out = db._tls_hardened_conninfo(_URL)
    assert "sslmode=verify-full" in out
    marker = "sslrootcert="
    from urllib.parse import unquote

    ca_path = unquote(out.split(marker, 1)[1].split("&")[0])
    # The env value is .strip()ed before writing, so compare modulo outer
    # whitespace — libpq does not care about a trailing newline.
    assert Path(ca_path).read_text(encoding="utf-8") == pem.strip()


def test_existing_query_params_use_ampersand(monkeypatch, tmp_path):
    ca = tmp_path / "ca.pem"
    ca.write_text("x")
    monkeypatch.setenv("DATABASE_SSL_CA", str(ca))
    url = _URL + "?application_name=engine"
    out = db._tls_hardened_conninfo(url)
    assert out.startswith(url + "&sslmode=verify-full")


def test_pool_checks_connections_before_checkout(monkeypatch):
    """A server-closed idle socket must be replaced before reaching a caller."""
    import psycopg_pool

    created = {}
    check_sentinel = object()

    class FakeConnection:
        def execute(self, _query):
            return None

    class FakePool:
        check_connection = check_sentinel

        def __init__(self, conninfo, **kwargs):
            created["conninfo"] = conninfo
            created["kwargs"] = kwargs

        @contextmanager
        def connection(self):
            yield FakeConnection()

        def close(self):
            return None

    monkeypatch.setattr(psycopg_pool, "ConnectionPool", FakePool)
    monkeypatch.setattr(db, "DATABASE_URL", _URL)
    monkeypatch.setattr(db, "ENABLED", True)
    monkeypatch.setattr(db, "_pool", None)
    monkeypatch.delenv("DATABASE_SSL_CA", raising=False)

    db.init_db()

    assert created["conninfo"] == _URL
    assert created["kwargs"]["check"] is check_sentinel
