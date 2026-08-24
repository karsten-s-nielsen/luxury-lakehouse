"""Tests for the query_databricks_sql helper + its chunk-download retry (analytics.databricks_sql_fetch)."""

from __future__ import annotations

import ast
import io
import pathlib
from typing import Any

import pyarrow as pa
import pytest
from requests.exceptions import ChunkedEncodingError

from analytics.databricks_sql_fetch import (
    _CHUNK_MAX_ATTEMPTS,
    _fetch_chunk_tables,
    query_databricks_sql,
)


def test_query_databricks_sql_is_callable() -> None:
    assert callable(query_databricks_sql)


def _arrow_stream_bytes(table: pa.Table) -> bytes:
    """Serialize a table to Arrow IPC stream bytes (what an EXTERNAL_LINKS chunk returns)."""
    sink = io.BytesIO()
    with pa.ipc.new_stream(sink, table.schema) as writer:
        writer.write_table(table)
    return sink.getvalue()


class _Resp:
    def __init__(self, *, json_data: dict[str, Any] | None = None, content: bytes | None = None) -> None:
        self._json = json_data
        self.content = content

    def raise_for_status(self) -> None:
        return None

    def json(self) -> dict[str, Any]:
        assert self._json is not None
        return self._json


def _install_fake_get(monkeypatch: pytest.MonkeyPatch, *, fail_downloads: int, stream: bytes | None) -> dict[str, int]:
    """Patch requests.get: chunk-manifest URLs return one external_link; the download fails
    ``fail_downloads`` times (ChunkedEncodingError) then returns ``stream``. Also no-op sleep.
    Returns a mutable counter of download attempts.
    """
    calls = {"downloads": 0}

    def fake_get(url: str, **_kw: Any) -> _Resp:
        if "/result/chunks/" in url:  # manifest fetch (re-fetched each retry -> fresh presigned link)
            return _Resp(json_data={"external_links": [{"external_link": "https://presigned.example/chunk"}]})
        calls["downloads"] += 1  # the external_link download
        if calls["downloads"] <= fail_downloads:
            raise ChunkedEncodingError("Connection broken: IncompleteRead(21421116 bytes read, 61300 more expected)")
        assert stream is not None
        return _Resp(content=stream)

    monkeypatch.setattr("analytics.databricks_sql_fetch.requests.get", fake_get)
    monkeypatch.setattr("analytics.databricks_sql_fetch.time.sleep", lambda _s: None)
    return calls


def test_fetch_chunk_tables_retries_transient_download_failure(monkeypatch: pytest.MonkeyPatch) -> None:
    """A single transient IncompleteRead is retried (fresh manifest) and the chunk still loads."""
    table = pa.table({"a": [1, 2, 3]})
    calls = _install_fake_get(monkeypatch, fail_downloads=1, stream=_arrow_stream_bytes(table))

    tables = _fetch_chunk_tables("https://host/api/2.0/sql/statements", "stmt-1", 0, {})

    assert len(tables) == 1
    assert tables[0].num_rows == 3
    assert calls["downloads"] == 2  # failed once, retried once, succeeded


def test_fetch_chunk_tables_raises_after_max_attempts(monkeypatch: pytest.MonkeyPatch) -> None:
    """A persistently-truncating chunk fails loud after _CHUNK_MAX_ATTEMPTS, not silently."""
    calls = _install_fake_get(monkeypatch, fail_downloads=_CHUNK_MAX_ATTEMPTS + 5, stream=None)

    with pytest.raises(RuntimeError, match=r"chunk 0 download failed after"):
        _fetch_chunk_tables("https://host/api/2.0/sql/statements", "stmt-1", 0, {})

    assert calls["downloads"] == _CHUNK_MAX_ATTEMPTS  # exactly the attempt budget, then raise


def test_query_databricks_sql_defined_only_in_shared_module() -> None:
    """No script or module may carry its own copy of query_databricks_sql — every consumer
    imports the retry-hardened one from analytics.databricks_sql_fetch (DRY guard).

    The 3 HF-Jobs scripts (xtgk-v2 + the two shot publishers) each carried a private copy with
    NO chunk-download retry; they were consolidated onto the shared helper. This is the anti-drift
    net so a fresh copy-paste cannot silently re-introduce the un-hardened path.
    """
    repo_root = pathlib.Path(__file__).resolve().parents[2]
    canonical = (repo_root / "src" / "analytics" / "databricks_sql_fetch.py").resolve()

    definers: list[pathlib.Path] = []
    candidates = [*(repo_root / "scripts").glob("*.py"), *(repo_root / "src").rglob("*.py")]
    for py in candidates:
        try:
            tree = ast.parse(py.read_text(encoding="utf-8"))
        except (SyntaxError, UnicodeDecodeError):
            continue
        for node in ast.walk(tree):
            if isinstance(node, ast.FunctionDef | ast.AsyncFunctionDef) and node.name == "query_databricks_sql":
                definers.append(py.resolve())
                break

    # Non-vacuity: the scan MUST find the canonical definition, else it proves nothing.
    assert canonical in definers, f"scan did not find the canonical definition at {canonical} — test is vacuous"
    extras = sorted(str(p.relative_to(repo_root)) for p in definers if p != canonical)
    assert not extras, (
        "query_databricks_sql must be imported from analytics.databricks_sql_fetch, "
        f"not redefined. Offending copies: {extras}"
    )
