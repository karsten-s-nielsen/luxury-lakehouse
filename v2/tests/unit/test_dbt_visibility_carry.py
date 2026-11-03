"""S1 Task 11: dbt silver/gold models over encrypted-local bronze CARRY `_visibility`, they do not filter (plan T11).

`tracking_cross_provider` reads the native bronze tracking tables. On an encrypted-local lake those rows carry
`_visibility`, and the model carries it through (it does not drop private rows — filtering is the read seam's job
at `scoped_reads`). So an operator read of the model returns a private match's rows and an anonymous read does not.
This is the encrypted-lake dbt build: the keyed profile attaches the encrypted DuckLake via the ducklake secret.
"""

from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path

import pytest
from lakekit import PLAYERS, StubLease, lake_roots, players

from lakehouse.bronze.writers import LocalNativeWriter, Put
from lakehouse.catalog.working import WorkingCatalog
from lakehouse.config.model import load_defaults
from lakehouse.resolve.links import run_resolution
from lakehouse.runs.dbt import parse_manifest, run_dbt
from lakehouse.runs.history import MatchRow, replace_match
from lakehouse.services.authz import Principal, Role, scoped_reads
from lakehouse.storage.encryption import ENCRYPTION_KEY_ENV
from lakehouse.storage.local import LocalStorage

_KEY = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
_T = datetime(2026, 1, 2, 3, 4, 5, tzinfo=UTC)
_A, _B = 5001, 5002  # A public, B private


def test_tracking_cross_provider_contract_declares_visibility(tmp_path: Path) -> None:
    manifest = parse_manifest(tmp_path / "dbt_parse", caps=load_defaults("laptop").caps)
    node = next(n for n in manifest["nodes"].values() if n["name"] == "tracking_cross_provider")
    assert "_visibility" in node["columns"], "the gold contract must declare the carried _visibility column"


def _match_row(key: int, native: str, visibility: str) -> MatchRow:
    return MatchRow(
        match_key=key,
        provider="skillcorner",
        native_match_id=native,
        visibility=visibility,  # type: ignore[arg-type]
        kept_channel="pining",
        kept_reason="kept",
        competition_id=None,
        competition_name=None,
        season_id=None,
        season_name=None,
        has_360_frames=None,
        run_id="r",
        updated_at=_T,
    )


def test_operator_sees_a_private_tracking_row_and_anon_does_not(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv(ENCRYPTION_KEY_ENV, _KEY)  # dbt's env_var reads os.environ for the keyed profile
    (tmp_path / "lake").mkdir(parents=True, exist_ok=True)
    (tmp_path / "lake-restricted").mkdir(parents=True, exist_ok=True)
    storage = LocalStorage(lake_roots(tmp_path))
    settings = load_defaults("laptop")
    cat = WorkingCatalog.open(
        storage, settings, StubLease(run_id="r"), work_dir=tmp_path / "w", env={ENCRYPTION_KEY_ENV: _KEY}
    )

    writer = LocalNativeWriter(cat, storage)
    for key, native, visibility in ((_A, "a", "public"), (_B, "b", "private")):
        put = Put(PLAYERS, players(key, 2, 0), is_tracking=True, schema=None, source_hash=None)
        row = writer.write_table(
            put,
            match_key=key,
            visibility=visibility,  # type: ignore[arg-type]
            content_version="v1",
            run_id="r",
            out_dir=tmp_path / "o",
            ingested_at=_T,
        )
        match = _match_row(key, native, visibility)
        writer.swap(key, [row], set(), also=lambda m=match: replace_match(cat.conn, m, [], []))
    cat.commit("run_end")

    run_resolution(cat.conn, storage=storage, settings=settings, run_id="r")
    result = run_dbt(cat, run_id="r", caps=settings.caps)
    assert result.success, [r for r in result.results if r.status not in ("success", "pass")]

    # S1-IMPL-09 (B-14): a dbt build on an encrypted lake must leak no key value into any artifact — the rendered
    # profiles.yml, the target/, or the dbt log — because the profile uses an env_var reference, not the literal.
    # Skip files that cannot be read (the attached encrypted catalog is locked on Windows; it holds no cleartext).
    leaks: list[str] = []
    for path in tmp_path.rglob("*"):
        try:
            if path.is_file() and _KEY.encode() in path.read_bytes():
                leaks.append(str(path))
        except OSError:
            continue
    assert leaks == [], f"the encryption key leaked into a dbt artifact: {leaks}"

    found = cat.conn.execute(
        "SELECT match_key, _visibility FROM lake.main.tracking_cross_provider ORDER BY match_key"
    ).fetchall()
    rows = [{"match_key": int(mk), "_visibility": str(vis)} for mk, vis in found]
    assert {r["match_key"] for r in rows} == {_A, _B}, "the model carries the private match's rows (no filter)"
    assert all(r["_visibility"] == "private" for r in rows if r["match_key"] == _B)

    operator = scoped_reads(None, Principal("op", frozenset({Role.OPERATOR})))
    anon = scoped_reads(None, Principal(None, frozenset({Role.ANONYMOUS})))
    assert {r["match_key"] for r in operator.visible(rows)} == {_A, _B}, "an operator sees the private rows"
    assert {r["match_key"] for r in anon.visible(rows)} == {_A}, "an anonymous caller never sees the private rows"
    cat.close()
