"""A run whose environment resolves another encryption key than the process environment stops at its start (audit
T10). The dbt step reads the key from the process environment (an `env_var` reference in its rendered profile), so
such a run would ingest everything and then fail at dbt with "Wrong encryption key". The error names the variable
and never a value; the run takes no lease."""

from __future__ import annotations

from pathlib import Path
from typing import Any

import pytest
from lakekit import LakeKit
from runkit import NOW, save_settings

from lakehouse.config.model import ResourceCaps
from lakehouse.errors import ConfigError
from lakehouse.runs.dbt import DbtResult
from lakehouse.runs.run import RunResult, reparse_matches, run_ingestion
from lakehouse.storage.encryption import ENCRYPTION_KEY_ENV

KEY_A = "a" * 64
KEY_B = "b" * 64


def _dbt(cat: Any, *, run_id: str, caps: ResourceCaps) -> DbtResult:
    return DbtResult(success=True, results=[], run_results_key=f"runs/dbt/{run_id}/run_results.json")


def _ingest(kit: LakeKit, env: dict[str, str], dbt: Any = _dbt) -> RunResult:
    return run_ingestion(
        storage=kit.storage,
        trigger="cli",
        adapters={},
        parsers={},
        now=lambda: NOW,
        work_dir=kit.base / "w",
        env=env,
        dbt=dbt,
    )


def _reparse(kit: LakeKit, env: dict[str, str]) -> RunResult:
    return reparse_matches(
        storage=kit.storage, parsers={}, target="all", now=lambda: NOW, work_dir=kit.base / "w", env=env, dbt=_dbt
    )


@pytest.fixture(autouse=True)
def _saved(kit: LakeKit) -> None:
    save_settings(kit.storage, [])


def _no_lease_taken(base: Path) -> bool:
    return not list((base / "lake" / "runs" / "lease").glob("*.json"))


@pytest.mark.parametrize(("run_env", "process_key"), [(KEY_A, KEY_B), (KEY_A, None), (None, KEY_A)])
@pytest.mark.parametrize("start", [_ingest, _reparse])
def test_a_key_that_dbt_cannot_see_stops_the_run_at_its_start(
    kit: LakeKit, monkeypatch: pytest.MonkeyPatch, run_env: str | None, process_key: str | None, start: Any
) -> None:
    if process_key is None:
        monkeypatch.delenv(ENCRYPTION_KEY_ENV, raising=False)
    else:
        monkeypatch.setenv(ENCRYPTION_KEY_ENV, process_key)
    env = {} if run_env is None else {ENCRYPTION_KEY_ENV: run_env}
    with pytest.raises(ConfigError, match=ENCRYPTION_KEY_ENV) as caught:
        start(kit, env)
    assert KEY_A not in str(caught.value) and KEY_B not in str(caught.value)
    assert _no_lease_taken(kit.base)


def test_the_same_key_runs(kit: LakeKit, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv(ENCRYPTION_KEY_ENV, KEY_A)
    assert _ingest(kit, {ENCRYPTION_KEY_ENV: KEY_A}).status == "ok"


def test_without_dbt_the_process_key_does_not_matter(kit: LakeKit, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv(ENCRYPTION_KEY_ENV, raising=False)
    assert _ingest(kit, {ENCRYPTION_KEY_ENV: KEY_A}, dbt=None).status == "ok"
