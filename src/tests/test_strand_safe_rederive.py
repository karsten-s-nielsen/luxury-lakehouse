"""Static guards for strand-safe re-derive (ADR-043). Filesystem-only — no warehouse."""

from __future__ import annotations

import re
from pathlib import Path

import yaml

from ingestion.rederive_planner import _TABLE_MARTS, D_REPROCESS_MODELS
from ingestion.refresh_synced_tables import SYNCED_TABLES

_REPO = Path(__file__).resolve().parents[2]
_MARTS = _REPO / "dbt_project" / "models" / "marts"
_DBT_PROJECT_YML = _REPO / "dbt_project" / "dbt_project.yml"
_MAT_RE = re.compile(r"materialized\s*=\s*'(\w+)'")
_CDF_RE = re.compile(r"delta\.enableChangeDataFeed['\"]\s*:\s*['\"]true['\"]")


def _triggered_source_tables() -> set[str]:
    return {c.source_table for c in SYNCED_TABLES if c.scheduling_policy == "TRIGGERED"}


def _mart_sql(model: str) -> str:
    return (_MARTS / f"{model}.sql").read_text(encoding="utf-8")


def _materialization(model: str) -> str:
    m = _MAT_RE.search(_mart_sql(model))
    assert m, f"{model}: no materialized=... in config"
    return m.group(1)


def test_dtb_exhaustively_partition_the_triggered_set() -> None:
    triggered = _triggered_source_tables()
    d_set, t_set = set(D_REPROCESS_MODELS), set(_TABLE_MARTS)
    b_set = triggered - d_set - t_set
    # Total + pairwise-disjoint partition (T3): no TRIGGERED mart may be unclassified.
    assert d_set <= triggered, f"D models not in TRIGGERED set: {d_set - triggered}"
    assert t_set <= triggered, f"T models not in TRIGGERED set: {t_set - triggered}"
    assert d_set | t_set | b_set == triggered
    assert d_set & t_set == set() and d_set & b_set == set() and t_set & b_set == set()


def test_t_marts_are_table_and_others_incremental() -> None:
    triggered = _triggered_source_tables()
    for model in _TABLE_MARTS:
        assert _materialization(model) == "table", f"{model} routed to T but is not materialized='table'"
    for model in triggered - set(_TABLE_MARTS):
        assert _materialization(model) == "incremental", f"{model} (D/B) must be incremental"


def test_every_d_mart_is_incremental_and_carries_both_macros() -> None:
    for model in D_REPROCESS_MODELS:
        sql = _mart_sql(model)
        assert _materialization(model) == "incremental", f"{model} must be incremental for the D path"
        assert "reprocess_delete_hook(" in sql, f"{model} missing reprocess_delete_hook pre_hook"
        assert "reprocess_predicate(" in sql, f"{model} missing reprocess_predicate"


def test_registry_var_matches_synced_tables() -> None:
    # triggered_synced_marts is a FLAT list of dbt MODEL names (== source_table).
    raw = yaml.safe_load(_DBT_PROJECT_YML.read_text(encoding="utf-8"))
    registry = set(raw["vars"]["triggered_synced_marts"])
    triggered = _triggered_source_tables()
    assert registry == triggered, (
        f"dbt_project.yml triggered_synced_marts != SYNCED_TABLES TRIGGERED set; "
        f"missing={triggered - registry}, extra={registry - triggered}"
    )


def test_every_triggered_mart_declares_cdf_true() -> None:
    # C2 (live-confirmed all marts carry it): a TRIGGERED synced table requires CDF on the source.
    # m-1: assert the VALUE is 'true', not just that the key string appears (a 'false' would pass otherwise).
    for model in _triggered_source_tables():
        assert _CDF_RE.search(_mart_sql(model)), f"{model} missing `delta.enableChangeDataFeed: 'true'`"


def test_tripwire_is_wired_on_run_start() -> None:
    raw = yaml.safe_load(_DBT_PROJECT_YML.read_text(encoding="utf-8"))
    hooks = raw.get("on-run-start", [])
    assert any("assert_no_triggered_full_refresh" in h for h in hooks), "tripwire not wired in on-run-start"


def test_no_bare_full_refresh_of_triggered_source_in_committed_automation() -> None:
    # P2: include terraform/ — the dbt_full_refresh job parameter is the largest vector. A
    # parameterized `--dbt-full-refresh {{...}}` is NOT flagged (it selects no TRIGGERED mart
    # by NAME and is guarded by the runtime tripwire); only a hardcoded `--full-refresh` that
    # selects a TRIGGERED mart by name is an offender.
    #
    # m-2: this scan is NAME-BASED defense-in-depth only — it will NOT catch a `--full-refresh`
    # whose selection is a TAG (e.g. `--select tag:output_mart --full-refresh`) that happens to
    # include a TRIGGERED mart. That bypass is caught at execution by the runtime on-run-start
    # tripwire, which is the real guard; this static test is the cheap committed-automation net.
    triggered = _triggered_source_tables()
    scan_dirs = [
        _REPO / ".github" / "workflows",
        _REPO / "scripts",
        _REPO / "workflow-cards",
        _REPO / "terraform",
    ]
    offenders: list[str] = []
    for d in scan_dirs:
        if not d.exists():
            continue
        for f in d.rglob("*"):
            if not f.is_file() or f.name == "rederive_synced_marts.py":
                continue
            try:
                text = f.read_text(encoding="utf-8")
            except (UnicodeDecodeError, OSError):
                continue
            if "--full-refresh" not in text:
                continue
            for model in triggered:
                if re.search(rf"--select\s+\S*{re.escape(model)}\b", text) and "--full-refresh" in text:
                    offenders.append(f"{f}: --full-refresh selecting TRIGGERED {model}")
    assert not offenders, "Bare --full-refresh of TRIGGERED source(s) outside the re-derive tool:\n" + "\n".join(
        offenders
    )
