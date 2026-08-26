"""ADR-058 — offline structural guards for the sb360 distributed cogroup path.

The ``cogroup.applyInPandas`` Arrow round-trip CANNOT be unit-tested locally (pyspark is mocked, no
SparkSession — see conftest.py). These are the offline guards bounding the residual risk; the live
float64<->BIGINT schema seam is verified by the serverless probe (plan Task 8 Step 2).
"""

from __future__ import annotations

import inspect

import ingestion.action_context as ac


def _src(*fns) -> str:
    return "\n".join(inspect.getsource(f) for f in fns)


def test_cogroup_passes_explicit_result_schema() -> None:
    """applyInPandas MUST pass schema=_get_result_schema() — the proven-safe mechanism the tracking
    path already uses (test_action_context_createdataframe_schema)."""
    src = inspect.getsource(ac._process_statsbomb_matches)
    assert "applyInPandas(" in src
    assert "schema=_get_result_schema()" in src


def test_cogroup_replace_where_is_incremental_not_full_partition() -> None:
    """CRITICAL: discovery is incremental+capped — a full-partition replace would delete prior
    matches. The write MUST scope to match_id IN (...)."""
    src = inspect.getsource(ac._process_statsbomb_matches)
    assert "match_id IN (" in src
    # the bare full-partition form must NOT be the replace predicate
    assert "replace_where=\"data_source = 'statsbomb'\"" not in src


def test_cogroup_canonicalizes_ids_on_all_sides() -> None:
    """ADR-019: every join side (both _ck + the .isin filter) canonicalizes identically via
    cast('long').cast('string'), matching _find_sb360_new_ids."""
    src = _src(ac._process_statsbomb_matches, ac._canon_key)
    assert 'cast("long").cast("string")' in src
    # 2 .isin filters + 2 _ck withColumns all route through _canon_key
    assert inspect.getsource(ac._process_statsbomb_matches).count("_canon_key(") >= 4


def test_cogroup_empty_match_ids_short_circuits() -> None:
    """Belt-and-suspenders: empty match_ids -> 'match_id IN ()' SQL syntax error, so guard early."""
    src = inspect.getsource(ac._process_statsbomb_matches)
    assert "if not match_ids:" in src


def test_main_statsbomb_has_skip_guard() -> None:
    """The batch entry point owns its own empty-check (statsbomb left the drain skip-guard, ADR-058)."""
    src = inspect.getsource(ac.main_statsbomb)
    assert "if not ids:" in src
    assert "_process_statsbomb_matches" in src


def test_drain_does_not_process_statsbomb() -> None:
    """statsbomb exits the per-match drain — a stray statsbomb unit must fail loud, not silently
    revert to the slow per-match path."""
    import ingestion.drain_adapters as q

    src = inspect.getsource(q.SparkGameProcessor.process)
    assert "_process_statsbomb_match(" not in src  # the per-match CALL is gone (batch handles statsbomb)
    assert "raise" in src  # stray non-tracking provider fails loud
