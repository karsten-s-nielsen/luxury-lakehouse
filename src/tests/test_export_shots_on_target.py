"""Unit tests for the on-target shot export query.

Guards the D-0 on-target definition (spec 2026-06-20-psxg-tracking-extension):
the PSxG training population must be TRUE on-target
(`shot_outcome IN ('Goal','Saved','Post','Saved to Post')`), NOT the prior
`end_location_z IS NOT NULL` filter which was ~46% off-target (`Off T`).
P-1 verified tracking counts post/bar strikes as on-target, so `Post` /
`Saved to Post` are included for cross-modality parity.
"""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest

from ingestion.export_shots_on_target import _build_query

_ON_TARGET = ("'Goal'", "'Saved'", "'Post'", "'Saved to Post'")
# Off-target / no-z outcomes that the prior `z IS NOT NULL` filter let in.
_EXCLUDED = ("'Off T'", "'Blocked'", "'Wayward'", "'Saved Off Target'")


def test_build_query_restricts_to_true_on_target() -> None:
    sql = _build_query("soccer_analytics", "dev_gold")
    assert "shot_outcome IN" in sql, "on-target population must be filtered by shot_outcome"
    for outcome in _ON_TARGET:
        assert outcome in sql, f"on-target outcome {outcome} must be in the filter"


def test_build_query_excludes_off_target_outcomes() -> None:
    sql = _build_query("soccer_analytics", "dev_gold")
    for outcome in _EXCLUDED:
        assert outcome not in sql, f"off-target outcome {outcome} must NOT be in the population (D-0)"


def test_build_query_retains_z_guard_for_usable_coords() -> None:
    # z must still be non-null (the model needs the height coordinate).
    sql = _build_query("soccer_analytics", "dev_gold")
    assert "end_location_z IS NOT NULL" in sql


def test_main_refuses_a_schema_it_would_not_honour(monkeypatch: pytest.MonkeyPatch) -> None:
    """ADR-073 — a flag we ignore must fail loudly, not substitute silently.

    ``run_pipeline`` reads gold marts from ``DEFAULT_GOLD_SCHEMA`` regardless of
    ``--schema``. Accepting a different value and quietly using dev_gold would be
    the exact silent-substitution class this change exists to remove.

    The guard sits BEFORE the Spark session is built, so this needs no Spark.
    """
    from ingestion.export_shots_on_target import main

    monkeypatch.setattr("sys.argv", ["export_shots_on_target", "--schema", "some_other_schema"])
    with pytest.raises(SystemExit, match="not honoured"):
        main()


def test_main_accepts_the_gold_schema(monkeypatch: pytest.MonkeyPatch) -> None:
    """The guard must not fire on the value it mandates (catches an inverted condition)."""
    import ingestion.export_shots_on_target as mod
    from shared.constants import DEFAULT_GOLD_SCHEMA

    called: dict[str, object] = {}

    def _fake_run_pipeline(spark: object, catalog: str, schema: str, volume_path: str) -> int:
        called["schema"] = schema
        return 0

    monkeypatch.setattr("sys.argv", ["export_shots_on_target", "--schema", DEFAULT_GOLD_SCHEMA])
    monkeypatch.setattr(mod, "run_pipeline", _fake_run_pipeline)
    monkeypatch.setattr("ingestion.bootstrap.bootstrap_hooks", lambda *a, **k: None)
    monkeypatch.setitem(__import__("sys").modules, "pyspark.sql", MagicMock())

    mod.main()
    assert called["schema"] == DEFAULT_GOLD_SCHEMA
