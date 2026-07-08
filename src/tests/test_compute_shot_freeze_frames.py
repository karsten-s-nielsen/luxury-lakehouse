"""Unit tests for the ``compute_shot_freeze_frames`` driver's pure / seam pieces (Task 0.5).

The Spark read/convert/write is deploy-gated (no live job here). Covered offline:
  * ``--match-ids`` parsing;
  * the incremental missing-match discovery SQL (asserted on the query text it builds);
  * the per-period snapshot seam (``_convert_tracking_batch`` -> ``build_tracking_snapshots_spark``);
  * the per-match dispatch loop shape (``_process_match`` mocked; asserts per-unit calls + total).
"""

from __future__ import annotations

import logging

import pandas as pd
import pytest

import ingestion.shot_freeze_frames as sff

# ── --match-ids parsing ────────────────────────────────────────────────────


def test_parse_match_ids_none_and_empty_are_incremental() -> None:
    assert sff._parse_freeze_frame_match_ids_arg(None) is None
    assert sff._parse_freeze_frame_match_ids_arg("") is None
    assert sff._parse_freeze_frame_match_ids_arg("   ") is None


def test_parse_match_ids_multi_id() -> None:
    assert sff._parse_freeze_frame_match_ids_arg("skillcorner:111,222") == ("skillcorner", ["111", "222"])
    # whitespace + trailing comma tolerated
    assert sff._parse_freeze_frame_match_ids_arg("gradientsports: 10517 , ") == ("gradientsports", ["10517"])


def test_parse_match_ids_empty_id_list_is_incremental() -> None:
    assert sff._parse_freeze_frame_match_ids_arg("idsse:") is None


def test_parse_match_ids_rejects_unknown_provider_and_missing_colon() -> None:
    with pytest.raises(SystemExit):
        sff._parse_freeze_frame_match_ids_arg("wyscout:1")  # not a freeze-frame provider (no tracking/360)
    with pytest.raises(SystemExit):
        sff._parse_freeze_frame_match_ids_arg("noprovider")


def test_parse_match_ids_statsbomb_opt_in() -> None:
    # statsbomb (SB-360) is an accepted freeze-frame provider (opt-in backfill), distinct from tracking.
    assert sff._parse_freeze_frame_match_ids_arg("statsbomb:3893790,3895107") == (
        "statsbomb",
        ["3893790", "3895107"],
    )


# ── provider scoping (INTERIM: GS + SkillCorner only) ──────────────────────


def test_default_providers_is_exactly_gs_and_skillcorner() -> None:
    # INTERIM SCOPE: the daily/incremental run must default to GS + SkillCorner only.
    assert sff._parse_providers_arg(None) == frozenset({"gradientsports", "skillcorner"})
    assert sff._parse_providers_arg("") == frozenset({"gradientsports", "skillcorner"})
    assert sff._DEFAULT_PROVIDERS == "gradientsports,skillcorner"
    # statsbomb is a deliberate opt-in (the one-time SB-360 backfill) — NEVER in the daily default.
    assert "statsbomb" not in sff._parse_providers_arg(None)


def test_parse_providers_opt_in_and_reject_unknown() -> None:
    # idsse/metrica are a deliberate opt-in (allowed when explicitly named), unknowns are rejected.
    assert sff._parse_providers_arg("gradientsports,idsse") == frozenset({"gradientsports", "idsse"})
    with pytest.raises(SystemExit):
        sff._parse_providers_arg("gradientsports,wyscout")  # wyscout has no freeze frames


def test_parse_providers_accepts_statsbomb_opt_in() -> None:
    # statsbomb (SB-360) is a valid freeze-frame provider when explicitly named, alongside tracking.
    assert sff._parse_providers_arg("gradientsports,skillcorner,statsbomb") == frozenset(
        {"gradientsports", "skillcorner", "statsbomb"}
    )
    assert sff._parse_providers_arg("statsbomb") == frozenset({"statsbomb"})
    # The freeze-frame provider set is the tracking set PLUS statsbomb.
    assert sff._FREEZE_FRAME_PROVIDERS == sff._TRACKING_PROVIDERS | frozenset({"statsbomb"})


def test_units_from_match_ids_rejects_out_of_scope_provider() -> None:
    selected = frozenset({"gradientsports", "skillcorner"})
    # In-scope backfill builds units.
    assert sff._units_from_match_ids(("skillcorner", ["111", "222"]), selected) == [
        ("skillcorner", "111"),
        ("skillcorner", "222"),
    ]
    # Out-of-scope (idsse) is rejected loudly.
    with pytest.raises(SystemExit):
        sff._units_from_match_ids(("idsse", ["999"]), selected)


# ── incremental discovery SQL ──────────────────────────────────────────────


def test_missing_units_sql_scopes_to_selected_providers() -> None:
    selected = frozenset({"gradientsports", "skillcorner"})  # the interim default scope
    sql = sff._missing_units_sql("soccer_analytics", "dev_gold", selected)
    # Sources + the gold dim (match_key resolution) + the anti-set target.
    assert "soccer_analytics.bronze.spadl_actions" in sql
    assert "soccer_analytics.dev_gold.dim_matches" in sql
    assert "NOT IN (SELECT match_key FROM soccer_analytics.bronze.shot_freeze_frames)" in sql
    # Join uses the REAL column names on each side (2026-07-07 fix): dim_matches carries
    # provider / native_match_id / match_key — NOT data_source / match_id_native.
    assert "sa.data_source = dm.provider" in sql
    assert "sa.match_id_native AS STRING) = CAST(dm.native_match_id AS STRING)" in sql
    assert "dm.data_source" not in sql  # the drift that just bit — must NOT appear on the dim side
    assert "dm.match_id_native" not in sql
    # Shot filter uses type_id (bronze has type_id, NOT type_name).
    assert "sa.type_id = " in sql
    assert "type_name" not in sql
    # Discovery is SCOPED: only the selected providers appear; idsse/metrica are excluded.
    assert "'gradientsports'" in sql
    assert "'skillcorner'" in sql
    assert "'idsse'" not in sql
    assert "'metrica'" not in sql


def test_missing_statsbomb_units_sql_requires_360_and_filters_shots() -> None:
    # statsbomb discovery is a SEPARATE query: it MUST restrict to matches present in statsbomb_360
    # (a non-360 statsbomb match would produce empty frames and waste a work unit) and filter shots.
    sql = sff._missing_statsbomb_units_sql("soccer_analytics", "dev_gold")
    # The 360-existence restriction is the load-bearing difference from the generic tracking query.
    assert "soccer_analytics.bronze.statsbomb_360" in sql
    assert "CAST(match_id AS STRING) FROM soccer_analytics.bronze.statsbomb_360" in sql
    # Scoped to statsbomb + shots only; still resolves match_key and anti-sets already-processed matches.
    assert "sa.data_source = 'statsbomb'" in sql
    assert "sa.type_id = " in sql
    assert "type_name" not in sql
    assert "soccer_analytics.dev_gold.dim_matches" in sql
    assert "NOT IN (SELECT match_key FROM soccer_analytics.bronze.shot_freeze_frames)" in sql
    # Same real dim_matches column names (no drift onto the bronze.spadl_actions names).
    assert "sa.data_source = dm.provider" in sql
    assert "dm.data_source" not in sql
    assert "dm.match_id_native" not in sql


# ── per-period snapshot seam ───────────────────────────────────────────────


def test_period_snapshots_remaps_identity_and_threads_home(monkeypatch: pytest.MonkeyPatch) -> None:
    """Converts frames, applies the frame-compatible identity remap, threads ``home_team_id``.

    The regression this closes: the actions handed to ``build_tracking_snapshots_spark`` must carry
    the FRAME-COMPATIBLE (native) ``team_id`` — not the raw hashed BIGINT — else ``is_teammate`` is
    all-zero (2026-07-07 live finding). The real ``_resolve_enrichment_identity`` runs here.
    """
    import types

    import analytics.action_context.pipeline as ac_pipeline

    fake_frames = pd.DataFrame(
        {"frame_id": [1], "period_id": [1], "player_id": ["a"], "team_id": ["262"], "is_goalkeeper": [False]}
    )
    monkeypatch.setattr(ac_pipeline, "_convert_tracking_batch", lambda provider, trk, actions, meta: fake_frames.copy())

    captured: dict[str, object] = {}

    def _fake_build(
        actions_df: pd.DataFrame, tracking_df: pd.DataFrame, *, home_team_id: str | None = None
    ) -> pd.DataFrame:
        captured["actions"] = actions_df
        captured["frames"] = tracking_df
        captured["home_team_id"] = home_team_id
        return pd.DataFrame({c: [] for c in sff._SHOT_FF_COLUMNS})

    monkeypatch.setattr(sff, "build_tracking_snapshots_spark", _fake_build)

    meta = types.SimpleNamespace(home_team_id="262")
    actions = pd.DataFrame(
        {
            "game_id": [999],
            "action_id": [1],
            "type_id": [0],
            "period_id": [1],
            "team_id": [713441369811427677],  # hashed BIGINT (raw bronze)
            "team_id_native": ["262"],  # frame-compatible native id
            "player_id": [111],
            "player_id_native": ["p1"],
        }
    )
    trk = pd.DataFrame({"period": [1], "frame": [1]})
    sff._period_snapshots("skillcorner", trk, actions, meta, "match123")  # type: ignore[arg-type]

    frames = captured["frames"]
    assert isinstance(frames, pd.DataFrame)
    assert int(frames["game_id"].iloc[0]) == 999  # game_id stamped from actions
    remapped = captured["actions"]
    assert isinstance(remapped, pd.DataFrame)
    # team_id was remapped from the hashed BIGINT to the frame-compatible native id.
    assert str(remapped["team_id"].iloc[0]) == "262"
    assert captured["home_team_id"] == "262"  # frame-compatible home id threaded through


def test_period_snapshots_empty_inputs_return_empty() -> None:
    empty = pd.DataFrame()
    out = sff._period_snapshots("skillcorner", empty, empty, meta=object(), native_id="m")  # type: ignore[arg-type]
    assert list(out.columns) == list(sff._SHOT_FF_COLUMNS)
    assert out.empty


# ── per-match dispatch loop ────────────────────────────────────────────────


def test_process_match_dispatches_statsbomb_to_sb360_path(monkeypatch: pytest.MonkeyPatch) -> None:
    """statsbomb routes to the distinct SB-360 path BEFORE any tracking read (spark unused here)."""
    captured: dict[str, object] = {}

    def _fake_sb(spark, catalog, schema, gold_schema, native_id, task_logger):  # type: ignore[no-untyped-def]
        captured["native_id"] = native_id
        return (42, 7)

    monkeypatch.setattr(sff, "_process_statsbomb_match", _fake_sb)
    out = sff._process_match(
        None,  # type: ignore[arg-type]
        "soccer_analytics",
        "bronze",
        "dev_gold",
        "statsbomb",
        "3893790",
        logging.getLogger("test_sff"),
    )
    assert out == (42, 7)
    assert captured["native_id"] == "3893790"


def test_run_pipeline_processes_each_unit_and_sums_rows(monkeypatch: pytest.MonkeyPatch) -> None:
    calls: list[tuple[str, str]] = []

    def _fake_process(spark, catalog, schema, gold, provider, native_id, task_logger):  # type: ignore[no-untyped-def]
        calls.append((provider, native_id))
        return ({"skillcorner": 501, "gradientsports": 777}[provider], 10)

    monkeypatch.setattr(sff, "_process_match", _fake_process)

    total = sff.run_pipeline(
        spark=None,  # type: ignore[arg-type]
        catalog="soccer_analytics",
        schema="bronze",
        gold_schema="dev_gold",
        units=[("skillcorner", "111"), ("gradientsports", "222")],
        task_logger=logging.getLogger("test_sff"),
    )
    assert calls == [("skillcorner", "111"), ("gradientsports", "222")]
    assert total == 20


# ── shot_fidelity_version resolution (NULL -> default, loud; not a hard-fail) ────────────────────


def test_resolve_shot_fidelity_version_casts_string_and_int() -> None:
    log = logging.getLogger("test_sff")
    assert sff._resolve_shot_fidelity_version("2", "m1", log) == 2
    assert sff._resolve_shot_fidelity_version("1", "m1", log) == 1
    assert sff._resolve_shot_fidelity_version(2, "m1", log) == 2


def test_resolve_shot_fidelity_version_null_defaults_to_one_loudly(caplog: pytest.LogCaptureFixture) -> None:
    # A NULL shot_fidelity_version (StatsBomb metadata gap) must NOT hard-fail the backfill; it defaults
    # to silly_kicks' own non-2 default (1) and logs a VISIBLE warning naming the match (no silent degradation).
    log = logging.getLogger("test_sff_fidelity")
    with caplog.at_level(logging.WARNING, logger="test_sff_fidelity"):
        result = sff._resolve_shot_fidelity_version(None, "3998855", log)
    assert result == sff._DEFAULT_SHOT_FIDELITY_VERSION == 1
    # Visible (not silent) degradation: the default must be logged with the match id.
    assert any("3998855" in rec.getMessage() for rec in caplog.records)
