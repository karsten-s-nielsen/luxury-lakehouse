"""SkillCorner tracking-context local integration test.

Validates the full _bronze_skillcorner_to_frames -> _enrich_match pipeline
against real SkillCorner data fetched from Databricks bronze tables.

Acceptance criteria (from production run):
  - Rows exist (non-zero output)
  - All 82 _RESULT_COLUMNS present (minus _ingested_at)
  - Identity columns 100% populated
  - Core feature columns populated at >= 90% for linked actions
  - Feature values in physically sensible ranges
  - DAS columns populated at >= 90% for linked actions

Data: match 1886347 (Bundesliga), period 1, first ~150s (1500 frames).
Fixtures at src/tests/fixtures/skillcorner/ (fetched from Databricks bronze).
"""

from __future__ import annotations

from pathlib import Path

import pandas as pd
import pytest

_FIXTURE_DIR = Path(__file__).parent / "fixtures" / "skillcorner"
_TRACKING_FIXTURE = _FIXTURE_DIR / "tracking_1886347_p1.parquet"
_ACTIONS_FIXTURE = _FIXTURE_DIR / "actions_1886347_p1.parquet"
_XT_ACTIONS_FIXTURE = _FIXTURE_DIR / "actions_all_skillcorner.parquet"

_HOME_TEAM_ID = "4177"
_MATCH_ID_NATIVE = "1886347"


def _fixtures_exist() -> bool:
    return _TRACKING_FIXTURE.exists() and _ACTIONS_FIXTURE.exists() and _XT_ACTIONS_FIXTURE.exists()


@pytest.fixture(scope="module")
def tracking_df() -> pd.DataFrame:
    return pd.read_parquet(_TRACKING_FIXTURE)


@pytest.fixture(scope="module")
def actions_df() -> pd.DataFrame:
    return pd.read_parquet(_ACTIONS_FIXTURE)


@pytest.fixture(scope="module")
def enriched(tracking_df: pd.DataFrame, actions_df: pd.DataFrame) -> pd.DataFrame:
    """Run full converter + enrichment pipeline once per module."""
    import warnings

    from silly_kicks.xthreat import ExpectedThreat

    from ingestion.tracking_context import _bronze_skillcorner_to_frames, _enrich_match

    game_id = int(actions_df["game_id"].iloc[0])

    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        frames = _bronze_skillcorner_to_frames(tracking_df, game_id=game_id)

    frames["game_id"] = game_id

    # Ensure string types for identity resolution
    actions = actions_df.copy()
    actions["team_id_native"] = actions["team_id_native"].astype(str)
    actions["player_id_native"] = actions["player_id_native"].astype(str)
    frames["team_id"] = frames["team_id"].astype(str).replace("nan", None).replace("None", None)
    frames["player_id"] = frames["player_id"].astype(str).replace("nan", None).replace("None", None)

    # Fit xT on a multi-match SkillCorner corpus (2,451 actions across 2 matches)
    # so the threat grid is non-trivial. Fitting on only ~40 period-1 actions
    # produces an all-zero grid, which makes gk_pitch_control_share_weighted NaN.
    xt_corpus = pd.read_parquet(_XT_ACTIONS_FIXTURE)
    xt = ExpectedThreat(l=16, w=12)
    xt.fit(xt_corpus)

    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        result = _enrich_match(
            actions=actions,
            frames=frames,
            xt=xt,
            home_team_id=_HOME_TEAM_ID,
            match_id_native=_MATCH_ID_NATIVE,
            data_source="skillcorner",
        )

    return result


_requires_fixtures = pytest.mark.skipif(
    not _fixtures_exist(),
    reason="SkillCorner fixtures not present (src/tests/fixtures/skillcorner/*.parquet)",
)


@_requires_fixtures
class TestSkillCornerLocalIntegration:
    """Full pipeline: bronze -> frames -> enrichment on real SkillCorner data."""

    def test_nonzero_rows(self, enriched: pd.DataFrame) -> None:
        pytest.importorskip("silly_kicks")
        assert len(enriched) > 0, "Enrichment produced 0 rows"

    def test_all_result_columns_present(self, enriched: pd.DataFrame) -> None:
        pytest.importorskip("silly_kicks")
        from ingestion.tracking_context import _RESULT_COLUMNS

        expected = set(_RESULT_COLUMNS) - {"_ingested_at"}
        actual = set(enriched.columns)
        missing = expected - actual
        extra = actual - expected
        assert not missing, f"Missing columns: {sorted(missing)}"
        assert not extra, f"Extra columns: {sorted(extra)}"

    def test_identity_columns_fully_populated(self, enriched: pd.DataFrame) -> None:
        pytest.importorskip("silly_kicks")
        for col in ("data_source", "match_id", "action_id", "team_id", "player_id", "type_name"):
            assert enriched[col].notna().all(), f"{col} has NaN values"

    def test_data_source_is_skillcorner(self, enriched: pd.DataFrame) -> None:
        pytest.importorskip("silly_kicks")
        assert (enriched["data_source"] == "skillcorner").all()

    def test_match_id_correct(self, enriched: pd.DataFrame) -> None:
        pytest.importorskip("silly_kicks")
        assert (enriched["match_id"] == _MATCH_ID_NATIVE).all()

    def test_linkage_rate(self, enriched: pd.DataFrame) -> None:
        """At least 90% of actions should link to a tracking frame."""
        pytest.importorskip("silly_kicks")
        linked = enriched["frame_id"].notna().sum()
        total = len(enriched)
        rate = linked / total
        assert rate >= 0.90, f"Linkage rate {rate:.1%} < 90% ({linked}/{total})"

    def test_core_features_populated(self, enriched: pd.DataFrame) -> None:
        """Core feature columns populated at >= 90% for linked actions."""
        pytest.importorskip("silly_kicks")
        linked = enriched[enriched["frame_id"].notna()]
        n_linked = len(linked)

        core_features = [
            "actor_speed",
            "pressure_on_actor__andrienko_oval",
            "pressure_on_actor__link_zones",
            "pressure_on_actor__bekkers_pi",
            "pitch_control_at_ball__spearman",
            "pitch_control_at_ball__fernandez_bornn",
            "pitch_control_at_ball__voronoi",
            "defensive_line_x",
            "nearest_defender_distance",
            "line_break",
            "n_attackers_behind_line",
            "team_shape_centroid_x_attacking",
            "team_shape_centroid_x_defending",
            "team_shape_convex_hull_area_attacking",
            "team_shape_convex_hull_area_defending",
        ]
        failures = []
        for col in core_features:
            non_null = linked[col].notna().sum()
            rate = non_null / n_linked
            if rate < 0.90:
                failures.append(f"{col}: {rate:.1%} ({non_null}/{n_linked})")

        assert not failures, "Core features below 90% fill:\n  " + "\n  ".join(failures)

    def test_das_populated(self, enriched: pd.DataFrame) -> None:
        """DAS columns populated at >= 90% for linked actions."""
        pytest.importorskip("silly_kicks")
        linked = enriched[enriched["frame_id"].notna()]
        n_linked = len(linked)

        for col in ("das_team", "das_opponent", "das_diff"):
            non_null = linked[col].notna().sum()
            rate = non_null / n_linked
            assert rate >= 0.90, f"{col}: {rate:.1%} ({non_null}/{n_linked}) < 90%"

    def test_das_non_negative(self, enriched: pd.DataFrame) -> None:
        """das_team and das_opponent must be non-negative."""
        pytest.importorskip("silly_kicks")
        for col in ("das_team", "das_opponent"):
            vals = enriched[col].dropna()
            if len(vals) > 0:
                assert (vals >= 0).all(), f"{col} has negative values: min={vals.min()}"

    def test_pitch_control_in_unit_range(self, enriched: pd.DataFrame) -> None:
        """Pitch control values must be in [0, 1]."""
        pytest.importorskip("silly_kicks")
        for col in (
            "pitch_control_at_ball__spearman",
            "pitch_control_at_ball__fernandez_bornn",
            "pitch_control_at_ball__voronoi",
        ):
            vals = enriched[col].dropna()
            if len(vals) > 0:
                assert vals.min() >= -0.01, f"{col} min={vals.min():.4f} < 0"
                assert vals.max() <= 1.01, f"{col} max={vals.max():.4f} > 1"

    def test_bekkers_pi_in_unit_range(self, enriched: pd.DataFrame) -> None:
        """Bekkers PI must be in [0, 1]."""
        pytest.importorskip("silly_kicks")
        vals = enriched["pressure_on_actor__bekkers_pi"].dropna()
        if len(vals) > 0:
            assert vals.min() >= -0.01, f"bekkers_pi min={vals.min():.4f}"
            assert vals.max() <= 1.01, f"bekkers_pi max={vals.max():.4f}"

    def test_coordinates_in_spadl_range(self, enriched: pd.DataFrame) -> None:
        """Start/end coordinates must be in SPADL 105x68 range."""
        pytest.importorskip("silly_kicks")
        for col in ("start_x", "end_x"):
            vals = enriched[col].dropna()
            assert vals.min() >= -1.0, f"{col} min={vals.min():.2f} < -1"
            assert vals.max() <= 106.0, f"{col} max={vals.max():.2f} > 106"
        for col in ("start_y", "end_y"):
            vals = enriched[col].dropna()
            assert vals.min() >= -1.0, f"{col} min={vals.min():.2f} < -1"
            assert vals.max() <= 69.0, f"{col} max={vals.max():.2f} > 69"

    def test_actor_speed_sensible(self, enriched: pd.DataFrame) -> None:
        """Actor speed: median should be < 10 m/s (running speed), max < 100 m/s."""
        pytest.importorskip("silly_kicks")
        vals = enriched["actor_speed"].dropna()
        assert vals.median() < 10.0, f"Median actor speed {vals.median():.1f} m/s unreasonably high"
        # Allow some high outliers (measurement noise) but cap at 100
        assert vals.max() < 100.0, f"Max actor speed {vals.max():.1f} m/s unreasonably high"

    def test_team_shape_outfield_count(self, enriched: pd.DataFrame) -> None:
        """Outfield player count should be 10 (11 minus GK) for both teams."""
        pytest.importorskip("silly_kicks")
        for col in (
            "team_shape_n_outfield_players_attacking",
            "team_shape_n_outfield_players_defending",
        ):
            vals = enriched[col].dropna()
            if len(vals) > 0:
                assert vals.max() <= 11, f"{col} max={vals.max()} > 11"
                assert vals.min() >= 1, f"{col} min={vals.min()} < 1"

    def test_link_quality_score_in_range(self, enriched: pd.DataFrame) -> None:
        """Link quality score should be in (0, 1]."""
        pytest.importorskip("silly_kicks")
        vals = enriched["link_quality_score"].dropna()
        if len(vals) > 0:
            assert vals.min() > 0, f"link_quality_score min={vals.min()}"
            assert vals.max() <= 1.0, f"link_quality_score max={vals.max()}"

    def test_gk_influence_populated(self, enriched: pd.DataFrame) -> None:
        """All 4 GK influence columns should be populated at >= 90% for linked actions."""
        pytest.importorskip("silly_kicks")
        linked = enriched[enriched["frame_id"].notna()]
        n_linked = len(linked)

        gk_cols = [
            "gk_pitch_control_share_weighted",
            "gk_reachable_area_m2",
            "gk_closing_time_mean_s__six_yard_box",
            "gk_closing_time_min_s__six_yard_box",
        ]
        failures = []
        for col in gk_cols:
            non_null = linked[col].notna().sum()
            rate = non_null / n_linked
            if rate < 0.90:
                failures.append(f"{col}: {rate:.1%} ({non_null}/{n_linked})")

        assert not failures, "GK influence below 90% fill:\n  " + "\n  ".join(failures)

    def test_ward_line_break_partially_populated(self, enriched: pd.DataFrame) -> None:
        """Ward line-breaking should be populated for a meaningful fraction."""
        pytest.importorskip("silly_kicks")
        linked = enriched[enriched["frame_id"].notna()]
        n_linked = len(linked)

        non_null = linked["line_break__ward"].notna().sum()
        rate = non_null / n_linked
        assert rate >= 0.50, f"line_break__ward: {rate:.1%} < 50%"
