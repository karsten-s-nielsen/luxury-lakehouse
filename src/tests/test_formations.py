"""Tests for EFPI formation detection (analytics + pipeline)."""

from __future__ import annotations

import numpy as np
import pandas as pd
import pytest

from analytics.formation_detection import (
    FormationParams,
    FormationResult,
    FormationTemplate,
    _elastic_scale_templates,
    build_formation_templates,
    detect_formation,
    process_group_formations,
    process_match_formations,
    templates_from_serializable,
    templates_to_serializable,
)

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture(scope="module")
def templates() -> dict[int, dict[str, FormationTemplate]]:
    """Load formation templates once for the entire test module."""
    return build_formation_templates()


def _make_442_positions() -> np.ndarray:
    """10 outfield players in a 4-4-2 formation (StatsBomb 120x80).

    Back 4 at x~30, midfield 4 at x~60, forward 2 at x~90.
    Spread across y from 15 to 65.
    """
    return np.array(
        [
            # Back 4
            [30.0, 65.0],  # RB
            [30.0, 50.0],  # RCB
            [30.0, 30.0],  # LCB
            [30.0, 15.0],  # LB
            # Midfield 4
            [60.0, 65.0],  # RM
            [60.0, 50.0],  # RCM
            [60.0, 30.0],  # LCM
            [60.0, 15.0],  # LM
            # Forward 2
            [90.0, 45.0],  # RCF
            [90.0, 35.0],  # LCF
        ],
        dtype=np.float64,
    )


def _make_433_positions() -> np.ndarray:
    """10 outfield players in a 4-3-3 formation (StatsBomb 120x80).

    Back 4 at x~30, midfield 3 at x~55, forward 3 at x~90.
    """
    return np.array(
        [
            # Back 4
            [30.0, 65.0],  # RB
            [30.0, 50.0],  # RCB
            [30.0, 30.0],  # LCB
            [30.0, 15.0],  # LB
            # Midfield 3
            [55.0, 55.0],  # RCM
            [55.0, 40.0],  # CDM/CM
            [55.0, 25.0],  # LCM
            # Forward 3
            [90.0, 60.0],  # RW
            [90.0, 40.0],  # CF
            [90.0, 20.0],  # LW
        ],
        dtype=np.float64,
    )


# ---------------------------------------------------------------------------
# Template loading tests
# ---------------------------------------------------------------------------


class TestBuildFormationTemplates:
    """Tests for formation template loading from mplsoccer."""

    def test_templates_loaded(self, templates: dict[int, dict[str, FormationTemplate]]) -> None:
        """Templates should be non-empty and grouped by player count."""
        assert len(templates) > 0

    def test_10_player_templates_exist(self, templates: dict[int, dict[str, FormationTemplate]]) -> None:
        """Should have templates for 10 outfield players (the standard case)."""
        assert 10 in templates
        assert len(templates[10]) > 0

    def test_10_player_count(self, templates: dict[int, dict[str, FormationTemplate]]) -> None:
        """10-player templates should be the majority (53 expected)."""
        assert len(templates[10]) >= 50

    def test_8_player_templates_exist(self, templates: dict[int, dict[str, FormationTemplate]]) -> None:
        """Should have templates for 8 outfield players (3 red cards)."""
        assert 8 in templates

    def test_9_player_templates_exist(self, templates: dict[int, dict[str, FormationTemplate]]) -> None:
        """Should have templates for 9 outfield players (2 red cards)."""
        assert 9 in templates

    def test_template_coords_shape(self, templates: dict[int, dict[str, FormationTemplate]]) -> None:
        """Each template should have (n, 2) coords matching its group key."""
        for n_players, group in templates.items():
            for name, tmpl in group.items():
                assert tmpl.coords.shape == (n_players, 2), f"Template {name} has wrong shape"
                assert tmpl.labels.shape == (n_players,), f"Template {name} labels wrong shape"

    def test_442_template_exists(self, templates: dict[int, dict[str, FormationTemplate]]) -> None:
        """The 4-4-2 template should be present."""
        assert "442" in templates[10]

    def test_433_template_exists(self, templates: dict[int, dict[str, FormationTemplate]]) -> None:
        """The 4-3-3 template should be present."""
        assert "433" in templates[10]

    def test_statsbomb_coordinate_range(self, templates: dict[int, dict[str, FormationTemplate]]) -> None:
        """Template coordinates should be in StatsBomb 120x80 range."""
        for group in templates.values():
            for name, tmpl in group.items():
                assert np.all(tmpl.coords[:, 0] >= 0) and np.all(tmpl.coords[:, 0] <= 120), (
                    f"Template {name} x out of range"
                )
                assert np.all(tmpl.coords[:, 1] >= 0) and np.all(tmpl.coords[:, 1] <= 80), (
                    f"Template {name} y out of range"
                )

    def test_no_gk_in_templates(self, templates: dict[int, dict[str, FormationTemplate]]) -> None:
        """No template should contain a GK label."""
        for group in templates.values():
            for name, tmpl in group.items():
                assert "GK" not in tmpl.labels, f"Template {name} contains GK"

    def test_singleton_cache(self) -> None:
        """Calling build_formation_templates twice should return the same object."""
        t1 = build_formation_templates()
        t2 = build_formation_templates()
        assert t1 is t2


# ---------------------------------------------------------------------------
# Elastic scaling tests
# ---------------------------------------------------------------------------


class TestElasticScaling:
    """Tests for the joint elastic scaling of templates."""

    def test_identity_scaling(self) -> None:
        """Templates matching observed bounds should remain unchanged."""
        coords = np.array([[0.0, 0.0], [10.0, 10.0]])
        obs_min = np.array([0.0, 0.0])
        obs_max = np.array([10.0, 10.0])
        scaled = _elastic_scale_templates([coords], obs_min, obs_max)
        np.testing.assert_allclose(scaled[0], coords)

    def test_scaling_to_wider_range(self) -> None:
        """Templates should expand to a wider observed range."""
        coords = np.array([[0.0, 0.0], [10.0, 10.0]])
        obs_min = np.array([0.0, 0.0])
        obs_max = np.array([20.0, 20.0])
        scaled = _elastic_scale_templates([coords], obs_min, obs_max)
        np.testing.assert_allclose(scaled[0][0], [0.0, 0.0])
        np.testing.assert_allclose(scaled[0][1], [20.0, 20.0])

    def test_zero_range_guard(self) -> None:
        """Zero-range axis in templates should use scale=1."""
        # All templates at same x
        coords = np.array([[5.0, 0.0], [5.0, 10.0]])
        obs_min = np.array([10.0, 0.0])
        obs_max = np.array([20.0, 10.0])
        scaled = _elastic_scale_templates([coords], obs_min, obs_max)
        # y should scale normally, x should use scale=1
        np.testing.assert_allclose(scaled[0][:, 1], [0.0, 10.0])

    def test_empty_input(self) -> None:
        """Empty template list should return empty result."""
        result = _elastic_scale_templates([], np.array([0.0, 0.0]), np.array([10.0, 10.0]))
        assert result == []

    def test_joint_scaling_multiple_templates(self) -> None:
        """Multiple templates should share the same global scaling."""
        t1 = np.array([[0.0, 0.0], [5.0, 5.0]])
        t2 = np.array([[5.0, 5.0], [10.0, 10.0]])
        obs_min = np.array([0.0, 0.0])
        obs_max = np.array([100.0, 100.0])
        scaled = _elastic_scale_templates([t1, t2], obs_min, obs_max)
        # Global range is 0-10 -> 0-100 (scale = 10)
        np.testing.assert_allclose(scaled[0][0], [0.0, 0.0])
        np.testing.assert_allclose(scaled[0][1], [50.0, 50.0])
        np.testing.assert_allclose(scaled[1][0], [50.0, 50.0])
        np.testing.assert_allclose(scaled[1][1], [100.0, 100.0])


# ---------------------------------------------------------------------------
# Formation detection tests
# ---------------------------------------------------------------------------


class TestDetectFormation:
    """Tests for the EFPI detection algorithm."""

    def test_442_detected(self, templates: dict[int, dict[str, FormationTemplate]]) -> None:
        """A clear 4-4-2 arrangement should be detected as a valid formation."""
        xy = _make_442_positions()
        result = detect_formation(xy, templates)
        assert result is not None
        assert isinstance(result, FormationResult)
        assert result.name in templates[10]
        assert result.cost >= 0

    def test_442_correct_label(self, templates: dict[int, dict[str, FormationTemplate]]) -> None:
        """A clear 4-4-2 arrangement should be detected as '442'."""
        xy = _make_442_positions()
        result = detect_formation(xy, templates)
        assert result is not None
        assert result.name == "442", f"Expected '442', got '{result.name}'"

    def test_433_detected(self, templates: dict[int, dict[str, FormationTemplate]]) -> None:
        """A clear 4-3-3 arrangement should be detected as a valid formation."""
        xy = _make_433_positions()
        result = detect_formation(xy, templates)
        assert result is not None
        assert isinstance(result, FormationResult)
        assert result.name in templates[10]

    def test_433_correct_label(self, templates: dict[int, dict[str, FormationTemplate]]) -> None:
        """A clear 4-3-3 arrangement should be detected as a 4-3-3 variant."""
        xy = _make_433_positions()
        result = detect_formation(xy, templates)
        assert result is not None
        # Elastic scaling + Hungarian assignment can prefer close variants;
        # 4321, 41221, and 433 are within 2 cost units for this test fixture.
        assert result.name in ("433", "4321", "41221"), f"Expected a 4-3-3 variant, got '{result.name}'"

    def test_labels_count_matches_players(self, templates: dict[int, dict[str, FormationTemplate]]) -> None:
        """The number of assigned labels should match the number of input players."""
        xy = _make_442_positions()
        result = detect_formation(xy, templates)
        assert result is not None
        assert len(result.labels) == len(xy)

    def test_labels_are_nonempty(self, templates: dict[int, dict[str, FormationTemplate]]) -> None:
        """All assigned labels should be non-empty strings."""
        xy = _make_442_positions()
        result = detect_formation(xy, templates)
        assert result is not None
        for label in result.labels:
            assert isinstance(label, str)
            assert len(label) > 0

    def test_fewer_than_8_returns_none(self, templates: dict[int, dict[str, FormationTemplate]]) -> None:
        """Fewer than 8 outfield players should return None."""
        xy = np.array([[30.0, 40.0], [50.0, 40.0], [70.0, 40.0]], dtype=np.float64)
        result = detect_formation(xy, templates)
        assert result is None

    def test_exactly_8_players(self, templates: dict[int, dict[str, FormationTemplate]]) -> None:
        """8 outfield players should match 8-player templates."""
        rng = np.random.default_rng(42)
        xy = np.column_stack([rng.uniform(20, 100, 8), rng.uniform(10, 70, 8)])
        result = detect_formation(xy, templates)
        # Should find a match in the 8-player templates
        if 8 in templates:
            assert result is not None
            assert result.name in templates[8]

    def test_exactly_9_players(self, templates: dict[int, dict[str, FormationTemplate]]) -> None:
        """9 outfield players should match 9-player templates."""
        rng = np.random.default_rng(42)
        xy = np.column_stack([rng.uniform(20, 100, 9), rng.uniform(10, 70, 9)])
        result = detect_formation(xy, templates)
        if 9 in templates:
            assert result is not None
            assert result.name in templates[9]

    def test_11_players_returns_none(self, templates: dict[int, dict[str, FormationTemplate]]) -> None:
        """11 outfield players (no matching template count) should return None."""
        rng = np.random.default_rng(42)
        xy = np.column_stack([rng.uniform(20, 100, 11), rng.uniform(10, 70, 11)])
        result = detect_formation(xy, templates)
        # No templates for 11 outfield players (GK excluded = max 10)
        assert result is None

    def test_cost_is_finite(self, templates: dict[int, dict[str, FormationTemplate]]) -> None:
        """The cost should always be finite for valid input."""
        xy = _make_442_positions()
        result = detect_formation(xy, templates)
        assert result is not None
        assert np.isfinite(result.cost)

    def test_none_params_uses_defaults(self, templates: dict[int, dict[str, FormationTemplate]]) -> None:
        """None params should use defaults without error."""
        xy = _make_442_positions()
        result = detect_formation(xy, templates, params=None)
        assert result is not None


# ---------------------------------------------------------------------------
# process_match_formations tests
# ---------------------------------------------------------------------------


class TestProcessMatchFormations:
    """Tests for the match-level batch processing function."""

    def test_output_columns(self) -> None:
        """Output should have the expected column schema."""
        # 10 outfield players, 2 periods, 1 team, 1 frame per second for 600s
        rng = np.random.default_rng(42)
        n_players = 10
        n_frames = 100
        rows = []
        for frame_idx in range(n_frames):
            t = float(frame_idx)
            for pid in range(n_players):
                rows.append(
                    {
                        "period": 1,
                        "team": "home",
                        "player_id": f"p{pid}",
                        "timestamp_seconds": t,
                        "x": 30.0 + pid * 8.0 + rng.normal(0, 1),
                        "y": 15.0 + pid * 6.0 + rng.normal(0, 1),
                    }
                )
        df = pd.DataFrame(rows)
        result = process_match_formations(df, "match_1")

        expected_cols = {"match_id", "period", "team", "window_start_s", "window_end_s", "formation_label", "cost"}
        assert set(result.columns) == expected_cols

    def test_empty_input(self) -> None:
        """Empty tracking DataFrame should return empty result with correct columns."""
        df = pd.DataFrame(columns=pd.Index(["period", "team", "player_id", "timestamp_seconds", "x", "y"]))
        result = process_match_formations(df, "match_1")
        assert len(result) == 0
        expected_cols = {"match_id", "period", "team", "window_start_s", "window_end_s", "formation_label", "cost"}
        assert set(result.columns) == expected_cols

    def test_single_window(self) -> None:
        """Data within one window should produce exactly one result per team/period."""
        rng = np.random.default_rng(42)
        n_players = 10
        rows = []
        # All timestamps within first 300s window
        for frame_idx in range(50):
            t = float(frame_idx * 2)  # 0, 2, 4, ... 98
            for pid in range(n_players):
                rows.append(
                    {
                        "period": 1,
                        "team": "home",
                        "player_id": f"p{pid}",
                        "timestamp_seconds": t,
                        "x": 30.0 + pid * 8.0 + rng.normal(0, 1),
                        "y": 15.0 + pid * 6.0 + rng.normal(0, 1),
                    }
                )
        df = pd.DataFrame(rows)
        result = process_match_formations(df, "match_1")
        # Only 1 period + 1 team + all within 300s = 1 window
        assert len(result) == 1
        assert result.iloc[0]["match_id"] == "match_1"
        assert result.iloc[0]["period"] == 1
        assert result.iloc[0]["team"] == "home"

    def test_two_teams(self) -> None:
        """Two teams should produce separate formation results."""
        rng = np.random.default_rng(42)
        n_players = 10
        rows = []
        for team in ("home", "away"):
            for frame_idx in range(50):
                t = float(frame_idx * 2)
                for pid in range(n_players):
                    rows.append(
                        {
                            "period": 1,
                            "team": team,
                            "player_id": f"{team}_p{pid}",
                            "timestamp_seconds": t,
                            "x": 30.0 + pid * 8.0 + rng.normal(0, 1),
                            "y": 15.0 + pid * 6.0 + rng.normal(0, 1),
                        }
                    )
        df = pd.DataFrame(rows)
        result = process_match_formations(df, "match_1")
        assert len(result) == 2
        teams_in_result = set(result["team"])
        assert teams_in_result == {"home", "away"}

    def test_match_id_propagated(self) -> None:
        """Match ID should be propagated to all output rows."""
        rng = np.random.default_rng(42)
        rows = []
        for frame_idx in range(50):
            t = float(frame_idx * 2)
            for pid in range(10):
                rows.append(
                    {
                        "period": 1,
                        "team": "home",
                        "player_id": f"p{pid}",
                        "timestamp_seconds": t,
                        "x": 30.0 + pid * 8.0 + rng.normal(0, 1),
                        "y": 15.0 + pid * 6.0 + rng.normal(0, 1),
                    }
                )
        df = pd.DataFrame(rows)
        result = process_match_formations(df, "test_match_42")
        assert all(result["match_id"] == "test_match_42")

    def test_fewer_than_min_players_returns_empty(self) -> None:
        """With only 5 players, no formation should be detected."""
        rows = []
        for frame_idx in range(50):
            t = float(frame_idx * 2)
            for pid in range(5):
                rows.append(
                    {
                        "period": 1,
                        "team": "home",
                        "player_id": f"p{pid}",
                        "timestamp_seconds": t,
                        "x": 30.0 + pid * 10.0,
                        "y": 30.0 + pid * 5.0,
                    }
                )
        df = pd.DataFrame(rows)
        result = process_match_formations(df, "match_1")
        assert len(result) == 0


# ---------------------------------------------------------------------------
# FormationParams tests
# ---------------------------------------------------------------------------


class TestFormationParams:
    """Tests for the Pydantic params model."""

    def test_defaults(self) -> None:
        p = FormationParams()
        assert p.window_seconds == 300
        assert p.min_outfield_players == 8

    def test_override(self) -> None:
        p = FormationParams(window_seconds=600, min_outfield_players=9)
        assert p.window_seconds == 600
        assert p.min_outfield_players == 9


# ---------------------------------------------------------------------------
# FormationResult tests
# ---------------------------------------------------------------------------


class TestFormationResult:
    """Tests for the result dataclass."""

    def test_frozen(self) -> None:
        r = FormationResult(name="442", cost=10.5, labels=("RB", "RCB", "LCB", "LB"))
        with pytest.raises(AttributeError):
            r.name = "433"  # type: ignore[misc]

    def test_attributes(self) -> None:
        r = FormationResult(name="433", cost=5.0, labels=("RB",))
        assert r.name == "433"
        assert r.cost == 5.0
        assert r.labels == ("RB",)


# ---------------------------------------------------------------------------
# Pipeline UDF tests
# ---------------------------------------------------------------------------


class TestTemplateSerialization:
    """Tests for template serialization round-trip (driver -> executor)."""

    def test_round_trip(self, templates: dict[int, dict[str, FormationTemplate]]) -> None:
        """Serializing and deserializing should produce identical templates."""
        serialized = templates_to_serializable(templates)
        restored = templates_from_serializable(serialized)

        assert set(restored.keys()) == set(templates.keys())
        for n_players in templates:
            assert set(restored[n_players].keys()) == set(templates[n_players].keys())
            for name in templates[n_players]:
                orig = templates[n_players][name]
                rest = restored[n_players][name]
                np.testing.assert_array_equal(rest.coords, orig.coords)
                np.testing.assert_array_equal(rest.labels, orig.labels)

    def test_serialized_is_pickle_safe(self, templates: dict[int, dict[str, FormationTemplate]]) -> None:
        """Serialized data should contain only plain dicts, numpy arrays, and lists."""
        serialized = templates_to_serializable(templates)

        for n_players, group in serialized.items():
            assert isinstance(n_players, int)
            assert isinstance(group, dict)
            for name, entry in group.items():
                assert isinstance(name, str)
                assert isinstance(entry, dict)
                assert isinstance(entry["coords"], np.ndarray)
                assert isinstance(entry["labels"], list)
                assert all(isinstance(s, str) for s in entry["labels"])

    def test_detect_with_deserialized_templates(self, templates: dict[int, dict[str, FormationTemplate]]) -> None:
        """Detection should work identically with deserialized templates."""
        serialized = templates_to_serializable(templates)
        restored = templates_from_serializable(serialized)

        xy = _make_442_positions()
        result_orig = detect_formation(xy, templates)
        result_restored = detect_formation(xy, restored)

        assert result_orig is not None
        assert result_restored is not None
        assert result_orig.name == result_restored.name
        assert result_orig.cost == result_restored.cost
        assert result_orig.labels == result_restored.labels


class TestProcessGroupFormations:
    """Tests for the single-team single-period processing function."""

    def test_single_group(self, templates: dict[int, dict[str, FormationTemplate]]) -> None:
        """Should detect formation for a single team/period group."""
        rng = np.random.default_rng(42)
        n_players = 10
        rows = []
        for frame_idx in range(50):
            t = float(frame_idx * 2)
            for pid in range(n_players):
                rows.append(
                    {
                        "player_id": f"p{pid}",
                        "timestamp_seconds": t,
                        "x": 30.0 + pid * 8.0 + rng.normal(0, 1),
                        "y": 15.0 + pid * 6.0 + rng.normal(0, 1),
                    }
                )
        df = pd.DataFrame(rows)
        result = process_group_formations(df, "match_1", 1, "home", templates)

        assert len(result) == 1
        assert result.iloc[0]["match_id"] == "match_1"
        assert result.iloc[0]["period"] == 1
        assert result.iloc[0]["team"] == "home"

    def test_empty_input(self, templates: dict[int, dict[str, FormationTemplate]]) -> None:
        """Empty input should return empty result with correct columns."""
        df = pd.DataFrame(columns=pd.Index(["player_id", "timestamp_seconds", "x", "y"]))
        result = process_group_formations(df, "match_1", 1, "home", templates)
        assert len(result) == 0
        expected_cols = {"match_id", "period", "team", "window_start_s", "window_end_s", "formation_label", "cost"}
        assert set(result.columns) == expected_cols


class TestFormationUdf:
    """Tests for the applyInPandas UDF closure."""

    @pytest.fixture()
    def serialized_templates(
        self, templates: dict[int, dict[str, FormationTemplate]]
    ) -> dict[int, dict[str, dict[str, object]]]:
        """Serialize templates once for UDF tests."""
        return templates_to_serializable(templates)

    def test_udf_returns_correct_columns(self, serialized_templates: dict[int, dict[str, dict[str, object]]]) -> None:
        """UDF should return DataFrame with expected columns."""
        from ingestion.formations_common import RESULT_COLUMNS as _RESULT_COLUMNS
        from ingestion.formations_efpi import _make_formation_udf

        rng = np.random.default_rng(42)
        rows = []
        for frame_idx in range(50):
            t = float(frame_idx * 2)
            for pid in range(10):
                rows.append(
                    {
                        "match_id": "m1",
                        "period": 1,
                        "team": "home",
                        "player_id": f"p{pid}",
                        "timestamp_seconds": t,
                        "x": 30.0 + pid * 8.0 + rng.normal(0, 1),
                        "y": 15.0 + pid * 6.0 + rng.normal(0, 1),
                    }
                )
        pdf = pd.DataFrame(rows)

        udf_fn = _make_formation_udf(
            window_seconds=300,
            min_outfield_players=8,
            serialized_templates=serialized_templates,
        )
        result = udf_fn(pdf)

        for col in _RESULT_COLUMNS:
            assert col in result.columns, f"Missing column: {col}"

    def test_udf_empty_input(self, serialized_templates: dict[int, dict[str, dict[str, object]]]) -> None:
        """UDF with empty input should return empty DataFrame with correct columns."""
        from ingestion.formations_common import RESULT_COLUMNS as _RESULT_COLUMNS
        from ingestion.formations_efpi import _make_formation_udf

        pdf = pd.DataFrame(columns=pd.Index(["match_id", "period", "team", "player_id", "timestamp_seconds", "x", "y"]))
        udf_fn = _make_formation_udf(
            window_seconds=300,
            min_outfield_players=8,
            serialized_templates=serialized_templates,
        )
        result = udf_fn(pdf)

        assert len(result) == 0
        for col in _RESULT_COLUMNS:
            assert col in result.columns

    def test_udf_filters_null_players(self, serialized_templates: dict[int, dict[str, dict[str, object]]]) -> None:
        """UDF should filter out rows where player_id or team is null."""
        from ingestion.formations_efpi import _make_formation_udf

        rng = np.random.default_rng(42)
        rows = []
        for frame_idx in range(50):
            t = float(frame_idx * 2)
            for pid in range(10):
                rows.append(
                    {
                        "match_id": "m1",
                        "period": 1,
                        "team": "home",
                        "player_id": f"p{pid}",
                        "timestamp_seconds": t,
                        "x": 30.0 + pid * 8.0 + rng.normal(0, 1),
                        "y": 15.0 + pid * 6.0 + rng.normal(0, 1),
                    }
                )
            # Add a ball row (null player_id)
            rows.append(
                {
                    "match_id": "m1",
                    "period": 1,
                    "team": None,
                    "player_id": None,
                    "timestamp_seconds": t,
                    "x": 60.0,
                    "y": 40.0,
                }
            )
        pdf = pd.DataFrame(rows)

        udf_fn = _make_formation_udf(
            window_seconds=300,
            min_outfield_players=8,
            serialized_templates=serialized_templates,
        )
        result = udf_fn(pdf)

        # Should still detect formations (ball rows filtered out)
        assert len(result) > 0
