"""Performance benchmarks for critical-path analytics functions.

Uses pytest-benchmark to measure execution time of the four hot-path functions
that run inside ``applyInPandas`` on Databricks serverless executors, where the
1 GB UDF memory cap makes per-call efficiency critical.

Performance budgets (from CLAUDE.md):
    - Batched pitch control: <=5 ms per frame for 22 targets
    - Line-breaking detection: <=2 ms per pass
"""

from __future__ import annotations

from typing import Any

import numpy as np
import pandas as pd
import pytest

from analytics.augmentation import PerturbationConfig, perturb_positions
from analytics.defcon_lite import DefconLiteParams, assign_defensive_credits
from analytics.line_breaking import LineBreakingParams, detect_line_breaking
from analytics.obso import compute_obso_surface
from analytics.off_ball_xt import compute_off_ball_xt_frame
from analytics.pitch_control import (
    _USE_JAX,
    PitchControlParams,
    compute_pitch_control_at_points,
    compute_pitch_control_grid_fast,
)

try:
    from analytics.pitch_control_numba import influence_numba, tti_numba

    _USE_NUMBA = True
except ImportError:
    _USE_NUMBA = False

# ---------------------------------------------------------------------------
# Fixtures — shared across all benchmarks
# ---------------------------------------------------------------------------


@pytest.fixture
def players_df() -> pd.DataFrame:
    """Create a realistic 22-player DataFrame for benchmarking."""
    rng = np.random.default_rng(42)
    n_home, n_away = 11, 11
    home = pd.DataFrame(
        {
            "player_id": [f"home_{i}" for i in range(n_home)],
            "team": "home",
            "x": rng.uniform(10, 110, n_home),  # StatsBomb 120x80
            "y": rng.uniform(5, 75, n_home),
            "velocity_x": rng.uniform(-3, 3, n_home),
            "velocity_y": rng.uniform(-3, 3, n_home),
        }
    )
    away = pd.DataFrame(
        {
            "player_id": [f"away_{i}" for i in range(n_away)],
            "team": "away",
            "x": rng.uniform(10, 110, n_away),
            "y": rng.uniform(5, 75, n_away),
            "velocity_x": rng.uniform(-3, 3, n_away),
            "velocity_y": rng.uniform(-3, 3, n_away),
        }
    )
    return pd.concat([home, away], ignore_index=True)


@pytest.fixture
def target_points_22() -> np.ndarray:
    """22 target points matching player positions — one per player on the pitch."""
    rng = np.random.default_rng(99)
    return np.column_stack(
        [
            rng.uniform(10, 110, 22),
            rng.uniform(5, 75, 22),
        ]
    )


@pytest.fixture
def pitch_control_params() -> PitchControlParams:
    """Default pitch control parameters."""
    return PitchControlParams()


@pytest.fixture
def xt_grid() -> np.ndarray:
    """Synthetic 12x8 expected-threat grid (Karun Singh dimensions)."""
    rng = np.random.default_rng(7)
    # Values should increase toward the opponent goal (left-to-right in StatsBomb coords)
    base = np.linspace(0.0, 0.15, 12).reshape(12, 1) * np.ones((1, 8))
    noise = rng.uniform(-0.01, 0.01, (12, 8))
    return base + noise


@pytest.fixture
def defenders_df() -> pd.DataFrame:
    """Six nearby defenders in SPADL 105x68 coordinates for DEFCON benchmarking."""
    rng = np.random.default_rng(42)
    n = 6
    return pd.DataFrame(
        {
            "player_id": list(range(1, n + 1)),
            "team_id": [100] * n,
            "x": rng.uniform(50, 100, n),  # SPADL 105x68
            "y": rng.uniform(10, 58, n),
            "velocity_x": rng.uniform(-2, 2, n),
            "velocity_y": rng.uniform(-2, 2, n),
        }
    )


@pytest.fixture
def defcon_action() -> dict[str, object]:
    """A representative offensive action dict for DEFCON credit assignment."""
    return {
        "event_id": "bench_evt_001",
        "match_id": "3788741",
        "competition_id": "11",
        "season_id": "90",
        "action_player_id": 5503,
        "action_type": "pass",
        "action_x": 65.0,  # SPADL 105x68
        "action_y": 34.0,
        "offensive_value": 0.042,
    }


@pytest.fixture
def opponents_df() -> pd.DataFrame:
    """Ten opponents in StatsBomb 120x80 coords for line-breaking detection."""
    rng = np.random.default_rng(42)
    n = 10
    return pd.DataFrame(
        {
            "x": rng.uniform(40, 110, n),
            "y": rng.uniform(5, 75, n),
        }
    )


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _to_metres(players_df: pd.DataFrame, params: PitchControlParams) -> tuple[np.ndarray, np.ndarray]:
    """Convert StatsBomb DataFrame to metre-space arrays for benchmarking.

    Returns (positions_m, velocities_m) for the home team only.
    """
    from analytics.pitch_control import _col_f64, _sb_to_meters_x, _sb_to_meters_y

    home = pd.DataFrame(players_df[players_df["team"] == "home"])
    pos = np.column_stack(
        [
            _sb_to_meters_x(_col_f64(home, "x"), params),
            _sb_to_meters_y(_col_f64(home, "y"), params),
        ]
    )
    vel = np.column_stack(
        [
            _sb_to_meters_x(_col_f64(home, "velocity_x"), params),
            _sb_to_meters_y(_col_f64(home, "velocity_y"), params),
        ]
    )
    return pos, vel


# ---------------------------------------------------------------------------
# Benchmark tests
# ---------------------------------------------------------------------------


class TestBenchmarks:
    """Performance benchmarks for critical-path analytics functions.

    Each test calls ``benchmark()`` with the function under test and its
    arguments.  pytest-benchmark handles warmup iterations, statistical
    sampling, and report generation.

    Tests that have an explicit performance budget also assert that the
    median execution time stays within the budget.
    """

    # -- Pitch control (budget: <=5 ms for 22 targets) ----------------------

    def test_bench_batched_pitch_control(
        self,
        benchmark: Any,
        players_df: pd.DataFrame,
        target_points_22: np.ndarray,
        pitch_control_params: PitchControlParams,
    ) -> None:
        """Batched pitch control for 22 target points (one per player).

        Budget: <=5 ms per frame for 22 targets.
        """
        result = benchmark(compute_pitch_control_at_points, players_df, target_points_22, pitch_control_params)

        # Sanity: result shape and range
        assert result.shape == (22,)
        assert np.all((result >= 0.0) & (result <= 1.0))

        # Performance budget: median must be <= 5 ms
        # benchmark.stats is None when --benchmark-disable is used
        if benchmark.stats is not None:
            median_seconds: float = benchmark.stats["median"]
            assert median_seconds <= 0.005, (
                f"Batched pitch control median {median_seconds * 1000:.2f} ms exceeds 5 ms budget"
            )

    # -- Off-ball xT (no explicit budget, but depends on pitch control) -----

    def test_bench_off_ball_xt_frame(
        self,
        benchmark: Any,
        players_df: pd.DataFrame,
        xt_grid: np.ndarray,
        pitch_control_params: PitchControlParams,
    ) -> None:
        """Off-ball xT for a single 22-player frame.

        No hard budget, but this wraps ``compute_pitch_control_at_points``
        internally so it should stay close to the pitch-control budget.
        """
        result = benchmark(compute_off_ball_xt_frame, players_df, xt_grid, pitch_control_params)

        # Sanity: one row per player
        assert len(result) == 22
        expected_cols = {"player_id", "team", "x", "y", "xt_value", "pitch_control", "off_ball_xt"}
        assert expected_cols.issubset(set(result.columns))

    # -- DEFCON credit assignment (no explicit budget) ----------------------

    def test_bench_defcon_credit_assignment(
        self,
        benchmark: Any,
        defcon_action: dict[str, object],
        defenders_df: pd.DataFrame,
    ) -> None:
        """DEFCON credit assignment for one action with 6 nearby defenders."""
        result = benchmark(assign_defensive_credits, defcon_action, defenders_df, DefconLiteParams())

        # Sanity: result is a list of credit dicts
        assert isinstance(result, list)
        for credit in result:
            assert "event_id" in credit
            assert "defender_player_id" in credit

    # -- Line-breaking detection (budget: <=2 ms per pass) ------------------

    def test_bench_line_breaking_detection(
        self,
        benchmark: Any,
        opponents_df: pd.DataFrame,
    ) -> None:
        """Line-breaking detection for a single forward pass.

        Budget: <=2 ms per pass.
        """
        result = benchmark(
            detect_line_breaking,
            40.0,  # pass_start_x (StatsBomb 120x80)
            40.0,  # pass_start_y
            85.0,  # pass_end_x — forward pass
            42.0,  # pass_end_y — slight lateral shift
            opponents_df,
            LineBreakingParams(),
        )

        # Sanity: result has expected attributes
        assert hasattr(result, "is_line_breaking")
        assert hasattr(result, "lines_broken")
        assert hasattr(result, "line_breaking_type")

        # Performance budget: median must be <= 2 ms
        # benchmark.stats is None when --benchmark-disable is used
        if benchmark.stats is not None:
            median_seconds: float = benchmark.stats["median"]
            assert median_seconds <= 0.002, (
                f"Line-breaking detection median {median_seconds * 1000:.2f} ms exceeds 2 ms budget"
            )

    # -- Position jitter augmentation (budget: <=5 ms for 10 perturbations) -

    def test_perturb_positions_benchmark(
        self,
        benchmark: Any,
        players_df: pd.DataFrame,
    ) -> None:
        """Position jitter: <=5ms per frame for 10 perturbations (CI-safe)."""
        config = PerturbationConfig(n_perturbations=10)
        rng = np.random.default_rng(42)

        result = benchmark(perturb_positions, players_df, config, rng)

        # Sanity: 10 DataFrames, each with 22 players
        assert len(result) == 10
        for df in result:
            assert len(df) == 22

        # Performance budget: median must be <= 1 ms
        # benchmark.stats is None when --benchmark-disable is used
        if benchmark.stats is not None:
            median_seconds: float = benchmark.stats["median"]
            assert median_seconds <= 0.005, (
                f"Perturb positions median {median_seconds * 1000:.2f} ms exceeds 5 ms budget"
            )

    # -- OBSO surface (budget: <=5 ms for 104x68 grid) ----------------------

    def test_obso_surface_benchmark(
        self,
        benchmark: Any,
    ) -> None:
        """OBSO surface: <=5ms for 104x68 grid.

        Budget: <=5 ms per surface computation.
        """
        rng = np.random.default_rng(42)
        ppcf = rng.uniform(0.0, 1.0, (68, 104))
        transition = rng.uniform(0.0, 1.0, (64, 100))
        epv = rng.uniform(0.0, 0.3, (32, 50))
        grid_x = np.linspace(0, 120, 104)
        grid_y = np.linspace(0, 80, 68)

        result = benchmark(compute_obso_surface, ppcf, transition, epv, (60.0, 40.0), grid_x, grid_y)

        # Sanity: result shape and range
        assert result.shape == (68, 104)
        assert np.all(result >= 0.0)
        assert np.all(result <= 1.0)

        # Performance budget: median must be <= 5 ms
        # benchmark.stats is None when --benchmark-disable is used
        if benchmark.stats is not None:
            median_seconds: float = benchmark.stats["median"]
            assert median_seconds <= 0.005, f"OBSO surface median {median_seconds * 1000:.2f} ms exceeds 5 ms budget"

    # -- Team shape (budget: <=1 ms for 10 outfield players, <=2 ms for 22) -

    def test_bench_team_shape(self, benchmark: Any, players_df: pd.DataFrame) -> None:
        """Team shape computation: budget <=1ms for 10 outfield players."""
        from analytics.team_shape import TeamShapeParams, compute_team_shape

        home_df = players_df[players_df["team"] == "home"].copy()
        params = TeamShapeParams()
        home_x = np.asarray(home_df["x"])
        home_y = np.asarray(home_df["y"])

        result = benchmark(compute_team_shape, home_x, home_y, params)
        assert result is not None
        assert result.convex_hull_area > 0

        # Performance budget: median must be <= 1 ms
        # benchmark.stats is None when --benchmark-disable is used
        if benchmark.stats is not None:
            median_seconds: float = benchmark.stats["median"]
            assert median_seconds <= 0.001, f"Team shape median {median_seconds * 1000:.2f} ms exceeds 1 ms budget"

    def test_bench_team_shape_frame(self, benchmark: Any, players_df: pd.DataFrame) -> None:
        """Both teams shape: budget <=2ms for 22 players."""
        from analytics.team_shape import TeamShapeParams, compute_team_shape_frame

        params = TeamShapeParams()

        result = benchmark(compute_team_shape_frame, players_df, params)
        assert "home" in result
        assert "away" in result

        # Performance budget: median must be <= 2 ms
        # benchmark.stats is None when --benchmark-disable is used
        if benchmark.stats is not None:
            median_seconds: float = benchmark.stats["median"]
            assert median_seconds <= 0.002, (
                f"Team shape frame median {median_seconds * 1000:.2f} ms exceeds 2 ms budget"
            )


class TestJaxBenchmarks:
    """Performance benchmarks for JAX-accelerated pitch control kernels.

    Skipped when JAX is not installed.
    """

    @pytest.mark.skipif(not _USE_JAX, reason="JAX not installed")
    def test_bench_jax_batched_pitch_control(
        self,
        benchmark: Any,
        players_df: pd.DataFrame,
        target_points_22: np.ndarray,
        pitch_control_params: PitchControlParams,
    ) -> None:
        """JAX-accelerated batched pitch control for 22 targets (includes JIT warmup)."""
        # JIT warmup — first call triggers compilation
        compute_pitch_control_at_points(players_df, target_points_22, pitch_control_params)
        result = benchmark(compute_pitch_control_at_points, players_df, target_points_22, pitch_control_params)
        assert result.shape == (22,)
        assert np.all((result >= 0.0) & (result <= 1.0))

    @pytest.mark.skipif(not _USE_JAX, reason="JAX not installed")
    def test_bench_jax_dense_grid(
        self,
        benchmark: Any,
        players_df: pd.DataFrame,
        pitch_control_params: PitchControlParams,
    ) -> None:
        """JAX-accelerated dense grid (104x68 = 7,072 cells) pitch control."""
        # JIT warmup — first call triggers compilation
        compute_pitch_control_grid_fast(players_df, 104, 68, pitch_control_params)
        grid_x, grid_y, surface = benchmark(compute_pitch_control_grid_fast, players_df, 104, 68, pitch_control_params)
        assert grid_x.shape == (104,)
        assert grid_y.shape == (68,)
        assert surface.shape == (68, 104)


@pytest.mark.skipif(not _USE_NUMBA, reason="Numba not installed")
class TestNumbaParity:
    """Verify Numba kernels produce identical results to NumPy."""

    def test_tti_parity(
        self, players_df: pd.DataFrame, target_points_22: np.ndarray, pitch_control_params: PitchControlParams
    ) -> None:
        from analytics.pitch_control import _sb_to_meters_x, _sb_to_meters_y, _tti_numpy

        pos_m, vel_m = _to_metres(players_df, pitch_control_params)
        targets_m = np.column_stack(
            [
                _sb_to_meters_x(target_points_22[:, 0], pitch_control_params),
                _sb_to_meters_y(target_points_22[:, 1], pitch_control_params),
            ]
        )

        numpy_result = _tti_numpy(
            pos_m, vel_m, targets_m, pitch_control_params.reaction_time, pitch_control_params.max_acceleration
        )
        numba_result = tti_numba(
            pos_m, vel_m, targets_m, pitch_control_params.reaction_time, pitch_control_params.max_acceleration
        )

        np.testing.assert_allclose(numba_result, numpy_result, atol=1e-10)

    def test_influence_parity(self) -> None:
        rng = np.random.default_rng(42)
        team_tti = rng.uniform(0.5, 3.0, size=(11, 22))
        opp_min_tti = rng.uniform(0.5, 2.0, size=(22,))
        sigma = 0.45

        from analytics.pitch_control import _influence_numpy

        numpy_result = _influence_numpy(team_tti, opp_min_tti, sigma)
        numba_result = influence_numba(team_tti, opp_min_tti, sigma)

        np.testing.assert_allclose(numba_result, numpy_result, atol=1e-10)


@pytest.mark.skipif(not _USE_NUMBA, reason="Numba not installed")
class TestNumbaBenchmarks:
    """Benchmark Numba JIT vs NumPy for pitch control kernels."""

    def test_bench_numba_pitch_control_warm(
        self,
        benchmark: Any,
        players_df: pd.DataFrame,
        target_points_22: np.ndarray,
        pitch_control_params: PitchControlParams,
    ) -> None:
        """Numba warm benchmark — post-JIT-compile, against 5ms NumPy budget."""
        from analytics.pitch_control import _sb_to_meters_x, _sb_to_meters_y

        pos_m, vel_m = _to_metres(players_df, pitch_control_params)
        targets_m = np.column_stack(
            [
                _sb_to_meters_x(target_points_22[:, 0], pitch_control_params),
                _sb_to_meters_y(target_points_22[:, 1], pitch_control_params),
            ]
        )

        # Warmup: trigger JIT compilation
        tti_numba(pos_m, vel_m, targets_m, pitch_control_params.reaction_time, pitch_control_params.max_acceleration)

        def run() -> np.ndarray:
            return tti_numba(
                pos_m, vel_m, targets_m, pitch_control_params.reaction_time, pitch_control_params.max_acceleration
            )

        result = benchmark(run)
        assert result.shape == (pos_m.shape[0], targets_m.shape[0])
