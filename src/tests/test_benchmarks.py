"""Performance benchmarks for critical-path analytics functions.

Uses pytest-benchmark to measure execution time of the hot-path functions
that run inside ``applyInPandas`` on Databricks serverless executors, where the
1 GB UDF memory cap makes per-call efficiency critical.

Performance budgets (from CLAUDE.md):
    - Batched pitch control: <=5 ms per frame for 22 targets
    - Line-breaking detection: <=2 ms per pass
    - Shape graph construction: <=2 ms for 10 outfield players
    - Shape graph position inference: <=3 ms for 10 outfield players
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

try:
    from analytics.pitch_control import (
        _USE_JAX,
        PitchControlParams,
        compute_pitch_control_at_points,
        compute_pitch_control_grid_fast,
    )

    _HAS_PITCH_CONTROL = True
except ImportError:
    _USE_JAX = False
    _HAS_PITCH_CONTROL = False

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
def pitch_control_params():  # type: ignore[no-untyped-def]
    """Default pitch control parameters."""
    if not _HAS_PITCH_CONTROL:
        pytest.skip("pitch_control not available (JAX not installed)")
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

    @pytest.mark.skipif(not _HAS_PITCH_CONTROL, reason="pitch_control requires JAX")
    def test_bench_batched_pitch_control(
        self,
        benchmark: Any,
        players_df: pd.DataFrame,
        target_points_22: np.ndarray,
        pitch_control_params: Any,
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

    @pytest.mark.skipif(not _HAS_PITCH_CONTROL, reason="pitch_control requires JAX")
    def test_bench_off_ball_xt_frame(
        self,
        benchmark: Any,
        players_df: pd.DataFrame,
        xt_grid: np.ndarray,
        pitch_control_params: Any,
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

    # -- Shape graph formation detection (budget: <=2 ms for 10 players) ---

    def test_bench_shape_graph(self, benchmark: Any) -> None:
        """Shape graph construction: budget <=2ms for 10 outfield players.

        compute_shape_graph runs inside applyInPandas UDF per window per team.
        Typical input: 10 outfield player mean positions from a 5-minute window.
        """
        from analytics.shape_graph import compute_shape_graph

        rng = np.random.default_rng(42)
        # 10 outfield players on a 105x68m pitch
        positions = np.column_stack(
            [
                rng.uniform(0, 105, 10),
                rng.uniform(0, 68, 10),
            ]
        )

        result = benchmark(compute_shape_graph, positions)
        assert result is not None
        assert len(result.edges) > 0

        if benchmark.stats is not None:
            median_seconds: float = benchmark.stats["median"]
            assert median_seconds <= 0.002, f"Shape graph median {median_seconds * 1000:.2f} ms exceeds 2 ms budget"

    def test_bench_infer_positions(self, benchmark: Any) -> None:
        """Position inference: budget <=3ms for 10 outfield players.

        infer_positions runs immediately after compute_shape_graph in the UDF.
        Decomposes positions into 5x5 tactical grid (vertical + horizontal)
        via recursive face-center decomposition along both axes.
        """
        from analytics.shape_graph import compute_shape_graph, infer_positions

        rng = np.random.default_rng(42)
        positions = np.column_stack(
            [
                rng.uniform(0, 105, 10),
                rng.uniform(0, 68, 10),
            ]
        )
        sg = compute_shape_graph(positions)

        result = benchmark(infer_positions, sg, positions, 1.0)
        assert len(result) == 10
        assert all(hasattr(pl, "vertical") and hasattr(pl, "horizontal") for pl in result)

        if benchmark.stats is not None:
            median_seconds: float = benchmark.stats["median"]
            assert median_seconds <= 0.003, f"Infer positions median {median_seconds * 1000:.2f} ms exceeds 3 ms budget"


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


# ---------------------------------------------------------------------------
# ML Training Throughput Benchmarks
# ---------------------------------------------------------------------------
# Measures the hot paths in training pipelines: Dataset.__getitem__ (data
# loading) and model forward pass (GPU compute). These catch regressions
# from accidental per-sample allocation or removed register_buffer.
#
# Performance budgets:
#   - ScoutGPT __getitem__: < 0.1 ms per sample (pre-tensorized index lookup)
#   - ScoutGPT forward: < 10 ms per batch (CPU, batch_size=4, small config)
#   - Football2Vec v2 __getitem__: < 0.1 ms per sample
#   - Football2Vec v2 forward: < 5 ms per batch (CPU, batch_size=4, small config)
#   - Football2Vec 360 __getitem__: < 0.5 ms per sample (includes freeze frame)
#   - Football2Vec 360 forward: < 10 ms per batch (CPU, batch_size=4, small config)
#   - xG v2 forward: < 5 ms per batch (CPU, batch_size=4, small config)
# ---------------------------------------------------------------------------

torch = pytest.importorskip("torch")


def _rand_seq_lens(n: int, lo: int, hi: int, seed: int = 42) -> list[int]:
    """Generate n random sequence lengths in [lo, hi)."""
    g = torch.Generator().manual_seed(seed)
    return [torch.randint(lo, hi, (1,), generator=g).item() for _ in range(n)]


def _rand_int_seqs(lens: list[int], hi: int, seed: int = 42) -> list[list[int]]:
    g = torch.Generator().manual_seed(seed)
    return [torch.randint(0, hi, (sl,), generator=g).tolist() for sl in lens]


def _rand_float_seqs(lens: list[int], seed: int = 42) -> list[list[float]]:
    g = torch.Generator().manual_seed(seed)
    return [torch.rand(sl, generator=g).tolist() for sl in lens]


# --- ScoutGPT benchmarks ---


@pytest.fixture
def scoutgpt_dataset():  # type: ignore[no-untyped-def]
    """Small pre-tensorized ScoutGPT dataset for benchmarking."""
    from analytics.scoutgpt_training import ScoutGPTDataset

    n = 100
    lens = _rand_seq_lens(n, 4, 12, seed=42)
    return ScoutGPTDataset(
        action_types=_rand_int_seqs(lens, 23, seed=1),
        start_xs=_rand_float_seqs(lens, seed=2),
        start_ys=_rand_float_seqs(lens, seed=3),
        end_xs=_rand_float_seqs(lens, seed=4),
        end_ys=_rand_float_seqs(lens, seed=5),
        results=_rand_int_seqs(lens, 2, seed=6),
        vaep_values=_rand_float_seqs(lens, seed=7),
        time_deltas=_rand_float_seqs(lens, seed=8),
        player_idxs=_rand_int_seqs(lens, 50, seed=9),
        max_seq_len=32,
        competition_ids=[0] * n,
    )


def test_bench_scoutgpt_getitem(benchmark: Any, scoutgpt_dataset: Any) -> None:
    """ScoutGPTDataset.__getitem__ must be < 0.1ms (pre-tensorized index lookup)."""
    result = benchmark(scoutgpt_dataset.__getitem__, 0)
    assert "action_ids" in result
    assert result["action_ids"].shape[0] == 32


def test_bench_scoutgpt_forward(benchmark: Any) -> None:
    """ScoutGPT forward pass must be < 10ms on CPU (batch=4, small config)."""
    from analytics.scoutgpt_decoder import ScoutGPTConfig, ScoutGPTDecoder

    cfg = ScoutGPTConfig(hidden_dim=32, num_layers=1, num_heads=4, num_players=50, max_seq_len=32)
    model = ScoutGPTDecoder(cfg)
    model.eval()
    g = torch.Generator().manual_seed(42)
    batch = {
        "action_ids": torch.randint(0, 23, (4, 32), generator=g),
        "start_x": torch.rand(4, 32, generator=g),
        "start_y": torch.rand(4, 32, generator=g),
        "end_x": torch.rand(4, 32, generator=g),
        "end_y": torch.rand(4, 32, generator=g),
        "result": torch.randint(0, 2, (4, 32), generator=g),
        "time_delta": torch.rand(4, 32, generator=g),
        "player_ids": torch.randint(0, 50, (4, 32), generator=g),
        "attention_mask": torch.ones(4, 32, dtype=torch.bool),
    }

    def run() -> tuple[torch.Tensor, torch.Tensor]:
        with torch.no_grad():
            return model.predict(**batch)

    logits, _vaep = benchmark(run)
    assert logits.shape == (4, 32, 23)


# --- Football2Vec v2 benchmarks ---


@pytest.fixture
def f2v_dataset():  # type: ignore[no-untyped-def]
    """Small pre-tensorized Football2Vec v2 dataset for benchmarking."""
    import sys
    from pathlib import Path

    pytest.importorskip("pyarrow")
    scripts_dir = str(Path(__file__).resolve().parents[2] / "scripts")
    if scripts_dir not in sys.path:
        sys.path.insert(0, scripts_dir)
    from train_football2vec_v2_helpers import Football2VecDataset

    n = 100
    lens = _rand_seq_lens(n, 10, 50, seed=50)
    return Football2VecDataset(
        action_ids=_rand_int_seqs(lens, 23, seed=51),
        x_coords=_rand_float_seqs(lens, seed=52),
        y_coords=_rand_float_seqs(lens, seed=53),
        max_seq_len=64,
        mlm=True,
        competition_ids=[0] * n,
    )


def test_bench_f2v_getitem(benchmark: Any, f2v_dataset: Any) -> None:
    """Football2VecDataset.__getitem__ must be < 0.1ms (pre-tensorized + MLM mask)."""
    result = benchmark(f2v_dataset.__getitem__, 0)
    assert "action_ids" in result


def test_bench_f2v_forward(benchmark: Any) -> None:
    """Football2VecEncoder forward pass must be < 5ms on CPU (batch=4, small config)."""
    from analytics.football2vec_transformer import Football2VecConfig, Football2VecEncoder

    cfg = Football2VecConfig(hidden_dim=32, num_layers=1, num_heads=4, max_seq_len=64)
    model = Football2VecEncoder(cfg)
    model.eval()
    g = torch.Generator().manual_seed(42)
    action_ids = torch.randint(0, 23, (4, 50), generator=g)
    x_coords = torch.rand(4, 50, generator=g)
    y_coords = torch.rand(4, 50, generator=g)
    mask = torch.ones(4, 50, dtype=torch.bool)

    def run() -> torch.Tensor:
        with torch.no_grad():
            return model(action_ids, x_coords, y_coords, mask)

    result = benchmark(run)
    assert result.shape == (4, 32)


# --- Football2Vec 360 benchmarks ---


@pytest.fixture
def f2v360_dataset():  # type: ignore[no-untyped-def]
    """Small pre-tensorized Football2Vec 360 dataset for benchmarking."""
    import sys
    from pathlib import Path

    pytest.importorskip("pyarrow")
    scripts_dir = str(Path(__file__).resolve().parents[2] / "scripts")
    if scripts_dir not in sys.path:
        sys.path.insert(0, scripts_dir)
    from train_football2vec_360_helpers import Football2Vec360Dataset

    g = torch.Generator().manual_seed(42)
    n, mp = 50, 22
    lens = _rand_seq_lens(n, 5, 20, seed=60)
    ff = [[[torch.rand(4, generator=g).tolist() for _ in range(mp)] for _ in range(sl)] for sl in lens]
    return Football2Vec360Dataset(
        action_ids=_rand_int_seqs(lens, 23, seed=61),
        x_coords=_rand_float_seqs(lens, seed=62),
        y_coords=_rand_float_seqs(lens, seed=63),
        freeze_frames=ff,
        max_seq_len=32,
        max_players=mp,
        mlm=True,
        competition_ids=[0] * n,
    )


def test_bench_f2v360_getitem(benchmark: Any, f2v360_dataset: Any) -> None:
    """Football2Vec360Dataset.__getitem__ must be < 0.5ms (pre-tensorized + MLM mask)."""
    result = benchmark(f2v360_dataset.__getitem__, 0)
    assert "freeze_frames" in result


def test_bench_f2v360_forward(benchmark: Any) -> None:
    """Football2Vec360Encoder forward pass must be < 10ms on CPU (batch=4, small config)."""
    from analytics.football2vec_360 import Football2Vec360Config, Football2Vec360Encoder

    cfg = Football2Vec360Config(
        hidden_dim=32,
        num_layers=1,
        num_heads=4,
        max_seq_len=32,
        context_dim=8,
        deep_sets_hidden=16,
    )
    model = Football2Vec360Encoder(cfg)
    model.eval()
    g = torch.Generator().manual_seed(42)
    action_ids = torch.randint(0, 23, (4, 20), generator=g)
    x_coords = torch.rand(4, 20, generator=g)
    y_coords = torch.rand(4, 20, generator=g)
    mask = torch.ones(4, 20, dtype=torch.bool)
    context = torch.rand(4, 20, 22, 4, generator=g)

    def run() -> torch.Tensor:
        with torch.no_grad():
            return model(action_ids, x_coords, y_coords, mask, context)

    result = benchmark(run)
    assert result.shape == (4, 32 + 8)  # hidden_dim + context_dim
