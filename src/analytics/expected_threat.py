"""Expected Threat (xT) grid computation via Markov chain value iteration.

Replaces the static 12x8 Karun Singh seed with data-driven transition
probabilities computed from SPADL pass/shot events.

Reference: Karun Singh (2018) "Introducing Expected Threat (xT)"
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Literal

import numpy as np
import pandas as pd

try:
    import jax
    import jax.numpy as jnp

    _USE_JAX = True
except ImportError:
    _USE_JAX = False


CoordSystem = Literal["spadl", "statsbomb"]
"""Supported coordinate systems for xT grids and lookup queries."""


_PITCH_DIMS_BY_SYSTEM: dict[CoordSystem, tuple[float, float]] = {
    "spadl": (105.0, 68.0),
    "statsbomb": (120.0, 80.0),
}
"""Canonical pitch dimensions per supported coordinate system."""


@dataclass(frozen=True)
class ExpectedThreatParams:
    """Configuration for xT grid computation."""

    n_zones_x: int = 12
    n_zones_y: int = 8
    pitch_length: float = 105.0  # SPADL coordinates
    pitch_width: float = 68.0
    max_iterations: int = 100
    tolerance: float = 1e-5


@dataclass(frozen=True)
class XTGrid:
    """Typed xT lookup grid with embedded metadata.

    Owns the (x, y) → xT value mapping for a fixed grid resolution and
    coordinate system. Eliminates primitive obsession (np.ndarray with
    out-of-band dimensional knowledge) by carrying metadata, lookup, and
    structural / differential validation behavior on the type itself.

    Attributes:
        values: 2D float array of shape (n_zones_x, n_zones_y).
        pitch_length: Pitch length in the grid's coordinate system.
        pitch_width: Pitch width in the grid's coordinate system.
        coord_system: Coordinate system this grid was trained on.
        competition_id: Optional source competition identifier ("global"
            for cross-competition aggregate; otherwise the source
            ``competition_id`` or ``None`` for synthetic / test grids).

    Frozen dataclass: instances are immutable at the binding level. The
    inner ``values`` array is technically mutable but should not be
    modified post-construction. Frozen status enables safe Spark
    ``applyInPandas`` closure capture per project conventions.
    """

    values: np.ndarray
    pitch_length: float
    pitch_width: float
    coord_system: CoordSystem
    competition_id: str | None = None

    def __post_init__(self) -> None:
        if self.values.ndim != 2:
            msg = f"XTGrid.values must be 2D, got shape {self.values.shape}"
            raise ValueError(msg)
        if self.coord_system not in _PITCH_DIMS_BY_SYSTEM:
            msg = f"Unknown coord_system: {self.coord_system!r}. Supported: {sorted(_PITCH_DIMS_BY_SYSTEM)}"
            raise ValueError(msg)

    @property
    def n_zones_x(self) -> int:
        return int(self.values.shape[0])

    @property
    def n_zones_y(self) -> int:
        return int(self.values.shape[1])

    @property
    def shape(self) -> tuple[int, int]:
        return (self.n_zones_x, self.n_zones_y)

    def lookup(
        self,
        x: float,
        y: float,
        input_coord_system: CoordSystem,
    ) -> float:
        """Look up xT for a physical position.

        Handles coordinate-system conversion: if input is in a different
        system than the grid, scales proportionally before binning.

        Args:
            x: X coordinate.
            y: Y coordinate.
            input_coord_system: Coordinate system of the input (x, y).

        Returns:
            xT value for the cell containing (x, y), clamped to grid
            bounds. NaN inputs return 0.0.
        """
        if np.isnan(x) or np.isnan(y):
            return 0.0
        if input_coord_system != self.coord_system:
            in_length, in_width = _PITCH_DIMS_BY_SYSTEM[input_coord_system]
            x = x * self.pitch_length / in_length
            y = y * self.pitch_width / in_width
        zone_x = min(int(x / (self.pitch_length / self.n_zones_x)), self.n_zones_x - 1)
        zone_y = min(int(y / (self.pitch_width / self.n_zones_y)), self.n_zones_y - 1)
        zone_x = max(zone_x, 0)
        zone_y = max(zone_y, 0)
        return float(self.values[zone_x, zone_y])

    def validate_structural(
        self,
        *,
        max_value: float | None = None,
        require_directional: bool = False,
        min_attack_ratio: float = 5.0,
        min_rank_corr: float = 0.6,
    ) -> None:
        """Validate grid structure and value characteristics.

        Args:
            max_value: Optional upper bound for grid values. ``None``
                disables the upper-bound check (suitable for ExT v2
                conditional grids where near-box values may approach 1.0).
                The legacy v1 default was 0.50; pass it explicitly to
                preserve old behavior.
            require_directional: When True, assert the grid is materially
                attacking-directional (ADR-063). The caller passes True for
                the global grid (always) and for per-competition grids above
                a min-action threshold; small/noisy grids are exempt. This
                replaces the too-lax per-step ``diff >= -0.01`` monotonicity
                tolerance that let the stale symmetric/U-shaped grid (the
                negative-DZV root cause) pass undetected.
            min_attack_ratio: Minimum attacking-third-mean / defensive-third-
                mean ratio (default 5.0; a correct global grid is ~9.5).
            min_rank_corr: Minimum Spearman rank correlation between zone_x
                and per-zone_x mean (default 0.6) — a coarse shape gate so a
                grid cannot pass the 2-point ratio while dipping mid-pitch.

        Raises:
            ValueError: if the grid violates a structural invariant.
        """
        if self.values.min() < 0.0:
            msg = f"XTGrid contains negative values: min={self.values.min():.4f}"
            raise ValueError(msg)
        if max_value is not None and self.values.max() > max_value:
            msg = f"XTGrid max {self.values.max():.4f} exceeds max_value={max_value}"
            raise ValueError(msg)
        xt_range = self.values.max() - self.values.min()
        if xt_range < 0.05:
            msg = (
                f"XTGrid range too narrow ({xt_range:.4f}) — likely "
                f"coordinate orientation issue or insufficient training data"
            )
            raise ValueError(msg)
        if require_directional:
            self.assert_directional(min_attack_ratio=min_attack_ratio, min_rank_corr=min_rank_corr)

    def assert_directional(self, *, min_attack_ratio: float = 5.0, min_rank_corr: float = 0.6) -> None:
        """Assert the grid rises toward the attacking goal (ADR-063, review M5).

        Two robust checks (vs the fragile single-extreme-column ratio):
        (1) thirds-mean ratio = mean(attacking third) / mean(defensive third) >= min_attack_ratio;
        (2) Spearman rank correlation between zone_x and per-zone_x mean >= min_rank_corr.
        """
        row_means = self.values.mean(axis=1)  # per-zone_x mean; index 0 = own goal, -1 = attacking goal
        n = row_means.shape[0]
        third = max(1, n // 3)
        def_mean = float(row_means[:third].mean())
        att_mean = float(row_means[-third:].mean())
        ratio = att_mean / def_mean if def_mean > 0 else float("inf")
        if ratio < min_attack_ratio:
            msg = (
                f"XTGrid is not attacking-directional: attacking-third / defensive-third mean ratio "
                f"{ratio:.2f} < {min_attack_ratio} (defensive third {def_mean:.5f}, attacking third "
                f"{att_mean:.5f}). Likely a STALE grid built on pre-LTR data or an orientation regression "
                f"(ADR-063)."
            )
            raise ValueError(msg)
        # Spearman rank corr = Pearson on the ranks of row_means vs the (already sorted) zone indices.
        order = np.argsort(row_means, kind="stable")
        ranks = np.empty(n, dtype=np.float64)
        ranks[order] = np.arange(n, dtype=np.float64)
        zone_idx = np.arange(n, dtype=np.float64)
        rank_corr = float(np.corrcoef(ranks, zone_idx)[0, 1]) if n > 1 else 1.0
        if rank_corr < min_rank_corr:
            msg = (
                f"XTGrid shape is not monotone attacking-directional: zone_x↔xT rank correlation "
                f"{rank_corr:.2f} < {min_rank_corr} (dips/spikes mid-pitch). Likely an orientation "
                f"regression (ADR-063)."
            )
            raise ValueError(msg)

    def validate_differential(
        self,
        previous: XTGrid | None,
        *,
        max_relative_change: float = 0.30,
    ) -> None:
        """Validate this grid is not a dramatic departure from a baseline.

        Catches regressions: training-data corruption, convergence failure,
        coordinate flip, model bug. Compares peak value against the
        previous grid; rejects if relative change exceeds threshold.

        Args:
            previous: Previous run's grid. ``None`` on first run (skip).
            max_relative_change: Maximum allowed
                ``|new_max - prev_max| / prev_max``. Default 0.30 (30%).

        Raises:
            ValueError: if the differential exceeds the threshold.
        """
        if previous is None:
            return
        prev_max = float(previous.values.max())
        new_max = float(self.values.max())
        if prev_max <= 0.0:
            return
        relative = abs(new_max - prev_max) / prev_max
        if relative > max_relative_change:
            msg = (
                f"XTGrid max changed by {relative:.1%} "
                f"({prev_max:.4f} → {new_max:.4f}); exceeds "
                f"max_relative_change={max_relative_change:.1%}. "
                f"Investigate before accepting."
            )
            raise ValueError(msg)

    def to_dataframe(self) -> pd.DataFrame:
        """Serialize grid to long-format DataFrame for Delta write.

        Returns:
            DataFrame with columns ``zone_x``, ``zone_y``, ``xt_value``
            (and ``competition_id`` if set on this grid).
        """
        n_x, n_y = self.shape
        rows: list[dict[str, object]] = []
        for zx in range(n_x):
            for zy in range(n_y):
                row: dict[str, object] = {
                    "zone_x": zx,
                    "zone_y": zy,
                    "xt_value": round(float(self.values[zx, zy]), 5),
                }
                if self.competition_id is not None:
                    row["competition_id"] = self.competition_id
                rows.append(row)
        return pd.DataFrame(rows)


# SPADL action types
_MOVE_TYPES = frozenset(
    {
        "pass",
        "cross",
        "throw_in",
        "freekick_crossed",
        "freekick_short",
        "corner_crossed",
        "corner_short",
        "take_on",
        "dribble",
        "goalkick",
        "clearance",
    }
)
_SHOT_TYPES = frozenset({"shot", "shot_penalty", "shot_freekick"})


def _assign_zones(
    x: np.ndarray,
    y: np.ndarray,
    params: ExpectedThreatParams,
) -> np.ndarray:
    """Map (x, y) coordinates to flat zone indices."""
    zone_x = np.clip(
        (x / params.pitch_length * params.n_zones_x).astype(int),
        0,
        params.n_zones_x - 1,
    )
    zone_y = np.clip(
        (y / params.pitch_width * params.n_zones_y).astype(int),
        0,
        params.n_zones_y - 1,
    )
    return zone_x * params.n_zones_y + zone_y


def _build_transition_matrix(
    start_zones: np.ndarray,
    end_zones: np.ndarray,
    n_zones: int,
) -> np.ndarray:
    """Build row-normalized transition matrix from zone-to-zone moves."""
    transition = np.zeros((n_zones, n_zones), dtype=np.float64)
    np.add.at(transition, (start_zones, end_zones), 1.0)

    row_sums = transition.sum(axis=1, keepdims=True)
    row_sums = np.maximum(row_sums, 1.0)
    return transition / row_sums


def _value_iteration_numpy(
    shot_prob: np.ndarray,
    goal_prob: np.ndarray,
    move_prob: np.ndarray,
    transition: np.ndarray,
    max_iterations: int,
    tolerance: float,
) -> tuple[np.ndarray, int]:
    """Pure NumPy value iteration.

    Returns (xT vector, iterations used).
    """
    xt = np.zeros_like(shot_prob)
    for i in range(max_iterations):
        xt_new = shot_prob * goal_prob + move_prob * (transition @ xt)
        delta = float(np.max(np.abs(xt_new - xt)))
        xt = xt_new
        if delta < tolerance:
            return xt, i + 1
    return xt, max_iterations


if _USE_JAX:

    def _value_iteration_jax(
        shot_prob: np.ndarray,
        goal_prob: np.ndarray,
        move_prob: np.ndarray,
        transition: np.ndarray,
        max_iterations: int,
        tolerance: float,
    ) -> tuple[np.ndarray, int]:
        """JAX-accelerated value iteration for dense grids."""
        s = jnp.asarray(shot_prob)
        g = jnp.asarray(goal_prob)
        m = jnp.asarray(move_prob)
        t_mat = jnp.asarray(transition)

        @jax.jit
        def _step(xt: jax.Array) -> jax.Array:
            return s * g + m * (t_mat @ xt)

        xt = jnp.zeros_like(s)
        for i in range(max_iterations):
            xt_new = _step(xt)
            delta = float(jnp.max(jnp.abs(xt_new - xt)))
            xt = xt_new
            if delta < tolerance:
                return np.asarray(xt), i + 1
        return np.asarray(xt), max_iterations


@dataclass(frozen=True)
class ZoneCounters:
    """Bucketed per-zone aggregates needed for xT value iteration.

    Pre-aggregates the only information about an action set that the
    Markov-chain xT computation actually consumes: per-zone start counts,
    per-zone shot/goal/successful-move counts, and the start→end
    transition count matrix (successful moves only). Once an action set
    has been bucketed into a ``ZoneCounters``, the original row-level
    actions are no longer needed.

    Critically, ``ZoneCounters`` is **additive** under union of action
    sets:

        counters(A) + counters(B) == counters(concat(A, B))   for any A, B

    Additivity is what makes streaming xT computation possible — instead
    of pulling all 9.5M+ actions to driver memory at once
    (`fct_action_values` global rebuild — see OPT-1, 2026-05-03), the
    ingestion pipeline streams per-competition action slices to driver,
    buckets each into a small ``ZoneCounters`` (~9,600 floats fixed
    size, regardless of input rows), and accumulates across competitions
    before running value iteration once on the union counters. Driver
    memory peaks at single-competition slice size (~24 MB) instead of
    full-fact-table size (~456 MB).

    The dataclass is frozen so it is safe for `applyInPandas` closure
    capture per project conventions; ``__add__`` returns a new
    ``ZoneCounters`` rather than mutating self.
    """

    total_per_zone: np.ndarray
    shots_per_zone: np.ndarray
    goals_per_zone: np.ndarray
    succ_moves_per_zone: np.ndarray
    transition_counts: np.ndarray

    @classmethod
    def zero(cls, params: ExpectedThreatParams) -> ZoneCounters:
        """Construct an all-zero counter sized for the given grid params."""
        n_zones = params.n_zones_x * params.n_zones_y
        return cls(
            total_per_zone=np.zeros(n_zones, dtype=np.float64),
            shots_per_zone=np.zeros(n_zones, dtype=np.float64),
            goals_per_zone=np.zeros(n_zones, dtype=np.float64),
            succ_moves_per_zone=np.zeros(n_zones, dtype=np.float64),
            transition_counts=np.zeros((n_zones, n_zones), dtype=np.float64),
        )

    def __add__(self, other: ZoneCounters) -> ZoneCounters:
        """Return a new ZoneCounters whose entries are the elementwise sum."""
        return ZoneCounters(
            total_per_zone=self.total_per_zone + other.total_per_zone,
            shots_per_zone=self.shots_per_zone + other.shots_per_zone,
            goals_per_zone=self.goals_per_zone + other.goals_per_zone,
            succ_moves_per_zone=self.succ_moves_per_zone + other.succ_moves_per_zone,
            transition_counts=self.transition_counts + other.transition_counts,
        )

    @property
    def total_actions(self) -> int:
        """Total number of actions bucketed (sum of per-zone start counts)."""
        return int(self.total_per_zone.sum())


def bucket_actions_into_counters(
    actions_df: pd.DataFrame,
    params: ExpectedThreatParams,
) -> ZoneCounters:
    """Bin SPADL actions into per-zone aggregates needed for xT value iteration.

    The output ``ZoneCounters`` is fixed-size (O(n_zones²)) regardless of
    input row count — a 9.5M-row input and a 100-row input produce
    counters of identical shape. This is the primitive that makes
    streaming xT computation possible (see ``ZoneCounters`` docstring).

    Pure function: deterministic, no side effects, no I/O.
    """
    n_zones = params.n_zones_x * params.n_zones_y

    # Classify events
    type_names = actions_df["type_name"].values
    result_names = actions_df["result_name"].values
    is_move = np.array([t in _MOVE_TYPES for t in type_names], dtype=bool)
    is_shot = np.array([t in _SHOT_TYPES for t in type_names], dtype=bool)
    is_success = result_names == "success"

    # Assign zones
    start_x = np.asarray(actions_df["start_x"], dtype=np.float64)
    start_y = np.asarray(actions_df["start_y"], dtype=np.float64)
    end_x = np.asarray(actions_df["end_x"], dtype=np.float64)
    end_y = np.asarray(actions_df["end_y"], dtype=np.float64)

    start_zones = _assign_zones(start_x, start_y, params)
    end_zones = _assign_zones(end_x, end_y, params)

    # Per-zone counts
    total_per_zone = np.bincount(start_zones, minlength=n_zones).astype(np.float64)
    shots_per_zone = np.bincount(start_zones[is_shot], minlength=n_zones).astype(np.float64)
    goals_per_zone = np.bincount(start_zones[is_shot & is_success], minlength=n_zones).astype(np.float64)

    # Successful moves — failed moves lose possession (xT=0)
    successful_moves = is_move & is_success
    succ_moves_per_zone = np.bincount(start_zones[successful_moves], minlength=n_zones).astype(np.float64)

    # Raw transition counts (NOT row-normalized — normalization deferred
    # to xt_grid_from_counters so per-comp counters are additive).
    transition_counts = np.zeros((n_zones, n_zones), dtype=np.float64)
    np.add.at(
        transition_counts,
        (start_zones[successful_moves], end_zones[successful_moves]),
        1.0,
    )

    return ZoneCounters(
        total_per_zone=total_per_zone,
        shots_per_zone=shots_per_zone,
        goals_per_zone=goals_per_zone,
        succ_moves_per_zone=succ_moves_per_zone,
        transition_counts=transition_counts,
    )


def xt_grid_from_counters(
    counters: ZoneCounters,
    params: ExpectedThreatParams,
    *,
    competition_id: str | None = None,
) -> XTGrid:
    """Run Markov-chain value iteration on bucketed counters; return XTGrid.

    Companion to ``bucket_actions_into_counters`` — together they
    decompose the original ``compute_expected_threat_grid`` into a
    bucket step (which is additive across input slices) and a
    value-iteration step (which only needs the bucketed counters, not
    the original row-level actions). This decomposition is what enables
    streaming the global-grid computation across per-competition slices
    without loading all actions to driver memory at once.
    """
    n_zones = params.n_zones_x * params.n_zones_y

    # Probabilities per zone
    safe_total = np.maximum(counters.total_per_zone, 1.0)
    shot_prob = counters.shots_per_zone / safe_total
    goal_prob = np.where(
        counters.shots_per_zone > 0,
        counters.goals_per_zone / np.maximum(counters.shots_per_zone, 1.0),
        0.0,
    )
    # Use successful move probability — failed moves contribute xT=0 implicitly
    move_prob = counters.succ_moves_per_zone / safe_total

    # Row-normalize the transition matrix (deferred from bucketing so
    # per-comp counters remained additive).
    row_sums = counters.transition_counts.sum(axis=1, keepdims=True)
    row_sums = np.maximum(row_sums, 1.0)
    transition = counters.transition_counts / row_sums

    # Value iteration
    use_jax = _USE_JAX and n_zones > 200  # JAX overhead not worth it for small grids
    if use_jax:
        xt_flat, _iters = _value_iteration_jax(
            shot_prob,
            goal_prob,
            move_prob,
            transition,
            params.max_iterations,
            params.tolerance,
        )
    else:
        xt_flat, _iters = _value_iteration_numpy(
            shot_prob,
            goal_prob,
            move_prob,
            transition,
            params.max_iterations,
            params.tolerance,
        )

    return XTGrid(
        values=xt_flat.reshape(params.n_zones_x, params.n_zones_y),
        pitch_length=params.pitch_length,
        pitch_width=params.pitch_width,
        coord_system="spadl",
        competition_id=competition_id,
    )


def compute_expected_threat_grid(
    actions_df: pd.DataFrame,
    params: ExpectedThreatParams | None = None,
    *,
    competition_id: str | None = None,
) -> XTGrid:
    """Compute an xT grid from SPADL action data via Markov chain value iteration.

    Backwards-compatible wrapper: equivalent to
    ``xt_grid_from_counters(bucket_actions_into_counters(actions_df,
    params), params, competition_id=competition_id)`` for any single
    DataFrame. The two-stage form is preferred when the input is too
    large to fit in driver memory at once — see ``ZoneCounters``
    docstring for the streaming pattern.

    Args:
        actions_df: SPADL actions with columns: type_name, result_name,
            start_x, start_y, end_x, end_y. Coordinates in SPADL 105x68m.
        params: Grid and convergence parameters. Defaults if None.
        competition_id: Optional source competition identifier to embed
            on the returned XTGrid.

    Returns:
        XTGrid in SPADL coordinate system, with shape
        (n_zones_x, n_zones_y). Grid orientation: [0, 0] = own-goal
        bottom-left, [n_zones_x-1, n_zones_y-1] = opponent goal top-right.
        Matches the dbt seed CSV layout.
    """
    if params is None:
        params = ExpectedThreatParams()
    counters = bucket_actions_into_counters(actions_df, params)
    return xt_grid_from_counters(counters, params, competition_id=competition_id)
