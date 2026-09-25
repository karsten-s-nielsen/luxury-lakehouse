"""ADR-063 xT-grid structural / directionality / differential guards — reimplemented against a
fitted silly-kicks ``ExpectedThreat`` (ExT-v2 single-canonical-surface migration, lakehouse ADR-085).

These guards previously lived as methods on the retired in-repo ``analytics.expected_threat.XTGrid``.
They exist for the ADR-063 negative-DZV silent-staleness root cause (a stale symmetric/U-shaped grid
that froze for ~2 months because the too-lax per-step monotonicity tolerance let it pass) — do NOT drop
or weaken them; they are the hard gate that forces a re-run rather than propagating a broken grid.

**Orientation (ADR-041).** sk ``ExpectedThreat.xT`` is stored ``(w, l)`` = (y, x) with rows y-INVERTED.
The directionality check must run on a PHYSICALLY-oriented grid, so it samples the model through the sk
``physical_grid`` seam (which neutralises the y-inversion) at ascending physical x — never on the raw
``.xT`` storage. This makes the attacking gradient unambiguous: ascending physical x → toward the
attacking goal (canonical SPADL LTR). Structural / differential / drift operate on scalar
min/max/per-cell magnitudes and are orientation-agnostic, so they read ``.xT`` directly.
"""

from __future__ import annotations

import logging

import numpy as np
import numpy.typing as npt

# Canonical SPADL pitch dimensions (silly-kicks spadlconfig SSOT: 105 x 68).
_PITCH_LENGTH = 105.0
_PITCH_WIDTH = 68.0

# ADR-063 materiality (mirrors the retired producer constants).
_MATERIALITY_VALUE_FLOOR = 0.005


def _physical_profile(model: object, *, n_x: int = 16, n_y: int = 12) -> npt.NDArray[np.float64]:
    """Sample the fitted model on an ascending-physical-x grid → per-x-zone mean xT (length ``n_x``).

    Uses the sk ``physical_grid`` seam (ADR-041 orientation-neutralised), so index 0 = low physical x
    (defensive) and index -1 = high physical x (attacking goal), independent of the raw ``.xT`` storage
    orientation. Returns the mean over y for each x column.
    """
    from silly_kicks.xthreat import physical_grid

    cell_x = _PITCH_LENGTH / n_x
    cell_y = _PITCH_WIDTH / n_y
    xs = np.arange(n_x, dtype=np.float64) * cell_x + 0.5 * cell_x  # ascending physical x cell centres
    ys = np.arange(n_y, dtype=np.float64) * cell_y + 0.5 * cell_y
    # model is a fitted sk ExpectedThreat (typed `object` for guard flexibility); sk's physical_grid
    # stub narrows to ExpectedThreat|str|None, which pyright cannot reconcile with the object param.
    grid = np.asarray(physical_grid(model, xs, ys), dtype=np.float64)  # pyright: ignore[reportArgumentType]  # (n_y, n_x)
    return grid.mean(axis=0)  # per-x mean; index 0 = defensive, -1 = attacking


def validate_structural(
    xt: npt.NDArray[np.float64],
    *,
    max_value: float | None = None,
) -> None:
    """Non-negative / max-ceiling / not-too-narrow value checks on the raw ``.xT`` surface (ADR-063).

    Orientation-agnostic (scalar min/max/range). ``max_value=None`` disables the upper bound; the
    legacy v1 global ceiling was 0.50 — pass it explicitly to preserve that gate.
    """
    lo = float(xt.min())
    hi = float(xt.max())
    if lo < 0.0:
        raise ValueError(f"xT grid contains negative values: min={lo:.4f}")
    if max_value is not None and hi > max_value:
        raise ValueError(f"xT grid max {hi:.4f} exceeds max_value={max_value}")
    xt_range = hi - lo
    if xt_range < 0.05:
        raise ValueError(
            f"xT grid range too narrow ({xt_range:.4f}) — likely a coordinate-orientation issue or "
            f"insufficient training data"
        )


def assert_directional(
    model: object,
    *,
    competition_id: str | None = None,
    min_attack_ratio: float = 3.0,
    min_rank_corr: float = 0.6,
    logger: logging.Logger | None = None,
) -> None:
    """Assert the fitted grid rises toward the attacking goal (ADR-063, review M5).

    Samples the model's PHYSICALLY-oriented per-x profile (``physical_grid`` seam; ADR-041) and applies
    :func:`assert_profile_directional`. Raises ``ValueError`` on a non-directional (stale /
    orientation-regressed) grid — the hard gate that must not be softened.
    """
    assert_profile_directional(
        _physical_profile(model),
        competition_id=competition_id,
        min_attack_ratio=min_attack_ratio,
        min_rank_corr=min_rank_corr,
        logger=logger,
    )


def assert_profile_directional(
    row_means: npt.NDArray[np.float64],
    *,
    competition_id: str | None = None,
    min_attack_ratio: float = 3.0,
    min_rank_corr: float = 0.6,
    logger: logging.Logger | None = None,
) -> None:
    """Pure directionality gate on a per-x mean profile (index 0 = defensive, -1 = attacking).

    Two robust checks (ADR-063, review M5): (1) thirds-mean ratio = mean(attacking third) /
    mean(defensive third) >= ``min_attack_ratio``; (2) Spearman rank correlation between x-zone index
    and per-x mean >= ``min_rank_corr``. Measured values logged at INFO for threshold tuning.
    """
    n = row_means.shape[0]
    third = max(1, n // 3)
    def_mean = float(row_means[:third].mean())
    att_mean = float(row_means[-third:].mean())
    ratio = att_mean / def_mean if def_mean > 0 else float("inf")
    # Spearman rank corr = Pearson on the ranks of row_means vs the sorted zone indices.
    order = np.argsort(row_means, kind="stable")
    ranks = np.empty(n, dtype=np.float64)
    ranks[order] = np.arange(n, dtype=np.float64)
    zone_idx = np.arange(n, dtype=np.float64)
    rank_corr = float(np.corrcoef(ranks, zone_idx)[0, 1]) if n > 1 else 1.0

    if logger is not None:
        logger.info(
            "xT grid '%s' directionality: thirds-ratio=%.2f (threshold %.1f), rank-corr=%.2f "
            "(threshold %.1f), defensive-third=%.5f attacking-third=%.5f",
            competition_id or "?",
            ratio,
            min_attack_ratio,
            rank_corr,
            min_rank_corr,
            def_mean,
            att_mean,
        )

    if ratio < min_attack_ratio:
        raise ValueError(
            f"xT grid is not attacking-directional: attacking-third / defensive-third mean ratio "
            f"{ratio:.2f} < {min_attack_ratio} (defensive third {def_mean:.5f}, attacking third "
            f"{att_mean:.5f}). Likely a STALE grid built on pre-LTR data or an orientation regression "
            f"(ADR-063)."
        )
    if rank_corr < min_rank_corr:
        raise ValueError(
            f"xT grid shape is not monotone attacking-directional: x-zone↔xT rank correlation "
            f"{rank_corr:.2f} < {min_rank_corr} (dips/spikes mid-pitch). Likely an orientation "
            f"regression (ADR-063)."
        )


def validate_differential(
    new_xt: npt.NDArray[np.float64],
    previous_xt: npt.NDArray[np.float64] | None,
    *,
    max_relative_change: float = 0.30,
) -> None:
    """Reject a dramatic peak-value departure from the previous grid (ADR-063). ``None`` prev → skip.

    Advisory in the producer (WARN-only, ADR-063 H3) but kept exact here so the caller decides.
    """
    if previous_xt is None:
        return
    prev_max = float(previous_xt.max())
    new_max = float(new_xt.max())
    if prev_max <= 0.0:
        return
    relative = abs(new_max - prev_max) / prev_max
    if relative > max_relative_change:
        raise ValueError(
            f"xT grid max changed by {relative:.1%} ({prev_max:.4f} -> {new_max:.4f}); exceeds "
            f"max_relative_change={max_relative_change:.1%}. Investigate before accepting."
        )


def grid_drift(
    new_xt: npt.NDArray[np.float64],
    previous_xt: npt.NDArray[np.float64] | None,
) -> float | None:
    """Max relative per-cell change vs the last-propagated grid, among cells above the value floor.

    ``None`` when there is no comparable baseline (treat as material → write). ADR-063 R4(iv):
    the baseline is the CURRENT table grid, which — because writes happen only on material change — IS
    the last-propagated grid, so slow sub-threshold drift cannot accumulate unbounded.
    """
    if previous_xt is None or previous_xt.shape != new_xt.shape:
        return None
    mask = previous_xt >= _MATERIALITY_VALUE_FLOOR
    if not bool(mask.any()):
        return None
    rel = np.abs(new_xt[mask] - previous_xt[mask]) / previous_xt[mask]
    return float(rel.max())
