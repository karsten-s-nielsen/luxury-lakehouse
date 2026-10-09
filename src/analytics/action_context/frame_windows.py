"""Haloed frame-window build seam (pure core) — ADR-089.

A per-unit frame build (``build_unit_inputs`` -> sk ``convert_to_frames`` + ``PreprocessConfig``) run
inside a serverless ``mapInPandas`` UDF OOMs the ~1 GB worker on a dense tracking half (the built frame
+ the raw group coexist). This module bounds the worker peak by WINDOW size, not unit size: partition a
unit's frames into contiguous windows, replicate a halo into the neighbours, build each ``(core+halo)``
window, trim the halo, union the cores == the whole-unit built frame.

sk preprocess is a COMPOSED per-player stencil chain (verified sk 4.128): interpolate (<= ``max_gap_seconds``)
-> savgol smooth (``window_frames``) -> savgol-deriv velocity ON ``x_smoothed`` (another ``window_frames``).
The reach is ADDITIVE, so the halo is ``H = (window_frames - 1) + max_gap_frames`` (NOT ``max(...)``), per
provider via the frames' ``frame_rate`` (``hz``). ``window_frames``/``max_gap_frames`` and ``hz`` are sk's
own formula (``_velocity.py``: ``window_frames = max(round(sg_window_seconds*hz)|1, sg_poly_order+2)``,
odd-rounded; ``hz`` = the ``frame_rate`` column, else 25.0). The drift sentinel pins the sk INPUT config
per provider (not H, which is derived) so a sk change to the window/gap fails loud (ADR-067 lockstep).

The uncapped ``np.interp`` NaN-fill in ``derive_velocities`` (``_velocity.py:76-94``) means the windowed
build is byte-identical only where detection gaps near a window boundary are <= ``max_gap``; a ``>max_gap``
gap within ``(window_frames-1)`` of a boundary is a measured residual (spec §4.2a, conservative rule).

Pure: NO pyspark / ingestion imports (import-linter). ``ingestion.frame_window_dispatch`` wraps this for Spark.
"""

from __future__ import annotations

import math
from typing import TYPE_CHECKING, NamedTuple

if TYPE_CHECKING:  # pragma: no cover - typing only
    import pandas as pd

# Lakehouse provider -> sk preprocess-defaults key. idsse is the Sportec/DFL format (sk "sportec").
_SK_PROVIDER_KEY: dict[str, str] = {
    "idsse": "sportec",
    "metrica": "metrica",
    "skillcorner": "skillcorner",
    "gradientsports": "gradientsports",
}

_DEFAULT_HZ = 25.0  # sk _velocity.py fallback when a frame has no frame_rate column.

# Providers the seam serves. metrica is EXCLUDED (owner ruling + convert window-UNSAFE — its sk
# converter rebases time_seconds off the passed set's period_min; spec §4.1a / owner rulings) and
# keeps the whole-unit / floor-batched path at every site. Pure (analytics) so BOTH the ingestion
# Spark dispatch and the analytics local hexagon import the SAME membership (no drift).
SEAM_PROVIDERS: frozenset[str] = frozenset({"idsse", "skillcorner", "gradientsports"})

# Nominal per-provider capture rate (Hz) for halo sizing. The halo is SAFE when >= sk's actual
# reach, so a conservative (larger) hz only over-halos (more boundary rows, never fewer); the
# neutrality test guards correctness. idsse/metrica 25 fps, GS/skillcorner 10.
_PROVIDER_HZ: dict[str, float] = {"idsse": 25.0, "metrica": 25.0, "skillcorner": 10.0, "gradientsports": 10.0}


def provider_hz(provider: str) -> float:
    """Nominal capture rate (Hz) for ``provider`` (``_DEFAULT_HZ`` fallback) — the halo hz."""
    return _PROVIDER_HZ.get(provider, _DEFAULT_HZ)


class _SkCfg(NamedTuple):
    sg_window_seconds: float
    sg_poly_order: int
    max_gap_seconds: float


# Pinned sk-4.128 PreprocessConfig INPUTS per sk-provider key (read from
# silly_kicks/tracking/preprocess/_provider_defaults_generated.py, 2026-10-07). The sentinel below
# asserts the LIVE sk config still matches these; H is DERIVED from them via the sk formula (so this pin
# is the input, never tautologically H's own output). Derivation shown in the comment per value.
#   H values below are the COMPUTED halo_frames(provider, hz) at the representative provider hz (verified
#   2026-10-07). ceil() of (max_gap_seconds*hz) is a float operation — e.g. metrica 0.56*25 == 14.0000…2
#   so ceil==15 (not 14); a +1-frame over-halo is SAFE (strictly >= the true composed reach).
_PINNED_SK_CFG: dict[str, _SkCfg] = {
    # idsse: 0.4s/poly3/0.48s @25Hz -> wf=11, H=(11-1)+ceil(12.0)=22
    "sportec": _SkCfg(0.4, 3, 0.48),
    # GS: 0.333s/poly3/0.5s @10Hz -> wf=max(3,5)=5, H=(5-1)+ceil(5.0)=9
    "gradientsports": _SkCfg(0.333, 3, 0.5),
    # metrica: 0.4s/poly3/0.56s @25Hz -> wf=11, H=10+ceil(14.0000..2)=10+15=25
    "metrica": _SkCfg(0.4, 3, 0.56),
    # skillcorner: 1.0s/poly3/0.6s @10Hz -> wf=11, H=10+ceil(6.0)=16
    "skillcorner": _SkCfg(1.0, 3, 0.6),
}


def _resolve_sk_cfg(provider: str) -> _SkCfg:
    """The LIVE sk PreprocessConfig inputs for a lakehouse provider (reads sk, not the pin).

    Uses the PUBLIC ``get_provider_defaults()`` (a ``{sk_key: PreprocessConfig}`` map, sk ``__all__``)
    rather than the private ``_resolve.resolve_preprocess`` — same ``_PROVIDER_DEFAULTS`` source, so the
    pinned-config drift sentinel is unaffected (no new private sk import; test_no_new_private_sk_imports).
    """
    from silly_kicks.tracking.preprocess import get_provider_defaults

    key = _SK_PROVIDER_KEY[provider]
    cfg = get_provider_defaults()[key]
    return _SkCfg(float(cfg.sg_window_seconds), int(cfg.sg_poly_order), float(cfg.max_gap_seconds))


def _window_frames(cfg: _SkCfg, hz: float) -> int:
    """sk ``_velocity.py`` savgol window in frames: ``max(round(sg_window*hz)|1, poly+2)``, forced odd."""
    wf = max(round(cfg.sg_window_seconds * hz) | 1, cfg.sg_poly_order + 2)
    if wf % 2 == 0:
        wf += 1
    return wf


def halo_frames(provider: str, hz: float = _DEFAULT_HZ) -> int:
    """Composed-stencil halo radius in FRAMES for ``provider`` at frame-rate ``hz`` (spec §4.2).

    ``H = (window_frames - 1) + max_gap_frames`` — smooth-savgol ∘ velocity-savgol (``2·savgol_radius =
    window_frames-1``) + the preceding ``interpolate_frames`` reach (``max_gap_frames``). ``hz`` is the
    unit's ``frame_rate`` (data-driven; sk defaults 25.0). NEVER ``max(...)``.
    """
    cfg = _resolve_sk_cfg(provider)
    wf = _window_frames(cfg, hz)
    max_gap_frames = math.ceil(cfg.max_gap_seconds * hz)
    return (wf - 1) + max_gap_frames


# ── Drift sentinel (import-time) — pins the sk INPUT, fires loud on sk window/gap drift (ADR-067). ──
def _assert_sk_config_pinned() -> None:
    for provider, sk_key in _SK_PROVIDER_KEY.items():
        live = _resolve_sk_cfg(provider)
        pinned = _PINNED_SK_CFG[sk_key]
        if live != pinned:
            raise AssertionError(
                f"silly-kicks PreprocessConfig drift for {provider!r} (sk key {sk_key!r}): "
                f"live={live} != pinned={pinned}. The haloed-frame-window halo H is derived from these "
                f"(sg_window_seconds/sg_poly_order/max_gap_seconds); a change resizes the halo silently. "
                f"Re-derive H, update _PINNED_SK_CFG + the per-value derivation comment, and re-run the "
                f"neutrality + halo-sufficiency tests (ADR-089 / ADR-067)."
            )


_assert_sk_config_pinned()


# ── Window assignment + halo replication (pure pandas; one unit's frames) ──────────────────────────
# Helper columns are underscore-prefixed + dropped before the sk build sees them.
_WINDOW_COL = "_window_id"
_HALO_COL = "_is_halo"


def assign_window_ids(
    frames: pd.DataFrame,
    provider: str,
    *,
    target_window_frames: int,
    frame_col: str,
    hz: float = _DEFAULT_HZ,
) -> pd.DataFrame:
    """Add ``_window_id`` + ``_is_halo`` to a unit's raw frames (ONE ``(match, period)`` half).

    ``_window_id`` is on the dense DISTINCT-frame ordinal (NOT the raw row index — a frame has ~22 player
    rows). Each frame's rows appear once as CORE in their own window (``_is_halo=False``) and are
    REPLICATED as ``_is_halo=True`` into a neighbouring window when within ``H = halo_frames(provider,hz)``
    of that neighbour's boundary, so the neighbour's build sees a sequence-continuous halo (spec §4.3).
    The returned frame is LONGER than the input (the halo duplicates). Rows preserve all original columns.
    """
    import numpy as np
    import pandas as pd

    halo = halo_frames(provider, hz)
    distinct = np.sort(frames[frame_col].unique())
    ordinal = {f: i for i, f in enumerate(distinct)}  # dense 0-based frame ordinal
    ords = frames[frame_col].map(ordinal).to_numpy()
    core_w = ords // target_window_frames
    n_windows = int(core_w.max()) + 1 if len(core_w) else 0

    core = frames.copy()
    core[_WINDOW_COL] = core_w
    core[_HALO_COL] = False

    parts = [core]
    # left-halo: the first ``halo`` ordinals of window w (w>=1) also feed window w-1's RIGHT edge.
    pos_in_window = ords - core_w * target_window_frames
    left_mask = (pos_in_window < halo) & (core_w >= 1)
    if left_mask.any():
        lh = frames[left_mask].copy()
        lh[_WINDOW_COL] = core_w[left_mask] - 1
        lh[_HALO_COL] = True
        parts.append(lh)
    # right-halo: the last ``halo`` ordinals of window w (w < last) also feed window w+1's LEFT edge.
    dist_to_end = (core_w + 1) * target_window_frames - 1 - ords
    right_mask = (dist_to_end < halo) & (core_w < (n_windows - 1))
    if right_mask.any():
        rh = frames[right_mask].copy()
        rh[_WINDOW_COL] = core_w[right_mask] + 1
        rh[_HALO_COL] = True
        parts.append(rh)

    return pd.concat(parts, ignore_index=True)


def build_windowed(
    window_rows: pd.DataFrame,
    build_fn,
    *,
    frame_col: str,
) -> pd.DataFrame:
    """Build ONE window's ``(core+halo)`` raw frames, then TRIM the halo from the OUTPUT.

    ``build_fn`` is the sk build (``make_build_udf``'s body) over the raw rows; it never sees the helper
    columns. The halo rows feed the composed stencils (velocity continuity) but are dropped from the
    result so the union of windows == the whole-unit build. The trim matches on the raw ``frame_col``
    values that were CORE in this window (the sk build preserves per-frame identity 1:1, verified by the
    neutrality test); the built frame carries the same frame identifier under ``frame_col`` OR ``frame_id``.
    """
    core_frames = set(window_rows.loc[~window_rows[_HALO_COL], frame_col].unique())
    raw = window_rows.drop(columns=[_WINDOW_COL, _HALO_COL])
    built = build_fn(raw)
    built_key = frame_col if frame_col in built.columns else "frame_id"
    return built[built[built_key].isin(core_frames)].reset_index(drop=True)
