"""Thin lakehouse adapters onto the silly-kicks tracking frame builders (TF-23, ADR-034).

silly-kicks 4.34.0 ships pure bronze-consuming SkillCorner/Metrica ``convert_to_frames``
builders that own the coordinate rescale, ``ball_z`` recovery, GK derivation, speed, and the
geometric LTR orientation (``orient_frames_to_ltr_by_geometry`` when the orientation flags are
``None``). These adapters keep the *lakehouse-owned* concerns:

* **Clock (B', ADR-040):** the dispatcher rebases the per-batch clock to period-relative for the
  M13 ownership filter; the builder computes its own ``time_seconds`` (SkillCorner re-subtracts
  the nominal period offset, Metrica re-zeroes per input-batch min — broken under 250-frame
  batching). We therefore **discard the builder clock** and **overwrite** ``time_seconds`` via a
  ``(frame_id, period_id)`` map-join onto the dispatcher's period-relative clock (NOT positional —
  the builder drops NaN-ball / malformed rows, so row order will not align).
* **Velocity (ADR-067, delete-and-depend):** the builder's own ``preprocess=`` seam derives
  ``vx``/``vy``/``speed`` — the same seam GS and IDSSE already use — running interpolate ->
  smooth -> derive. The lakehouse's former ``_derive_velocities_savgol`` port is DELETED: it had
  dropped silly-kicks' ``len(x_vals) <= 1`` guard, crashing on 1-frame tracks and zeroing a whole
  work unit (2026-07-11, ``skillcorner:1552423:2`` — 0 of 550 actions, inside a SUCCESS run).

Returns ``(frames, report)``; the AC dispatch consumes only ``frames``.
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING, Any

import numpy as np
import pandas as pd

if TYPE_CHECKING:
    from silly_kicks.tracking.schema import TrackingConversionReport

logger = logging.getLogger(__name__)

# AC result-frame schema = the silly-kicks KLOPPY tracking columns + the lakehouse-derived
# velocity columns. Pinned + asserted so an upstream silly-kicks schema change fails loudly here
# rather than silently downstream.
_AC_FRAME_COLUMNS: frozenset[str] = frozenset(
    {
        "game_id",
        "period_id",
        "frame_id",
        "time_seconds",
        "frame_rate",
        "player_id",
        "team_id",
        "is_ball",
        "is_goalkeeper",
        "x",
        "y",
        "z",
        "speed",
        "speed_source",
        "ball_state",
        "team_attacking_direction",
        "confidence",
        "visibility",
        "source_provider",
        "is_goalkeeper_source",
        "vx",
        "vy",
    }
)


def _overwrite_time_seconds(frames: pd.DataFrame, period_relative_time: pd.DataFrame) -> pd.DataFrame:
    """B': replace the builder's ``time_seconds`` with the dispatcher's period-relative clock.

    Map-join (NOT positional) on ``(frame_id, period_id)``. Every builder-output frame must have a
    clock entry (the builder output is a subset of the bronze frames the dispatcher clocked); an
    unmapped row is a contract break and raises rather than silently leaving a stale builder clock.
    """
    tmap = {
        (int(f), int(p)): t
        for f, p, t in zip(
            period_relative_time["frame_id"],
            period_relative_time["period_id"],
            period_relative_time["time_seconds"],
            strict=False,
        )
    }
    out = frames.copy()
    out["time_seconds"] = [
        tmap.get((int(f), int(p)), np.nan)
        for f, p in zip(out["frame_id"].to_numpy(), out["period_id"].to_numpy(), strict=False)
    ]
    n_unmapped = int(out["time_seconds"].isna().sum())
    if n_unmapped:
        raise ValueError(
            f"sk_frame_adapters: {n_unmapped} frame row(s) had no period-relative clock match on "
            "(frame_id, period_id) — dispatcher clock and builder output disagree."
        )
    return out


def _preprocess_config() -> Any:
    """The builder's ``preprocess=`` argument (ADR-067).

    ``PreprocessConfig.default()`` — NEVER ``PreprocessConfig(derive_velocity=True)``. ``is_default()``
    is FLAG-based (set only by the ``default()`` factory), so a hand-built config is passed through
    ``resolve_preprocess`` UNPROMOTED and silently uses the universal ``sg_window_seconds=0.4``
    instead of SkillCorner's tuned 1.0 / Metrica's 0.4.

    No empty-bronze branch: the builders raise on empty input long before preprocess runs
    (``skillcorner.py:138`` reads ``src["match_id"].iloc[0]``), so empty bronze is unsupported
    upstream — as it was before this change. See ``test_empty_bronze_is_unsupported``.
    """
    from silly_kicks.tracking.preprocess import PreprocessConfig

    return PreprocessConfig.default()


def _finalize(frames: pd.DataFrame, *, derive_velocities: bool) -> pd.DataFrame:
    """Drop silly-kicks' preprocess scratch columns and assert the AC result-frame schema.

    Velocity derivation lives in the BUILDER now (``preprocess=``), not here — see the module
    docstring and ADR-067. ``provider`` is gone with it: it only ever selected SG parameters, which
    the builder resolves itself.
    """
    scratch = [c for c in ("x_smoothed", "y_smoothed", "_preprocessed_with") if c in frames.columns]
    if scratch:
        frames = frames.drop(columns=scratch)
    if not derive_velocities:
        # The builder emits `speed`/`speed_source` unconditionally but NOT vx/vy, so without the
        # preprocess pass the AC schema is short two columns and the drift check below would raise.
        # NaN is exactly what "no velocity derived" means.
        for col in ("vx", "vy"):
            if col not in frames.columns:
                frames[col] = np.nan
    if not frames.empty:
        frames = frames.sort_values(["frame_id", "is_ball"]).reset_index(drop=True)
    drift = set(frames.columns) ^ _AC_FRAME_COLUMNS
    if drift:
        raise ValueError(f"sk_frame_adapters: AC result-frame schema drift (symmetric diff): {sorted(drift)}")
    return frames


def convert_skillcorner_bronze_to_frames(
    bronze: pd.DataFrame,
    *,
    game_id: Any,
    home_team_id: Any,
    period_relative_time: pd.DataFrame,
    derive_velocities: bool = True,
) -> tuple[pd.DataFrame, TrackingConversionReport]:
    """Adapt post-join SkillCorner bronze to AC result frames via silly-kicks 4.34.0.

    Parameters
    ----------
    bronze : pd.DataFrame
        Post-``skillcorner_matches``-join bronze in the silly-kicks SkillCorner
        ``EXPECTED_INPUT_COLUMNS`` shape (``ball_x/ball_y/ball_z``, center-origin ``x``/``y``,
        ``team_id``, ``is_goalkeeper``, ``is_visible``, ``match_id``, ``frame``, ``period``,
        ``timestamp``, ``frame_rate``, and — since silly-kicks 4.87.0 — the per-match
        ``pitch_length``/``pitch_width`` (metres) that scale centre-origin metres to SPADL 105x68).
        Absent pitch dims trigger a logged ``assume_standard_pitch`` fallback (see body).
    game_id : Any
        Degenerate match id the AC path keys frames on (``int(actions["game_id"])``). Overrides
        the builder's ``game_id`` (which it sets to the bronze ``match_id``) for old-builder parity.
    home_team_id : Any
        Native SkillCorner home team id (stringified for the builder).
    period_relative_time : pd.DataFrame
        Dispatcher's period-relative clock: columns ``frame_id``, ``period_id``, ``time_seconds``.
    derive_velocities : bool
        Apply the lakehouse Savitzky-Golay velocity step after the builder (default True).

    Returns
    -------
    (frames, report)
        ``frames`` in AC result-frame schema (home-LTR, ``z`` recovered); ``report`` is the
        silly-kicks ``TrackingConversionReport`` (unused by the AC path).
    """
    from silly_kicks.tracking.skillcorner import convert_to_frames

    # silly-kicks 4.87.0: the SkillCorner builder SCALES centre-origin metres -> SPADL 105x68 using
    # the per-match pitch dimensions and now REQUIRES `pitch_length`/`pitch_width` columns on the
    # bronze (a fixed +52.5/+34 offset lands the goal line ~1-2 m off on a non-105 m pitch — 4/10
    # public SkillCorner matches are 104/106 m; the ADR-038 clamp/scale defect). We thread the REAL
    # dims: production denormalises them onto the post-join bronze from
    # `bronze.skillcorner_matches.pitch_length/width` (ingestion/action_context.py, alongside
    # team_id/is_goalkeeper), and the fixtures carry them. Only when they are GENUINELY absent do we
    # fall back to `assume_standard_pitch=True` — a LAST RESORT that reintroduces the goal-line error
    # on non-standard pitches, so it is logged at ERROR (never a silent 105x68 default; CLAUDE.md
    # telemetry rule). `.notna().any()` (not iloc[0]) is the has-dims signal: the join denormalises a
    # single non-null value across every row, so an unmapped-roster edge cannot fake presence.
    has_pitch_dims = (
        "pitch_length" in bronze.columns
        and "pitch_width" in bronze.columns
        and bool(bronze["pitch_length"].notna().any())
        and bool(bronze["pitch_width"].notna().any())
    )
    if not has_pitch_dims and not bronze.empty:
        logger.error(
            "convert_skillcorner_bronze_to_frames: bronze carries no pitch_length/pitch_width for "
            "game %s — falling back to assume_standard_pitch=105x68. This reintroduces the ADR-038 "
            "~1-2 m goal-line error on non-standard pitches; the real dims MUST be threaded from "
            "bronze.skillcorner_matches. Investigate the upstream metadata join.",
            game_id,
        )

    # Flags omitted => geometric LTR orientation (skillcorner.py: home_team_start_left is None).
    frames, report = convert_to_frames(
        bronze,
        home_team_id=str(home_team_id),
        output_convention="ltr",
        preprocess=_preprocess_config() if derive_velocities else None,
        assume_standard_pitch=not has_pitch_dims,
    )
    frames["game_id"] = game_id  # old-builder parity (builder sets game_id = bronze match_id)
    frames = _overwrite_time_seconds(frames, period_relative_time)
    frames = _finalize(frames, derive_velocities=derive_velocities)
    return frames, report


def convert_metrica_bronze_to_frames(
    bronze: pd.DataFrame,
    *,
    game_id: Any,
    jersey_to_player_id: dict[str, dict[str, str]],
    period_relative_time: pd.DataFrame,
    home_team_id: str = "Home",
    derive_velocities: bool = True,
) -> tuple[pd.DataFrame, TrackingConversionReport]:
    """Adapt Metrica bronze (frame-level JSON) to AC result frames via silly-kicks 4.34.0.

    The builder rescales 0-1 -> SPADL, positionally derives GK (Metrica is anonymised; no native
    GK seed), and geometrically orients to home-LTR (flags omitted). Two lakehouse overrides:

    * **Clock (D1/B'):** the builder re-zeroes ``time_seconds`` by the per-``period`` min of *its
      input batch* — batch-relative under 250-frame batching (a metrica period is 272-297 batches;
      ~99.6% would be re-zeroed to the batch start). We discard it and overwrite with the
      dispatcher's frame-number period-relative clock via a ``(frame_id, period_id)`` map-join.
    * **Synthetic-id surfacing (D5):** unmapped jerseys get a builder ``f"{team}_{jersey}"`` id
      (e.g. ``"Home_11"``) that will NOT match an action ``player_id_native`` (linkage break). The
      roster is built from the acting players, so a fallback should never fire for a *linked*
      action; we warn loudly with a count if any appears.

    Parameters
    ----------
    game_id : Any
        Degenerate match id (``int(actions["game_id"])``) the AC path keys frames on.
    jersey_to_player_id : dict[str, dict[str, str]]
        Per-team roster ``{"Home": {jersey: pid}, "Away": {...}}`` (outer keys MUST be the builder's
        hard-coded ``"Home"``/``"Away"`` labels).
    period_relative_time : pd.DataFrame
        Dispatcher's frame-number period-relative clock: ``frame_id``, ``period_id``,
        ``time_seconds``.
    """
    import warnings

    from silly_kicks.tracking.metrica import convert_to_frames

    frames, report = convert_to_frames(
        bronze,
        home_team_id=home_team_id,
        jersey_to_player_id=jersey_to_player_id,
        output_convention="ltr",
        preprocess=_preprocess_config() if derive_velocities else None,
    )
    frames["game_id"] = game_id  # builder leaves game_id unset for Metrica (no match_id input)
    frames = _overwrite_time_seconds(frames, period_relative_time)

    # D5: surface any synthetic f"{team}_{jersey}" fallback id (unmapped jersey -> linkage risk).
    synthetic = frames.loc[~frames["is_ball"], "player_id"].astype("string")
    n_synthetic = int(synthetic.str.match(r"^(Home|Away)_\d+$", na=False).sum())
    if n_synthetic:
        bad = sorted(synthetic[synthetic.str.match(r"^(Home|Away)_\d+$", na=False)].unique())
        warnings.warn(
            f"convert_metrica_bronze_to_frames: {n_synthetic} frame row(s) carry synthetic roster-"
            f"fallback player_id(s) {bad} (unmapped jersey) — these will not link to actions.",
            UserWarning,
            stacklevel=2,
        )
    frames = _finalize(frames, derive_velocities=derive_velocities)
    return frames, report
