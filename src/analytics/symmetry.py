"""Symmetry augmentation for tracking data (TacticAI, DeepMind 2024).

Produces up to 8x data from H-flip, V-flip, and team swap combinations.
All operations are pure pandas on DataFrames. No side effects.
"""

from __future__ import annotations

from dataclasses import dataclass

import pandas as pd


@dataclass(frozen=True)
class AugmentationConfig:
    """Pitch dimensions for coordinate mirroring."""

    pitch_length: float = 120.0  # StatsBomb x-axis
    pitch_width: float = 80.0  # StatsBomb y-axis
    x_col: str = "x"
    y_col: str = "y"
    vx_col: str = "velocity_x"
    vy_col: str = "velocity_y"
    ball_x_col: str = "ball_x"
    ball_y_col: str = "ball_y"
    team_col: str = "team"


def _default_config(config: AugmentationConfig | None) -> AugmentationConfig:
    """Return *config* or a default ``AugmentationConfig``."""
    return config if config is not None else AugmentationConfig()


def flip_horizontal(df: pd.DataFrame, config: AugmentationConfig | None = None) -> pd.DataFrame:
    """Mirror pitch left-to-right: x -> pitch_length - x, vx -> -vx.

    Mirrors the x coordinate, ball_x coordinate, and negates the
    x-component of velocity.  Columns that do not exist in *df* are
    silently skipped.

    Args:
        df: Tracking-frame DataFrame.
        config: Pitch dimensions and column names.

    Returns:
        A new DataFrame with mirrored x coordinates.
    """
    cfg = _default_config(config)
    out = df.copy()

    if cfg.x_col in out.columns:
        out[cfg.x_col] = cfg.pitch_length - out[cfg.x_col]

    if cfg.ball_x_col in out.columns:
        out[cfg.ball_x_col] = cfg.pitch_length - out[cfg.ball_x_col]

    if cfg.vx_col in out.columns:
        out[cfg.vx_col] = -out[cfg.vx_col]

    return out


def flip_vertical(df: pd.DataFrame, config: AugmentationConfig | None = None) -> pd.DataFrame:
    """Mirror pitch top-to-bottom: y -> pitch_width - y, vy -> -vy.

    Mirrors the y coordinate, ball_y coordinate, and negates the
    y-component of velocity.  Columns that do not exist in *df* are
    silently skipped.

    Args:
        df: Tracking-frame DataFrame.
        config: Pitch dimensions and column names.

    Returns:
        A new DataFrame with mirrored y coordinates.
    """
    cfg = _default_config(config)
    out = df.copy()

    if cfg.y_col in out.columns:
        out[cfg.y_col] = cfg.pitch_width - out[cfg.y_col]

    if cfg.ball_y_col in out.columns:
        out[cfg.ball_y_col] = cfg.pitch_width - out[cfg.ball_y_col]

    if cfg.vy_col in out.columns:
        out[cfg.vy_col] = -out[cfg.vy_col]

    return out


def swap_teams(df: pd.DataFrame, config: AugmentationConfig | None = None) -> pd.DataFrame:
    """Swap home/away labels in the team column.

    Maps ``'home'`` to ``'away'`` and ``'away'`` to ``'home'``.
    Other values are left unchanged.

    Args:
        df: Tracking-frame DataFrame.
        config: Pitch dimensions and column names.

    Returns:
        A new DataFrame with swapped team labels.
    """
    cfg = _default_config(config)
    out = df.copy()

    if cfg.team_col in out.columns:
        mapping = {"home": "away", "away": "home"}
        # NA values pass through unchanged; mapping.get needs a str key so we
        # check type before lookup (pandas-stubs types elements as Unknown | NAType).
        out[cfg.team_col] = out[cfg.team_col].map(
            lambda v: mapping.get(v, v) if isinstance(v, str) else v,
        )

    return out


# Canonical ordering of augmentation labels.  The identity transform is
# listed first so that ``include_original=True`` returns the original
# frame at index 0.
_TRANSFORMS: list[tuple[str, tuple[bool, bool, bool]]] = [
    ("original", (False, False, False)),
    ("h_flip", (True, False, False)),
    ("v_flip", (False, True, False)),
    ("team_swap", (False, False, True)),
    ("h_flip+v_flip", (True, True, False)),
    ("h_flip+team_swap", (True, False, True)),
    ("v_flip+team_swap", (False, True, True)),
    ("h_flip+v_flip+team_swap", (True, True, True)),
]


def augment_tracking_frame(
    df: pd.DataFrame,
    config: AugmentationConfig | None = None,
    include_original: bool = True,
) -> list[pd.DataFrame]:
    """Generate all 8 symmetry variants of a tracking frame.

    Combinations: {identity, H-flip} x {identity, V-flip} x {identity, team-swap}
    = 2 x 2 x 2 = 8 variants.

    Each returned DataFrame has an ``augmentation`` column indicating the
    transform applied (e.g. ``'original'``, ``'h_flip'``,
    ``'h_flip+v_flip+team_swap'``).

    Args:
        df: Tracking-frame DataFrame.
        config: Pitch dimensions and column names.
        include_original: If ``True`` (default), the unmodified frame is
            included as the first element.

    Returns:
        List of 8 (or 7 if *include_original* is ``False``) DataFrames.
    """
    cfg = _default_config(config)
    variants: list[pd.DataFrame] = []

    for label, (do_h, do_v, do_swap) in _TRANSFORMS:
        if label == "original" and not include_original:
            continue

        variant = df.copy()
        if do_h:
            variant = flip_horizontal(variant, cfg)
        if do_v:
            variant = flip_vertical(variant, cfg)
        if do_swap:
            variant = swap_teams(variant, cfg)

        variant["augmentation"] = label
        variants.append(variant)

    return variants
