"""Physics-based tracking augmentation with position jitter.

Composes TacticAI symmetry augmentation (8x) with Gaussian position
perturbation (Nx) to produce 8*(1+N) = 88x training data from a single
tracking frame.  All operations are pure NumPy/pandas with no side effects.

Symmetry operates in StatsBomb 120x80 coordinates; jitter operates in
centered meter-space (origin at pitch midpoint).  Coordinate conversion
is handled internally.

Reference: TacticAI (Wang et al., Nature Communications 2024) — symmetry
augmentation foundation.
"""

from __future__ import annotations

from dataclasses import dataclass

import numpy as np
import pandas as pd

from analytics.symmetry import AugmentationConfig, augment_tracking_frame

# ---------------------------------------------------------------------------
# Constants — StatsBomb and meter-space dimensions
# ---------------------------------------------------------------------------

_SB_LENGTH = 120.0
_SB_WIDTH = 80.0
_PITCH_LENGTH_M = 105.0
_PITCH_WIDTH_M = 68.0


# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class PerturbationConfig:
    """Physical constraints for position jitter."""

    max_speed_ms: float = 12.0  # Elite sprint ceiling (m/s)
    max_acceleration_ms2: float = 7.0  # From PitchControlParams
    jitter_sigma_m: float = 0.10  # Position noise std dev (meters)
    n_perturbations: int = 10  # Draws per frame
    pitch_length_m: float = 105.0  # Meter-space bounds
    pitch_width_m: float = 68.0


# ---------------------------------------------------------------------------
# Coordinate conversion helpers (StatsBomb 120x80 <-> centered meter-space)
# ---------------------------------------------------------------------------


def _sb_to_meters_x(x: np.ndarray, pitch_length_m: float = _PITCH_LENGTH_M) -> np.ndarray:
    """Convert StatsBomb x-coordinates to centered meter-space."""
    return (x / _SB_LENGTH) * pitch_length_m - pitch_length_m / 2.0


def _sb_to_meters_y(y: np.ndarray, pitch_width_m: float = _PITCH_WIDTH_M) -> np.ndarray:
    """Convert StatsBomb y-coordinates to centered meter-space."""
    return (y / _SB_WIDTH) * pitch_width_m - pitch_width_m / 2.0


def _meters_to_sb_x(x_m: np.ndarray, pitch_length_m: float = _PITCH_LENGTH_M) -> np.ndarray:
    """Convert centered meter-space x-coordinates to StatsBomb."""
    return (x_m + pitch_length_m / 2.0) / pitch_length_m * _SB_LENGTH


def _meters_to_sb_y(y_m: np.ndarray, pitch_width_m: float = _PITCH_WIDTH_M) -> np.ndarray:
    """Convert centered meter-space y-coordinates to StatsBomb."""
    return (y_m + pitch_width_m / 2.0) / pitch_width_m * _SB_WIDTH


def _sb_vel_to_ms_x(vx: np.ndarray) -> np.ndarray:
    """Convert StatsBomb velocity_x (SB units/s) to m/s."""
    return vx * (_PITCH_LENGTH_M / _SB_LENGTH)


def _sb_vel_to_ms_y(vy: np.ndarray) -> np.ndarray:
    """Convert StatsBomb velocity_y (SB units/s) to m/s."""
    return vy * (_PITCH_WIDTH_M / _SB_WIDTH)


def _ms_vel_to_sb_x(vx_ms: np.ndarray) -> np.ndarray:
    """Convert m/s velocity_x to StatsBomb units/s."""
    return vx_ms * (_SB_LENGTH / _PITCH_LENGTH_M)


def _ms_vel_to_sb_y(vy_ms: np.ndarray) -> np.ndarray:
    """Convert m/s velocity_y to StatsBomb units/s."""
    return vy_ms * (_SB_WIDTH / _PITCH_WIDTH_M)


# ---------------------------------------------------------------------------
# Core perturbation
# ---------------------------------------------------------------------------


def perturb_positions(
    df: pd.DataFrame,
    config: PerturbationConfig,
    rng: np.random.Generator,
) -> list[pd.DataFrame]:
    """Generate N Gaussian-perturbed copies of a tracking frame.

    Algorithm:
        1. Convert positions and velocities from StatsBomb to centered meter-space.
        2. Draw independent Gaussian noise: dx, dy ~ N(0, jitter_sigma_m).
        3. Add noise to each player's (x, y) position.
        4. Clamp positions to pitch bounds.
        5. Re-derive velocities from the position delta (perturbed - original)
           added to the original velocity, ensuring kinematic consistency.
        6. Clamp resulting speed to max_speed_ms by scaling the velocity vector.
        7. Convert back to StatsBomb coordinates.

    Args:
        df: Tracking-frame DataFrame with columns: x, y, velocity_x, velocity_y,
            plus any metadata columns (player_id, team, ball_x, ball_y, etc.).
            Coordinates in StatsBomb 120x80.
        config: Physical constraints for perturbation.
        rng: NumPy random number generator for reproducibility.

    Returns:
        List of ``config.n_perturbations`` DataFrames, each with added
        ``augmentation`` (``"jitter_0"`` through ``"jitter_{N-1}"``) and
        ``jitter_seed`` columns.  Coordinates in StatsBomb 120x80.
    """
    n_pert = config.n_perturbations
    n_players = len(df)
    half_length = config.pitch_length_m / 2.0
    half_width = config.pitch_width_m / 2.0

    # Extract original positions and velocities as NumPy arrays
    x_sb = np.asarray(df["x"], dtype=np.float64)
    y_sb = np.asarray(df["y"], dtype=np.float64)
    vx_sb = np.asarray(df["velocity_x"], dtype=np.float64)
    vy_sb = np.asarray(df["velocity_y"], dtype=np.float64)

    # Convert to centered meter-space — all (n_players,) arrays
    x_m = _sb_to_meters_x(x_sb, config.pitch_length_m)
    y_m = _sb_to_meters_y(y_sb, config.pitch_width_m)
    vx_ms = _sb_vel_to_ms_x(vx_sb)
    vy_ms = _sb_vel_to_ms_y(vy_sb)

    # --- Fully vectorized: all perturbations at once (n_pert, n_players) ---

    # Draw Gaussian noise
    dx_all = rng.normal(0.0, config.jitter_sigma_m, size=(n_pert, n_players))
    dy_all = rng.normal(0.0, config.jitter_sigma_m, size=(n_pert, n_players))

    # Record seeds for reproducibility tracking
    seed_values = rng.integers(0, 2**31, size=n_pert)

    # Perturb positions: broadcast original (n_players,) across (n_pert, n_players)
    x_pert = x_m[np.newaxis, :] + dx_all  # (n_pert, n_players)
    y_pert = y_m[np.newaxis, :] + dy_all

    # Clamp to pitch bounds
    np.clip(x_pert, -half_length, half_length, out=x_pert)
    np.clip(y_pert, -half_width, half_width, out=y_pert)

    # Re-derive velocities: original velocity + position delta
    vx_pert = vx_ms[np.newaxis, :] + (x_pert - x_m[np.newaxis, :])
    vy_pert = vy_ms[np.newaxis, :] + (y_pert - y_m[np.newaxis, :])

    # Clamp speed: scale velocity vector down if it exceeds max_speed_ms
    speed = np.sqrt(vx_pert**2 + vy_pert**2)
    over_limit = speed > config.max_speed_ms
    scale = np.where(over_limit, config.max_speed_ms / np.maximum(speed, 1e-10), 1.0)
    vx_pert *= scale
    vy_pert *= scale

    # Convert all back to StatsBomb — still (n_pert, n_players)
    x_sb_all = _meters_to_sb_x(x_pert, config.pitch_length_m)
    y_sb_all = _meters_to_sb_y(y_pert, config.pitch_width_m)
    vx_sb_all = _ms_vel_to_sb_x(vx_pert)
    vy_sb_all = _ms_vel_to_sb_y(vy_pert)

    # --- Build output DataFrames efficiently ---
    # Pre-extract metadata columns once (avoids repeated df.copy())
    meta_cols = [c for c in df.columns if c not in {"x", "y", "velocity_x", "velocity_y"}]
    meta_dict = {c: df[c].to_numpy() for c in meta_cols}

    results: list[pd.DataFrame] = []
    for i in range(n_pert):
        data: dict[str, object] = {}
        for c in meta_cols:
            data[c] = meta_dict[c].copy()
        data["x"] = x_sb_all[i]
        data["y"] = y_sb_all[i]
        data["velocity_x"] = vx_sb_all[i]
        data["velocity_y"] = vy_sb_all[i]
        data["augmentation"] = np.full(n_players, f"jitter_{i}", dtype=object)
        data["jitter_seed"] = np.full(n_players, int(seed_values[i]), dtype=np.int64)
        results.append(pd.DataFrame(data))

    return results


# ---------------------------------------------------------------------------
# Full augmentation pipeline: symmetry x jitter
# ---------------------------------------------------------------------------


def augment_full(
    df: pd.DataFrame,
    sym_config: AugmentationConfig,
    pert_config: PerturbationConfig,
    rng: np.random.Generator,
) -> list[pd.DataFrame]:
    """Compose symmetry augmentation (8x) with position jitter (Nx).

    Produces ``8 * (1 + N)`` total variants: 8 un-jittered symmetry variants
    plus 8*N jittered copies.

    Symmetry operates in StatsBomb 120x80 coordinates.  Jitter operates in
    centered meter-space.  Coordinate conversion is handled internally.

    Args:
        df: Tracking-frame DataFrame in StatsBomb 120x80 coordinates.
        sym_config: Configuration for symmetry augmentation.
        pert_config: Configuration for position jitter.
        rng: NumPy random number generator for reproducibility.

    Returns:
        List of ``8 * (1 + pert_config.n_perturbations)`` DataFrames,
        all in StatsBomb 120x80 coordinates.
    """
    # Generate 8 symmetry variants (including original)
    symmetry_variants = augment_tracking_frame(df, sym_config, include_original=True)

    results: list[pd.DataFrame] = []

    for variant in symmetry_variants:
        sym_label = str(variant["augmentation"].iloc[0])

        # Append the un-jittered symmetry variant
        results.append(variant)

        # Generate jittered copies of this symmetry variant
        jittered = perturb_positions(variant, pert_config, rng)
        for jittered_df in jittered:
            # Combine augmentation labels: "original+jitter_0", "h_flip+jitter_3", etc.
            jitter_label = str(jittered_df["augmentation"].iloc[0])
            jittered_df["augmentation"] = f"{sym_label}+{jitter_label}"
            results.append(jittered_df)

    return results
