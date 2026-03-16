# /// script
# requires-python = ">=3.10"
# dependencies = [
#     "numpy>=1.24",
#     "pandas>=2.0",
#     "pyarrow>=14.0",
#     "huggingface-hub>=0.25.0",
#     "mlflow>=2.17.0",
# ]
# ///
"""Compute Expected Threat (xT) grids on HuggingFace Jobs (CPU).

Downloads SPADL action data from HF Dataset, normalizes coordinate orientation,
computes per-competition + global xT grids via Markov chain value iteration,
and publishes results to HF Hub.

Reference: Karun Singh (2018) "Introducing Expected Threat (xT)"

Usage (HF Jobs CLI):
    hf jobs uv run scripts/compute_xt_grid_hf.py \
        --flavor cpu-basic --timeout 30m \
        --secrets HF_TOKEN=$HF_TOKEN
"""

from __future__ import annotations

import json
import os
import tempfile
from dataclasses import dataclass
from pathlib import Path

import numpy as np
import pandas as pd

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
HF_ORG = "luxury-lakehouse"
SPADL_DATASET = f"{HF_ORG}/spadl-vaep-action-values"
OUTPUT_DATASET = f"{HF_ORG}/expected-threat-grids"


@dataclass(frozen=True)
class ExpectedThreatParams:
    """Configuration for xT grid computation (mirrors src/analytics/expected_threat.py)."""

    n_zones_x: int = 12
    n_zones_y: int = 8
    pitch_length: float = 105.0  # SPADL coordinates
    pitch_width: float = 68.0
    max_iterations: int = 100
    tolerance: float = 1e-5


# SPADL action types relevant to xT
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
_RELEVANT_TYPES = _MOVE_TYPES | _SHOT_TYPES


# ---------------------------------------------------------------------------
# Coordinate normalization
# ---------------------------------------------------------------------------


def _normalize_attack_direction(df: pd.DataFrame, params: ExpectedThreatParams) -> pd.DataFrame:
    """Normalize coordinates so all actions attack toward x=pitch_length.

    SPADL coordinates should have x increasing in the attacking direction, but
    some data exports (particularly Wyscout) have inconsistent orientation where
    approximately half the actions have flipped coordinates.

    Fix: for each (match_id, team_id, period), check if shots cluster in the
    defensive half (x < pitch_length/2). If so, flip all coordinates for that group.
    For groups without shots, infer from the same team's other period (teams swap
    sides each half).
    """
    pitch_l = params.pitch_length
    pitch_w = params.pitch_width
    midfield = pitch_l / 2.0

    df = df.copy()

    # Identify shots
    is_shot = df["type_name"].isin(_SHOT_TYPES)
    shots = df[is_shot]

    if shots.empty:
        return df

    # Compute mean shot x per (match_id, team_id, period)
    shot_means = shots.groupby(["match_id", "team_id", "period"])["start_x"].mean()

    # Build flip lookup: True if shots are in defensive half
    flip_lookup: dict[tuple, bool] = {}
    for key, mean_x in shot_means.items():
        flip_lookup[key] = mean_x < midfield  # type: ignore[index]

    # For groups without shots, infer from the same team's other period
    group_keys = set(df.groupby(["match_id", "team_id", "period"]).groups.keys())

    for key in group_keys:
        if key not in flip_lookup:
            match_id, team_id, period = key  # type: ignore[misc]
            # Try the other period (1↔2)
            other_period = 2 if period == 1 else 1
            other_key = (match_id, team_id, other_period)
            if other_key in flip_lookup:
                # Opposite of the other period (teams swap sides)
                flip_lookup[key] = not flip_lookup[other_key]
            # If neither period has shots, don't flip (assume correct)

    # Count flips for logging
    n_flip = sum(1 for v in flip_lookup.values() if v)
    n_total = len(flip_lookup)
    print(f"  Coordinate normalization: {n_flip}/{n_total} team-period groups flipped")

    if n_flip == 0:
        return df

    # Apply flips vectorized: build a flip mask
    flip_mask = np.zeros(len(df), dtype=bool)
    for key, should_flip in flip_lookup.items():
        if should_flip:
            match_id, team_id, period = key  # type: ignore[misc]
            group_mask = (df["match_id"] == match_id) & (df["team_id"] == team_id) & (df["period"] == period)
            flip_mask |= group_mask.values

    # Flip coordinates where needed
    for col in ["start_x", "end_x"]:
        if col in df.columns:
            df.loc[flip_mask, col] = pitch_l - df.loc[flip_mask, col]
    for col in ["start_y", "end_y"]:
        if col in df.columns:
            df.loc[flip_mask, col] = pitch_w - df.loc[flip_mask, col]

    # Verify: shots should now cluster in attacking half
    post_shots = df[is_shot]
    post_mean = post_shots["start_x"].mean()
    post_pct_attacking = (post_shots["start_x"] > midfield).mean() * 100
    print(f"  Post-normalization: shot mean x={post_mean:.1f}, {post_pct_attacking:.0f}% in attacking half")

    return df


# ---------------------------------------------------------------------------
# xT computation (inlined from src/analytics/expected_threat.py)
# ---------------------------------------------------------------------------


def _assign_zones(x: np.ndarray, y: np.ndarray, params: ExpectedThreatParams) -> np.ndarray:
    """Map (x, y) coordinates to flat zone indices."""
    zone_x = np.clip((x / params.pitch_length * params.n_zones_x).astype(int), 0, params.n_zones_x - 1)
    zone_y = np.clip((y / params.pitch_width * params.n_zones_y).astype(int), 0, params.n_zones_y - 1)
    return zone_x * params.n_zones_y + zone_y


def _build_transition_matrix(start_zones: np.ndarray, end_zones: np.ndarray, n_zones: int) -> np.ndarray:
    """Build row-normalized transition matrix from zone-to-zone moves."""
    transition = np.zeros((n_zones, n_zones), dtype=np.float64)
    np.add.at(transition, (start_zones, end_zones), 1.0)
    row_sums = np.maximum(transition.sum(axis=1, keepdims=True), 1.0)
    return transition / row_sums


def _value_iteration(
    shot_prob: np.ndarray,
    goal_prob: np.ndarray,
    move_prob: np.ndarray,
    transition: np.ndarray,
    max_iters: int,
    tol: float,
) -> tuple[np.ndarray, int]:
    """NumPy value iteration for xT."""
    xt = np.zeros_like(shot_prob)
    for i in range(max_iters):
        xt_new = shot_prob * goal_prob + move_prob * (transition @ xt)
        delta = float(np.max(np.abs(xt_new - xt)))
        xt = xt_new
        if delta < tol:
            return xt, i + 1
    return xt, max_iters


def compute_xt_grid(actions_df: pd.DataFrame, params: ExpectedThreatParams) -> np.ndarray:
    """Compute xT grid from SPADL action data via Markov chain value iteration."""
    n_zones = params.n_zones_x * params.n_zones_y

    type_names = actions_df["type_name"].values
    result_names = actions_df["result_name"].values
    is_move = np.array([t in _MOVE_TYPES for t in type_names], dtype=bool)
    is_shot = np.array([t in _SHOT_TYPES for t in type_names], dtype=bool)
    is_success = result_names == "success"

    start_zones = _assign_zones(
        np.asarray(actions_df["start_x"], dtype=np.float64),
        np.asarray(actions_df["start_y"], dtype=np.float64),
        params,
    )
    end_zones = _assign_zones(
        np.asarray(actions_df["end_x"], dtype=np.float64),
        np.asarray(actions_df["end_y"], dtype=np.float64),
        params,
    )

    total_per_zone = np.bincount(start_zones, minlength=n_zones).astype(np.float64)
    shots_per_zone = np.bincount(start_zones[is_shot], minlength=n_zones).astype(np.float64)
    goals_per_zone = np.bincount(start_zones[is_shot & is_success], minlength=n_zones).astype(np.float64)
    succ_moves_per_zone = np.bincount(start_zones[is_move & is_success], minlength=n_zones).astype(np.float64)

    safe_total = np.maximum(total_per_zone, 1.0)
    shot_prob = shots_per_zone / safe_total
    goal_prob = np.where(shots_per_zone > 0, goals_per_zone / shots_per_zone, 0.0)
    # Use successful move probability (not 1-shot_prob) — failed moves lose
    # possession and contribute xT=0, which is handled implicitly by the lower
    # multiplier on the transition term.
    succ_move_prob = succ_moves_per_zone / safe_total

    transition = _build_transition_matrix(
        start_zones[is_move & is_success],
        end_zones[is_move & is_success],
        n_zones,
    )

    xt_flat, iters = _value_iteration(
        shot_prob,
        goal_prob,
        succ_move_prob,
        transition,
        params.max_iterations,
        params.tolerance,
    )
    print(f"  Converged in {iters} iterations")
    return xt_flat.reshape(params.n_zones_x, params.n_zones_y)


def grid_to_dataframe(grid: np.ndarray, competition_id: str) -> pd.DataFrame:
    """Convert xT grid array to DataFrame matching Delta table schema."""
    n_x, n_y = grid.shape
    rows: list[dict[str, object]] = []
    for zx in range(n_x):
        for zy in range(n_y):
            rows.append(
                {
                    "zone_x": zx,
                    "zone_y": zy,
                    "xt_value": round(float(grid[zx, zy]), 5),
                    "competition_id": competition_id,
                }
            )
    return pd.DataFrame(rows)


def validate_xt_grid(grid: np.ndarray) -> None:
    """Validate computed xT grid meets data quality requirements."""
    xt_range = grid.max() - grid.min()
    if xt_range < 0.05:
        msg = f"Grid range too narrow ({xt_range:.4f}) — likely coordinate orientation issue"
        raise ValueError(msg)
    if grid.max() > 0.50:
        msg = f"Grid max too high: {grid.max():.4f}"
        raise ValueError(msg)
    row_means = grid.mean(axis=1)
    # xT should generally increase toward the attacking goal (zone_x=11)
    # Allow small decreases (-0.005) but overall trend must be increasing
    if row_means[-1] < row_means[0]:
        msg = f"Grid not increasing toward goal: zone_x=0 mean={row_means[0]:.5f} > zone_x=11 mean={row_means[-1]:.5f}"
        raise ValueError(msg)


# ---------------------------------------------------------------------------
# Main pipeline
# ---------------------------------------------------------------------------


def main() -> None:
    """Download SPADL actions, normalize coordinates, compute xT grids, publish to HF Hub."""
    from huggingface_hub import HfApi, get_token, hf_hub_download

    hf_token = os.environ.get("HF_TOKEN", "") or (get_token() or "")
    if not hf_token:
        raise RuntimeError("HF_TOKEN environment variable required")

    api = HfApi(token=hf_token)
    params = ExpectedThreatParams()

    # ------------------------------------------------------------------
    # 1. Load SPADL data from HF Hub
    # ------------------------------------------------------------------
    print("=== Loading SPADL actions from HF Hub ===")

    # Find parquet files — use only data.parquet (HF viewer canonical files),
    # skip part-* files which are duplicates of the same data
    all_items = list(api.list_repo_tree(SPADL_DATASET, repo_type="dataset", recursive=True))
    parquet_files = [f.path for f in all_items if hasattr(f, "size") and f.path.endswith("/data.parquet")]
    # Fall back: if no data.parquet found, use all parquet files
    if not parquet_files:
        parquet_files = [f.path for f in all_items if hasattr(f, "size") and f.path.endswith(".parquet")]

    print(f"  Downloading {len(parquet_files)} parquet files...")
    dfs: list[pd.DataFrame] = []
    for pf in parquet_files:
        local = hf_hub_download(SPADL_DATASET, pf, repo_type="dataset", token=hf_token)
        df = pd.read_parquet(local)
        # Extract data_source from Hive partition path
        if "data_source=" in pf:
            ds = pf.split("data_source=")[1].split("/")[0]
            df["data_source"] = ds
        dfs.append(df)
        print(f"    {pf}: {len(df):,} rows")

    all_actions = pd.concat(dfs, ignore_index=True)

    # Deduplicate in case of overlapping exports
    if "action_value_id" in all_actions.columns:
        before = len(all_actions)
        all_actions = all_actions.drop_duplicates(subset=["action_value_id"])
        if len(all_actions) < before:
            print(f"  Deduplicated: {before:,} -> {len(all_actions):,} rows")

    print(f"  Total actions: {len(all_actions):,}")

    # Filter to xT-relevant types and rename columns
    all_actions = all_actions[all_actions["action_type"].isin(_RELEVANT_TYPES)].copy()
    all_actions = all_actions.rename(columns={"action_type": "type_name", "action_result": "result_name"})
    print(f"  xT-relevant actions: {len(all_actions):,}")

    if len(all_actions) < 1000:
        raise ValueError(f"Too few actions ({len(all_actions)}) for meaningful xT computation")

    # ------------------------------------------------------------------
    # 1b. Normalize coordinate orientation
    # ------------------------------------------------------------------
    print("\n=== Normalizing coordinate orientation ===")
    all_actions = _normalize_attack_direction(all_actions, params)

    # ------------------------------------------------------------------
    # 2. Compute per-competition grids
    # ------------------------------------------------------------------
    print("\n=== Computing per-competition xT grids ===")
    competitions = sorted(all_actions["competition_id"].dropna().unique())
    print(f"  {len(competitions)} competitions found")

    all_grids: list[pd.DataFrame] = []
    for comp_id in competitions:
        comp_actions = all_actions[all_actions["competition_id"] == comp_id]
        n_events = len(comp_actions)
        if n_events < 100:
            print(f"  Competition {comp_id}: {n_events} events -- skipping (too few)")
            continue
        grid = compute_xt_grid(comp_actions, params)
        grid_df = grid_to_dataframe(grid, str(comp_id))
        all_grids.append(grid_df)
        print(f"  Competition {comp_id}: {n_events:,} events, max xT={grid.max():.5f}")

    # ------------------------------------------------------------------
    # 3. Global grid (all competitions combined)
    # ------------------------------------------------------------------
    print("\n=== Computing global xT grid ===")
    global_grid = compute_xt_grid(all_actions, params)
    validate_xt_grid(global_grid)
    global_df = grid_to_dataframe(global_grid, "global")
    all_grids.append(global_df)
    print(f"  Global: {len(all_actions):,} events, max xT={global_grid.max():.5f}")

    # Print grid summary
    row_means = global_grid.mean(axis=1)
    print(f"  Zone x=0 (defense): {row_means[0]:.5f}")
    print(f"  Zone x=11 (attack): {row_means[-1]:.5f}")
    print(f"  Range: {global_grid.min():.5f} to {global_grid.max():.5f}")

    # ------------------------------------------------------------------
    # 3b. Log xT grid to MLflow as artifact for provenance
    # ------------------------------------------------------------------
    tracking_uri = os.environ.get("MLFLOW_TRACKING_URI", "")
    if tracking_uri:
        import mlflow

        print("\n=== Logging xT grid to MLflow ===")
        mlflow.set_tracking_uri(tracking_uri)
        mlflow.set_experiment("/soccer_analytics/expected_threat")

        with mlflow.start_run(run_name="xt_grid_computation"):
            mlflow.log_params(
                {
                    "n_zones_x": params.n_zones_x,
                    "n_zones_y": params.n_zones_y,
                    "pitch_length": params.pitch_length,
                    "pitch_width": params.pitch_width,
                    "max_iterations": params.max_iterations,
                    "tolerance": params.tolerance,
                    "n_competitions": len(all_grids) - 1,
                    "total_actions": len(all_actions),
                    "training_env": "hf_jobs_cpu",
                }
            )
            mlflow.log_metrics(
                {
                    "global_max_xt": float(global_grid.max()),
                    "global_min_xt": float(global_grid.min()),
                    "global_range": float(global_grid.max() - global_grid.min()),
                }
            )

            # Save grid as temporary JSON artifact
            with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as grid_f:
                grid_json = {
                    "shape": list(global_grid.shape),
                    "values": global_grid.tolist(),
                    "zone_x_labels": list(range(params.n_zones_x)),
                    "zone_y_labels": list(range(params.n_zones_y)),
                }
                json.dump(grid_json, grid_f, indent=2)
                grid_artifact_path = grid_f.name
            mlflow.log_artifact(grid_artifact_path, "xt_grid")
            os.unlink(grid_artifact_path)

        print("  xT grid logged to MLflow")
    else:
        print("\n=== MLflow skipped (MLFLOW_TRACKING_URI not set) ===")

    # ------------------------------------------------------------------
    # 4. Publish to HF Hub
    # ------------------------------------------------------------------
    print("\n=== Publishing to HF Hub ===")
    combined_df = pd.concat(all_grids, ignore_index=True)

    with tempfile.TemporaryDirectory() as tmpdir:
        data_dir = Path(tmpdir) / "data"
        data_dir.mkdir()

        # Save all grids as parquet (long format)
        combined_df.to_parquet(str(data_dir / "grids.parquet"), index=False)

        # Save global grid as CSV in dbt seed format (long: zone_x, zone_y, xt_value)
        global_seed = global_df[["zone_x", "zone_y", "xt_value"]].copy()
        global_seed.to_csv(str(data_dir / "xt_grid_global.csv"), index=False)

        # Save metadata
        metadata = {
            "params": {
                "n_zones_x": params.n_zones_x,
                "n_zones_y": params.n_zones_y,
                "pitch_length": params.pitch_length,
                "pitch_width": params.pitch_width,
                "max_iterations": params.max_iterations,
                "tolerance": params.tolerance,
            },
            "competitions": [str(c) for c in competitions],
            "n_competitions_computed": len(all_grids) - 1,  # exclude global
            "total_actions": len(all_actions),
            "global_max_xt": float(global_grid.max()),
            "global_min_xt": float(global_grid.min()),
        }
        with open(str(Path(tmpdir) / "metadata.json"), "w") as f:
            json.dump(metadata, f, indent=2)

        # Dataset card
        card = """---
license: mit
tags:
  - soccer
  - football
  - expected-threat
  - xT
  - analytics
size_categories:
  - n<1K
---

# Expected Threat (xT) Grids

Data-driven xT grids computed via Markov chain value iteration from SPADL action data.

## Method

Karun Singh (2018) "Introducing Expected Threat (xT)" — 12x8 grid, SPADL 105x68m coordinates.
Value iteration converges to the probability that possession starting in each zone leads to a goal.

## Contents

- `data/grids.parquet` — All grids (per-competition + global) in long format
- `data/xt_grid_global.csv` — Global grid CSV (dbt seed format)
- `metadata.json` — Computation parameters and statistics

## Columns (grids.parquet)

| Column | Type | Description |
|--------|------|-------------|
| zone_x | int | X zone index (0-11, attacking direction) |
| zone_y | int | Y zone index (0-7, pitch width) |
| xt_value | float | Expected threat value |
| competition_id | string | Competition ID or "global" |

## Usage

```python
import pandas as pd
from huggingface_hub import hf_hub_download

path = hf_hub_download("luxury-lakehouse/expected-threat-grids", "data/grids.parquet", repo_type="dataset")
grids = pd.read_parquet(path)
global_grid = grids[grids["competition_id"] == "global"]
```

## License

MIT — derived from StatsBomb and Wyscout open data via SPADL.
"""
        with open(str(Path(tmpdir) / "README.md"), "w") as f:
            f.write(card)

        api.create_repo(OUTPUT_DATASET, repo_type="dataset", exist_ok=True, token=hf_token)
        api.upload_folder(
            folder_path=tmpdir,
            repo_id=OUTPUT_DATASET,
            repo_type="dataset",
            token=hf_token,
        )

    print(f"\n  Published: https://huggingface.co/datasets/{OUTPUT_DATASET}")
    print(f"  Competitions: {len(all_grids) - 1}")
    print(f"  Global max xT: {global_grid.max():.5f}")
    print("xT grid computation complete!")


if __name__ == "__main__":
    main()
