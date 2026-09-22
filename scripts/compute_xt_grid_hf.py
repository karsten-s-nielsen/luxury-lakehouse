# /// script
# requires-python = ">=3.10,<3.11"
# dependencies = [
#     "luxury-lakehouse[spadl] @ https://huggingface.co/luxury-lakehouse/build-artifacts/resolve/main/luxury_lakehouse-0.5.112-py3-none-any.whl",
#     "numpy>=1.24",
#     "pandas>=2.0",
#     "pyarrow>=14.0",
#     "huggingface-hub>=1.5.0",
#     "mlflow-skinny>=2.17.0",
# ]
# ///
"""Compute Expected Threat (xT) grids on HuggingFace Jobs (CPU).

ExT-v2 single-canonical-surface (lakehouse ADR-085): fits a silly-kicks ``ExpectedThreat`` (16x12) per
competition + global, and publishes each fitted model's ``to_dict()`` JSON to HF Hub (mirroring the
bronze ``expected_threat_grids`` surface the live Databricks producer writes). The ``[spadl]`` wheel
extra pulls silly-kicks 4.121.0 (the ADR-100 serialize seam). Reuses the live producer's fit +
name->id mapping helpers (``ingestion.expected_threat``) and the ADR-063 guards
(``analytics.xt_grid_guards``) so the HF and Databricks paths stay identical.

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
from pathlib import Path

import numpy as np
import pandas as pd

from ingestion.hf_jobs_cost import HF_RATE_CPU_BASIC, HFJobsCostRecorder
from workflows import workflow

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
HF_ORG = "luxury-lakehouse"
SPADL_DATASET = f"{HF_ORG}/spadl-vaep-action-values"
OUTPUT_DATASET = f"{HF_ORG}/expected-threat-grids"

# Canonical single-surface grid resolution (sk default; matches ingestion.expected_threat._XT_L/_XT_W).
_XT_L = 16
_XT_W = 12
_PITCH_LENGTH = 105.0
_PITCH_WIDTH = 68.0

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


def _normalize_attack_direction(df: pd.DataFrame) -> pd.DataFrame:
    """No-op (ADR-063): the lakehouse SPADL is canonically LTR (ADR-022) — shots are 99.8-100%
    in the attacking half across every provider/period, so no orientation normalization is needed.

    The legacy shot-cluster + per-period "teams swap sides" inference actively CORRUPTED already-LTR
    data. Retained as a documented no-op (this HF script is NOT the live bronze writer —
    ``ingestion.expected_threat`` is — but keep both paths orientation-safe). See ADR-063.
    """
    return df


def _global_physical_grid_long(global_model: object) -> pd.DataFrame:
    """Long-format (zone_x, zone_y, xt_value) of the global model's PHYSICALLY-oriented 16x12 grid.

    Kept for the human-readable ``xt_grid_global.csv`` viewer artifact (the canonical payload is the
    ``to_dict`` JSON). Sampled through the ``physical_grid`` seam (ADR-041 orientation-neutralised).
    """
    from silly_kicks.xthreat import physical_grid

    cell_x = _PITCH_LENGTH / _XT_L
    cell_y = _PITCH_WIDTH / _XT_W
    xs = np.arange(_XT_L, dtype=np.float64) * cell_x + 0.5 * cell_x
    ys = np.arange(_XT_W, dtype=np.float64) * cell_y + 0.5 * cell_y
    grid = np.asarray(physical_grid(global_model, xs, ys), dtype=np.float64)  # (n_y, n_x)
    rows = [
        {"zone_x": int(xi), "zone_y": int(yi), "xt_value": round(float(grid[yi, xi]), 5)}
        for xi in range(_XT_L)
        for yi in range(_XT_W)
    ]
    return pd.DataFrame(rows)


# ---------------------------------------------------------------------------
# Main pipeline
# ---------------------------------------------------------------------------


@workflow("wf-xt-grids", phase="grid_computation")
def main() -> None:
    """Download SPADL actions, fit sk ExpectedThreat per competition + global, publish to HF Hub."""
    from huggingface_hub import HfApi, get_token, hf_hub_download

    from analytics.xt_grid_guards import assert_directional, validate_structural
    from ingestion.expected_threat import _fit_sk_grid

    hf_token = os.environ.get("HF_TOKEN", "") or (get_token() or "")
    if not hf_token:
        raise RuntimeError("HF_TOKEN environment variable required")

    api = HfApi(token=hf_token)

    recorder = HFJobsCostRecorder(
        workflow_id="wf-xt-grids",
        phase="grid_computation",
        rate_usd_per_hour=HF_RATE_CPU_BASIC,
        repo_id=OUTPUT_DATASET,
    )
    recorder.start()

    # ------------------------------------------------------------------
    # 1. Load SPADL data from HF Hub
    # ------------------------------------------------------------------
    print("=== Loading SPADL actions from HF Hub ===")

    all_items = list(api.list_repo_tree(SPADL_DATASET, repo_type="dataset", recursive=True))
    parquet_files = [f.path for f in all_items if hasattr(f, "size") and f.path.endswith("/data.parquet")]
    if not parquet_files:
        parquet_files = [f.path for f in all_items if hasattr(f, "size") and f.path.endswith(".parquet")]

    print(f"  Downloading {len(parquet_files)} parquet files...")
    dfs: list[pd.DataFrame] = []
    for pf in parquet_files:
        local = hf_hub_download(SPADL_DATASET, pf, repo_type="dataset", token=hf_token)
        df = pd.read_parquet(local)
        if "data_source=" in pf:
            ds = pf.split("data_source=")[1].split("/")[0]
            df["data_source"] = ds
        dfs.append(df)
        print(f"    {pf}: {len(df):,} rows")

    all_actions = pd.concat(dfs, ignore_index=True)

    _dataset_info = api.repo_info(repo_id=SPADL_DATASET, repo_type="dataset")
    _dataset_commit = _dataset_info.sha

    if "action_value_id" in all_actions.columns:
        before = len(all_actions)
        all_actions = all_actions.drop_duplicates(subset=["action_value_id"])
        if len(all_actions) < before:
            print(f"  Deduplicated: {before:,} -> {len(all_actions):,} rows")

    print(f"  Total actions: {len(all_actions):,}")

    all_actions = all_actions[all_actions["action_type"].isin(_RELEVANT_TYPES)].copy()
    all_actions = all_actions.rename(columns={"action_type": "type_name", "action_result": "result_name"})
    print(f"  xT-relevant actions: {len(all_actions):,}")

    if len(all_actions) < 1000:
        raise ValueError(f"Too few actions ({len(all_actions)}) for meaningful xT computation")

    all_actions = _normalize_attack_direction(all_actions)

    # ------------------------------------------------------------------
    # 2. Fit per-competition models (persist each as to_dict JSON)
    # ------------------------------------------------------------------
    print("\n=== Fitting per-competition xT models ===")
    competitions = sorted(all_actions["competition_id"].dropna().unique())
    print(f"  {len(competitions)} competitions found")

    grid_rows: list[dict[str, object]] = []
    for comp_id in competitions:
        comp_actions = all_actions[all_actions["competition_id"] == comp_id]
        n_events = len(comp_actions)
        if n_events < 100:
            print(f"  Competition {comp_id}: {n_events} events -- skipping (too few)")
            continue
        model = _fit_sk_grid(comp_actions)
        grid_rows.append(
            {
                "competition_id": str(comp_id),
                "xt_model_json": json.dumps(model.to_dict()),
                "format_version": 1,
            }
        )
        print(f"  Competition {comp_id}: {n_events:,} events, max xT={float(np.asarray(model.xT).max()):.5f}")

    # ------------------------------------------------------------------
    # 3. Global model (all competitions combined) + ADR-063 hard gate
    # ------------------------------------------------------------------
    print("\n=== Fitting global xT model ===")
    global_model = _fit_sk_grid(all_actions)
    global_xt = np.asarray(global_model.xT, dtype=np.float64)
    validate_structural(global_xt, max_value=0.50)
    assert_directional(global_model, competition_id="global")
    grid_rows.append(
        {
            "competition_id": "global",
            "xt_model_json": json.dumps(global_model.to_dict()),
            "format_version": 1,
        }
    )
    print(f"  Global: {len(all_actions):,} events, max xT={float(global_xt.max()):.5f}")
    print(f"  Range: {float(global_xt.min()):.5f} to {float(global_xt.max()):.5f}")

    # ------------------------------------------------------------------
    # 3b. Log to MLflow for provenance
    # ------------------------------------------------------------------
    tracking_uri = os.environ.get("MLFLOW_TRACKING_URI", "")
    if tracking_uri:
        import mlflow

        print("\n=== Logging xT model to MLflow ===")
        mlflow.set_tracking_uri(tracking_uri)
        mlflow.set_experiment("/soccer_analytics/expected_threat")

        with mlflow.start_run(run_name="xt_grid_computation"):
            mlflow.log_params(
                {
                    "n_zones_x": _XT_L,
                    "n_zones_y": _XT_W,
                    "pitch_length": _PITCH_LENGTH,
                    "pitch_width": _PITCH_WIDTH,
                    "n_competitions": len(grid_rows) - 1,
                    "total_actions": len(all_actions),
                    "training_env": "hf_jobs_cpu",
                    "silly_kicks_serialize_format_version": 1,
                }
            )
            mlflow.log_param("spadl_vaep_action_values_commit", _dataset_commit)
            mlflow.log_metrics(
                {
                    "global_max_xt": float(global_xt.max()),
                    "global_min_xt": float(global_xt.min()),
                    "global_range": float(global_xt.max() - global_xt.min()),
                }
            )

            with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as grid_f:
                json.dump(global_model.to_dict(), grid_f, indent=2)
                grid_artifact_path = grid_f.name
            mlflow.log_artifact(grid_artifact_path, "xt_model")
            os.unlink(grid_artifact_path)

        print("  xT model logged to MLflow")
    else:
        print("\n=== MLflow skipped (MLFLOW_TRACKING_URI not set) ===")

    # ------------------------------------------------------------------
    # 4. Publish to HF Hub
    # ------------------------------------------------------------------
    print("\n=== Publishing to HF Hub ===")
    grids_df = pd.DataFrame(grid_rows)  # canonical payload: one to_dict row per competition + global

    with tempfile.TemporaryDirectory() as tmpdir:
        data_dir = Path(tmpdir) / "data"
        data_dir.mkdir()

        # Canonical: fitted models as to_dict JSON (mirrors bronze expected_threat_grids).
        grids_df.to_parquet(str(data_dir / "grids.parquet"), index=False)

        # Human-readable viewer/seed artifact: the global grid in physical long format.
        _global_physical_grid_long(global_model).to_csv(str(data_dir / "xt_grid_global.csv"), index=False)

        metadata: dict[str, object] = {
            "grid": {
                "n_zones_x": _XT_L,
                "n_zones_y": _XT_W,
                "pitch_length": _PITCH_LENGTH,
                "pitch_width": _PITCH_WIDTH,
            },
            "serialize_format_version": 1,
            "payload": "silly-kicks ExpectedThreat.to_dict() per competition + global (ADR-085/ADR-100)",
            "competitions": [str(c) for c in competitions],
            "n_competitions_computed": len(grid_rows) - 1,  # exclude global
            "total_actions": len(all_actions),
            "global_max_xt": float(global_xt.max()),
            "global_min_xt": float(global_xt.min()),
        }
        metadata = recorder.complete(metadata, row_count=len(grids_df))
        with open(str(Path(tmpdir) / "metadata.json"), "w") as f:
            json.dump(metadata, f, indent=2)

        # Data payload + README from the in-repo source of truth (shared helper — no inline README).
        from ingestion.hf_publish import get_hf_card_path, upload_hf_readme

        api.create_repo(OUTPUT_DATASET, repo_type="dataset", exist_ok=True, token=hf_token)
        api.upload_folder(
            folder_path=tmpdir,
            repo_id=OUTPUT_DATASET,
            repo_type="dataset",
            token=hf_token,
        )
        readme_result = upload_hf_readme(
            repo_id=OUTPUT_DATASET,
            readme_path=get_hf_card_path("expected-threat-grids.md", kind="dataset"),
            hf_token=hf_token,
        )
        print(f"  Uploaded README: {readme_result['commit_url']} (sha256={readme_result['sha256'][:8]})")

    print(f"\n  Published: https://huggingface.co/datasets/{OUTPUT_DATASET}")
    print(f"  Competitions: {len(grid_rows) - 1}")
    print(f"  Global max xT: {float(global_xt.max()):.5f}")
    print("xT grid computation complete!")


if __name__ == "__main__":
    main()
