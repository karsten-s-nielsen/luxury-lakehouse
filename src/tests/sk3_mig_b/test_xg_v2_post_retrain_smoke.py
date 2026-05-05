# ruff: noqa: S608 — SQL built from gold_schema fixture + module-level _EVAL_FOLD_SIZE; no user input.
"""Post-retrain smoke gate for xG v2.

Spec §3 acceptance criteria (canonical reference for the trained-model gate pattern):
- Calibration: held-out ECE < 0.05 against the StatsBomb shots-on-target eval fold.
- Bounds: 100% predictions in [0, 1].
- CI band: xg_ci_upper - xg_ci_lower median > 0 (MC dropout actually firing).
- Envelope: feature_names + tabular_dim present in the @Champion weights bundle
  (ADR-012 §2 enforced; v2->v1 fallback already removed in SK3-MIG-A).

Failure halts orchestrator (spec §5.2) before Lakebase synced refresh.
Restoration: revert to prior Champion via
  set_and_verify_mlflow_champion("xg_model_v2", version=PRIOR_VERSION)

Eval-fold strategy (Phase 0.5 finding override):
The plan §2.2 used `WHERE match_id IN (...)` but fct_xg_predictions_v2 has no
match_id column (only match_key). We use a deterministic ORDER BY shot_id LIMIT N
slice instead — stable across retrains, no dim_matches dependency.
"""

from __future__ import annotations

import json
from typing import TYPE_CHECKING, Any

import numpy as np
import pytest

if TYPE_CHECKING:
    from databricks.sdk import WorkspaceClient

from src.tests.sk3_mig_b.conftest import execute_sql

# Eval fold size — large enough for stable ECE estimation, small enough for
# fast smoke-gate dispatch. ~1k shots covers ~50 matches at typical 20 shots/match.
_EVAL_FOLD_SIZE = 1000


@pytest.fixture(scope="module")
def champion_envelope_features(
    workspace_client: WorkspaceClient,
    catalog: str,
) -> dict[str, Any]:
    """Read v2 Champion weights envelope from UC Volume + extract feature_names."""
    volume_path = f"/Volumes/{catalog}/dev_gold/model_weights/xg_model_v2"
    files_api = workspace_client.files

    envelope_candidates = ["envelope.json", "weights_envelope.json"]
    envelope_bytes: bytes | None = None
    for fname in envelope_candidates:
        try:
            response = files_api.download(f"{volume_path}/{fname}")
            envelope_bytes = response.contents.read() if response.contents else None
            if envelope_bytes:
                break
        except Exception as exc:
            # S112 — log on continue. Both candidate filenames may legitimately
            # fail the first time we run this gate; the post-loop None-check raises.
            print(f"[xg_v2_smoke] envelope candidate {fname!r} not readable: {exc}")
            continue

    if envelope_bytes is None:
        pytest.skip(
            f"Could not read v2 envelope from {volume_path}. "
            "Skipping until Phase 9 retrain runs upload_weights_to_uc_volume (ADR-012)."
        )

    return json.loads(envelope_bytes.decode("utf-8"))


def test_envelope_carries_feature_names(
    champion_envelope_features: dict[str, Any],
) -> None:
    """ADR-012 §2 grace-period closed in SK3-MIG-A — envelope MUST carry feature_names."""
    feature_names = champion_envelope_features.get("feature_names")
    assert feature_names is not None, (
        "v2 envelope is missing 'feature_names'. "
        "ADR-012 §2 grace-period was closed in SK3-MIG-A; "
        "envelope without feature_names is a regression. "
        "Verify scripts/train_xg_v2_hf.py emitted feature_names at training time."
    )
    assert isinstance(feature_names, list), f"feature_names must be a list, got {type(feature_names)}"
    assert len(feature_names) > 0, "feature_names is empty"


def test_envelope_tabular_dim_consistent(
    champion_envelope_features: dict[str, Any],
) -> None:
    """Defense-in-depth: feature_names length must equal tabular_dim."""
    feature_names = champion_envelope_features["feature_names"]
    tabular_dim = champion_envelope_features.get("tabular_dim")
    assert tabular_dim is not None, "tabular_dim missing from envelope (defense-in-depth check)"
    assert len(feature_names) == tabular_dim, (
        f"Envelope corrupted at training time: feature_names={len(feature_names)} != tabular_dim={tabular_dim}"
    )


def test_predictions_within_bounds(
    workspace_client: WorkspaceClient,
    warehouse_id: str,
    gold_schema: str,
) -> None:
    """100% predictions in [0, 1]. Spec §3 absolute bound."""
    sql = f"""
    SELECT
      COUNT(*) AS n_total,
      SUM(CASE WHEN xg_set_encoder < 0 OR xg_set_encoder > 1 THEN 1 ELSE 0 END) AS n_out_of_bounds,
      SUM(CASE WHEN xg_set_encoder IS NULL THEN 1 ELSE 0 END) AS n_null
    FROM (
      SELECT xg_set_encoder
      FROM {gold_schema}.fct_xg_predictions_v2
      ORDER BY shot_id
      LIMIT {_EVAL_FOLD_SIZE}
    )
    """
    rows = execute_sql(workspace_client, warehouse_id, sql)
    if not rows:
        pytest.skip("No rows returned — fct_xg_predictions_v2 empty; skip until Phase 9 retrain")
    n_total = int(rows[0][0])
    n_out = int(rows[0][1])
    n_null = int(rows[0][2])

    if n_total == 0:
        pytest.skip("Eval fold empty — skip until Phase 9 retrain")
    if n_null > 0:
        pytest.skip(
            f"{n_null} of {n_total} predictions are NULL — "
            "pre-retrain state; skip until Phase 9 retrain populates predictions"
        )
    assert n_out == 0, (
        f"{n_out} of {n_total} predictions outside [0, 1] — "
        "v2 retrain produced out-of-bounds output. Halt + investigate."
    )


def test_ci_band_active(
    workspace_client: WorkspaceClient,
    warehouse_id: str,
    gold_schema: str,
) -> None:
    """CI band median > 0 — MC dropout actually firing. Spec §3."""
    sql = f"""
    SELECT percentile_approx(xg_ci_upper - xg_ci_lower, 0.5) AS ci_band_median
    FROM (
      SELECT xg_ci_upper, xg_ci_lower
      FROM {gold_schema}.fct_xg_predictions_v2
      WHERE xg_ci_upper IS NOT NULL AND xg_ci_lower IS NOT NULL
      ORDER BY shot_id
      LIMIT {_EVAL_FOLD_SIZE}
    )
    """
    rows = execute_sql(workspace_client, warehouse_id, sql)
    if not rows or rows[0][0] is None:
        pytest.skip("CI band columns missing or all NULL — skip until Phase 9 retrain")
    ci_band_median = float(rows[0][0])
    assert ci_band_median > 0, (
        f"CI band median = {ci_band_median} — MC dropout produced zero-width CIs. "
        "Verify n_dropout_samples > 1 in train_xg_v2_hf.py + that dropout layers fire at inference."
    )


def test_held_out_ece_below_threshold(
    workspace_client: WorkspaceClient,
    warehouse_id: str,
    gold_schema: str,
) -> None:
    """Held-out ECE < 0.05 on the eval fold. Spec §3."""
    # Pull (predicted, actual) pairs from fct_xg_predictions_v2 joined with fct_shots.
    sql = f"""
    SELECT p.xg_set_encoder, CAST(s.is_goal AS DOUBLE) AS is_goal
    FROM (
      SELECT shot_id, xg_set_encoder
      FROM {gold_schema}.fct_xg_predictions_v2
      ORDER BY shot_id
      LIMIT {_EVAL_FOLD_SIZE}
    ) p
    INNER JOIN {gold_schema}.fct_shots s
      ON p.shot_id = s.shot_id
    """
    rows = execute_sql(workspace_client, warehouse_id, sql)
    if not rows:
        pytest.skip("No (prediction, actual) pairs available — skip until Phase 9 retrain")

    # Filter out NULL predictions (pre-retrain state)
    valid_rows = [(r[0], r[1]) for r in rows if r[0] is not None and r[1] is not None]
    if len(valid_rows) < len(rows) * 0.5:
        pytest.skip(
            f"Only {len(valid_rows)}/{len(rows)} predictions are non-NULL — "
            "pre-retrain state; skip until Phase 9 retrain populates predictions"
        )

    preds = np.array([float(r[0]) for r in valid_rows])
    actuals = np.array([float(r[1]) for r in valid_rows])

    # ECE: 10 equal-frequency bins; per-bin |mean_pred - mean_actual|; weighted.
    n_bins = 10
    bin_edges = np.quantile(preds, np.linspace(0, 1, n_bins + 1))
    bin_edges[-1] += 1e-9
    bin_indices = np.digitize(preds, bin_edges[1:-1])

    ece = 0.0
    for b in range(n_bins):
        mask = bin_indices == b
        if not mask.any():
            continue
        bin_pred = preds[mask].mean()
        bin_actual = actuals[mask].mean()
        ece += (mask.sum() / len(preds)) * abs(bin_pred - bin_actual)

    assert ece < 0.05, (
        f"Held-out ECE = {ece:.4f} > 0.05. Calibration regressed post-retrain. "
        "Halt and investigate before Lakebase synced refresh."
    )
