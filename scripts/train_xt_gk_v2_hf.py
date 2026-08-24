# /// script
# requires-python = ">=3.10,<3.11"
# dependencies = [
#     "luxury-lakehouse[spadl] @ https://huggingface.co/luxury-lakehouse/build-artifacts/resolve/main/luxury_lakehouse-0.5.102-py3-none-any.whl",
#     "numpy>=1.24",
#     "pandas>=2.0",
#     "pyarrow>=14.0",
#     "scikit-learn>=1.3.0",
#     "requests>=2.31",
#     "huggingface-hub>=1.5.0",
#     "mlflow>=2.17.0",
#     "databricks-sdk>=0.102.0",
# ]
# ///
"""Fit the xT-GK **v2** possession-value + turnover-cost surfaces on HuggingFace Jobs (CPU).

xT-GK v2 REPLACES the in-repo v1 ``add_xt_gk`` metric (spec §7.4). Only the ``retention`` model ships
bundled in the wheel (``GkRetentionModel.from_variant``); ``MarkovPossessionValue`` (possession value V)
and ``EmpiricalTurnoverValue`` (turnover cost V_opp) expose ``.fit()`` only, so v2 is a **fit-on-corpus
training sub-project** (ADR-012 trainer), not inline wiring. The fitted bundle feeds
``ingestion.xt_gk_v2_writer`` (ADR-013), which scores ``xt_gk_v2`` per GK-distribution action.

**Fit corpus (review-4 A2/A5 — precise, or ``.fit()`` RAISES):** AC-enriched actions joined to
``fct_shot_xg``, carrying on every row: non-null ``game_id`` (turnover ``.fit`` hard-raises on null —
ADR-017/019 match boundary), ``possession_id`` (else ``add_possessions`` runs), ``start_x/start_y`` +
``end_x/end_y``, ``type_id`` + ``result_id`` (``validate_possession_value_input``), a ``pressure`` column
(AC-layer ``pressure_on_actor__andrienko_oval`` — lives in ``spadl_action_context``, NOT
``fct_action_values``), and the xG column (left-joined from ``fct_shot_xg``; NaN for non-shot actions).

**Acyclicity (review-4 A1):** the corpus is the **v2-free** surface — ``bronze.spadl_action_context`` +
``bronze.spadl_actions`` + ``fct_shot_xg`` — NEVER the post-join ``fct_action_context`` mart (which
contains the writer's own v2 output).

**Delivery (ADR-012):** ``require_mlflow_env`` at the top of ``main()``; ``upload_weights_to_uc_volume``
for the single-JSON bundle (``MarkovPossessionValue.save`` writes a directory and
``EmpiricalTurnoverValue`` has no serializer, while the ADR-012 helper delivers a single ``.json`` — so
``ingestion.xt_gk_v2_writer.serialize_xt_gk_v2_bundle`` packs all three ports into one envelope);
``set_and_verify_mlflow_champion`` after registration (zombie-alias guard).

Usage (HF Jobs CLI) — secrets ENCRYPTED via ``--secrets`` (never ``--env``, ADR-012):

    hf jobs uv run scripts/train_xt_gk_v2_hf.py \\
        --flavor cpu-basic --timeout 60m \\
        --secrets HF_TOKEN=$HF_TOKEN \\
        --secrets DATABRICKS_CLIENT_ID=$DATABRICKS_CLIENT_ID \\
        --secrets DATABRICKS_CLIENT_SECRET=$DATABRICKS_CLIENT_SECRET \\
        --env MLFLOW_TRACKING_URI=$MLFLOW_TRACKING_URI \\
        --env DATABRICKS_HOST=$DATABRICKS_HOST \\
        --env DATABRICKS_SQL_WAREHOUSE_ID=$DATABRICKS_SQL_WAREHOUSE_ID
"""

from __future__ import annotations

import logging
import os
from typing import TYPE_CHECKING, Any

import pandas as pd

from analytics.databricks_sql_fetch import query_databricks_sql
from ingestion.artifact_deploy import (
    require_mlflow_env,
    set_and_verify_mlflow_champion,
    upload_weights_to_uc_volume,
)
from ingestion.xt_gk_v2_writer import (
    MODEL_NAME,
    PRESSURE_COLUMN,
    WEIGHTS_FILENAME,
    serialize_xt_gk_v2_bundle,
)
from workflows import workflow

if TYPE_CHECKING:  # pragma: no cover - typing only
    from silly_kicks.xtgk import EmpiricalTurnoverValue, MarkovPossessionValue, PressureLevels

# Validated HF Jobs flavor — single source of truth.
VALIDATED_HF_FLAVOR: str = "cpu-basic"

# uv silent-downgrade footgun (CLAUDE.md): a top-level silly-kicks pin in PEP 723 deps silently
# overrides the wheel's transitive pin, so we do NOT pin it and assert the runtime minimum instead.
_REQUIRED_SK_MIN: tuple[int, int, int] = (4, 90, 1)

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s — %(message)s")
logger = logging.getLogger(__name__)

CATALOG = "soccer_analytics"
SCHEMA = "dev_gold"
# The xG column read from fct_shot_xg (spec ADR-013; confirm the exact name against the live mart before
# the Part-B dispatch — the v3 pre-shot xG mart column).
XG_COLUMN = "xg"

# Fit corpus SQL (review-4 A1/A2). v2-FREE surface: bronze.spadl_actions (type_id/result_id/possession_id
# + full SPADL geometry) x bronze.spadl_action_context (AC-layer pressure) x fct_shot_xg (xG), NEVER the
# post-join fct_action_context mart. game_id = the native match id (non-null match boundary for the
# turnover scan). Aliased to the silly-kicks xtgk column names. Static template (no user input) — the
# pressure alias equals PRESSURE_COLUMN ("pressure") and the xG column is XG_COLUMN ("xg"); keep in sync.
_FIT_CORPUS_SQL = """
SELECT
    sa.match_id                            AS game_id,
    sa.period_id                           AS period_id,
    sa.action_id                           AS action_id,
    sa.team_id                             AS team_id,
    sa.time_seconds                        AS time_seconds,
    sa.type_id                             AS type_id,
    sa.result_id                           AS result_id,
    sa.possession_id                       AS possession_id,
    sa.start_x                             AS start_x,
    sa.start_y                             AS start_y,
    sa.end_x                               AS end_x,
    sa.end_y                               AS end_y,
    ac.pressure_on_actor__andrienko_oval   AS pressure,
    xg.xg                                  AS xg
FROM soccer_analytics.bronze.spadl_actions sa
JOIN soccer_analytics.bronze.spadl_action_context ac
    ON  ac.data_source = sa.data_source
    AND ac.match_id    = sa.match_id
    AND ac.action_id   = sa.action_id
LEFT JOIN soccer_analytics.dev_gold.dim_matches dm
    ON  dm.provider        = sa.data_source
    AND dm.native_match_id = sa.match_id
LEFT JOIN soccer_analytics.dev_gold.fct_shot_xg xg
    ON  xg.match_key = dm.match_key
    AND xg.action_id = sa.action_id
WHERE sa.match_id IS NOT NULL
  AND ac.pressure_on_actor__andrienko_oval IS NOT NULL
"""


def _assert_silly_kicks_min() -> None:
    import silly_kicks

    actual = tuple(int(p) for p in silly_kicks.__version__.split(".")[:3])
    if actual < _REQUIRED_SK_MIN:
        raise RuntimeError(
            f"silly-kicks {silly_kicks.__version__} < required "
            f"{'.'.join(str(p) for p in _REQUIRED_SK_MIN)} — refusing to fit xt_gk_v2."
        )


# ---------------------------------------------------------------------------
# Databricks SQL (Statement Execution API — no Spark on HF Jobs)
# ---------------------------------------------------------------------------


def load_fit_corpus(host: str, token: str, warehouse_id: str) -> pd.DataFrame:
    """Load the v2-free fit corpus (AC pressure x SPADL surface x fct_shot_xg)."""
    logger.info("Loading xt_gk_v2 fit corpus from Databricks")
    df = query_databricks_sql(host, token, _FIT_CORPUS_SQL, warehouse_id)
    logger.info("Fit corpus: %d rows", len(df))
    return df


# ---------------------------------------------------------------------------
# Pure fit (unit-tested with a synthetic corpus)
# ---------------------------------------------------------------------------


def fit_xt_gk_v2(
    actions: pd.DataFrame,
    *,
    xg_column: str = XG_COLUMN,
    pressure_column: str = PRESSURE_COLUMN,
) -> tuple[MarkovPossessionValue, EmpiricalTurnoverValue, PressureLevels]:
    """Fit the possession-value + turnover-cost surfaces on ``actions``; return ``(pv, tc, pl)``.

    ``PressureLevels`` is fit ONCE and shared by both surfaces and the metric so the terciles are
    self-consistent (v2's "never refit" guard). Rows with a NaN pressure are dropped: ``PressureLevels``
    fits on non-null pressure, but both ``.fit`` calls then ``apply`` it, which raises on any NaN.
    """
    from silly_kicks.xtgk import EmpiricalTurnoverValue, MarkovPossessionValue, PressureLevels

    a = actions.copy()
    a[pressure_column] = pd.to_numeric(a[pressure_column], errors="coerce")
    before = len(a)
    a = a[a[pressure_column].notna()].reset_index(drop=True)
    if len(a) < before:
        logger.info("Dropped %d rows with NaN pressure before fit", before - len(a))
    if "game_id" not in a.columns or a["game_id"].isna().any():
        raise ValueError("fit corpus must carry a non-null game_id on every row (turnover match boundary)")

    pl = PressureLevels().fit(a[pressure_column])
    logger.info("Fitting MarkovPossessionValue on %d actions (xg_column=%s)", len(a), xg_column)
    pv = MarkovPossessionValue().fit(a, xg_column=xg_column, pressure_column=pressure_column, pressure_levels=pl)
    logger.info("Fitting EmpiricalTurnoverValue")
    tc = EmpiricalTurnoverValue().fit(a, xg_column=xg_column, pressure_column=pressure_column, pressure_levels=pl)
    return pv, tc, pl


# ---------------------------------------------------------------------------
# Entry point (HF Jobs)
# ---------------------------------------------------------------------------


def _log_and_register_mlflow(envelope: bytes, run_id_holder: dict[str, str]) -> None:
    """Log the fitted bundle as an MLflow artifact + register the model, verifying @Champion (ADR-012)."""
    import mlflow
    from mlflow.tracking import MlflowClient

    from shared.constants import mlflow_model_uri

    fqn = mlflow_model_uri(CATALOG, SCHEMA, MODEL_NAME)
    with mlflow.start_run() as run:
        run_id_holder["run_id"] = run.info.run_id
        mlflow.log_dict({"format": "xt_gk_v2_bundle", "bytes": len(envelope)}, "bundle_meta.json")
        mlflow.pyfunc.log_model(
            name=MODEL_NAME,
            python_model=_XtGkV2Model(),
            registered_model_name=fqn,
        )
        set_and_verify_mlflow_champion(MlflowClient(), mlflow_fqn=fqn, run_id=run.info.run_id)


class _XtGkV2Model:
    """Minimal MLflow pyfunc wrapper — the served artifact is the UC-Volume JSON bundle (ADR-012)."""

    def predict(self, context: Any, model_input: Any) -> Any:
        raise NotImplementedError("xt_gk_v2 scoring runs in ingestion.xt_gk_v2_writer, not MLflow serving")


@workflow("wf-xt-gk-v2", phase="training")
def main() -> None:
    """Load the fit corpus, fit possession-value + turnover-cost, deliver the bundle via ADR-012."""
    _assert_silly_kicks_min()
    require_mlflow_env()  # fail loud BEFORE any work (ADR-012)

    from ingestion.databricks_auth import bearer_token, workspace_client

    host = os.environ["DATABRICKS_HOST"].replace("https://", "").rstrip("/")
    # M2M-aware (ADR-079): resolve the SQL bearer via the SDK provider chain (M2M OAuth or
    # a static token), NOT os.environ["DATABRICKS_TOKEN"] — so the fit-corpus read works
    # when the job is launched with service-principal client-id/secret.
    token = bearer_token()
    warehouse_id = os.environ["DATABRICKS_SQL_WAREHOUSE_ID"]

    corpus = load_fit_corpus(host, token, warehouse_id)
    pv, tc, pl = fit_xt_gk_v2(corpus)
    envelope = serialize_xt_gk_v2_bundle(pv, tc, pl, xg_column=XG_COLUMN, pressure_column=PRESSURE_COLUMN)

    # ADR-012 leg 1: UC Volume (the writer's load path).
    w = workspace_client()
    upload_weights_to_uc_volume(
        w,
        catalog=CATALOG,
        schema=SCHEMA,
        model_name=MODEL_NAME,
        filename=WEIGHTS_FILENAME,
        weights_bytes=envelope,
    )
    # ADR-012 leg 2: MLflow registry + @Champion.
    _log_and_register_mlflow(envelope, {})
    logger.info("xt_gk_v2 fit complete (bundle=%d bytes)", len(envelope))


if __name__ == "__main__":
    main()
