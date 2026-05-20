"""DEFCON-lite shared utilities for 360 and tracking paths.

Contains constants, shared UDF builders, and MLflow model loading
used by both ``defcon_lite_360`` and ``defcon_lite_tracking``.
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING

import pandas as pd

from ingestion.utils import _load_mlflow_artifact_hash, verify_artifact_hash
from shared.constants import mlflow_model_uri

if TYPE_CHECKING:
    from analytics.defcon_lite import DefconLiteParams  # noqa: F401

__all__ = [
    "_ACTION_PREFIX",
    "_FF_PREFIX",
    "_TABLE_NAME",
    "_VALUE_UDF_INPUT_COLS",
    "_make_values_udf",
    "_try_load_champion_defcon",
]

_TABLE_NAME = "defcon_results"

# ---------------------------------------------------------------------------
# Column name prefixes used to distinguish actions vs freeze-frame columns
# after joining in Spark.  The UDF splits them back apart.
# ---------------------------------------------------------------------------
_ACTION_PREFIX = "act_"
_FF_PREFIX = "ff_"


def _try_load_champion_defcon(
    logger: logging.Logger,
    catalog: str,
    schema: str,
) -> bytes | None:
    """Try to load DEFCON value estimator from MLflow @Champion alias.

    Returns serialized XGBRegressor bytes if found, None otherwise.
    Falls back gracefully when mlflow is not installed or model is not registered.
    """
    try:
        import importlib

        mlflow_pyfunc = importlib.import_module("mlflow.pyfunc")
        mlflow_tracking = importlib.import_module("mlflow.tracking")
    except (ImportError, ModuleNotFoundError):
        logger.info("mlflow not available — will use per-match DEFCON training")
        return None

    model_name = mlflow_model_uri(catalog, schema, "defcon_model")
    try:
        model_uri = f"models:/{model_name}@Champion"
        logger.info("Loading DEFCON @Champion from %s", model_uri)
        champion = mlflow_pyfunc.load_model(model_uri)
        unwrapped = champion.unwrap_python_model()  # type: ignore[union-attr]
        regressor = unwrapped.regressor  # type: ignore[union-attr]
        model_bytes = bytes(regressor.get_booster().save_raw("json"))

        # SEC2: verify artifact integrity against recorded MLflow tag (if any)
        client = mlflow_tracking.MlflowClient()
        verify_artifact_hash(
            data=model_bytes,
            expected_sha256=_load_mlflow_artifact_hash(client, model_name, alias="Champion"),
            artifact_label=f"{model_name}_regressor",
            logger=logger,
        )

        logger.info("Loaded DEFCON @Champion from MLflow (%d bytes)", len(model_bytes))
        return model_bytes
    except Exception:  # noqa: BLE001 — MLflow registry raises many unrelated exception types on missing Champion
        logger.info("DEFCON @Champion not found in MLflow registry — will use per-match training", exc_info=True)
        return None


# Module-level column contract: the 18 columns Pass 2 receives from Pass 1
# output (credits_schema). Guards against column-list drift between the join
# output and what the UDF actually reads. Same LL1 latent-bug class as PR #230.
_VALUE_UDF_INPUT_COLS: tuple[str, ...] = (
    "event_id",
    "match_id",
    "competition_id",
    "season_id",
    "defender_player_id",
    "defender_team_id",
    "defender_x",
    "defender_y",
    "action_player_id",
    "action_type",
    "action_x",
    "action_y",
    "credit_type",
    "confidence",
    "dist_to_ball",
    "pitch_control_at_action",
    "offensive_value",
    "vaep_target",
)


def _make_values_udf(
    disturb_radius_m: float,
    deter_cone_angle_deg: float,
    pitch_length: float,
    pitch_width: float,
    data_source: str,
    champion_model_bytes: bytes | None = None,
) -> object:
    """Build the Pass 2 ``applyInPandas`` UDF closure for DEFCON value estimation.

    Args:
        disturb_radius_m: DEFCON disturb radius parameter.
        deter_cone_angle_deg: DEFCON deter cone angle parameter.
        pitch_length: Pitch length in meters.
        pitch_width: Pitch width in meters.
        data_source: Data source tag for the output rows.
        champion_model_bytes: Optional serialized XGBRegressor bytes from
            MLflow @Champion. When provided, the UDF scores with this
            pre-trained model instead of training per-match.

    Returns:
        A callable ``(pd.DataFrame) -> pd.DataFrame`` suitable for
        ``applyInPandas``.
    """
    _disturb_r = disturb_radius_m
    _deter_angle = deter_cone_angle_deg
    _pitch_l = pitch_length
    _pitch_w = pitch_width
    _data_source = data_source
    _champion_raw = champion_model_bytes

    def _udf(pdf: pd.DataFrame) -> pd.DataFrame:
        """Estimate DEFCON values for one match's credits."""
        import pandas as _pd

        from analytics.defcon_lite import DefconLiteParams as _Params

        _empty_cols = _pd.Index(
            [
                "event_id",
                "match_id",
                "competition_id",
                "season_id",
                "defender_player_id",
                "defender_team_id",
                "defender_x",
                "defender_y",
                "action_player_id",
                "action_type",
                "action_x",
                "action_y",
                "credit_type",
                "confidence",
                "defcon_value",
                "dist_to_ball",
                "pitch_control_at_action",
                "data_source",
            ]
        )

        if pdf.empty:
            return _pd.DataFrame(columns=_empty_cols)

        params = _Params(
            disturb_radius_m=_disturb_r,
            deter_cone_angle_deg=_deter_angle,
            pitch_length=_pitch_l,
            pitch_width=_pitch_w,
        )

        if _champion_raw is not None:
            # Use pre-trained @Champion model — deserialize once per executor
            from analytics.defcon_lite import extract_features as _extract

            if not hasattr(_udf, "_model_cache"):
                _udf._model_cache = {}  # type: ignore[attr-defined]
            cache: dict = _udf._model_cache  # type: ignore[attr-defined]
            if "champion" not in cache:
                from xgboost import XGBRegressor

                m = XGBRegressor()
                m.load_model(bytearray(_champion_raw))
                cache["champion"] = m

            features = _extract(pdf, params)
            result = pdf.copy()
            result["defcon_value"] = cache["champion"].predict(features)
            result = result.drop(columns=["offensive_value", "vaep_target"], errors="ignore")
        else:
            # Fall back to per-match training
            from analytics.defcon_lite import estimate_values_for_match as _estimate

            result = _estimate(pdf, params)

        result["data_source"] = _data_source

        return _pd.DataFrame(result)

    return _udf
