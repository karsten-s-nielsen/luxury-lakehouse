"""Pre-shot xG **v3** scoring pipeline — canonical-SPADL, two-mode gate (Task 1.9, §C1).

Loads the SPADL-native set encoder ``xg_model_v3@Champion`` (raw xG) from MLflow (preferred)
or UC Volume (fallback), together with the trainer-shipped per-provider + pooled OOF
calibrators (``_calibrators``) and the excluded-shot-type penalty constant (``_penalty_xg``).
Scores every shot-family row in ``soccer_analytics.dev_gold.fct_action_values`` — joining
freeze frames from ``soccer_analytics.bronze.shot_freeze_frames`` on ``(match_key, action_id)``
(empty freeze set → zero-context) — via the SHARED serve functions
``analytics.xg_model.build_features`` + ``analytics.xg_freeze_frame.normalize_freeze_frame``
(the SAME entry points the trainer uses; cross-entry-point parity test).

Per provider a **two-mode gate** (``analytics.xg_calibration`` — carried forward, never
re-derived) keeps SELECTION and CERTIFICATION separate (M2):

* **selection** (``select_scoring_mode``) picks context-aware vs tabular-only by the held-out
  discrimination CI lower bound;
* **certification** (``is_mode_certified`` + ``calibration_ok_n_aware``) is an absolute bar —
  the shipped mode's AUC-CI *lower bound* must clear the StatsBomb-relative floor AND aggregate
  calibration must pass. A mode can be selected yet uncertified → ``ood_flag=True``.

**Calibration (the only one, M1):** the model emits RAW xG; this scorer applies the single
per-provider OOF calibrator. **N4 fallback:** a provider with no per-provider calibrator gets
the **pooled** calibrator AND ``ood_flag=True``. ``shot_penalty`` rows bypass the encoder and
take the constant ``_penalty_xg`` from the envelope (never recomputed live).

**Gate-evidence source (interim):** the trainer envelope ships ``_calibrators`` + ``_penalty_xg``
today, but not the per-provider AUC-CI gate evidence. The scorer reads optional per-provider
gate evidence from ``envelope["_gate"]`` and **fail-safes** every provider without evidence to
``tabular_only`` + ``ood_flag=True`` (the plan's documented interim: GS/SC flagged until the
trainer ships ``_gate``). The gate LOGIC is fully wired + unit-tested for when evidence is present.

Writes ``bronze.xg_shot_predictions`` with ``replaceWhere`` per ``match_key``.
"""

from __future__ import annotations

import importlib
import json
import logging
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

import numpy as np
import numpy.typing as npt

from analytics.set_encoder import deserialize_set_encoder_weights, encode_player_set, predict_xg_with_uncertainty

# --- SHARED serve functions (M2 parity — the SAME entry points the trainer imports) ---
from analytics.xg_calibration import (
    AucCi,
    IsotonicParams,
    PlattParams,
    apply_isotonic,
    apply_platt,
    calibration_ok_n_aware,
    is_mode_certified,
    select_scoring_mode,
)
from analytics.xg_freeze_frame import SPADL_PITCH, normalize_freeze_frame
from analytics.xg_model import XGModelConfig, build_features, spadl_shot_geometry
from shared.constants import DEFAULT_GOLD_SCHEMA, mlflow_model_uri

if TYPE_CHECKING:
    import pandas as pd
    from pyspark.sql import SparkSession

logger = logging.getLogger(__name__)

_TABLE_NAME = "xg_shot_predictions"
_MODEL_NAME = "xg_model_v3"

# v3 tabular feature order (geometry only + set_cardinality) — the uniform, provider-agnostic
# set the trainer pins (train_xg_v3_hf.UNIFORM_FEATURE_NAMES). The envelope's ``feature_names``
# is authoritative at runtime; this constant is the reindex fallback + the e2e test's default.
UNIFORM_FEATURE_NAMES: tuple[str, ...] = (
    "distance_to_goal",
    "shot_angle",
    "location_x",
    "location_y",
    "set_cardinality",
)

# Shot-family action types scored here (spec §C1 — score ALL, incl. penalties).
_SHOT_ACTION_TYPES: tuple[str, ...] = ("shot", "shot_freekick", "shot_penalty")
_PENALTY_ACTION_TYPE = "shot_penalty"

_SCORING_MODE_CONTEXT = "context_aware"
_SCORING_MODE_TABULAR = "tabular_only"
_PENALTY_SCORING_MODE = "penalty_constant"

# Two-mode gate defaults (StatsBomb-relative floor: max(sb_auc - margin, floor)).
DEFAULT_GATE_MARGIN = 0.05
DEFAULT_GATE_FLOOR = 0.5

# Deterministic MC-dropout seed (matches the v2 scorer's fixed-seed inference).
_MC_SEED = 42

# ── Bronze writer column contract (ADR-002 §4 — pinned against the DDL by the schema-drift
# guard in src/tests/test_xg_shot_scorer.py). ``_ingested_at`` is appended by
# write_delta_table (NOT emitted here) and is DELIBERATELY absent from this constant. ──
_XG_SHOT_PRED_COLUMNS: tuple[str, ...] = (
    "match_id_native",
    "match_key",
    "action_id",
    "data_source",
    "xg",
    "xg_ci_low",
    "xg_ci_high",
    "scoring_mode",
    "ood_flag",
)
# Column -> Spark SQL type category (kept in lockstep with the DDL types).
_XG_SHOT_PRED_TYPES: dict[str, str] = {
    "match_id_native": "string",
    "match_key": "long",
    "action_id": "long",
    "data_source": "string",
    "xg": "double",
    "xg_ci_low": "double",
    "xg_ci_high": "double",
    "scoring_mode": "string",
    "ood_flag": "boolean",
}


# ===========================================================================
# Gate evidence + decisions (PURE)
# ===========================================================================


@dataclass(frozen=True)
class GateConfig:
    """StatsBomb-relative two-mode-gate thresholds."""

    sb_auc: float
    margin: float = DEFAULT_GATE_MARGIN
    floor: float = DEFAULT_GATE_FLOOR


@dataclass(frozen=True)
class ProviderGateEvidence:
    """Per-provider held-out evidence for the two-mode gate.

    ``context_ci`` / ``tabular_ci`` are the AUC confidence intervals of the two scoring modes;
    ``sum_xg`` / ``sum_goals`` / ``n`` are the aggregate-calibration inputs for
    ``calibration_ok_n_aware``.
    """

    context_ci: AucCi
    tabular_ci: AucCi
    sum_xg: float
    sum_goals: float
    n: int


def decide_scoring_mode(evidence: ProviderGateEvidence | None, cfg: GateConfig) -> tuple[str, bool]:
    """Return ``(scoring_mode, ood_flag)`` for a provider — selection and certification SEPARATE.

    Selection (``select_scoring_mode``) picks the mode; certification (``is_mode_certified`` on
    the shipped mode's CI lower bound AND ``calibration_ok_n_aware``) decides trust. A mode can be
    selected yet uncertified → ``ood_flag=True``. With no evidence, fail-safe to
    ``tabular_only`` + flagged (the documented interim behavior).
    """
    if evidence is None:
        return (_SCORING_MODE_TABULAR, True)

    mode = select_scoring_mode(
        evidence.context_ci, evidence.tabular_ci, sb_auc=cfg.sb_auc, margin=cfg.margin, floor=cfg.floor
    )
    shipped_ci = evidence.context_ci if mode == _SCORING_MODE_CONTEXT else evidence.tabular_ci
    certified = is_mode_certified(
        shipped_ci, sb_auc=cfg.sb_auc, margin=cfg.margin, floor=cfg.floor
    ) and calibration_ok_n_aware(evidence.sum_xg, evidence.sum_goals, evidence.n)
    return (mode, not certified)


def _parse_auc_ci(d: dict[str, Any]) -> AucCi:
    return AucCi(auc=float(d["auc"]), lo=float(d["lo"]), hi=float(d["hi"]))


def parse_gate(envelope: dict[str, Any]) -> tuple[GateConfig, dict[str, ProviderGateEvidence]]:
    """Parse the optional ``_gate`` evidence block; absent → empty evidence (fail-safe).

    Schema (when present)::

        "_gate": {
            "sb_auc": float, "margin": float, "floor": float,
            "per_provider": {
                "<provider>": {"context": {auc,lo,hi}, "tabular": {auc,lo,hi},
                               "sum_xg": float, "sum_goals": float, "n": int}
            }
        }
    """
    gate = envelope.get("_gate")
    if not gate:
        return (GateConfig(sb_auc=0.0), {})
    cfg = GateConfig(
        sb_auc=float(gate.get("sb_auc", 0.0)),
        margin=float(gate.get("margin", DEFAULT_GATE_MARGIN)),
        floor=float(gate.get("floor", DEFAULT_GATE_FLOOR)),
    )
    evidence: dict[str, ProviderGateEvidence] = {}
    for provider, e in (gate.get("per_provider") or {}).items():
        evidence[str(provider)] = ProviderGateEvidence(
            context_ci=_parse_auc_ci(e["context"]),
            tabular_ci=_parse_auc_ci(e["tabular"]),
            sum_xg=float(e["sum_xg"]),
            sum_goals=float(e["sum_goals"]),
            n=int(e["n"]),
        )
    return (cfg, evidence)


def build_provider_decisions(
    evidence_map: dict[str, ProviderGateEvidence], cfg: GateConfig
) -> dict[str, tuple[str, bool]]:
    """Materialize the per-provider ``(scoring_mode, provider_ood)`` decision from gate evidence."""
    return {provider: decide_scoring_mode(evidence, cfg) for provider, evidence in evidence_map.items()}


# ===========================================================================
# Calibration application (PURE)
# ===========================================================================


def _apply_entry(raw: npt.NDArray[np.float64], entry: dict[str, Any]) -> npt.NDArray[np.float64]:
    kind = entry["kind"]
    params = entry["params"]
    if kind == "platt":
        return apply_platt(raw, PlattParams.from_dict(params))
    if kind == "isotonic":
        return apply_isotonic(raw, IsotonicParams.from_dict(params))
    raise ValueError(f"Unknown calibrator kind {kind!r} (expected 'platt' or 'isotonic')")


def calibrate_xg(
    raw: npt.NDArray[np.float64], data_source: str, calibrators: dict[str, Any]
) -> tuple[npt.NDArray[np.float64], bool]:
    """Apply the per-provider OOF calibrator to raw xG; N4 pooled fallback + flag.

    Returns ``(calibrated, fell_back)`` where ``fell_back`` is True iff the provider had no
    per-provider calibrator and the pooled one was used (the row is then OOD-flagged upstream).
    """
    per_provider = calibrators.get("per_provider") or {}
    entry = per_provider.get(data_source)
    fell_back = False
    if entry is None:
        entry = calibrators.get("pooled")
        fell_back = True
        if entry is None:
            raise RuntimeError(
                f"No per-provider calibrator for {data_source!r} and no pooled calibrator to fall back on"
            )
    return _apply_entry(np.asarray(raw, dtype=np.float64), entry), fell_back


def penalty_prediction(penalty_xg: float) -> float:
    """The constant ``shot_penalty`` xG taken verbatim from the envelope (never recomputed)."""
    return float(penalty_xg)


# ===========================================================================
# Model envelope loading (PURE parse)
# ===========================================================================


@dataclass(frozen=True)
class LoadedXgV3Model:
    """The parsed ``xg_model_v3`` weight envelope + serve-time evidence (ADR-012 §2)."""

    weights: dict[str, npt.NDArray[np.floating[Any]]]
    feature_names: list[str]
    tabular_dim: int
    calibrators: dict[str, Any]
    penalty_xg: float | None
    gate_config: GateConfig
    provider_evidence: dict[str, ProviderGateEvidence]


def parse_envelope(weights_bytes: bytes) -> LoadedXgV3Model:
    """Parse the JSON weight envelope: weights + feature_names + calibrators + penalty + gate.

    ``feature_names`` is mandatory (ADR-012 §2 — legacy envelopes without it raise). The set
    encoder weight arrays are reconstructed via ``deserialize_set_encoder_weights`` (base64, no
    pickle); the ``_isotonic_*`` / ``_mc_*`` sidecar arrays it also returns are harmless extras.
    """
    envelope = json.loads(weights_bytes.decode("utf-8"))
    feature_names = envelope.get("feature_names")
    if not feature_names:
        raise RuntimeError(
            "xg_model_v3 weights envelope is missing 'feature_names' (ADR-012 §2). "
            "Retrain + register a Champion via scripts/train_xg_v3_hf.py."
        )
    tabular_dim = int(envelope.get("tabular_dim", len(feature_names)))
    calibrators = envelope.get("_calibrators") or {"per_provider": {}, "pooled": None}
    penalty_xg = envelope.get("_penalty_xg")
    gate_config, provider_evidence = parse_gate(envelope)
    return LoadedXgV3Model(
        weights=deserialize_set_encoder_weights(weights_bytes),
        feature_names=[str(f) for f in feature_names],
        tabular_dim=tabular_dim,
        calibrators=calibrators,
        penalty_xg=None if penalty_xg is None else float(penalty_xg),
        gate_config=gate_config,
        provider_evidence=provider_evidence,
    )


# ===========================================================================
# Core scoring (PURE — no Spark, no torch)
# ===========================================================================


def _freeze_lookup(
    freeze_pdf: pd.DataFrame | None,
) -> dict[tuple[Any, Any], tuple[npt.NDArray[np.float64], bool]]:
    """Index freeze-frame rows by ``(match_key, action_id)`` → (raw (N,4) player set, attacks_high_x).

    The (N,4) array is raw SPADL ``[x, y, is_keeper, is_teammate]`` metres — NOT yet normalized
    (``normalize_freeze_frame`` is applied per shot at score time, mirroring the trainer). The
    ``(match_key, action_id)`` key is per-match, never ``action_id`` alone (§5 invariant).
    """
    import pandas as _pd

    lookup: dict[tuple[Any, Any], tuple[npt.NDArray[np.float64], bool]] = {}
    if freeze_pdf is None or len(freeze_pdf) == 0:
        return lookup
    has_orientation = "shooter_attacks_high_x" in freeze_pdf.columns
    for (match_key, action_id), group in freeze_pdf.groupby(["match_key", "action_id"]):
        players = np.column_stack(
            [
                group["x"].to_numpy(dtype=np.float64),
                group["y"].to_numpy(dtype=np.float64),
                group["is_keeper"].to_numpy(dtype=np.float64),
                group["is_teammate"].to_numpy(dtype=np.float64),
            ]
        )
        raw = group["shooter_attacks_high_x"].iloc[0] if has_orientation else True
        attacks_high = True if _pd.isna(raw) else bool(raw)
        lookup[(match_key, action_id)] = (players, attacks_high)
    return lookup


def _build_tabular(
    shots_pdf: pd.DataFrame,
    set_cardinalities: list[int],
    feature_names: list[str],
) -> npt.NDArray[np.float64]:
    """SPADL-native tabular feature matrix via the SHARED ``build_features`` (M2 train/serve parity).

    Mirrors the trainer's ``build_spadl_tabular``: canonical-SPADL geometry from ``start_x/start_y``
    (goal ``(105, 34)``) + ``location_x/y`` + the R3 ``set_cardinality`` feature, reindexed to the
    envelope's ``feature_names`` order. Never rescales to StatsBomb units.
    """
    df = shots_pdf.copy().reset_index(drop=True)
    if "start_x" in df.columns and "start_y" in df.columns:
        geom = [spadl_shot_geometry(float(x), float(y)) for x, y in zip(df["start_x"], df["start_y"], strict=True)]
        df["distance_to_goal"] = [g[0] for g in geom]
        df["shot_angle"] = [g[1] for g in geom]
        df["location_x"] = df["start_x"].astype(float)
        df["location_y"] = df["start_y"].astype(float)
    df["set_cardinality"] = [int(c) for c in set_cardinalities]
    x, _ = build_features(df, XGModelConfig(), expected_features=feature_names)
    return x.to_numpy(dtype=np.float64)


def score_shot_rows(
    shots_pdf: pd.DataFrame,
    freeze_pdf: pd.DataFrame | None,
    *,
    weights: dict[str, npt.NDArray[np.floating[Any]]],
    feature_names: list[str],
    calibrators: dict[str, Any],
    penalty_xg: float | None,
    provider_decisions: dict[str, tuple[str, bool]],
    seed: int = _MC_SEED,
) -> pd.DataFrame:
    """Score a batch of shots → one ``bronze.xg_shot_predictions`` row per shot (per-match UDF seam).

    Exactly one output row per input shot row — the freeze set for a shot's ``(match_key, action_id)``
    collapses into a single player set (no fan-out). ``provider_decisions`` maps
    ``data_source → (scoring_mode, provider_ood)``; a provider absent from it fail-safes to
    ``tabular_only`` + flagged. ``shot_penalty`` rows take the constant ``penalty_xg`` and bypass the
    encoder + calibrator. All other rows: encode context (context-aware) or zero-context (tabular-only),
    MC-dropout mean + 95% CI, then the single per-provider calibrator (pooled fallback → ood).
    """
    import pandas as _pd

    n = len(shots_pdf)
    shots = shots_pdf.reset_index(drop=True)
    lookup = _freeze_lookup(freeze_pdf)

    match_keys = shots["match_key"].to_numpy()
    action_ids = shots["action_id"].to_numpy()
    action_types = shots["action_type"].astype("string").to_numpy()
    data_sources = shots["data_source"].astype("string").to_numpy()

    # Per-shot resolved scoring mode + whether context is actually available.
    modes: list[str] = []
    provider_ood: list[bool] = []
    player_sets: list[npt.NDArray[np.float64]] = []
    set_cardinalities: list[int] = []
    for i in range(n):
        ds = str(data_sources[i])
        mode, p_ood = provider_decisions.get(ds, (_SCORING_MODE_TABULAR, True))
        players, attacks_high = lookup.get((match_keys[i], action_ids[i]), (None, True))
        if mode == _SCORING_MODE_CONTEXT and players is not None and players.shape[0] > 0:
            norm = normalize_freeze_frame(players, SPADL_PITCH, shooter_attacks_high_x=attacks_high)
            player_sets.append(norm)
            set_cardinalities.append(int(norm.shape[0]))
            modes.append(_SCORING_MODE_CONTEXT)
        else:
            # tabular-only: zero context + set_cardinality 0 (the trained zero-context path).
            player_sets.append(np.empty((0, 4), dtype=np.float64))
            set_cardinalities.append(0)
            modes.append(_SCORING_MODE_TABULAR)
        provider_ood.append(bool(p_ood))

    tabular = _build_tabular(shots, set_cardinalities, feature_names)

    xg = np.full(n, np.nan, dtype=np.float64)
    xg_ci_low = np.full(n, np.nan, dtype=np.float64)
    xg_ci_high = np.full(n, np.nan, dtype=np.float64)
    out_mode: list[str] = list(modes)
    ood = list(provider_ood)

    for i in range(n):
        if str(action_types[i]) == _PENALTY_ACTION_TYPE:
            if penalty_xg is None:
                raise RuntimeError("shot_penalty row encountered but the envelope carries no _penalty_xg constant")
            pen = penalty_prediction(penalty_xg)
            xg[i] = xg_ci_low[i] = xg_ci_high[i] = pen
            out_mode[i] = _PENALTY_SCORING_MODE
            ood[i] = False
            continue

        context = encode_player_set(player_sets[i], weights)
        mean, _std, ci_lo_raw, ci_hi_raw = predict_xg_with_uncertainty(tabular[i], context, weights, random_state=seed)
        calibrated, fell_back = calibrate_xg(
            np.array([mean, ci_lo_raw, ci_hi_raw], dtype=np.float64), str(data_sources[i]), calibrators
        )
        xg[i] = float(calibrated[0])
        xg_ci_low[i] = float(min(calibrated[1], calibrated[2]))
        xg_ci_high[i] = float(max(calibrated[1], calibrated[2]))
        ood[i] = bool(provider_ood[i] or fell_back)

    return _pd.DataFrame(
        {
            "match_id_native": shots["match_id_native"].astype("string").to_numpy()
            if "match_id_native" in shots.columns
            else np.array([None] * n, dtype=object),
            "match_key": match_keys,
            "action_id": action_ids,
            "data_source": data_sources,
            "xg": xg,
            "xg_ci_low": xg_ci_low,
            "xg_ci_high": xg_ci_high,
            "scoring_mode": out_mode,
            "ood_flag": ood,
        }
    )[list(_XG_SHOT_PRED_COLUMNS)]


# ===========================================================================
# Spark I/O (main) — validated by the live gate, not by unit tests
# ===========================================================================


def _xg_shot_pred_struct_type() -> Any:
    """Cogroup ``applyInPandas`` output schema for ``score_shot_rows`` (lazy pyspark import)."""
    from pyspark.sql.types import (
        BooleanType,
        DoubleType,
        LongType,
        StringType,
        StructField,
        StructType,
    )

    type_map = {"long": LongType(), "double": DoubleType(), "boolean": BooleanType(), "string": StringType()}
    return StructType([StructField(name, type_map[_XG_SHOT_PRED_TYPES[name]], True) for name in _XG_SHOT_PRED_COLUMNS])


def _try_load_champion_xg_v3(log: logging.Logger, catalog: str, gold_schema: str) -> bytes | None:
    """Load ``xg_model_v3@Champion`` weight bytes from the MLflow UC registry, else None.

    Resolves the Champion alias, downloads the
    ``model_weights.json`` artifact, and SEC2-verifies its hash. Returns None (→ UC Volume fallback)
    when the model / alias is not registered.
    """
    from ingestion.utils import _load_mlflow_artifact_hash, verify_artifact_hash

    try:
        mlflow_mod = importlib.import_module("mlflow")
        mlflow_tracking = importlib.import_module("mlflow.tracking")
    except (ImportError, ModuleNotFoundError):
        log.info("mlflow not available — will try UC Volume for xg_model_v3 weights")
        return None

    model_name = mlflow_model_uri(catalog, gold_schema, _MODEL_NAME)
    try:
        client = mlflow_tracking.MlflowClient()  # type: ignore[union-attr]
        alias_info = client.get_model_version_by_alias(model_name, "Champion")
        artifact_path = mlflow_mod.artifacts.download_artifacts(  # type: ignore[union-attr]
            run_id=alias_info.run_id, artifact_path="model_weights.json"
        )
        with open(artifact_path, "rb") as f:
            weights_bytes = f.read()
        verify_artifact_hash(
            data=weights_bytes,
            expected_sha256=_load_mlflow_artifact_hash(client, model_name, alias="Champion"),
            artifact_label=f"{model_name}_weights",
            logger=log,
        )
        log.info("Loaded xg_model_v3 @Champion from MLflow (%d bytes, run=%s)", len(weights_bytes), alias_info.run_id)
        return weights_bytes
    except Exception:  # noqa: BLE001 — MLflow registry raises many unrelated types on missing Champion
        log.info("xg_model_v3 @Champion not found in MLflow registry", exc_info=True)
        return None


def _load_champion_weights(spark: SparkSession, catalog: str, gold_schema: str, log: logging.Logger) -> bytes:
    """Load the v3 Champion weight bytes: MLflow alias first, UC Volume fallback (ADR-012 loading path)."""
    from ingestion.utils import _load_volume_sidecar_hash, verify_artifact_hash

    weights_bytes = _try_load_champion_xg_v3(log, catalog, gold_schema)
    if weights_bytes is not None:
        return weights_bytes

    volume_path = f"/Volumes/{catalog}/{gold_schema}/model_weights/{_MODEL_NAME}/model_weights.json"
    try:
        row = spark.read.format("binaryFile").load(volume_path).first()
        if row is None:
            raise RuntimeError(f"UC Volume file is empty: {volume_path}")
        weights_bytes = row["content"]
    except Exception as exc:
        raise RuntimeError(
            f"xg_model_v3 weights not available at MLflow @Champion OR UC Volume {volume_path}. "
            "Train + register a model via scripts/train_xg_v3_hf.py before running the scorer."
        ) from exc
    verify_artifact_hash(
        data=weights_bytes,
        expected_sha256=_load_volume_sidecar_hash(volume_path),
        artifact_label=f"{_MODEL_NAME}_weights_volume",
        logger=log,
    )
    log.info("Loaded xg_model_v3 weights from UC Volume (%d bytes)", len(weights_bytes))
    return weights_bytes


def _load_shots(spark: SparkSession, catalog: str, gold_schema: str) -> Any:
    """All shot-family rows from ``fct_action_values`` (native ids + geometry + provenance)."""
    from pyspark.sql import functions as spark_fn  # type: ignore[import-not-found]

    families = ", ".join(f"'{t}'" for t in _SHOT_ACTION_TYPES)
    return (
        spark.table(f"{catalog}.{gold_schema}.fct_action_values")
        .where(f"action_type IN ({families})")
        .select(
            spark_fn.col("match_id_native").cast("string").alias("match_id_native"),
            "match_key",
            "action_id",
            "data_source",
            "action_type",
            "start_x",
            "start_y",
        )
    )


def _load_freeze_frames(spark: SparkSession, catalog: str) -> Any:
    """Freeze-frame player rows keyed on ``(match_key, action_id)`` for the context-aware path."""
    return spark.table(f"{catalog}.bronze.shot_freeze_frames").select(
        "match_key", "action_id", "x", "y", "is_keeper", "is_teammate", "shooter_attacks_high_x"
    )


def run_pipeline(
    spark: SparkSession,
    catalog: str,
    schema: str,
    gold_schema: str,
    log: logging.Logger,
) -> int:
    """Score every shot in ``fct_action_values`` → ``bronze.xg_shot_predictions`` (replaceWhere/match_key).

    Loads the v3 Champion + calibrators + penalty + gate evidence, computes the per-provider two-mode
    decisions once, then distributes scoring across executors via ``groupBy(match_key).cogroup(...)``
    so each match's shots see their own freeze frames. Idempotent bulk write over the covered
    ``match_key`` set.
    """
    from ingestion.utils import write_delta_table

    weights_bytes = _load_champion_weights(spark, catalog, gold_schema, log)
    model = parse_envelope(weights_bytes)
    provider_decisions = build_provider_decisions(model.provider_evidence, model.gate_config)
    log.info(
        "Loaded xg_model_v3: %d features, %d per-provider calibrators, penalty_xg=%s, gate providers=%s",
        len(model.feature_names),
        len(model.calibrators.get("per_provider") or {}),
        model.penalty_xg,
        sorted(provider_decisions),
    )

    shots = _load_shots(spark, catalog, gold_schema)
    freeze = _load_freeze_frames(spark, catalog)

    match_keys = sorted({int(r["match_key"]) for r in shots.select("match_key").distinct().collect()})
    if not match_keys:
        log.info("No shots to score — nothing to do")
        return 0

    weights = model.weights
    feature_names = model.feature_names
    calibrators = model.calibrators
    penalty_xg = model.penalty_xg

    def _udf(shots_pdf: pd.DataFrame, freeze_pdf: pd.DataFrame) -> pd.DataFrame:
        return score_shot_rows(
            shots_pdf,
            freeze_pdf,
            weights=weights,
            feature_names=feature_names,
            calibrators=calibrators,
            penalty_xg=penalty_xg,
            provider_decisions=provider_decisions,
        )

    scored = (
        shots.groupBy("match_key")
        .cogroup(freeze.groupBy("match_key"))
        .applyInPandas(_udf, schema=_xg_shot_pred_struct_type())
    )

    key_list = ", ".join(str(k) for k in match_keys)
    row_count = write_delta_table(
        scored,
        catalog,
        schema,
        _TABLE_NAME,
        replace_where=f"match_key IN ({key_list})",
        logger=log,
    )
    log.info("Wrote %d xg_shot_predictions across %d matches", row_count, len(match_keys))
    return 0


def main() -> None:
    """CLI entry point — ``compute_xg_shot_scores`` mega-job task."""
    import argparse

    from ingestion.utils import configure_logging, get_spark_session
    from shared.constants import IDENTIFIER_RE

    parser = argparse.ArgumentParser(description="Score shots with the xg_model_v3 pre-shot xG model")
    parser.add_argument("--catalog", default="soccer_analytics")
    parser.add_argument("--schema", default="bronze")
    parser.add_argument("--gold-schema", default=DEFAULT_GOLD_SCHEMA, help="Gold schema (fct_action_values + registry)")
    args = parser.parse_args()

    for field_name, value in (("catalog", args.catalog), ("schema", args.schema), ("gold-schema", args.gold_schema)):
        if not IDENTIFIER_RE.match(value):
            raise SystemExit(f"Invalid {field_name} '{value}': must match {IDENTIFIER_RE.pattern}")

    log = configure_logging("xg_shot_scorer")
    spark = get_spark_session()

    from ingestion.bootstrap import bootstrap_hooks

    bootstrap_hooks(spark, args.catalog, args.schema)

    log.info("Starting xg_model_v3 scoring into %s.%s", args.catalog, args.schema)
    run_pipeline(spark, args.catalog, args.schema, args.gold_schema, log)


if __name__ == "__main__":
    main()
