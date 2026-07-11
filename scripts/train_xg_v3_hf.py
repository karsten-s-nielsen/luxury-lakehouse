# /// script
# requires-python = ">=3.10,<3.11"
# dependencies = [
#     "luxury-lakehouse[spadl] @ https://huggingface.co/luxury-lakehouse/build-artifacts/resolve/main/luxury_lakehouse-0.5.77-py3-none-any.whl",
#     "numpy>=1.24",
#     "pandas>=2.0",
#     "pyarrow>=14.0",
#     "torch>=2.0",
#     "scikit-learn>=1.3.0",
#     # analytics.xg_model imports XGBClassifier at module level (v1/v2 baseline path); the
#     # trainer imports build_features/spadl_shot_geometry from it, so xgboost must be present
#     # even though v3's set encoder is pure PyTorch. Same rationale as train_vaep_model_hf.
#     "xgboost>=2.0",
#     "huggingface-hub>=1.5.0",
#     "mlflow>=2.17.0",
#     "databricks-sdk>=0.102.0",
# ]
# ///
"""Train xG **v3** — SPADL-native Deep-Sets set encoder — on HuggingFace Jobs.

v3 is a *re-coordination + retrain* of the v2 set encoder (Deep Sets, Zaheer et al.
2017 + MC-dropout, Gal & Ghahramani 2016), with three changes vs v2 (design spec
``2026-07-05-canonical-spadl-preshot-xg-unification-design.md`` §4):

1. **Canonical SPADL 105x68 geometry — never StatsBomb yards.** Tabular geometry
   (``distance_to_goal`` / ``shot_angle``) is computed from the action-stream SPADL
   ``start_x/start_y`` via ``analytics.xg_model.spadl_shot_geometry`` (goal at
   ``(105, 34)``, width ``7.32 m``). Freeze frames normalize ``÷105,÷68`` via the
   coordinate-invariant C2 port ``analytics.xg_freeze_frame.normalize_freeze_frame``.
   The envelope records ``coordinate_system: "spadl_105x68"`` (ADR-012 §2).

2. **Trained ON the tracking cohorts.** The GS / SkillCorner full-22 freeze frames
   (from ``bronze.shot_freeze_frames``) are IN the training set, held out cleanly via
   **GroupKFold-by-``match_key``**, so full-22 scoring is in-distribution rather than
   OOD (fixes review-B2 at the source). Zero-context (Wyscout / non-360-SB) shots stay
   in training exactly as v2 already does (an empty ``(0,4)`` player set), so the
   tabular-only path is a *trained* prediction.

3. **Set-cardinality feature (R3).** The Deep-Sets encoder SUMS over the player set,
   so a full-22 set has systematically larger context magnitude than an SB-360 partial
   set. ``set_cardinality`` (number of players encoded; 0 for zero-context) lets the
   prediction MLP disentangle count from summed magnitude. It rides through the SHARED
   ``analytics.xg_model.build_features``.

**M2 train/serve parity:** feature assembly imports the SAME package-level
``build_features`` + ``normalize_freeze_frame`` the serving scorer
(``ingestion.xg_shot_scorer``) uses — via the installed ``luxury-lakehouse[spadl]``
wheel. This file re-inlines only genuinely trainer-only glue (the PyTorch model,
training loop, MC-dropout eval, weight export). Re-inlining feature assembly would
defeat the parity gate.

**Population (pre-flight, spec §4.1.1):** train on the OPEN-PLAY family
``{shot, shot_freekick}``. ``shot_penalty`` is EXCLUDED (fixed-geometry ~0.76
conversion craters a geometry model; penalties get a constant xG at scoring time in a
later task). ``action_result == 'yellow_card'`` rows are dropped. Goal label:
``action_result == 'success'``.

Registers the MLflow model **``xg_model_v3``** — a separate registry entry (the legacy
``xg_model_v2`` was retired with the v2 producer chain, ADR-066).

Usage (HF Jobs CLI) — secrets ENCRYPTED via ``--secrets`` (never ``--env``, which is
visible via ``hf jobs inspect``):

    hf jobs uv run scripts/train_xg_v3_hf.py \\
        --flavor l40sx1 --timeout 90m \\
        --secrets HF_TOKEN=$HF_TOKEN \\
        --secrets DATABRICKS_TOKEN=$DATABRICKS_TOKEN \\
        --env MLFLOW_TRACKING_URI=$MLFLOW_TRACKING_URI \\
        --env DATABRICKS_HOST=$DATABRICKS_HOST

Artifacts produced (all three mandatory on success — ADR-012):
  - HF Hub model repo ``luxury-lakehouse/xg-v3-model-set-encoder``
  - MLflow UC Registry ``soccer_analytics.dev_gold.xg_model_v3@Champion``
  - UC Volume ``/Volumes/soccer_analytics/dev_gold/model_weights/xg_model_v3/``
"""

from __future__ import annotations

import base64
import dataclasses
import json
import logging
import os
import time
from collections.abc import Sequence
from typing import TYPE_CHECKING, Any

import numpy as np
import numpy.typing as npt
import pandas as pd
from sklearn.metrics import brier_score_loss, roc_auc_score
from sklearn.model_selection import GroupKFold

from analytics.set_encoder import serialize_set_encoder_weights

# --- SHARED feature code (M2 parity — imported from the installed wheel on HF Jobs) ---
from analytics.xg_calibration import (
    IsotonicParams,
    PlattParams,
    apply_isotonic,
    apply_platt,
    bootstrap_auc_ci,
    choose_calibrator,
)
from analytics.xg_freeze_frame import SPADL_PITCH, normalize_freeze_frame
from analytics.xg_model import XGModelConfig, build_features, spadl_shot_geometry
from ingestion.artifact_deploy import (
    require_mlflow_env,
    set_and_verify_mlflow_champion,
    upload_weights_to_uc_volume,
)
from ingestion.hf_jobs_cost import HF_RATE_A10G_LARGE, HFJobsCostRecorder
from ingestion.hf_publish import get_hf_card_path, restricted_repo_id, upload_hf_readme
from shared.constants import mlflow_model_uri
from workflows import workflow

if TYPE_CHECKING:  # annotations only — never imported at runtime in the local test env
    import torch
    import torch.nn as nn

# torch is an HF-Jobs-only dependency. Guard the import so the pure helpers below
# (population filter, feature assembly, envelope builder, GroupKFold split) stay
# importable in a torch-free local environment for unit testing. Only the two
# nn.Module / Dataset subclasses evaluate their base class at definition time and
# therefore must live behind the guard; every torch-using *function* body is
# resolved lazily at call time (on HF Jobs, where torch is always present).
try:
    import torch as _torch_mod
    import torch.nn as _nn_mod

    torch = _torch_mod
    nn = _nn_mod
    _TORCH_AVAILABLE = True
except ModuleNotFoundError:  # pragma: no cover - exercised only in torch-free CI
    _TORCH_AVAILABLE = False


# Validated HF Jobs flavor — single source of truth.
VALIDATED_HF_FLAVOR: str = "l40sx1"

# uv silent-downgrade footgun (CLAUDE.md): a top-level silly-kicks pin in PEP 723
# deps silently overrides the wheel's transitive pin — so we do NOT pin it here and
# instead assert the runtime minimum. Keep in lockstep with the other trainers.
_REQUIRED_SK_MIN: tuple[int, int, int] = (4, 43, 0)


def _assert_silly_kicks_min() -> None:
    import silly_kicks

    actual = tuple(int(p) for p in silly_kicks.__version__.split(".")[:3])
    if actual < _REQUIRED_SK_MIN:
        raise RuntimeError(
            f"silly-kicks {silly_kicks.__version__} < required "
            f"{'.'.join(str(p) for p in _REQUIRED_SK_MIN)} — refusing to train."
        )


logging.basicConfig(
    format='{"time":"%(asctime)s","level":"%(levelname)s","module":"%(module)s","message":"%(message)s"}',
    level=logging.INFO,
    datefmt="%Y-%m-%dT%H:%M:%S",
)
logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

# The canonical coordinate contract recorded in the weight envelope (ADR-012 §2).
COORDINATE_SYSTEM = "spadl_105x68"

# OPEN-PLAY training shot family (spec §4.1.1). shot_penalty is EXCLUDED: fixed-geometry
# ~0.76 conversion + freeze-frame-non-informative — it craters a geometry model. Penalties
# get a constant xG at scoring time in a separate scorer task, NOT here.
_TRAINING_SHOT_TYPES: tuple[str, ...] = ("shot", "shot_freekick")

# Goal label: a shot is a goal iff action_result == 'success' (pre-flight decision).
_GOAL_RESULT = "success"
# Non-outcome result to drop entirely (2 GS rows) — logged.
_DROP_RESULT = "yellow_card"
# The excluded fixed-geometry shot type. The trainer computes its empirical conversion rate over the
# LOADED (pre-filter) dataset and ships it as a constant ``_penalty_xg`` — the scorer reads it, never
# recomputes (computing it over the penalty-EXCLUDED training set is 0/0 -> NaN).
_PENALTY_SHOT_TYPE = "shot_penalty"

# v3 ships EXACTLY these geometry-only tabular features, in this pinned order (design spec §4). No
# StatsBomb categoricals (body-part / technique / type / play-pattern), no ``end_location``, no
# ``period``/``minute`` — the uniform set is what the serving scorer's tabular-only mode reproduces
# for every provider. ``build_features`` reindexes to this list (pads any missing with 0.0), so the
# feature order is deterministic regardless of which raw columns a provider's shot rows happen to carry.
UNIFORM_FEATURE_NAMES: tuple[str, ...] = (
    "distance_to_goal",
    "shot_angle",
    "location_x",
    "location_y",
    "set_cardinality",
)

# Restricted tracking cohorts (skillcorner is per-match restricted; gradientsports is provider-default
# restricted). Used ONLY for the loud corpus-composition assertion — the authoritative publish split
# keys on the per-row ``access_tier`` (ADR-064), never on this set.
_RESTRICTED_COHORT_PROVIDERS: frozenset[str] = frozenset({"skillcorner", "gradientsports"})

# Two-mode-gate thresholds SHIPPED in the envelope's ``_gate`` block. The scorer's ``parse_gate``
# reads margin/floor FROM the shipped block (falling back to its own defaults only when absent), so
# these are the authoritative values at score time. Kept numerically in lockstep with
# ``ingestion.xg_shot_scorer.DEFAULT_GATE_MARGIN`` / ``DEFAULT_GATE_FLOOR`` (StatsBomb-relative floor
# ``max(sb_auc - margin, floor)``).
_GATE_MARGIN = 0.05
_GATE_FLOOR = 0.5
# Deterministic bootstrap for the per-provider AUC confidence intervals in the gate evidence.
_GATE_BOOTSTRAP_SEED = 0

BATCH_SIZE = 256
MAX_EPOCHS = 50
LEARNING_RATE = 1e-3
WEIGHT_DECAY = 1e-4
EARLY_STOPPING_PATIENCE = 5
MC_DROPOUT_SAMPLES = 50
N_SPLITS = 5
RANDOM_STATE = 42

HF_ORG = "luxury-lakehouse"
SHOTS_DATASET = f"{HF_ORG}/xg-shot-data-v3"
FREEZE_FRAME_DATASET = f"{HF_ORG}/xg-shot-freeze-frames"
V3_MODEL_REPO = f"{HF_ORG}/xg-v3-model-set-encoder"

CATALOG = "soccer_analytics"
SCHEMA = "dev_gold"
MODEL_NAME = "xg_model_v3"


@dataclasses.dataclass(frozen=True)
class SetEncoderConfig:
    """Immutable configuration for the set encoder architecture."""

    player_feature_dim: int = 4
    encoder_hidden: int = 32
    context_dim: int = 16
    pred_hidden_1: int = 64
    pred_hidden_2: int = 32
    dropout_p: float = 0.1


# ===========================================================================
# PURE, TORCH-FREE HELPERS (unit-tested in src/tests/test_train_xg_v3.py)
# ===========================================================================


def select_training_shots(df: pd.DataFrame) -> pd.DataFrame:
    """Filter to the OPEN-PLAY training shot family (spec §4.1.1).

    Keeps rows with ``action_type IN {shot, shot_freekick}`` (excludes ``shot_penalty``)
    and drops ``action_result == 'yellow_card'`` rows (logging the count). Returns a fresh
    frame; the input is never mutated.
    """
    action_type = df["action_type"].astype("string")
    action_result = df["action_result"].astype("string")
    is_family = action_type.isin(_TRAINING_SHOT_TYPES)
    is_yellow = action_result == _DROP_RESULT

    n_yellow_in_family = int((is_family & is_yellow).sum())
    if n_yellow_in_family:
        logger.info("Dropping %d '%s' shot rows from training", n_yellow_in_family, _DROP_RESULT)

    keep = is_family & ~is_yellow
    return df.loc[keep].reset_index(drop=True)


def label_is_goal(df: pd.DataFrame) -> pd.Series:
    """Goal label from the SPADL outcome: 1 iff ``action_result == 'success'``."""
    return (df["action_result"].astype("string") == _GOAL_RESULT).astype(int)


def parse_freeze_frames_spadl(
    shots_df: pd.DataFrame,
    freeze_df: pd.DataFrame | None,
) -> list[npt.NDArray[np.floating[Any]]]:
    """Per-shot SPADL-normalized player sets from ``bronze.shot_freeze_frames`` rows.

    For each shot (keyed on ``(match_key, action_id)``), collect its freeze-frame player rows, stack
    ``[x, y, is_keeper, is_teammate]`` (raw SPADL metres) and apply the SHARED C2 port
    ``normalize_freeze_frame`` (÷105,÷68 + shooter-attacks-high-x orientation). Shots
    without a freeze frame get an empty ``(0,4)`` array — the trained zero-context path.

    The join key is ``(match_key, action_id)``, NOT ``action_id`` alone: ``action_id`` is per-match,
    not global (§5 invariant — the single most important one). Two matches each carrying a shot with
    the same ``action_id`` would otherwise have their freeze frames unioned onto BOTH shots.
    """
    n_shots = len(shots_df)
    empty = np.empty((0, 4), dtype=np.float64)
    if freeze_df is None or len(freeze_df) == 0:
        logger.info("No freeze-frame data; all %d shots use the zero-context path", n_shots)
        return [empty.copy() for _ in range(n_shots)]

    groups: dict[Any, pd.DataFrame] = dict(iter(freeze_df.groupby(["match_key", "action_id"])))
    result: list[npt.NDArray[np.floating[Any]]] = []
    matched = 0
    for match_key, action_id in zip(shots_df["match_key"], shots_df["action_id"], strict=True):
        group = groups.get((match_key, action_id))
        if group is None or len(group) == 0:
            result.append(empty.copy())
            continue
        players = np.column_stack(
            [
                group["x"].to_numpy(dtype=np.float64),
                group["y"].to_numpy(dtype=np.float64),
                group["is_keeper"].to_numpy(dtype=np.float64),
                group["is_teammate"].to_numpy(dtype=np.float64),
            ]
        )
        raw = group["shooter_attacks_high_x"].iloc[0] if "shooter_attacks_high_x" in group.columns else True
        # NA / missing orientation defaults to attacks-high-x (frames are already home-LTR;
        # the point-reflection only fires for a confirmed away-attacking shot).
        attacks_high = True if pd.isna(raw) else bool(raw)
        result.append(normalize_freeze_frame(players, SPADL_PITCH, shooter_attacks_high_x=attacks_high))
        matched += 1
    logger.info("Freeze-frame matched: %d / %d shots (%.1f%%)", matched, n_shots, 100.0 * matched / max(n_shots, 1))
    return result


def build_spadl_tabular(
    shots_df: pd.DataFrame,
    player_sets: Sequence[npt.NDArray[np.floating[Any]]],
    config: XGModelConfig | None = None,
    expected_features: list[str] | None = None,
) -> tuple[pd.DataFrame, pd.Series]:
    """SPADL-native tabular feature matrix + label via the SHARED ``build_features``.

    Adds the canonical-SPADL geometry (``distance_to_goal`` / ``shot_angle`` from
    ``start_x/start_y`` at goal ``(105, 34)``, ``location_x/y``) and the R3
    ``set_cardinality`` feature (number of players in the shot's freeze frame; 0 for
    zero-context), then delegates to ``analytics.xg_model.build_features`` — the SAME
    entry point the serving scorer calls (M2 parity). Never rescales to StatsBomb units.

    ``set_cardinality`` is taken from ``player_sets`` so every zero-context row is 0,
    matching the shape the scorer's tabular-only mode must reproduce.

    The output feature set is pinned to :data:`UNIFORM_FEATURE_NAMES` (geometry only — no StatsBomb
    categoricals, no ``end_location``/``period``/``minute``): ``build_features`` reindexes to that
    deterministic order, so a provider whose shot rows carry extra columns cannot perturb the trained
    feature vector. Pass an explicit ``expected_features`` only to override this (e.g. re-scoring
    against a legacy envelope's feature order).
    """
    config = config or XGModelConfig()
    df = shots_df.copy().reset_index(drop=True)

    if "start_x" in df.columns and "start_y" in df.columns:
        geom = [spadl_shot_geometry(float(x), float(y)) for x, y in zip(df["start_x"], df["start_y"], strict=True)]
        df["distance_to_goal"] = [g[0] for g in geom]
        df["shot_angle"] = [g[1] for g in geom]
        df["location_x"] = df["start_x"].astype(float)
        df["location_y"] = df["start_y"].astype(float)

    df["set_cardinality"] = [int(np.asarray(ps).shape[0]) for ps in player_sets]
    expected = expected_features if expected_features is not None else list(UNIFORM_FEATURE_NAMES)
    return build_features(df, config, expected_features=expected)


def penalty_goal_rate(df: pd.DataFrame) -> float:
    """Empirical ``shot_penalty`` conversion rate over the LOADED (pre-population-filter) dataset.

    This MUST be computed BEFORE ``select_training_shots`` drops the penalty rows: the penalty-excluded
    training set has zero penalties, so computing the rate there is ``0/0`` and returns ``NaN``. The
    trainer ships this scalar as the envelope's ``_penalty_xg`` constant (fixed-geometry penalties are
    excluded from the geometry model and get this constant xG at scoring time). Returns ``NaN`` when the
    frame carries no penalty rows.
    """
    is_pen = df["action_type"].astype("string") == _PENALTY_SHOT_TYPE
    n = int(is_pen.sum())
    if n == 0:
        return float("nan")
    goals = int((df.loc[is_pen, "action_result"].astype("string") == _GOAL_RESULT).sum())
    return goals / n


def _calibrator_entry(
    raw: npt.NDArray[Any], y: npt.NDArray[Any], groups: npt.NDArray[Any], n_splits: int
) -> dict[str, Any]:
    """Fit ONE leak-free OOF calibrator via ``analytics.xg_calibration.choose_calibrator``.

    Returns a JSON-safe ``{"kind": "platt"|"isotonic", "params": {...}}``. ``choose_calibrator`` prefers
    Platt and only switches to isotonic when isotonic strictly beats it on group-disjoint out-of-fold
    Brier; it is robust to single-class / single-group inputs (degenerate constant Platt), so per-provider
    fitting never raises. We do NOT re-derive any calibration primitive here — the module owns it.
    """
    kind, params = choose_calibrator(raw, y, groups, n_splits=n_splits)
    if isinstance(params, (PlattParams, IsotonicParams)):
        return {"kind": kind, "params": params.to_dict()}
    raise TypeError(f"choose_calibrator returned an unserializable params type: {type(params)!r}")


def fit_calibrators(
    oof_raw: npt.NDArray[Any],
    y: npt.NDArray[Any],
    data_source: npt.NDArray[Any],
    match_keys: npt.NDArray[Any],
    *,
    n_splits: int = N_SPLITS,
) -> dict[str, Any]:
    """Fit per-provider AND pooled OOF calibrators from the model's RAW out-of-fold predictions.

    The model emits RAW (uncalibrated) xG. The trainer fits these calibrators as EVIDENCE + serve-time
    parameters and ships them under the envelope's ``_calibrators`` — it applies NOTHING to the served
    weights. The SCORER (a later task) applies them. Both the pooled calibrator (across all providers)
    and one calibrator per ``data_source`` are fit on group-disjoint out-of-fold predictions
    (GroupKFold-by-``match_key``), so nothing leaks.

    Returns ``{"per_provider": {provider: entry}, "pooled": entry}`` where each ``entry`` is the JSON-safe
    ``{"kind", "params"}`` from :func:`_calibrator_entry`.
    """
    raw = np.asarray(oof_raw, dtype=np.float64)
    labels = np.asarray(y)
    providers = np.asarray(data_source)
    groups = np.asarray(match_keys)

    per_provider: dict[str, Any] = {}
    for prov in np.unique(providers):
        mask = providers == prov
        per_provider[str(prov)] = _calibrator_entry(raw[mask], labels[mask], groups[mask], n_splits)
    return {"per_provider": per_provider, "pooled": _calibrator_entry(raw, labels, groups, n_splits)}


def _apply_calibrator_to_raw(
    raw: npt.NDArray[np.float64], provider: str, calibrators: dict[str, Any]
) -> npt.NDArray[np.float64]:
    """Apply a provider's fitted calibrator to raw xG (pooled fallback), MIRRORING the scorer's
    ``ingestion.xg_shot_scorer.calibrate_xg``: per-provider entry first, else the pooled entry.

    Used only to compute the aggregate-calibration inputs (``sum_xg``) for the gate evidence — the
    served weights stay RAW; this is evidence, not a transform applied to the model.
    """
    per_provider = calibrators.get("per_provider") or {}
    entry = per_provider.get(provider) or calibrators.get("pooled")
    if entry is None:
        # No calibrator at all — return raw (the gate's calibration check then uses raw sums).
        return np.asarray(raw, dtype=np.float64)
    kind, params = entry["kind"], entry["params"]
    if kind == "platt":
        return apply_platt(raw, PlattParams.from_dict(params))
    if kind == "isotonic":
        return apply_isotonic(raw, IsotonicParams.from_dict(params))
    raise ValueError(f"Unknown calibrator kind {kind!r} (expected 'platt' or 'isotonic')")


def build_gate_evidence(
    oof: dict[str, npt.NDArray[Any]],
    calibrators: dict[str, Any],
    *,
    margin: float = _GATE_MARGIN,
    floor: float = _GATE_FLOOR,
    n_boot: int = 2000,
    seed: int = _GATE_BOOTSTRAP_SEED,
) -> dict[str, Any]:
    """Build the ``_gate`` evidence block the scorer's ``parse_gate`` reads verbatim (Task 1.4-1.7 follow-on).

    Without this block the scorer fail-safes EVERY provider to ``tabular_only + ood_flag=True`` (and the
    consumer drops ood-flagged cohorts), so nothing certifies. From the leak-free GroupKFold OOF bundle
    (``crossval_oos_both_modes``) this emits, per provider:

      - ``context`` / ``tabular``: the ``bootstrap_auc_ci`` AUC + CI on that provider's held-out
        CONTEXT-aware and TABULAR-only RAW predictions (``{"auc", "lo", "hi"}``);
      - ``sum_xg``: sum of the CALIBRATED context-mode xG (provider calibrator, pooled fallback — mirrors
        the scorer), ``sum_goals``: sum of the OOF labels, ``n``: row count — the ``calibration_ok_n_aware``
        inputs.

    ``sb_auc`` is StatsBomb's context-aware OOF AUC point estimate (SB-360 context is in this full-corpus
    model); if StatsBomb has no two-class context OOF this run, it falls back to the best-available
    provider's context AUC, else ``0.0`` (which makes ``floor`` the binding bar — safe). ``margin`` /
    ``floor`` are shipped so the scorer gates against the trainer's thresholds.

    A provider whose held-out labels are single-class (AUC undefined) is OMITTED — the scorer then
    fail-safes it to ``tabular_only`` + flagged, which is the correct conservative behavior.
    """
    proba = np.asarray(oof["proba"], dtype=np.float64)
    tab_proba = np.asarray(oof["tabular_proba"], dtype=np.float64)
    y = np.asarray(oof["y"])
    data_source = np.asarray(oof["data_source"])

    per_provider: dict[str, Any] = {}
    context_auc: dict[str, float] = {}
    for prov in np.unique(data_source):
        mask = data_source == prov
        yv = y[mask]
        if np.unique(yv).size < 2:
            continue  # AUC undefined → no evidence → scorer fail-safes this provider (conservative).
        c_auc, c_lo, c_hi = bootstrap_auc_ci(proba[mask], yv, n_boot=n_boot, seed=seed)
        t_auc, t_lo, t_hi = bootstrap_auc_ci(tab_proba[mask], yv, n_boot=n_boot, seed=seed)
        calibrated_ctx = _apply_calibrator_to_raw(proba[mask], str(prov), calibrators)
        per_provider[str(prov)] = {
            "context": {"auc": c_auc, "lo": c_lo, "hi": c_hi},
            "tabular": {"auc": t_auc, "lo": t_lo, "hi": t_hi},
            "sum_xg": float(np.sum(calibrated_ctx)),
            "sum_goals": float(np.sum(yv.astype(np.float64))),
            "n": int(yv.size),
        }
        context_auc[str(prov)] = c_auc

    sb_auc = context_auc.get("statsbomb")
    if sb_auc is None:
        sb_auc = max(context_auc.values()) if context_auc else 0.0
    return {
        "sb_auc": float(sb_auc),
        "margin": float(margin),
        "floor": float(floor),
        "per_provider": per_provider,
    }


def build_weight_envelope(
    numpy_weights: dict[str, npt.NDArray[np.floating[Any]]],
    feature_names: list[str],
    *,
    isotonic_x: npt.NDArray[np.floating[Any]] | None = None,
    isotonic_y: npt.NDArray[np.floating[Any]] | None = None,
    mc_z_multiplier: float | None = None,
    mc_dropout_p_inference: float | None = None,
    calibrators: dict[str, Any] | None = None,
    penalty_xg: float | None = None,
    gate: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Serialize weights and inject the ADR-012 §2 envelope contract fields.

    Injects, at the envelope top level:
      - ``feature_names``: the trained tabular feature order (inference reindexes to it);
      - ``tabular_dim``: ``len(feature_names)`` (redundant consistency check);
      - ``coordinate_system``: ``"spadl_105x68"`` — guards a v2(yards)/v3(SPADL) mixup.

    Optional isotonic-calibrator thresholds + MC-dropout inference params ride alongside
    the weights (mirrors the v2 trainer's ``_isotonic_*`` / ``_mc_*`` sidecar arrays).

    Single-calibration ownership (Task 1.6): the served weights stay RAW. The per-provider + pooled OOF
    calibrators ride under ``_calibrators`` and the excluded-shot-type penalty constant under
    ``_penalty_xg`` — both are EVIDENCE / serve-time parameters the SCORER applies; nothing is baked
    into the weight arrays. The ``_gate`` two-mode-gate evidence (per-provider AUC-CIs + calibration
    sums) rides alongside — the scorer's ``parse_gate`` reads it verbatim to CERTIFY context-aware modes.
    """
    weights = dict(numpy_weights)
    if isotonic_x is not None and isotonic_y is not None:
        weights["_isotonic_X"] = np.asarray(isotonic_x, dtype=np.float64)
        weights["_isotonic_y"] = np.asarray(isotonic_y, dtype=np.float64)
    if mc_z_multiplier is not None:
        weights["_mc_z_multiplier"] = np.array([mc_z_multiplier], dtype=np.float64)
    if mc_dropout_p_inference is not None:
        weights["_mc_dropout_p_inference"] = np.array([mc_dropout_p_inference], dtype=np.float64)

    envelope = json.loads(serialize_set_encoder_weights(weights).decode("utf-8"))
    envelope["feature_names"] = list(feature_names)
    envelope["tabular_dim"] = len(feature_names)
    envelope["coordinate_system"] = COORDINATE_SYSTEM
    if calibrators is not None:
        envelope["_calibrators"] = calibrators
    if penalty_xg is not None:
        envelope["_penalty_xg"] = float(penalty_xg)
    if gate is not None:
        envelope["_gate"] = gate
    return envelope


def serialize_envelope(envelope: dict[str, Any]) -> bytes:
    """Serialize the finalized envelope dict to JSON bytes (no pickle)."""
    return json.dumps(envelope).encode("utf-8")


def groupkfold_splits(
    match_keys: npt.NDArray[Any] | Sequence[Any],
    n_splits: int = N_SPLITS,
) -> list[tuple[npt.NDArray[np.intp], npt.NDArray[np.intp]]]:
    """GroupKFold-by-``match_key`` train/test index splits — no same-match leakage.

    Every fold's fit-group set is disjoint from its measure-group set, so the GS/SC OOS
    discrimination reported per provider is measured on held-out matches (spec §4.1).
    """
    groups = np.asarray(match_keys)
    gkf = GroupKFold(n_splits=n_splits)
    x_placeholder = np.zeros((len(groups), 1))
    return [(tr.astype(np.intp), te.astype(np.intp)) for tr, te in gkf.split(x_placeholder, groups=groups)]


def compute_metrics(y_true: npt.NDArray[Any], proba: npt.NDArray[Any]) -> dict[str, float]:
    """OOS metric bundle: ROC-AUC, Brier, Brier-skill vs base rate, ECE (10 uniform bins)."""
    y = np.asarray(y_true, dtype=np.float64)
    p = np.clip(np.asarray(proba, dtype=np.float64), 1e-15, 1 - 1e-15)
    base_rate = float(y.mean()) if len(y) else 0.0
    brier = float(brier_score_loss(y, p))
    brier_base = float(brier_score_loss(y, np.full_like(y, base_rate))) if len(y) else float("nan")
    brier_skill = 1.0 - brier / brier_base if brier_base > 0 else float("nan")
    # Expected calibration error over 10 uniform-width bins.
    bins = np.linspace(0.0, 1.0, 11)
    idx = np.clip(np.digitize(p, bins) - 1, 0, 9)
    ece = 0.0
    for b in range(10):
        mask = idx == b
        if mask.any():
            ece += (mask.mean()) * abs(p[mask].mean() - y[mask].mean())
    auc = float(roc_auc_score(y, p)) if len(np.unique(y)) > 1 else float("nan")
    return {
        "roc_auc": auc,
        "brier": brier,
        "brier_skill": float(brier_skill),
        "ece": float(ece),
    }


def _read_repo_parquet(api: Any, downloader: Any, repo_id: str, hf_token: str) -> pd.DataFrame:
    """Concatenate every parquet file in an HF dataset repo (empty frame if the repo is absent/empty).

    Mirrors the enumerate-then-download path ``main`` uses for the public repo: ``list_repo_tree`` +
    ``hf_hub_download`` + ``pd.read_parquet``. A missing repo (``RepositoryNotFoundError``) or a repo
    with no parquet files yields an EMPTY frame — the caller decides whether that is fail-loud.
    """
    from huggingface_hub.utils import RepositoryNotFoundError

    try:
        items = list(api.list_repo_tree(repo_id, repo_type="dataset", recursive=True))
    except RepositoryNotFoundError:
        return pd.DataFrame()
    parquet = [f.path for f in items if hasattr(f, "size") and f.path.endswith(".parquet")]
    if not parquet:
        return pd.DataFrame()
    return pd.concat(
        [pd.read_parquet(downloader(repo_id, p, repo_type="dataset", token=hf_token)) for p in parquet],
        ignore_index=True,
    )


def load_dataset_both_repos(api: Any, repo_id: str, hf_token: str) -> pd.DataFrame:
    """Load + concatenate parquet from the PUBLIC repo AND its ADR-049 ``-restricted`` companion.

    The restricted cohorts (SkillCorner / GradientSports) live in a permanent private companion repo
    (``<repo_id>-restricted``, org-members only) per ADR-049 / ADR-064. Training on the public repo
    alone would silently drop them — so this reads BOTH and fails LOUD (raises ``RuntimeError``,
    ERROR-level, non-zero exit) when the restricted companion yields zero rows, rather than quietly
    training public-only. The consumer records both commit SHAs in provenance (see ``main``).

    Requires an org-scoped ``hf_token`` (the restricted repos are private).
    """
    from huggingface_hub import hf_hub_download

    public_df = _read_repo_parquet(api, hf_hub_download, repo_id, hf_token)
    if public_df.empty:
        raise RuntimeError(f"No parquet rows found in public dataset {repo_id}")

    restricted_id = restricted_repo_id(repo_id)
    restricted_df = _read_repo_parquet(api, hf_hub_download, restricted_id, hf_token)
    if restricted_df.empty:
        raise RuntimeError(
            f"Restricted companion {restricted_id} yielded zero rows — refusing to silently train "
            "public-only. The private cohort (SkillCorner / GradientSports) is expected (ADR-049/ADR-064); "
            "check the org-scoped HF token and that the companion repo has been published."
        )
    logger.info(
        "Loaded %s: %d public + %d restricted = %d rows",
        repo_id,
        len(public_df),
        len(restricted_df),
        len(public_df) + len(restricted_df),
    )
    return pd.concat([public_df, restricted_df], ignore_index=True)


# ===========================================================================
# TORCH-DEPENDENT TRAINING CODE (HF Jobs only; guarded so the module imports
# without torch for the pure-helper unit tests)
# ===========================================================================

if _TORCH_AVAILABLE:

    class SetEncoderXG(nn.Module):
        """Deep Sets xG model: per-player encoder + sum pooling + prediction MLP."""

        def __init__(self, tabular_dim: int, config: SetEncoderConfig) -> None:
            super().__init__()
            self.config = config
            self.encoder = nn.Sequential(
                nn.Linear(config.player_feature_dim, config.encoder_hidden),
                nn.ReLU(),
                nn.Linear(config.encoder_hidden, config.context_dim),
                nn.ReLU(),
            )
            self.predictor = nn.Sequential(
                nn.Linear(tabular_dim + config.context_dim, config.pred_hidden_1),
                nn.ReLU(),
                nn.Dropout(config.dropout_p),
                nn.Linear(config.pred_hidden_1, config.pred_hidden_2),
                nn.ReLU(),
                nn.Dropout(config.dropout_p),
                nn.Linear(config.pred_hidden_2, 1),
            )

        def forward(self, tabular: torch.Tensor, all_players: torch.Tensor, set_sizes: torch.Tensor) -> torch.Tensor:
            batch_size = tabular.shape[0]
            device = tabular.device
            context_dim = self.config.context_dim
            if all_players.shape[0] > 0:
                encoded = self.encoder(all_players)
                shot_indices = torch.repeat_interleave(torch.arange(batch_size, device=device), set_sizes)
                context = torch.zeros(batch_size, context_dim, device=device)
                context.scatter_add_(0, shot_indices.unsqueeze(1).expand_as(encoded), encoded)
            else:
                context = torch.zeros(batch_size, context_dim, device=device)
            return self.predictor(torch.cat([tabular, context], dim=1))

    class ShotDataset(torch.utils.data.Dataset):  # type: ignore[type-arg]
        """Shots with variable-size freeze-frame player sets."""

        def __init__(
            self,
            tabular: npt.NDArray[np.floating[Any]],
            player_sets: list[npt.NDArray[np.floating[Any]]],
            targets: npt.NDArray[np.integer[Any]],
        ) -> None:
            self.tabular = tabular.astype(np.float32)
            self.player_sets = player_sets
            self.targets = targets.astype(np.float32)

        def __len__(self) -> int:
            return len(self.targets)

        def __getitem__(self, idx: int) -> tuple[torch.Tensor, torch.Tensor, torch.Tensor]:
            return (
                torch.from_numpy(self.tabular[idx]),
                torch.from_numpy(self.player_sets[idx].astype(np.float32)),
                torch.tensor(self.targets[idx], dtype=torch.float32),
            )


def collate_fn(batch: list[Any]) -> tuple[Any, Any, Any, Any]:
    """Pack variable-size player sets into a flat tensor + per-shot sizes."""
    tabular_list: list[Any] = []
    player_list: list[Any] = []
    size_list: list[int] = []
    target_list: list[Any] = []
    for tab, players, target in batch:
        tabular_list.append(tab)
        player_list.append(players)
        size_list.append(len(players))
        target_list.append(target)
    tabular = torch.stack(tabular_list)
    targets = torch.stack(target_list)
    set_sizes = torch.tensor(size_list, dtype=torch.long)
    all_players = torch.cat(player_list) if sum(size_list) > 0 else torch.empty(0, 4, dtype=torch.float32)
    return tabular, all_players, set_sizes, targets


def _make_loader(
    tabular: npt.NDArray[np.floating[Any]],
    player_sets: list[npt.NDArray[np.floating[Any]]],
    targets: npt.NDArray[np.integer[Any]],
    *,
    shuffle: bool,
    device: Any,
) -> Any:
    return torch.utils.data.DataLoader(
        ShotDataset(tabular, player_sets, targets),
        batch_size=BATCH_SIZE,
        shuffle=shuffle,
        collate_fn=collate_fn,
        num_workers=2,
        pin_memory=getattr(device, "type", "cpu") == "cuda",
    )


def train_model(
    model: Any,
    train_loader: Any,
    val_loader: Any,
    device: Any,
) -> dict[str, list[float]]:
    """Train with BCE loss + Adam + early stopping on validation Brier score."""
    optimizer = torch.optim.Adam(model.parameters(), lr=LEARNING_RATE, weight_decay=WEIGHT_DECAY)
    criterion = nn.BCEWithLogitsLoss()
    best_val_brier = float("inf")
    patience_counter = 0
    best_state: dict[str, Any] = {}
    history: dict[str, list[float]] = {"train_loss": [], "val_brier": [], "val_auc": []}

    for epoch in range(MAX_EPOCHS):
        epoch_start = time.time()
        model.train()
        total_loss = 0.0
        n_batches = 0
        for tabular, all_players, set_sizes, targets in train_loader:
            tabular = tabular.to(device)
            all_players = all_players.to(device)
            set_sizes = set_sizes.to(device)
            targets = targets.to(device)
            optimizer.zero_grad()
            logits = model(tabular, all_players, set_sizes).squeeze(1)
            loss = criterion(logits, targets)
            loss.backward()
            optimizer.step()
            total_loss += loss.item()
            n_batches += 1

        proba, targets_arr = _predict(model, val_loader, device)
        val_brier = float(brier_score_loss(targets_arr, proba))
        val_auc = float(roc_auc_score(targets_arr, proba)) if len(np.unique(targets_arr)) > 1 else float("nan")
        history["train_loss"].append(total_loss / max(n_batches, 1))
        history["val_brier"].append(val_brier)
        history["val_auc"].append(val_auc)
        logger.info(
            "Epoch %d/%d — loss=%.4f val_brier=%.4f val_auc=%.4f (%.1fs)",
            epoch + 1,
            MAX_EPOCHS,
            history["train_loss"][-1],
            val_brier,
            val_auc,
            time.time() - epoch_start,
        )
        if val_brier < best_val_brier:
            best_val_brier = val_brier
            patience_counter = 0
            best_state = {k: v.clone() for k, v in model.state_dict().items()}
        else:
            patience_counter += 1
            if patience_counter >= EARLY_STOPPING_PATIENCE:
                logger.info("Early stopping at epoch %d", epoch + 1)
                break
    if best_state:
        model.load_state_dict(best_state)
    return history


def _predict(model: Any, loader: Any, device: Any) -> tuple[npt.NDArray[Any], npt.NDArray[Any]]:
    """Forward pass over a loader → (proba, targets)."""
    model.eval()
    proba: list[float] = []
    targets: list[float] = []
    with torch.no_grad():
        for tabular, all_players, set_sizes, tgt in loader:
            logits = model(tabular.to(device), all_players.to(device), set_sizes.to(device)).squeeze(1)
            proba.extend(torch.sigmoid(logits).cpu().numpy().tolist())
            targets.extend(tgt.numpy().tolist())
    return np.array(proba), np.array(targets)


def evaluate_mc_dropout(
    model: Any,
    loader: Any,
    device: Any,
    *,
    n_samples: int = MC_DROPOUT_SAMPLES,
    config: SetEncoderConfig | None = None,
) -> dict[str, float]:
    """MC-dropout empirical 95% coverage + the inference z-multiplier / dropout-p."""
    config = config or SetEncoderConfig()
    _, targets_arr = _predict(model, loader, device)
    n_total = len(targets_arr)
    mc_dropout_p = min(config.dropout_p * 3.0, 0.5)
    for module in model.modules():
        if isinstance(module, torch.nn.Dropout):
            module.p = mc_dropout_p

    mc_predictions = np.zeros((n_samples, n_total), dtype=np.float64)
    for s in range(n_samples):
        model.train()
        idx = 0
        with torch.no_grad():
            for tabular, all_players, set_sizes, _tgt in loader:
                logits = model(tabular.to(device), all_players.to(device), set_sizes.to(device)).squeeze(1)
                p = torch.sigmoid(logits).cpu().numpy()
                mc_predictions[s, idx : idx + len(p)] = p
                idx += len(p)
    model.eval()
    for module in model.modules():
        if isinstance(module, torch.nn.Dropout):
            module.p = config.dropout_p

    means = mc_predictions.mean(axis=0)
    stds = mc_predictions.std(axis=0)
    best_z = 1.96
    for z in np.arange(1.0, 6.0, 0.1):
        lo = np.clip(means - z * stds, 0.0, 1.0)
        hi = np.clip(means + z * stds, 0.0, 1.0)
        if float(((targets_arr >= lo) & (targets_arr <= hi)).sum()) / max(n_total, 1) >= 0.95:
            best_z = float(z)
            break
    lo = np.clip(means - best_z * stds, 0.0, 1.0)
    hi = np.clip(means + best_z * stds, 0.0, 1.0)
    covered = ((targets_arr >= lo) & (targets_arr <= hi)).sum()
    return {
        "mc_coverage_95": float(covered) / max(n_total, 1),
        "mc_mean_std": float(np.mean(stds)),
        "mc_z_multiplier": best_z,
        "mc_dropout_p_inference": mc_dropout_p,
    }


def export_weights_to_numpy(model: Any) -> dict[str, npt.NDArray[np.floating[Any]]]:
    """PyTorch state_dict → NumPy weight dict in the set_encoder serialization layout."""
    sd = model.state_dict()
    return {
        "encoder_fc1_weight": sd["encoder.0.weight"].cpu().numpy().astype(np.float64),
        "encoder_fc1_bias": sd["encoder.0.bias"].cpu().numpy().astype(np.float64),
        "encoder_fc2_weight": sd["encoder.2.weight"].cpu().numpy().astype(np.float64),
        "encoder_fc2_bias": sd["encoder.2.bias"].cpu().numpy().astype(np.float64),
        "pred_fc1_weight": sd["predictor.0.weight"].cpu().numpy().astype(np.float64),
        "pred_fc1_bias": sd["predictor.0.bias"].cpu().numpy().astype(np.float64),
        "pred_fc2_weight": sd["predictor.3.weight"].cpu().numpy().astype(np.float64),
        "pred_fc2_bias": sd["predictor.3.bias"].cpu().numpy().astype(np.float64),
        "pred_fc3_weight": sd["predictor.6.weight"].cpu().numpy().astype(np.float64),
        "pred_fc3_bias": sd["predictor.6.bias"].cpu().numpy().astype(np.float64),
    }


def _train_one_fold(
    x_train: npt.NDArray[np.floating[Any]],
    train_players: list[npt.NDArray[np.floating[Any]]],
    y_train: npt.NDArray[np.integer[Any]],
    x_val: npt.NDArray[np.floating[Any]],
    val_players: list[npt.NDArray[np.floating[Any]]],
    y_val: npt.NDArray[np.integer[Any]],
    *,
    tabular_dim: int,
    config: SetEncoderConfig,
    device: Any,
) -> Any:
    """Train a fresh model on one fold's train split; return the fitted model."""
    torch.manual_seed(RANDOM_STATE)
    model = SetEncoderXG(tabular_dim=tabular_dim, config=config).to(device)
    train_loader = _make_loader(x_train, train_players, y_train, shuffle=True, device=device)
    val_loader = _make_loader(x_val, val_players, y_val, shuffle=False, device=device)
    train_model(model, train_loader, val_loader, device)
    return model


def crossval_oos_both_modes(
    x_tabular: pd.DataFrame,
    player_sets: list[npt.NDArray[np.floating[Any]]],
    y: npt.NDArray[np.integer[Any]],
    match_keys: npt.NDArray[Any],
    data_source: npt.NDArray[Any],
    *,
    tabular_dim: int,
    config: SetEncoderConfig,
    device: Any,
) -> tuple[dict[str, dict[str, dict[str, float]]], dict[str, npt.NDArray[Any]]]:
    """GroupKFold-OOS metrics per provider, in BOTH scoring modes, plus the raw OOF bundle.

    For each fold: train on the fold-train split (context-aware features), then score the
    held-out fold two ways —
      (a) ``context_aware``: real freeze frame + real ``set_cardinality``;
      (b) ``tabular_only``:  zeroed player set + ``set_cardinality = 0`` (the trained
          zero-context path — matches the scorer's tabular-only mode exactly).

    Returns ``(report, oof)`` where ``report`` is ``{provider: {mode: metric_bundle}}`` plus an ``"all"``
    provider bucket, and ``oof`` carries the pooled CONTEXT-AWARE out-of-fold RAW predictions aligned by
    row (``proba`` / ``y`` / ``data_source`` / ``match_key``) — the leak-free basis the trainer fits its
    per-provider + pooled calibrators on (Task 1.6). Calibrators use the context-aware OOF because that is
    the RAW output the served model produces with a real freeze frame.
    """
    x_values = x_tabular.to_numpy(dtype=np.float64)
    # set_cardinality column index (for the tabular-only zeroing of the held-out fold).
    card_idx = list(x_tabular.columns).index("set_cardinality") if "set_cardinality" in x_tabular.columns else None
    zero_players = np.empty((0, 4), dtype=np.float64)

    oos: dict[str, dict[str, list[float]]] = {}  # provider -> mode -> pooled proba
    oos_y: dict[str, dict[str, list[float]]] = {}
    # Flat, row-aligned pooled OOF accumulators (for leak-free calibrator fitting + gate evidence).
    # ``proba`` is the CONTEXT-aware RAW prediction; ``tabular_proba`` is the TABULAR-only RAW
    # prediction for the SAME held-out rows — both aligned to (y / data_source / match_key).
    oof_proba: list[float] = []
    oof_tab_proba: list[float] = []
    oof_y: list[float] = []
    oof_provider: list[Any] = []
    oof_match_key: list[Any] = []

    def _record(provider: str, mode: str, proba: npt.NDArray[Any], ytrue: npt.NDArray[Any]) -> None:
        oos.setdefault(provider, {}).setdefault(mode, []).extend(proba.tolist())
        oos_y.setdefault(provider, {}).setdefault(mode, []).extend(ytrue.tolist())

    for fold, (train_idx, test_idx) in enumerate(groupkfold_splits(match_keys, N_SPLITS)):
        model = _train_one_fold(
            x_values[train_idx],
            [player_sets[i] for i in train_idx],
            y[train_idx],
            x_values[test_idx],
            [player_sets[i] for i in test_idx],
            y[test_idx],
            tabular_dim=tabular_dim,
            config=config,
            device=device,
        )
        # (a) context-aware: real player sets.
        ctx_players = [player_sets[i] for i in test_idx]
        ctx_loader = _make_loader(x_values[test_idx], ctx_players, y[test_idx], shuffle=False, device=device)
        ctx_proba, ctx_y = _predict(model, ctx_loader, device)
        # (b) tabular-only: zero context + set_cardinality forced to 0.
        x_tab = x_values[test_idx].copy()
        if card_idx is not None:
            x_tab[:, card_idx] = 0.0
        tab_players = [zero_players for _ in test_idx]
        tab_loader = _make_loader(x_tab, tab_players, y[test_idx], shuffle=False, device=device)
        tab_proba, tab_y = _predict(model, tab_loader, device)

        providers = data_source[test_idx]
        for prov in np.unique(providers):
            pmask = providers == prov
            _record(str(prov), "context_aware", ctx_proba[pmask], ctx_y[pmask])
            _record(str(prov), "tabular_only", tab_proba[pmask], tab_y[pmask])
        _record("all", "context_aware", ctx_proba, ctx_y)
        _record("all", "tabular_only", tab_proba, tab_y)
        # Row-aligned OOF (RAW) for calibrator fitting + gate evidence: the held-out rows in THIS
        # fold, in BOTH modes (ctx_y == tab_y == y[test_idx], so a single y stream aligns to both).
        oof_proba.extend(ctx_proba.tolist())
        oof_tab_proba.extend(tab_proba.tolist())
        oof_y.extend(ctx_y.tolist())
        oof_provider.extend(providers.tolist())
        oof_match_key.extend(match_keys[test_idx].tolist())
        logger.info("OOS fold %d/%d scored (%d held-out shots)", fold + 1, N_SPLITS, len(test_idx))

    report: dict[str, dict[str, dict[str, float]]] = {}
    for provider, modes in oos.items():
        report[provider] = {}
        for mode, proba in modes.items():
            report[provider][mode] = compute_metrics(np.array(oos_y[provider][mode]), np.array(proba))
    oof: dict[str, npt.NDArray[Any]] = {
        "proba": np.array(oof_proba, dtype=np.float64),
        "tabular_proba": np.array(oof_tab_proba, dtype=np.float64),
        "y": np.array(oof_y),
        "data_source": np.array(oof_provider),
        "match_key": np.array(oof_match_key),
    }
    return report, oof


# ===========================================================================
# Entry point (HF Jobs)
# ===========================================================================


@workflow("wf-xg-v2", phase="training")
def main() -> None:
    """Load shots + freeze frames, retrain xG v3 (SPADL-native), deliver via ADR-012."""
    _assert_silly_kicks_min()

    from huggingface_hub import HfApi, get_token

    # Pre-flight: fail loud if MLflow registration env vars are missing (ADR-002/012).
    require_mlflow_env()

    pipeline_start = time.time()
    hf_token = os.environ.get("HF_TOKEN", "") or (get_token() or "")
    if not hf_token:
        raise RuntimeError("HF_TOKEN environment variable required")

    api = HfApi(token=hf_token)
    recorder = HFJobsCostRecorder(
        workflow_id="wf-xg-v2",
        phase="training",
        rate_usd_per_hour=HF_RATE_A10G_LARGE,
        repo_id=V3_MODEL_REPO,
        repo_type="model",
    )
    # Create the model repo EARLY: the cost recorder's start()/complete() upload cost telemetry to
    # V3_MODEL_REPO before the publish step runs, so the repo must exist first or those fire-and-forget
    # uploads 404. The later create_repo(..., exist_ok=True) in the publish step is then a no-op.
    api.create_repo(V3_MODEL_REPO, exist_ok=True, repo_type="model", token=hf_token)
    recorder.start()
    device = torch.device("cuda" if torch.cuda.is_available() else "cpu")
    logger.info("Using device: %s", device)

    def _both_repo_shas(repo_id: str) -> dict[str, str]:
        """Commit SHAs of a public dataset repo AND its ``-restricted`` companion (provenance)."""
        return {
            "public": api.repo_info(repo_id=repo_id, repo_type="dataset").sha,
            "restricted": api.repo_info(repo_id=restricted_repo_id(repo_id), repo_type="dataset").sha,
        }

    # 1. Load the SPADL action-stream shot rows (all providers) — public repo AND its restricted
    # companion (ADR-049/ADR-064); fail-loud if the private cohort is missing (never train public-only).
    logger.info("=== Loading shot data (SPADL action stream, public + restricted) ===")
    shots = load_dataset_both_repos(api, SHOTS_DATASET, hf_token)
    shots_commit = _both_repo_shas(SHOTS_DATASET)

    # Penalty constant (Task 1.6): the empirical shot_penalty conversion rate MUST be measured on the
    # LOADED corpus, BEFORE select_training_shots drops the penalty rows (afterwards it is 0/0 -> NaN).
    penalty_xg = penalty_goal_rate(shots)
    logger.info("Empirical shot_penalty xG constant (pre-filter): %.4f", penalty_xg)

    # Corpus-composition assertion: the restricted cohorts (SkillCorner / GradientSports) MUST be present
    # — a loud check that the private data actually made it in (not silently dropped upstream).
    provider_counts = shots["data_source"].astype(str).value_counts()
    n_skillcorner = int(provider_counts.get("skillcorner", 0))
    n_restricted = int(sum(provider_counts.get(p, 0) for p in _RESTRICTED_COHORT_PROVIDERS))
    logger.info("Corpus: %d skillcorner shots, %d restricted-cohort shots", n_skillcorner, n_restricted)
    if n_restricted == 0:
        raise RuntimeError(
            "Corpus-composition check failed: zero restricted-cohort "
            f"({sorted(_RESTRICTED_COHORT_PROVIDERS)}) shots after loading both repos — the restricted "
            "companion loaded but carried no tracking-cohort shots. Refusing to train a partial corpus."
        )

    # 2. Population filter (spec §4.1.1) + goal label.
    shots = select_training_shots(shots).reset_index(drop=True)
    shots["is_goal"] = label_is_goal(shots)
    logger.info("Training shots after population filter: %d (goal rate %.4f)", len(shots), shots["is_goal"].mean())

    # 3. Load freeze frames (bronze.shot_freeze_frames) — public repo AND its restricted companion.
    logger.info("=== Loading freeze frames (public + restricted) ===")
    freeze_df: pd.DataFrame | None = load_dataset_both_repos(api, FREEZE_FRAME_DATASET, hf_token)
    ff_commit = _both_repo_shas(FREEZE_FRAME_DATASET)
    logger.info("Freeze-frame rows: %d", len(freeze_df))

    # 4. Feature assembly (SHARED build_features — M2 parity) + SPADL-normalized player sets.
    logger.info("=== Building SPADL-native features ===")
    config = XGModelConfig()
    player_sets = parse_freeze_frames_spadl(shots, freeze_df)
    x_tabular, y_series = build_spadl_tabular(shots, player_sets, config)
    y = y_series.to_numpy()
    tabular_dim = x_tabular.shape[1]
    feature_names = list(x_tabular.columns)
    match_keys = shots["match_key"].to_numpy()
    data_source = shots["data_source"].astype(str).to_numpy()
    logger.info("Tabular dim: %d | features: %s", tabular_dim, feature_names)

    enc_config = SetEncoderConfig()

    # 5. OOS report — BOTH scoring modes per provider (GroupKFold-by-match, spec §4.3/§5.3).
    logger.info("=== GroupKFold OOS (both modes, per provider) ===")
    oos_report, oof = crossval_oos_both_modes(
        x_tabular,
        player_sets,
        y,
        match_keys,
        data_source,
        tabular_dim=tabular_dim,
        config=enc_config,
        device=device,
    )
    for provider, modes in oos_report.items():
        for mode, m in modes.items():
            logger.info(
                "OOS[%s/%s] auc=%.4f brier=%.4f brier_skill=%.4f ece=%.4f",
                provider,
                mode,
                m["roc_auc"],
                m["brier"],
                m["brier_skill"],
                m["ece"],
            )
    sb_auc = oos_report.get("statsbomb", {}).get("context_aware", {}).get("roc_auc", float("nan"))
    logger.info("StatsBomb context-aware OOS AUC (relative-floor reference for §5.3): %.4f", sb_auc)

    # 6. Fit the shipped champion on ALL data.
    logger.info("=== Training champion on all shots ===")
    full_loader = _make_loader(x_tabular.to_numpy(dtype=np.float64), player_sets, y, shuffle=True, device=device)
    torch.manual_seed(RANDOM_STATE)
    model = SetEncoderXG(tabular_dim=tabular_dim, config=enc_config).to(device)
    logger.info("Model parameters: %d", sum(p.numel() for p in model.parameters()))
    history = train_model(model, full_loader, full_loader, device)

    # 7. Isotonic calibration on pooled OOS context-aware predictions (leakage-free).
    from sklearn.isotonic import IsotonicRegression

    eval_loader = _make_loader(x_tabular.to_numpy(dtype=np.float64), player_sets, y, shuffle=False, device=device)
    raw_proba, raw_targets = _predict(model, eval_loader, device)
    ir = IsotonicRegression(y_min=0.0, y_max=1.0, out_of_bounds="clip")
    ir.fit(raw_proba, raw_targets)
    mc_metrics = evaluate_mc_dropout(model, eval_loader, device, config=enc_config)

    # 7b. Single-calibration ownership (Task 1.6): fit per-provider + pooled OOF calibrators from the
    # leak-free GroupKFold context-aware out-of-fold RAW predictions. The model output stays RAW; the
    # scorer (a later task) applies these. Nothing is applied to the served weights here.
    calibrators = fit_calibrators(oof["proba"], oof["y"], oof["data_source"], oof["match_key"])
    logger.info(
        "Fitted calibrators — pooled=%s, per-provider=%s",
        calibrators["pooled"]["kind"],
        {p: e["kind"] for p, e in calibrators["per_provider"].items()},
    )

    # 7c. Two-mode-gate evidence (Task 1.4-1.7 follow-on): per-provider AUC-CIs (context vs tabular) +
    # calibration sums, shipped under _gate so the scorer's parse_gate can CERTIFY context-aware modes.
    # Without it the scorer fail-safes every provider to tabular_only + ood_flag=True (nothing certifies).
    gate = build_gate_evidence(oof, calibrators)
    logger.info(
        "Gate evidence — sb_auc=%.4f, providers=%s",
        gate["sb_auc"],
        sorted(gate["per_provider"]),
    )

    # 8. Export weights + build the ADR-012 §2 envelope (feature_names/tabular_dim/coord system +
    # the Task 1.6 _calibrators / _penalty_xg serve-time evidence).
    numpy_weights = export_weights_to_numpy(model)
    envelope = build_weight_envelope(
        numpy_weights,
        feature_names,
        isotonic_x=np.asarray(ir.X_thresholds_, dtype=np.float64),
        isotonic_y=np.asarray(ir.y_thresholds_, dtype=np.float64),
        mc_z_multiplier=mc_metrics["mc_z_multiplier"],
        mc_dropout_p_inference=mc_metrics["mc_dropout_p_inference"],
        calibrators=calibrators,
        penalty_xg=None if np.isnan(penalty_xg) else penalty_xg,
        gate=gate,
    )
    weight_bytes = serialize_envelope(envelope)

    # Roundtrip guard on the injected contract fields (ADR-012 §2).
    rt = json.loads(weight_bytes.decode("utf-8"))
    if rt.get("feature_names") != feature_names or rt.get("tabular_dim") != tabular_dim:
        raise RuntimeError("feature_names/tabular_dim lost through JSON round-trip")
    if rt.get("coordinate_system") != COORDINATE_SYSTEM:
        raise RuntimeError(f"coordinate_system must be {COORDINATE_SYSTEM!r}; got {rt.get('coordinate_system')!r}")
    for key, meta in rt["weights"].items():
        arr = np.frombuffer(base64.b64decode(meta["data"]), dtype=np.float64).copy().reshape(meta["shape"])
        if not np.allclose(arr, numpy_weights.get(key, arr)):
            raise ValueError(f"Roundtrip mismatch for {key}")
    logger.info("Envelope validated (coordinate_system=%s, %d features)", COORDINATE_SYSTEM, len(feature_names))

    # 9. MLflow registration (ADR-012 — require_mlflow_env enforced at entry).
    mlflow_fqn = mlflow_model_uri(CATALOG, SCHEMA, MODEL_NAME)
    tracking_uri = os.environ["MLFLOW_TRACKING_URI"]
    import mlflow

    mlflow.set_tracking_uri(tracking_uri)
    mlflow.set_experiment(f"/soccer_analytics/{MODEL_NAME}")
    with mlflow.start_run(run_name="xg_v3_spadl_set_encoder_hf_jobs"):
        mlflow.log_params(
            {
                "architecture": "deep_sets_set_encoder",
                "coordinate_system": COORDINATE_SYSTEM,
                "batch_size": BATCH_SIZE,
                "max_epochs": MAX_EPOCHS,
                "learning_rate": LEARNING_RATE,
                "tabular_dim": tabular_dim,
                "n_shots": len(shots),
                "n_parameters": sum(p.numel() for p in model.parameters()),
                "n_splits": N_SPLITS,
                "device": str(device),
                "xg_shot_data_commit": shots_commit["public"],
                "xg_shot_data_commit_restricted": shots_commit["restricted"],
                "shot_freeze_frames_commit": ff_commit["public"],
                "shot_freeze_frames_commit_restricted": ff_commit["restricted"],
                "training_shot_family": ",".join(_TRAINING_SHOT_TYPES),
                "penalty_xg_constant": penalty_xg,
                "calibrator_pooled_kind": calibrators["pooled"]["kind"],
            }
        )
        for provider, modes in oos_report.items():
            for mode, m in modes.items():
                for name, val in m.items():
                    if not np.isnan(val):
                        mlflow.log_metric(f"oos_{provider}_{mode}_{name}", val)
        for name, val in mc_metrics.items():
            mlflow.log_metric(name, val)
        for i in range(len(history["train_loss"])):
            mlflow.log_metric("train_loss", history["train_loss"][i], step=i)
            mlflow.log_metric("val_brier", history["val_brier"][i], step=i)

        import tempfile

        with tempfile.NamedTemporaryFile(mode="wb", suffix=".json", delete=False, dir="/tmp") as tmp:
            tmp.write(weight_bytes)
            tmp_path = tmp.name
        final_path = os.path.join(os.path.dirname(tmp_path), "model_weights.json")
        os.replace(tmp_path, final_path)
        mlflow.log_artifact(final_path)

        class _W(mlflow.pyfunc.PythonModel):  # type: ignore[misc]
            def predict(self, context: Any, mi: pd.DataFrame) -> np.ndarray:  # type: ignore[override]
                return np.zeros(len(mi))

        mlflow.pyfunc.log_model(
            python_model=_W(),
            artifact_path="xg_v3_model",
            registered_model_name=mlflow_fqn,
            input_example=pd.DataFrame({"x": [0.0]}),
        )
        run_id = mlflow.active_run().info.run_id
    client = mlflow.tracking.MlflowClient()
    set_and_verify_mlflow_champion(client, mlflow_fqn=mlflow_fqn, run_id=run_id)

    # 10. Publish to HF Hub (weights + metrics + card).
    metrics_payload: dict[str, Any] = {
        "model": MODEL_NAME,
        "coordinate_system": COORDINATE_SYSTEM,
        "oos_by_provider_mode": oos_report,
        "mc_dropout": mc_metrics,
        "calibrators": calibrators,
        "gate": gate,
        "penalty_xg": None if np.isnan(penalty_xg) else penalty_xg,
        "config": {"tabular_dim": tabular_dim, "feature_names": feature_names, "n_shots": len(shots)},
        "dataset_commits": {"xg_shot_data": shots_commit, "shot_freeze_frames": ff_commit},
    }
    metrics_payload = recorder.complete(metrics_payload, row_count=len(shots))
    api.create_repo(V3_MODEL_REPO, exist_ok=True, repo_type="model", token=hf_token)
    api.upload_file(
        path_or_fileobj=weight_bytes, path_in_repo="model_weights.json", repo_id=V3_MODEL_REPO, token=hf_token
    )
    api.upload_file(
        path_or_fileobj=json.dumps(metrics_payload, indent=2, default=str).encode("utf-8"),
        path_in_repo="metrics.json",
        repo_id=V3_MODEL_REPO,
        token=hf_token,
    )
    readme_result = upload_hf_readme(
        repo_id=V3_MODEL_REPO,
        readme_path=get_hf_card_path("xg-v3-model-card.md", kind="model"),
        hf_token=hf_token,
        repo_type="model",
    )
    logger.info("Uploaded model card: %s", readme_result["commit_url"])

    # 11. UC Volume (ADR-012 second delivery leg) + sidecar hash.
    from databricks.sdk import WorkspaceClient

    volume_result = upload_weights_to_uc_volume(
        WorkspaceClient(),
        catalog=CATALOG,
        schema=SCHEMA,
        model_name=MODEL_NAME,
        filename="model_weights.json",
        weights_bytes=weight_bytes,
    )
    logger.info("UC Volume publish complete: %s", volume_result["path"])
    logger.info("Published: https://huggingface.co/%s (%.1fs)", V3_MODEL_REPO, time.time() - pipeline_start)


if __name__ == "__main__":
    main()
