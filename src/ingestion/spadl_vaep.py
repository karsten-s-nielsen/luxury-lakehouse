"""VAEP action valuation (inference) pipeline.

Orchestrates the end-to-end SPADL conversion and VAEP scoring pipeline:
converts bronze events to SPADL (via :mod:`ingestion.spadl_conversion`),
loads pre-trained VAEP models from the MLflow registry, and scores every
action with offensive/defensive value.

Training code lives in :mod:`ingestion.vaep_training`.
SPADL conversion code lives in :mod:`ingestion.spadl_conversion`.

Bronze tables produced:
  - spadl_actions         -- SPADL-formatted actions (intermediate)
  - vaep_action_values    -- SPADL actions with VAEP scores (final output)

Design: "Fetch Once, Fork Twice" -- ingestion tasks populate bronze,
this pipeline reads from bronze.  No external API calls.  Supports
incremental runs by skipping games already converted / scored.
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING, Any

import pandas as pd

from ingestion.guards import FilterResult, timed_check
from ingestion.spadl_conversion import (
    _SPADL_TABLE,
    _convert_gradientsports_from_bronze,
    _convert_idsse_from_bronze,
    _convert_metrica_from_bronze,
    _convert_skillcorner_from_bronze,
    _convert_statsbomb_from_bronze,
    _convert_wyscout_from_bronze,
    _read_existing_match_ids,
)
from ingestion.utils import (
    _load_mlflow_artifact_hash,
    configure_logging,
    get_spark_session,
    parse_ingestion_args,
    verify_artifact_hash,
    write_delta_table,
)
from shared.constants import DEFAULT_GOLD_SCHEMA, mlflow_model_uri
from workflows import workflow
from workflows.exceptions import WorkflowSkippedError

if TYPE_CHECKING:
    from pyspark.sql import SparkSession
    from xgboost import XGBClassifier


_SPADL_SCHEMA = (
    "game_id BIGINT, original_event_id STRING, period_id BIGINT, time_seconds DOUBLE, "
    "team_id BIGINT, player_id BIGINT, start_x DOUBLE, start_y DOUBLE, end_x DOUBLE, end_y DOUBLE, "
    "type_id BIGINT, result_id BIGINT, bodypart_id BIGINT, action_id BIGINT, "
    "competition_id BIGINT, season_id BIGINT, data_source STRING, _ingested_at TIMESTAMP, match_id BIGINT, "
    # Provider-namespaced StatsBomb-native fields surfaced via silly-kicks 1.5.0+
    # ``preserve_native`` kwarg on convert_to_actions. NULL for non-StatsBomb sources.
    "statsbomb_possession_id BIGINT, statsbomb_possession_team_id BIGINT, "
    "statsbomb_play_pattern STRING, statsbomb_under_pressure BOOLEAN, "
    # LL2: 6 post-conversion enrichment columns from apply_spadl_enrichments.
    # add_possessions → possession_id_heuristic
    # add_gk_role → gk_role
    # add_pre_shot_gk_context → 4 columns. See ADR-016.
    "possession_id_heuristic BIGINT, gk_role STRING, "
    "gk_was_distributing BOOLEAN, gk_was_engaged BOOLEAN, "
    "gk_actions_in_possession BIGINT, defending_gk_player_id BIGINT, "
    # LL2 Path B: native (string) provider identifiers for dim_teams /
    # dim_competitions joins on (provider, native_id). Populated for ALL sources;
    # mirrors the ADR-011 dim_competitions pattern (provider + native_competition_id +
    # legacy competition_id INT NULL for non-numeric IDs). For StatsBomb/Wyscout
    # the values are stringified ints; for IDSSE these are 'DFL-CLU-XXXXXX' /
    # 'DFL-COM-XXXXXX' / 'DFL-SEA-XXXXXX'; for Metrica these are
    # 'metrica-sample' / 'metrica-open-2017' / synthetic 'Sample_Game_N-Home/Away'.
    "team_id_native STRING, home_team_id_native STRING, "
    "competition_native_id STRING, season_native_id STRING, "
    # match_id_native: required for fct_action_values to JOIN dim_matches on
    # (provider, native_match_id). For SB/WS it equals str(match_id); for
    # IDSSE/Metrica it's the original string ('J03WMX' / 'Sample_Game_1') while
    # the BIGINT match_id is its deterministic hash.
    "match_id_native STRING, "
    # PR-LL3 S2: player_id_native - stringified source player identifier.
    # SB/WS: stringified int; IDSSE: DFL-OBJ-XXXXXX; Metrica: PlayerN.
    "player_id_native STRING, "
    # PR-Cycle-A.4 (2026-04-30, ADR-018): silly-kicks 2.5.0 sportec tackle
    # qualifier passthrough as ``<col>_native`` (STRING) + ``<col>_key``
    # (BIGINT surrogate via ``hash_native_id_to_bigint``) per LL2 Path B
    # convention. Pre-2.5.0 silly-kicks silently hashed sportec player/team
    # IDs to int (lossy); 2.5.0 emits native DFL OBJ/CLU strings, and we
    # surface BOTH the string + a deterministic BIGINT key for Kimball
    # joins. NULL on non-sportec rows.
    "tackle_winner_player_id_native STRING, tackle_winner_player_key BIGINT, "
    "tackle_winner_team_id_native STRING, tackle_winner_team_key BIGINT, "
    "tackle_loser_player_id_native STRING, tackle_loser_player_key BIGINT, "
    "tackle_loser_team_id_native STRING, tackle_loser_team_key BIGINT"
)
_VAEP_TABLE = "vaep_action_values"
_VAEP_SCHEMA = (
    "game_id BIGINT, match_id BIGINT, original_event_id STRING, period_id BIGINT, "
    "time_seconds DOUBLE, team_id BIGINT, player_id BIGINT, start_x DOUBLE, start_y DOUBLE, "
    "end_x DOUBLE, end_y DOUBLE, type_id BIGINT, action_type STRING, result_id BIGINT, "
    "action_result STRING, bodypart_id BIGINT, bodypart STRING, offensive_value DOUBLE, "
    "defensive_value DOUBLE, vaep_value DOUBLE, competition_id BIGINT, season_id BIGINT, "
    "data_source STRING, _ingested_at TIMESTAMP, "
    # LL2: action_id surfaced through to vaep_action_values (was never carried
    # through pre-LL2 — bronze.spadl_actions.action_id existed but was 100% NULL).
    "action_id BIGINT, "
    # Provider-namespaced StatsBomb-native fields (carried through from spadl_actions).
    # NULL for non-StatsBomb sources.
    "statsbomb_possession_id BIGINT, statsbomb_possession_team_id BIGINT, "
    "statsbomb_play_pattern STRING, statsbomb_under_pressure BOOLEAN, "
    # LL2: 6 post-conversion enrichment columns. See ADR-016.
    "possession_id_heuristic BIGINT, gk_role STRING, "
    "gk_was_distributing BOOLEAN, gk_was_engaged BOOLEAN, "
    "gk_actions_in_possession BIGINT, defending_gk_player_id BIGINT, "
    # LL2 Path B: native string identifiers (carried through from spadl_actions).
    # See _SPADL_SCHEMA for naming + value conventions.
    "team_id_native STRING, home_team_id_native STRING, "
    "competition_native_id STRING, season_native_id STRING, "
    "match_id_native STRING, "
    "player_id_native STRING, "
    # PR-Cycle-A.4 (2026-04-30, ADR-018): silly-kicks 2.5.0 sportec tackle
    # qualifier passthrough — ``<col>_native`` STRING + ``<col>_key`` BIGINT.
    # Carried through from spadl_actions; see _SPADL_SCHEMA for full rationale.
    "tackle_winner_player_id_native STRING, tackle_winner_player_key BIGINT, "
    "tackle_winner_team_id_native STRING, tackle_winner_team_key BIGINT, "
    "tackle_loser_player_id_native STRING, tackle_loser_player_key BIGINT, "
    "tackle_loser_team_id_native STRING, tackle_loser_team_key BIGINT"
)


class _VaepGuard:
    """SkipGuard adapter for SPADL/VAEP pipeline.

    Two-stage guard: checks both SPADL conversion and VAEP scoring,
    returning combined metadata with match ID lists for each stage.
    """

    workflow_id = "wf-vaep"

    def check(self, spark: SparkSession, catalog: str, schema: str) -> FilterResult:
        """Check if SPADL conversion or VAEP scoring has new work."""
        from ingestion.guards import ensure_table, find_new_ids

        spadl_table = f"{catalog}.{schema}.{_SPADL_TABLE}"
        vaep_table = f"{catalog}.{schema}.{_VAEP_TABLE}"

        ensure_table(spark, spadl_table, _SPADL_SCHEMA)
        ensure_table(spark, vaep_table, _VAEP_SCHEMA)

        # Stage 1: Source events not yet in SPADL — 4-source union (LL2 Path B).
        sb_new = find_new_ids(
            spark,
            f"{catalog}.{schema}.statsbomb_events",
            spadl_table,
        )
        ws_new = find_new_ids(
            spark,
            f"{catalog}.{schema}.wyscout_events",
            spadl_table,
            id_column="matchId",
            results_id_column="match_id",
        )

        # IDSSE + Metrica use STRING bronze match_ids that we hash to BIGINT
        # for spadl_actions. find_new_ids string-cast comparison would compare
        # 'idsse_J03WMX' vs '12345' (the hashed value as string) and find
        # everything "new" — wrong. Compute the diff ourselves: hash bronze
        # strings, scope spadl_actions to the source via data_source filter.
        idsse_new = self._diff_hashed_source_against_spadl(
            spark,
            bronze_table=f"{catalog}.{schema}.idsse_events",
            spadl_table=spadl_table,
            data_source="idsse",
        )
        metrica_new = self._diff_hashed_source_against_spadl(
            spark,
            bronze_table=f"{catalog}.{schema}.metrica_events",
            spadl_table=spadl_table,
            data_source="metrica",
        )
        sc_new = self._diff_hashed_source_against_spadl(
            spark,
            bronze_table=f"{catalog}.{schema}.skillcorner_events",
            spadl_table=spadl_table,
            data_source="skillcorner",
        )
        gs_new = self._diff_hashed_source_against_spadl(
            spark,
            bronze_table=f"{catalog}.{schema}.gradientsports_events",
            spadl_table=spadl_table,
            data_source="gradientsports",
        )

        # Stage 2: SPADL actions not yet scored with VAEP
        unscored = find_new_ids(
            spark,
            spadl_table,
            vaep_table,
        )

        total_new = (
            len(sb_new) + len(ws_new) + len(idsse_new) + len(metrica_new) + len(sc_new) + len(gs_new) + len(unscored)
        )

        if total_new == 0:
            return FilterResult(workflow_id=self.workflow_id, count=0)

        # Per-provider keys satisfy the test_guard_conformance contract
        # (count == sum-of-metadata-list-lengths) directly. run_pipeline
        # unions them at consumption time so Stage 2 scores newly-converted
        # match_ids in the same run. Production invariant: a match_id is
        # EITHER in events∧¬spadl OR in spadl∧¬vaep, not both.
        return FilterResult(
            workflow_id=self.workflow_id,
            count=total_new,
            metadata={
                "sb_new": sb_new,
                "ws_new": ws_new,
                "idsse_new": idsse_new,
                "metrica_new": metrica_new,
                "sc_new": sc_new,
                "gs_new": gs_new,
                "unscored_vaep_match_ids": sorted(unscored),
            },
        )

    @staticmethod
    def _diff_hashed_source_against_spadl(
        spark: SparkSession,
        bronze_table: str,
        spadl_table: str,
        data_source: str,
    ) -> list[str]:
        """Count new matches for a hashed-ID source (IDSSE / Metrica / SkillCorner / GradientSports).

        Bronze tables use STRING match_ids. spadl_actions uses BIGINT
        (deterministically hashed via ``hash_native_id_to_bigint``).
        Hashes all bronze strings, scopes spadl_actions to the source via
        ``data_source`` filter, returns the BIGINT diff as strings.

        Raises if the bronze table does not exist — a missing source table
        is a deployment error that must surface immediately, not be silently
        swallowed.
        """
        from ingestion.spadl_adapter import hash_native_id_to_bigint

        bronze_rows = spark.table(bronze_table).select("match_id").distinct().collect()
        bronze_strings = [str(row["match_id"]) for row in bronze_rows if row["match_id"] is not None]

        if not bronze_strings:
            return []

        bronze_hashed: set[int] = {hash_native_id_to_bigint(s) for s in bronze_strings}

        # spadl_actions BIGINT match_ids for this source (spadl_table was
        # ensure_table'd by the caller, so it always exists).
        ds_quoted = data_source.replace("'", "''")
        spadl_rows = (
            spark.table(spadl_table).filter(f"data_source = '{ds_quoted}'").select("match_id").distinct().collect()
        )
        in_spadl: set[int] = set()
        for row in spadl_rows:
            v = row["match_id"]
            if v is None:
                continue
            try:
                in_spadl.add(int(v))
            except (TypeError, ValueError):
                continue

        return [str(h) for h in sorted(bronze_hashed - in_spadl)]


skip_guard = _VaepGuard()


_VALID_CHUNK_PROVIDERS = frozenset(
    {"statsbomb", "wyscout", "idsse", "metrica", "skillcorner", "gradientsports", "score"}
)


def _parse_vaep_match_ids_arg(raw: str | None) -> tuple[str, list[int]] | None:
    """Parse a for_each_task chunk string into (provider, match_ids).

    Chunk grammar:
        convert_chunk := provider ":" match_id_list
        score_chunk   := "score:" match_id_list
        match_id_list := BIGINT ("," BIGINT)*

    Returns None for None/empty input. Raises SystemExit on invalid format.
    """
    if not raw:
        return None

    if ":" not in raw:
        raise SystemExit(f"--match-ids must be 'provider:id1,id2,...' — got '{raw}'")

    provider, ids_str = raw.split(":", 1)

    if provider not in _VALID_CHUNK_PROVIDERS:
        raise SystemExit(f"Unknown provider '{provider}' — must be one of {sorted(_VALID_CHUNK_PROVIDERS)}")

    try:
        match_ids = [int(mid) for mid in ids_str.split(",")]
    except ValueError as exc:
        raise SystemExit(f"--match-ids contains non-integer IDs: {exc}") from exc

    return (provider, match_ids)


_CHUNK_SIZES: dict[str, int] = {
    "statsbomb": 200,
    "wyscout": 200,
    "idsse": 50,
    "metrica": 50,
    "skillcorner": 50,
    "gradientsports": 50,
    "score": 200,
}

_PROVIDER_METADATA_KEYS: dict[str, str] = {
    "sb_new": "statsbomb",
    "ws_new": "wyscout",
    "idsse_new": "idsse",
    "metrica_new": "metrica",
    "sc_new": "skillcorner",
    "gs_new": "gradientsports",
}


def _build_chunks(metadata: dict[str, Any]) -> list[str]:
    """Build for_each_task chunk strings from guard metadata.

    Each chunk is ``"provider:id1,id2,...,idN"`` for convert chunks
    or ``"score:id1,id2,...,idN"`` for score-only chunks.
    """
    chunks: list[str] = []

    # Convert chunks (per-provider) — direct access, guard always emits all keys
    for meta_key, provider in _PROVIDER_METADATA_KEYS.items():
        ids = metadata[meta_key]
        if not ids:
            continue
        chunk_size = _CHUNK_SIZES[provider]
        for i in range(0, len(ids), chunk_size):
            batch = ids[i : i + chunk_size]
            chunks.append(f"{provider}:{','.join(str(mid) for mid in batch)}")

    # Score-only chunks (unscored matches already in spadl_actions)
    unscored = metadata["unscored_vaep_match_ids"]
    if unscored:
        chunk_size = _CHUNK_SIZES["score"]
        for i in range(0, len(unscored), chunk_size):
            batch = unscored[i : i + chunk_size]
            chunks.append(f"score:{','.join(str(mid) for mid in batch)}")

    return chunks


def _get_feature_fns() -> list[Any]:
    """Return the standard VAEP feature function list (lazy import)."""
    import silly_kicks.vaep.features as fs

    return [
        fs.actiontype_onehot,
        fs.result_onehot,
        fs.bodypart_onehot,
        fs.time,
        fs.startlocation,
        fs.endlocation,
        fs.startpolar,
        fs.endpolar,
        fs.movement,
        fs.team,
        fs.time_delta,
    ]


_NB_PREV_ACTIONS = 3


# ---------------------------------------------------------------------------
# Phase C -- Load pre-trained VAEP models
# ---------------------------------------------------------------------------
# Training code (extract_features_for_games, train_vaep_models) has been
# extracted to ingestion.vaep_training. Production training runs on HF Jobs
# via scripts/train_vaep_model_hf.py.


def _try_load_champion_vaep(
    logger: logging.Logger,
    catalog: str,
    schema: str,
) -> tuple[XGBClassifier, XGBClassifier] | None:
    """Try to load VAEP models from MLflow @Champion alias.

    Returns (model_scores, model_concedes) if found, None otherwise.
    Falls back gracefully when mlflow is not installed or models are not registered.
    """
    try:
        import importlib

        mlflow_pyfunc = importlib.import_module("mlflow.pyfunc")
        mlflow_tracking = importlib.import_module("mlflow.tracking")
    except (ImportError, ModuleNotFoundError):
        logger.info("mlflow not available -- will train VAEP models from scratch")
        return None

    model_name = mlflow_model_uri(catalog, schema, "vaep_model")
    try:
        model_uri = f"models:/{model_name}@Champion"
        logger.info("Loading VAEP @Champion from %s", model_uri)
        champion = mlflow_pyfunc.load_model(model_uri)
        # The pyfunc wrapper stores both models as a dict of XGBClassifier
        unwrapped = champion.unwrap_python_model()  # type: ignore[union-attr]
        model_scores: XGBClassifier = unwrapped.scores_model  # type: ignore[union-attr]
        model_concedes: XGBClassifier = unwrapped.concedes_model  # type: ignore[union-attr]

        # SEC2: verify artifact integrity against recorded MLflow tag (if any)
        scores_raw = bytes(model_scores.get_booster().save_raw("json"))
        concedes_raw = bytes(model_concedes.get_booster().save_raw("json"))
        client = mlflow_tracking.MlflowClient()
        expected_hash = _load_mlflow_artifact_hash(client, model_name, alias="Champion")
        verify_artifact_hash(
            data=scores_raw,
            expected_sha256=expected_hash,
            artifact_label=f"{model_name}_scores",
            logger=logger,
        )
        verify_artifact_hash(
            data=concedes_raw,
            expected_sha256=expected_hash,
            artifact_label=f"{model_name}_concedes",
            logger=logger,
        )

        logger.info("Loaded VAEP @Champion models from MLflow")
        return model_scores, model_concedes
    except Exception:  # noqa: BLE001 — MLflow registry raises many unrelated exception types on missing Champion
        logger.info("VAEP @Champion not found in MLflow registry -- will train from scratch", exc_info=True)
        return None


def _load_models(
    catalog: str,
    schema: str,
    logger: logging.Logger,
) -> tuple[XGBClassifier, XGBClassifier] | None:
    """Load VAEP models from MLflow @Champion registry.

    Training is handled externally by HF Jobs (``scripts/train_vaep_model_hf.py``).
    Returns ``None`` when no Champion model is registered.
    """
    champion_models = _try_load_champion_vaep(logger, catalog, DEFAULT_GOLD_SCHEMA)
    if champion_models is not None:
        return champion_models

    logger.warning(
        "No Champion VAEP model found in MLflow registry. "
        "Run scripts/train_vaep_model_hf.py on HF Jobs to train and register a model."
    )
    return None


# ---------------------------------------------------------------------------
# Phase D -- Score all actions & write
# ---------------------------------------------------------------------------


def _make_scoring_udf(scores_raw: bytes, concedes_raw: bytes) -> object:
    """Build the ``applyInPandas`` UDF closure for VAEP scoring.

    Models are deserialized from raw bytes (captured in the closure) and
    cached in a function-level dict so each executor deserializes only once.
    This avoids UC Volume FUSE limitations on serverless where XGBoost's
    C-level file I/O cannot read/write Volume paths.

    Args:
        scores_raw: Raw bytes from ``model.get_booster().save_raw("json")``.
        concedes_raw: Raw bytes from ``model.get_booster().save_raw("json")``.

    Returns:
        A callable ``(pd.DataFrame) -> pd.DataFrame`` suitable for
        ``applyInPandas``.
    """
    _nb_prev = _NB_PREV_ACTIONS

    def _udf(pdf: pd.DataFrame) -> pd.DataFrame:
        """Score one competition's SPADL actions with VAEP models."""
        import pandas as _pd

        _output_cols = _pd.Index(
            [
                "game_id",
                "match_id",
                "original_event_id",
                "period_id",
                "time_seconds",
                "team_id",
                "player_id",
                "start_x",
                "start_y",
                "end_x",
                "end_y",
                "type_id",
                "action_type",
                "result_id",
                "action_result",
                "bodypart_id",
                "bodypart",
                "offensive_value",
                "defensive_value",
                "vaep_value",
                "competition_id",
                "season_id",
                "data_source",
                # LL2: action_id surfaced through (was 100% NULL pre-LL2).
                "action_id",
                # Provider-namespaced StatsBomb-native fields carried through from
                # spadl_actions. NULL on Wyscout / IDSSE / Metrica code paths.
                "statsbomb_possession_id",
                "statsbomb_possession_team_id",
                "statsbomb_play_pattern",
                "statsbomb_under_pressure",
                # LL2: 6 post-conversion enrichment columns from apply_spadl_enrichments.
                "possession_id_heuristic",
                "gk_role",
                "gk_was_distributing",
                "gk_was_engaged",
                "gk_actions_in_possession",
                "defending_gk_player_id",
                # LL2 Path B: native string identifiers carried through.
                "team_id_native",
                "home_team_id_native",
                "competition_native_id",
                "season_native_id",
                "match_id_native",
                "player_id_native",
                # PR-LL2 Path B close-out (2026-04-29, ADR-018): silly-kicks 2.0.0
                # sportec tackle qualifier columns carried through from spadl_actions.
                "tackle_winner_player_id_native",
                "tackle_winner_player_key",
                "tackle_winner_team_id_native",
                "tackle_winner_team_key",
                "tackle_loser_player_id_native",
                "tackle_loser_player_key",
                "tackle_loser_team_id_native",
                "tackle_loser_team_key",
            ]
        )

        if pdf.empty:
            return _pd.DataFrame(columns=_output_cols)

        import silly_kicks.spadl as _spadl
        import silly_kicks.vaep.features as _fs
        import silly_kicks.vaep.formula as _vaepformula

        _feature_fns: list = [
            _fs.actiontype_onehot,
            _fs.result_onehot,
            _fs.bodypart_onehot,
            _fs.time,
            _fs.startlocation,
            _fs.endlocation,
            _fs.startpolar,
            _fs.endpolar,
            _fs.movement,
            _fs.team,
            _fs.time_delta,
        ]

        # Load models with executor-level caching (deserialize from bytes)
        if not hasattr(_udf, "_model_cache"):
            _udf._model_cache = {}  # type: ignore[attr-defined]

        cache: dict = _udf._model_cache  # type: ignore[attr-defined]
        if "scores" not in cache:
            from xgboost import XGBClassifier

            m_scores = XGBClassifier()
            m_scores.load_model(bytearray(scores_raw))
            cache["scores"] = m_scores

            m_concedes = XGBClassifier()
            m_concedes.load_model(bytearray(concedes_raw))
            cache["concedes"] = m_concedes

        model_scores = cache["scores"]
        model_concedes = cache["concedes"]

        named = _spadl.add_names(pdf)  # type: ignore[arg-type]
        game_ids = named["game_id"].unique()

        # Pre-build game index (CLAUDE.md: no boolean mask filter inside loops)
        _game_groups = dict(iter(named.groupby("game_id")))

        all_scored: list[_pd.DataFrame] = []
        for game_id in game_ids:
            game_actions = _game_groups.get(game_id, _pd.DataFrame()).reset_index(drop=True)
            if len(game_actions) < 2:
                continue
            # Defense-in-depth: loud failure if NULL team_id reaches scoring
            _null_team_count = game_actions["team_id"].isna().sum()
            if _null_team_count:
                raise RuntimeError(
                    f"VAEP scoring received {_null_team_count} NULL team_id rows "
                    f"for game_id={game_id}. "
                    f"SPADL conversion must resolve team_id before scoring."
                )
            try:
                gamestates = _fs.gamestates(game_actions, nb_prev_actions=_nb_prev)  # type: ignore[arg-type]
                x_game = _pd.concat([fn(gamestates) for fn in _feature_fns], axis=1)

                p_scores = _pd.Series(model_scores.predict_proba(x_game)[:, 1])
                p_concedes = _pd.Series(model_concedes.predict_proba(x_game)[:, 1])
                values = _vaepformula.value(game_actions, p_scores, p_concedes)  # type: ignore[arg-type]

                game_out = _pd.DataFrame(
                    game_actions[
                        [
                            c
                            for c in [
                                "game_id",
                                "match_id",
                                "original_event_id",
                                "period_id",
                                "time_seconds",
                                "team_id",
                                "player_id",
                                "start_x",
                                "start_y",
                                "end_x",
                                "end_y",
                                "type_id",
                                "type_name",
                                "result_id",
                                "result_name",
                                "bodypart_id",
                                "bodypart_name",
                                # LL2: action_id carried through from spadl_actions.
                                "action_id",
                                # Carry through provider-namespaced StatsBomb-native
                                # fields (NULL for non-StatsBomb sources).
                                "statsbomb_possession_id",
                                "statsbomb_possession_team_id",
                                "statsbomb_play_pattern",
                                "statsbomb_under_pressure",
                                # LL2: 6 enrichment columns (populated for ALL sources).
                                "possession_id_heuristic",
                                "gk_role",
                                "gk_was_distributing",
                                "gk_was_engaged",
                                "gk_actions_in_possession",
                                "defending_gk_player_id",
                                # LL2 Path B: native string identifiers.
                                "team_id_native",
                                "home_team_id_native",
                                "competition_native_id",
                                "season_native_id",
                                "match_id_native",
                                "player_id_native",
                                # PR-LL2 Path B close-out (2026-04-30, ADR-018):
                                # silly-kicks 2.0.0 sportec tackle qualifier
                                # passthrough. Must be in this projection list
                                # AND in `_output_cols` AND in `vaep_schema`
                                # StructType — drift between these layers is
                                # the LL1 latent-bug class. Adding a column
                                # requires updating ALL FOUR places (DDL +
                                # StructType + per-game projection + output
                                # column index).
                                "tackle_winner_player_id_native",
                                "tackle_winner_player_key",
                                "tackle_winner_team_id_native",
                                "tackle_winner_team_key",
                                "tackle_loser_player_id_native",
                                "tackle_loser_player_key",
                                "tackle_loser_team_id_native",
                                "tackle_loser_team_key",
                            ]
                            if c in game_actions.columns
                        ]
                    ].copy()
                )
                game_out = game_out.rename(
                    columns={
                        "type_name": "action_type",
                        "result_name": "action_result",
                        "bodypart_name": "bodypart",
                    }
                )
                game_out["offensive_value"] = values["offensive_value"].values
                game_out["defensive_value"] = values["defensive_value"].values
                game_out["vaep_value"] = values["vaep_value"].values

                # Carry through partition keys from the input
                game_out["competition_id"] = pdf["competition_id"].iloc[0]
                game_out["season_id"] = pdf["season_id"].iloc[0]
                game_out["data_source"] = pdf["data_source"].iloc[0]

                all_scored.append(game_out)
            except Exception as exc:
                # Surface per-game failures with game_id context so Spark
                # propagates them to the driver. The previous silent-swallow
                # pattern hid scoring-UDF failures entirely — bronze.vaep_action_values
                # would get zero rows for any failing game with no trace, and
                # daily job runs reported SUCCEEDED regardless. This is
                # load-bearing for detecting regressions in silly_kicks feature
                # functions, VAEP model drift, and downstream schema changes.
                msg = f"VAEP scoring failed for game_id={game_id}"
                raise RuntimeError(msg) from exc

        if not all_scored:
            return _pd.DataFrame(columns=_output_cols)

        result: _pd.DataFrame = _pd.concat(all_scored, ignore_index=True)[_output_cols]  # type: ignore[assignment]
        return result

    return _udf


# ---------------------------------------------------------------------------
# Pipeline orchestration
# ---------------------------------------------------------------------------


@workflow("wf-vaep", phase="inference")
def run_pipeline(
    spark: SparkSession,
    catalog: str,
    schema: str,
    logger: logging.Logger,
    *,
    filter_result: FilterResult,
    ctx=None,
) -> int:
    """Execute the full SPADL/VAEP pipeline.

    Memory strategy: never hold all data in memory.  Use Delta as
    intermediate storage between phases:

    1. Read bronze events, convert to SPADL per-competition -> append Delta (incremental)
    2. Read a small training subset from Delta -> extract features -> train (or load cached)
    3. Read per-competition from Delta -> score unscored games -> write results (incremental)
    """
    if filter_result.count == 0:
        raise WorkflowSkippedError("No new work")

    # Union per-provider new match IDs with unscored_vaep_match_ids at consumption
    # time so Stage 2 sees the match_ids Stage 1 is about to add to spadl_actions
    # in the same run. Production invariant: new_spadl ∩ unscored = ∅
    # (a match_id is EITHER in events∧¬spadl OR in spadl∧¬vaep, not both),
    # so the union is lossless.
    unscored_ids = sorted(
        set(filter_result.metadata["unscored_vaep_match_ids"])
        | set(filter_result.metadata["sb_new"])
        | set(filter_result.metadata["ws_new"])
        | set(filter_result.metadata["idsse_new"])
        | set(filter_result.metadata["metrica_new"])
        | set(filter_result.metadata["sc_new"])
        | set(filter_result.metadata["gs_new"]),
    )

    spadl_table = f"{catalog}.{schema}.{_SPADL_TABLE}"

    # Phase A+B: Convert events from bronze to SPADL (incremental)
    existing_spadl_matches = _read_existing_match_ids(spark, catalog, schema, _SPADL_TABLE, logger)
    if existing_spadl_matches:
        logger.info("Found %d games already in %s -- will skip", len(existing_spadl_matches), _SPADL_TABLE)

    sb_wrote = _convert_statsbomb_from_bronze(spark, catalog, schema, logger, existing_spadl_matches)
    ws_wrote = _convert_wyscout_from_bronze(spark, catalog, schema, logger, existing_spadl_matches)
    # LL2 Path B: 4-source coverage. IDSSE + Metrica use string match_ids
    # (hashed to BIGINT inside their UDFs for spadl_actions compat).
    idsse_wrote = _convert_idsse_from_bronze(spark, catalog, schema, logger, existing_spadl_matches)
    metrica_wrote = _convert_metrica_from_bronze(spark, catalog, schema, logger, existing_spadl_matches)
    sc_wrote = _convert_skillcorner_from_bronze(spark, catalog, schema, logger, existing_spadl_matches)
    gs_wrote = _convert_gradientsports_from_bronze(spark, catalog, schema, logger, existing_spadl_matches)

    any_wrote = sb_wrote or ws_wrote or idsse_wrote or metrica_wrote or sc_wrote or gs_wrote
    if not any_wrote and not existing_spadl_matches:
        msg = "No SPADL actions produced from any source (SB / WS / IDSSE / Metrica / SC / GS)"
        logger.error(msg)
        raise RuntimeError(msg)

    # Verify SPADL table has data (limit(1) avoids full DAG recomputation -- exact count not needed here)
    if spark.table(spadl_table).limit(1).count() == 0:
        msg = "SPADL table exists but is empty -- no actions to score"
        logger.error(msg)
        raise RuntimeError(msg)
    logger.info("SPADL table %s has data -- proceeding to scoring", spadl_table)

    # Phase C: Load pre-trained models from MLflow @Champion
    # Training is handled by HF Jobs (scripts/train_vaep_model_hf.py)
    spadl_sdf = spark.table(spadl_table)

    models = _load_models(catalog, schema, logger)

    if models is None:
        return 0

    model_scores, model_concedes = models

    # Phase D: Score unscored games via applyInPandas (distributed on executors)
    # Use guard-provided unscored IDs instead of inline re-computation
    unscored_match_ids = unscored_ids

    if not unscored_match_ids:
        logger.info("No unscored matches -- nothing to do")
        return 0

    # Serialize models to bytes for executor distribution via UDF closure.
    # XGBoost's C-level save_model/load_model cannot use UC Volume FUSE on
    # serverless, so we pass raw bytes through the closure instead.
    scores_raw = bytes(model_scores.get_booster().save_raw("json"))
    concedes_raw = bytes(model_concedes.get_booster().save_raw("json"))
    logger.info("Serialized VAEP models: scores=%d bytes, concedes=%d bytes", len(scores_raw), len(concedes_raw))

    from pyspark.sql import functions as spark_fn

    logger.info(
        "Scoring %d unscored matches via applyInPandas",
        len(unscored_match_ids),
    )

    unscored_sdf = spadl_sdf.filter(spark_fn.col("match_id").isin(unscored_match_ids))

    vaep_schema = _vaep_output_schema()

    scoring_udf = _make_scoring_udf(scores_raw, concedes_raw)
    # Group by match_id -- each match is ~1,600 SPADL actions (~5 MB), well
    # within the 800 MB serverless UDF budget.  Competition-level grouping
    # OOMs on large datasets (La Liga = 600K+ rows per group).  The model
    # cache (_model_cache) loads once per executor, not per group.
    scored_sdf = unscored_sdf.groupBy("match_id", "data_source").applyInPandas(
        scoring_udf,  # type: ignore[arg-type]
        schema=vaep_schema,
    )

    # Build replaceWhere predicate targeting only unscored match_ids so
    # existing VAEP scores are preserved (not destroyed by bare overwrite).
    ids_sql = ", ".join(str(mid) for mid in unscored_match_ids)
    write_delta_table(
        scored_sdf,
        catalog,
        schema,
        _VAEP_TABLE,
        replace_where=f"match_id IN ({ids_sql})",
        logger=logger,
    )

    logger.info("SPADL/VAEP pipeline complete -- scoring distributed across executors")
    return 0


# ---------------------------------------------------------------------------
# VAEP output schema (shared by run_pipeline and _run_chunk)
# ---------------------------------------------------------------------------


def _vaep_output_schema() -> Any:
    """Return the output StructType for VAEP scored actions.

    Shared by run_pipeline (monolithic) and _run_chunk (for_each_task iteration).
    """
    from pyspark.sql.types import BooleanType, DoubleType, LongType, StringType, StructField, StructType

    return StructType(
        [
            StructField("game_id", LongType()),
            StructField("match_id", LongType()),
            StructField("original_event_id", StringType()),
            StructField("period_id", LongType()),
            StructField("time_seconds", DoubleType()),
            StructField("team_id", LongType()),
            StructField("player_id", LongType()),
            StructField("start_x", DoubleType()),
            StructField("start_y", DoubleType()),
            StructField("end_x", DoubleType()),
            StructField("end_y", DoubleType()),
            StructField("type_id", LongType()),
            StructField("action_type", StringType()),
            StructField("result_id", LongType()),
            StructField("action_result", StringType()),
            StructField("bodypart_id", LongType()),
            StructField("bodypart", StringType()),
            StructField("offensive_value", DoubleType()),
            StructField("defensive_value", DoubleType()),
            StructField("vaep_value", DoubleType()),
            StructField("competition_id", LongType()),
            StructField("season_id", LongType()),
            StructField("data_source", StringType()),
            StructField("action_id", LongType()),
            StructField("statsbomb_possession_id", LongType()),
            StructField("statsbomb_possession_team_id", LongType()),
            StructField("statsbomb_play_pattern", StringType()),
            StructField("statsbomb_under_pressure", BooleanType()),
            StructField("possession_id_heuristic", LongType()),
            StructField("gk_role", StringType()),
            StructField("gk_was_distributing", BooleanType()),
            StructField("gk_was_engaged", BooleanType()),
            StructField("gk_actions_in_possession", LongType()),
            StructField("defending_gk_player_id", LongType()),
            StructField("team_id_native", StringType()),
            StructField("home_team_id_native", StringType()),
            StructField("competition_native_id", StringType()),
            StructField("season_native_id", StringType()),
            StructField("match_id_native", StringType()),
            StructField("player_id_native", StringType()),
            StructField("tackle_winner_player_id_native", StringType()),
            StructField("tackle_winner_player_key", LongType()),
            StructField("tackle_winner_team_id_native", StringType()),
            StructField("tackle_winner_team_key", LongType()),
            StructField("tackle_loser_player_id_native", StringType()),
            StructField("tackle_loser_player_key", LongType()),
            StructField("tackle_loser_team_id_native", StringType()),
            StructField("tackle_loser_team_key", LongType()),
        ]
    )


# ---------------------------------------------------------------------------
# Chunked execution (for_each_task iteration)
# ---------------------------------------------------------------------------

_VAEP_MODEL_CACHE_BASE = "/Volumes/{catalog}/{schema}/model_weights/vaep_cache"


def _cache_vaep_models_to_volume(
    catalog: str,
    schema: str,
    logger: logging.Logger,
) -> str | None:
    """Download VAEP Champion models from MLflow and cache to UC Volume.

    Writes scores.xgb + concedes.xgb to a run-scoped directory under the
    existing model_weights Volume (Terraform-managed, ingestion SP has grants).
    Returns the directory path, or None if no Champion model exists.

    Prerequisite: UC Volume /Volumes/soccer_analytics/dev_gold/model_weights/
    must exist. Already Terraform-managed (terraform/modules/catalog/main.tf:134-137)
    with ingestion SP grants — no manual creation needed.
    """
    import os
    import time

    models = _load_models(catalog, schema, logger)
    if models is None:
        return None

    model_scores, model_concedes = models

    scores_raw = bytes(model_scores.get_booster().save_raw("json"))
    concedes_raw = bytes(model_concedes.get_booster().save_raw("json"))

    # Run-scoped directory prevents interference between concurrent preflights
    run_id = os.environ.get("DATABRICKS_RUN_ID", str(int(time.time())))
    base_path = _VAEP_MODEL_CACHE_BASE.format(catalog=catalog, schema="dev_gold")
    model_dir = f"{base_path}/{run_id}"

    # Write both model files
    scores_path = f"{model_dir}/scores.xgb"
    concedes_path = f"{model_dir}/concedes.xgb"

    os.makedirs(model_dir, exist_ok=True)
    with open(scores_path, "wb") as f:
        f.write(scores_raw)
    with open(concedes_path, "wb") as f:
        f.write(concedes_raw)

    logger.info(
        "Cached VAEP models to %s (scores=%d bytes, concedes=%d bytes)",
        model_dir,
        len(scores_raw),
        len(concedes_raw),
    )

    # Cleanup: remove directories older than 7 days (non-critical)
    _cleanup_old_model_cache(base_path, max_age_days=7, logger=logger)

    return model_dir


def _cleanup_old_model_cache(base_path: str, max_age_days: int, logger: logging.Logger) -> None:
    """Remove model cache directories older than max_age_days."""
    import os
    import shutil
    import time

    if not os.path.isdir(base_path):
        return

    cutoff = time.time() - (max_age_days * 86400)
    for entry in os.listdir(base_path):
        entry_path = os.path.join(base_path, entry)
        if os.path.isdir(entry_path):
            try:
                mtime = os.path.getmtime(entry_path)
                if mtime < cutoff:
                    shutil.rmtree(entry_path)
                    logger.info("Cleaned old model cache: %s", entry_path)
            except OSError as exc:
                logger.debug("Model cache cleanup skipped for %s: %s", entry_path, exc)


def _load_model_path_from_task_value(spark: Any, logger: logging.Logger) -> str:
    """Read vaep_model_path task value from preflight."""
    try:
        from pyspark.dbutils import DBUtils  # type: ignore[import-not-found]

        dbutils = DBUtils(spark)
        path = dbutils.jobs.taskValues.get(
            taskKey="preflight_spadl_vaep",
            key="vaep_model_path",
        )
        logger.info("Read model path from task value: %s", path)
        return str(path)
    except (ImportError, AttributeError, RuntimeError) as exc:
        raise SystemExit(
            "Cannot read vaep_model_path task value — "
            "are you running outside for_each_task? Use monolithic mode (no --match-ids)."
        ) from exc


def _read_cached_models(model_dir: str, logger: logging.Logger) -> tuple[bytes, bytes]:
    """Read cached XGBoost model bytes from UC Volume."""
    scores_path = f"{model_dir}/scores.xgb"
    concedes_path = f"{model_dir}/concedes.xgb"

    with open(scores_path, "rb") as f:
        scores_raw = f.read()
    with open(concedes_path, "rb") as f:
        concedes_raw = f.read()

    logger.info(
        "Loaded cached models: scores=%d bytes, concedes=%d bytes",
        len(scores_raw),
        len(concedes_raw),
    )
    return scores_raw, concedes_raw


def _run_chunk(
    spark: Any,
    catalog: str,
    schema: str,
    logger: logging.Logger,
    provider: str,
    match_ids: list[int],
) -> None:
    """Execute one for_each_task iteration: convert (if needed) + score.

    Args:
        provider: One of the _VALID_CHUNK_PROVIDERS. "score" means skip conversion.
        match_ids: BIGINT match IDs to process.
    """
    from pyspark.sql import functions as spark_fn

    spadl_table = f"{catalog}.{schema}.{_SPADL_TABLE}"

    # Phase A: Convert (unless this is a score-only chunk)
    if provider != "score":
        existing_spadl_matches = _read_existing_match_ids(spark, catalog, schema, _SPADL_TABLE, logger)
        converters = {
            "statsbomb": _convert_statsbomb_from_bronze,
            "wyscout": _convert_wyscout_from_bronze,
            "idsse": _convert_idsse_from_bronze,
            "metrica": _convert_metrica_from_bronze,
            "skillcorner": _convert_skillcorner_from_bronze,
            "gradientsports": _convert_gradientsports_from_bronze,
        }
        convert_fn = converters[provider]
        convert_fn(spark, catalog, schema, logger, existing_spadl_matches, match_id_filter=set(match_ids))
        logger.info("Phase A complete: converted %s chunk (%d match_ids)", provider, len(match_ids))

        # Post-conversion verification: ensure chunk match_ids landed in spadl_actions
        converted_count = (
            spark.table(spadl_table)
            .filter(spark_fn.col("match_id").isin(match_ids))
            .select("match_id")
            .distinct()
            .count()
        )
        if converted_count == 0:
            raise RuntimeError(
                f"Post-conversion check failed: 0 of {len(match_ids)} match_ids "
                f"found in {spadl_table} after {provider} conversion"
            )
        if converted_count < len(match_ids):
            logger.warning(
                "Partial conversion: %d/%d match_ids found in %s",
                converted_count,
                len(match_ids),
                spadl_table,
            )
        logger.info("Post-conversion: %d/%d match_ids in spadl_actions", converted_count, len(match_ids))

    # Phase B: Score via VAEP
    model_path = _load_model_path_from_task_value(spark, logger)
    scores_raw, concedes_raw = _read_cached_models(model_path, logger)

    unscored_sdf = spark.table(spadl_table).filter(spark_fn.col("match_id").isin(match_ids))

    vaep_schema = _vaep_output_schema()
    scoring_udf = _make_scoring_udf(scores_raw, concedes_raw)

    scored_sdf = unscored_sdf.groupBy("match_id", "data_source").applyInPandas(
        scoring_udf,  # type: ignore[arg-type]
        schema=vaep_schema,
    )

    ids_sql = ", ".join(str(mid) for mid in match_ids)
    write_delta_table(
        scored_sdf,
        catalog,
        schema,
        _VAEP_TABLE,
        replace_where=f"match_id IN ({ids_sql})",
        logger=logger,
    )

    logger.info("Chunk complete: provider=%s, %d matches scored", provider, len(match_ids))


# ---------------------------------------------------------------------------
# Entry points
# ---------------------------------------------------------------------------


def main_preflight() -> None:
    """CLI entry point for SPADL/VAEP preflight (guard + chunk emission + model cache).

    Runs the skip guard, builds chunk strings from per-provider match ID lists,
    caches VAEP models to UC Volume, and emits both as Databricks task values
    for downstream for_each_task iterations.
    """
    args = parse_ingestion_args("Preflight: discover unprocessed SPADL/VAEP matches and emit chunks")
    logger = configure_logging("spadl_vaep_preflight")
    spark = get_spark_session()

    from ingestion.bootstrap import bootstrap_hooks

    bootstrap_hooks(spark, args.catalog, args.schema)

    filter_result = timed_check(skip_guard, spark, args.catalog, args.schema)

    chunks = _build_chunks(filter_result.metadata) if filter_result.count > 0 else []
    logger.info(
        "VAEP preflight: %d matches across %d chunks",
        filter_result.count,
        len(chunks),
    )

    # Cache models to UC Volume (only if there's work to do)
    model_path: str | None = None
    if chunks:
        model_path = _cache_vaep_models_to_volume(args.catalog, args.schema, logger)
        if model_path is None:
            raise SystemExit(
                "No Champion VAEP model found in MLflow — cannot score. Run scripts/train_vaep_model_hf.py first."
            )

    # Emit task values
    try:
        from pyspark.dbutils import DBUtils  # type: ignore[import-not-found]

        dbutils = DBUtils(spark)
        dbutils.jobs.taskValues.set(key="spadl_vaep_chunks", value=chunks)
        logger.info("Wrote task value 'spadl_vaep_chunks' (%d chunks)", len(chunks))
        if model_path:
            dbutils.jobs.taskValues.set(key="vaep_model_path", value=model_path)
            logger.info("Wrote task value 'vaep_model_path' = %s", model_path)
    except (ImportError, AttributeError, RuntimeError) as exc:
        logger.warning("Task values not available (likely standalone mode) -- %s", exc)


def main() -> None:
    """CLI entry point for SPADL conversion and VAEP action valuation.

    Two modes:
    - **Chunk mode** (``--match-ids "provider:id1,id2"``): processes one chunk
      from the for_each_task fan-out. Reads model from UC Volume task value.
    - **Monolithic mode** (no ``--match-ids``): runs the full pipeline for all
      providers. Preserved for local development and debugging.
    """
    args = parse_ingestion_args(
        "Compute SPADL actions and VAEP scores",
        extra_args=[("--match-ids", {"type": str, "default": None, "help": "provider:id1,id2 from for_each_task"})],
    )
    logger = configure_logging("spadl_vaep")
    spark = get_spark_session()

    from ingestion.bootstrap import bootstrap_hooks

    bootstrap_hooks(spark, args.catalog, args.schema)

    parsed = _parse_vaep_match_ids_arg(getattr(args, "match_ids", None))

    if parsed is None:
        # Monolithic mode (backward compat / local dev)
        logger.warning(
            "Running in monolithic mode (no --match-ids). Production uses preflight_spadl_vaep + for_each_task."
        )
        filter_result = timed_check(skip_guard, spark, args.catalog, args.schema)
        logger.info("Starting SPADL/VAEP pipeline into %s.%s", args.catalog, args.schema)
        run_pipeline(spark, args.catalog, args.schema, logger, filter_result=filter_result)
    else:
        # Chunk mode (for_each_task iteration)
        provider, match_ids = parsed
        logger.info("Chunk mode: provider=%s, %d match_ids", provider, len(match_ids))
        _run_chunk(spark, args.catalog, args.schema, logger, provider, match_ids)


if __name__ == "__main__":
    main()
