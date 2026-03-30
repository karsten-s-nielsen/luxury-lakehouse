# Databricks notebook source
# MAGIC %md
# MAGIC # Publish Gold Datasets to HuggingFace Hub
# MAGIC Export Delta Lake gold tables as Parquet datasets with HF dataset cards.
# MAGIC
# MAGIC **Architecture**: Spark writes Parquet to a UC Volume (executor-side),
# MAGIC then the driver uploads from the Volume path to HF Hub. This keeps
# MAGIC data on executors — no `.toPandas()` OOM risk.

# COMMAND ----------

# MAGIC %pip install huggingface_hub -q
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

import os
import shutil
from pathlib import Path

from huggingface_hub import HfApi

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
CATALOG = "soccer_analytics"
GOLD_SCHEMA = "dev_gold"
HF_ORG = "luxury-lakehouse"

CARD_BASE = "/Workspace/Users/karstenskyt@gmail.com/luxury-lakehouse/docs/huggingface/dataset-cards"

# UC Volume for staging Parquet exports (Spark CAN write here on serverless)
_VOLUME_STAGE = f"/Volumes/{CATALOG}/bronze/libs/hf_export"

# Serverless local tmp for non-Spark files (dataset cards, model cards)
_LOCAL_TMP = "/local_disk0/tmp"

# HF token: prefer Databricks secrets, fall back to env var
try:
    hf_token: str = dbutils.secrets.get(scope="hf", key="token")  # noqa: F821
except Exception:
    hf_token = os.environ.get("HF_TOKEN", "")
    if not hf_token:
        raise RuntimeError("HF token not found")  # noqa: B904

os.environ["HF_TOKEN"] = hf_token
api = HfApi()

# Ensure staging volume directory exists
dbutils.fs.mkdirs(_VOLUME_STAGE.replace("/Volumes/", "dbfs:/Volumes/"))  # noqa: F821
print("Setup complete")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Helper: `publish_dataset`

# COMMAND ----------


def publish_dataset(
    sql_query: str,
    repo_name: str,
    card_path: str,
    partition_cols: list[str] | None = None,
) -> None:
    """Export a Spark SQL query to Parquet via UC Volume and upload to HF Hub.

    Spark writes Parquet to UC Volume (executor-side, no driver OOM risk).
    Driver then uploads from the Volume path to HuggingFace.
    """
    repo_id = f"{HF_ORG}/{repo_name}"
    vol_dir = f"{_VOLUME_STAGE}/{repo_name}"
    data_dir = f"{vol_dir}/data"

    try:
        df = spark.sql(sql_query)  # noqa: F821
        row_count = df.count()
        if row_count == 0:
            raise ValueError(f"Query returned 0 rows for {repo_name}")
        print(f"  Rows: {row_count:,}")

        # Spark writes Parquet to UC Volume (executor-side)
        writer = df.write.mode("overwrite")
        if partition_cols:
            writer = writer.partitionBy(*partition_cols)
        writer.parquet(data_dir)
        print(f"  Parquet written to {data_dir}")

        # Copy dataset card as README.md into the volume staging dir
        card_src = Path(card_path)
        card_dst = Path(vol_dir) / "README.md"
        shutil.copy2(str(card_src), str(card_dst))

        # Upload from UC Volume path to HF Hub
        api.create_repo(repo_id=repo_id, repo_type="dataset", exist_ok=True, token=hf_token)
        api.upload_folder(
            folder_path=vol_dir,
            repo_id=repo_id,
            repo_type="dataset",
            token=hf_token,
        )
        print(f"  Published: https://huggingface.co/datasets/{repo_id}")

    finally:
        # Clean up staging directory
        shutil.rmtree(vol_dir, ignore_errors=True)


# COMMAND ----------

# MAGIC %md
# MAGIC ## Dataset 1: SPADL/VAEP Action Values

# COMMAND ----------

print("Publishing SPADL/VAEP Action Values ...")

publish_dataset(
    sql_query=f"""
        SELECT action_value_id, match_id, player_id, team_id,
               competition_id, season_id, period, time_seconds,
               minute, second, start_x, start_y, end_x, end_y,
               action_type, action_result, bodypart,
               offensive_value, defensive_value, vaep_value,
               data_source, original_event_id
        FROM {CATALOG}.{GOLD_SCHEMA}.fct_action_values
    """,  # noqa: S608
    repo_name="spadl-vaep-action-values",
    card_path=f"{CARD_BASE}/spadl-vaep.md",
    partition_cols=["data_source"],
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Dataset 2: Line-Breaking Passes

# COMMAND ----------

print("Publishing Line-Breaking Passes ...")

publish_dataset(
    sql_query=f"""
        SELECT pass_id, match_id, player_id, team_id, pass_recipient_id,
               competition_id, season_id, period, minute, second,
               start_x, start_y, end_x, end_y,
               pass_type, pass_height, body_part, pass_length, pass_angle_radians,
               pass_outcome, is_cross, is_switch, is_through_ball, is_complete, is_progressive,
               pass_direction, is_line_breaking, lines_broken, line_breaking_type,
               data_source
        FROM {CATALOG}.{GOLD_SCHEMA}.fct_passes
    """,  # noqa: S608
    repo_name="line-breaking-passes",
    card_path=f"{CARD_BASE}/line-breaking.md",
    partition_cols=["data_source"],
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Dataset 3: Player Embeddings (Career / Season / Per-Match)

# COMMAND ----------

print("Publishing Player Embeddings ...")

_emb_repo = f"{HF_ORG}/football2vec-player-embeddings"
_emb_card = f"{CARD_BASE}/player-embeddings.md"
_emb_vol = f"{_VOLUME_STAGE}/football2vec-player-embeddings"

try:
    # --- Career vectors (default config) ---
    career_dir = f"{_emb_vol}/data/career"
    career_df = spark.sql(f"""
        SELECT e.canonical_player_id, p.player_name,
               e.behavioral_vector, e.stat_vector,
               e.total_matches, e.data_sources
        FROM {CATALOG}.{GOLD_SCHEMA}.fct_player_embeddings_career e
        LEFT JOIN {CATALOG}.{GOLD_SCHEMA}.dim_players p
          ON e.canonical_player_id = p.canonical_player_id
    """)  # noqa: F821, S608
    career_count = career_df.count()
    if career_count == 0:
        raise ValueError("Career embeddings: 0 rows")
    career_df.write.mode("overwrite").parquet(career_dir)
    print(f"  Career vectors: {career_count:,} rows")

    # --- Season vectors ---
    season_dir = f"{_emb_vol}/data/season"
    season_df = spark.sql(f"""
        SELECT embedding_season_id, canonical_player_id, competition_id, season_id,
               behavioral_vector, stat_vector, matches_in_sample, data_sources
        FROM {CATALOG}.{GOLD_SCHEMA}.fct_player_embeddings_season
    """)  # noqa: F821, S608
    season_count = season_df.count()
    if season_count == 0:
        raise ValueError("Season embeddings: 0 rows")
    season_df.write.mode("overwrite").parquet(season_dir)
    print(f"  Season vectors: {season_count:,} rows")

    # --- Per-match vectors ---
    match_dir = f"{_emb_vol}/data/per_match"
    match_df = spark.sql(f"""
        SELECT embedding_id, canonical_player_id, match_id, data_source,
               behavioral_vector, stat_vector
        FROM {CATALOG}.{GOLD_SCHEMA}.fct_player_embeddings
    """)  # noqa: F821, S608
    match_count = match_df.count()
    if match_count == 0:
        raise ValueError("Per-match embeddings: 0 rows")
    match_df.write.mode("overwrite").parquet(match_dir)
    print(f"  Per-match vectors: {match_count:,} rows")

    # --- Copy dataset card ---
    shutil.copy2(_emb_card, os.path.join(_emb_vol, "README.md"))

    # --- Upload ---
    api.create_repo(repo_id=_emb_repo, repo_type="dataset", exist_ok=True, token=hf_token)
    api.upload_folder(
        folder_path=_emb_vol,
        repo_id=_emb_repo,
        repo_type="dataset",
        token=hf_token,
    )
    print(f"  Published: https://huggingface.co/datasets/{_emb_repo}")

finally:
    shutil.rmtree(_emb_vol, ignore_errors=True)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Dataset 4: Pitch Control Tracking Data

# COMMAND ----------

print("Publishing Pitch Control Tracking Data ...")

publish_dataset(
    sql_query=f"""
        SELECT t.tracking_id, t.match_id, t.player_id, t.team, t.period, t.frame,
               t.timestamp_seconds, t.x, t.y, t.ball_x, t.ball_y,
               t.velocity_x, t.velocity_y, t.speed_ms,
               pc.pitch_control_value,
               t.source_provider, t.frame_rate
        FROM {CATALOG}.{GOLD_SCHEMA}.fct_tracking_frames t
        INNER JOIN {CATALOG}.dev_silver.stg_pitch_control__values pc
          ON t.tracking_id = pc.tracking_id
    """,  # noqa: S608
    repo_name="pitch-control-tracking",
    card_path=f"{CARD_BASE}/pitch-control.md",
    partition_cols=["source_provider"],
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Dataset 5: xG Freeze-Frame Positions (D17)

# COMMAND ----------

print("Publishing xG Freeze-Frame Positions ...")

publish_dataset(
    sql_query=f"""
        SELECT
            e.event_id,
            e.match_id,
            e.competition_id,
            e.season_id,
            ff.location[0] / 120.0 AS player_x_norm,
            ff.location[1] / 80.0  AS player_y_norm,
            COALESCE(ff.keeper, false)   AS is_keeper,
            COALESCE(ff.teammate, false) AS is_teammate
        FROM {CATALOG}.dev_silver.stg_statsbomb__events e
        LATERAL VIEW EXPLODE(
            from_json(
                e.shot_freeze_frame,
                'ARRAY<STRUCT<location:ARRAY<DOUBLE>,teammate:BOOLEAN,actor:BOOLEAN,keeper:BOOLEAN>>'
            )
        ) AS ff
        WHERE e.event_type = 'Shot'
          AND e.shot_freeze_frame IS NOT NULL
    """,  # noqa: S608
    repo_name="xg-freeze-frame-data",
    card_path=f"{CARD_BASE}/xg-freeze-frame.md",
    partition_cols=["competition_id"],
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Update Model Card on HuggingFace Hub

# COMMAND ----------

import shutil as _shutil  # noqa: E402
import tempfile as _tempfile  # noqa: E402
from pathlib import Path as _Path  # noqa: E402

_WORKSPACE_ROOT = "/Workspace/Users/karstenskyt@gmail.com/luxury-lakehouse"

_model_card_src = _Path(f"{_WORKSPACE_ROOT}/docs/huggingface/model-card.md")
_model_repo = f"{HF_ORG}/football2vec-statsbomb-wyscout"
_model_tmp = _tempfile.mkdtemp(prefix="hf_model_card_", dir=_LOCAL_TMP)
try:
    _shutil.copy2(str(_model_card_src), f"{_model_tmp}/README.md")
    api.upload_folder(
        folder_path=_model_tmp,
        repo_id=_model_repo,
        repo_type="model",
        token=hf_token,
    )
    print(f"  Model card updated: https://huggingface.co/{_model_repo}")
finally:
    _shutil.rmtree(_model_tmp, ignore_errors=True)

print("Model card update complete!")
print("NOTE: Org card -> paste docs/huggingface/org-card.md via HF web UI")
print("NOTE: Org interests -> paste docs/huggingface/org-interests.md via HF web UI")

# COMMAND ----------

print("All publishing complete!")
