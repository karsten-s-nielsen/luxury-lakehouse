# Databricks notebook source
# MAGIC %md
# MAGIC # Publish OBSO/PAUSA Prerequisite Data to HuggingFace Hub
# MAGIC Export IDSSE events and ELASTIC sync results as Parquet datasets with HF
# MAGIC dataset cards.  These serve as inputs for the D16 OBSO HF Jobs GPU script.
# MAGIC
# MAGIC **Architecture**: Spark writes Parquet to a UC Volume (executor-side),
# MAGIC then the driver uploads from the Volume path to HF Hub.  This keeps
# MAGIC data on executors — no `.toPandas()` OOM risk.
# MAGIC
# MAGIC **Prerequisite HF datasets:**
# MAGIC - Tracking data already published at `luxury-lakehouse/pitch-control-tracking`
# MAGIC   (IDSSE partition: `source_provider=idsse`)
# MAGIC
# MAGIC **Datasets published by this notebook:**
# MAGIC - `luxury-lakehouse/obso-pausa-inputs` — IDSSE events + ELASTIC sync results

# COMMAND ----------

import os
import shutil
from pathlib import Path

from huggingface_hub import HfApi

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
CATALOG = "soccer_analytics"
SCHEMA = "bronze"
HF_ORG = "luxury-lakehouse"
REPO_NAME = "obso-pausa-inputs"
REPO_ID = f"{HF_ORG}/{REPO_NAME}"

# UC Volume for staging Parquet exports (Spark CAN write here on serverless)
_VOLUME_STAGE = f"/Volumes/{CATALOG}/bronze/libs/hf_export"

# Serverless local tmp for non-Spark files (dataset cards)
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
# MAGIC ## Export IDSSE Events

# COMMAND ----------

print("Exporting IDSSE events ...")

events_vol = f"{_VOLUME_STAGE}/{REPO_NAME}"
events_data = f"{events_vol}/data/events"

try:
    events_df = spark.sql(f"""
        SELECT match_id, event_id, event_type, timestamp_seconds,
               period, player_id, team, x, y
        FROM {CATALOG}.{SCHEMA}.idsse_events
    """)  # noqa: F821, S608
    events_count = events_df.count()
    if events_count == 0:
        raise ValueError("idsse_events: 0 rows — run ingest_idsse_events first")
    events_df.write.mode("overwrite").partitionBy("match_id").parquet(events_data)
    print(f"  Events: {events_count:,} rows written to {events_data}")
except Exception as e:
    print(f"  ERROR exporting events: {e}")
    raise

# COMMAND ----------

# MAGIC %md
# MAGIC ## Export ELASTIC Sync Results

# COMMAND ----------

print("Exporting ELASTIC sync results ...")

sync_data = f"{events_vol}/data/elastic_sync"

try:
    sync_df = spark.sql(f"""
        SELECT match_id, event_id, frame_id,
               alignment_confidence, alignment_error_seconds
        FROM {CATALOG}.{SCHEMA}.elastic_sync_results
    """)  # noqa: F821, S608
    sync_count = sync_df.count()
    if sync_count == 0:
        raise ValueError("elastic_sync_results: 0 rows — run compute_elastic_sync first")
    sync_df.write.mode("overwrite").partitionBy("match_id").parquet(sync_data)
    print(f"  Sync results: {sync_count:,} rows written to {sync_data}")
except Exception as e:
    print(f"  ERROR exporting sync results: {e}")
    raise

# COMMAND ----------

# MAGIC %md
# MAGIC ## Write Dataset Card and Upload

# COMMAND ----------

# PR 4c: dataset card published via the shared helper from the in-repo
# source of truth at docs/huggingface/dataset-cards/obso-pausa-inputs.md,
# which lives at CARD_BASE below (Workspace path mirroring the lakehouse
# repo). The prior inline-string card was deleted to eliminate drift
# between this notebook and the in-repo markdown.
CARD_BASE = "/Workspace/Users/karstenskyt@gmail.com/luxury-lakehouse/docs/huggingface/dataset-cards"
card_src = Path(CARD_BASE) / "obso-pausa-inputs.md"
card_dst = Path(events_vol) / "README.md"
shutil.copy2(str(card_src), str(card_dst))
print(f"  Dataset card copied from {card_src}")

# Upload to HF Hub
api.create_repo(repo_id=REPO_ID, repo_type="dataset", exist_ok=True, token=hf_token)
api.upload_folder(
    folder_path=events_vol,
    repo_id=REPO_ID,
    repo_type="dataset",
    token=hf_token,
)
print(f"  Published: https://huggingface.co/datasets/{REPO_ID}")

# COMMAND ----------

# Clean up staging directory
shutil.rmtree(events_vol, ignore_errors=True)
print("Staging directory cleaned up")
print("OBSO/PAUSA input data publishing complete!")
