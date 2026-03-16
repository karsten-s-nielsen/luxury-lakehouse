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

# Write dataset card README.md
card = """\
---
language: [en]
license: mit
task_categories: [tabular-regression]
tags:
  - sports-analytics
  - soccer
  - football
  - obso
  - pausa
  - elastic-sync
  - event-data
  - idsse
  - bundesliga
size_categories:
  - 1K-10K
configs:
  - config_name: events
    data_files:
      - split: train
        path: "data/events/**/*.parquet"
  - config_name: elastic_sync
    data_files:
      - split: train
        path: "data/elastic_sync/**/*.parquet"
---

# OBSO/PAUSA Input Data (IDSSE Events + ELASTIC Sync)

Prerequisite datasets for computing OBSO value surfaces and PAUSA pass timing
scores. Contains IDSSE Bundesliga event data and ELASTIC event-tracking
synchronization results for 7 matches.

Part of the [(Right! Luxury!) Lakehouse](https://huggingface.co/luxury-lakehouse)
soccer analytics platform.

## Datasets

### Events (`data/events/`)

DFL event data from 7 IDSSE Bundesliga matches. One row per event with position
data. Coordinates are in DFL pitch-origin meters (x: 0-105, y: 0-68).

| Column | Type | Description |
|--------|------|-------------|
| `match_id` | string | Match identifier (`idsse_J03...`) |
| `event_id` | string | Unique event identifier within match |
| `event_type` | string | DFL event type (`Play`, `KickOff`, `TacklingGame`, etc.) |
| `timestamp_seconds` | double | Seconds from period start |
| `period` | int | Match half (1 or 2) |
| `player_id` | string | DFL PersonId of acting player |
| `team` | string | Team affiliation (`home` or `away`) |
| `x` | double | Event x-coordinate (DFL pitch-origin meters, 0-105) |
| `y` | double | Event y-coordinate (DFL pitch-origin meters, 0-68) |

### ELASTIC Sync Results (`data/elastic_sync/`)

Event-to-frame alignments produced by the ELASTIC algorithm (Kim et al. 2025).
Maps each event to its best-matching tracking frame via ball acceleration and
player-ball proximity features.

| Column | Type | Description |
|--------|------|-------------|
| `match_id` | string | Match identifier (`idsse_J03...`) |
| `event_id` | string | Event identifier (joins to events) |
| `frame_id` | int | Best-matching tracking frame number |
| `alignment_confidence` | double | Confidence score (0 to 1) |
| `alignment_error_seconds` | double | Time error between event and aligned frame |

## Coordinate Systems

- **Events**: DFL pitch-origin meters (x: 0-105, y: 0-68). Transform to StatsBomb
  120x80: `x_sb = x / 105.0 * 120.0`, `y_sb = y / 68.0 * 80.0`.
- **Tracking** (separate dataset): Already in StatsBomb 120x80 at
  `luxury-lakehouse/pitch-control-tracking` (IDSSE partition).
- **Frame mapping**: `elastic_sync.frame_id` maps to tracking `frame` column.

## Usage with OBSO Pipeline

```python
from datasets import load_dataset

events = load_dataset("luxury-lakehouse/obso-pausa-inputs", "events")["train"]
sync = load_dataset("luxury-lakehouse/obso-pausa-inputs", "elastic_sync")["train"]
tracking = load_dataset("luxury-lakehouse/pitch-control-tracking",
                        data_files="data/source_provider=idsse/**/*.parquet")["train"]
```

## References

- Bassek et al. (2025). "An integrated dataset of spatiotemporal and event data
  in elite soccer." Scientific Data, Nature. CC-BY 4.0.
- Kim, H.S. et al. (2025). "ELASTIC: Event-Tracking Data Synchronization in Soccer
  Without Annotated Event Locations." ECML-PKDD MLSA 2025. arXiv:2508.09238.

## License

MIT — computed from IDSSE open data (CC-BY 4.0).
"""

card_path = Path(events_vol) / "README.md"
with open(str(card_path), "w", encoding="utf-8") as f:
    f.write(card)
print("  Dataset card written")

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
