# Databricks notebook source
# MAGIC %md
# MAGIC # Football2Vec Training Pipeline
# MAGIC Train gensim Doc2Vec on StatsBomb + Wyscout event sequences.

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE VOLUME IF NOT EXISTS soccer_analytics.dev_gold.model_weights;

# COMMAND ----------

import sys

sys.path.insert(0, "/Workspace/Users/karstenskyt@gmail.com/luxury-lakehouse/src")

from analytics.football2vec import TokenizerConfig, TrainingConfig, tokenize_match_events, train_model

print("Imports OK")

# COMMAND ----------

# Load StatsBomb events with canonical_player_id
# Uses staging view (stg_statsbomb__events) which parses JSON location and renames columns.
# Column mapping: event_index -> event_index, event_type -> event_type
sb_events = spark.sql("""
    SELECT CAST(p.canonical_player_id AS STRING) AS canonical_player_id,
           CAST(e.match_id AS STRING) AS match_id,
           e.event_type,
           e.location_x AS x, e.location_y AS y,
           e.index AS event_index,
           e.play_pattern,
           e.pass_cross,
           'statsbomb' AS data_source
    FROM soccer_analytics.dev_silver.stg_statsbomb__events e
    JOIN soccer_analytics.dev_gold.dim_players p ON e.player_id = p.player_id
    WHERE e.location_x IS NOT NULL AND e.location_y IS NOT NULL
      AND e.player_id IS NOT NULL
""").toPandas()  # noqa: F821 — spark is a Databricks runtime global

print(f"StatsBomb events: {len(sb_events):,}")

# COMMAND ----------

# Load Wyscout events with canonical_player_id
# Uses staging view (stg_wyscout__events) which parses positions JSON and scales to 120x80.
# Column mapping: event_sec -> event_index, start_x/start_y -> x/y
wy_events = spark.sql("""
    SELECT CAST(p.canonical_player_id AS STRING) AS canonical_player_id,
           CAST(e.match_id AS STRING) AS match_id,
           e.event_type,
           e.start_x AS x, e.start_y AS y,
           e.event_sec AS event_index,
           e.sub_event_type,
           'wyscout' AS data_source
    FROM soccer_analytics.dev_silver.stg_wyscout__events e
    JOIN soccer_analytics.dev_gold.dim_players p
      ON e.player_id = p.player_id
    WHERE e.start_x IS NOT NULL AND e.start_y IS NOT NULL
      AND e.player_id IS NOT NULL
""").toPandas()  # noqa: F821 — spark is a Databricks runtime global

print(f"Wyscout events: {len(wy_events):,}")

# COMMAND ----------

# Tokenize events
config = TokenizerConfig()

sb_docs = tokenize_match_events(sb_events, config)
wy_docs = tokenize_match_events(wy_events, config)
all_docs = {**sb_docs, **wy_docs}

print(f"Total player-match documents: {len(all_docs):,}")
print(f"  StatsBomb: {len(sb_docs):,}")
print(f"  Wyscout: {len(wy_docs):,}")

# COMMAND ----------

# Train Doc2Vec model
training_config = TrainingConfig()
model = train_model(all_docs, training_config)

print(f"Vocabulary size: {len(model.wv):,}")
print(f"Document vectors: {len(model.dv):,}")
print(f"Vector size: {model.vector_size}")

# COMMAND ----------

# Save to UC Volume
model_path = "/Volumes/soccer_analytics/dev_gold/model_weights/football2vec"
dbutils.fs.mkdirs(model_path)  # noqa: F821 — dbutils is a Databricks runtime global
local_path = f"{model_path}/player2vec.model"
model.save(local_path)
print(f"Model saved to {local_path}")

# Verify
from gensim.models.doc2vec import Doc2Vec  # noqa: E402 — cell-based notebook import

loaded = Doc2Vec.load(local_path)
print(f"Verified — vocab: {len(loaded.wv):,}, docs: {len(loaded.dv):,}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Publish to HuggingFace Hub

# COMMAND ----------

import os  # noqa: E402 — cell-based notebook import

try:
    hf_token = dbutils.secrets.get(scope="hf", key="token")  # noqa: F821 — dbutils is a Databricks runtime global
    os.environ["HF_TOKEN"] = hf_token
    from huggingface_hub import HfApi

    api = HfApi()
    api.upload_folder(
        folder_path="/Volumes/soccer_analytics/dev_gold/model_weights/football2vec/",
        repo_id="luxury-lakehouse/football2vec-statsbomb-wyscout",
        token=hf_token,
    )
    print("Published to HuggingFace Hub")
except Exception as exc:
    print(f"HF publish skipped (set secret scope 'hf' key 'token' to enable): {exc}")

# COMMAND ----------

print("Training pipeline complete!")
