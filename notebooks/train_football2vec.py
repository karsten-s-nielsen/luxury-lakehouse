# Databricks notebook source
# MAGIC %md
# MAGIC # Football2Vec Training Pipeline
# MAGIC Train gensim Doc2Vec on StatsBomb + Wyscout event sequences.
# MAGIC
# MAGIC **Output:**
# MAGIC - Model weights &rarr; `/Volumes/soccer_analytics/dev_gold/model_weights/football2vec/`
# MAGIC - MLflow Registry &rarr; `soccer_analytics.dev_gold.football2vec` (`@Champion` alias)
# MAGIC - HuggingFace Hub &rarr; `luxury-lakehouse/football2vec-statsbomb-wyscout`

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE VOLUME IF NOT EXISTS soccer_analytics.dev_gold.model_weights;

# COMMAND ----------

import sys

sys.path.insert(0, "/Workspace/Users/karstenskyt@gmail.com/luxury-lakehouse/src")

import mlflow  # noqa: E402 — pre-installed on Databricks runtime

from analytics.football2vec import (
    Football2VecModel,
    TokenizerConfig,
    TrainingConfig,
    tokenize_match_events,
    train_model,
)

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

# MAGIC %md
# MAGIC ## Train Doc2Vec with MLflow Tracking

# COMMAND ----------

training_config = TrainingConfig()

mlflow.set_experiment("/soccer_analytics/football2vec")

# Capture Delta table versions for reproducibility (E5)
_sb_version = spark.sql(  # noqa: F821 — spark is a Databricks runtime global
    "DESCRIBE HISTORY soccer_analytics.dev_silver.stg_statsbomb__events LIMIT 1"
).first()["version"]
_ws_version = spark.sql(  # noqa: F821 — spark is a Databricks runtime global
    "DESCRIBE HISTORY soccer_analytics.dev_silver.stg_wyscout__events LIMIT 1"
).first()["version"]

with mlflow.start_run(run_name="football2vec_training") as run:
    # Log parameters
    mlflow.log_params(
        {
            "vector_size": training_config.vector_size,
            "window": training_config.window,
            "min_count": training_config.min_count,
            "epochs": training_config.epochs,
            "dm": training_config.dm,
            "grid_cols": config.grid_cols,
            "grid_rows": config.grid_rows,
            "n_statsbomb_docs": len(sb_docs),
            "n_wyscout_docs": len(wy_docs),
            "n_total_docs": len(all_docs),
            "n_statsbomb_events": len(sb_events),
            "n_wyscout_events": len(wy_events),
            "stg_statsbomb__events_delta_version": int(_sb_version),
            "stg_wyscout__events_delta_version": int(_ws_version),
        }
    )

    # Train model
    model = train_model(all_docs, training_config)

    # Log training metrics
    mlflow.log_metrics(
        {
            "vocabulary_size": len(model.wv),
            "document_vectors": len(model.dv),
            "vector_size": model.vector_size,
        }
    )

    print(f"Vocabulary size: {len(model.wv):,}")
    print(f"Document vectors: {len(model.dv):,}")
    print(f"Vector size: {model.vector_size}")

    # Save model to UC Volume first (needed for pyfunc artifact path)
    model_path = "/Volumes/soccer_analytics/dev_gold/model_weights/football2vec"
    dbutils.fs.mkdirs(model_path)  # noqa: F821 — dbutils is a Databricks runtime global
    local_path = f"{model_path}/player2vec.model"
    model.save(local_path)
    print(f"Model saved to {local_path}")

    # Save tokenizer config alongside model
    import json  # noqa: E402 — cell-based notebook import

    tokenizer_config_data = {
        "grid_cols": config.grid_cols,
        "grid_rows": config.grid_rows,
        "pitch_length": config.pitch_length,
        "pitch_width": config.pitch_width,
    }
    tokenizer_config_path = f"{model_path}/tokenizer_config.json"
    with open(tokenizer_config_path, "w") as f:
        json.dump(tokenizer_config_data, f, indent=2)

    # Log Football2Vec pyfunc model with artifacts pointing to UC Volume
    mlflow.pyfunc.log_model(
        artifact_path="football2vec_model",
        python_model=Football2VecModel(),
        artifacts={"model_dir": model_path},
        registered_model_name="soccer_analytics.dev_gold.football2vec",
    )

    run_id = run.info.run_id
    print(f"\nMLflow run ID: {run_id}")

# COMMAND ----------

# Verify model load
from gensim.models.doc2vec import Doc2Vec  # noqa: E402 — cell-based notebook import

loaded = Doc2Vec.load(local_path)
print(f"Verified — vocab: {len(loaded.wv):,}, docs: {len(loaded.dv):,}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Register @Champion Alias

# COMMAND ----------

client = mlflow.tracking.MlflowClient()
model_name = "soccer_analytics.dev_gold.football2vec"

latest_versions = client.search_model_versions(f"name='{model_name}'", order_by=["version_number DESC"], max_results=1)
if latest_versions:
    latest_version = latest_versions[0].version
    client.set_registered_model_alias(model_name, "Champion", latest_version)
    print(f"Set @Champion alias to version {latest_version}")
else:
    print("WARNING: No model versions found — alias not set")

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
