# Databricks notebook source
# MAGIC %md
# MAGIC # Custom xG Model Training
# MAGIC
# MAGIC Train logistic regression baseline + calibrated XGBoost for expected goals prediction.
# MAGIC Uses all shots from StatsBomb + Wyscout (~131K shots).
# MAGIC
# MAGIC **Output:**
# MAGIC - Model weights &rarr; `/Volumes/soccer_analytics/dev_gold/model_weights/xg_model/`
# MAGIC - HuggingFace Hub &rarr; `luxury-lakehouse/xg-model-statsbomb-wyscout`

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE VOLUME IF NOT EXISTS soccer_analytics.dev_gold.model_weights;

# COMMAND ----------

import sys

sys.path.insert(0, "/Workspace/Users/karstenskyt@gmail.com/luxury-lakehouse/src")

from analytics.xg_model import (
    XGModelConfig,
    build_features,
    evaluate_model,
    serialize_logistic_model,
    serialize_xgboost_model,
    train_logistic_baseline,
    train_xgboost_model,
)

print("Imports OK")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Load Data

# COMMAND ----------

shots_df = spark.sql(  # noqa: F821 — spark is a Databricks runtime global
    "SELECT * FROM soccer_analytics.dev_gold.fct_shots "
    "WHERE is_goal IS NOT NULL AND competition_id IS NOT NULL"
).toPandas()
shots_df = shots_df.dropna(subset=["is_goal", "competition_id"]).reset_index(drop=True)
print(f"Loaded {len(shots_df):,} shots")
print(f"  StatsBomb: {(shots_df['data_source'] == 'statsbomb').sum():,}")
print(f"  Wyscout:   {(shots_df['data_source'] == 'wyscout').sum():,}")
print(f"  Goal rate: {shots_df['is_goal'].mean():.1%}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Build Features and Split

# COMMAND ----------

from sklearn.model_selection import train_test_split  # noqa: E402 — cell-based notebook import

config = XGModelConfig()
X, y = build_features(shots_df, config)
print(f"Feature matrix: {X.shape}")
print(f"Target: {y.sum():,} goals / {len(y):,} shots ({y.mean():.1%} goal rate)")

# Stratified split by competition_id
X_train, X_test, y_train, y_test = train_test_split(
    X, y, test_size=config.test_size, random_state=config.random_state,
    stratify=shots_df["competition_id"],
)
print(f"Train: {len(X_train):,}, Test: {len(X_test):,}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Train Models

# COMMAND ----------

logistic_model = train_logistic_baseline(X_train, y_train, random_state=config.random_state)
print("Logistic baseline trained")

xgboost_model = train_xgboost_model(X_train, y_train, config)
print("XGBoost (calibrated) trained")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Evaluate

# COMMAND ----------

from sklearn.metrics import brier_score_loss  # noqa: E402 — cell-based notebook import

baseline_cols = [c for c in ["distance_to_goal", "shot_angle"] if c in X_test.columns]

logistic_metrics = evaluate_model(logistic_model, X_test[baseline_cols], y_test)
xgboost_metrics = evaluate_model(xgboost_model, X_test, y_test)

print("=== Logistic Baseline ===")
for k, v in logistic_metrics.items():
    print(f"  {k}: {v:.4f}")

print("\n=== XGBoost (Calibrated) ===")
for k, v in xgboost_metrics.items():
    print(f"  {k}: {v:.4f}")

# StatsBomb xG benchmark (StatsBomb shots only)
test_indices = X_test.index
sb_mask = (shots_df.loc[test_indices, "data_source"] == "statsbomb") & shots_df.loc[test_indices, "statsbomb_xg"].notna()
if sb_mask.any():
    sb_xg = shots_df.loc[test_indices[sb_mask], "statsbomb_xg"].clip(0.01, 0.99)
    sb_y = y_test.loc[sb_mask]
    sb_brier = brier_score_loss(sb_y, sb_xg)
    custom_brier = brier_score_loss(sb_y, xgboost_model.predict_proba(X_test.loc[sb_mask])[:, 1])
    print(f"\n=== StatsBomb Benchmark ===")
    print(f"  StatsBomb xG Brier: {sb_brier:.4f}")
    print(f"  Custom xG Brier:    {custom_brier:.4f}")
    print(f"  Ratio:              {custom_brier / sb_brier:.2%}")

    # Spec requirement: custom xG Brier within 10% of StatsBomb xG
    assert custom_brier <= sb_brier * 1.10, (
        f"FAIL: Custom Brier {custom_brier:.4f} > 110% of StatsBomb Brier {sb_brier:.4f}"
    )
    print("  Within 10% threshold")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Save Model Weights

# COMMAND ----------

import json  # noqa: E402 — cell-based notebook import

model_dir = "/Volumes/soccer_analytics/dev_gold/model_weights/xg_model"
dbutils.fs.mkdirs(model_dir)  # noqa: F821 — dbutils is a Databricks runtime global

logistic_bytes = serialize_logistic_model(logistic_model)
xgboost_bytes = serialize_xgboost_model(xgboost_model)

# Write to UC Volume (using Python file I/O on FUSE mount)
with open(f"{model_dir}/logistic_model.json", "wb") as f:
    f.write(logistic_bytes)
with open(f"{model_dir}/xgboost_model.json", "wb") as f:
    f.write(xgboost_bytes)

# Save evaluation metrics
metrics = {
    "logistic": logistic_metrics,
    "xgboost": xgboost_metrics,
    "config": {
        "n_estimators": config.n_estimators,
        "max_depth": config.max_depth,
        "learning_rate": config.learning_rate,
        "calibration_method": config.calibration_method,
        "n_features": len(X_train.columns),
        "feature_names": list(X_train.columns),
        "n_train": len(X_train),
        "n_test": len(X_test),
    },
}
with open(f"{model_dir}/metrics.json", "w") as f:
    json.dump(metrics, f, indent=2)

print(f"Saved to {model_dir}/")
print(f"  logistic_model.json: {len(logistic_bytes):,} bytes")
print(f"  xgboost_model.json:  {len(xgboost_bytes):,} bytes")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Publish to HuggingFace Hub

# COMMAND ----------

try:
    hf_token = dbutils.secrets.get(scope="hf", key="token")  # noqa: F821 — dbutils is a Databricks runtime global
    from huggingface_hub import HfApi  # noqa: E402 — cell-based notebook import

    api = HfApi(token=hf_token)
    repo_id = "luxury-lakehouse/xg-model-statsbomb-wyscout"

    # Create repo if needed
    api.create_repo(repo_id, exist_ok=True, repo_type="model")

    # Upload model weights
    api.upload_file(
        path_or_fileobj=logistic_bytes,
        path_in_repo="logistic_model.json",
        repo_id=repo_id,
    )
    api.upload_file(
        path_or_fileobj=xgboost_bytes,
        path_in_repo="xgboost_model.json",
        repo_id=repo_id,
    )
    api.upload_file(
        path_or_fileobj=json.dumps(metrics, indent=2).encode(),
        path_in_repo="metrics.json",
        repo_id=repo_id,
    )

    # Upload model card as README.md
    model_card_path = "/Workspace/Users/karstenskyt@gmail.com/luxury-lakehouse/docs/huggingface/xg-model-card.md"
    api.upload_file(
        path_or_fileobj=model_card_path,
        path_in_repo="README.md",
        repo_id=repo_id,
    )

    print(f"Published to https://huggingface.co/{repo_id}")
except Exception as exc:
    print(f"HF Hub publish skipped (set secret scope 'hf' key 'token' to enable): {exc}")

# COMMAND ----------

print("Training pipeline complete!")
