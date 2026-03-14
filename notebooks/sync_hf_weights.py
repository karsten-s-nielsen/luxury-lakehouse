# Databricks notebook source
# MAGIC %md
# MAGIC # Sync Model Weights from HuggingFace Hub to UC Volume
# MAGIC
# MAGIC Downloads trained model weights from HF Hub model repos and writes them
# MAGIC to UC Volumes for use by Databricks inference pipelines.
# MAGIC
# MAGIC **Models synced:**
# MAGIC - `luxury-lakehouse/xg-model-statsbomb-wyscout` &rarr; `/Volumes/.../xg_model/`

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE VOLUME IF NOT EXISTS soccer_analytics.dev_gold.model_weights;

# COMMAND ----------

import os

from huggingface_hub import HfApi, hf_hub_download

# HF token: prefer Databricks secrets, fall back to env var
try:
    hf_token: str = dbutils.secrets.get(scope="hf", key="token")  # noqa: F821
except Exception:
    hf_token = os.environ.get("HF_TOKEN", "")
    if not hf_token:
        raise RuntimeError("HF token not found")  # noqa: B904

api = HfApi(token=hf_token)
print("Setup complete")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Sync xG Model Weights

# COMMAND ----------

xg_repo = "luxury-lakehouse/xg-model-statsbomb-wyscout"
xg_vol = "/Volumes/soccer_analytics/dev_gold/model_weights/xg_model"

# Check if weights already exist
xg_exists = os.path.exists(f"{xg_vol}/xgboost_model.json")
if xg_exists:
    print(f"xG model weights already present at {xg_vol} — skipping")
else:
    dbutils.fs.mkdirs(xg_vol)  # noqa: F821

    for filename in ["logistic_model.json", "xgboost_model.json", "metrics.json"]:
        try:
            local_path = hf_hub_download(
                repo_id=xg_repo,
                filename=filename,
                token=hf_token,
            )
            dest = f"{xg_vol}/{filename}"
            with open(local_path, "rb") as src_f, open(dest, "wb") as dst_f:
                dst_f.write(src_f.read())
            print(f"  Synced: {filename}")
        except Exception as exc:
            print(f"  SKIP: {filename} — {exc}")
    print(f"xG model: synced to {xg_vol}")

# COMMAND ----------

# Verify all model files are in place
print("\n=== Verification ===")
for label, vol_path, required in [
    ("xG Model", xg_vol, ["xgboost_model.json", "logistic_model.json"]),
]:
    print(f"\n{label} ({vol_path}):")
    for f in required:
        path = f"{vol_path}/{f}"
        if os.path.exists(path):
            size = os.path.getsize(path)
            print(f"  OK  {f} ({size:,} bytes)")
        else:
            print(f"  MISSING  {f}")

print("\nSync complete!")
