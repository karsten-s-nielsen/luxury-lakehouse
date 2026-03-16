# Databricks notebook source
# MAGIC %md
# MAGIC # Import OBSO Results to Delta
# MAGIC Reads `pausa_raw_scores.parquet` from UC Volume staging and writes to bronze Delta.

# COMMAND ----------

from datetime import datetime, timezone

catalog = "soccer_analytics"
schema = "bronze"
staging_path = f"/Volumes/{catalog}/{schema}/libs/hf_export/pausa_raw_scores.parquet"
table_name = f"{catalog}.{schema}.pausa_raw_scores"

# Read Parquet from UC Volume
scores_df = spark.read.parquet(staging_path)  # noqa: F821
row_count = scores_df.count()
print(f"Read {row_count} rows from {staging_path}")
print(f"Columns: {scores_df.columns}")

# Add audit column
from pyspark.sql import functions as F  # noqa: N812, E402

scores_df = scores_df.withColumn("_ingested_at", F.current_timestamp())

# Write to Delta with replaceWhere on match_id
match_ids = [row["match_id"] for row in scores_df.select("match_id").distinct().collect()]
match_ids_str = ", ".join(f"'{mid}'" for mid in match_ids)
replace_expr = f"match_id IN ({match_ids_str})"

(
    scores_df.write
    .format("delta")
    .mode("overwrite")
    .option("replaceWhere", replace_expr)
    .option("overwriteSchema", "true")
    .saveAsTable(table_name)
)

print(f"Wrote {row_count} rows to {table_name}")
print(f"Match IDs: {match_ids}")

# Verify
verify_count = spark.table(table_name).count()  # noqa: F821
print(f"Verification: {verify_count} rows in {table_name}")
