# Databricks notebook source
CATALOG = "soccer_analytics"

# COMMAND ----------

# Check if bronze/intermediate DEFCON tables exist
tables_to_check = [
    f"{CATALOG}.bronze.fct_defcon_results",
    f"{CATALOG}.dev_gold.fct_defcon_results",
    f"{CATALOG}.dev_gold.fct_defcon_actions",
    f"{CATALOG}.dev_gold.fct_defensive_values",
]

results = []
for tbl in tables_to_check:
    try:
        cnt = spark.sql(f"SELECT COUNT(*) AS cnt FROM {tbl}").collect()[0]["cnt"]
        results.append(f"{tbl}: {cnt} rows")
    except Exception as e:
        results.append(f"{tbl}: ERROR - {str(e)[:100]}")

dbutils.notebook.exit(" | ".join(results))
