# Databricks notebook source
# MAGIC %md
# MAGIC # Export Demo Data for HF Space
# MAGIC
# MAGIC Exports sample tracking data (D1) and DEFCON pressure data (D2) to UC Volume.
# MAGIC Download from Volume to `demo_space/data/` locally after running.

# COMMAND ----------

CATALOG = "soccer_analytics"
VOLUME_PATH = f"/Volumes/{CATALOG}/bronze/libs/hf_export/demo"

# COMMAND ----------

# MAGIC %md
# MAGIC ## D1: Sample Tracking Data (2 Metrica matches at 1fps)

# COMMAND ----------

tracking_df = spark.sql(f"""
    SELECT
        match_id,
        period,
        frame,
        timestamp_seconds,
        player_id,
        team,
        x,
        y,
        velocity_x,
        velocity_y,
        ball_x,
        ball_y,
        speed_ms,
        source_provider,
        frame_rate
    FROM {CATALOG}.dev_gold.fct_tracking_frames
    WHERE source_provider = 'metrica'
      AND MOD(frame, frame_rate) = 0  -- 1fps sampling
    ORDER BY match_id, period, frame, player_id
""")

print(f"Tracking rows (1fps): {tracking_df.count():,}")

tracking_df.coalesce(1).write.mode("overwrite").parquet(f"{VOLUME_PATH}/sample_tracking")

# COMMAND ----------

# MAGIC %md
# MAGIC ## D2: DEFCON Pressure (denormalized with player names + match labels)

# COMMAND ----------

# Aggregate directly from fct_defcon_actions (fct_defcon_pressure is empty
# when defcon_enabled=false in dbt). Join dim_players on action_player_id.
pressure_df = spark.sql(f"""
    WITH pressure_agg AS (
        SELECT
            action_player_id,
            match_id,
            competition_id,
            season_id,
            data_source,
            SUM(defcon_value) AS total_pressure,
            COUNT(*) AS total_defensive_actions,
            SUM(CASE WHEN credit_type = 'intercept' THEN defcon_value ELSE 0 END) AS intercept_pressure,
            SUM(CASE WHEN credit_type = 'concede' THEN defcon_value ELSE 0 END) AS concede_pressure,
            SUM(CASE WHEN credit_type = 'disturb' THEN defcon_value ELSE 0 END) AS disturb_pressure,
            SUM(CASE WHEN credit_type = 'deter' THEN defcon_value ELSE 0 END) AS deter_pressure,
            SUM(CASE WHEN credit_type = 'intercept' THEN 1 ELSE 0 END) AS intercept_count,
            SUM(CASE WHEN credit_type = 'concede' THEN 1 ELSE 0 END) AS concede_count,
            SUM(CASE WHEN credit_type = 'disturb' THEN 1 ELSE 0 END) AS disturb_count,
            SUM(CASE WHEN credit_type = 'deter' THEN 1 ELSE 0 END) AS deter_count
        FROM {CATALOG}.dev_gold.fct_defcon_actions
        WHERE action_player_id IS NOT NULL
        GROUP BY action_player_id, match_id, competition_id, season_id, data_source
    )
    SELECT
        dp.action_player_id AS player_id,
        p.player_name,
        dp.match_id,
        dp.competition_id,
        dp.season_id,
        dp.data_source,
        CONCAT(ms.home_team_name, ' ', ms.home_score, '-', ms.away_score, ' ', ms.away_team_name) AS match_label,
        dp.total_pressure,
        dp.total_defensive_actions,
        dp.intercept_pressure,
        dp.concede_pressure,
        dp.disturb_pressure,
        dp.deter_pressure,
        dp.intercept_count,
        dp.concede_count,
        dp.disturb_count,
        dp.deter_count
    FROM pressure_agg dp
    LEFT JOIN {CATALOG}.dev_gold.dim_players p
        ON dp.action_player_id = p.player_id
    LEFT JOIN {CATALOG}.dev_gold.fct_match_summary ms
        ON dp.match_id = ms.match_id
    ORDER BY dp.action_player_id, dp.match_id
""")

print(f"Pressure rows: {pressure_df.count():,}")

pressure_df.coalesce(1).write.mode("overwrite").parquet(f"{VOLUME_PATH}/defcon_pressure")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Download Instructions
# MAGIC
# MAGIC From local machine:
# MAGIC ```bash
# MAGIC # Tracking data
# MAGIC databricks fs cp "dbfs:/Volumes/soccer_analytics/bronze/libs/hf_export/demo/sample_tracking/" \
# MAGIC   demo_space/data/sample_tracking.parquet --recursive --profile OAUTH
# MAGIC
# MAGIC # DEFCON pressure
# MAGIC databricks fs cp "dbfs:/Volumes/soccer_analytics/bronze/libs/hf_export/demo/defcon_pressure/" \
# MAGIC   demo_space/data/defcon_pressure.parquet --recursive --profile OAUTH
# MAGIC ```
# MAGIC
# MAGIC Note: Spark writes a directory with `part-*.parquet` files. After download,
# MAGIC consolidate into a single file:
# MAGIC ```python
# MAGIC import pandas as pd
# MAGIC df = pd.read_parquet("demo_space/data/sample_tracking.parquet")
# MAGIC df.to_parquet("demo_space/data/sample_tracking.parquet", index=False)
# MAGIC ```
