# /// script
# requires-python = ">=3.10"
# dependencies = ["databricks-sql-connector>=3.0.0"]
# ///
"""Confirm GK redesign data volumes against live gold tables.
Q1 offensive: distributions per GK (xt_gk volume).
Q2 defensive: sweeper-family (pitch-control/reachable/closing) non-null counts vs shots-faced.
"""
import os, sys, io
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")
from databricks import sql

HOST = os.environ["DATABRICKS_HOST"].replace("https://", "")
HTTP_PATH = os.environ["DATABRICKS_HTTP_PATH"]
TOKEN = os.environ["DATABRICKS_TOKEN"]

conn = sql.connect(server_hostname=HOST, http_path=HTTP_PATH, access_token=TOKEN)

def q(label, sql_text):
    print(f"\n===== {label} =====")
    print(sql_text.strip())
    try:
        with conn.cursor() as c:
            c.execute(sql_text)
            cols = [d[0] for d in c.description]
            rows = c.fetchall()
            print(" | ".join(cols))
            for r in rows:
                print(" | ".join("" if v is None else str(v) for v in r))
    except Exception as e:
        print(f"!! ERROR: {e}")

# 0. locate the gold schema holding fct_action_context
q("LOCATE fct_action_context", """
SELECT table_catalog, table_schema
FROM system.information_schema.tables
WHERE table_name = 'fct_action_context'
ORDER BY table_catalog, table_schema
""")
# Resolve schema (prefer a *gold* schema) and run the volume queries.
schema = None
with conn.cursor() as c:
    c.execute("""SELECT table_catalog, table_schema FROM system.information_schema.tables
                 WHERE table_name='fct_action_context' ORDER BY (table_schema LIKE '%gold%') DESC""")
    rows = c.fetchall()
    if rows:
        schema = f"{rows[0][0]}.{rows[0][1]}"
print(f"\n>>> Using schema: {schema}")

if schema:
    G = schema
    Gs = G  # assume fct_gk_tracking_stats in same schema; verify
    q("OFFENSIVE — distributions per GK (per provider)", f"""
    WITH g AS (
      SELECT gk_player_key, data_source, SUM(n_distributions) AS nd
      FROM {Gs}.fct_gk_tracking_stats
      WHERE n_distributions IS NOT NULL
      GROUP BY gk_player_key, data_source
    )
    SELECT data_source,
           COUNT(*) AS gks,
           ROUND(AVG(nd),1) AS avg_dist,
           MAX(nd) AS max_dist,
           SUM(CASE WHEN nd>=50  THEN 1 ELSE 0 END) AS gks_ge50,
           SUM(CASE WHEN nd>=100 THEN 1 ELSE 0 END) AS gks_ge100
    FROM g GROUP BY data_source ORDER BY data_source
    """)
    q("DEFENSIVE — sweeper-family non-null vs shots, per GK (per provider)", f"""
    WITH d AS (
      SELECT defending_gk_player_key AS gk, data_source,
             COUNT(*) AS defended_actions,
             COUNT(gk_pitch_control_share_weighted) AS pc_nn,
             COUNT(gk_reachable_area_m2)            AS ra_nn,
             COUNT(gk_closing_time_min_s__six_yard_box) AS ct_nn,
             SUM(CASE WHEN pre_shot_gk_x IS NOT NULL THEN 1 ELSE 0 END) AS shots_faced
      FROM {G}.fct_action_context
      WHERE defending_gk_player_key IS NOT NULL
      GROUP BY defending_gk_player_key, data_source
    )
    SELECT data_source,
           COUNT(*) AS gks,
           ROUND(AVG(defended_actions),0) AS avg_def_actions,
           ROUND(AVG(pc_nn),0) AS avg_pc_nn,
           ROUND(AVG(ra_nn),0) AS avg_ra_nn,
           ROUND(AVG(ct_nn),0) AS avg_ct_nn,
           ROUND(AVG(shots_faced),1) AS avg_shots,
           MAX(shots_faced) AS max_shots,
           SUM(CASE WHEN pc_nn>=50 THEN 1 ELSE 0 END) AS gks_pc_ge50
    FROM d GROUP BY data_source ORDER BY data_source
    """)

conn.close()
print("\nDONE")
