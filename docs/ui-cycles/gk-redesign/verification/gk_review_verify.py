# /// script
# requires-python = ">=3.10"
# dependencies = ["databricks-sql-connector>=3.0.0"]
# ///
"""Verify the implementing session's review claims live (B1 sign, C2 merge/provider, C3 variance)."""
import os, sys, io
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")
from databricks import sql
conn = sql.connect(server_hostname=os.environ["DATABRICKS_HOST"].replace("https://",""),
                   http_path=os.environ["DATABRICKS_HTTP_PATH"], access_token=os.environ["DATABRICKS_TOKEN"])
G = "soccer_analytics.dev_gold"
def q(label, s):
    print(f"\n===== {label} =====")
    try:
        with conn.cursor() as c:
            c.execute(s); cols=[d[0] for d in c.description]; rows=c.fetchall()
            print(" | ".join(cols))
            for r in rows: print(" | ".join("" if v is None else str(v) for v in r))
    except Exception as e:
        print(f"!! ERROR: {e}")

# B1 — xT-GK composite + component sign distribution (offensive domain)
q("B1 xT-GK + components sign (fct_action_context, xt_gk IS NOT NULL)", f"""
SELECT COUNT(*) n,
  ROUND(AVG(xt_gk),4) avg_xtgk, ROUND(MIN(xt_gk),4) min_xtgk, ROUND(MAX(xt_gk),4) max_xtgk,
  ROUND(AVG(CASE WHEN xt_gk<0 THEN 1.0 ELSE 0 END),3) frac_neg_xtgk,
  ROUND(AVG(xt_gk_pev),4) avg_pev, ROUND(AVG(CASE WHEN xt_gk_pev>0 THEN 1.0 ELSE 0 END),3) pev_pos,
  ROUND(AVG(xt_gk_dzv),4) avg_dzv, ROUND(AVG(CASE WHEN xt_gk_dzv>0 THEN 1.0 ELSE 0 END),3) dzv_pos,
  ROUND(AVG(xt_gk_rav),4) avg_rav, ROUND(AVG(CASE WHEN xt_gk_rav>0 THEN 1.0 ELSE 0 END),3) rav_pos,
  ROUND(AVG(xt_gk_base),4) avg_base, ROUND(AVG(gk_completion),3) avg_completion
FROM {G}.fct_action_context WHERE xt_gk IS NOT NULL
""")

# B1 at the DISPLAY grain — per-GK dist_xt_gk_mean by provider
q("B1 per-GK dist_xt_gk_mean by provider (fct_gk_tracking_stats)", f"""
WITH g AS (SELECT gk_player_key, data_source, AVG(dist_xt_gk_mean) m, AVG(dist_xt_gk_counter_mean) mc
           FROM {G}.fct_gk_tracking_stats WHERE dist_xt_gk_mean IS NOT NULL GROUP BY gk_player_key, data_source)
SELECT data_source, COUNT(*) gks, ROUND(AVG(m),4) avg_default, ROUND(MIN(m),4) min_d, ROUND(MAX(m),4) max_d,
  ROUND(AVG(CASE WHEN m<0 THEN 1.0 ELSE 0 END),3) frac_neg, ROUND(AVG(mc),4) avg_counter
FROM g GROUP BY data_source ORDER BY data_source
""")

# C2a — does any canonical_player_key span >1 tracking data_source?
q("C2a dim_players key columns", """
SELECT column_name FROM system.information_schema.columns
WHERE table_schema='dev_gold' AND table_name='dim_players'
  AND (column_name LIKE '%canonical%' OR column_name LIKE '%player_key%') ORDER BY column_name
""")
q("C2a canonical spanning >1 tracking provider", f"""
WITH gk AS (
  SELECT DISTINCT dp.canonical_player_key AS ck, s.data_source
  FROM {G}.fct_goalkeeper_stats s JOIN {G}.dim_players dp ON dp.player_key = s.player_key
  WHERE s.data_source IN ('gradientsports','skillcorner','idsse')
)
SELECT COUNT(*) total_canonical,
       SUM(CASE WHEN nds>1 THEN 1 ELSE 0 END) span_multi_tracking
FROM (SELECT ck, COUNT(DISTINCT data_source) nds FROM gk GROUP BY ck)
""")

# C3 — variance term in pooled / shot_stopping marts?
q("C3 fct_gk_shot_stopping_pooled columns", """
SELECT column_name FROM system.information_schema.columns
WHERE table_schema='dev_gold' AND table_name='fct_gk_shot_stopping_pooled' ORDER BY ordinal_position
""")
q("C3 fct_gk_shot_stopping columns", """
SELECT column_name FROM system.information_schema.columns
WHERE table_schema='dev_gold' AND table_name='fct_gk_shot_stopping' ORDER BY ordinal_position
""")

# C2b — per-provider sweeper means (systematic provider effect?)
q("C2b per-provider sweeper means (fct_gk_tracking_stats)", f"""
SELECT data_source, COUNT(*) gkm,
  ROUND(AVG(reachable_area_mean_m2),1) avg_reach, ROUND(AVG(pc_share_mean),4) avg_pc,
  ROUND(AVG(closing_min_six_yard_mean_s),2) avg_close, ROUND(AVG(n_defended_actions),0) avg_def
FROM {G}.fct_gk_tracking_stats WHERE n_defended_actions IS NOT NULL GROUP BY data_source ORDER BY data_source
""")
conn.close(); print("\nDONE")
