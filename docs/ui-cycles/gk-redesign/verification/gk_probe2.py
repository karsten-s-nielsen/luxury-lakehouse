# /// script
# requires-python = ">=3.10"
# dependencies = ["databricks-sql-connector>=3.0.0"]
# ///
import os,sys,io
sys.stdout=io.TextIOWrapper(sys.stdout.buffer,encoding="utf-8",errors="replace")
from databricks import sql
conn=sql.connect(server_hostname=os.environ["DATABRICKS_HOST"].replace("https://",""),http_path=os.environ["DATABRICKS_HTTP_PATH"],access_token=os.environ["DATABRICKS_TOKEN"])
G="soccer_analytics.dev_gold"
def q(l,s):
    print(f"\n== {l} ==")
    try:
        with conn.cursor() as c:
            c.execute(s);cols=[d[0] for d in c.description];rows=c.fetchall()
            print(" | ".join(cols))
            for r in rows: print(" | ".join("" if v is None else str(v) for v in r))
    except Exception as e: print("ERR",e)
q("fct_goalkeeper_stats by data_source (+gp non-null)", f"SELECT data_source, COUNT(*) n, COUNT(goals_prevented) gp_nn FROM {G}.fct_goalkeeper_stats GROUP BY data_source ORDER BY data_source")
q("fct_goalkeeper_stats key cols", "SELECT column_name FROM system.information_schema.columns WHERE table_schema='dev_gold' AND table_name='fct_goalkeeper_stats' AND (column_name LIKE '%player_key%' OR column_name LIKE '%canonical%') ORDER BY column_name")
q("fct_gk_shot_stopping by data_source", f"SELECT data_source, COUNT(*) n, COUNT(DISTINCT player_key) gks FROM {G}.fct_gk_shot_stopping GROUP BY data_source ORDER BY data_source")
conn.close();print("\nDONE")
