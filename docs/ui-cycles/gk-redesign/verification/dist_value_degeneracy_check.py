"""Reproduces the 2026-06-22 finding that the offensive 6-preset xT-GK "fit-ladder"
is statistically degenerate, and that a 2-axis distribution profile is supportable.

Run:  python dist_value_degeneracy_check.py
Auth: ~/.databrickscfg [DEFAULT] PAT, via SDK Statement Execution API.
      (The databricks-sql-connector Thrift path rejected this PAT; the SDK path works.)
Deps: databricks-sdk, numpy, pandas, scipy.
"""
from __future__ import annotations
import io, sys
import numpy as np, pandas as pd
from scipy import stats  # noqa: F401  (kept for interactive follow-ups)
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.sql import StatementState

sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")

WAREHOUSE = "6c3b36ca64d183fe"          # soccer-analytics-warehouse-dev
WC = 4074842662800745980                # FIFA Men's World Cup competition_key
_w = WorkspaceClient(profile="DEFAULT")


def q(sql: str) -> pd.DataFrame:
    r = _w.statement_execution.execute_statement(
        warehouse_id=WAREHOUSE, statement=sql,
        catalog="soccer_analytics", schema="dev_gold", wait_timeout="50s",
    )
    if r.status.state != StatementState.SUCCEEDED:
        raise RuntimeError(r.status.error.message if r.status.error else r.status.state)
    cols = [c.name for c in r.manifest.schema.columns]
    data = r.result.data_array or []
    df = pd.DataFrame(data, columns=cols)
    return df.apply(pd.to_numeric, errors="ignore")


# --- Stats-grain cohort: the 6 preset means + completion/pressure/volume per keeper ---
stats_df = q(f"""
SELECT s.gk_player_key, p.player_display_name AS name,
  SUM(COALESCE(s.n_distributions,0)) AS n_dist,
  SUM(s.dist_xt_gk_counter_mean   *s.n_distributions)/NULLIF(SUM(s.n_distributions),0) AS m_counter,
  SUM(s.dist_xt_gk_direct_mean    *s.n_distributions)/NULLIF(SUM(s.n_distributions),0) AS m_direct,
  SUM(s.dist_xt_gk_mean           *s.n_distributions)/NULLIF(SUM(s.n_distributions),0) AS m_default,
  SUM(s.dist_xt_gk_high_press_mean*s.n_distributions)/NULLIF(SUM(s.n_distributions),0) AS m_highpress,
  SUM(s.dist_xt_gk_possession_mean*s.n_distributions)/NULLIF(SUM(s.n_distributions),0) AS m_poss,
  SUM(s.dist_xt_gk_low_block_mean *s.n_distributions)/NULLIF(SUM(s.n_distributions),0) AS m_lowblock
FROM fct_gk_tracking_stats s
JOIN dim_matches dm ON dm.match_key=s.match_key
JOIN dim_players p ON p.player_key=s.gk_player_key
WHERE dm.competition_key={WC} AND s.n_distributions IS NOT NULL
GROUP BY s.gk_player_key, p.player_display_name""")
for c in stats_df.columns[2:]:
    stats_df[c] = pd.to_numeric(stats_df[c], errors="coerce")

presets = ["m_counter", "m_direct", "m_default", "m_highpress", "m_poss", "m_lowblock"]
M = stats_df[presets].to_numpy()
d = stats_df.m_default.dropna()
print(f"N keepers={len(stats_df)}  xT-GK pct_negative={(d<0).mean()*100:.0f}%  mean={d.mean():.4f}")

between = stats_df[presets].mean(axis=1).std(ddof=1)
within = np.median(M.max(axis=1) - M.min(axis=1))
print(f"[1] between-keeper level SD={between:.4f}  within-keeper preset spread(med)={within:.4f}  ratio={between/within:.1f}x")

ranks = pd.DataFrame(M, columns=presets).rank()
off = ranks.corr("spearman").to_numpy()[np.triu_indices(6, 1)]
print(f"[2] keeper-ranking Spearman across preset pairs: min={off.min():.3f} mean={off.mean():.3f}")
best = pd.Series(np.array(presets)[M.argmax(1)]).value_counts().to_dict()
print(f"[3] best-fit across cohort: {best}")

delta = M - np.median(M, axis=0)
resid = delta - delta.mean(1, keepdims=True)
print(f"[4] suggested vs-cohort-delta ladder: level SD={delta.mean(1).std(ddof=1):.4f}  residual SHAPE SD={resid.std(ddof=1):.4f} (=noise)")

# --- Action-grain: the supportable 2-axis profile signals (n>=20) ---
act = q(f"""
SELECT p.player_display_name AS name, COUNT(*) AS n,
  AVG(a.xt_gk) AS value, AVG(CASE WHEN a.xt_gk>0 THEN 1.0 ELSE 0.0 END) AS adds_threat,
  AVG(a.gk_completion) AS completion, AVG(a.end_x-a.start_x) AS progress_m,
  AVG(a.xt_gk_dzv) AS dzv, AVG(a.xt_gk_pev) AS pev
FROM fct_gk_tracking_actions a
JOIN dim_matches dm ON dm.match_key=a.match_key
JOIN dim_players p ON p.player_key=a.player_key
WHERE dm.competition_key={WC} AND a.xt_gk IS NOT NULL
GROUP BY p.player_display_name HAVING COUNT(*)>=20""")
for c in ["value", "adds_threat", "completion", "progress_m", "dzv", "pev"]:
    act[c] = pd.to_numeric(act[c], errors="coerce")
print(f"\n[5] action-grain n>=20 keepers={len(act)}")
for c in ["value", "adds_threat", "completion", "progress_m", "dzv", "pev"]:
    v = act[c]
    print(f"    {c:12s} mean={v.mean():+.4f} sd={v.std():.4f} range=[{v.min():+.4f},{v.max():+.4f}]")
print("[6] correlations:\n" + act[["value", "adds_threat", "completion", "progress_m", "dzv"]].corr().round(2).to_string())
