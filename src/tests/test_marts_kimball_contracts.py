# ruff: noqa: S608 — _CASES are module-level tuples, not user input.
"""PR 6 live invariants — every Kimball-keyed mart's surrogate FKs must
be populated above their per-(mart, key) threshold.

Renamed from test_marts_player_key_contracts.py and parameterized over
(mart, key_column, threshold) tuples covering:

  - PR 5b's six embedding marts (player_key) — threshold 0.99.
  - PR 6's five defcon/GK marts (player_key, team_key, match_key,
    action_player_key where present) — thresholds calibrated per
    Phase 0 Task 0.7 measurement on dev_gold.

Phase 0 Task 0.7 originally observed defender_player_id resolution
at ~16% on stg_defcon__results, but that figure was an artifact of the
pre-2026-04-27 INT cast in `stg_defcon__results.sql` truncating the
synthetic LONG IDs from `monotonically_increasing_id()` into the real
player_id range by accident. After the BIGINT widening (PRs #208-#210
plus the defcon-cast-fix branch), the synthetic IDs land in the
9-10-digit range where they no longer overlap with real StatsBomb
player_ids — measured at 0.06% (469 / 828k) on the 2026-04-27 dev
rebuild. The defender_player_key tests for fct_defensive_values and
fct_defcon_actions are therefore removed: the column is structurally
unresolvable for 360-anonymous freeze-frame defenders, and the 16%
figure cannot be regressed-against because it was never semantic.

team_key on those marts moves from 0.10 floor to 0.99 — PR 6-followup
#209 derives team_key from the teammate flag + per-match team-pair, so
post-fix coverage is 100% (measured 828634/828634). action_player_key
remains 0.99 — real attacker IDs resolve fully via the dim_players
join.

Skips when DATABRICKS_* env vars are absent (air-gapped CI). Otherwise
runs against dev_gold via the standard SQL warehouse connection.

Mirrors the connection pattern used by other live tests
(test_int_player_xref_invariants.py, test_marts_live_schema.py): the
``pytest.importorskip("databricks.sql")`` guards against missing the
optional databricks-sql-connector dependency in air-gapped CI runs.
"""

from __future__ import annotations

import os

import pytest

databricks_sql = pytest.importorskip("databricks.sql")

requires_databricks = pytest.mark.skipif(
    not all(os.environ.get(v) for v in ("DATABRICKS_HOST", "DATABRICKS_HTTP_PATH", "DATABRICKS_TOKEN")),
    reason="Databricks SQL env vars not set",
)

# (mart, key_column, non_null_rate_threshold)
#
# Calibrated 2026-04-27 against post-defcon-cast-fix dev_gold:
#   - match_key, team_key, action_player_key: 100% on 828634 rows
#   - defender player_key on fct_defensive_values / fct_defcon_actions:
#     0.06% (structural — see module docstring). Not tested.
_CASES: tuple[tuple[str, str, float], ...] = (
    # PR 5b — player_key on six embedding marts
    ("fct_player_embeddings", "player_key", 0.99),
    ("fct_player_embeddings_season", "player_key", 0.99),
    ("fct_player_embeddings_career", "player_key", 0.99),
    ("fct_player_embeddings_season_360", "player_key", 0.99),
    ("fct_player_embeddings_career_360", "player_key", 0.99),
    ("fct_player_percentiles", "player_key", 0.99),
    # PR 6 — fct_defensive_values (real match + team; defender 360-synthetic
    # player_key intentionally unresolved, no test).
    ("fct_defensive_values", "match_key", 0.99),
    ("fct_defensive_values", "team_key", 0.99),
    # PR 6 — fct_defcon_actions (real match + team + action_player; defender
    # 360-synthetic player_key intentionally unresolved, no test).
    ("fct_defcon_actions", "match_key", 0.99),
    ("fct_defcon_actions", "team_key", 0.99),
    ("fct_defcon_actions", "action_player_key", 0.99),
    # PR 6 — fct_defcon_pressure (action_player only — real player IDs)
    ("fct_defcon_pressure", "match_key", 0.99),
    ("fct_defcon_pressure", "player_key", 0.99),
    # PR 6 — goalkeeper marts (real GK player IDs)
    ("fct_goalkeeper_stats", "match_key", 0.99),
    ("fct_goalkeeper_stats", "team_key", 0.99),
    ("fct_goalkeeper_stats", "player_key", 0.99),
    ("fct_gk_actions_detail", "match_key", 0.99),
    ("fct_gk_actions_detail", "team_key", 0.99),
    ("fct_gk_actions_detail", "player_key", 0.99),
    # PR 7 — conformed-fact closures (Step IX). Initial thresholds are 0.0
    # (smoke-test the column exists and the test runs); thresholds calibrated
    # post-Phase-2 rebuild per `feedback_evidence_before_claim` and committed
    # in the same PR's 2nd commit (or a follow-up if needed).
    ("fct_passes", "team_key", 0.0),
    ("fct_passes", "passer_player_key", 0.0),
    # recipient_player_key — NULL by design on incomplete passes; threshold
    # stays 0.0 even after calibration. Test asserts column existence only.
    ("fct_passes", "recipient_player_key", 0.0),
    ("fct_shots", "team_key", 0.0),
    ("fct_shots", "player_key", 0.0),
    ("fct_action_values", "team_key", 0.0),
    ("fct_action_values", "player_key", 0.0),
    ("fct_line_breaking_results", "team_key", 0.0),
    ("fct_line_breaking_results", "player_key", 0.0),
    # PR 7 — fct_match_summary cross-provider home/away resolution.
    ("fct_match_summary", "home_team_key", 0.0),
    ("fct_match_summary", "away_team_key", 0.0),
    # PR 7 — tracking subsystem (existing at checkpoint d4f2004 + new this PR).
    ("fct_tracking_frames", "match_key", 0.0),
    ("fct_tracking_frames", "team_key", 0.0),
    ("fct_tracking_frames", "player_key", 0.0),
    ("fct_player_positions", "match_key", 0.0),
    ("fct_player_positions", "team_key", 0.0),
    ("fct_player_positions", "player_key", 0.0),
    ("fct_position_maps", "match_key", 0.0),
    ("fct_position_maps", "team_key", 0.0),
    ("fct_position_maps", "player_key", 0.0),
    ("fct_formation_labels", "match_key", 0.0),
    ("fct_formation_labels", "team_key", 0.0),
    ("fct_physical_stats", "match_key", 0.0),
    ("fct_physical_stats", "player_key", 0.0),
    # PR 7 — off-ball / space / discipline.
    ("fct_off_ball_xt", "match_key", 0.0),
    ("fct_off_ball_xt", "player_key", 0.0),
    ("fct_discipline_events", "match_key", 0.0),
    ("fct_discipline_events", "team_key", 0.0),
    ("fct_discipline_events", "player_key", 0.0),
    # PR 7 — pausa subsystem (only when pausa_enabled at runtime; the live
    # query handles disabled-mart-empty-result via the threshold check).
    ("fct_pass_timing", "match_key", 0.0),
    ("fct_pass_timing", "player_key", 0.0),
    ("fct_pausa_rankings", "player_key", 0.0),
)


@pytest.fixture(scope="module")
def conn():
    c = databricks_sql.connect(
        server_hostname=os.environ["DATABRICKS_HOST"].replace("https://", "").rstrip("/"),
        http_path=os.environ["DATABRICKS_HTTP_PATH"],
        access_token=os.environ["DATABRICKS_TOKEN"],
    )
    yield c
    c.close()


@requires_databricks
@pytest.mark.parametrize(("mart", "key_column", "threshold"), _CASES)
def test_kimball_key_populated(conn, mart: str, key_column: str, threshold: float) -> None:
    """Each (mart, key) pair must have non-NULL rate >= threshold on dev_gold."""
    catalog = "soccer_analytics"
    schema = "dev_gold"
    table = f"{catalog}.{schema}.{mart}"

    cur = conn.cursor()
    cur.execute(f"SELECT count(*) AS total, count({key_column}) AS non_null FROM {table}")
    row = cur.fetchall()[0]
    assert row is not None, f"empty result on {table}"
    total = int(row[0])
    non_null = int(row[1])

    if total == 0:
        # Mart unbuilt (e.g., feature gate off) — skip rather than fail.
        pytest.skip(f"{mart} has zero rows — feature gate may be off")

    rate = non_null / total
    assert rate >= threshold, (
        f"{mart}.{key_column}: non-NULL rate {rate:.4f} below {threshold} threshold "
        f"(total={total}, non_null={non_null}). Investigate dim resolution."
    )
