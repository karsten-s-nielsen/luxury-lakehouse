# ruff: noqa: S608 — _CASES are module-level tuples, not user input.
"""PR 6 live invariants — every Kimball-keyed mart's surrogate FKs must
be populated above their per-(mart, key) threshold.

Renamed from test_marts_player_key_contracts.py and parameterized over
(mart, key_column, threshold) tuples covering:

  - PR 5b's six embedding marts (player_key) — threshold 0.99.
  - PR 6's five defcon/GK marts (player_key, team_key, match_key,
    action_player_key where present) — thresholds calibrated per
    Phase 0 Task 0.7 measurement on dev_gold.

Phase 0 Task 0.7 finding (2026-04-26): defender_player_id resolution
against dim_players is structurally low (~16% on stg_defcon__results)
because 360-synthetic defenders use synthetic IDs that don't appear in
dim_players. action_player_id (real attackers) resolves at 100%. The
floor for defender keys is therefore set conservatively at 0.10 to
catch real regressions while accommodating the 360-synthetic floor.

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
# Defender-on-defcon thresholds set to 0.10 per Phase 0 Task 0.7 (360-synthetic
# floor at ~0.16); raise/lower at first dev rebuild measurement if needed.
_CASES: tuple[tuple[str, str, float], ...] = (
    # PR 5b — player_key on six embedding marts
    ("fct_player_embeddings", "player_key", 0.99),
    ("fct_player_embeddings_season", "player_key", 0.99),
    ("fct_player_embeddings_career", "player_key", 0.99),
    ("fct_player_embeddings_season_360", "player_key", 0.99),
    ("fct_player_embeddings_career_360", "player_key", 0.99),
    ("fct_player_percentiles", "player_key", 0.99),
    # PR 6 — fct_defensive_values (defender keys; 360-synthetic floor ~16%)
    ("fct_defensive_values", "match_key", 0.99),
    ("fct_defensive_values", "team_key", 0.10),
    ("fct_defensive_values", "player_key", 0.10),
    # PR 6 — fct_defcon_actions (defender low-resolution; action_player full)
    ("fct_defcon_actions", "match_key", 0.99),
    ("fct_defcon_actions", "team_key", 0.10),
    ("fct_defcon_actions", "player_key", 0.10),
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
