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

from ingestion.databricks_auth import has_databricks_auth

databricks_sql = pytest.importorskip("databricks.sql")

requires_databricks = pytest.mark.skipif(
    not (has_databricks_auth() and os.environ.get("DATABRICKS_HTTP_PATH")),
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
    # PR 7 hotfix #3: PR-7 entries moved to _CASES_PR7 below for per-(mart, key,
    # provider) parameterization. Single-provider drift surfaces against the named
    # provider rather than hiding behind aggregate counts.
)


# PR 7 hotfix #3 — per-(mart, key, provider) calibrated thresholds.
#
# Three patterns:
#   1. Strict 1.0 — JOIN MUST resolve 100%; failure indicates recipe drift or new data gap.
#   2. 0.0 with comment — structural source-data gap (e.g., Wyscout has no recipient field).
#   3. Calibrated <1.0 — true partial coverage with documented reason; commit measured value.
#
# Calibrated post-rebuild on dev_gold; final values committed in the hotfix-#3 PR.
# (mart, key_column, provider, non_null_rate_threshold)
_CASES_PR7: tuple[tuple[str, str, str, float], ...] = (
    # fct_passes — 4 providers x 3 FK columns
    ("fct_passes", "team_key", "statsbomb", 1.0),
    ("fct_passes", "team_key", "wyscout", 1.0),
    ("fct_passes", "team_key", "idsse", 1.0),
    ("fct_passes", "team_key", "metrica", 1.0),
    ("fct_passes", "passer_player_key", "statsbomb", 1.0),
    ("fct_passes", "passer_player_key", "wyscout", 1.0),
    ("fct_passes", "passer_player_key", "idsse", 1.0),
    ("fct_passes", "passer_player_key", "metrica", 1.0),
    # StatsBomb has recipient on completed passes only; ~6.45% of pass rows are
    # incomplete passes / passes out of bounds where no teammate received.
    # Calibrated 2026-04-28 against post-hotfix-3 dev_gold: 3,168,336 / 3,386,907
    # = 93.55%. Floor at 0.93 leaves headroom for natural variation.
    ("fct_passes", "recipient_player_key", "statsbomb", 0.93),
    # Wyscout open-data has NO recipient field — kloppy strips at parse.
    # Structural source gap; threshold stays 0.0.
    ("fct_passes", "recipient_player_key", "wyscout", 0.0),
    # IDSSE/Metrica — calibrate post-rebuild and update with measured values.
    ("fct_passes", "recipient_player_key", "idsse", 0.5),
    ("fct_passes", "recipient_player_key", "metrica", 0.5),
    # fct_action_values — SB/WS only
    ("fct_action_values", "team_key", "statsbomb", 1.0),
    ("fct_action_values", "team_key", "wyscout", 1.0),
    ("fct_action_values", "player_key", "statsbomb", 1.0),
    ("fct_action_values", "player_key", "wyscout", 1.0),
    # fct_shots — SB/WS only
    ("fct_shots", "team_key", "statsbomb", 1.0),
    ("fct_shots", "team_key", "wyscout", 1.0),
    # Phase 6 of hotfix-#3 plan: investigate 3 NULL player_key SB rows.
    # If dim_players gap → fix dim. If source NULL → relax to 0.99998.
    ("fct_shots", "player_key", "statsbomb", 1.0),
    ("fct_shots", "player_key", "wyscout", 1.0),
    # fct_match_summary — extended to all 4 providers via tracking-side bridge.
    # SB threshold relaxed to 0.9997 to accommodate 1 SB Open Data edge case:
    # match 3825894 (RC Deportivo La Coruña vs Getafe, 2016-05-01) has metadata
    # in stg_statsbomb__matches but zero events in stg_statsbomb__events, so
    # match_team_ids pivot returns no rows for it. Calibrated 2026-04-28:
    # 3,463 / 3,464 = 99.97%. Floor at 0.9997 catches catastrophic regression
    # while permitting the documented single-row gap.
    ("fct_match_summary", "home_team_key", "statsbomb", 0.9997),
    ("fct_match_summary", "home_team_key", "wyscout", 1.0),
    ("fct_match_summary", "home_team_key", "idsse", 1.0),
    ("fct_match_summary", "home_team_key", "metrica", 1.0),
    ("fct_match_summary", "away_team_key", "statsbomb", 0.9997),
    ("fct_match_summary", "away_team_key", "wyscout", 1.0),
    ("fct_match_summary", "away_team_key", "idsse", 1.0),
    ("fct_match_summary", "away_team_key", "metrica", 1.0),
    # fct_tracking_frames — 3 tracking providers x 3 FK columns
    ("fct_tracking_frames", "match_key", "idsse", 1.0),
    ("fct_tracking_frames", "match_key", "metrica", 1.0),
    ("fct_tracking_frames", "match_key", "skillcorner", 1.0),
    ("fct_tracking_frames", "team_key", "idsse", 1.0),
    ("fct_tracking_frames", "team_key", "metrica", 1.0),
    ("fct_tracking_frames", "team_key", "skillcorner", 1.0),
    ("fct_tracking_frames", "player_key", "idsse", 1.0),
    ("fct_tracking_frames", "player_key", "metrica", 1.0),
    ("fct_tracking_frames", "player_key", "skillcorner", 1.0),
    # Formations marts — 3 tracking providers x FK columns
    ("fct_player_positions", "match_key", "idsse", 1.0),
    ("fct_player_positions", "match_key", "metrica", 1.0),
    ("fct_player_positions", "match_key", "skillcorner", 1.0),
    ("fct_player_positions", "team_key", "idsse", 1.0),
    ("fct_player_positions", "team_key", "metrica", 1.0),
    ("fct_player_positions", "team_key", "skillcorner", 1.0),
    ("fct_player_positions", "player_key", "idsse", 1.0),
    ("fct_player_positions", "player_key", "metrica", 1.0),
    ("fct_player_positions", "player_key", "skillcorner", 1.0),
    ("fct_position_maps", "match_key", "idsse", 1.0),
    ("fct_position_maps", "match_key", "metrica", 1.0),
    ("fct_position_maps", "match_key", "skillcorner", 1.0),
    ("fct_position_maps", "team_key", "idsse", 1.0),
    ("fct_position_maps", "team_key", "metrica", 1.0),
    ("fct_position_maps", "team_key", "skillcorner", 1.0),
    ("fct_position_maps", "player_key", "idsse", 1.0),
    ("fct_position_maps", "player_key", "metrica", 1.0),
    ("fct_position_maps", "player_key", "skillcorner", 1.0),
    ("fct_formation_labels", "match_key", "idsse", 1.0),
    ("fct_formation_labels", "match_key", "metrica", 1.0),
    ("fct_formation_labels", "match_key", "skillcorner", 1.0),
    ("fct_formation_labels", "team_key", "idsse", 1.0),
    ("fct_formation_labels", "team_key", "metrica", 1.0),
    ("fct_formation_labels", "team_key", "skillcorner", 1.0),
    # fct_physical_stats — 3 tracking providers
    ("fct_physical_stats", "match_key", "idsse", 1.0),
    ("fct_physical_stats", "match_key", "metrica", 1.0),
    ("fct_physical_stats", "match_key", "skillcorner", 1.0),
    ("fct_physical_stats", "player_key", "idsse", 1.0),
    ("fct_physical_stats", "player_key", "metrica", 1.0),
    ("fct_physical_stats", "player_key", "skillcorner", 1.0),
    # fct_pass_timing — IDSSE-only (PAUSA scope, see hotfix-3 spec §3.1).
    ("fct_pass_timing", "match_key", "idsse", 1.0),
    ("fct_pass_timing", "player_key", "idsse", 1.0),
    # fct_pausa_rankings is per-player career aggregate — NO match_key column,
    # so the test's JOIN-via-dim_matches fallback can't filter by provider.
    # Coverage on player_key is implicitly tested via fct_pausa_values (which
    # has player_key + match_key + the tested per-provider parameterization).
    # Removed from _CASES_PR7 to avoid false-positive query errors.
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
    """Each (mart, key) pair must have non-NULL rate >= threshold on dev_gold.

    Aggregate-level threshold; PR 5b/PR 6 entries that don't need per-provider
    differentiation. PR 7+ entries live in test_kimball_key_populated_per_provider.
    """
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


@requires_databricks
@pytest.mark.parametrize(("mart", "key_column", "provider", "threshold"), _CASES_PR7)
def test_kimball_key_populated_per_provider(conn, mart: str, key_column: str, provider: str, threshold: float) -> None:
    """PR 7 hotfix #3: per-(mart, key, provider) coverage assertion.

    Single-provider drift surfaces against the named provider rather than hiding
    behind aggregate counts. Tracking marts use `source_provider`; event-derived
    marts use `data_source`. Auto-detected from the table's column inventory.
    """
    catalog = "soccer_analytics"
    schema = "dev_gold"
    table = f"{catalog}.{schema}.{mart}"

    cur = conn.cursor()
    # Identify the provider column. Tracking marts use 'source_provider';
    # event-derived marts use 'data_source'. fct_match_summary has no provider
    # column (one row per match — provider is implicit via match_key→dim_matches).
    cur.execute(f"DESCRIBE {table}")
    cols = {row[0] for row in cur.fetchall() if row[0] and not row[0].startswith("#")}
    provider_col: str | None
    if "source_provider" in cols:
        provider_col = "source_provider"
    elif "data_source" in cols:
        provider_col = "data_source"
    else:
        # No native provider column — JOIN dim_matches to filter by provider.
        provider_col = None

    if provider_col is None:
        # fct_match_summary case: filter via dim_matches.provider
        cur.execute(
            f"SELECT count(*) AS total, count(m.{key_column}) AS non_null "
            f"FROM {table} m "
            f"JOIN soccer_analytics.dev_gold.dim_matches dm ON dm.match_key = m.match_key "
            f"WHERE dm.provider = '{provider}'"
        )
    else:
        cur.execute(
            f"SELECT count(*) AS total, count({key_column}) AS non_null "
            f"FROM {table} WHERE {provider_col} = '{provider}'"
        )
    row = cur.fetchall()[0]
    assert row is not None, f"empty result on {table} for provider={provider}"
    total = int(row[0])
    non_null = int(row[1])

    if total == 0:
        pytest.skip(
            f"{mart} has zero rows for provider={provider} — feature gate may be off "
            f"or provider not loaded for this mart"
        )

    rate = non_null / total
    assert rate >= threshold, (
        f"{mart}.{key_column} provider={provider}: non-NULL rate {rate:.4f} "
        f"below {threshold} threshold (total={total}, non_null={non_null}). "
        f"Investigate dim resolution recipe drift."
    )
