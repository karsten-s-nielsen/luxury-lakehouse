"""Live-schema drift guard for the pre-shot freeze-frame builder + driver (2026-07-07).

The builder (`analytics.action_context.tracking_snapshots`) and the driver
(`ingestion.shot_freeze_frames`) read specific columns from EVERY input table:
`bronze.spadl_actions`, `dev_gold.dim_matches`, and the four tracking bronze tables. If a column
they rely on is renamed/removed upstream, the read silently degrades (all-NA/all-zero) or dies at
runtime with `UNRESOLVED_COLUMN` — NOT a unit-test failure. Two real incidents motivated this:
  * 2026-07-07a: builder read `type_name`/`team_attacking_direction` (absent) → all-NA/all-zero.
  * 2026-07-07b: driver joined `dim_matches.data_source`/`match_id_native` (absent — the real names
    are `provider`/`native_match_id`) → `UNRESOLVED_COLUMN` at runtime; the mocked SQL-string test
    passed because the guard only knew `spadl_actions`.
This test pins the required input columns of EVERY driver input against the authoritative schema
(the bronze DDL constant, the tracking `*_SELECT_COLS` constants, and the verified live
`dim_matches` column allowlist) so a rename on ANY input fails a unit test, not a live job.

Note: `match_key` is DELIBERATELY absent from the spadl required set — it is the Kimball surrogate
the driver resolves from gold `dim_matches` (ADR-013: bronze carries only native ids).
"""

from __future__ import annotations

from ingestion.spadl_vaep import _SPADL_SCHEMA

# Columns the freeze-frame builder + driver read directly from bronze.spadl_actions.
# (team_id is remapped to team_id_native by _resolve_enrichment_identity; both are required inputs.)
_REQUIRED_SPADL_INPUT_COLUMNS: frozenset[str] = frozenset(
    {
        "action_id",
        "period_id",
        "time_seconds",
        "game_id",
        "type_id",  # shot filter (NOT type_name)
        "team_id",
        "team_id_native",  # frame-compatible remap source
        "home_team_id_native",
        "player_id",
        "player_id_native",
        "data_source",
        "match_id_native",
    }
)

# Columns the builder/driver must NEVER rely on — they do not exist on bronze.spadl_actions and
# reading them is exactly the silent-degradation class this guard exists to catch.
_FORBIDDEN_INPUT_COLUMNS: frozenset[str] = frozenset({"type_name", "team_attacking_direction", "match_key"})


def _bronze_spadl_columns() -> frozenset[str]:
    """Parse the column names out of the authoritative `_SPADL_SCHEMA` DDL string."""
    cols: set[str] = set()
    for token in _SPADL_SCHEMA.split(","):
        token = token.strip()
        if not token:
            continue
        cols.add(token.split()[0])
    return frozenset(cols)


def test_required_input_columns_exist_in_bronze_spadl_actions() -> None:
    bronze = _bronze_spadl_columns()
    missing = _REQUIRED_SPADL_INPUT_COLUMNS - bronze
    assert not missing, (
        f"Freeze-frame builder/driver read column(s) absent from bronze.spadl_actions: {sorted(missing)}. "
        "Either the DDL drifted or a builder/driver read a wrong column name (silent all-NA/all-zero risk)."
    )


def test_forbidden_columns_are_not_in_bronze_schema() -> None:
    # Encodes WHY these are forbidden: they are NOT bronze columns, so any code reading them degrades
    # silently. If one of these ever legitimately lands in the DDL, revisit the builder contract.
    bronze = _bronze_spadl_columns()
    present = _FORBIDDEN_INPUT_COLUMNS & bronze
    assert not present, f"Unexpectedly found forbidden column(s) in bronze.spadl_actions DDL: {sorted(present)}"


def test_match_key_is_not_a_bronze_column() -> None:
    # ADR-013: match_key is a gold surrogate the driver resolves from dim_matches, not bronze.
    assert "match_key" not in _bronze_spadl_columns()


# ── dev_gold.dim_matches identity contract ─────────────────────────────────
# Verified live 2026-07-07: dim_matches' actual columns (there is no Python schema constant for it,
# so this allowlist IS the verified live contract — update it if the mart schema changes).
_DIM_MATCHES_COLUMNS: frozenset[str] = frozenset(
    {
        "match_key",
        "competition_key",
        "provider",
        "native_match_id",
        "match_date",
        "home_team_id_native",
        "away_team_id_native",
        "access_tier",  # ADR-064 per-match tier the driver resolves alongside match_key + stamps per row
    }
)


def test_driver_dim_matches_join_uses_real_columns() -> None:
    # The driver's SSOT constants must name columns that ACTUALLY exist on dim_matches — including the
    # ADR-064 access_tier the driver resolves in the SAME read as match_key (a rename would silently
    # degrade the per-row stamp to NULL → over-restriction downstream, exactly this guard's remit).
    from ingestion.shot_freeze_frames import (
        _DIM_MATCHES_ACCESS_TIER_COL,
        _DIM_MATCHES_KEY_COL,
        _DIM_MATCHES_NATIVE_ID_COL,
        _DIM_MATCHES_PROVIDER_COL,
    )

    for col in (
        _DIM_MATCHES_PROVIDER_COL,
        _DIM_MATCHES_NATIVE_ID_COL,
        _DIM_MATCHES_KEY_COL,
        _DIM_MATCHES_ACCESS_TIER_COL,
    ):
        assert col in _DIM_MATCHES_COLUMNS, f"dim_matches has no column {col!r}"


def test_dim_matches_does_not_have_spadl_side_names() -> None:
    # The exact drift that bit: data_source / match_id_native are the bronze.spadl_actions names,
    # NOT dim_matches columns. If they ever appear here, the join contract changed — revisit.
    assert "data_source" not in _DIM_MATCHES_COLUMNS
    assert "match_id_native" not in _DIM_MATCHES_COLUMNS


def test_driver_spadl_join_constants_exist_in_bronze() -> None:
    from ingestion.shot_freeze_frames import _SPADL_DATA_SOURCE_COL, _SPADL_NATIVE_ID_COL, _SPADL_TYPE_ID_COL

    bronze = _bronze_spadl_columns()
    for col in (_SPADL_DATA_SOURCE_COL, _SPADL_NATIVE_ID_COL, _SPADL_TYPE_ID_COL):
        assert col in bronze, f"bronze.spadl_actions has no column {col!r}"


def test_discovery_sql_references_only_existing_columns() -> None:
    # End-to-end string check: the generated SQL must join on the real dim_matches names and never
    # reference the non-existent dm.data_source / dm.match_id_native (the runtime UNRESOLVED_COLUMN).
    from ingestion.shot_freeze_frames import _missing_units_sql

    sql = _missing_units_sql("soccer_analytics", "dev_gold", frozenset({"gradientsports", "skillcorner"}))
    assert "dm.provider" in sql
    assert "dm.native_match_id" in sql
    assert "dm.match_key" in sql
    assert "dm.data_source" not in sql
    assert "dm.match_id_native" not in sql
    assert "type_name" not in sql  # shot filter is type_id, not the absent type_name


# ── tracking bronze source tables (frame/id columns the driver + converter need) ─────────
# The driver reads each tracking bronze via the provider's *_SELECT_COLS projection, then rebases
# the clock + groups by period. These are the columns it MUST have; asserting them ⊆ the SELECT_COLS
# constants ties the guard to the real projection (a dropped column fails here, not on a live job).
_TRACKING_REQUIRED_COLUMNS: dict[str, frozenset[str]] = {
    "idsse": frozenset({"match_id", "period", "frame"}),
    "metrica": frozenset({"match_id", "period", "frame", "frame_rate"}),
    "skillcorner": frozenset({"match_id", "period", "frame", "timestamp"}),
    "gradientsports": frozenset({"match_id", "period", "frame_num", "period_elapsed_time"}),
}


def test_tracking_select_cols_cover_driver_requirements() -> None:
    from ingestion.action_context import _GRADIENTSPORTS_TRACKING_SELECT_COLS
    from ingestion.tracking_context import (
        _IDSSE_TRACKING_SELECT_COLS,
        _METRICA_TRACKING_SELECT_COLS,
        _SKILLCORNER_TRACKING_SELECT_COLS,
    )

    select_cols = {
        "idsse": set(_IDSSE_TRACKING_SELECT_COLS),
        "metrica": set(_METRICA_TRACKING_SELECT_COLS),
        "skillcorner": set(_SKILLCORNER_TRACKING_SELECT_COLS),
        "gradientsports": set(_GRADIENTSPORTS_TRACKING_SELECT_COLS),
    }
    for provider, required in _TRACKING_REQUIRED_COLUMNS.items():
        missing = required - select_cols[provider]
        assert not missing, f"{provider} tracking projection is missing driver-required column(s): {sorted(missing)}"
