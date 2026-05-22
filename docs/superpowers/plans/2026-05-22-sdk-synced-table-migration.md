# SDK Synced Table Migration Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Migrate all 41 Lakebase synced tables from UI-created / Terraform-imported to SDK-managed via `w.postgres.create_synced_table()`, remove the Terraform synced_tables module, and promote 9 additional fact tables from SNAPSHOT to TRIGGERED (CDF-based) scheduling.

**Architecture:** Replace the `SYNCED_TABLES: list[tuple[str, str | None]]` constant with a `SyncedTableConfig` frozen dataclass carrying name, source table, primary key columns, scheduling policy, and optional schema override. Switch all consumers from raw REST calls to `/api/2.0/database/synced_tables/` to the Databricks SDK `w.postgres.*` methods. A one-shot migration script deletes all 41 tables, enables CDF on TRIGGERED sources, recreates all 41 via SDK, and runs the full maintenance pipeline.

**Tech Stack:** Python 3.10, databricks-sdk 0.110.0 (`PostgresAPI`), psycopg2, pytest, Terraform 1.9+

**Spec:** `docs/superpowers/specs/2026-05-21-sdk-synced-table-migration-design.md`

**Note on TRIGGERED counts:** The spec header says "15 TRIGGERED" but the detailed table lists 12 rows (3 existing + 9 new promotions). The detailed table is authoritative: **12 TRIGGERED + 29 SNAPSHOT = 41 total**.

---

## File Structure

| File | Action | Responsibility |
|------|--------|---------------|
| `src/ingestion/refresh_synced_tables.py` | Modify | `SyncedTableConfig` dataclass, 41 configs with PK + policy, postgres SDK migration for `_get_pipeline_id` + `wait_until_online` + `_derive_upstream_tables` |
| `src/tests/test_refresh_synced_tables.py` | Modify | Tests for `SyncedTableConfig`, scheduling policy distribution, PK invariants, updated mocks |
| `scripts/migrate_synced_tables.py` | Create | One-shot migration: Phase 0 smoke test, Phase 1 delete, Phase 2 CDF, Phase 3 create, Phase 4 wait+maintain |
| `scripts/delete_synced_table.py` | Modify | Switch `w.database.delete_synced_database_table()` to `w.postgres.delete_synced_table()` |
| `scripts/fix_event_log_ownership.py` | Modify | Switch `_get_pipeline_id` from raw REST to SDK `w.postgres.get_synced_table()` |
| `scripts/grant_synced_table_permissions.py` | Modify | Switch `_enumerate_pipelines` and `_resolve_database_project_name` to SDK postgres path |
| `tests/data_quality/test_synced_tables_online.py` | Modify | Switch from raw REST to SDK, unpack `SyncedTableConfig` |
| `terraform/modules/synced_tables/` | Delete | Entire module (main.tf, variables.tf, versions.tf, outputs.tf) |
| `terraform/environments/dev/main.tf` | Modify | Remove `module "synced_tables" { ... }` block (lines 192-203) |
| `docs/superpowers/adrs/ADR-026-sdk-managed-synced-table-lifecycle.md` | Create | ADR documenting the SDK migration decision |
| `TODO.md` | Modify | Close #1 (TF workaround), G2G3 (SDK hardening), update PR-gamma |
| `docs/superpowers/adrs/ADR-005-lakebase-synced-table-grants.md` | Modify | Note SDK path replaces UI+TF workflow |
| `docs/engineering/conventions.md` | Modify | Update Lakebase Ops recreation procedure |
| `CLAUDE.md` | Modify | Update index recreation bullet to reference SDK path |

**Pre-existing:** `pyproject.toml` already has `databricks-sdk==0.110.0` pinned (applied in prior session, uncommitted). This rides with the PR commit.

**Outage window (explicit design decision):** The migration deletes all 41 tables then recreates them, producing a ~30-minute window where the Taipy app has degraded Lakebase connectivity. This is acceptable because: (1) the app is low-traffic with no SLA, (2) a rolling migration (delete-create-wait per table) adds significant code complexity for no user-facing benefit, and (3) the wall-clock is dominated by the wait-for-ONLINE phase which happens after all tables are recreated. Schedule during off-peak hours.

**Rollback procedure:** If Phase 3 fails mid-flight, re-run `scripts/migrate_synced_tables.py` — creates are idempotent (tolerates "already exists"). If the SDK API itself is broken, fall back to the Databricks UI to recreate individual tables manually, then re-import their pipeline IDs into the Python SSOT. The Terraform module cannot be restored (it was a configuration-free import shell anyway). Full rollback to pre-migration state requires: (1) restore `terraform/modules/synced_tables/` from git, (2) recreate all 41 via UI, (3) `terraform import` each, (4) revert Python changes. This is deliberately expensive — the migration is a one-way door by design.

---

### Task 1: SyncedTableConfig Dataclass + SYNCED_TABLES Migration

**Files:**
- Modify: `src/ingestion/refresh_synced_tables.py:49-188`
- Test: `src/tests/test_refresh_synced_tables.py`

- [ ] **Step 1: Write failing tests for SyncedTableConfig**

Add these tests to `src/tests/test_refresh_synced_tables.py`:

```python
def test_synced_table_config_is_frozen_dataclass() -> None:
    """SyncedTableConfig must be a frozen dataclass with the expected fields."""
    from dataclasses import fields

    from ingestion.refresh_synced_tables import SyncedTableConfig

    cfg = SyncedTableConfig(
        name="fct_test_synced",
        source_table="fct_test",
        primary_key_columns=("test_id",),
    )
    assert cfg.name == "fct_test_synced"
    assert cfg.source_table == "fct_test"
    assert cfg.primary_key_columns == ("test_id",)
    assert cfg.scheduling_policy == "SNAPSHOT"
    assert cfg.schema_override is None

    field_names = {f.name for f in fields(cfg)}
    assert field_names == {"name", "source_table", "primary_key_columns", "scheduling_policy", "schema_override"}

    # frozen — assignment must raise
    with pytest.raises(AttributeError):
        cfg.name = "mutated"  # type: ignore[misc]


def test_synced_table_config_triggered() -> None:
    """SyncedTableConfig with TRIGGERED policy."""
    from ingestion.refresh_synced_tables import SyncedTableConfig

    cfg = SyncedTableConfig(
        name="fct_passes_synced",
        source_table="fct_passes",
        primary_key_columns=("pass_id",),
        scheduling_policy="TRIGGERED",
    )
    assert cfg.scheduling_policy == "TRIGGERED"


def test_synced_table_config_schema_override() -> None:
    """SyncedTableConfig with schema_override for observability tables."""
    from ingestion.refresh_synced_tables import SyncedTableConfig

    cfg = SyncedTableConfig(
        name="workflow_cost_live_synced",
        source_table="workflow_cost_live",
        primary_key_columns=("run_id",),
        schema_override="observability",
    )
    assert cfg.schema_override == "observability"


def test_synced_table_config_rejects_invalid_scheduling_policy() -> None:
    """SyncedTableConfig.__post_init__ must reject typos like TRIGGRED."""
    from ingestion.refresh_synced_tables import SyncedTableConfig

    with pytest.raises(ValueError, match="Invalid scheduling_policy"):
        SyncedTableConfig(
            name="fct_bad_synced",
            source_table="fct_bad",
            primary_key_columns=("id",),
            scheduling_policy="TRIGGRED",  # type: ignore[arg-type]
        )
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `uv run pytest src/tests/test_refresh_synced_tables.py::test_synced_table_config_is_frozen_dataclass -v`
Expected: FAIL with `ImportError: cannot import name 'SyncedTableConfig'`

- [ ] **Step 3: Implement SyncedTableConfig dataclass**

Add at the top of `src/ingestion/refresh_synced_tables.py` (after the imports, before `DEFAULT_CATALOG`):

```python
from dataclasses import dataclass
from typing import Literal

_SCHEDULING_POLICIES = ("SNAPSHOT", "TRIGGERED")


@dataclass(frozen=True)
class SyncedTableConfig:
    """Single source of truth for a Lakebase synced table definition.

    All consumers — create, delete, refresh, grants, indexes — read from the
    ``SYNCED_TABLES`` list of these configs. No metadata split between TF and Python.
    """

    name: str  # e.g. "fct_shots_synced"
    source_table: str  # e.g. "fct_shots"
    primary_key_columns: tuple[str, ...]
    scheduling_policy: Literal["SNAPSHOT", "TRIGGERED"] = "SNAPSHOT"
    schema_override: str | None = None  # None -> DEFAULT_SCHEMA ("dev_gold")

    def __post_init__(self) -> None:
        if self.scheduling_policy not in _SCHEDULING_POLICIES:
            msg = (
                f"Invalid scheduling_policy {self.scheduling_policy!r} for {self.name}. "
                f"Must be one of {_SCHEDULING_POLICIES}"
            )
            raise ValueError(msg)
```

- [ ] **Step 4: Run tests to verify SyncedTableConfig tests pass**

Run: `uv run pytest src/tests/test_refresh_synced_tables.py::test_synced_table_config_is_frozen_dataclass src/tests/test_refresh_synced_tables.py::test_synced_table_config_triggered src/tests/test_refresh_synced_tables.py::test_synced_table_config_schema_override -v`
Expected: PASS

- [ ] **Step 5: Write failing tests for SYNCED_TABLES scheduling distribution and PK invariants**

Add to `src/tests/test_refresh_synced_tables.py`:

```python
def test_synced_tables_scheduling_policy_distribution() -> None:
    """12 TRIGGERED + 29 SNAPSHOT = 41 total."""
    from ingestion.refresh_synced_tables import SYNCED_TABLES

    triggered = [c for c in SYNCED_TABLES if c.scheduling_policy == "TRIGGERED"]
    snapshot = [c for c in SYNCED_TABLES if c.scheduling_policy == "SNAPSHOT"]
    assert len(triggered) == 12, f"Expected 12 TRIGGERED, got {len(triggered)}: {[c.name for c in triggered]}"
    assert len(snapshot) == 29, f"Expected 29 SNAPSHOT, got {len(snapshot)}: {[c.name for c in snapshot]}"


def test_synced_tables_all_have_primary_keys() -> None:
    """Every SyncedTableConfig must have at least one PK column."""
    from ingestion.refresh_synced_tables import SYNCED_TABLES

    for config in SYNCED_TABLES:
        assert len(config.primary_key_columns) >= 1, f"{config.name} has no PK columns"


def test_synced_tables_names_end_with_synced() -> None:
    """Every synced table name must end with _synced."""
    from ingestion.refresh_synced_tables import SYNCED_TABLES

    for config in SYNCED_TABLES:
        assert config.name.endswith("_synced"), f"{config.name} does not end with _synced"


def test_synced_tables_source_table_matches_name() -> None:
    """source_table must equal name with _synced suffix stripped."""
    from ingestion.refresh_synced_tables import SYNCED_TABLES

    for config in SYNCED_TABLES:
        assert config.source_table == config.name.removesuffix("_synced"), (
            f"{config.name}: source_table={config.source_table!r} "
            f"does not match name without _synced suffix"
        )


def test_synced_tables_only_one_observability_override() -> None:
    """Only workflow_cost_live_synced uses schema_override."""
    from ingestion.refresh_synced_tables import SYNCED_TABLES

    overrides = [c for c in SYNCED_TABLES if c.schema_override is not None]
    assert len(overrides) == 1
    assert overrides[0].name == "workflow_cost_live_synced"
    assert overrides[0].schema_override == "observability"
```

- [ ] **Step 6: Migrate SYNCED_TABLES from tuples to SyncedTableConfig list**

Replace the `SYNCED_TABLES` constant in `src/ingestion/refresh_synced_tables.py` (lines 139-188) with:

```python
SYNCED_TABLES: list[SyncedTableConfig] = [
    # ── Fact tables ──────────────────────────────────────────────────────────
    SyncedTableConfig("fct_shots_synced", "fct_shots", ("shot_id",)),
    SyncedTableConfig("fct_xg_predictions_v2_synced", "fct_xg_predictions_v2", ("shot_id",)),
    SyncedTableConfig("fct_passes_synced", "fct_passes", ("pass_id",), "TRIGGERED"),
    SyncedTableConfig("fct_player_stats_synced", "fct_player_stats", ("player_stats_id",)),
    SyncedTableConfig("fct_match_summary_synced", "fct_match_summary", ("match_id",)),
    SyncedTableConfig("fct_player_embeddings_synced", "fct_player_embeddings", ("embedding_id",), "TRIGGERED"),
    SyncedTableConfig("fct_action_values_synced", "fct_action_values", ("action_value_id",), "TRIGGERED"),
    SyncedTableConfig("fct_tracking_frames_synced", "fct_tracking_frames", ("tracking_id",), "TRIGGERED"),
    SyncedTableConfig("fct_physical_stats_synced", "fct_physical_stats", ("physical_stats_id",)),
    SyncedTableConfig("fct_defensive_values_synced", "fct_defensive_values", ("defensive_value_id",), "TRIGGERED"),
    SyncedTableConfig("fct_defcon_actions_synced", "fct_defcon_actions", ("defcon_action_id",), "TRIGGERED"),
    SyncedTableConfig("fct_defcon_pressure_synced", "fct_defcon_pressure", ("pressure_id",), "TRIGGERED"),
    SyncedTableConfig("fct_workflow_costs_synced", "fct_workflow_costs", ("task_key", "usage_date", "job_run_id")),
    SyncedTableConfig("fct_formation_labels_synced", "fct_formation_labels", ("formation_label_id",)),
    SyncedTableConfig("fct_player_positions_synced", "fct_player_positions", ("position_id",)),
    SyncedTableConfig("fct_position_maps_synced", "fct_position_maps", ("position_map_id",)),
    SyncedTableConfig("fct_player_embeddings_career_synced", "fct_player_embeddings_career", ("canonical_player_id",)),
    SyncedTableConfig("fct_player_embeddings_season_synced", "fct_player_embeddings_season", ("embedding_season_id",)),
    SyncedTableConfig("fct_line_breaking_results_synced", "fct_line_breaking_results", ("line_breaking_id",), "TRIGGERED"),
    SyncedTableConfig("fct_pausa_rankings_synced", "fct_pausa_rankings", ("player_id",)),
    SyncedTableConfig("fct_player_percentiles_synced", "fct_player_percentiles", ("player_id", "competition_id", "season_id")),
    SyncedTableConfig("fct_off_ball_xt_synced", "fct_off_ball_xt", ("off_ball_xt_id",), "TRIGGERED"),
    SyncedTableConfig("fct_goalkeeper_stats_synced", "fct_goalkeeper_stats", ("gk_stat_id",)),
    SyncedTableConfig("fct_player_embeddings_career_360_synced", "fct_player_embeddings_career_360", ("canonical_player_id",)),
    SyncedTableConfig("fct_player_embeddings_season_360_synced", "fct_player_embeddings_season_360", ("embedding_season_360_id",)),
    SyncedTableConfig("fct_space_creation_synced", "fct_space_creation", ("space_creation_id",), "TRIGGERED"),
    SyncedTableConfig("fct_pausa_values_synced", "fct_pausa_values", ("pass_id",), "TRIGGERED"),
    SyncedTableConfig("fct_pass_timing_synced", "fct_pass_timing", ("player_id", "match_id")),
    SyncedTableConfig("fct_tracking_avg_positions_synced", "fct_tracking_avg_positions", ("avg_position_id",)),
    SyncedTableConfig("fct_tracking_shape_timeline_synced", "fct_tracking_shape_timeline", ("shape_timeline_id",), "TRIGGERED"),
    # Pre-aggregated marts
    SyncedTableConfig("fct_heatmap_agg_synced", "fct_heatmap_agg", ("competition_id", "team_id", "action_type", "x_bin", "y_bin")),
    SyncedTableConfig("fct_vaep_breakdown_agg_synced", "fct_vaep_breakdown_agg", ("competition_id", "team_id", "player_id", "action_type")),
    SyncedTableConfig("fct_gk_actions_detail_synced", "fct_gk_actions_detail", ("gk_action_id",)),
    SyncedTableConfig("fct_funnel_stages_agg_synced", "fct_funnel_stages_agg", ("match_id", "team_id", "game_state")),
    SyncedTableConfig("fct_discipline_events_synced", "fct_discipline_events", ("event_id",)),
    SyncedTableConfig("fct_tracking_context_synced", "fct_tracking_context", ("tracking_context_id",)),
    # ── Cost / Observability ─────────────────────────────────────────────────
    SyncedTableConfig("workflow_cost_live_synced", "workflow_cost_live", ("run_id",), schema_override="observability"),
    # ── Dimension tables ─────────────────────────────────────────────────────
    SyncedTableConfig("dim_players_synced", "dim_players", ("player_id",)),
    SyncedTableConfig("dim_teams_synced", "dim_teams", ("team_id",)),
    SyncedTableConfig("dim_competitions_synced", "dim_competitions", ("competition_id",)),
    SyncedTableConfig("dim_matches_synced", "dim_matches", ("match_key",)),
]
```

- [ ] **Step 7: Update the existing drift guard test**

Replace `test_synced_tables_list_has_41_entries` in `src/tests/test_refresh_synced_tables.py`:

```python
def test_synced_tables_list_has_41_entries() -> None:
    """SYNCED_TABLES drift guard — 41 SyncedTableConfig entries."""
    from ingestion.refresh_synced_tables import SYNCED_TABLES, SyncedTableConfig

    assert len(SYNCED_TABLES) == 41
    assert all(isinstance(c, SyncedTableConfig) for c in SYNCED_TABLES)
```

- [ ] **Step 8: Run all tests to verify they pass**

Run: `uv run pytest src/tests/test_refresh_synced_tables.py -v`
Expected: All tests PASS

- [ ] **Step 9: Run ruff + pyright**

Run: `uv run ruff check src/ingestion/refresh_synced_tables.py src/tests/test_refresh_synced_tables.py && uv run pyright src/ingestion/refresh_synced_tables.py src/tests/test_refresh_synced_tables.py`
Expected: Clean

- [ ] **Step 10: Commit**

```bash
git add src/ingestion/refresh_synced_tables.py src/tests/test_refresh_synced_tables.py
git commit -m "feat(synced-tables): SyncedTableConfig dataclass replaces tuple SSOT

Promote SYNCED_TABLES from list[tuple[str, str | None]] to
list[SyncedTableConfig] with frozen dataclass carrying name,
source_table, primary_key_columns, scheduling_policy, and
schema_override. All 41 tables now have PKs extracted from
Terraform HCL. 12 TRIGGERED + 29 SNAPSHOT.

Co-Authored-By: Claude Opus 4.6 <noreply@anthropic.com>"
```

---

### Task 2: Refresh Module Postgres API Migration

**Files:**
- Modify: `src/ingestion/refresh_synced_tables.py:216-548`
- Test: `src/tests/test_refresh_synced_tables.py`

This task switches `_get_pipeline_id`, `wait_until_online`, `_derive_upstream_tables`, and `main()` from raw REST / tuple unpacking to SDK + SyncedTableConfig.

- [ ] **Step 1: Write failing test for _get_pipeline_id SDK path**

**SDK response shape (verified empirically against databricks-sdk 0.110.0):**

| Access path | Field | Type | Notes |
|---|---|---|---|
| `meta.status.pipeline_id` | pipeline_id | `Optional[str]` | Used by `_get_pipeline_id` |
| `meta.status.detailed_state` | detailed_state | `Optional[SyncedTableState]` | **Enum**, not string. `enum == "string"` is `False`; use `.value` for comparison |
| `meta.status.project` | project | `Optional[str]` | May contain project reference (empirical check needed) |
| `meta.spec.*` | spec fields | typed | `SyncedTableSyncedTableSpec` |

Both `pipeline_id` and `detailed_state` live on `meta.status` (`SyncedTableSyncedTableStatus`). There is no `data_synchronization_status` attribute on the postgres API response.

Add to `src/tests/test_refresh_synced_tables.py`:

```python
def test_get_pipeline_id_uses_postgres_sdk(monkeypatch: pytest.MonkeyPatch) -> None:
    """_get_pipeline_id must use ws.postgres.get_synced_table, not raw REST."""
    import ingestion.refresh_synced_tables as mod

    mock_ws = MagicMock()
    mock_meta = MagicMock()
    mock_meta.status.pipeline_id = "test-pipeline-uuid"
    mock_ws.postgres.get_synced_table.return_value = mock_meta
    monkeypatch.setattr(mod, "WorkspaceClient", lambda: mock_ws)

    from ingestion.refresh_synced_tables import _get_pipeline_id

    result = _get_pipeline_id("fct_shots_synced", catalog="soccer_analytics", schema="dev_gold")
    assert result == "test-pipeline-uuid"
    mock_ws.postgres.get_synced_table.assert_called_once_with(
        name="synced_tables/soccer_analytics.dev_gold.fct_shots_synced"
    )
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest src/tests/test_refresh_synced_tables.py::test_get_pipeline_id_uses_postgres_sdk -v`
Expected: FAIL (current implementation uses raw REST with `headers` parameter)

- [ ] **Step 3: Rewrite _get_pipeline_id to use SDK**

Replace `_get_pipeline_id` in `src/ingestion/refresh_synced_tables.py`:

```python
def _get_pipeline_id(
    table: str,
    *,
    catalog: str,
    schema: str,
) -> str:
    """Fetch the pipeline_id backing a synced table via the SDK postgres API.

    catalog/schema are required keyword args — never reads module state.
    """
    full_name = f"{catalog}.{schema}.{table}"
    ws = _get_workspace_client()
    meta = ws.postgres.get_synced_table(name=f"synced_tables/{full_name}")
    status = getattr(meta, "status", None)
    pid = getattr(status, "pipeline_id", None) if status else None
    if not pid:
        msg = f"Synced table {full_name} has no pipeline_id in status"
        raise RuntimeError(msg)
    return pid
```

**Note:** The `headers` parameter is removed. Update call sites in `main()`:

In `main()`, change:
```python
pipeline_id = _get_pipeline_id(table, headers, catalog=args.catalog, schema=schema)
```
to:
```python
pipeline_id = _get_pipeline_id(table, catalog=args.catalog, schema=schema)
```

- [ ] **Step 4: Run test to verify it passes**

Run: `uv run pytest src/tests/test_refresh_synced_tables.py::test_get_pipeline_id_uses_postgres_sdk -v`
Expected: PASS

- [ ] **Step 5: Rewrite wait_until_online to use SDK**

Replace `wait_until_online` in `src/ingestion/refresh_synced_tables.py`:

```python
def wait_until_online(
    table_fqn: str,
    *,
    timeout_s: int = 600,
    poll_interval_s: int = 15,
) -> None:
    """Poll a Lakebase synced table until detailed_state == SYNCED_TABLE_ONLINE_NO_PENDING_UPDATE.

    Uses the SDK postgres API (``w.postgres.get_synced_table()``) instead of
    raw REST. Switched in the SDK synced table migration (ADR-026).

    Args:
        table_fqn: Fully-qualified Unity Catalog name of the synced table,
            e.g. ``"soccer_analytics.dev_gold.fct_action_values_synced"``.
        timeout_s: Maximum total wait time. Default 600s (10 min).
        poll_interval_s: Seconds between status polls. Default 15s.

    Raises:
        TimeoutError: if the table does not reach ONLINE within ``timeout_s``.
        RuntimeError: if the table hits a terminal failure state.
    """
    if not IDENTIFIER_RE.match(table_fqn.split(".")[-1]):
        raise ValueError(f"Invalid table_fqn last-segment: {table_fqn!r}")

    ws = _get_workspace_client()
    name = f"synced_tables/{table_fqn}"

    start = time.monotonic()
    last_state: str | None = None
    while True:
        meta = ws.postgres.get_synced_table(name=name)
        status = getattr(meta, "status", None)
        detailed_state = getattr(status, "detailed_state", None)
        # SDK returns SyncedTableState enum; extract .value for string comparison
        # against module-level constants (enum == str is False in Python).
        detailed_state_str = detailed_state.value if detailed_state else None
        last_state = detailed_state_str

        if detailed_state_str == SYNCED_TABLE_ONLINE_STATE:
            return

        if detailed_state_str in _SYNCED_TABLE_TERMINAL_FAILURE_STATES:
            raise RuntimeError(f"Synced table {table_fqn} reached terminal failure state {detailed_state_str!r}")

        elapsed = time.monotonic() - start
        if elapsed > timeout_s:
            raise TimeoutError(
                f"Synced table {table_fqn} did not reach {SYNCED_TABLE_ONLINE_STATE} "
                f"within {timeout_s}s (last detailed_state: {last_state!r}, elapsed: {elapsed:.1f}s)"
            )

        time.sleep(poll_interval_s)
```

- [ ] **Step 6: Update _derive_upstream_tables for SyncedTableConfig**

Replace `_derive_upstream_tables` in `src/ingestion/refresh_synced_tables.py`:

```python
def _derive_upstream_tables(catalog: str, default_schema: str) -> list[str]:
    """Derive upstream Delta table FQNs from SYNCED_TABLES.

    For each ``SyncedTableConfig``, uses ``source_table`` and qualifies with
    the override schema (or default).
    """
    tables: list[str] = []
    for config in SYNCED_TABLES:
        effective_schema = config.schema_override or default_schema
        tables.append(f"{catalog}.{effective_schema}.{config.source_table}")
    return tables
```

- [ ] **Step 7: Update main() for SyncedTableConfig iteration**

In `main()`, update the table_schema_map construction and selected iteration:

```python
    # Build lookup: table_name -> config
    table_config_map: dict[str, SyncedTableConfig] = {c.name: c for c in SYNCED_TABLES}
    all_table_names = list(table_config_map.keys())

    if args.tables:
        selected = [t.strip() for t in args.tables.split(",") if t.strip()]
        for t in selected:
            if t not in table_config_map:
                print(f"ERROR: Unknown table '{t}'. Valid: {', '.join(all_table_names)}")
                sys.exit(1)
    else:
        selected = all_table_names

    headers = _get_auth_headers()
    headers["Content-Type"] = "application/json"

    total = len(selected)
    errors = 0
    triggered: list[tuple[str, str]] = []  # (table, pipeline_id)

    for i, table in enumerate(selected, 1):
        try:
            config = table_config_map[table]
            schema = config.schema_override or args.schema
            pipeline_id = _get_pipeline_id(table, catalog=args.catalog, schema=schema)
            # ... rest of the loop body unchanged except no headers in _get_pipeline_id ...
```

- [ ] **Step 8: Remove `import requests` if no longer needed**

Check whether any function still uses `requests` directly. After this task:
- `_get_pipeline_id` — SDK (no requests)
- `wait_until_online` — SDK (no requests)
- `_trigger_refresh` — still raw REST (needs requests)
- `_poll_pipeline` — still raw REST (needs requests)
- `_fetch_table_owner` — still raw REST (needs requests)
- `_check_event_log_ownership` — via `_fetch_table_owner` (needs requests)

**`requests` import stays.** No change needed.

- [ ] **Step 9: Run all tests**

Run: `uv run pytest src/tests/test_refresh_synced_tables.py -v`
Expected: All tests PASS

- [ ] **Step 10: Run ruff + pyright**

Run: `uv run ruff check src/ingestion/refresh_synced_tables.py && uv run pyright src/ingestion/refresh_synced_tables.py`
Expected: Clean

- [ ] **Step 11: Commit**

```bash
git add src/ingestion/refresh_synced_tables.py src/tests/test_refresh_synced_tables.py
git commit -m "feat(synced-tables): postgres SDK migration for refresh module

Switch _get_pipeline_id and wait_until_online from raw REST
/api/2.0/database/ to w.postgres.get_synced_table() SDK method.
Update _derive_upstream_tables and main() for SyncedTableConfig
iteration. Pipeline trigger/poll stays on /api/2.0/pipelines/
(same endpoint for both creation paths).

Co-Authored-By: Claude Opus 4.6 <noreply@anthropic.com>"
```

---

### Task 3: Script Migrations (delete, fix_event_log, grants)

**Files:**
- Modify: `scripts/delete_synced_table.py:83-94`
- Modify: `scripts/fix_event_log_ownership.py:289-319`
- Modify: `scripts/grant_synced_table_permissions.py:131-180`

- [ ] **Step 1: Update delete_synced_table.py**

In `scripts/delete_synced_table.py`, change the delete call (line 90):

```python
    # Step 1: Delete synced table via Databricks SDK
    print(f"\n[1/2] Deleting synced table: {full_name}")
    try:
        ws.postgres.delete_synced_table(name=f"synced_tables/{full_name}")
        print("  OK — synced table deleted")
    except Exception as exc:
        print(f"  ERROR: {exc}")
        print("  (If 'not found', the table may already be deleted. Continuing to PG cleanup.)")
```

Also update the final print message (line 122):

```python
    print(f"\nDone. Recreate {table_name} via scripts/migrate_synced_tables.py or SDK.")
```

- [ ] **Step 2: Update fix_event_log_ownership.py _get_pipeline_id**

In `scripts/fix_event_log_ownership.py`, replace `_get_pipeline_id` (lines 289-319):

```python
def _get_pipeline_id(
    *,
    ws: WorkspaceClient,
    catalog: str,
    schema: str,
    table: str,
) -> str:
    """Fetch the pipeline_id backing a synced table via the SDK postgres API.

    The returned pipeline_id is validated against the canonical UUID-36
    format before being returned — the value is later interpolated into SQL
    strings and URL paths.
    """
    full_name = f"{catalog}.{schema}.{table}"
    meta = ws.postgres.get_synced_table(name=f"synced_tables/{full_name}")
    status = getattr(meta, "status", None)
    pipeline_id = getattr(status, "pipeline_id", None) if status else None
    if not pipeline_id:
        msg = f"synced_tables API returned no pipeline_id for {full_name}"
        raise RuntimeError(msg)
    if not _validate_uuid(pipeline_id):
        msg = f"synced_tables API returned non-UUID pipeline_id {pipeline_id!r} for {full_name}"
        raise RuntimeError(msg)
    return pipeline_id
```

**Note:** The signature changes from `(*, host, headers, catalog, schema, table)` to `(*, ws, catalog, schema, table)`. Update ALL call sites in the file to pass `ws=ws` instead of `host=host, headers=headers`. Search for `_get_pipeline_id(` in the file and update each call.

- [ ] **Step 3: Update fix_event_log_ownership.py SYNCED_TABLES iteration**

Find all `for table_name, schema_override in SYNCED_TABLES:` or `for table_name, schema in ...` patterns and update to iterate `SyncedTableConfig`:

```python
for config in SYNCED_TABLES:
    table_name = config.name
    schema = config.schema_override or DEFAULT_SCHEMA
```

- [ ] **Step 4: Update grant_synced_table_permissions.py _enumerate_pipelines**

In `scripts/grant_synced_table_permissions.py`, replace `_enumerate_pipelines` (lines 163-180):

```python
def _enumerate_pipelines(ws: WorkspaceClient) -> list[tuple[str, str, str]]:
    """Resolve all synced tables' backing pipeline_ids via the SDK postgres API.

    Returns list of (table_name, schema, pipeline_id). Raises on resolve
    failure — no silent drops.
    """
    resolved: list[tuple[str, str, str]] = []
    for config in SYNCED_TABLES:
        schema = config.schema_override or DEFAULT_SCHEMA
        full = f"{DEFAULT_CATALOG}.{schema}.{config.name}"
        meta = ws.postgres.get_synced_table(name=f"synced_tables/{full}")
        status = getattr(meta, "status", None)
        pid = getattr(status, "pipeline_id", None) if status else None
        if not pid:
            msg = f"Synced table {full} has no pipeline_id in status"
            raise RuntimeError(msg)
        resolved.append((config.name, schema, pid))
    return resolved
```

- [ ] **Step 5: Update grant_synced_table_permissions.py _resolve_database_project_name**

In `scripts/grant_synced_table_permissions.py`, replace `_resolve_database_project_name` (lines 131-160):

```python
def _resolve_database_project_name(ws: WorkspaceClient, synced_table_full_name: str) -> str:
    """Resolve the short project name (e.g. 'soccer-analytics-dev').

    The Permissions API identifies database-projects by short name, NOT by the
    UID synced-table metadata exposes as effective_database_project_id. This
    helper bridges the two via ws.postgres.list_projects().

    The typed SDK ``SyncedTable`` does NOT expose ``effective_database_project_id``
    (verified against databricks-sdk 0.110.0 — fields are: create_time, name,
    spec, status, uid). However, ``SyncedTableSyncedTableStatus.project`` may
    contain the project reference directly.

    Strategy (execute in order, stop at first success):
    1. Try ``meta.status.project`` — if it returns a project slug like
       ``"projects/soccer-analytics-dev"``, strip the prefix and return.
    2. Fall back to raw ``ws.api_client.do("GET", ...)`` against the
       ``/api/2.0/postgres/synced_tables/`` path to get
       ``effective_database_project_id``, then resolve via ``list_projects()``.
    """
    # Attempt 1: typed SDK status.project field
    meta = ws.postgres.get_synced_table(name=f"synced_tables/{synced_table_full_name}")
    status = getattr(meta, "status", None)
    project_ref = getattr(status, "project", None) if status else None
    if project_ref and isinstance(project_ref, str) and project_ref.startswith(_PROJECT_NAME_PREFIX):
        return project_ref[len(_PROJECT_NAME_PREFIX) :]

    # Attempt 2: raw API for effective_database_project_id
    raw = ws.api_client.do("GET", f"/api/2.0/postgres/synced_tables/synced_tables/{synced_table_full_name}")
    uid = raw.get("effective_database_project_id") if isinstance(raw, dict) else None
    if not uid:
        msg = f"Synced table {synced_table_full_name} has no project ref in status or effective_database_project_id in raw API"
        raise RuntimeError(msg)
    for project in ws.postgres.list_projects():
        if project.uid == uid:
            if not project.name or not project.name.startswith(_PROJECT_NAME_PREFIX):
                msg = f"Project uid={uid!r} has unexpected name {project.name!r} (expected 'projects/<slug>')"
                raise RuntimeError(msg)
            return project.name[len(_PROJECT_NAME_PREFIX) :]
    msg = f"No Lakebase project has uid={uid!r}; cannot resolve permissions-API short name"
    raise RuntimeError(msg)
```

- [ ] **Step 6: Update grant_synced_table_permissions.py sample_full construction**

In `main()` (line 331):
```python
    sample_config = SYNCED_TABLES[0]
    sample_full = f"{DEFAULT_CATALOG}.{sample_config.schema_override or DEFAULT_SCHEMA}.{sample_config.name}"
```

- [ ] **Step 7: Write mock test for fix_event_log_ownership._get_pipeline_id signature**

Add to `src/tests/test_migrate_synced_tables.py` (the script-level test file):

```python
def test_fix_event_log_get_pipeline_id_uses_sdk() -> None:
    """fix_event_log_ownership._get_pipeline_id must accept ws kwarg (not host/headers)
    and call ws.postgres.get_synced_table with the synced_tables/ prefix."""
    import scripts.fix_event_log_ownership as mod

    mock_ws = MagicMock()
    mock_status = MagicMock()
    mock_status.pipeline_id = "abc-123-def-456"
    mock_meta = MagicMock()
    mock_meta.status = mock_status
    mock_ws.postgres.get_synced_table.return_value = mock_meta

    # Must accept ws= keyword (not host=/headers=)
    pipeline_id = mod._get_pipeline_id(
        ws=mock_ws, catalog="soccer_analytics", schema="dev_gold", table="fct_test_synced",
    )
    assert pipeline_id == "abc-123-def-456"
    mock_ws.postgres.get_synced_table.assert_called_once_with(
        name="synced_tables/soccer_analytics.dev_gold.fct_test_synced"
    )
```

- [ ] **Step 8: Run ruff + pyright on all three scripts**

Run: `uv run ruff check scripts/delete_synced_table.py scripts/fix_event_log_ownership.py scripts/grant_synced_table_permissions.py && uv run pyright scripts/delete_synced_table.py scripts/fix_event_log_ownership.py scripts/grant_synced_table_permissions.py`
Expected: Clean

- [ ] **Step 9: Commit**

```bash
git add scripts/delete_synced_table.py scripts/fix_event_log_ownership.py scripts/grant_synced_table_permissions.py src/tests/test_migrate_synced_tables.py
git commit -m "feat(synced-tables): postgres SDK migration for maintenance scripts

Switch delete_synced_table, fix_event_log_ownership, and
grant_synced_table_permissions from /api/2.0/database/ raw REST
to w.postgres.* SDK methods. Update all SYNCED_TABLES iteration
for SyncedTableConfig unpacking.

Co-Authored-By: Claude Opus 4.6 <noreply@anthropic.com>"
```

---

### Task 4: test_synced_tables_online.py Postgres API Migration

**Files:**
- Modify: `tests/data_quality/test_synced_tables_online.py`

- [ ] **Step 1: Rewrite the test to use SDK instead of raw REST**

Replace the entire file `tests/data_quality/test_synced_tables_online.py`:

```python
"""Live health check: every synced table in ``ingestion.refresh_synced_tables.SYNCED_TABLES``
must be in ``SYNCED_TABLE_ONLINE_NO_PENDING_UPDATE`` state.

Uses the Databricks SDK ``w.postgres.get_synced_table()`` — consistent with all
other consumers post SDK synced table migration (ADR-026).

Requires live Databricks API access (DATABRICKS_HOST + DATABRICKS_TOKEN).
Skipped when those env vars are unset.
"""

from __future__ import annotations

import logging
import os

import pytest

pytest.importorskip("databricks.sdk", reason="databricks-sdk not installed (run `uv sync --extra sdk`)")

requires_databricks = pytest.mark.skipif(
    not all(os.environ.get(var) for var in ("DATABRICKS_HOST", "DATABRICKS_TOKEN")),
    reason="DATABRICKS_HOST + DATABRICKS_TOKEN env vars required for live state check",
)

_LOGGER = logging.getLogger("test_synced_tables_online")

_CATALOG = os.environ.get("UC_CATALOG", "soccer_analytics")
_DEFAULT_SCHEMA = os.environ.get("GOLD_SCHEMA", "dev_gold")


@requires_databricks
def test_all_synced_tables_online() -> None:
    """Every entry in ``SYNCED_TABLES`` must report
    ``SYNCED_TABLE_ONLINE_NO_PENDING_UPDATE`` from the postgres API."""
    from databricks.sdk import WorkspaceClient

    from ingestion.refresh_synced_tables import SYNCED_TABLE_ONLINE_STATE, SYNCED_TABLES

    ws = WorkspaceClient()
    failures: list[str] = []

    for config in SYNCED_TABLES:
        schema = config.schema_override or _DEFAULT_SCHEMA
        full_name = f"{_CATALOG}.{schema}.{config.name}"
        name = f"synced_tables/{full_name}"
        try:
            meta = ws.postgres.get_synced_table(name=name)
            status = getattr(meta, "status", None)
            raw_state = getattr(status, "detailed_state", None)
            # SDK returns SyncedTableState enum; extract .value for string comparison
            detailed_state = raw_state.value if raw_state else "UNKNOWN"
        except Exception as exc:
            failures.append(f"{full_name}: SDK error — {exc}")
            continue
        if detailed_state != SYNCED_TABLE_ONLINE_STATE:
            failures.append(
                f"{full_name}: detailed_state={detailed_state!r} "
                f"(expected {SYNCED_TABLE_ONLINE_STATE!r}). "
                f"Investigate via Databricks UI or "
                f"`python scripts/delete_synced_table.py {config.name}` "
                f"followed by `python scripts/migrate_synced_tables.py`."
            )
        else:
            _LOGGER.info("OK %s — %s", full_name, detailed_state)

    assert not failures, "Synced-table health check failures:\n" + "\n".join(f"  - {msg}" for msg in failures)
```

- [ ] **Step 2: Run ruff + pyright**

Run: `uv run ruff check tests/data_quality/test_synced_tables_online.py && uv run pyright tests/data_quality/test_synced_tables_online.py`
Expected: Clean

- [ ] **Step 3: Commit**

```bash
git add tests/data_quality/test_synced_tables_online.py
git commit -m "feat(synced-tables): postgres SDK migration for online health check

Switch test_synced_tables_online from raw REST to SDK
w.postgres.get_synced_table(). Iterate SyncedTableConfig
instead of (name, schema_override) tuples.

Co-Authored-By: Claude Opus 4.6 <noreply@anthropic.com>"
```

---

### Task 5: Migration Script

**Files:**
- Create: `scripts/migrate_synced_tables.py`

- [ ] **Step 1: Create the migration script**

Create `scripts/migrate_synced_tables.py`:

```python
#!/usr/bin/env python3
"""One-shot SDK synced table migration (ADR-026).

Migrates all 41 Lakebase synced tables from UI-created / Terraform-imported
to SDK-managed via ``w.postgres.create_synced_table()``.

Four phases:
  Phase 0 — Smoke test (create + grants + delete on throwaway table)
  Phase 1 — Delete all 41 synced tables
  Phase 2 — Enable CDF on TRIGGERED source tables
  Phase 3 — Create all 41 via SDK
  Phase 4 — Wait until all ONLINE, then run maintenance pipeline

Usage:
    uv run python scripts/migrate_synced_tables.py                    # Full migration
    uv run python scripts/migrate_synced_tables.py --phase 0          # Smoke test only
    uv run python scripts/migrate_synced_tables.py --skip-phase 0     # Skip smoke test

Idempotent: re-running after partial failure picks up where it left off.
Phase 1 tolerates "not found" errors. Phase 2 is idempotent (SET TBLPROPERTIES).
Phase 3 tolerates "already exists" errors.

**Outage window:** Phases 1-3 delete all 41 tables then recreate them. The Taipy
app has degraded Lakebase connectivity (~30 min) during this window. This is an
explicit design choice: the app is low-traffic, a rolling migration adds significant
complexity for no user-facing benefit, and the total wall-clock is dominated by
Phase 4 (wait for ONLINE) which happens after tables already exist.

Auth: uses WorkspaceClient — must run as workspace admin with PAT.
"""

from __future__ import annotations

import argparse
import re
import subprocess
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed

from databricks.sdk import WorkspaceClient
from databricks.sdk.service.postgres import (
    SyncedTable,
    SyncedTableSyncedTableSpec,
    SyncedTableSyncedTableSpecSyncedTableSchedulingPolicy as SchedulingPolicy,
)
from databricks.sdk.service.sql import StatementState

# All identifiers interpolated into SQL / PG queries must match this pattern.
_IDENTIFIER_RE = re.compile(r"^[a-zA-Z_][a-zA-Z0-9_]*$")

from ingestion.refresh_synced_tables import (
    DEFAULT_CATALOG,
    DEFAULT_SCHEMA,
    SYNCED_TABLES,
    SyncedTableConfig,
    wait_until_online,
)

_SCHEDULING_POLICY_MAP: dict[str, SchedulingPolicy] = {
    "SNAPSHOT": SchedulingPolicy.SNAPSHOT,
    "TRIGGERED": SchedulingPolicy.TRIGGERED,
}

_BRANCH = "projects/soccer-analytics-dev/branches/production"
_PG_DATABASE = "databricks_postgres"
_SMOKE_TEST_TABLE = "dim_competitions_synced_sdk_test"
_SMOKE_TEST_SOURCE = "dim_competitions"

# Databricks SQL warehouse for Statement Execution API (CDF enablement)
_WAREHOUSE_ID_ENV = "DATABRICKS_HTTP_PATH"


def _log(phase: int, msg: str) -> None:
    print(f"[Phase {phase}] {msg}", flush=True)


def _get_warehouse_id() -> str:
    """Extract warehouse ID from DATABRICKS_HTTP_PATH env var.

    The env var has format: /sql/1.0/warehouses/<warehouse_id>
    """
    import os
    import re

    http_path = os.environ.get(_WAREHOUSE_ID_ENV, "")
    match = re.search(r"/warehouses/([a-f0-9]+)$", http_path)
    if not match:
        msg = (
            f"Cannot extract warehouse ID from {_WAREHOUSE_ID_ENV}={http_path!r}. "
            f"Set DATABRICKS_HTTP_PATH=/sql/1.0/warehouses/<id>"
        )
        raise RuntimeError(msg)
    return match.group(1)


def _create_synced_table(ws: WorkspaceClient, config: SyncedTableConfig, catalog: str, default_schema: str) -> None:
    """Create a single synced table via the SDK postgres API using typed objects."""
    schema = config.schema_override or default_schema
    synced_table_id = f"{catalog}.{schema}.{config.name}"
    source_fqn = f"{catalog}.{schema}.{config.source_table}"

    try:
        policy = _SCHEDULING_POLICY_MAP[config.scheduling_policy]
    except KeyError:
        raise ValueError(
            f"Unknown scheduling_policy {config.scheduling_policy!r} for {config.name}. "
            f"Valid values: {sorted(_SCHEDULING_POLICY_MAP)}"
        ) from None

    ws.postgres.create_synced_table(
        synced_table=SyncedTable(
            spec=SyncedTableSyncedTableSpec(
                source_table_full_name=source_fqn,
                branch=_BRANCH,
                primary_key_columns=list(config.primary_key_columns),
                scheduling_policy=policy,
                postgres_database=_PG_DATABASE,
                create_database_objects_if_missing=True,
            ),
        ),
        synced_table_id=synced_table_id,
    )


def _delete_synced_table(ws: WorkspaceClient, config: SyncedTableConfig, catalog: str, default_schema: str) -> bool:
    """Delete a single synced table. Returns True if deleted, False if not found."""
    schema = config.schema_override or default_schema
    full_name = f"{catalog}.{schema}.{config.name}"
    name = f"synced_tables/{full_name}"
    try:
        ws.postgres.delete_synced_table(name=name)
        return True
    except Exception as exc:
        if "not found" in str(exc).lower() or "does not exist" in str(exc).lower():
            return False
        raise


def phase_0_smoke_test(ws: WorkspaceClient, catalog: str, default_schema: str) -> None:
    """Phase 0: Create a throwaway synced table, verify it works, delete it."""
    _log(0, f"Creating throwaway table: {_SMOKE_TEST_TABLE}")
    smoke_config = SyncedTableConfig(
        name=_SMOKE_TEST_TABLE,
        source_table=_SMOKE_TEST_SOURCE,
        primary_key_columns=("competition_id",),
    )

    # Clean up any leftover from a prior run
    _delete_synced_table(ws, smoke_config, catalog, default_schema)
    # Brief pause to let the async delete operation propagate on Databricks' side
    # before attempting to create a table with the same name.
    time.sleep(5)

    # Create
    _create_synced_table(ws, smoke_config, catalog, default_schema)
    _log(0, "Created — waiting for ONLINE state")

    # Wait
    full_name = f"{catalog}.{default_schema}.{_SMOKE_TEST_TABLE}"
    wait_until_online(full_name, timeout_s=300, poll_interval_s=10)
    _log(0, "ONLINE — verifying PG-side data")

    # Verify data actually synced to PostgreSQL
    import os
    import uuid

    import psycopg2

    lakebase_host = os.environ.get("LAKEBASE_HOST", "")
    if lakebase_host:
        endpoint = os.environ.get(
            "LAKEBASE_ENDPOINT_NAME",
            "projects/soccer-analytics-dev/branches/production/endpoints/primary",
        )
        host = (ws.config.host or "").rstrip("/")
        auth_headers: dict[str, str] = ws.config.authenticate()  # type: ignore[assignment]
        import base64
        import json

        import requests  # noqa: E402 — lazy import, only needed when LAKEBASE_HOST is set

        resp = requests.post(
            f"{host}/api/2.0/postgres/credentials",
            headers={**auth_headers, "Content-Type": "application/json"},
            json={"endpoint": endpoint, "request_id": str(uuid.uuid4())},
            verify=True,
            timeout=(10, 30),
        )
        resp.raise_for_status()
        token = resp.json()["token"]
        payload_b64 = token.split(".")[1]
        payload_b64 += "=" * (4 - len(payload_b64) % 4)
        username = json.loads(base64.b64decode(payload_b64))["sub"]

        # Validate identifiers before interpolation into SQL (OWASP SQL injection defence)
        for _label, _val in [("schema", default_schema), ("table", _SMOKE_TEST_TABLE)]:
            if not _IDENTIFIER_RE.match(_val):
                raise ValueError(f"Invalid {_label} identifier: {_val!r}")

        conn = psycopg2.connect(
            host=lakebase_host, port=5432, dbname="databricks_postgres",
            user=username, password=token, sslmode="require",
        )
        with conn.cursor() as cur:
            cur.execute(f'SELECT COUNT(*) FROM {default_schema}."{_SMOKE_TEST_TABLE}"')
            row_count = cur.fetchone()[0]
        conn.close()
        if row_count > 0:
            _log(0, f"PG verification PASSED — {row_count} rows in {_SMOKE_TEST_TABLE}")
        else:
            _log(0, f"WARNING: PG table exists but has 0 rows (may still be syncing)")
    else:
        _log(0, "LAKEBASE_HOST not set — skipping PG-side verification")

    # Clean up
    _delete_synced_table(ws, smoke_config, catalog, default_schema)
    _log(0, "Cleaned up throwaway table")
    _log(0, "SMOKE TEST PASSED")


def phase_1_delete_all(ws: WorkspaceClient, catalog: str, default_schema: str) -> None:
    """Phase 1: Delete all 41 synced tables."""
    _log(1, f"Deleting {len(SYNCED_TABLES)} synced tables")
    deleted = 0
    not_found = 0
    for i, config in enumerate(SYNCED_TABLES, 1):
        try:
            if _delete_synced_table(ws, config, catalog, default_schema):
                deleted += 1
                print(f"  [{i}/{len(SYNCED_TABLES)}] Deleted: {config.name}")
            else:
                not_found += 1
                print(f"  [{i}/{len(SYNCED_TABLES)}] Not found (already deleted): {config.name}")
        except Exception as exc:
            print(f"  [{i}/{len(SYNCED_TABLES)}] ERROR deleting {config.name}: {exc}")
            raise
    _log(1, f"COMPLETE — {deleted} deleted, {not_found} already gone")


def phase_2_enable_cdf(ws: WorkspaceClient, catalog: str, default_schema: str) -> None:
    """Phase 2: Enable CDF on source tables for TRIGGERED synced tables."""
    triggered = [c for c in SYNCED_TABLES if c.scheduling_policy == "TRIGGERED"]
    _log(2, f"Enabling CDF on {len(triggered)} TRIGGERED source tables")

    warehouse_id = _get_warehouse_id()

    for config in triggered:
        schema = config.schema_override or default_schema
        source_fqn = f"{catalog}.{schema}.{config.source_table}"
        stmt = f"ALTER TABLE {source_fqn} SET TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true')"

        result = ws.statement_execution.execute_statement(
            statement=stmt,
            warehouse_id=warehouse_id,
            wait_timeout="30s",
        )
        if result.status and result.status.state == StatementState.SUCCEEDED:
            print(f"  CDF enabled: {source_fqn}")
        else:
            error = getattr(result.status, "error", None) if result.status else None
            msg = f"CDF enablement failed for {source_fqn}: {error}"
            raise RuntimeError(msg)

    _log(2, "COMPLETE — CDF enabled on all TRIGGERED sources")


def phase_3_create_all(ws: WorkspaceClient, catalog: str, default_schema: str) -> None:
    """Phase 3: Create all 41 synced tables via SDK."""
    _log(3, f"Creating {len(SYNCED_TABLES)} synced tables")
    created = 0
    already_exists = 0
    for i, config in enumerate(SYNCED_TABLES, 1):
        try:
            _create_synced_table(ws, config, catalog, default_schema)
            created += 1
            print(f"  [{i}/{len(SYNCED_TABLES)}] Created: {config.name} ({config.scheduling_policy})")
        except Exception as exc:
            if "already exists" in str(exc).lower():
                already_exists += 1
                print(f"  [{i}/{len(SYNCED_TABLES)}] Already exists: {config.name}")
            else:
                print(f"  [{i}/{len(SYNCED_TABLES)}] ERROR creating {config.name}: {exc}")
                raise
    _log(3, f"COMPLETE — {created} created, {already_exists} already existed")


def phase_4_wait_and_maintain(ws: WorkspaceClient, catalog: str, default_schema: str) -> None:
    """Phase 4: Wait for all tables to come ONLINE, then run maintenance."""
    _log(4, f"Waiting for {len(SYNCED_TABLES)} tables to come ONLINE")

    def _wait_one(config: SyncedTableConfig) -> tuple[str, bool, str]:
        schema = config.schema_override or default_schema
        fqn = f"{catalog}.{schema}.{config.name}"
        try:
            wait_until_online(fqn, timeout_s=1200, poll_interval_s=15)
            return (config.name, True, "ONLINE")
        except Exception as exc:
            return (config.name, False, str(exc))

    with ThreadPoolExecutor(max_workers=10) as executor:
        futures = {executor.submit(_wait_one, c): c for c in SYNCED_TABLES}
        failures: list[str] = []
        for future in as_completed(futures):
            name, ok, msg = future.result()
            if ok:
                print(f"  {name}: ONLINE")
            else:
                print(f"  {name}: FAILED — {msg}")
                failures.append(f"{name}: {msg}")

    if failures:
        print(f"\nERROR: {len(failures)} tables failed to come online:")
        for f in failures:
            print(f"  - {f}")
        sys.exit(1)

    _log(4, "All tables ONLINE — running maintenance pipeline")

    # Run the full maintenance pipeline: ownership -> grants -> indexes -> verify
    subprocess.run(
        ["uv", "run", "python", "scripts/maintain_synced_tables.py", "--skip-refresh"],
        check=True,
    )

    _log(4, "MAINTENANCE COMPLETE")


def main() -> None:
    parser = argparse.ArgumentParser(
        description="One-shot SDK synced table migration (ADR-026).",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    parser.add_argument("--phase", type=int, help="Run only this phase (0-4)")
    parser.add_argument("--skip-phase", type=int, action="append", default=[], help="Skip these phases")
    parser.add_argument("--catalog", default=DEFAULT_CATALOG, help=f"Catalog (default: {DEFAULT_CATALOG})")
    parser.add_argument("--schema", default=DEFAULT_SCHEMA, help=f"Default schema (default: {DEFAULT_SCHEMA})")
    args = parser.parse_args()

    ws = WorkspaceClient()
    phases = {
        0: ("Smoke test", phase_0_smoke_test),
        1: ("Delete all", phase_1_delete_all),
        2: ("Enable CDF", phase_2_enable_cdf),
        3: ("Create all", phase_3_create_all),
        4: ("Wait + maintain", phase_4_wait_and_maintain),
    }

    for phase_num, (label, fn) in phases.items():
        if args.phase is not None and phase_num != args.phase:
            continue
        if phase_num in args.skip_phase:
            print(f"\n{'='*60}")
            print(f"SKIPPING Phase {phase_num}: {label}")
            continue
        print(f"\n{'='*60}")
        print(f"Phase {phase_num}: {label}")
        print(f"{'='*60}")
        fn(ws, args.catalog, args.schema)

    print(f"\n{'='*60}")
    print("MIGRATION COMPLETE")
    print(f"{'='*60}")


if __name__ == "__main__":
    main()
```

- [ ] **Step 2: Run ruff + pyright**

Run: `uv run ruff check scripts/migrate_synced_tables.py && uv run pyright scripts/migrate_synced_tables.py`
Expected: Clean

- [ ] **Step 3: Commit**

```bash
git add scripts/migrate_synced_tables.py
git commit -m "feat(synced-tables): one-shot SDK migration script (ADR-026)

Five-phase migration: smoke test, delete 41, enable CDF on
TRIGGERED sources, create 41 via w.postgres.create_synced_table(),
wait for ONLINE + run maintenance pipeline.

Co-Authored-By: Claude Opus 4.6 <noreply@anthropic.com>"
```

---

### Task 6: Migration Script Tests

**Files:**
- Create: `src/tests/test_migrate_synced_tables.py`

- [ ] **Step 1: Write tests for _get_warehouse_id**

Create `src/tests/test_migrate_synced_tables.py`:

```python
"""Tests for scripts/migrate_synced_tables.py — migration script unit tests."""

from __future__ import annotations

import os
from unittest.mock import MagicMock, patch

import pytest


def test_get_warehouse_id_extracts_from_http_path(monkeypatch: pytest.MonkeyPatch) -> None:
    """_get_warehouse_id must extract the warehouse ID from DATABRICKS_HTTP_PATH."""
    monkeypatch.setenv("DATABRICKS_HTTP_PATH", "/sql/1.0/warehouses/abc123def456")

    import scripts.migrate_synced_tables as mod

    assert mod._get_warehouse_id() == "abc123def456"


def test_get_warehouse_id_raises_on_missing_env(monkeypatch: pytest.MonkeyPatch) -> None:
    """_get_warehouse_id must raise RuntimeError when env var is missing."""
    monkeypatch.delenv("DATABRICKS_HTTP_PATH", raising=False)

    import scripts.migrate_synced_tables as mod

    with pytest.raises(RuntimeError, match="Cannot extract warehouse ID"):
        mod._get_warehouse_id()


def test_get_warehouse_id_raises_on_malformed_path(monkeypatch: pytest.MonkeyPatch) -> None:
    """_get_warehouse_id must raise on paths that don't match the expected format."""
    monkeypatch.setenv("DATABRICKS_HTTP_PATH", "/sql/1.0/clusters/abc123")

    import scripts.migrate_synced_tables as mod

    with pytest.raises(RuntimeError, match="Cannot extract warehouse ID"):
        mod._get_warehouse_id()
```

- [ ] **Step 2: Write tests for _create_synced_table and _delete_synced_table**

Add to `src/tests/test_migrate_synced_tables.py`:

```python
def test_create_synced_table_uses_typed_sdk_objects() -> None:
    """_create_synced_table must pass typed SyncedTable + SyncedTableSyncedTableSpec."""
    from databricks.sdk.service.postgres import (
        SyncedTable,
        SyncedTableSyncedTableSpec,
        SyncedTableSyncedTableSpecSyncedTableSchedulingPolicy as SchedulingPolicy,
    )

    from ingestion.refresh_synced_tables import SyncedTableConfig

    import scripts.migrate_synced_tables as mod

    mock_ws = MagicMock()
    config = SyncedTableConfig(
        name="fct_test_synced",
        source_table="fct_test",
        primary_key_columns=("test_id",),
        scheduling_policy="TRIGGERED",
    )

    mod._create_synced_table(mock_ws, config, "soccer_analytics", "dev_gold")

    mock_ws.postgres.create_synced_table.assert_called_once()
    call_kwargs = mock_ws.postgres.create_synced_table.call_args
    assert call_kwargs.kwargs["synced_table_id"] == "soccer_analytics.dev_gold.fct_test_synced"

    synced_table = call_kwargs.kwargs["synced_table"]
    assert isinstance(synced_table, SyncedTable)
    assert isinstance(synced_table.spec, SyncedTableSyncedTableSpec)
    assert synced_table.spec.source_table_full_name == "soccer_analytics.dev_gold.fct_test"
    assert synced_table.spec.scheduling_policy == SchedulingPolicy.TRIGGERED
    assert synced_table.spec.primary_key_columns == ["test_id"]


def test_delete_synced_table_returns_true_on_success() -> None:
    """_delete_synced_table returns True when deletion succeeds."""
    from ingestion.refresh_synced_tables import SyncedTableConfig

    import scripts.migrate_synced_tables as mod

    mock_ws = MagicMock()
    config = SyncedTableConfig(
        name="fct_test_synced",
        source_table="fct_test",
        primary_key_columns=("test_id",),
    )

    result = mod._delete_synced_table(mock_ws, config, "soccer_analytics", "dev_gold")
    assert result is True
    mock_ws.postgres.delete_synced_table.assert_called_once_with(
        name="synced_tables/soccer_analytics.dev_gold.fct_test_synced"
    )


def test_delete_synced_table_returns_false_on_not_found() -> None:
    """_delete_synced_table returns False when table doesn't exist."""
    from ingestion.refresh_synced_tables import SyncedTableConfig

    import scripts.migrate_synced_tables as mod

    mock_ws = MagicMock()
    mock_ws.postgres.delete_synced_table.side_effect = Exception("Resource not found")
    config = SyncedTableConfig(
        name="fct_test_synced",
        source_table="fct_test",
        primary_key_columns=("test_id",),
    )

    result = mod._delete_synced_table(mock_ws, config, "soccer_analytics", "dev_gold")
    assert result is False


def test_delete_synced_table_raises_on_unexpected_error() -> None:
    """_delete_synced_table must propagate non-not-found errors."""
    from ingestion.refresh_synced_tables import SyncedTableConfig

    import scripts.migrate_synced_tables as mod

    mock_ws = MagicMock()
    mock_ws.postgres.delete_synced_table.side_effect = RuntimeError("Permission denied")
    config = SyncedTableConfig(
        name="fct_test_synced",
        source_table="fct_test",
        primary_key_columns=("test_id",),
    )

    with pytest.raises(RuntimeError, match="Permission denied"):
        mod._delete_synced_table(mock_ws, config, "soccer_analytics", "dev_gold")
```

- [ ] **Step 3: Write test for --phase / --skip-phase argument edge case**

Add to `src/tests/test_migrate_synced_tables.py`:

```python
def test_phase_skip_phase_conflict_skips(monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]) -> None:
    """--phase 0 --skip-phase 0 should skip phase 0 (skip wins), not execute it."""
    import scripts.migrate_synced_tables as mod

    # Mock WorkspaceClient to avoid live API calls
    mock_ws = MagicMock()
    monkeypatch.setattr(mod, "WorkspaceClient", lambda: mock_ws)

    # Patch argparse to return the conflicting args
    monkeypatch.setattr(
        mod.argparse.ArgumentParser, "parse_args",
        lambda self, args=None, namespace=None: mod.argparse.Namespace(
            phase=0, skip_phase=[0], catalog="test", schema="test",
        ),
    )

    mod.main()
    captured = capsys.readouterr()
    assert "SKIPPING Phase 0" in captured.out
    # phase_0_smoke_test should NOT have been called
    mock_ws.postgres.create_synced_table.assert_not_called()
```

- [ ] **Step 4: Run tests**

Run: `uv run pytest src/tests/test_migrate_synced_tables.py -v`
Expected: All PASS

- [ ] **Step 5: Run ruff + pyright**

Run: `uv run ruff check src/tests/test_migrate_synced_tables.py && uv run pyright src/tests/test_migrate_synced_tables.py`
Expected: Clean

- [ ] **Step 6: Commit**

```bash
git add src/tests/test_migrate_synced_tables.py
git commit -m "test(synced-tables): unit tests for migration script

Tests for _get_warehouse_id, _create_synced_table (typed SDK objects),
_delete_synced_table (success/not-found/error), and phase-skip
behavior (--phase 0 --skip-phase 0 verifies skip wins via capsys).

Co-Authored-By: Claude Opus 4.6 <noreply@anthropic.com>"
```

---

### Task 7: Terraform Cleanup

**Files:**
- Delete: `terraform/modules/synced_tables/main.tf`
- Delete: `terraform/modules/synced_tables/variables.tf`
- Delete: `terraform/modules/synced_tables/versions.tf`
- Delete: `terraform/modules/synced_tables/outputs.tf`
- Modify: `terraform/environments/dev/main.tf:192-203`

- [ ] **Step 1: Delete the synced_tables module directory**

```bash
rm -rf terraform/modules/synced_tables/
```

- [ ] **Step 2: Remove the module block from dev main.tf**

In `terraform/environments/dev/main.tf`, delete lines 192-203 (the `module "synced_tables"` block and its comment):

```hcl
# ── Module: Synced Tables ────────────────────────────────────────────────────
# Mirrors gold-layer Delta tables into Lakebase for low-latency app queries.

module "synced_tables" {
  source = "../../modules/synced_tables"

  catalog_name           = module.workspace.catalog_name
  database_instance_name = module.lakebase.instance_name
  environment            = var.environment
  gold_schema            = "${var.environment}_gold"
  observability_schema   = "observability"
}
```

Replace with a comment:

```hcl
# ── Synced Tables (removed — ADR-026) ───────────────────────────────────────
# Synced tables are now SDK-managed via scripts/migrate_synced_tables.py.
# The terraform/modules/synced_tables/ module was removed.
# State cleanup: terraform state rm 'module.synced_tables.*' (run before apply).
```

- [ ] **Step 3: Verify terraform fmt**

Run: `cd terraform/environments/dev && terraform fmt -check`
Expected: Clean (if terraform is installed locally; skip if not)

- [ ] **Step 4: Document the state cleanup command**

The state cleanup (`terraform state rm` for all 40 resources) must run before `terraform apply` in the live environment. This is an operator action, not automated in the PR. Add a note to the PR description:

```
## Operator action required before terraform apply

After merging, before running `terraform apply`:

```bash
cd terraform/environments/dev
terraform init
# Remove all 40 synced table resources from state (dim_matches was never in TF)
for resource in fct_shots fct_xg_predictions_v2 fct_passes fct_player_stats fct_match_summary fct_player_embeddings fct_player_embeddings_season fct_discipline_events fct_player_embeddings_career fct_action_values fct_tracking_frames fct_physical_stats fct_defensive_values fct_defcon_pressure fct_defcon_actions fct_pausa_values fct_pass_timing fct_tracking_avg_positions fct_tracking_shape_timeline fct_formation_labels fct_goalkeeper_stats fct_line_breaking_results fct_off_ball_xt fct_pausa_rankings fct_player_percentiles fct_player_positions fct_position_maps fct_space_creation fct_tracking_context fct_player_embeddings_career_360 fct_player_embeddings_season_360 dim_players dim_teams dim_competitions fct_workflow_costs workflow_cost_live fct_heatmap_agg fct_vaep_breakdown_agg fct_gk_actions_detail fct_funnel_stages_agg; do
  terraform state rm "module.synced_tables.databricks_database_synced_database_table.${resource}" 2>/dev/null || true
done
terraform plan  # Should show no synced-table drift
```
```

- [ ] **Step 5: Commit**

```bash
git add -A terraform/modules/synced_tables/ terraform/environments/dev/main.tf
git commit -m "feat(synced-tables): remove Terraform synced_tables module (ADR-026)

SDK-managed synced tables replace the Terraform module. The
lifecycle { ignore_changes = all } workaround and UI-creation
requirement are eliminated. State cleanup (terraform state rm)
is an operator action documented in the PR.

Co-Authored-By: Claude Opus 4.6 <noreply@anthropic.com>"
```

---

### Task 8: ADR-026

**Files:**
- Create: `docs/superpowers/adrs/ADR-026-sdk-managed-synced-table-lifecycle.md`

- [ ] **Step 1: Write ADR-026**

Create `docs/superpowers/adrs/ADR-026-sdk-managed-synced-table-lifecycle.md`:

```markdown
# ADR-026: SDK-Managed Synced Table Lifecycle

| Field | Value |
|---|---|
| **Date** | 2026-05-22 |
| **Status** | Accepted |
| **Deciders** | Karsten Skyt Nielsen |

## Context

Lakebase synced tables were created via the Databricks UI (the only supported path for Autoscaling projects) and imported into Terraform state. The Terraform provider (`databricks_database_synced_database_table`) only exposed `database_instance_name` (Provisioned path), not the project/branch selection needed for Autoscaling. The `lifecycle { ignore_changes = all }` workaround made TF a config-free import shell: no updates, no real management.

Three gaps blocked the public-repo goal:

- **G1 (closed PR 4b):** `wait_until_online()` helper for post-creation polling.
- **G2:** `refresh_synced_tables.py` hit `/api/2.0/database/synced_tables/` (legacy Provisioned endpoint). SDK-created tables live under `/api/2.0/postgres/synced_tables/` (Autoscaling endpoint). The two paths are not interchangeable.
- **G3:** Grants and event_log ownership against SDK-created tables was unverified.

Databricks SDK 0.110.0 shipped `PostgresAPI` with full CRUD: `create_synced_table`, `get_synced_table`, `delete_synced_table`. Empirically verified (2026-05-21) on a throwaway `dim_competitions_synced_sdk_test` table.

## Decision

All 41 Lakebase synced tables are managed via `w.postgres.create_synced_table()` from the Databricks SDK. The Terraform `synced_tables` module is removed entirely. The `SYNCED_TABLES` constant in `src/ingestion/refresh_synced_tables.py` is the single source of truth, promoted from `list[tuple[str, str | None]]` to `list[SyncedTableConfig]` with frozen dataclass carrying name, source table, primary key columns, scheduling policy, and schema override.

12 tables use TRIGGERED (CDF-based) scheduling; 29 use SNAPSHOT.

## Alternatives considered

| Option | Pros | Cons | Why rejected |
|---|---|---|---|
| A. Wait for TF provider Autoscaling support | Zero custom tooling | Provider issue #5456 has no timeline; blocks public repo indefinitely | Indefinite wait |
| B. Raw REST API wrapper | No SDK dependency | Fragile, no type safety, duplicates SDK work, two API paths to maintain | SDK exists and works |
| C. SDK-managed (chosen) | Type-safe, single API path, full CRUD, idempotent migration script | SDK version pin (0.110.0) | -- |

## Consequences

### Positive

- Public repo blocker removed (no `lifecycle { ignore_changes = all }` hack).
- Synced table creation, deletion, and recreation are fully scriptable.
- 12 fact tables promoted to TRIGGERED (CDF-based incremental sync).
- Single source of truth for all synced table metadata (`SyncedTableConfig`).
- No legacy `/api/2.0/database/synced_tables/` calls remain in the codebase.

### Negative

- Hard dependency on `databricks-sdk>=0.110.0` (already in `[sdk]` optional extra).
- One-shot migration requires ~30 min downtime (delete + recreate + wait for ONLINE).
- Terraform state cleanup is a manual operator action before `terraform apply`.

### Neutral

- Migration script (`scripts/migrate_synced_tables.py`) is a one-shot tool that remains for future table additions/recreations.
- CDF enablement is derived from `SyncedTableConfig.scheduling_policy` -- adding a TRIGGERED table automatically enables CDF.

## Related

- **Specs:** `docs/superpowers/specs/2026-05-21-sdk-synced-table-migration-design.md`
- **ADRs:** supersedes the TF workaround documented in `ADR-005`
- **External references:** Databricks SDK `PostgresAPI` (v0.110.0), TF provider issue #5456
```

- [ ] **Step 2: Commit**

```bash
git add docs/superpowers/adrs/ADR-026-sdk-managed-synced-table-lifecycle.md
git commit -m "docs: ADR-026 SDK-managed synced table lifecycle

Co-Authored-By: Claude Opus 4.6 <noreply@anthropic.com>"
```

---

### Task 9: Documentation Updates

**Files:**
- Modify: `TODO.md`
- Modify: `docs/superpowers/adrs/ADR-005-lakebase-synced-table-grants.md`
- Modify: `docs/engineering/conventions.md`
- Modify: `CLAUDE.md`

- [ ] **Step 1: Read current state of each file**

Read the TODO.md, ADR-005, conventions.md, and CLAUDE.md sections that need updating.

- [ ] **Step 2: Update TODO.md**

Close the following items:
- **#1** (TF workaround / `lifecycle { ignore_changes = all }`) — mark as DONE with ADR-026 reference
- **G2G3** (SDK hardening gaps) — mark as DONE
- **PR-gamma** — note that 12 tables already migrated to TRIGGERED; remaining candidates are follow-up

- [ ] **Step 3: Update ADR-005**

Add a note to `docs/superpowers/adrs/ADR-005-lakebase-synced-table-grants.md`:

> **2026-05-22 update (ADR-026):** Synced tables are now SDK-managed via `w.postgres.create_synced_table()`. The "create in Databricks UI, then terraform import" workflow is replaced by `scripts/migrate_synced_tables.py`. Grants and event_log ownership procedures are unchanged — they operate on PG-side objects and DLT pipelines regardless of creation path.

- [ ] **Step 4: Update conventions.md Lakebase Ops section**

Update the Lakebase Ops recreation procedure in `docs/engineering/conventions.md` to reference the SDK path:

> **Synced table recreation (post ADR-026):** Run `uv run python scripts/migrate_synced_tables.py --phase 3` to recreate tables, then `--phase 4` for wait + maintenance. For single-table recreation, use `scripts/delete_synced_table.py` followed by running the migration script with `--phase 3`.

- [ ] **Step 5: Update CLAUDE.md index recreation bullet**

In `CLAUDE.md` under "Database Performance > Lakebase (PostgreSQL) — Synced Tables", update the "Index recreation after synced table rebuild" bullet:

> - **Index recreation after synced table rebuild**: Custom PG indexes are dropped when a synced table is recreated. The daily `.github/workflows/lakebase-grants.yml` GitHub Action reapplies them automatically. For immediate manual post-recreation repair, use `uv run python scripts/maintain_synced_tables.py --skip-refresh`. For full migration (all 41 tables), use `uv run python scripts/migrate_synced_tables.py` which runs the maintenance pipeline in Phase 4. See `docs/engineering/conventions.md` Lakebase Ops.

- [ ] **Step 6: Commit**

```bash
git add TODO.md docs/superpowers/adrs/ADR-005-lakebase-synced-table-grants.md docs/engineering/conventions.md CLAUDE.md
git commit -m "docs: close TODO #1/G2G3, update Lakebase Ops for SDK path (ADR-026)

Co-Authored-By: Claude Opus 4.6 <noreply@anthropic.com>"
```

---

### Task 10: Full Verification

- [ ] **Step 1: Run ruff on all modified files**

```bash
uv run ruff check src/ingestion/refresh_synced_tables.py src/tests/test_refresh_synced_tables.py scripts/delete_synced_table.py scripts/fix_event_log_ownership.py scripts/grant_synced_table_permissions.py scripts/migrate_synced_tables.py tests/data_quality/test_synced_tables_online.py
```
Expected: Clean

- [ ] **Step 2: Run pyright on all modified files**

```bash
uv run pyright src/ingestion/refresh_synced_tables.py src/tests/test_refresh_synced_tables.py scripts/delete_synced_table.py scripts/fix_event_log_ownership.py scripts/grant_synced_table_permissions.py scripts/migrate_synced_tables.py tests/data_quality/test_synced_tables_online.py
```
Expected: Clean

- [ ] **Step 3: Run full unit test suite**

```bash
uv run pytest src/tests/ -v
```
Expected: All tests pass (filter known pre-existing failures per `project_known_pretest_failures_on_main_2026_05_04.md`)

- [ ] **Step 4: Verify no legacy database API calls remain**

```bash
# Must return zero matches (excluding this plan and the ADR)
rg "/api/2.0/database/synced_tables" src/ scripts/ tests/ --type py
```
Expected: No matches

- [ ] **Step 5: Verify SYNCED_TABLES count**

```bash
uv run python -c "from ingestion.refresh_synced_tables import SYNCED_TABLES; print(f'{len(SYNCED_TABLES)} tables'); t = sum(1 for c in SYNCED_TABLES if c.scheduling_policy == 'TRIGGERED'); print(f'{t} TRIGGERED, {len(SYNCED_TABLES)-t} SNAPSHOT')"
```
Expected: `41 tables`, `12 TRIGGERED, 29 SNAPSHOT`

- [ ] **Step 6: Squash into single commit for PR**

Per user preference (single commit per PR), squash all task commits:

```bash
git rebase -i <base-sha>  # squash into single commit
```

Final commit message:

```
feat(synced-tables): SDK-managed synced table lifecycle (ADR-026)

Migrate all 41 Lakebase synced tables from UI-created / Terraform-imported
to SDK-managed via w.postgres.create_synced_table(). Remove Terraform
synced_tables module. Promote 9 fact tables from SNAPSHOT to TRIGGERED.

- SyncedTableConfig frozen dataclass replaces tuple SSOT
- All consumers switch from /api/2.0/database/ to w.postgres.* SDK
- One-shot migration script: smoke test -> delete -> CDF -> create -> maintain
- ADR-026 documents the decision
- pyproject.toml pins databricks-sdk==0.110.0

12 TRIGGERED + 29 SNAPSHOT = 41 total.
```

---

## Post-Merge Operator Actions

These are NOT automated by the PR. The operator runs them after merge:

1. **Terraform state cleanup** (before `terraform apply`):
   ```bash
   cd terraform/environments/dev && terraform init
   for resource in fct_shots fct_xg_predictions_v2 ... (all 40); do
     terraform state rm "module.synced_tables.databricks_database_synced_database_table.${resource}" 2>/dev/null || true
   done
   terraform plan  # verify no drift
   ```

2. **Run the migration script**:
   ```bash
   uv run python scripts/migrate_synced_tables.py
   ```
   Expected wall-clock: ~30-45 minutes (dominated by Phase 4 wait-for-ONLINE).

3. **Verify all tables ONLINE**:
   ```bash
   uv run pytest tests/data_quality/test_synced_tables_online.py -v
   ```
