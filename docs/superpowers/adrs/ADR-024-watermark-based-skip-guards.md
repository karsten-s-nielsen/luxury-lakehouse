# ADR-024: Watermark-Based Skip Guards via Delta DESCRIBE HISTORY

| Field | Value |
|---|---|
| **Date** | 2026-05-06 |
| **Status** | Accepted |
| **Deciders** | Karsten Skyt Nielsen |

## Context

The Databricks daily pipeline runs 28 tasks. Compute tasks have effective anti-join skip guards (`find_new_ids`), but 10 downstream tasks — 5 HF publishers, 3 dbt build stages, refresh_synced_tables, and model_validation — either had no guard or unconditionally returned `count=1`. On a steady-state day with no new data, these tasks still boot environments, run pipelines, and exit — wasting DBUs.

The codebase already has three guard patterns (anti-join, HF SHA, count-based) but none that answer "has any upstream Delta table changed since my last run?" without scanning data. Delta Lake's `DESCRIBE HISTORY` command provides table version metadata at zero data-scan cost.

## Decision

Introduce a fourth guard pattern — **watermark-based skip guards** — that compares Delta table versions from `DESCRIBE HISTORY` against stored watermarks in `observability.workflow_watermarks`. Apply it to the 10 downstream tasks that lack effective guards.

The watermark table stores `(workflow_id, upstream_table, last_seen_version, checked_at)` rows. `check_upstream_freshness` filters `DESCRIBE HISTORY` output to data-changing operations only (`WRITE`, `MERGE`, `DELETE`, `UPDATE`, `CREATE TABLE AS SELECT`, `CREATE OR REPLACE TABLE AS SELECT`, `RESTORE`), ignoring OPTIMIZE/VACUUM from Predictive Optimization. Guards fail open (first run, missing tables, inaccessible watermark table all trigger execution).

Upstream table lists are derived from existing sources — workflow card `inputs.tables` or the in-code `SYNCED_TABLES` list — so no separately maintained dependency lists exist.

## Alternatives considered

| Option | Pros | Cons | Why rejected |
|---|---|---|---|
| A. `max(_ingested_at)` comparison | Simple, uses existing column | Requires table scan; dbt marts lack `_ingested_at` | Full scan cost defeats the purpose of a skip guard |
| B. Preflight task emitting task values | Single Spark session for all checks; downstream tasks don't boot at all | Requires Databricks task-value wiring; more complex DAG change | Deferred as future upgrade — current approach supports clean migration path |
| C. Watermark via DESCRIBE HISTORY | Zero data-scan cost; works on all tables; operation-type filtering | New metadata table; DESCRIBE HISTORY is novel to this codebase | Chosen |

## Consequences

### Positive

- 10 downstream tasks skip on steady-state days (no new data), saving ~$0.50+/day in DBUs.
- Guard pattern is reusable for future downstream tasks.
- Watermark table enables operational observability (last-run timestamps per workflow per upstream table).
- Design supports clean upgrade to preflight-task approach (Approach B) without changing core functions.

### Negative

- New Delta metadata table (`observability.workflow_watermarks`) that must be maintained.
- `DESCRIBE HISTORY LIMIT 20` may miss very old data-changing ops if 20+ maintenance ops intervene (mitigated: fail open on first run/missing watermarks).
- dbt model SQL changes don't trigger watermark-based re-run (acceptable: SQL changes deploy via PR + CI; daily schedule correctly skips when data hasn't changed).

### Neutral

- Operator runbook for manual re-run: `DELETE FROM observability.workflow_watermarks WHERE workflow_id = '<id>'` forces next execution.
- StatsBomb guard separately hardened to anti-join pattern (not watermark) since it checks a live external API.

## Related

- **Specs:** `docs/superpowers/specs/2026-05-06-skip-guard-watermark-design.md`
- **Plans:** `docs/superpowers/plans/2026-05-06-skip-guard-watermark.md`
