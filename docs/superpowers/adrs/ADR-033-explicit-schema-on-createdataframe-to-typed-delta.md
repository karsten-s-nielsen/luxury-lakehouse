# ADR-033: Explicit schema on every createDataFrame writing to a typed Delta table

| Field | Value |
|---|---|
| **Date** | 2026-05-31 |
| **Status** | Accepted |
| **Deciders** | Karsten Nielsen |

## Context

The first-ever serverless run of the AC-1 event-only path (wyscout match
`2565706`, 2026-05-31) failed fast (221 s, not the open tracking hang) with:

```
[DELTA_FAILED_TO_MERGE_FIELDS] Failed to merge fields 'frame_id' and 'frame_id' SQLSTATE: 22005
```

Root cause: `_process_event_only_match` and `_process_statsbomb_match` called
`spark.createDataFrame(out_pdf)` with **no schema**. The action-context output
has 104 columns, but event-only / sb360 enrichment populates only ~24 of them;
`analytics.action_context.schema.build_output` fills the ~80 absent tracking
columns via `out[col] = np.nan`, producing all-NULL **float64** pandas columns.
Spark infers those as `DoubleType`. `ingestion.utils.write_delta_table` writes
with `replaceWhere` + `mergeSchema=true`, so the inferred `DoubleType frame_id`
is merged against the table's declared `BIGINT frame_id` — and Delta rejects the
type change. The bug had never shipped because the event-only serverless path
had never run: the table was previously empty (the tracking path hangs — see
project memory `ac1-serverless-hang-open`), and the one populated game (metrica)
went through the local runner's typed chunked `INSERT INTO`.

The tracking path was always safe: it passes `schema=_get_result_schema()` to
`applyInPandas`, so the executor output is typed. Only the two driver-side
`createDataFrame` writers lacked the equivalent.

This is a *class* of bug: any `spark.createDataFrame(pandas_df)` feeding a typed
Delta table where the pandas frame can contain all-NULL columns will infer the
wrong type (`DoubleType` for numeric-NULL, `NullType`/`void` for object-NULL)
and either fail to merge or get silently dropped. `ingestion.utils.finalize_bronze_df`
already documents the `NullType`-drop variant for bronze parsers; this ADR
covers the typed-mart variant.

## Decision

Every `spark.createDataFrame(...)` call that writes to a schema-typed Delta
table MUST pass an explicit `schema=`. For the action-context table that is
`schema=_get_result_schema()` (the same `StructType` the tracking
`applyInPandas` path uses), making the driver-side event-only / sb360 writers
type-identical to the executor tracking writer. Enforced by the AST sentinel
`src/tests/test_action_context_createdataframe_schema.py`.

## Alternatives considered

| Option | Pros | Cons | Why rejected |
|---|---|---|---|
| A. `finalize_bronze_df`-style nullable-dtype cast before write | Reuses an existing helper | Casts pandas dtypes one-by-one; still relies on inference matching the table; doesn't guarantee BIGINT vs DOUBLE | Indirect; the StructType already exists and is authoritative |
| B. Drop `mergeSchema` from `write_delta_table` | Removes the merge step that fails | `mergeSchema` is load-bearing for legitimate additive schema evolution across the whole codebase; removing it is a far wider blast radius | Disproportionate to the fix |
| C. Explicit `schema=_get_result_schema()` on every createDataFrame (chosen) | One-line, authoritative, makes driver + executor paths type-identical, AST-testable | Must remember it on any future driver-side writer | — |

## Consequences

### Positive

- Event-only (wyscout, statsbomb) and sb360 serverless writes succeed — ~5,400
  event-only games become processable once dispatched (3,463 statsbomb + 1,941
  wyscout, minus the sb360 subset).
- Driver-side and executor (`applyInPandas`) writers now share one schema source
  of truth (`_get_result_schema()` ← `ACTION_CONTEXT_DDL`).
- The AST sentinel catches any new unschema'd `createDataFrame` in the module at
  CI time, offline (no Spark needed).

### Negative

- A new driver-side writer in `action_context.py` must remember the `schema=`
  kwarg or the sentinel fails — intentional friction.

### Neutral

- Does not touch the open tracking `applyInPandas` hang — that is a separate
  defect (`ac1-serverless-hang-open`). This ADR only fixes the event-only write.

## CLAUDE.md Amendment

None required. Reinforces the existing "Content validation — verify DataFrame
schema … before every Delta write" hardening rule with a concrete, testable
mechanism for the typed-mart case (complements `finalize_bronze_df` for bronze).

## Related

- **Issues / PRs:** the AC-1 event-only serverless enablement PR (this change)
- **ADRs:** complements ADR-028 (AC-1 hexagon); independent of ADR-031/032
  (executor visibility) and the open tracking hang
- **Code:** `src/ingestion/action_context.py` (`_process_event_only_match`,
  `_process_statsbomb_match`), `src/analytics/action_context/schema.py`
  (`build_output`, `ACTION_CONTEXT_DDL`), `src/ingestion/utils.py`
  (`write_delta_table` mergeSchema, `finalize_bronze_df` NullType variant)
- **Tests:** `src/tests/test_action_context_createdataframe_schema.py`
- **External references:** Delta Lake schema enforcement / `mergeSchema` —
  https://docs.databricks.com/delta/update-schema.html
