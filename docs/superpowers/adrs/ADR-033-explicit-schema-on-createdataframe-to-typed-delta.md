# ADR-033: Explicit schema on every createDataFrame writing to a typed Delta table

| Field | Value |
|---|---|
| **Date** | 2026-05-31 |
| **Status** | Accepted (amended 2026-06-02 — string all-NULL columns need object/None; see Amendment) |
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

## Amendment (2026-06-02): explicit schema is necessary but NOT sufficient for STRING all-NULL columns

### What broke (after this ADR shipped)

The first serverless runs of the event-only path *with* the explicit schema in place
(statsbomb match `15978`, then wyscout match `1694390`) failed with:

```
PySparkTypeError: Exception thrown when converting pandas.Series (float64) with name
'defending_gk_player_id_native' to Arrow Array (string).
  ... ArrowTypeError: Expected a string or bytes dtype, got float64
```

at `spark.createDataFrame(out_pdf, schema=_get_result_schema())` (`_process_event_only_match`
/ `_process_statsbomb_match`).

### Why the original decision was insufficient

The explicit `schema=` fixes the **numeric** all-NULL case: a float64-NaN column declared
`DOUBLE`/`BIGINT` converts to a typed-null Arrow array fine (no more `DoubleType`-vs-`BIGINT`
merge failure). But for a column declared **`STRING`**, `build_output`'s `out[col] = np.nan`
still produces a **float64** pandas column, and Spark Connect's Arrow serializer **cannot cast
float64 → StringType** — it raises rather than silently mis-infers. So the explicit schema turned
a silent-wrong-type into a hard `ArrowTypeError`, but did not make the *string* all-NULL case
work. Intermittent by nature: it only triggers when a match resolves **no** defending GK (all-NULL
`defending_gk_player_id_native`) — statsbomb hit it on its 4th match, wyscout on its 1st. Both
event-only providers, via the shared `build_output`.

### Decision (amendment)

`build_output` (the shared driver+executor formatter) now fills/coerces **STRING** output
columns to **object/`None`**, never `np.nan` (float64), so the explicit-schema Arrow write maps
them to `StringType` null columns. The STRING column set is derived from `ACTION_CONTEXT_DDL`
(the single source of truth → `_STRING_OUTPUT_COLUMNS`), so it is **drift-safe** — any STRING
column added to the DDL is covered automatically, with no second list to maintain. This mirrors
the GradientSports single-source-of-truth id-coercion discipline (ADR-034). Guarded by three
regression tests in `src/tests/action_context/test_schema.py` (absent-string-column,
present-but-all-NULL-string-column, real-string-values-preserved).

Net rule: a typed Delta write from pandas needs **both** (a) an explicit `schema=` (numeric
inference/merge — original decision) **and** (b) dtype-correct fill for non-numeric columns
(string → object/None — this amendment). `_get_result_schema()` + `build_output` together now
satisfy both.

### Amendment v2 (2026-06-02, same day): STRING columns must be STRINGIFIED, not just null-coerced

The v1 amendment above (null → `None`, keep object) was an **incomplete diagnosis**. It assumed the
failing column was *all-NULL*; it is not. `defending_gk_player_id_native` holds **numeric GK player
ids** — statsbomb/wyscout player ids are integers, which pandas stores as **float64** once NaN-mixed.
So on the deployed v1 wheel (0.5.11) the same write still failed, with the error now reading
`Expected bytes, got a 'float' object` (object series, but a real `5522.0` float survived) instead of
`got float64`. v1's NaN→None left the numeric floats untouched.

**Corrected fix:** `build_output` **stringifies** every STRING column via `_to_native_string` —
null → `None`, integral float → its int-string (`5522.0 → "5522"`, not `"5522.0"`), else `str(v)`;
idempotent for already-string columns. A STRING-typed column must contain `str`/`None`, never a
numeric. Added regression test `test_string_columns_stringify_numeric_ids` (the case v1's tests
missed — they only exercised NaN, never numeric values, which is how the half-fix shipped). v1
landed in #333; this v2 correction is a follow-up PR. Process lesson: reproduce the *exact* failing
value shape locally before fixing (the v1 tests passed while production still failed).
