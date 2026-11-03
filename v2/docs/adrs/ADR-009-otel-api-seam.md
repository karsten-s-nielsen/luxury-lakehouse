# ADR-009: The OpenTelemetry API seam and its stable names

## Status

Accepted (2026-10-27). Owner decision: option (b), the OpenTelemetry API in no-op mode (spec §21, §29 guesses
35–38). Written in plan Task 21.

## Context

- Cycle 1 builds ingestion only. Observability (the OpenTelemetry SDK, the Collector, exporters and dashboards) is
  the next cycle (spec §2, §21).
- The observability cycle must not have to edit every step of the run to add traces. So cycle 1 code must already
  open a span for each step, with the attributes that the dashboards will group by.
- A span or attribute name that changes breaks every dashboard, alert and saved query that reads it. So each name
  is a public interface from the day it ships (Hyrum's law).
- Run history (`ops_runs`, `ops_module_runs`, `ops_match_steps`, `ops_errors`) stays the data source of the Ops
  page. OpenTelemetry adds an export later. It does not replace run history (spec §21, guess 37).
- Logs are JSON lines on stdout, with the source, the row counts and the time of each step (spec §21).

## Decision

**One module holds the seam.** `lakehouse.telemetry` imports only `opentelemetry-api`. Without an SDK every call
is a no-op. The module holds:

- `SpanName` and `AttributeName`, two `Literal` types that list every name. `SPAN_NAMES` and `ATTRIBUTE_NAMES` are
  the same names as frozen sets. Pyright rejects a name that is not in a list, at the call site.
- `step_span(name, attributes, *, log)`, the only way to open a span. It opens the span with the given attributes.
  It yields a `Step`, and the step sets what it learns while it runs (`step.set("lakehouse.rows", n)`). When the
  step ends, `step_span` sets those attributes on the span and writes one JSON log line.
- `tracer` and `meter` (`lakehouse`). Cycle 1 records no metrics. The observability cycle adds the instruments
  with names under `lakehouse.`, in this module and in this ADR.

**Span names** (one per step of the run, spec §17.2):

| Span | Step |
|------|------|
| `lakehouse.run` | the whole run, after the lease |
| `lakehouse.run.lease` | take the lease |
| `lakehouse.run.config` | read the saved config once |
| `lakehouse.run.visibility_check` | the full listing of a pining module, for the visibility flags |
| `lakehouse.run.list` | list a module, or fetch one source-level listing |
| `lakehouse.run.fetch` | fetch the artifacts of one match |
| `lakehouse.run.archive` | archive the deliveries of one match |
| `lakehouse.run.parse` | parse one match |
| `lakehouse.run.write` | write the bronze files of one match or one listing |
| `lakehouse.run.move` | move the files of one match to the root of its new visibility |
| `lakehouse.run.move_batch` | move up to B matches from private to public with one publish (T3 spec D3) |
| `lakehouse.run.commit` | register a match or listing write and publish it when the checkpoint policy publishes (T3 spec D1), or the run end |
| `lakehouse.run.resolve` | entity resolution (Task 22) |
| `lakehouse.run.dbt` | the dbt hand-off (Task 23) |
| `lakehouse.run.merge` | the run-end maintenance: rewrite and merge the small non-bronze data files (T12 spec D3) |
| `lakehouse.run.cleanup` | the guarded cleanup (Task 12) |
| `lakehouse.restore` | a catalog restore (spec §10.8) |
| `lakehouse.relocate` | a lake relocation (spec §10.5.1) |

**Attribute names:**

| Attribute | Value |
|-----------|-------|
| `lakehouse.run_id` | the run id (a restore or relocate has its own run id) |
| `lakehouse.trigger` | `schedule`, `button` or `cli` |
| `lakehouse.module_id` | the source module, for example `idsse_public` |
| `lakehouse.provider` | the provider, for example `idsse` |
| `lakehouse.channel` | `public` or `pining` |
| `lakehouse.match_key` | the Kimball match key |
| `lakehouse.rows` | the rows that the step listed, parsed or wrote |
| `lakehouse.bytes` | the bytes that the step fetched or archived |
| `lakehouse.catalog_seq` | the catalog version that the step published |
| `lakehouse.lease_generation` | the lease generation that the run holds |

**The log line of a step** has `msg` (the span name), `source` (the module id, else the provider, else
`lakehouse`), each attribute under its name without the `lakehouse.` prefix (`rows`, `bytes`, `catalog_seq`, ...),
`elapsed_ms` and `status`. The status is `ok`, or `error` when the step raised. The exception then propagates.

**Rules:**

- Add a name to `lakehouse.telemetry` and to this ADR before any code uses it. Never rename or remove a name. A
  replacement is a new name, and the old name stays until no consumer reads it.
- Open every span with `step_span` and a literal name. Never call `tracer.start_as_current_span` outside
  `lakehouse.telemetry`.
- Never put a value of a private match into an attribute or a log field. The attributes are keys, counts and names
  of sources, never data values (spec §9).
- Guards (`tests/unit/test_skeleton.py`): every `step_span` call has a literal name in `SPAN_NAMES`; no module
  other than `lakehouse.telemetry` opens a span itself; every name in both lists is used; `step_span` refuses an
  unregistered span or attribute name at run time too. `tests/unit/test_run_steps.py` checks the log lines of a
  real run: source, rows, bytes, catalog sequence, lease generation, time and status.

## Consequences

- The observability cycle installs the SDK and the Collector, and configures exporters. It changes no step of the
  run, and every dashboard can rely on the names above.
- Each step costs one no-op span and one log line. The run writes one log line per step and per match, so a run of
  1,000 matches writes some thousands of lines. That is the price of the per-step timing of spec §21.
- A new step needs a new span name here first. The guards fail until the name is in both places.
- Run history and the logs hold the same counts. The Ops page keeps reading run history.

## Amendment (2026-10-28, observability-and-ops cycle)

Cycle 2 adds five attribute names to `AttributeName`, additively (no name is renamed or removed, so ADR-009's
Hyrum's-law invariant holds):

- `lakehouse.layer` — the medallion layer of a step (`bronze`, `resolution`, `dbt`). The run sets it on every data
  step; the dependency view and the monitoring page group by it.
- `lakehouse.state` — `ok`, `failed` or `skipped`. `step_span` sets it when the step ends; `record_skipped` carries
  the `skipped` state for a step that does not run.
- `lakehouse.execution_context` — `local` or `hf_job`, set on the `lakehouse.run` span from the run environment.
- `lakehouse.flavour` — the HF-Job flavour, set on the `lakehouse.run` span when present.
- `lakehouse.visibility` — `public` or `private`, set by the run on a match-level step so the exporter does not have
  to infer it; absent, the exporter derives visibility from the module id.

The SDK installs through `install_providers` (a holder, not the one-shot global `set_tracer_provider`), and the lake
exporter persists finished spans to the run-written observability object. See ADR-012.
