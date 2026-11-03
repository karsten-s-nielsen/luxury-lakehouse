# ADR-012: The run-written observability object, its exporter and the dbt-DAG read

## Status

Accepted (2026-10-28). Owner decisions: the lake-object OTel backend, the spend model, and dependency option (b)
(spec `docs/specs/2026-10-28-v2-observability-ops-design.md` §7). Written in plan Tasks 2-5, 9, 10.

## Context

- Cycle 1 ships the OpenTelemetry API seam in no-op mode (ADR-009): the run opens a span per step, no SDK is
  installed, every call is a no-op.
- Cycle 2 must observe spend per run and the dependencies between layers, and show collected logs, with the backend
  swappable so the full OTel Collector drops in later with no change to the run's logic (spec §12).
- The object must be run-written and single-writer. The web app (the Space) never runs ingestion (spec §17.1), so
  it must have no path to write the object (Reviewer B OBS-B32). A run writes it whether local or an HF Job; both
  use `hf://` roots, so the signal is the process role (the run), never the storage root (OBS-B23).

## Decision

**One per-run object per concern, outside the DuckLake catalog.** `observability.py` writes the task rows at
`runs/observability/{run_id}.json` and the collected logs at `runs/logs/{run_id}.json`; `runs/dbt.py` retains dbt's
`manifest.json` at `runs/dbt/{run_id}/manifest.json`. Each is read back through the `_Store` seam (the real
`Storage.get`/`put`). A missing object reads back as empty, never a crash.

**The exporter is a plug-in behind the SDK's interface.** `telemetry.py` `install_providers(mode, *, exporter)`
installs the providers without the one-shot global `set_tracer_provider`, so a test or a later Collector swaps the
mode freely. The run entrypoint installs `export` with `LakeSpanExporter(ObservabilityWriter(...))`; a test installs
`memory`; cycle 1 and the Space install `off`. `LakeSpanExporter` turns each finished span into a `TaskRow` and
pushes the batch to a `SpanSink`; `ObservabilityWriter` is the sink that persists it. A second sink (the Collector)
is one swap with no change to the run's instrumentation (spec test 15).

**Spend is a run-level dated estimate** (spec §7.3). `execution_context` (`local`/`hf_job`) and the HF-Job
`flavour` are read from the run environment onto the `lakehouse.run` span. The exporter prices only the span that
carries the context (the run span), so cost is attributed once per run, never summed over steps. A local run is
free; an HF Job costs `flavour_rate × process-duration-hours` from the dated rate table (`cpu-basic` $0.01/h,
`cpu-upgrade` $0.03/h, dated 2026-10-26). A missing or unknown context or flavour records `cost_usd = null` with a
visible `cost_error`, never a silent 0 and never a failed run.

**The dependency view** (spec §7.4) shows the run-step layer edges (`bronze → resolution → dbt`, containment) and
expands the dbt step into the silver→gold model edges read from the retained `manifest.json`.

**Visibility** (spec §7.2, §6.4) is `public`, `private` or `null` per row. The exporter takes an explicit
`lakehouse.visibility` span attribute when the run sets one (a match's own visibility), else derives it from the
module id: a `*_pining` module-level step is `private`, a public direct module is `public`, a run-level step is
`null`. A `null` or `private` row is never shown to an anonymous caller.

**Only the run writes the object.** `observe_run` (in `cli.py`) is called from the CLI `run`/`reparse` entrypoints
only; the HF-Job run is the same CLI `run` command (`schedule/base.py`), launched with the Job env keys the owner
sets (`LAKEHOUSE_EXECUTION_CONTEXT`, `LAKEHOUSE_HF_FLAVOUR`). The web app never calls it: the **Space installs the
`off` / read path only** (it never runs ingestion), so it never exports and has no path to write the object. A test
guards that the web package imports no writer path.

## Consequences

- The observability object is read directly by `RunService` (`task_rows`, `dependency`, `logs`), not modelled as a
  DuckLake table, so no dbt staging/mart model is added (spec §10 "if the object is modelled" is not triggered).
- Run history (`ops_runs`, …) stays the primary Ops data source (ADR-009); this object adds the OTel cost and
  dependency view, it does not replace run history.
- The new span attributes `lakehouse.layer`, `lakehouse.state`, `lakehouse.execution_context`, `lakehouse.flavour`
  and `lakehouse.visibility` are additive to ADR-009; see that ADR's amendment.

## Verification (2026-10-28, cycle 2)

Measured on the full v2 tree (Mac Mini): ruff, ruff format, lint-imports (5 contracts kept), pyright (0 errors) and
pip-audit all green; `pytest -m "not live and not hf and not bench and not integration"` 1035 passed, 3 OS-gated
skips; `pytest -m integration` 5 passed (socket auth, origin lock, the run observability e2e). Every spec §11 item
has a named passing test.
