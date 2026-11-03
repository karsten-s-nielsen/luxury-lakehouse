# Lakehouse v2, cycle 2: observability and operations UI — design

| Field | Value |
|---|---|
| Date | 2026-10-28 |
| Status | Round 3 draft. Round 1 brainstormed with the owner (scope = §13). Round 1 reviewed by two independent reviewers; round 2 addressed every finding; round 2 re-reviewed (`D:\Development\_reviews\2026-10-28-v2-observability-ops-spec-r2-A.md`, `-r2-B.md`): A — all round-1 resolved, 1 SHOULD; B — all BLOCKING resolved, 4 PARTIAL + 1 SHOULD, "small and textual". Round 3 applied every round-2 finding and the owner's B15 decision (b): show silver→gold this cycle via `manifest.json` (§7.4, §13). **Both reviewers APPROVED round 3** (`...-spec-r3-A.md`, `-r3-B.md`): A — all A findings closed; B — all findings resolved, one optional CONSIDER (B18), applied. §15 maps each finding. **Approved by the owner on 2026-10-28.** Amended 2026-10-28 after plan review round 1: owner decision B19 = (a) — cycle 2 also builds the Schedule-save and Review-queue actions (§2 G1, §8, §13); and the reviewer tightenings B22 (anon sees only `public` rows) and B23/B32 (the observability writer is run-only — invoked only from the run entrypoint, never the web app). Further refined after plan review round 2 (B22 pining-module rule, B24 env-key owner gate, B32 process-role signal) and round 3 (B34 transitive web-app check). The plan is in `v2/docs/plans/`. The cycle-1 render-only decision is recorded as cycle-1 spec §29.17 (pointers in cycle-1 §19.3 and plan Task 28). |
| Owner | Karsten Nielsen |
| Branch | a new feature branch off `main` after cycle 1 lands (one branch per cycle). |
| Inputs | cycle 1 design (`2026-10-26-v2-ingestion-design.md`: §8.3 object writers, §17.1 "ingestion never runs in the web app", §17.2 run order, §19 Ops UI, §21 observability seam, §22.2 HF rates, §29.6/§29.17), v1 observability (`_observability__sources.yml`, `fct_workflow_costs`, `CostEstimateHook`, `workflows.py`), `ROADMAP.md` "Observability Layer", `TODO.md` D7, the installed `taipy-gui` 4.1.1 / `flask-socketio` 5.7.0 sources, and `v2/src` (`telemetry.py`, `logging.py`, `runs/run.py`, `runs/dbt.py`, `ui/shell.py`, `web/`). |

**How to read this document.** Cycle-1 conventions: **Owner decision**, **Author's call**, **Builder**, **Must**/**Should**. Cross-references use section numbers, not line numbers.

---

## 1. Intent

Cycle 1 built ingestion and shipped the Ops pages as **render-only** Markdown over the services layer: they display state, and the operator acts through the **CLI** (`run`, `config set`, `schedule`, `decisions`). Cycle 1 does not need UI actions, so the functional actions move to cycle 2 (owner decision, cycle-1 spec §29.17). Cycle 1 also calls the OpenTelemetry **API** but installs no SDK, so nothing is collected (cycle 1 §21).

Cycle 2 makes the operator able to **run and configure** ingestion from the UI, and makes the platform **observable** — spend per run and the dependencies between layers — so the owner can see where time and money go. A small first cut, built to extend cleanly (§12).

Spend is new in kind from v1: a run is **free** when it runs locally (laptop, Mac Mini, CLI), and **costs money** only when delegated to an HF Job. The public HF Space never runs ingestion (cycle 1 §17.1); its "Run now" triggers an HF Job.

## 2. Goals

- **G1. Interactive operations actions.** The Ops pages act through the services layer: Modules (active/inactive, Run now), Filter editor (edit and save a filter, preview a live source without ingesting), Schedule (save/pause/resume, Run now), and the Review queue (accept-recommended, undo, decide) — every functional action of cycle-1 §19.3, which cycle 1 shipped render-only (cycle-1 §29.17; owner decision B19 = (a), §13).
- **G2. A public/authenticated split that covers the real action path.** Anonymous visitors read the monitoring pages and read-only views of public config; every change and every view of private match data requires a signed-in operator. The split is enforced where Taipy sends actions — over socket.io — not only on Flask routes (§6).
- **G3. Lake-object observability.** The OTel SDK collects the API calls cycle 1 emits; an exporter writes per-run cost, lifecycle and the inter-layer dependency to a run-written lake object the Ops pages read (§7).
- **G4. A spend view and a dependency view.** Spend per run (0 local, flavour-rate × duration for an HF Job) and the layer dependency graph of a run, including the dbt silver→gold edges (§7.4, owner decision (b)).
- **G5. A log-viewer page.** A signed-in operator sees the most recent N structured logs, filtered by level and free text, every field masked.
- **G6. Clean extension.** The full OTel Collector and its exporters drop in with no change to the run's logic; the log viewer gains a public private-scrubbed view with no page rewrite (§12).

## 3. Non-goals for cycle 2 (owner decisions)

- The full OTel **Collector** and its exporters (S3 personal tier, Grafana/Datadog enterprise). Cycle 2 writes a lake object only.
- A **public, anonymous log view** (the log viewer is authenticated-only this cycle).
- Observing anything but the **ingestion tasks** of cycle 1.
- Dashboards, alerting, SLIs/SLOs, metric aggregation over history.
- Changes to v1, to v1's `CostEstimateHook`, or to any v1 Taipy page.

## 4. Anti-goals

- Private match data (names, dates, tournaments of a private match) reaches an anonymous visitor on any page or in any error.
- A change of any kind succeeds without a signed-in operator and the write check, **whether it arrives as an HTTP route or a Taipy socket.io action**.
- A spend figure that is silently wrong: a non-zero cost for a free local run, a zero for a billed HF Job, or a **silent zero** for an unknown or missing flavour (that must be a visible flag).
- Instrumentation the later Collector cannot consume without changing the run's logic.
- A secret value in the collected log copy (an exception text or an extra field, not only the message).
- A silent fallback, default or skip in the new pages.

## 5. Constraints

- Build on cycle 1. The services layer is the only entry point for the Ops pages; pages hold no business logic (cycle 1 §19.6, Taipy guard 1).
- The OTel seam of cycle 1 stands: the run calls `opentelemetry-api` (ADR-009, `telemetry.py`); cycle 2 adds the SDK and an exporter, never a second telemetry path. The run may gain span **attributes** and the dbt step may **retain** `manifest.json` (§7.1, §7.4), but no change to its control flow.
- Logs stay JSON lines on stdout with masking (`logging.py`, cycle 1 §20). Cycle 2 masks every field of the collected copy, not only the message (§9).
- Every run-written object follows cycle 1 §8.3: one writer per object family under one lock. The observability object is written by the **run** (run-side).
- Owner standing rules: one feature branch, one end-of-cycle commit with an explicit approval gate, no subagents, STE prose, two independent reviewers for this spec and its plan.
- Python 3.12, the v2 lockfile, the v2 check set and its CI, and the `live`/`heavy`/`hf`/`bench`/OS marker tiers. New test markers are registered in `pyproject.toml` (`--strict-markers`).

## 6. Authorization model (owner decision; access map approved)

Cycle 1 §19.2 enforced sign-in on every Ops **route** via a Flask `before_request` guard, and cycle 1's CSRF check is also a `before_request`. Both run only for Flask HTTP requests. **Taipy sends `on_action`/`on_change` over socket.io, which does not pass Flask's `before_request`** (`flask-socketio` `_SocketIOMiddleware`, `_handle_event`). So route-level checks alone do not cover the actions cycle 2 adds (Reviewer B OBS-B01). Cycle 2 enforces at every entry point.

### 6.1 Two axes

- **Read vs write.** Anonymous visitors read the §6.3 anon pages; every write needs a signed-in operator and the write check. No anonymous change of any kind.
- **Public vs private data.** Anonymous visitors see only non-private content. Names, dates and tournaments of **private** matches — in dimension views, the pining filter preview, and **run errors** — stay behind sign-in (cycle 1 §9, §29.6, §19.2).

### 6.2 Three enforcement points (author's call; builder finalizes)

1. **The socket.io connection.** Authenticated from the session cookie on connect; an unauthenticated connection receives only `public` page events and invokes no action handler. `cors_allowed_origins` is set explicitly to the install's own origin — the Space origin on HF, the localhost origin for a local install — and never left at a permissive or framework-default value.
2. **The action handlers.** Every `on_action`/`on_change` handler begins with a server-side check keyed to the session — sign-in, the write permission, the caller scope — **before** it touches a service. All actions route through one guarded dispatch; a handler with no check cannot be registered (fail closed).
3. **The page content.** Content is resolved **per caller** at request time, not once at build (cycle 1 `ui/shell.py` renders once at build and has no caller scope — this changes). An anonymous caller gets the `public` scope; an operator gets the full scope. A page cannot widen its own scope.

### 6.3 Access map (approved; completed per Reviewer B OBS-B09)

| Page / action | Anonymous (read-only) | Authenticated operator |
|---|---|---|
| Spend per run; layer dependency view (no match identity, §7.2) | view | view |
| Modules / Filter editor — public config summary | view | view |
| Log viewer | no (authenticated-only this cycle) | full |
| Runs (errors may carry private match names), Review queue, Schedule, Settings | no | view |
| Private tournament names, pining filter preview, private dimension views | no | view |
| Any change — active/inactive, save filter, Run now, schedule, review decision | no | perform (write check) |

### 6.4 The private-data boundary

- A `public` service read returns only rows whose visibility is explicitly `public` (a visibility-scoped read path in the services layer). A `null` or `private` visibility is **never** anon-visible (fail-safe, Reviewer B OBS-B22). The spend/dependency view carries **no match identity** and is safe for anon by column scope; every page that can expose a private match is operator-only.
- The Space holds no gateway key and no owner token (cycle 1 §10.5.1, §20); public pages read only the lake-root catalog's non-private dimensions and the observability object.

## 7. Observability: lake-object OTel (owner decision)

"Instrument once, observe anywhere": the run emits OTel signals; the backend is swappable. Cycle 2's backend is a lake object; the later sub-cycle swaps in the Collector with no change to the run's logic (§12).

### 7.1 Collection and the run-code boundary

- Add `opentelemetry-sdk`. A tracer and meter provider install at process start behind one factory in `telemetry.py`; a test or local run installs a no-op or in-memory provider.
- The run opens a span per step (`step_span`) with ADR-009 attributes (`telemetry.py` `SPAN_NAMES`/`ATTRIBUTE_NAMES`). **New attributes — `layer`, `state`, `execution_context`, `flavour` — do not exist yet** (Reviewer B OBS-B07); cycle 2 adds them to the existing `step_span` calls. This is an **additive attribute change**, not a change to the run's control flow.
- The **dbt step retains `manifest.json`** this cycle (owner decision (b), §7.4). `runs/dbt.py` does not keep it today; cycle 2 writes it beside the step's output so the dependency view can read the dbt model DAG. Retaining a file is additive, not a logic change.
- A span processor / metric reader writes to the lake exporter (§7.2). With no SDK, the API calls stay a no-op.
- **Trace-to-log correlation.** With the SDK installed, the current span's `trace_id`/`span_id` are injected into every JSON log record — an **additive** field change at the log handler. No consumer pins the log field set. With no SDK, the fields are absent.

### 7.2 The run-written lake object (run-side; Reviewer B OBS-B08)

- Written by the **run**, so it follows cycle 1 §8.3's single-writer rule for a run-written object (the "run writes a small object at each step, outside the catalog" pattern, cycle 1 §17.1). It is not a DuckLake table and not operator-side. **The Space never writes it:** the observability writer is invoked **only from the run entrypoint**, never from the web app, so the Space — a web-app process with no run — has no path to it. A run writes it whether it is local or an HF Job; the Space and the Job both use `hf://` roots (`web/main.py` `_mode`), so the signal is the **process role (the run)**, never the storage root — keying on `hf://` would wrongly block the Job's own writes (Reviewer B OBS-B23, OBS-B32). **Author's call, builder confirms** the storage (a Parquet/JSON object family under the install's storage, keyed by `run_id`).
- One row **per run step (span)**. Columns (author's call, builder finalizes): `run_id`, `task` (span name), `parent_task` (the containment edge), `layer`, `started_at`, `duration_seconds`, `row_count`, `state` (ok / failed / skipped), `visibility` (public / private / null), plus run-level `execution_context`, `flavour`, `cost_usd` and `cost_error` on the run row.
- **Visibility rule (Reviewer A D2-SPEC-07, Reviewer B OBS-B22):** a step's `visibility` is `private` if **any** match the step touched is private, else `public`. A **module-level** step (list/write/commit) carries no `match_key`; it is `private` if its module is a **pining** module — any `*_pining` module can serve private entries (cycle 1 §9; the four pining modules mix public and private entries) — and `public` for a public direct module. A run-level step with no module or match is `null`. Per §6.4, `null` and `private` are never shown to anon, so a mixed-pining-module step cannot leak.
- Run history stays the primary Ops data source (cycle 1 §21); this object **adds** the OTel cost and dependency view.

### 7.3 Spend model (owner decision; Reviewer B OBS-B04, B05, B16)

- **Source of context and flavour.** `execution_context` ∈ {`local`, `hf_job`}, and `flavour` (HF Job only), are read at run start from the run's environment: an HF Job sets them from its Job spec (the flavour the owner launched it with, surfaced as an env var); a local run sets `execution_context = local` and no flavour. The builder names the exact env keys in the plan. The public Space never runs ingestion (cycle 1 §17.1); a Space "Run now" triggers an HF Job, which reports `hf_job`.
- `cost_usd = 0` when `execution_context = local`.
- `cost_usd = flavour_rate(flavour) × duration_hours` when `execution_context = hf_job`. The rate table (cycle 1 §22.2: `cpu-basic` $0.01/h, `cpu-upgrade` $0.03/h, dated 2026-10-26) is one constant with its date.
- **Unknown or missing flavour (B16):** the ingestion run still **succeeds** (the data is what matters); the cost row records `cost_usd = null` with a visible `cost_error` flag, never a silent 0 and never a failed run.
- **Cost is a run-level figure**, attributed once to the run, not summed over nested spans (the `lakehouse.run` span encloses the steps — summing double-counts, B05).
- **Duration bounds (B05):** the measured duration is the `lakehouse.run` span, span start to end. It does **not** include the lease acquisition (which is outside `lakehouse.run`, `run.py`) or any HF-Job scheduling time before the process starts. The cost is therefore an estimate over measured **process** duration; the UI states this bound, so a reader does not mistake it for billed wall-clock.
- The UI shows cost as a **dated estimate**: "est. $X at `<rate-table-date>` rates (process time)", naming the rate table as the provenance. It is a static-table estimate, not live HF billing (owner decision §13).

### 7.4 The dependency view (owner decision (b); Reviewer A D2-SPEC-06, Reviewer B OBS-B06, B15)

- **Run-step edges** are the span parent/child (containment/sequence), in the order the run executes (cycle 1 §17.2): **bronze (per provider) → resolution → dbt**. These edges are containment, not data dependencies, and the view labels them so.
- **The dbt step** is one run span, but cycle 2 **keeps `manifest.json`** (§7.1) and reads its model DAG, so the view expands the dbt step into its real data-dependency edges: the **silver staging models → the gold mart models** (owner decision (b)). The silver→gold edge that G4 implies is shown this cycle, not deferred.
- The view shows, per run, each run step with its layer, parent, duration, rows, state and the run's cost, and inside the dbt step the silver→gold model graph — enough to see which layer, step or model dominated time, and where spend went.

## 8. Interactive ingestion config (owner decision)

Cycle 2 wires the config actions through the services layer and the guarded action dispatch (§6.2). No new business logic in the pages.

- **Modules page:** an active/inactive switch per module (writes the config under the config lock); "Run now" starts a background run (local: the CLI run path; Space: triggers an HF Job) and does not block the page. Pining token status shows present/missing, never the value.
- **Filter editor:** edit and save the clauses of one module; "Preview" reads the live source, lists its tournaments with counts and the selected marks, and never ingests. Draft vs saved side by side, what a save would change ("+34 matches would be added · 0 would stop updating"), and the StatsBomb 360 affordance (cycle 1 §19.3, §29.13). Private-tournament rows are operator-only (§6).
- **Schedule page (B19 = (a)):** save, pause and resume the schedule, and "Run now", through `services/schedule.py`; none needs a secret value (cycle 1 §18).
- **Review queue page (B19 = (a)):** accept-recommended, undo and decide (must-link / cannot-link), through `services/review.py`; a hand-checked decision grows the truth set (cycle 1 §15.5).
- Every change and "Run now" passes the write check in the action dispatch (§6.2). A narrowed filter keeps its data (cycle 1 §8.1).

## 9. The log-viewer page (owner decision)

- Authenticated-only. Shows the most recent **N** records (author's call: N configurable, default 200), newest first; filters by **level** (the JSON `level` field) and **free text** over `msg` (and the logger name).
- **Masking (Reviewer B OBS-B02, B17).** Cycle 1 masks the message through the logging record factory, but extra fields and exception text are masked only by the stdout stream wrapper. The collected copy must have **every field masked** — the collection path applies the stdout masker to the whole record, not only the message. A test puts a secret in an extra field and in a traceback and asserts neither reaches the object or the page.
- Source: the collected log copy — **author's call, builder confirms** the sink (per-run log objects or one rolling object, default and shape in §14), in the §7.2 run-written object family. It does not change how logs are emitted.
- Each record carries the span `trace_id`/`span_id` when the SDK is on (§7.1), and its `visibility`, so an operator pivots to the task and a later public view can redact private-match records with no page rewrite (§12).

## 10. Repository and layout

- New code under `v2/src/lakehouse/`: service methods in `services/` (the visibility-scoped read path and the guarded action dispatch), the SDK factory and exporter in `telemetry.py` (and `telemetry_export.py` if large), `manifest.json` retention in `runs/dbt.py`, new pages under `ui/pages/`, the socket.io/connection and per-caller-content changes in `ui/` and `web/`.
- New ADRs in `v2/docs/adrs/`: the authorization model (three enforcement points + the visibility-scoped read path), and the run-written observability object, its exporter and the dbt-DAG read. ADR-009 is extended with the new attributes, not replaced.
- dbt: if the observability object is modelled for reading, add a staging/mart model and document every column (cycle 1 §12.8).

## 11. Verification items (named tests and tiers)

New markers (`integration`) are registered in `pyproject.toml`. The plan pins exact names.

1. `test_anon_public_page_no_private` (unit) — an anon `public` page returns 200, shows no private-match row.
2. `test_anon_socket_action_refused` (integration) — an anonymous socket.io `on_action` is refused and changes nothing.
3. `test_cross_origin_socket_refused` (integration) — a socket.io connection from an origin other than the install's own is refused.
4. `test_action_requires_operator_and_write` (unit) — every action handler refuses without sign-in and without the write check; an unchecked handler cannot be registered.
5. `test_public_read_scope` (unit) — a `public` read returns only public rows; an operator read returns all; a page cannot widen scope.
6. `test_module_toggle_persists` (unit) — a toggle persists through the config lock; the next run sees it.
7. `test_preview_no_write_private_operator_only` (unit) — Preview lists counts, writes no bronze; private tournaments only for an operator.
8. `test_run_now_nonblocking` (unit) — "Run now" starts a run (local path or HF-Job trigger); the page stays responsive.
9. `test_task_rows_attributes` (unit) — with the SDK on, one row per step with `parent_task`, `layer`, `duration`, `rows`, `state`, `visibility`; no SDK → no-op.
10. `test_spend_contexts` (unit) — `local` → 0; `hf_job` → `flavour_rate × duration`; unknown/missing flavour **and a missing `execution_context`** → run succeeds, `cost_usd = null` + `cost_error`, never a silent 0 (a missing context never defaults to `local`); cost attributed once per run.
11. `test_dependency_topology` (unit) — run-step edges are `bronze → resolution → dbt` (containment, labelled so); the dbt step expands to the silver→gold model edges read from `manifest.json`.
12. `test_log_viewer_filters_and_masking` (unit) — shows the most recent N, filters by level and text, shows no secret in a message, an extra field, or a traceback.
13. `test_log_record_trace_and_visibility` (unit) — with the SDK on, records carry `trace_id`/`span_id`; every record carries `visibility`.
14. `test_observability_writer_run_only` (unit) — the web app (Space) has no code path that writes the observability object; a run writes it whether local or an HF Job (both `hf://`). The signal is the process role (the run), not the storage root.
15. `test_clean_extension_exporter_swap` (unit) — a second (in-memory) exporter swaps in with no change to the run; the log viewer's data source is behind one interface.

## 12. Clean-extension invariants (owner decision)

- **Observability backend is swappable.** Instrumentation is OTel API only; the lake exporter is a plug-in behind the SDK's exporter interface. Adding the Collector (OTLP/HTTP → S3 or enterprise) is config plus a new exporter, no change to the run's logic.
- **The log view extends to public.** Every record carries its `visibility`; a later public view passes a `public` scope that redacts private-match records, no change to the authenticated page.
- **Spend extends to new task kinds.** The cost model keys on `execution_context` and `flavour`, not the task kind, so later processing and ML tasks price by the same rule.

## 13. Owner decisions (2026-10-28 brainstorm and reviews)

- Cycle 2 = interactive ingestion config + observability (spend per run, inter-layer dependencies) + a log viewer; scope small, clean extension required.
- The monitoring UI is strictly for spend-on-tasks and inter-layer dependencies, as v1's operations UI does; adapted for v2's ingestion-only tasks and free-vs-HF-Job spend.
- Authorization: public read-only on monitoring and public config viewing; authenticated for every change and private-match data; the §6.3 map is approved.
- OTel depth: the lake-object path this cycle; the Collector deferred, extending cleanly later.
- Spend: context-derived (0 local, flavour×duration HF Job), a dated estimate over process time, not live HF billing.
- Log viewer: authenticated-only this cycle; a public private-scrubbed view later.
- **Cycle 2 owns the functional Ops actions** because cycle 1 does not need them (CLI is cycle 1's operator path); cycle-1 spec §29.17, aligning cycle-1 §19.3 and plan Task 28.
- **B15 = (b):** show the silver→gold dbt edges this cycle by keeping and parsing `manifest.json` (§7.4); not deferred.
- **B19 = (a):** cycle 2 builds **every** functional Ops action of cycle-1 §19.3 — Modules and Filter (as before) **and** Schedule-save and the Review queue (accept-recommended / undo / decide) (§2 G1, §8); cycle-1 §29.17 is read to cover all of §19.3, not only the first four.
- `/mad-scientist-skills:observability-audit` was run; result §13.1.

### 13.1 observability-audit result (2026-10-28)

Run as a Phase-0 anti-pattern scan plus the planning checklist. v2's logging is **clean**: JSON lines, a namespaced (`lakehouse`) logger, masking at the stdout stream, no silent exception swallow (ADR-002 and ruff `BLE001` hold; the `except BaseException` blocks re-raise after cleanup). The skill's worked-example blocker (2026-04-12 warm-tier WARNING-swallow) is a **v1** incident v2 prevents. Gaps it helped surface: trace-to-log correlation (§7.1) and record-level masking of extra fields (§9). Applicable: the standard / OTel-native tier, `run_results.json`/`manifest.json` parsing, the dual-export / personal-tier pattern. Out of scope: the enterprise tiers and the SLO/alerting/dashboard/health-probe phases. **Skill feedback:** it has no personal / local lake-object (DuckDB/Parquet) backend tier, which is v2's model.

## 14. Open author's calls for review

- The storage shape of the observability and log objects (§7.2, §9): per-run objects versus one rolling object, and the default N (200).
- Whether spend and dependencies are two pages or two sections of one monitoring page.
- The exact `step_span` attributes added for `layer`/`state`/`execution_context`/`flavour`, the env keys that carry context and flavour (§7.3), and whether `state="skipped"` needs a span the run does not emit today (OBS-B07).

## 15. Review findings and resolutions

Round 1: A — 2 SHOULD, 3 CONSIDER; B — 3 BLOCKING, 6 SHOULD, 5 CONSIDER. Round 2: A — all round-1 resolved, 1 SHOULD; B — all BLOCKING resolved, 4 PARTIAL + 1 SHOULD.

| Finding | Round | Resolution |
|---|---|---|
| B OBS-B01 socket.io bypass | 1 Blocking | §6.2 three enforcement points + explicit origin; tests 2–4. |
| B OBS-B02 log copy not fully masked | 1 Blocking | §9 masks every field; test 12. |
| B OBS-B03 cycle-1 actions unrecorded | 1 Blocking | cycle-1 §29.17 owner record; §1 premise. |
| B OBS-B04 context/flavour source; missing flavour | 1 SHOULD / 2 PARTIAL | §7.3 names the env source; missing flavour → `cost_error`, run succeeds. |
| B OBS-B05 nested-span double-count; job time | 1 SHOULD / 2 PARTIAL | §7.3 run-level cost; duration bound stated (process time), lease/pre-process gap named. |
| B OBS-B06 / A D2-SPEC-06 layer order | 1 SHOULD / 2 PARTIAL | §7.4 `bronze → resolution → dbt` (containment); test 11. |
| B OBS-B07 missing attrs; "no run code changes" | 1 SHOULD | §7.1 additive span attributes; §4 reworded. |
| B OBS-B08 run-side; Space export | 1 SHOULD / 2 PARTIAL | §7.2 run-side in the body; Space refuses the write; test 14. |
| B OBS-B09 access map; `match_key` visibility | 1 SHOULD | §6.3 completed; §7.2 `visibility` column. |
| B OBS-B15 silver→gold edge deferral | 2 SHOULD | owner (b): shown this cycle via `manifest.json` (§7.4, §13); §12/§14 agree. |
| A D2-SPEC-01 CSRF fail-closed | 1 SHOULD | superseded by B01: action-layer enforcement (§6.2). |
| A D2-SPEC-02 cost unlabelled | 1 SHOULD | §7.3 dated estimate + provenance. |
| A D2-SPEC-07 multi-match step visibility | 2 CONSIDER | §7.2 any-private → private. |
| B OBS-B16 unknown-flavour outcome | 2 CONSIDER | §7.3 run succeeds; cost flagged; test 10. |
| B OBS-B17 marker/masking/origin/pointers | 2 CONSIDER | §5 register markers; §9 masking is the record factory's; §6.2 local origin; cycle-1 §19.3/Task 28 pointers added. |
| B OBS-B13 anon load / single replica | 1/2 CONSIDER | reads take no lock; the plan sizes anon read load against the single Space replica. |
| cross-refs (A D2-SPEC-03 / B OBS-B10, B17) | 1/2 | section numbers throughout; this table's pointers corrected. |
| B OBS-B18 duration wording; `cost_error` in column list | 3 CONSIDER | §7.3 "span start to end"; §7.2 adds `cost_error` to the columns. |
| B OBS-B19 schedule/review actions in no cycle | plan 1 | owner (a): §2 G1, §8 build them; cycle-1 §29.17/§19.3 cover all §19.3 actions. |
| B OBS-B20 telemetry/logs not wired into the real run | plan 1 | plan Task 10 wires the CLI/HF-Job run + an e2e test. |
| B OBS-B22 pining modules mix public/private | plan 2 | §7.2: a `*_pining` module-level step is `private`; test on a mixed pining module. |
| B OBS-B24 missing context / custom env keys | plan 2 | §7.3 + plan Task 3: a missing `execution_context` → `cost_error`, never `local`; the owner sets the job env keys unconditionally + an in-Job check. |
| B OBS-B32 writer refusal signal (Space vs Job both `hf://`) | plan 2 | §7.2 + §11 test 14: the writer is run-only (web app has no path); signal is the process role, not `hf://`. |
| B OBS-B33 Task 8 omitted `runs.py` Run now | plan 2 CONSIDER | plan Task 8 adds `ui/pages/runs.py`. |
