# Lakehouse v2, cycle 2: observability and operations UI — implementation plan

> **For agentic workers:** REQUIRED SUB-SKILL: use superpowers:executing-plans to implement this plan task by task (native; no subagents — owner rule). Steps use checkbox (`- [ ]`) syntax. **Commit discipline (owner rule): no per-task commits. Each task ends tests-green; the single cycle commit is the final Gate task, behind an explicit owner-approval gate.**

**Goal:** make the Ops UI run and configure ingestion interactively (Modules, Filter, Schedule, Review), observe spend-per-run and inter-layer dependencies via a lake-object OTel backend wired into the real run, and view collected logs — with a public/authenticated split enforced at the Taipy action layer.

**Architecture:** add the OpenTelemetry SDK behind the existing API seam; a custom exporter writes one row per run step to a run-written lake object; the Ops pages read it. Enforcement moves from Flask routes to three points (socket.io connection, a guarded action dispatch, per-caller content). The dbt step retains `manifest.json` so the dependency view shows the silver→gold model DAG. The providers and the log collector are wired into the real CLI/HF-Job run, not only into tests.

**Tech Stack:** Python 3.12, `opentelemetry-api` (present) + `opentelemetry-sdk` (new), Taipy GUI 4.1.1 + Flask + flask-socketio 5.7.0, DuckDB/DuckLake, dbt, pyarrow, pytest.

**Spec:** `v2/docs/specs/2026-10-28-v2-observability-ops-design.md` (owner-approved 2026-10-28, A/B-approved round 3, amended after plan review round 1 with owner decision B19 = (a)). Read it with this plan.

> **AMENDED by the live-web-enforcement addendum** (`docs/specs/2026-10-28-v2-live-web-enforcement-design.md` + `docs/plans/2026-10-28-v2-live-web-enforcement-plan.md`, A/B-approved). The addendum and this plan land in the **same single commit**. It supersedes this plan's **Task 7** anon-admitting socket (`build_socketio` + `test_socket_authz.py`) and `render_for` anon gating, and this plan's **Task 9** anon-Monitoring handling: the final model is an operators-only Taipy app under `/ops` plus a separate static public page at `/`. Do not read Task 7/9's socket/`render_for` steps in isolation — see the addendum's Reconciliation section for the exact removals/re-homes (Reviewer B LWE-B15).

## Global Constraints

- Python 3.12; the v2 lockfile; add only `opentelemetry-sdk` (no second telemetry path). (spec §5)
- The run calls the OTel **API** only; the SDK + exporter + the dbt `manifest.json` retention are additive; **no change to the run's control flow**. (§5, §7.1, §7.4)
- Logs stay JSON lines on stdout, masked; the collected copy masks **every** field, not only the message. (§5, §9)
- Run-written objects: one writer per family under one lock (cycle 1 §8.3); the observability object is **run-side**; the run's writer **refuses on a Space install** (the refusal is in the writer, not a storage handle). (§7.2)
- No anonymous change of any kind, via HTTP route **or** socket.io action; no private-match data (names, dates, tournaments, run errors) to anon; a `null` or `private` visibility is never anon-visible (fail-safe). (§4, §6)
- Cost is never a silent 0: unknown/missing flavour → `cost_usd=null` + `cost_error`, the run still succeeds. (§4, §7.3)
- New pytest markers (`integration`) registered in `pyproject.toml` with their tests (`--strict-markers`). (§5)
- Owner rules: one feature branch off `main` (no worktrees, no subagents); **one commit for the whole cycle** at the final Gate task with an explicit approval gate (no micro-commits); STE prose. The v2 check set + CI and the `live`/`heavy`/`hf`/`bench`/OS/`integration` tiers gate completion.

## Review Focus

Five input classes the spec implies; each pinned to a named test.

1. A cross-origin socket.io connection or an anonymous `on_action` slipping a write past the route checks (Task 7 `test_cross_origin_socket_refused`, `test_anon_socket_action_refused`; Task 6 `test_action_requires_operator_and_write`; Task 8 `test_no_page_action_bypasses_dispatch`).
2. A secret in an exception traceback or an extra log field reaching the collected object and the viewer (Task 5 `test_log_viewer_filters_and_masking`).
3. A private-match name in a Runs-page error, or a private-module step row, reaching anon (Task 9 `test_operator_only_pages`; Task 2 `test_module_step_visibility`; Task 6 `test_public_read_scope`).
4. An HF Job whose flavour env key is absent/unknown silently pricing the run at 0 (Task 3 `test_spend_contexts`).
5. A run that skipped dbt or a missing `manifest.json` breaking the dependency view (Task 4 `test_dependency_topology_missing_manifest`).

---

### Task 0: Branch precondition

**Files:** none (git only).

- [ ] **Step 1: Verify cycle 1 has landed on `main`** (`git log --oneline main` shows the cycle-1 commit; `git status` is clean). If cycle 1 is not yet committed/merged, **stop and ask the owner** — cycle-2 edits must not mix into cycle 1's commit (the current tree is `feat/v2-ingestion` with `?? v2/`, Reviewer B OBS-B25).
- [ ] **Step 2: Create the cycle-2 branch off `main`** (`git switch -c feat/v2-observability-ops main`). One branch for the cycle; no worktree.

### Task 1: OTel SDK, provider factory, and the new span attributes

**Files:** Modify `v2/src/lakehouse/telemetry.py`, `v2/src/lakehouse/runs/run.py`, `v2/pyproject.toml` + `uv.lock`. Test `v2/tests/unit/test_telemetry.py`.

**Interfaces — Produces:** `install_providers(mode: Literal["off","memory","export"], *, exporter=None) -> TelemetryProviders`; attribute names `lakehouse.layer`, `lakehouse.state`, `lakehouse.execution_context`, `lakehouse.flavour` in `ATTRIBUTE_NAMES`; each `step_span` sets `layer` (bronze/silver/gold/resolution; the dbt step's layer is `"dbt"`) and `state` (ok/failed/skipped). A **skipped** step emits a zero-duration span so `state="skipped"` has a carrier (spec §7.1, §14).

- [ ] **Step 1: Write the failing test** — `test_providers_off_is_noop`; `test_providers_memory_records_span_with_attributes` (a step span carries `lakehouse.layer`, `lakehouse.state`); `test_skipped_step_emits_span` (a skipped step produces a zero-duration span with `state="skipped"`); `test_new_attribute_names_registered`.
- [ ] **Step 2:** Run `pytest tests/unit/test_telemetry.py -v`. Expected FAIL (`install_providers` undefined; attributes absent).
- [ ] **Step 3:** Add `opentelemetry-sdk`; implement `install_providers`; add the four attribute names; set `layer`/`state` on each `step_span`; emit the skipped-step span. No run control-flow change.
- [ ] **Step 4:** Run `pytest tests/unit/test_telemetry.py -v`. Expected PASS.
- [ ] **Step 5:** `pytest tests/unit/test_telemetry.py -q` green. No commit.

### Task 2: Observability object — schema, writer, exporter seam

**Files:** Create `v2/src/lakehouse/observability.py`, `v2/src/lakehouse/telemetry_export.py`. Test `v2/tests/unit/test_observability.py`.

**Interfaces — Consumes:** Task 1 providers/attributes. **Produces:** `TaskRow` (`run_id, task, parent_task, layer, started_at, duration_seconds, row_count, state, visibility`; run-level `execution_context, flavour, cost_usd, cost_error`); `ObservabilityWriter` (one implementation of the exporter **seam** `SpanSink`, so a second sink swaps in — Reviewer B OBS-B30); `write_task_rows(storage, run_id, rows)` / `read_task_rows(storage, run_id)`. The writer is **invoked only from the run entrypoint**, never the web app, so the Space (web-app, no run) has no path to it; a run writes whether local or an HF Job (both `hf://`) — the signal is the process role, not the storage root (OBS-B23, OBS-B32).

- [ ] **Step 1: Write the failing test** — `test_task_rows_attributes` (spec §11 item 9 — one row per step with `parent_task`, `layer`, `duration`, `rows`, `state`; no SDK → no-op); `test_module_step_visibility` (OBS-B22 / D2-SPEC-07: a module-level step for a `*_pining` module — mixed public/private entries, e.g. `idsse_pining` — is `private`; a public direct module is `public`; a run-level step with no module/match is `null`; a step whose match is private is `private`); `test_observability_writer_run_only` (spec test 14 — the web app has no code path, **direct or transitive**, that writes the object; a run writes it, local and an `hf://` HF Job, OBS-B32). Because `allow_indirect_imports` is true (OBS-B34), add a dedicated `import-linter` forbidden contract `lakehouse.web -> lakehouse.observability` (indirect included) and assert the writer is unreachable from the web entrypoint.
- [ ] **Step 2:** Run `pytest tests/unit/test_observability.py -v`. Expected FAIL.
- [ ] **Step 3:** Implement `TaskRow`, the `SpanSink` seam + `ObservabilityWriter`, `write_task_rows`/`read_task_rows`, the visibility rule, and the Space-install refusal.
- [ ] **Step 4:** Run; Expected PASS.
- [ ] **Step 5:** Task suite green. No commit.

### Task 3: Spend model

**Files:** Modify `v2/src/lakehouse/observability.py` (cost on the run row; the dated flavour-rate table), `v2/src/lakehouse/runs/run.py` (read `execution_context`/`flavour` from named env keys at run start). Test `v2/tests/unit/test_spend.py`.

**Interfaces — Produces:** `run_cost(execution_context, flavour, duration_hours) -> tuple[float|None, str|None]`; env keys `LAKEHOUSE_EXECUTION_CONTEXT` (`local`/`hf_job`) and `LAKEHOUSE_HF_FLAVOUR` (HF Job only), read at run start.

- [ ] **Step 1: Write the failing test** — `test_spend_contexts` (spec test 10): `local` → `(0.0, None)`; `hf_job`+`cpu-basic`+2h → `(0.02, None)`; unknown flavour → `(None, "unknown flavour …")`; missing flavour → `(None, …)`; **a missing `execution_context` → `(None, "missing execution_context …")`, never `local`** (no silent 0, OBS-B24); the run still succeeds on a cost error; cost attributed **once** per run (not summed over nested spans); duration = the `lakehouse.run` span, span start to end.
- [ ] **Step 2:** Run `pytest tests/unit/test_spend.py -v`. Expected FAIL.
- [ ] **Step 3:** Implement `run_cost` + the rate table (`cpu-basic`=0.01, `cpu-upgrade`=0.03 USD/h, dated `2026-10-26`); wire the env keys and the run-level attribution in `run.py`.
- [ ] **Step 4:** Run; Expected PASS. **Owner gate (unconditional, OBS-B24):** HF never sets these custom keys, so the owner must configure the scheduled HF Job to set `LAKEHOUSE_EXECUTION_CONTEXT=hf_job` and `LAKEHOUSE_HF_FLAVOUR=<flavour>`; the Job checks both are present at start and records `cost_error` (never a silent 0) if absent. State this in ADR-012 and surface it to the owner at deploy (it may require recreating or rescheduling the job).
- [ ] **Step 5:** Task suite green. No commit.

### Task 4: manifest.json retention and the dbt model DAG

**Files:** Modify `v2/src/lakehouse/runs/dbt.py` (retain `manifest.json`), `v2/src/lakehouse/observability.py` (read the DAG). Test `v2/tests/unit/test_dbt_dag.py`.

**Interfaces — Produces:** `dbt_model_dag(manifest_path) -> list[tuple[str,str]]` (silver→gold edges); `dependency_view(run_id) -> DependencyView` (run-step edges + the dbt sub-DAG).

- [ ] **Step 1: Write the failing test** — `test_dependency_topology` (spec test 11): run-step edges `bronze → resolution → dbt` (labelled containment); the dbt step expands to silver→gold edges from a fixture `manifest.json`. `test_dependency_topology_missing_manifest` (Review Focus 5): no dbt / absent manifest → the run-step edges with an empty dbt sub-DAG and a visible note, never a crash.
- [ ] **Step 2:** Run `pytest tests/unit/test_dbt_dag.py -v`. Expected FAIL.
- [ ] **Step 3:** Retain `manifest.json`; implement `dbt_model_dag`, `dependency_view` (order from cycle-1 §17.2; absent manifest → empty sub-DAG + note).
- [ ] **Step 4:** Run; Expected PASS.
- [ ] **Step 5:** Task suite green. No commit.

### Task 5: Log collection — trace correlation and full-field masking

**Files:** Modify `v2/src/lakehouse/logging.py`, `v2/src/lakehouse/observability.py`. Test `v2/tests/unit/test_log_collection.py`.

**Interfaces — Produces:** `collect_logs(storage, run_id)` handler; `read_logs(storage, run_id, limit, level=None, text=None) -> list[LogRecord]` (each carries `trace_id`, `span_id`, `visibility` — visibility from the span's `match_key` at log time).

- [ ] **Step 1: Write the failing test** — `test_log_viewer_filters_and_masking` (spec test 12): a secret in `msg`, an **extra field**, and an **exception traceback** — none reaches the object or `read_logs`; filters by level and text work. `test_log_record_trace_and_visibility` (spec test 13): SDK on → records carry `trace_id`/`span_id`; every record carries `visibility`.
- [ ] **Step 2:** Run `pytest tests/unit/test_log_collection.py -v`. Expected FAIL.
- [ ] **Step 3:** Inject the trace fields; apply the stdout masker to **every** record field (not only `msg`); implement `collect_logs`/`read_logs`.
- [ ] **Step 4:** Run; Expected PASS.
- [ ] **Step 5:** Task suite green. No commit.

### Task 6: Visibility-scoped reads and the guarded action dispatch

**Files:** Modify/create `v2/src/lakehouse/services/`. Test `v2/tests/unit/test_authz.py`.

**Interfaces — Produces:** `Scope` (`public`/`operator`); `scoped_reads(services, scope)` (a `public` read returns only `public` rows; `null`/`private` never); `dispatch(action_name, session, **kwargs)` raising `Unauthorized`; a registry such that an unchecked action name cannot be registered.

- [ ] **Step 1: Write the failing test** — `test_public_read_scope` (spec test 5): `public` → only `public` rows; `operator` → all; a page cannot widen scope. `test_action_requires_operator_and_write` (spec test 4): `dispatch` refuses without sign-in and without the write permission; an unregistered-check action raises at registration.
- [ ] **Step 2:** Run `pytest tests/unit/test_authz.py -v`. Expected FAIL.
- [ ] **Step 3:** Implement `Scope`, `scoped_reads`, `dispatch`, the fail-closed registry.
- [ ] **Step 4:** Run; Expected PASS.
- [ ] **Step 5:** Task suite green. No commit.

### Task 7: socket.io connection auth, origin lock, per-caller content

**Files:** Modify `v2/src/lakehouse/web/`, `v2/src/lakehouse/ui/shell.py`, `v2/pyproject.toml` (register the `integration` marker). Test `v2/tests/integration/test_socket_authz.py`, `v2/tests/unit/test_page_scope.py`.

**Interfaces — Consumes:** Task 6 `Scope`, `dispatch`. **Produces:** authenticated socket connection; `render_for(route, services, scope)` (per-caller content; replaces render-once-at-build).

- [ ] **Step 1: Register the `integration` marker in `pyproject.toml`, then write the failing test** — `test_anon_socket_action_refused` (spec test 2, `integration`); `test_cross_origin_socket_refused` (spec test 3, `integration`); `test_anon_public_page_no_private` (spec test 1). Marker registered first so collection does not error under `--strict-markers` (OBS-B29).
- [ ] **Step 2:** Run `pytest tests/integration/test_socket_authz.py tests/unit/test_page_scope.py -v`. Expected FAIL.
- [ ] **Step 3:** Implement the connection auth (session cookie on connect), the `cors_allowed_origins` lock (Space origin on HF, localhost locally), and per-caller `render_for`.
- [ ] **Step 4:** Run; Expected PASS.
- [ ] **Step 5:** Task suite green. No commit.

### Task 8: Interactive Ops actions — Modules, Filter, Schedule, Review (B19 = (a))

**Files:** Modify `v2/src/lakehouse/ui/pages/{modules,filters,runs,schedule,review}.py` (`runs.py` also has a "Run now", OBS-B33). Test `v2/tests/unit/test_config_actions.py`.

**Interfaces — Consumes:** Task 6 `dispatch`; services `RunService.trigger`, config set, filter preview/diff, `services/schedule.py`, `services/review.py`.

- [ ] **Step 1: Write the failing test** — `test_module_toggle_persists` (spec test 6); `test_preview_no_write_private_operator_only` (spec test 7); `test_run_now_nonblocking` (spec test 8); `test_schedule_save_through_dispatch` (save/pause/resume via `dispatch`); `test_review_decisions_through_dispatch` (accept-recommended / undo / decide via `dispatch`); `test_no_page_action_bypasses_dispatch` (OBS-B21 — enumerate every `on_action`/`on_change` name in every page; each routes through `dispatch`).
- [ ] **Step 2:** Run `pytest tests/unit/test_config_actions.py -v`. Expected FAIL.
- [ ] **Step 3:** Wire every page action (Modules active/Run now, Filter edit/save/Preview, Schedule save/pause/resume/Run now, Review accept/undo/decide) through `dispatch` + the services.
- [ ] **Step 4:** Run; Expected PASS.
- [ ] **Step 5:** Task suite green. No commit.

### Task 9: Monitoring pages (spend + dependency) and the log viewer

**Files:** Create `v2/src/lakehouse/ui/pages/monitoring.py`, `v2/src/lakehouse/ui/pages/logs.py`. Modify `v2/src/lakehouse/ui/shell.py` (register pages + scopes). Test `v2/tests/unit/test_monitoring_pages.py`.

**Interfaces — Consumes:** Task 2 `read_task_rows`, Task 4 `dependency_view`, Task 5 `read_logs`, Task 6 `Scope`.

- [ ] **Step 1: Write the failing test** — `test_monitoring_public_no_match_identity` (the spend/dependency view carries no match name/id, anon-safe); `test_operator_only_pages` (OBS-B31 — Runs, Review, Schedule and Settings refuse anon; Runs because its errors may carry private names); `test_log_viewer_authenticated_only_topN` (operator-only, most recent N newest-first). The spend figure shows the dated estimate "est. $X at 2026-10-26 rates (process time)".
- [ ] **Step 2:** Run `pytest tests/unit/test_monitoring_pages.py -v`. Expected FAIL.
- [ ] **Step 3:** Implement the monitoring page(s) (author's call §14: one page/two sections or two pages) and the authenticated log-viewer page, reading through the scoped services.
- [ ] **Step 4:** Run; Expected PASS.
- [ ] **Step 5:** Task suite green. No commit.

### Task 10: Wire telemetry and log collection into the real run (Reviewer B OBS-B20)

**Files:** Modify `v2/src/lakehouse/cli.py` (the `run`/`reparse` entrypoints) and the HF-Job entrypoint. Test `v2/tests/integration/test_run_observability_e2e.py`.

**Interfaces — Consumes:** Task 1 `install_providers`, Task 2 `ObservabilityWriter`, Task 5 `collect_logs`.

- [ ] **Step 1: Write the failing test** — `test_run_writes_observability_and_logs` (`integration`): run the CLI `run` on the offline fixture lake; `read_task_rows` returns one row per step with layer/state/cost; `read_logs` returns the run's masked records. Today nothing is wired, so a real run writes nothing (OBS-B20): the test fails.
- [ ] **Step 2:** Run `pytest tests/integration/test_run_observability_e2e.py -v`. Expected FAIL (empty rows/logs).
- [ ] **Step 3:** Wire `install_providers("export", exporter=ObservabilityWriter(...))` and `collect_logs` into the CLI `run`/`reparse` entrypoints and the HF-Job entrypoint **only** — the web app imports no writer path, so the Space has no way to write the object (OBS-B32). State in ADR-012 that the **Space installs the `off`/read path only** (it never runs ingestion, cycle 1 §17.1), so it never exports, and that the owner sets the Job env keys (Task 3 gate).
- [ ] **Step 4:** Run; Expected PASS.
- [ ] **Step 5:** Task suite green. No commit.

### Task 11: ADRs, dbt model, and the clean-extension test

**Files:** Create `v2/docs/adrs/ADR-011-authz-model.md`, `v2/docs/adrs/ADR-012-observability-object.md`. Modify `v2/docs/adrs/ADR-009-*.md` (the four new attributes); dbt staging/mart model + `_observability__sources.yml` if the object is modelled; document every column (cycle 1 §12.8). Test `v2/tests/unit/test_clean_extension.py`.

- [ ] **Step 1: Write the failing test** — `test_clean_extension_sink_swap` (spec test 15): a second (in-memory) `SpanSink` swaps in with no change to the run; the log viewer's data source is behind one interface.
- [ ] **Step 2:** Run `pytest tests/unit/test_clean_extension.py -v`. Expected PASS or FAIL — the seam is defined in Task 2, so this test **confirms** the swap; if it does not already pass, the seam is incomplete, fix it here (OBS-B30).
- [ ] **Step 3:** Write the ADRs (Nygard, STE); add the dbt model + column docs.
- [ ] **Step 4:** Run the ADR/dbt column-doc check (the cycle-1 `test_bronze_docs` pattern) if the object is modelled. Expected PASS.
- [ ] **Step 5:** Task suite green. No commit.

### Task 12: Full verification across tiers

- [ ] **Step 1:** Run the v2 check set with `pytest -m "not live and not hf and not bench and not integration"` (excludes the socket/e2e integration tests — A D2-PLAN-02); ruff, ruff format, pyright, lint-imports, pip-audit. Temp redirected to D: (C: is system-only). Expected: all green.
- [ ] **Step 2:** Run `pytest -m integration -v`. Expected PASS (socket auth + the run e2e).
- [ ] **Step 3: Spec coverage.** Confirm each spec §11 item has a named passing test and each §15 finding is exercised; list any skip with its reason; stop and ask the owner before the Gate if any §11 item is unmet.
- [ ] **Step 4:** Record the measured results in ADR-011/012. Tests green. No commit.

### Task 13: Gate — the single cycle commit (owner approval)

- [ ] **Step 1:** Show the owner `git status --short`, the file list, and `git diff --stat`.
- [ ] **Step 2:** Ask for an explicit yes for this one commit. Do not commit without it.
- [ ] **Step 3:** On the yes, make one commit (Conventional Commits, ending with the attribution lines). Do not push.

---

## Self-review (writing-plans checklist)

1. **Spec coverage:** §6 auth → Tasks 6, 7; §7.1 SDK/attrs → Task 1; §7.2 object/visibility/writer → Task 2; §7.3 spend → Task 3; §7.4 dbt DAG → Task 4; §9 logs → Task 5; §8 actions incl. Schedule+Review (B19=a) → Task 8; §4/§6.3 pages + log viewer → Task 9; the real-run wiring (G3–G5) → Task 10; §10 ADRs/dbt → Task 11; §11 tests distributed; §12 clean extension → Tasks 2/11; §13 owner decisions honored (one commit Task 13); branch precondition → Task 0. No gap.
2. **Step scan:** each step names its test (spec values) or a signature + a run command; no "handle edge cases" lines; no full bodies.
3. **Type consistency:** `TaskRow`, `SpanSink`, `ObservabilityWriter`, `read_task_rows`, `Scope`, `dispatch`, `render_for`, `dependency_view`, `run_cost`, `collect_logs`, `read_logs` used with one name/signature across tasks.
4. **Review Focus:** all five map to a named test (cross-origin/anon/bypass → Tasks 6/7/8; secret in traceback/extra → Task 5; private name/module to anon → Tasks 2/6/9; silent-0 flavour → Task 3; missing manifest → Task 4).
5. **Proportion:** signatures, test names and run commands, not bodies; shorter than the program it describes.
