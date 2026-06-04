# Design: AC-1 ghost-GK backend selection + provenance, period work-units, watchdog/timeout knobs

| Field | Value |
|---|---|
| **Date** | 2026-06-03 |
| **Status** | Draft (brainstorming output; design-locked with user 2026-06-03) |
| **Branch** | `feat/ac1-ghost-gk-backend-period-units` |
| **silly-kicks** | **floor 4.9.1 → 4.11.0** (bundled, decision (A) 2026-06-03). `add_ghost_gk` `kde_backend` set unchanged (all 5 values present; default `vectorized`). 4.10.0's serve-carrier fix shifts `ghost_gk_x/y` on ~0.4% of frames ⇒ **both goldens re-baseline (required)**. 4.11.0's only addition (`xCrossAttempt` / TF-17) ships **untrained** — **NOT consumed, no column added** (user 2026-06-03). |
| **ADR** | ADR-035 amendment (ghost-GK backend selection + provenance) + ADR-037 amendment (period work-units + watchdog/timeout) |
| **Supersedes** | the 4-item plan in `project_ghost_gk_backend_provenance_followup` memory (refined here after investigation) |
| **Review** | external review 2026-06-03 incorporated (behavioral-test gap, ValueError-not-SystemExit, mislabel fix, HF card, AI-gov, pure-helper extraction) |

## 1. Context & goals

PR #337 (ADR-039) shipped the ghost-GK enrichment with a **hard-coded** `kde_backend="fft-cic"`.
`fft-cic` is the fast-approx CIC backend (~95% mode-exact) adopted because the **exact** backends
(`scipy`/`vectorized`/`cpu-numba`) cannot finish a full tracking game inside the per-game watchdog
(ADR-035, ADR-037). The follow-up makes the backend **selectable** so batch jobs — not just local
one-offs — can opt into higher-accuracy ghost-GK, with the selection recorded per row for honest
consumer segmentation, and removes the cold-start friction that makes exact-backend runs impractical.

Four coupled goals, **design-locked with the user 2026-06-03**:

1. **Selectable ghost-GK backend** with a per-installation default, a per-run override, and per-row
   provenance — full flexibility for **batch jobs**, not only `submit_ac1_oneshot.py`.
2. **Period work-units for all tracking providers** (per-`(match, period)` like IDSSE already does) —
   smaller units parallelise better under the drain and give the per-game watchdog per-half headroom,
   which is what makes exact-backend runs fit.
3. **Preflight task timeout 300 → 600 s** — the analytics-env cold start busts 300 s (observed live
   2026-06-03).
4. **Per-game watchdog 1800 → 2700 s** + a run-level override knob — headroom for slower exact backends.

## 2. Guiding architecture principle — domain policy vs. infra policy

Two kinds of configuration are in play and they live in different places:

- **`kde_backend` is domain policy** — it specifies *how to compute* a unit of work. It rides **on the
  `WorkUnit`** (the work specification), persisted in the queue. The processor stays a pure function
  `process(unit) → result` with no ambient run-level state. This is the most hexagonal, most TDD-able,
  and gives batch jobs full flexibility (per-provider/per-match backends in one run are a preflight-only
  change later, with zero further plumbing).
- **`watchdog_budget_s` is infra/orchestration policy** — an operational safety ceiling. It does **not**
  belong on the domain `WorkUnit`; it rides on the **drain worker / run**. Polluting the work spec with a
  timeout would leak an infra concern into the domain.

This split is the whole design in one sentence: the queue carries *what + how-to-compute*; the worker
carries *operational ceilings*.

## 3. Backend selection — resolution hierarchy (Mechanism C + resolver)

The backend has a **resolution hierarchy**, resolved **once at the adapter boundary** (preflight or
oneshot), then stamped onto every `WorkUnit`:

```
1. Explicit per-run flag      --ghost-gk-backend <b>             (highest — one specific run)
2. Per-installation default   var.ghost_gk_backend_default       (this deployment's default)
3. Hardcoded fallback         "fft-cic"                          (lowest — current behaviour)
        │
        ▼  resolve_ghost_gk_backend(explicit, installation_default)   ← pure, validated, fail-loud
   WorkUnit(provider, match_id, period, kde_backend=<resolved>)
        │  enqueue → queue (single source of truth) → units_for_worker
        ▼
   SparkGameProcessor.process(unit) → _process_tracking_match(kde_backend=unit.kde_backend)
        │  → _make_action_context_udf → enrich_batch → _enrich_{tracking,sb360}_match → add_ghost_gk(kde_backend=…)
        ▼
   ghost_gk_method = unit.kde_backend         (provenance, correct by construction)
```

A **single pure resolver** owns precedence + allowlist validation; everything upstream (TF var, CLI flag)
feeds it, everything downstream consumes the resolved value.

### 3.1 The resolver (new module)

`src/analytics/action_context/ghost_gk_backend.py` (domain layer — stdlib only):

```python
GHOST_GK_KDE_BACKENDS: frozenset[str] = frozenset(
    {"scipy", "vectorized", "cpu-numba", "fft", "fft-cic"}
)
DEFAULT_GHOST_GK_BACKEND = "fft-cic"

def resolve_ghost_gk_backend(explicit: str | None, installation_default: str | None) -> str:
    """Resolve the ghost-GK KDE backend by precedence: explicit > installation default > fallback.

    Empty string and None are treated as "unset" at each level (Databricks job-parameter
    substitution yields "" for an unset {{job.parameters.*}}). Fail loud on an unknown value.
    """
    for candidate in (explicit, installation_default, DEFAULT_GHOST_GK_BACKEND):
        val = candidate.strip() if candidate and candidate.strip() else None
        if val is None:
            continue
        if val not in GHOST_GK_KDE_BACKENDS:
            raise SystemExit(
                f"Unknown ghost-GK backend {val!r}. Valid: {sorted(GHOST_GK_KDE_BACKENDS)}"
            )
        return val
    return DEFAULT_GHOST_GK_BACKEND
```

This function is the only place that knows the allowlist or the precedence — trivially TDD-able with a
truth table. The `installation_default` is read from an env var (`AC1_GHOST_GK_BACKEND`) at the boundary,
which Terraform populates from `var.ghost_gk_backend_default` (see §6).

**Domain-purity (review #5):** the resolver raises **`ValueError`** on an unknown value — NOT `SystemExit`.
`SystemExit` is a CLI/process concern; raising it from a pure domain function is the same infra-leaking-into-domain
smell §2 warns against, and would kill the process if any non-CLI caller (the future per-provider backend map; a
test harness) passes a bad value. The CLI boundary (`main_preflight` / `main`) catches the `ValueError` and
re-raises `SystemExit(...)` so operator fail-loud behaviour is unchanged.

**Belt-and-braces (review #8):** `WorkUnit.__post_init__` validates `kde_backend` against
`GHOST_GK_KDE_BACKENDS` (both are domain) so a value that bypasses the resolver (a direct
`WorkUnit(kde_backend="typo")`) is rejected before it enters the queue, rather than failing deep in
silly-kicks. `__post_init__` on a frozen dataclass may only *read* fields (raise on invalid) — no mutation.

### 3.2 Why per-unit (Mechanism C), not run-level config on the processor

- **Single source of truth** — `units_for_worker()` returns *fully-specified* units; the drain worker
  needs no second channel (job-param → task-value) to learn the backend. The queue already crosses the
  preflight→drain task boundary; the backend rides it for free.
- **Pure core** — `process(WorkUnit(kde_backend="scipy"))` is directly testable; no mocking of Databricks
  job parameters / task values.
- **Provenance alignment** — `ghost_gk_method` *is* `unit.kde_backend`; they validate each other in e2e.
- **Future flexibility (the stated goal)** — heterogeneous backends in one run (e.g. skillcorner on
  `cpu-numba`, gradientsports on `fft-cic`) become a preflight-only stamping change; the seam already
  supports it.
- **Low-risk migration** — the queue is a `run_id`-keyed Delta table; `ALTER ADD COLUMN kde_backend STRING`
  (NULL → `fft-cic` on read) carries no historical-data semantics.

## 4. New column: `ghost_gk_method` (provenance)

| Column | Type | Producer | Notes |
|---|---|---|---|
| `ghost_gk_method` | STRING | enrich path | the resolved `kde_backend` per row: one of `{scipy, vectorized, cpu-numba, fft, fft-cic}` on tracking/SB360 rows; **NULL** on event-only rows (no ghost-GK) |

`ghost_gk_method` scopes **only** to the `ghost_gk_*` columns (x/y/spread). It is **orthogonal** to
`pitch_control_method` (PR #337), which governs the pitch-control-derived metrics (OBSO/PAUSA/gk_influence).
Justification for a dedicated column (vs. inferring from `data_source`): the backend is a **run-time choice**,
not inferable from any persisted field — a pure Hyrum's-law case. Mart consumers segment on it; re-running a
provider with an exact backend OVERWRITES the `fft-cic` `ghost_gk_*` values with different numbers (95%
mode-exact; multi-metre flips on bimodal grids), so goldens stay on `fft-cic` and consumers must be able to
tell which backend produced a given row.

Column count: `RESULT_COLUMNS` / `ACTION_CONTEXT_DDL` 110 → **111**.

## 5. Period work-units for all tracking providers

IDSSE already enqueues per-`(match, period)` units; the other three tracking providers
(`metrica`, `skillcorner`, `gradientsports`) currently enqueue whole-match units. We make them all
per-period. **The processing + write path already supports this** — no correctness work is needed there:

- `_process_tracking_match` already takes `period_filter` and, when it is set, writes with
  `replaceWhere("match_id = '…' AND period_id = {period_filter}")` (action_context.py:1372-1375), and
  `enrich_batch` already filters actions to the period (pipeline.py:220). So two per-period units of the
  same match write **disjoint** Delta partitions — no double-write, no collision. **This was the one
  "open correctness item" in the memory; investigation + external review confirm it is already handled.**
  To make the invariant *testable* (review #4 — the harness has no local Spark/Delta), the `replaceWhere`
  predicate is extracted into a pure helper `_period_replace_where(match_id, period_filter) -> str` that a
  unit test asserts includes `period_id` iff `period_filter` is set.
- Whole-match `spadl_actions` is still loaded (no period filter) so `add_game_state` running-score stays
  correct across halves. **This whole-match-SPADL path is provider-agnostic** (action_context.py:1212-1218,
  filtered only by `match_id_native + data_source`) — IDSSE merely already exercises it per-period in
  production; it is **not** an IDSSE-gated "template" (review verified). Out-of-period actions simply don't
  link to frames.

The only change is **enqueue-side**: replace `_find_tracking_new_ids` (returns `list[str]`) with
`_find_tracking_new_period_pairs` (returns `list[tuple[str, int]]`), mirroring
`_find_idsse_new_period_pairs` (action_context.py:477-514): `SELECT DISTINCT match_id, period FROM
bronze.{provider}_tracking`, anti-join the results table on `(match_id, period_id)`, emit
`WorkUnit(provider, match_id, period, kde_backend)`. All three non-IDSSE tracking tables already carry a
`period` column.

The per-game watchdog automatically becomes **per-half** (it wraps each `processor.process(unit)` call;
a unit is now a half). No watchdog code change beyond the constant bump (§7).

**`_TIER_COST_S` note (surfaced, not changed):** `drain.py:19` estimates `tracking = 1800.0` for LPT
worker load-balancing. With per-half units the true cost is ~half, but the LPT bin-packer only needs
**rank order** (tracking ≫ statsbomb ≫ event_only), which still holds. Left unchanged per the handoff;
flagged here so a future tuning pass knows it is now a per-half estimate.

## 6. Terraform — per-installation default + run override + drain knob

```hcl
variable "ghost_gk_backend_default" {
  type    = string
  default = "fft-cic"   # an installation overrides this (e.g. "cpu-numba") for always-accurate ghost-GK
}
variable "watchdog_budget_s" {
  type    = string
  default = ""          # empty → in-code default WATCHDOG_BUDGET_S (2700)
}

# Job-level parameters (overridable at run-now via job_parameters JSON):
parameter { name = "ghost_gk_backend"  default = var.ghost_gk_backend_default }
parameter { name = "watchdog_budget_s" default = var.watchdog_budget_s }
```

Wiring:
- **preflight_action_context** task gains
  `"--ghost-gk-backend", "{{job.parameters.ghost_gk_backend}}"` — the preflight resolves it (explicit
  flag > `AC1_GHOST_GK_BACKEND` env > `fft-cic`) and stamps every `WorkUnit`. (The installation default
  reaches the preflight as the job-parameter *default* `var.ghost_gk_backend_default`; the env var is the
  belt-and-braces path for non-job entry points.)
- **compute_action_context** (drain) task gains
  `"--watchdog-budget-s", "{{job.parameters.watchdog_budget_s}}"` — the drain worker passes it to
  `drain_worker(budget_s=…)`. The drain needs **no** backend arg — the backend rides the queue.
- The 5 preflight tasks' `timeout_seconds` 300 → 600 (§goal 3).

Defaulting `var.ghost_gk_backend_default = "fft-cic"` means **zero behaviour change** until an
installation opts in.

## 7. Watchdog / timeout knobs

- `WATCHDOG_BUDGET_S = 1800 → 2700` in `src/analytics/action_context/drain.py:16` (the default `budget_s`
  applied per unit at drain.py:131/147).
- `main_drain_worker` gains `--watchdog-budget-s` (str, default `None`/`""` → `WATCHDOG_BUDGET_S`), passed
  as `drain_worker(..., budget_s=…)`. This is the **drain-path** override.
- **Oneshot/for-each path has no in-process watchdog** (`main()` calls `_process_tracking_match`
  directly, bounded only by the Databricks task timeout). So the oneshot escape hatch for a slow
  exact-backend run is a `--timeout-seconds` arg on `submit_ac1_oneshot.py` that sets the submitted
  `jobs.SubmitTask(timeout_seconds=…)` — **not** an in-process watchdog (there is nothing to override
  there).

## 8. Entry-point coverage (both paths)

| Entry point (pyproject) | Function | Backend arg | Watchdog/timeout |
|---|---|---|---|
| `preflight_action_context` | `main_preflight` | `--ghost-gk-backend` → resolve → stamp units | — |
| `compute_action_context_drain_worker` | `main_drain_worker` | (rides queue per-unit) | `--watchdog-budget-s` → `drain_worker(budget_s)` |
| `compute_action_context` (for-each / oneshot) | `main` | `--ghost-gk-backend` → resolve → `_process_tracking_match(kde_backend)` | task-level `timeout_seconds` (set by submit script) |
| `submit_ac1_oneshot.py` (operator) | — | `--ghost-gk-backend` → wheel param | `--timeout-seconds` → `SubmitTask(timeout_seconds)` |

## 9. Threading `kde_backend` through the core

`kde_backend: str` (no default at the call sites that must pass it; `= "fft-cic"` only at the outermost
boundary defaults) threads:

`main`/`SparkGameProcessor.process` → `_process_tracking_match` / `_process_statsbomb_match` →
`_make_action_context_udf` (closure capture) → `enrich_batch` → `_enrich_tracking_match` /
`_enrich_sb360_match` → replaces the hard-coded `kde_backend="fft-cic"` in `add_ghost_gk`
(enrich.py:304 tracking, enrich.py:433 SB360) → `out["ghost_gk_method"] = kde_backend`.

Event-only path (`_enrich_event_only_match`) sets no ghost-GK → `ghost_gk_method` stays NULL (set
explicitly to `None` for clarity, mirroring how `pitch_control_method` is NULL there).

## 10. Full touch-list (files)

**Domain / analytics:**
- `src/analytics/action_context/ghost_gk_backend.py` — **new**: `GHOST_GK_KDE_BACKENDS`,
  `resolve_ghost_gk_backend` (raises `ValueError`, not `SystemExit`).
- `src/analytics/action_context/work_unit.py` — add `kde_backend: str = "fft-cic"` to `WorkUnit` +
  `__post_init__` validation against `GHOST_GK_KDE_BACKENDS` (raise `ValueError`).
- `src/analytics/action_context/schema.py` — add `ghost_gk_method` to `RESULT_COLUMNS` + `ACTION_CONTEXT_DDL`
  (110 → 111); update the column-count comment.
- `src/analytics/action_context/enrich.py` — thread `kde_backend` into `_enrich_tracking_match` /
  `_enrich_sb360_match`; set `ghost_gk_method`.
- `src/analytics/action_context/drain.py` — `WATCHDOG_BUDGET_S` 1800 → 2700.

**Ingestion / orchestration:**
- `src/analytics/action_context/pipeline.py` — `enrich_batch` + `run_work_unit` gain `kde_backend`.
  **(NOT `src/ingestion/pipeline.py` — that module does not exist; both functions live in
  `analytics/action_context/pipeline.py`. The threading chain crosses the layer boundary:
  `ingestion.action_context._make_action_context_udf` — the Spark UDF factory, correctly in `ingestion/`
  — calls `analytics.action_context.pipeline.enrich_batch`.)**
- `src/ingestion/action_context.py` — `_make_action_context_udf`, `_process_tracking_match`,
  `_process_statsbomb_match` (+ SB360 helper) gain `kde_backend`; extract pure
  `_period_replace_where(match_id, period_filter) -> str` (testability, review #4); `main` + `main_preflight`
  parse `--ghost-gk-backend`, call `resolve_ghost_gk_backend`, and **catch `ValueError` → `SystemExit`** at
  this CLI boundary; `main_drain_worker` parses `--watchdog-budget-s` (guarded int parse); replace
  `_find_tracking_new_ids` with `_find_tracking_new_period_pairs`; update its call site to emit per-period
  units with the resolved `kde_backend`.
- `src/ingestion/action_context_queue.py` — add `("kde_backend", "string", True)` to `_QUEUE_COLUMNS`;
  `enqueue()` writes `unit.kde_backend`; extract pure `_row_to_work_unit(row) -> WorkUnit` (NULL →
  `"fft-cic"`) used by `units_for_worker()` (testability, review #3/#4); `SparkGameProcessor.process` passes
  `unit.kde_backend` to `_process_tracking_match`.
- `scripts/submit_ac1_oneshot.py` — `--ghost-gk-backend` + `--timeout-seconds` args.

**dbt:**
- `dbt_project/models/staging/action_context/stg_action_context__values.sql` — `cast(ghost_gk_method as string)`.
- `dbt_project/models/marts/fct_action_context.sql` — `ghost_gk_method` in `action_raw` CTE + `final` SELECT.
- `dbt_project/models/marts/_marts__models.yml` — contract column entry (`data_type: string`).

**Migrations:**
- `scripts/migrations/2026-06-03-add-ghost-gk-method-to-action-context.sql` — `ALTER bronze.spadl_action_context
  ADD COLUMNS (ghost_gk_method STRING)`.
- `scripts/migrations/2026-06-03-add-kde-backend-to-action-context-work-queue.sql` — `ALTER
  observability.action_context_work_queue ADD COLUMNS (kde_backend STRING)`; **also** update the canonical
  CREATE-TABLE DDL that `test_work_queue_schema_parity.py` parses
  (`scripts/migrations/2026-06-02-create-action-context-work-queue.sql`) to keep parity green
  (idempotent `CREATE TABLE IF NOT EXISTS` — no-op on the live table).

**Terraform:**
- `terraform/modules/workflows/main.tf` — 2 new variables + 2 job parameters + preflight `--ghost-gk-backend`
  + drain `--watchdog-budget-s` + 5× `timeout_seconds` 300 → 600 + plumb `AC1_GHOST_GK_BACKEND` env on the
  preflight/compute environments.

**Tests (behavioral-first — review #2/#3):** the harness is **pure-pandas** (no local Spark/Delta);
`run_work_unit` recomputes the real enrich on fixtures (`test_mini_golden.py` pattern). Prefer behavioral
tests; where a kernel is Spark-bound, extract a pure helper and test that.
- `src/tests/action_context/test_ghost_gk_backend_resolver.py` — **new**: precedence + validation truth
  table (raises **`ValueError`** on unknown). The one excellent test from the first draft — kept.
- **Headline behavioral test (review #2)** — `test_ghost_gk_method_provenance.py`: recompute via
  `run_work_unit(WorkUnit(provider="idsse", match_id="J03WMXmini", period=1, kde_backend="fft"))`, with a
  **spy** on `silly_kicks.tracking.features.add_ghost_gk` that records `kde_backend` then delegates to the
  real function. Assert (a) the spy saw `kde_backend == "fft"` (proves the backend reaches the
  *computation*, catching label/computation drift), and (b) `result["ghost_gk_method"] == "fft"` on every
  row. This replaces the brittle `inspect.getsource`/`inspect.signature` string-matches from the first
  draft. *(Event-only NULL: no wyscout fixture exists for a behavioral assertion; covered by the explicit
  `None` assignment in `_enrich_event_only_match` + the nullable schema column. Gap noted honestly.)*
- `_period_replace_where` pure-helper test — predicate includes `period_id` iff `period_filter` set
  (review #4 disjoint-write guard, made testable without Spark).
- `_row_to_work_unit` pure-helper test (extracted from `units_for_worker`) — NULL `kde_backend` → `"fft-cic"`.
- `WorkUnit.__post_init__` test — `WorkUnit(kde_backend="typo")` raises `ValueError`; valid value accepted.
- `src/tests/test_action_context_createdataframe_schema.py` — assert `ghost_gk_method` in `RESULT_COLUMNS` +
  `ACTION_CONTEXT_DDL` (this one is legitimately a schema-constant assertion, not source-greppery).
- `src/tests/action_context/oracle_map.py` — add `"ghost_gk_method": ("categorical", None, None)` to
  `INVARIANT_ONLY`.
- `test_work_queue_schema_parity.py` — stays green via the canonical-DDL update above.
- `_find_tracking_new_period_pairs` — Spark-bound (queries `spark.table`); not pure-pandas-testable in this
  harness. Guarded by a focused shape assertion + **live validation** (a scoped AC-1 run); flagged rather
  than pretend a Delta round-trip exists.
- **mini-golden + full golden — RE-BASELINE REQUIRED** (now that 4.11.0/4.10.0 is bundled): 4.10.0's
  serve-carrier fix shifts `ghost_gk_x/y` on ~0.4% of frames, AND the new `ghost_gk_method` column is added.
  Regenerate **both** (`scripts/build_ac1_mini_golden.py` + the full `J03WMX_p1`) per the HARD RULE, in this
  PR. (Independent of backend selection: default stays `fft-cic`; the shift is the library bump.)

**Dependencies (bundled — decision (A)):**
- `pyproject.toml` — `silly-kicks[das,ghost-gk]>=4.9.1,<5` → `>=4.11.0,<5`.
- `scripts/submit_ac1_oneshot.py` PEP 723 dep `silly-kicks>=4.9.1,<5` → `>=4.11.0,<5` (AC-1 tool).
- **Decision #3 (user 2026-06-03 = bump all — keep floors in sync):** the 6 trainers'
  `_REQUIRED_SK_MIN = (4, 9, 1)` → `(4, 11, 0)` (`train_football2vec{,_360,_v2}.py`, `train_scoutgpt_hf.py`,
  `train_vaep_model_hf.py`, `train_xg_v2_hf.py`) **+** `src/tests/test_sk3_mig_b_orchestrator_invariants.py`
  §2.10.5 (which hardcodes the expected `(4, 9, 1)` literal — NOT tied to the pyproject floor, so it must be
  bumped in lock-step or the test fails). The ADR notes trainer artifacts are unchanged-until-next-run and
  4.11.0's `xCrossAttempt` is unused by them too.
- Adopt via `uv lock --refresh-package silly-kicks` + `uv sync --inexact` (NEVER pip `--force-reinstall`).
- **No new column for 4.11.0's `xCrossAttempt` (TF-17)** — ships untrained, weights not shipped; not consumed.

**Evidenced re-baseline scope (review v2 #2 — enumerated from `CHANGELOG.md`, not asserted):** across the
**4.9.1 → 4.11.0** span the *only* AC-1-relevant numeric change is **`ghost_gk_x/y/spread`**, from 4.10.0:
- 4.10.0 ghost-GK serve-carrier consistency fix → `ghost_gk_x/y` change on **0.4% of frames** (max 4.03 m,
  median 0 m), applies to all variants; PLUS a quality-equivalent **default-weights re-fit** (held-out
  KDE-mode MAE 4.47 vs 4.41 m) — both touch ghost-GK *only*. The new `carrier=` kwarg is additive/optional;
  AC-1's `add_ghost_gk(model="default", …)` call is unchanged. The `full`-weights→Hub packaging change is
  **irrelevant** (AC-1 uses `model="default"`, the wheel-bundled variant).
- 4.11.0 adds `xCrossAttempt` only (not wired into any xfn list); `_build_occurrence_labels` was extracted
  from `build_xshot_labels` as a **bit-identical wrapper → xShotOccurrence (consumed by AC-1) is unchanged.**
- **No change** to DAS, pitch control, OBSO, PAUSA, shape graph, line-breaking, team shape across the span.
- **Tripwire:** if the T16 golden regen shows value churn in *any* column other than `ghost_gk_x/y/spread`,
  STOP and reconcile against the changelog — do not absorb it (evidence-before-claim; the golden HARD RULE).

**Docs:**
- ADR-035 amendment — ghost-GK backend selection (resolution hierarchy, per-unit storage) + `ghost_gk_method`
  provenance + exact-backend-overwrites-fft-cic consequence + the 4.10.0 carrier-fix re-baseline.
- ADR-037 amendment — period work-units for all tracking providers; watchdog 1800 → 2700 (+ override);
  preflight 300 → 600.
- `CLAUDE.md` "Performance Budgets" — 1800 → 2700 references.
- **HF dataset card (review #10)** — `docs/huggingface/dataset-cards/spadl-action-context.md`: document
  `ghost_gk_method` (and confirm any 4.10.0-driven `ghost_gk_*` note). Same precedent as PR #337's GK-metrics
  card update; "HF artifact link completeness" applies within this PR.
- `docs/c4/architecture.dsl` — edit the actionContext element description if the seam description changes
  (concise, ≤200 chars — EDIT, don't append).

**AI governance (review #11):** ghost-GK / `ghost_gk_method` is a **derived spatial metric**, NOT a
per-player *evaluative* system → **not** in `PER_PLAYER_EVALUATIVE_CARDS`. Determination is explicit:
`AI_GOVERNANCE.md` / model-card updates are **N/A** for this PR. (Confirm by running
`uv run pytest src/tests/test_ai_governance_md.py -v` stays green — no card inventory change expected.)

**Wheel:** `uv run python scripts/bump_wheel.py` (0.5.15 → 0.5.16) — never edit version manually.

## 11. Out of scope (explicit)

- **4.11.0 `xCrossAttempt` (TF-17)** — ships **untrained** (weights not shipped); AC-1 does **not** consume
  it and adds **no** column (user 2026-06-03). The 4.11.0 floor bump is a version move only; its numeric
  content for AC-1 is the bundled **4.10.0** ghost-GK serve-carrier re-baseline.
- **Write-batching / commit-contention mitigation** — deferred (8 drain workers → `_delta_log` S3-400
  retry storm; noisy but harmless at ~1000 units; real fix is per-worker `ResultSink` batching later).
- **Per-provider / per-match backend map** — the per-unit seam supports it; not built now (a preflight-only
  follow-up; the resolver's `ValueError` contract keeps it safe to call from non-CLI code).
- **`_TIER_COST_S` re-tuning** — left as-is (rank order holds); noted in §5.

## 12. Risks / verification

- **Queue schema parity test** — adding to `_QUEUE_COLUMNS` without updating the canonical DDL the test
  parses fails CI; handled in §10.
- **`.toPandas()` boundedness** — any insertion above the exempted `.toPandas()` line in
  `action_context.py` shifts its `_topandas_exemptions.yml` line key; re-grep + update + run
  `test_topandas_boundedness` before push.
- **Signature changes are REPO-WIDE** — the AC-1 driver lives in BOTH `main` (for-each) AND
  `main_drain_worker` (drain) call paths; grep all callers of every changed signature
  (`_process_tracking_match`, `enrich_batch`, `_make_action_context_udf`, `_process_statsbomb_match`).
- **pyright via grep, not `| tail`** — the error-count summary scrolls off above the version warning.
- **Full `uv run pytest src/tests/`** before push — line-keyed AST guards (`test_topandas_boundedness`,
  `validate_workflow_cards`, `test_work_queue_schema_parity`) are separate CI steps not in any subset.
- **dbt contract validation timing (review #6)** — `contract: enforced` validates the new column's type
  only on a **full-refresh**; an incremental run absorbs it via `on_schema_change='append_new_columns'`
  (the accepted pattern here, same as PR #337's `pitch_control_method`). Confirm the post-merge
  `dbt-live-ci` does a full-refresh (or that the first incremental is acceptable) so the contract is
  actually validated, not just assumed.
- **`--watchdog-budget-s` parse guard (review #9)** — `int(raw_budget)` on a malformed operator value
  raises an opaque `ValueError` mid-drain; wrap with a clear message / bounded check.
- **silly-kicks adoption footgun** — bump via `uv lock --refresh-package silly-kicks` + `uv sync --inexact`;
  verify the installed version is 4.11.0 and the `kde_backend` set still equals `GHOST_GK_KDE_BACKENDS`
  before relying on the allowlist. PEP 723 scripts: the wheel's `[spadl]` extra is the single source of
  truth; the `_REQUIRED_SK_MIN` runtime assertions are the guard.
