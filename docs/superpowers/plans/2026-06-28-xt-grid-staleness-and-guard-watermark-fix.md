# xT-grid staleness fix + watermark-freshness guard adoption — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make the live `global` xT grid directional again (fixing the negative `xt_gk_dzv`), and stop the
build-if-absent guard class from silently re-staling derived artifacts after an upstream re-derivation.

**Architecture:** The grid math is already correct; the live grid is a ~2-month-stale snapshot that the
build-if-absent guard never recomputes. Phase 1 (Tier A, urgent): add a directionality assertion to the grid build,
migrate the `expected_threat` guard to the existing watermark-freshness primitive (`check_upstream_freshness`/
`record_watermarks`), bump the wheel, then do a one-time corrective rebuild + re-materialize `fct_action_context` +
off-ball xT. Phases 2–3 (Tier B/C systemic) are scoped follow-ups with open per-artifact design decisions.

**Tech Stack:** Python 3.10, NumPy, PySpark (Databricks serverless), Delta Lake, pytest, the luxury-lakehouse wheel,
`ingestion.guards` watermark framework, workflow cards (YAML).

**Reference:** ADR-063 (rev 2); `docs/investigations/2026-06-28-xt-grid-stale-not-directionality-root-cause.md`;
review `docs/investigations/2026-06-28-xt-grid-fix-plan-review.md`.

---

## Revision 2 (post-review, 2026-06-28) — SUPERSEDES Phase 1 below where they conflict

The xT-GK side's review (`…-xt-grid-fix-plan-review.md`) green-lit Tier A's mechanism but raised two blockers
(H1, H3), a required addition (H4), and M/L items. Disposition (verified against code):

| Item | Disposition |
|---|---|
| **H1** transitive freshness gap (consumers don't watermark on the grid) | **ACCEPT** — add grid→consumer watermark edges (AC, off-ball-xt). |
| **H2** "cheap" measured on wrong cost; per-version churn | **ACCEPT core / REFUTE specific** — `check_upstream_freshness` already excludes OPTIMIZE/VACUUM (`_DATA_CHANGING_OPS`), so that part is moot; but daily WRITE churn is real → producer **writes only on material change**. |
| **H3** `validate_differential(0.30)` × auto-rebuild deadlock | **ACCEPT** — demote to WARN; `record_watermarks` only after validated write. |
| **H4** cross-cutting staleness monitor | **ACCEPT** — ships in this PR as the interim backstop. |
| **M5** directionality check fragile + scope mismatch | **ACCEPT** — thirds-mean ratio + Spearman rank-corr; global-only. |
| **M6** per-comp grids unused | **ACCEPT (verified)** — only `global` is consumed → delete per-comp grids. |
| **M7** `fct_tracking_context` left stale | **REFUTE** — `tracking_context` fits its *own* xT (`ExpectedThreat().fit()`, tracking_context.py:1450); it does NOT read the bronze grid. ADR-063 corrected; no runbook entry needed. |
| **M8** rollback under-specified | **ACCEPT** — non-destructive: snapshot grid to a side table + `replace_where` overwrite (Delta history); no `DELETE`-before-validate. |
| **L9/L10/L11/L12** | **ACCEPT** — module constant (no `if False`); pure decision fn tested without Spark; verify alerting pages on the `raise`; ADR title reframed. |

**Rev-2 task set (replaces Phase-1 Tasks 1–6 where noted):**

- **R1 (was Task 1) — directionality assert, robust form.** `validate_structural(require_directional=True)` computes
  the **thirds-mean ratio** = `mean(values[8:12].mean())` (attacking third, zone_x 8–11) ÷ `mean(values[0:4].mean())`
  (defensive third, zone_x 0–3); assert `>= 5`. **Plus** a coarse shape gate: Spearman rank-corr between `zone_x`
  (0..11) and per-zone_x mean `>= 0.6`. Drop the single-extreme-column ratio and the `-0.01` diff check. Global-only.
- **R2 — delete per-competition grids (M6).** Remove the per-comp loop from `expected_threat.run_pipeline` and the
  per-comp `find_new_ids` logic from the guard; the producer builds the **global grid only**. Drop existing per-comp
  rows in the corrective run. (Producer + guard both simplify: guard = "rebuild global when `fct_action_values`
  watermark changed".)
- **R3 (revises Task 2) — guard = watermark-only.** `_ExpectedThreatGuard.check` = `check_upstream_freshness(
  fct_action_values)` → `need_global` on change/first-run. Extract the decision into a **pure function**
  `_decide(upstream_changed: bool, global_exists: bool) -> bool` (need_global) and unit-test it without Spark (L10).
  Use a module constant `_WORKFLOW_ID = "wf-xt-grids"` (L9). Card declares the `fct_action_values` delta-table input.
- **R4 — producer: write-only-on-material-change + WARN differential + record-on-success (H2/H3).** After computing
  the global grid: run `validate_structural(require_directional=True)` (HARD gate — raise → no watermark recorded).
  Load the previous grid; if `max(abs(new - previous)) < 1e-3` and the directionality signature is unchanged, **skip
  the write** (no version bump). Else `replace_where` overwrite. Demote `validate_differential` to a logged WARN
  (+ alert), never raise. Call `record_watermarks` **only** on the success path (written or intentionally-skipped),
  **after** `validate_structural` passed — assert via test that a `validate_structural` raise leaves no watermark.
- **R5 — consumer watermark edges (H1).** Add `bronze.expected_threat_grids` as a watermark input to
  **`compute_action_context`** and **`compute_off_ball_xt`** (card `inputs.tables` + the guard consults
  `check_upstream_freshness` on it + `record_watermarks` after). When the grid version bumps (material change only),
  these re-materialize. **Open decision (needs sign-off): AC's guard is currently per-match `find_new_ids`; making it
  "reprocess ALL on grid change" is a behavioral change — confirm the desired semantics (full re-materialize on grid
  change vs. a scoped subset).**
- **R6 — cross-cutting staleness monitor (H4).** A scheduled check: for each registered derived table, compare its
  recorded watermark against `max(upstream watermarks)`; alert when it lags beyond a threshold. Detect-and-alert
  only (no rebuild). Covers all tiers. (New small module + a workflow card + an alert sink; L11: confirm it pages.)
- **R7 (revises Task 6) — non-destructive corrective rebuild (M8).** Snapshot the ~96-row grid to
  `bronze.expected_threat_grids_backup_20260628` first. Deploy Tier A → producer's first post-deploy run recomputes,
  `validate_structural` gates, `replace_where` overwrites global (Delta history retains the prior version). Verify
  directionality (thirds ratio ≥ 5). The R5 consumer edges then trigger the `fct_action_context` re-materialize +
  off-ball-xT recompute. **No `DELETE`.** Acceptance: `xt_gk_dzv < 0` count → 0; `xt_gk_pev` up; ping analysis side.
- **Task 4 (HF normalizer no-op) + Task 5 (wheel bump):** unchanged.

**Open decisions for sign-off before implementation:** (a) M6 — confirm deleting per-comp grids (verified unused, but
it removes a built artifact); (b) R5 — AC's "re-materialize on grid change" semantics (full vs scoped); (c) R4 —
materiality threshold `ε = 1e-3` for write-if-changed. Tier B/C unchanged from the original plan below.

### Revision 3 (sign-off resolutions) — OVERRIDES R-tasks above where noted

The other session signed off on (a)/(b)/(c). Resolutions:

- **M6 → KEEP-AND-GUARD (withdraw the delete).** Per-competition xT is roadmapped (ExT v2:
  `specs/2026-04-25-ext-v2-reproduction-design.md`). So **R2's deletion is withdrawn** — keep computing per-comp
  grids, and **R1's directionality assert applies to per-comp grids above a min-action threshold** (the danger was
  being unguarded; the guard removes it). R1 reverts to "global always + per-comp above `min_actions` (e.g. 5000)".
  *Operator confirm: is ExT v2 still active? If parked, revert to delete.*
- **R5 → SPLIT the grid-derived projection (columns, not matches).** Column-scoping is infeasible in the monolithic
  `_enrich_tracking_match` UDF, so extract `xt_gk_*` into a **separate cheap grid-derived pass/stage** keyed on the
  grid watermark (off-ball xT is already separate). On a material grid change it refreshes `xt_gk_*` for **all
  matches** without re-running ghost-GK/DAS. New tasks: (R5a) extract `compute_xt_gk_projection` from AC enrich into
  its own stage writing the `xt_gk_*` columns; (R5b) watermark it on `bronze.expected_threat_grids`; (R5c) keep
  off-ball-xt's own grid watermark edge. *This is a real AC refactor — biggest scope item; confirm before building.*
- **R4 → drift-bounded, relative, measured (replaces the bare `ε=1e-3`).** (i) Add a one-off **measurement step**:
  capture real append-to-append grid drift over a few `fct_action_values` updates before fixing the threshold.
  (ii) Metric = **max relative per-cell change among cells above a value floor**, weighted toward the
  defensive/keeper third (where `xt_gk` lives). (iii) Anchor: material iff it moves any `xt_gk` component > ~1e-4
  (downstream report precision). (iv) **Gate against the LAST-PROPAGATED grid** (stored separately — the grid
  consumers were last materialized on), NOT the previous daily compute, so cumulative sub-threshold drift is bounded
  by the threshold. New task: persist a `last_propagated_grid` baseline + compare against it in the write-if-changed
  check (R4 in R-task set).

**Net rev-3 build order:** R1(global+per-comp guard) → R2(keep per-comp, simplify guard to watermark) →
R3(pure decision fn, watermark guard, card input) → R4(write-if-changed vs last-propagated grid + WARN differential +
record-on-success + drift measurement) → R5a/b/c(split xt_gk projection + consumer watermark edges) →
R6(staleness monitor) → R7(non-destructive corrective rebuild) → Task 4(HF no-op) → Task 5(wheel bump). Bite-sized
TDD steps to be regenerated against this order once R5's split scope is confirmed.

### Revision 4 (decisions CONFIRMED — design locked, ready to build)

`…-xt-grid-fix-decisions.md` (confirmed by Karsten) settles the two open items. Final, locked:

- **M6 = keep-and-guard; IMPLEMENT per-comp gating.** R2's deletion is **cancelled** — keep per-comp grids. R1 must
  apply `require_directional` to **global (always) + per-comp above `min_actions`** (define the threshold, e.g.
  5000; add a test that a below-threshold comp does NOT false-fail). The per-comp loop stays in the producer.
- **R5 = INTERIM full-AC re-materialize on material grid change; DEFER the split.** R5a (projection split) is
  **removed from this initiative**. R5 reduces to **R5b only**: add `bronze.expected_threat_grids` as a watermark
  input to `compute_action_context` (full re-materialize on a material grid version bump) and keep off-ball-xt's
  grid watermark edge. The grid is global → all matches' xt_gk invalidate together (a match subset is wrong); the
  full pass is acceptable *because R4 makes material changes rare*.
- **R4 = LOAD-BEARING hard constraint.** R5-interim is safe ONLY if R4 ships with BOTH (1) material-only ε
  (measured first; relative/zone-aware) AND (2) gate-vs-last-PROPAGATED-grid. The plan persists a
  `last_propagated_grid` baseline and compares against it (NOT the previous daily compute). If R4 can't deliver
  (1)+(2), R5 reverts to the projection split (R5a) — do not ship R5-interim without R4 (1)+(2).
- **Sequencing constraint:** H1 (R5b) + H3 (R4) must land **before** the one-time rebuild (R7).
- **M7 stays refuted** (no `fct_tracking_context` re-materialize — it fits its own xT). **M8** = Delta time-travel
  rollback (+ side-table snapshot). L9/L10/L11/L12 as in rev 2.

**LOCKED build order:** R1 → R2 → R3 → R4 (material + last-propagated baseline + measurement) → R5b (AC + off-ball
grid watermark edges) → R6 (staleness monitor) → R7 (non-destructive corrective rebuild) → Task 4 → Task 5. Ready to
regenerate bite-sized TDD steps and implement.

### Revision 5 — IMPLEMENTED (2026-06-28, branch `feat/xt-grid-staleness-fix-adr-063`, wheel 0.5.54, uncommitted)

Code complete + tested (266 passed / 14 skipped in the affected-subsystem sweep; ruff clean; pyright 0 errors):
- **R1** `analytics/expected_threat.py::validate_structural(require_directional=…)` + public `assert_directional()`
  (thirds-mean ratio ≥3 + Spearman rank-corr ≥0.6); jax-free tests `test_xt_grid_directionality.py`.
- **R2/R3** `ingestion/expected_threat.py` watermark guard (`check_upstream_freshness` on `fct_action_values`) + pure
  `_decide_rebuild`; `_WORKFLOW_ID` const; card input already present. Tests `test_expected_threat_guard.py`.
- **R4** write-if-changed (`_write_grid_if_material` + `_grid_drift`, gate vs current=last-propagated grid, floor
  0.005, rel threshold 0.10 PROVISIONAL — logged each run); `validate_differential` → WARN; `record_watermarks`
  only after a validated run. Per-comp directionality gate above `_MIN_ACTIONS_DIRECTIONAL=5000`.
- **R5b** AC (`main_preflight._force_full_rematerialize_on_grid_change` — DELETE tracking AC rows on material grid
  change) + off-ball (`check()` wipes results on grid change → anti-join re-discovers all); grid declared as a
  card input on both; conformance allowlists extended.
- **R6** `ingestion/staleness_monitor.py` (`find_stale_artifacts` pure + `run_monitor` ERROR-alert); tests
  `test_staleness_monitor.py`. **SCHEDULED**: `wf-staleness-monitor.yaml` card + `run_staleness_monitor` daily-job
  terraform task (env `default`, depends_on `dbt_build_output_marts`) + pyproject entry point + parity mapping —
  all card/parity/conformance tests green.
- HF `_normalize_attack_direction` → no-op; wheel 0.5.53 → 0.5.54 (`bump_wheel.py`, 29 files, lock).

### Revision 6 — threshold recalibration (2026-06-28, post-deploy)

The first corrective-rebuild run FAILED the directionality assert: the real global grid measures a **thirds-mean
ratio of 4.54**, below the 5.0 bar. Root cause: the 5.0 threshold was calibrated from the **single-column** `x11/x0`
ratio (~9.5 in the investigation) but the assert (per review M5) uses the structurally-lower **thirds-mean** ratio.
The grid is correct/directional (4.5×; stale symmetric was ~0.96). Fix: default `min_attack_ratio` **5.0 → 3.0**
(separates 4.54 from ~1.0 symmetric/inverted with margin), and `assert_directional` now **logs the measured
thirds-ratio + rank-corr at INFO every build** so the bar can be raised toward 3.5 data-drivenly later. Tests
`test_real_grid_shape_{passes_at_default,would_fail_old_5}_threshold`. Wheel 0.5.54 → 0.5.55.

**Remaining (post-merge ops only — no code left):** R7 corrective rebuild is automatic (first daily run rebuilds the
grid → the R5b edges re-materialize `fct_action_context` + off-ball xT); confirm + ping the analysis side. R4's `ε`
(`_MATERIALITY_REL_THRESHOLD`, provisional 0.10) to be tuned from the per-run drift the producer logs once real data
accrues; likewise monitor the logged thirds-ratio and consider raising `min_attack_ratio` toward 3.5.

---

## File structure

| File | Change | Responsibility |
|---|---|---|
| `src/analytics/expected_threat.py` | Modify `XTGrid.validate_structural` | add `require_directional` att/def-ratio assertion |
| `src/ingestion/expected_threat.py` | Modify `_ExpectedThreatGuard.check` + `run_pipeline` | watermark freshness + `record_watermarks` |
| `workflow-cards/wf-xt-grids.yaml` | Modify `inputs` | declare `fct_action_values` as a delta-table input (watermark contract) |
| `src/tests/test_expected_threat.py` (or existing) | Add tests | directionality assert; guard fires on watermark change |
| `pyproject.toml`, wheel consumers | Modify (via `bump_wheel.py`) | wheel version bump (code ships in wheel) |
| `scripts/compute_xt_grid_hf.py` | Modify `_normalize_attack_direction` | side cleanup: no-op for LTR input |

---

## Phase 1 — Tier A: xT grid fix (urgent; gates the xT-GK rebuild)

### Task 1: Directionality assertion in `XTGrid.validate_structural`

**Files:**
- Modify: `src/analytics/expected_threat.py` (`XTGrid.validate_structural`, ~L131–163)
- Test: `src/tests/test_expected_threat.py` (add)

- [ ] **Step 1: Write the failing tests**

```python
# src/tests/test_expected_threat.py
import numpy as np
import pytest
from analytics.expected_threat import XTGrid


def _grid(values: np.ndarray) -> XTGrid:
    return XTGrid(values=values, pitch_length=105.0, pitch_width=68.0, coord_system="spadl",
                  competition_id="global")


def test_validate_structural_rejects_nondirectional_global_grid():
    # U-shaped (stale-grid signature): high at both ends, low middle. shape (12, 8).
    col = np.array([0.0176, 0.0161, 0.0101, 0.0070, 0.0057, 0.0053,
                    0.0053, 0.0056, 0.0067, 0.0092, 0.0155, 0.0172])
    values = np.repeat(col[:, None], 8, axis=1)
    with pytest.raises(ValueError, match="directional"):
        _grid(values).validate_structural(max_value=0.50, require_directional=True)


def test_validate_structural_accepts_directional_global_grid():
    # Monotonic rise (correct grid): att/def ratio ~9.5.
    col = np.array([0.0071, 0.0070, 0.0071, 0.0076, 0.0085, 0.0096,
                    0.0113, 0.0138, 0.0176, 0.0235, 0.0481, 0.0674])
    values = np.repeat(col[:, None], 8, axis=1)
    # Must not raise.
    _grid(values).validate_structural(max_value=0.50, require_directional=True)
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `uv run pytest src/tests/test_expected_threat.py -k validate_structural -v`
Expected: FAIL — `validate_structural() got an unexpected keyword argument 'require_directional'`.

- [ ] **Step 3: Implement the assertion**

In `src/analytics/expected_threat.py`, change the signature and add the directionality block. Replace the existing
method head and the trailing monotonicity block:

```python
    def validate_structural(self, *, max_value: float | None = None, require_directional: bool = False,
                            min_attack_ratio: float = 5.0) -> None:
        """Validate grid structure. With require_directional=True (global grid), also assert the grid is
        materially attacking-directional: mean(xt at attacking end) / mean(xt at own-goal end) >= min_attack_ratio.
        The correct global grid measures ~9.5; the stale symmetric grid was ~0.98 (ADR-063)."""
        if self.values.min() < 0.0:
            raise ValueError(f"XTGrid contains negative values: min={self.values.min():.4f}")
        if max_value is not None and self.values.max() > max_value:
            raise ValueError(f"XTGrid max {self.values.max():.4f} exceeds max_value={max_value}")
        xt_range = self.values.max() - self.values.min()
        if xt_range < 0.05:
            raise ValueError(
                f"XTGrid range too narrow ({xt_range:.4f}) — likely coordinate orientation issue "
                f"or insufficient training data"
            )
        if require_directional:
            row_means = self.values.mean(axis=1)  # per-zone_x mean; index 0 = own goal, -1 = attacking goal
            own = float(row_means[0])
            att = float(row_means[-1])
            ratio = att / own if own > 0 else float("inf")
            if ratio < min_attack_ratio:
                raise ValueError(
                    f"XTGrid is not attacking-directional: attack/defence ratio {ratio:.2f} < "
                    f"{min_attack_ratio} (own-goal mean {own:.5f}, attacking mean {att:.5f}). Likely a STALE "
                    f"grid built on pre-LTR data, or an orientation regression. See ADR-063."
                )
```

(Remove the old `row_means`/`np.diff(...) >= -0.01` monotonicity block — the ratio check supersedes it.)

- [ ] **Step 4: Run the tests to verify they pass**

Run: `uv run pytest src/tests/test_expected_threat.py -k validate_structural -v`
Expected: PASS (both).

- [ ] **Step 5: Pass `require_directional=True` at the global call sites**

In `src/ingestion/expected_threat.py` (~L324):
```python
            global_xt_grid.validate_structural(max_value=0.50, require_directional=True)
```
In `scripts/compute_xt_grid_hf.py` (~L269):
```python
    global_grid.validate_structural(max_value=0.50, require_directional=True)
```

- [ ] **Step 6: Run ruff + the existing expected-threat tests, then commit**

Run: `uv run ruff check src/analytics/expected_threat.py src/ingestion/expected_threat.py && uv run pytest src/tests/test_expected_threat.py -q`
Expected: PASS.
```bash
git add src/analytics/expected_threat.py src/ingestion/expected_threat.py scripts/compute_xt_grid_hf.py src/tests/test_expected_threat.py
git commit -m "feat(xt): assert global xT grid is attacking-directional (ADR-063)"
```

### Task 2: Watermark-freshness in the `expected_threat` guard

**Files:**
- Modify: `src/ingestion/expected_threat.py` (`_ExpectedThreatGuard.check` ~L59–96; `run_pipeline` ~L340 end)
- Test: `src/tests/test_expected_threat.py`

- [ ] **Step 1: Write the failing test** (guard forces a full rebuild when the upstream watermark changed)

```python
def test_guard_forces_rebuild_when_upstream_changed(monkeypatch):
    from ingestion import expected_threat as et

    # Upstream watermark says "changed" (count=1); guard must set need_global=True
    # and treat every competition as needing a grid (full corrective rebuild).
    monkeypatch.setattr(et, "check_upstream_freshness",
                        lambda *a, **k: __import__("ingestion.guards", fromlist=["FilterResult"]).FilterResult(
                            workflow_id="wf-xt-grids", count=1))
    monkeypatch.setattr(et, "find_new_ids", lambda *a, **k: [])
    monkeypatch.setattr(et, "_list_relevant_competition_ids", lambda *a, **k: ["7", "37", "global_src"])

    class _FakeSpark:  # minimal: .table(...).select(...).distinct().collect() chain for the existing-global probe
        def table(self, *_a):
            return self
        def select(self, *_a):
            return self
        def distinct(self):
            return self
        def collect(self):
            return []
        def sql(self, *_a):
            return self

    res = et.skip_guard.check(_FakeSpark(), "cat", "schema")
    assert res.count > 0
    assert res.metadata["need_global"] is True
    assert set(res.metadata["new_competition_ids"]) == {"7", "37", "global_src"}
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `uv run pytest src/tests/test_expected_threat.py -k forces_rebuild -v`
Expected: FAIL (guard does not yet consult `check_upstream_freshness`).

- [ ] **Step 3: Implement the watermark check in the guard**

In `src/ingestion/expected_threat.py`, add the imports and extend `check`:

```python
from ingestion.guards import (
    check_upstream_freshness, ensure_table, find_new_ids,
    record_watermarks, resolve_upstream_tables_from_card,
)
```
At the end of `_ExpectedThreatGuard.check`, before building the FilterResult:
```python
        # Watermark freshness (ADR-063): if the upstream mart was re-derived (e.g. the SPADL->LTR
        # migration), rebuild ALL grids — find_new_ids only catches genuinely-new competitions and would
        # otherwise leave every existing grid stale.
        upstream = resolve_upstream_tables_from_card(self.workflow_id, catalog, schema)
        upstream_changed = check_upstream_freshness(spark, catalog, self.workflow_id, upstream).count > 0
        if upstream_changed:
            need_global = True
            all_comps = _list_relevant_competition_ids(spark, catalog)
            new_comps = sorted(set(new_comps) | set(all_comps))

        total = len(new_comps) + (1 if need_global else 0)
        if total == 0:
            return FilterResult(workflow_id=self.workflow_id, count=0)
        return FilterResult(
            workflow_id=self.workflow_id, count=total,
            metadata={"new_competition_ids": sorted(new_comps), "need_global": need_global},
        )
```

- [ ] **Step 4: Call `record_watermarks` after a successful build**

At the end of `run_pipeline` (after grids are written, before `return`):
```python
    upstream = resolve_upstream_tables_from_card(self.workflow_id if False else "wf-xt-grids", catalog, schema)
    record_watermarks(spark, catalog, "wf-xt-grids", upstream)
```

- [ ] **Step 5: Run the test to verify it passes**

Run: `uv run pytest src/tests/test_expected_threat.py -k forces_rebuild -v`
Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add src/ingestion/expected_threat.py src/tests/test_expected_threat.py
git commit -m "feat(xt): watermark-freshness guard rebuilds grids on upstream change (ADR-063)"
```

### Task 3: Declare `fct_action_values` as a delta-table input on the workflow card

**Files:**
- Modify: `workflow-cards/wf-xt-grids.yaml` (`inputs` section)

- [ ] **Step 1: Inspect an existing watermark card for the exact format**

Run: `grep -A6 "inputs:" workflow-cards/wf-dbt-build-output-marts.yaml`
Expected: shows `inputs.tables` entries with `source: delta-table` and `id: {catalog}.{schema}.<table>`.

- [ ] **Step 2: Add the input to `wf-xt-grids.yaml`**

In the card's front-matter `inputs:` block, add (matching the observed format):
```yaml
inputs:
  tables:
    - id: "{catalog}.dev_gold.fct_action_values"
      source: delta-table
```
(Use the schema the live `_GOLD_TABLE` lookup uses — `dev_gold`. `resolve_upstream_tables_from_card` substitutes
`{catalog}`/`{schema}`; pin `dev_gold` explicitly since the grid reads gold, not the guard's `schema` arg.)

- [ ] **Step 3: Run the guard-conformance test**

Run: `uv run pytest src/tests/test_guard_conformance.py -k "watermark or card_inputs or expected_threat" -v`
Expected: PASS — the card now satisfies the "modules using check_upstream_freshness have delta-table inputs" +
"call record_watermarks" contracts.

- [ ] **Step 4: Commit**

```bash
git add workflow-cards/wf-xt-grids.yaml
git commit -m "chore(xt): declare fct_action_values delta-table input for watermark guard (ADR-063)"
```

### Task 4: Side cleanup — neutralize the LTR-corrupting normalizer in the HF script

**Files:**
- Modify: `scripts/compute_xt_grid_hf.py` (`_normalize_attack_direction`)

- [ ] **Step 1: Make it a no-op for already-LTR input** (the live SPADL is LTR; the no-shot "swap sides" inference
  flips already-correct team-periods). Replace the body with an early return + a log, OR delete the call at L243.
  Minimal:
```python
def _normalize_attack_direction(df: pd.DataFrame, params: ExpectedThreatParams) -> pd.DataFrame:
    """No-op: lakehouse SPADL is canonically LTR (ADR-022). The legacy shot-cluster + per-period swap-flip
    corrupts already-LTR data (no-shot team-periods get spuriously flipped). Retained as a documented no-op;
    see ADR-063."""
    return df
```

- [ ] **Step 2: Commit**

```bash
git add scripts/compute_xt_grid_hf.py
git commit -m "fix(xt): neutralize LTR-corrupting normalizer in compute_xt_grid_hf (ADR-063)"
```

### Task 5: Wheel bump (code ships in the wheel)

- [ ] **Step 1: Bump + propagate**

```bash
# edit pyproject.toml [project] version -> next patch
uv run python scripts/bump_wheel.py
uv lock
uv run python scripts/bump_wheel.py --check
uv run pytest src/tests/test_wheel_constants.py -q
```
Expected: `--check` consistent; test PASS.

- [ ] **Step 2: Commit** `git commit -am "chore(wheel): bump for ADR-063 xT grid fix"`

### Task 6 (operational runbook — NOT TDD): corrective rebuild + re-materialize

> Run after Phase 1 merges and CI deploys the wheel + the env. Each step is operator-driven; verify before/after.

- [ ] **Step 1: Snapshot current state** — `SELECT competition_id, COUNT(*) FROM bronze.expected_threat_grids GROUP BY competition_id` (record row counts for rollback awareness).
- [ ] **Step 2: Wipe the grid table** (all rows — global + per-comp incl. inverted): `DELETE FROM soccer_analytics.bronze.expected_threat_grids` (or `TRUNCATE`). With no previous grid, `validate_differential` is skipped (returns early on `previous is None`).
- [ ] **Step 3: Run the build** — `jobs.run_now` the `compute_expected_threat` task (dev job schedule is PAUSED; trigger via SDK `run_now(only=["compute_expected_threat"])` analogous to the AC recompute). The guard now sees all comps absent + watermark changed → full rebuild; `validate_structural(require_directional=True)` gates it.
- [ ] **Step 4: Verify the grid** — re-run the directionality query: `att_to_def_ratio` for `global` ≥ 5 (expect ~9–10); per-zone_x monotone rise; max xT ~0.17.
- [ ] **Step 5: Re-materialize `fct_action_context`** — the AC recompute (wipe `bronze.spadl_action_context` for tracking providers → `run_now(only=[preflight_action_context, compute_action_context])` → `rederive_synced_marts.py --select fct_action_context` → `lakebase-grants.yml`), per the established procedure ([[project-sk-435-xtgk-pev-dzv-cycle]]). xt-gk now runs on the directional grid; DZV ≥ 0 follows.
- [ ] **Step 6: Recompute off-ball xT** — `compute_off_ball_xt` reads the same global grid; re-run it.
- [ ] **Step 7: Acceptance** — `COUNT(*) WHERE data_source='gradientsports' AND xt_gk_dzv < 0` → 0; `xt_gk_pev` means up vs baseline; ping the xT-GK analysis side for the full cohort re-analysis (treat `dzv_avg ≈ +0.01` as a sanity band per their green-light).

---

## Phase 2 — Tier B: expensive-model retrains (`xg_model_v2`, `player_embeddings_v1`/`v2`)

**Open design decision (needs sign-off before coding):** these retrain on HF Jobs; a naive upstream-version watermark
would retrain on every daily `fct_action_values` write — too expensive. Proposed primitive: a new
`guards.check_contract_version(spark, catalog, workflow_id, contract_version: str)` that stores a
**feature/contract version string** per workflow and fires only when the constant changes (a deliberate bump on a
feature/orientation contract change) or a manual `force_rebuild` job param. Migrate `xg_model_v2` and the embeddings
guards from `find_new_ids`-only to `find_new_ids` + `check_contract_version`. **Do not implement until the primitive
shape is agreed** — write a sub-plan after Phase 1 lands.

## Phase 3 — Tier C: per-id incremental pipelines

`defcon_lite_*`, `tracking_context`, `elastic_sync`, `entity_resolution`, `tracking_metadata`, `line_breaking`,
`pausa`, `formations_*`, `spadl_vaep` are per-id and recompute correctly when their id is wiped. After an upstream
re-derivation (e.g. SPADL re-conversion), already-present ids are stale until manually wiped — the **current
operational norm** (used for the AC recompute). Phase 3 = (a) document this norm in `docs/engineering/conventions.md`
(a "re-derivation checklist": which derived tables to wipe after a SPADL/feature migration), and (b) optionally
design a per-id content-version (hash of the id's input slice) as a future enhancement. No urgent code change.

---

## Self-review checklist (done)

- **Spec coverage:** directionality assert (Task 1) ✓; watermark guard (Task 2) ✓; card input contract (Task 3) ✓;
  HF-script normalizer cleanup (Task 4) ✓; wheel bump (Task 5) ✓; corrective rebuild + re-materialize + acceptance
  (Task 6) ✓; systemic Tier B/C scoped (Phases 2–3) ✓; the green-light's four refinements: ≥5 threshold (Task 1),
  generalize the guard (Phases 2–3), explicit one-time differential handling (Task 6 Step 2 — full wipe sidesteps it,
  no lingering bypass), downstream-scale heads-up (Task 6 Step 7) ✓.
- **Placeholders:** Phase 1 steps carry concrete code/commands; Tier B/C are intentionally design-stage (separable
  subsystems with open decisions) per the writing-plans multi-subsystem guidance — flagged as needing sign-off, not
  left as silent TODOs.
- **Type consistency:** `validate_structural(require_directional=...)` signature is used identically at both call
  sites; `check_upstream_freshness`/`record_watermarks`/`resolve_upstream_tables_from_card` match
  `ingestion.guards` signatures.

> **Executor note:** verify exact line numbers before editing (the cited lines are from 2026-06-28 HEAD), and
> confirm the `wf-xt-grids.yaml` input format against a live watermark card (`wf-dbt-build-output-marts`,
> `wf-hf-sync`) — `resolve_upstream_tables_from_card` reads `inputs.tables`/`inputs.datasets` where
> `source == "delta-table"`.
