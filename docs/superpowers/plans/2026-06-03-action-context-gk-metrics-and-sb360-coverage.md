# Action-Context GK Metrics + SB360 Coverage — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Persist xShotOccurrence + the full `gk_influence` zone set on `fct_action_context` for tracking providers, and expand the StatsBomb-360 freeze-frame path to every action-context metric the data empirically supports — with explicit pitch-control-method provenance.

**Architecture:** Pure-pandas enrich changes in `_enrich_tracking_match` / `_enrich_sb360_match`; 6 new columns flow through the DDL-single-source schema → applyInPandas StructType → bronze → dbt staging/mart → HF (`SELECT *`). SB360 pitch-control-dependent metrics use `voronoi` (position-only); a new `pitch_control_method` column records provenance. Migration applied operator-side (CI re-wiring is a separate PR).

**Tech Stack:** Python 3.10, pandas, silly-kicks 4.9.1 (`add_xshot_occurrence`, `add_gk_influence`; 4.9.1 adds the DAS empty-frame-batch fix), xgboost-cpu 3.2.0, dbt (Databricks), the AC-1 hexagon (`run_work_unit`).

**Spec:** `docs/superpowers/specs/2026-06-03-action-context-gk-metrics-and-sb360-coverage-design.md`

**Branch:** `feat/ac1-gk-metrics-sb360-coverage` (off `main` @ `1568a5b`).

> **GIT DISCIPLINE (read first):** This is ONE feature = ONE commit = ONE PR. Tasks below **stage only** (`git add`); they do NOT commit. A single commit is proposed at the very end (Task C5) and requires explicit user approval (CLAUDE.md §Git Workflow — commit is a user-gated action). Do not run `git commit` mid-plan.

> **LOCATING EDITS:** Line numbers below are approximate (`~Lnnn`). Locate edits by **anchor column name** (`gk_closing_time_min_s__six_yard_box`, `ghost_gk_spread`), not line number.

---

## File structure (created / modified)

| File | Change |
|------|--------|
| `src/analytics/action_context/schema.py` | +6 cols to `RESULT_COLUMNS` + `ACTION_CONTEXT_DDL`; bump master tally L17 (104→110) |
| `src/analytics/action_context/enrich.py` | tracking: `zone_names=`, explicit `method/pitch_control_method="spearman"`, `add_xshot_occurrence`, `pitch_control_method='spearman'`; sb360: reconstruct `xt` caller-side + 7 steps + `'voronoi'` |
| `src/analytics/action_context/pipeline.py` | sb360 branch: `_reconstruct_xt(...)` + pass `xt` into `_enrich_sb360_match` |
| `src/tests/action_context/oracle_map.py` | `INVARIANT_ONLY` += 5 numeric + 1 categorical |
| `dbt_project/models/staging/action_context/stg_action_context__values.sql` | +6 `cast(...)` |
| `dbt_project/models/marts/fct_action_context.sql` | +6 passthrough in BOTH select blocks (action_raw CTE + final, ~L89 / ~L225) |
| `dbt_project/models/marts/_marts__models.yml` | +6 contract cols in the **single** `fct_action_context` block (anchor: its `gk_closing_time_min_s__six_yard_box` / `ghost_gk_spread`) |
| `scripts/migrations/2026-06-03-add-xshot-gk-zones-to-action-context.sql` | Create — idempotent ALTER ADD 6 cols |
| `scripts/extract_action_context_fixture.py` | Extend — emit `sb360.parquet` for statsbomb (durable fixture recipe) |
| `scripts/build_ac1_full_golden.py` | Create — committed full-golden regen (mirrors `build_ac1_mini_golden.py`) |
| `src/tests/test_action_context_enrichment.py` | +tracking assertions on `J03WMXmini` (6 cols, ranges, unpatched xS) |
| `src/tests/action_context/test_sb360_coverage.py` | Create — SB360 supported-vs-NULL (test-first) |
| `src/tests/fixtures/action_context/statsbomb/3835328/{actions,sb360,xt_grid,meta}.parquet` | Create |
| `src/tests/fixtures/action_context/idsse/{J03WMX_p1,J03WMXmini_p1}/golden.parquet` | Re-baseline |
| `docs/superpowers/adrs/ADR-039-*.md` | Create |
| `ARCHITECTURE.md`, `NOTICE`, `workflow-cards/wf-action-context.yaml`, `src/tests/test_architecture_md_appendix.py` | xS academic reference |
| `docs/huggingface/dataset-cards/spadl-action-context.md` | provenance + sparsity notes |
| `pyproject.toml` + consumers (via `bump_wheel.py`) | 0.5.14 → 0.5.15 |

---

## Phase A — GK metrics on the tracking path + schema + provenance

### Task A1: Schema — add the 6 columns (DDL single source)

**Files:** Modify `src/analytics/action_context/schema.py`; Test `src/tests/test_action_context_createdataframe_schema.py`.

- [ ] **Step 1: Failing test** — append:

```python
def test_new_gk_and_xshot_columns_present():
    from analytics.action_context.schema import RESULT_COLUMNS, ACTION_CONTEXT_DDL
    new_cols = [
        "gk_closing_time_mean_s__near_post", "gk_closing_time_min_s__near_post",
        "gk_closing_time_mean_s__far_post", "gk_closing_time_min_s__far_post",
        "xshot_occurrence", "pitch_control_method",
    ]
    for c in new_cols:
        assert c in RESULT_COLUMNS, f"{c} missing from RESULT_COLUMNS"
        assert c in ACTION_CONTEXT_DDL, f"{c} missing from ACTION_CONTEXT_DDL"
    assert "pitch_control_method STRING" in ACTION_CONTEXT_DDL
    assert "xshot_occurrence DOUBLE" in ACTION_CONTEXT_DDL
```

- [ ] **Step 2: Run — expect FAIL.** `uv run pytest src/tests/test_action_context_createdataframe_schema.py::test_new_gk_and_xshot_columns_present -v`

- [ ] **Step 3:** In `RESULT_COLUMNS`, after the `gk_closing_time_min_s__six_yard_box` anchor add (mean→min order), and update the `# GK influence (4)` comment to `(8)`:

```python
    "gk_closing_time_mean_s__near_post",
    "gk_closing_time_min_s__near_post",
    "gk_closing_time_mean_s__far_post",
    "gk_closing_time_min_s__far_post",
```
Before `# Audit (1)` / `"_ingested_at"` add:
```python
    # xShotOccurrence (Pipping-Gamón, Feng & Sabin 2026) (1)
    "xshot_occurrence",
    # Pitch-control provenance for persisted pitch-control-derived metrics (1)
    "pitch_control_method",
```

- [ ] **Step 4:** In `ACTION_CONTEXT_DDL`, after the `gk_closing_time_min_s__six_yard_box DOUBLE` anchor:
```python
    "gk_closing_time_mean_s__near_post DOUBLE, gk_closing_time_min_s__near_post DOUBLE, "
    "gk_closing_time_mean_s__far_post DOUBLE, gk_closing_time_min_s__far_post DOUBLE, "
```
After the `ghost_gk_spread DOUBLE, ` anchor (before `_ingested_at TIMESTAMP`):
```python
    "xshot_occurrence DOUBLE, pitch_control_method STRING, "
```

- [ ] **Step 5:** Bump the master count comment at `schema.py:17` (e.g. `# ... = 104` → `110`) — L2 cleanliness.

- [ ] **Step 6: Run — expect PASS** (new test + existing DDL↔StructType parity; STRING handled by `_ddl_string_columns`):
`uv run pytest src/tests/test_action_context_createdataframe_schema.py -v`

- [ ] **Step 7: Stage** (no commit): `git add src/analytics/action_context/schema.py src/tests/test_action_context_createdataframe_schema.py`

### Task A2: enrich.py tracking — gk zones + xShotOccurrence + provenance (explicit method)

**Files:** Modify `src/analytics/action_context/enrich.py`; Test `src/tests/test_action_context_enrichment.py`.

- [ ] **Step 1: Failing test** — uses the fast **mini** fixture (H2), real unpatched bundled xS model:

```python
def test_tracking_mini_gains_gk_zones_xshot_and_provenance():
    import pandas as pd
    from analytics.action_context.local.parquet_sources import (
        ParquetActionsSource, ParquetFrameSource, ParquetMatchMetadataSource, ParquetXtSource)
    from analytics.action_context.pipeline import run_work_unit
    from analytics.action_context.work_unit import WorkUnit

    class _C:
        df = None
        def write(self, wu, r): self.df = r; return len(r)
    root = "src/tests/fixtures/action_context"; s = _C()
    run_work_unit(WorkUnit(provider="idsse", match_id="J03WMXmini", period=1),
                  frames=ParquetFrameSource(root), actions=ParquetActionsSource(root),
                  xt=ParquetXtSource(root), meta=ParquetMatchMetadataSource(root), sink=s)
    df = s.df
    for c in ("gk_closing_time_mean_s__near_post", "gk_closing_time_min_s__near_post",
              "gk_closing_time_mean_s__far_post", "gk_closing_time_min_s__far_post",
              "xshot_occurrence", "pitch_control_method"):
        assert c in df.columns, f"{c} missing"
    xs = pd.to_numeric(df["xshot_occurrence"], errors="coerce").dropna()
    if len(xs):
        assert ((xs >= 0) & (xs <= 1)).all()
    assert set(df["pitch_control_method"].dropna().unique()) <= {"spearman"}
```

- [ ] **Step 2: Run — expect FAIL.** `uv run pytest src/tests/test_action_context_enrichment.py::test_tracking_mini_gains_gk_zones_xshot_and_provenance -v`

- [ ] **Step 3:** Edit Step 13 `add_gk_influence` — add `zone_names` + explicit `method="spearman"` (L1, self-documenting):
```python
    out = add_gk_influence(
        out, tracking_df, xt, links=links, home_team_id=home_team_id,
        pitch_control_cache=pc_cache, method="spearman",
        zone_names=["six_yard_box", "near_post", "far_post"],
    )
```

- [ ] **Step 4:** Make the existing tracking `add_obso` / `add_pausa` calls pass `pitch_control_method="spearman"` explicitly (behaviour-neutral — it is the current default; keeps the provenance label honest, L1).

- [ ] **Step 5:** Append the xShotOccurrence step at the end of the tracking chain (after `add_sync_score`), importing `add_xshot_occurrence` from `silly_kicks.tracking`:
```python
    # xShotOccurrence (xS) — P(shot attempted); Pipping-Gamón, Feng & Sabin (2026), arXiv:2512.00203.
    # Bundled default XGBoost (model=None; no network, serverless-safe). Reuse the shared PC cache.
    out = add_xshot_occurrence(
        out, tracking_df, model=None, links=links, home_team_id=home_team_id, pitch_control_cache=pc_cache)
    out["pitch_control_method"] = "spearman"
```

- [ ] **Step 6: Run — expect PASS.** `uv run pytest src/tests/test_action_context_enrichment.py::test_tracking_mini_gains_gk_zones_xshot_and_provenance -v`

- [ ] **Step 7: Stage:** `git add src/analytics/action_context/enrich.py src/tests/test_action_context_enrichment.py`

### Task A3: oracle_map range/invariant checks

**Files:** Modify `src/tests/action_context/oracle_map.py`.

- [ ] **Step 1:** Add to `INVARIANT_ONLY`:
```python
    "gk_closing_time_mean_s__near_post": ("float", 0.0, None),
    "gk_closing_time_min_s__near_post": ("float", 0.0, None),
    "gk_closing_time_mean_s__far_post": ("float", 0.0, None),
    "gk_closing_time_min_s__far_post": ("float", 0.0, None),
    "xshot_occurrence": ("float", 0.0, 1.0),
    "pitch_control_method": ("categorical", None, None),
```

- [ ] **Step 2: Confirm interim safety (L4).** `build_oracle_specs(ac_columns=list(golden_df.columns), …)` only builds a spec for a column present in the golden, so these new entries are **dormant** (no `KeyError`) until the golden carries them (Task A6). Run now to confirm green:
`uv run pytest src/tests/action_context/test_differential.py -v` → PASS (new cols skipped).

- [ ] **Step 3: Stage:** `git add src/tests/action_context/oracle_map.py`

### Task A4: dbt staging + mart + contract

**Files:** Modify `stg_action_context__values.sql`, `fct_action_context.sql`, `_marts__models.yml`.

- [ ] **Step 1: staging** — after `cast(gk_closing_time_min_s__six_yard_box …)`:
```sql
        cast(gk_closing_time_mean_s__near_post as double) as gk_closing_time_mean_s__near_post,
        cast(gk_closing_time_min_s__near_post as double) as gk_closing_time_min_s__near_post,
        cast(gk_closing_time_mean_s__far_post as double) as gk_closing_time_mean_s__far_post,
        cast(gk_closing_time_min_s__far_post as double) as gk_closing_time_min_s__far_post,
```
after `cast(ghost_gk_spread …)`:
```sql
        cast(xshot_occurrence as double) as xshot_occurrence,
        cast(pitch_control_method as string) as pitch_control_method,
```

- [ ] **Step 2: mart `.sql` — BOTH select blocks** (action_raw CTE + final). After the `gk_closing_time_min_s__six_yard_box` anchor in each:
```sql
        gk_closing_time_mean_s__near_post,
        gk_closing_time_min_s__near_post,
        gk_closing_time_mean_s__far_post,
        gk_closing_time_min_s__far_post,
```
after the `ghost_gk_spread` anchor in each:
```sql
        xshot_occurrence,
        pitch_control_method,
```
(Match each block's trailing-comma convention.)

- [ ] **Step 3: contract `.yml` — the SINGLE `fct_action_context` block** (M3 — the file's other gk-column block is `fct_tracking_context`; do NOT touch it). After its `gk_closing_time_min_s__six_yard_box` entry:
```yaml
      - name: gk_closing_time_mean_s__near_post
        data_type: double
      - name: gk_closing_time_min_s__near_post
        data_type: double
      - name: gk_closing_time_mean_s__far_post
        data_type: double
      - name: gk_closing_time_min_s__far_post
        data_type: double
```
after its `ghost_gk_spread` entry:
```yaml
      - name: xshot_occurrence
        data_type: double
      - name: pitch_control_method
        data_type: string
```

- [ ] **Step 4: YAML sanity** — `uv run python -c "import yaml; yaml.safe_load(open('dbt_project/models/marts/_marts__models.yml')); print('YAML_OK')"` → `YAML_OK`.

- [ ] **Step 5: Stage:** `git add dbt_project/models/staging/action_context/stg_action_context__values.sql dbt_project/models/marts/fct_action_context.sql dbt_project/models/marts/_marts__models.yml`

### Task A5: bronze migration

**Files:** Create `scripts/migrations/2026-06-03-add-xshot-gk-zones-to-action-context.sql`.

- [ ] **Step 1: Write it** (idempotent; operator-applied per §11/§13):
```sql
-- Adds xShotOccurrence, gk_influence near/far-post closing-time zones, and the
-- pitch_control_method provenance column to bronze.spadl_action_context.
-- Idempotent: ALTER ... ADD COLUMNS is skip-if-exists handled by _runner.py.
ALTER TABLE soccer_analytics.bronze.spadl_action_context ADD COLUMNS (
  gk_closing_time_mean_s__near_post DOUBLE,
  gk_closing_time_min_s__near_post DOUBLE,
  gk_closing_time_mean_s__far_post DOUBLE,
  gk_closing_time_min_s__far_post DOUBLE,
  xshot_occurrence DOUBLE,
  pitch_control_method STRING
);
```

- [ ] **Step 2: Stage:** `git add scripts/migrations/2026-06-03-add-xshot-gk-zones-to-action-context.sql`

### Task A6: committed full-golden builder + re-baseline both goldens

**Files:** Create `scripts/build_ac1_full_golden.py`; Modify the two `golden.parquet`.

- [ ] **Step 1: Write the committed full-golden builder** (M2 — durable recipe, mirrors `scripts/build_ac1_mini_golden.py`):
```python
"""Regenerate the full AC-1 golden (idsse J03WMX p1) via the real run_work_unit chain.

    uv run python scripts/build_ac1_full_golden.py
    git add src/tests/fixtures/action_context/idsse/J03WMX_p1/golden.parquet
"""
from __future__ import annotations
import pandas as pd
from analytics.action_context.local.parquet_sources import (
    ParquetActionsSource, ParquetFrameSource, ParquetMatchMetadataSource, ParquetXtSource)
from analytics.action_context.pipeline import run_work_unit
from analytics.action_context.work_unit import WorkUnit

_ROOT = "src/tests/fixtures/action_context"
_DST = _ROOT + "/idsse/J03WMX_p1/golden.parquet"

def main() -> None:
    class _C:
        df = None
        def write(self, wu, r): self.df = r; return len(r)
    s = _C()
    run_work_unit(WorkUnit(provider="idsse", match_id="J03WMX", period=1),
                  frames=ParquetFrameSource(_ROOT), actions=ParquetActionsSource(_ROOT),
                  xt=ParquetXtSource(_ROOT), meta=ParquetMatchMetadataSource(_ROOT), sink=s)
    assert s.df is not None
    s.df.to_parquet(_DST, index=False)
    print(f"froze {_DST}: {len(s.df)} rows x {len(s.df.columns)} cols")

if __name__ == "__main__":
    main()
```

- [ ] **Step 2: Confirm the recompute differs from the frozen golden (RED):**
`AC1_E2E=1 uv run pytest src/tests/action_context/test_e2e.py -v` → FAIL (column-set mismatch: +6 cols).

- [ ] **Step 3: Regenerate the full golden** (fft-cic backend, current default):
`uv run python scripts/build_ac1_full_golden.py` → `froze … 97 rows x 109 cols`.

- [ ] **Step 4: Review the diff before trusting (L-NEW3 / capture-before-cleanup):**
```bash
uv run python -c "import pandas as pd; g=pd.read_parquet('src/tests/fixtures/action_context/idsse/J03WMX_p1/golden.parquet'); print([c for c in g.columns if 'near_post' in c or c in ('xshot_occurrence','pitch_control_method')]); print('xshot non-null', g['xshot_occurrence'].notna().sum()); print('pcm', g['pitch_control_method'].dropna().unique())"
```
Expected: 6 new columns present; xshot non-null > 0; `pcm == ['spearman']`.

- [ ] **Step 5: Regenerate the mini golden:** `uv run python scripts/build_ac1_mini_golden.py`

- [ ] **Step 6: Gates:** `AC1_E2E=1 uv run pytest src/tests/action_context/test_e2e.py src/tests/action_context/test_mini_golden.py src/tests/action_context/test_differential.py -v` → PASS.

- [ ] **Step 7: Stage:** `git add scripts/build_ac1_full_golden.py src/tests/fixtures/action_context/idsse/J03WMX_p1/golden.parquet src/tests/fixtures/action_context/idsse/J03WMXmini_p1/golden.parquet`

---

## Phase B — SB360 freeze-frame coverage (test-first)

### Task B1: extend the extract tool, build + commit the SB360 fixture, write RED test

**Files:** Modify `scripts/extract_action_context_fixture.py`; Create `src/tests/action_context/test_sb360_coverage.py` + the fixture parquets.

- [ ] **Step 1: Extend `extract_action_context_fixture.py`** (M2 — durable recipe) to emit `sb360.parquet` when `--provider statsbomb` and `bronze.statsbomb_360` has rows for the match: pull `id, teammate, keeper, location` from `statsbomb_360`, build the snapshot rows (`action_id, team_id, is_goalkeeper, x, y`) exactly as `ingestion.action_context._run_sb360_enrichment` does (map `original_event_id`→`action_id`; team via teammate flag; parse `location` JSON), and write `sb360.parquet`. Also write `xt_grid.parquet` (already pulled) + `meta.parquet` (`home_team_id` = first team).

- [ ] **Step 2: Build the committed fixture** (slice to the first ~150 snapshot-bearing actions to keep it small):
`DATABRICKS_SQL_WAREHOUSE_ID=$(…from DATABRICKS_HTTP_PATH…) uv run python scripts/extract_action_context_fixture.py --provider statsbomb --match-id 3835328 --no-oracles --max-actions 150`
Expected: `statsbomb/3835328/{actions,sb360,xt_grid,meta}.parquet` written, `sb360.parquet` non-empty. (Add a `--max-actions` arg if the tool lacks one.)

- [ ] **Step 3: Write the RED test** — `src/tests/action_context/test_sb360_coverage.py`:
```python
import pandas as pd
from analytics.action_context.local.parquet_sources import (
    ParquetActionsSource, ParquetFrameSource, ParquetMatchMetadataSource, ParquetXtSource)
from analytics.action_context.pipeline import run_work_unit
from analytics.action_context.work_unit import WorkUnit

def _run():
    class _C:
        df = None
        def write(self, wu, r): self.df = r; return len(r)
    root = "src/tests/fixtures/action_context"; s = _C()
    run_work_unit(WorkUnit(provider="statsbomb", match_id="3835328", period=None),
                  frames=ParquetFrameSource(root), actions=ParquetActionsSource(root),
                  xt=ParquetXtSource(root), meta=ParquetMatchMetadataSource(root), sink=s)
    return s.df

def test_sb360_supported_metrics_populate():
    df = _run()
    def nn(c): return int(pd.to_numeric(df[c], errors="coerce").notna().sum())
    assert nn("ghost_gk_x") > 0
    assert nn("obso_actual") > 0
    assert nn("pausa_composite") > 0
    assert nn("gk_pitch_control_share_weighted") > 0
    assert nn("gk_closing_time_mean_s__near_post") > 0
    assert df["shape_graph_density_defending"].notna().any()
    assert nn("xshot_occurrence") > 0
    assert set(df["pitch_control_method"].dropna().unique()) <= {"voronoi"}

def test_sb360_unsupported_metrics_null():
    df = _run()
    for c in ("das_diff", "blocking_score", "space_created_m2_team", "elastic_confidence"):
        assert pd.to_numeric(df[c], errors="coerce").notna().sum() == 0, f"{c} unexpectedly populated"
```

- [ ] **Step 4: Run — expect partial FAIL (RED):** `uv run pytest src/tests/action_context/test_sb360_coverage.py -v`
Expected: `supported` FAILS (cols NULL today); `unsupported` PASSES.

- [ ] **Step 5: Stage:** `git add scripts/extract_action_context_fixture.py src/tests/action_context/test_sb360_coverage.py src/tests/fixtures/action_context/statsbomb/3835328/`

### Task B2: wire supported steps into `_enrich_sb360_match` (GREEN)

**Files:** Modify `src/analytics/action_context/pipeline.py` (sb360 branch) + `src/analytics/action_context/enrich.py` (`_enrich_sb360_match`).

- [ ] **Step 0: Thread `xt` into the sb360 branch (M1).** In `pipeline.py`, the sb360 branch (~L206-211) calls `_enrich_sb360_match(actions, frames_pdf, meta.home_team_id)` with **no `xt` in scope** (`_reconstruct_xt` at ~L214 is tracking-only). Add `xt = _reconstruct_xt(xt_grid_data, xt_l, xt_w)` inside the sb360 branch (the grid params are already `enrich_batch` arguments) and change the call to `_enrich_sb360_match(actions, frames_pdf, meta.home_team_id, xt)`. Update the `_enrich_sb360_match` signature to accept `xt`.

- [ ] **Step 1: Append the supported steps** in `_enrich_sb360_match` after Step 5 (`add_team_shape`), before `return out`. Extend imports: add `add_pressure_on_actor, add_shape_graph, add_obso, add_pausa` to the `from silly_kicks.tracking import (...)` group; add `from silly_kicks.tracking.features import add_ghost_gk, add_gk_influence`; `from silly_kicks.tracking import add_xshot_occurrence`. (NOT `add_cover_shadows` — measured 0.)
```python
    # SB360 coverage (spec §3.2): single-frame-supportable metrics; pitch-control-dependent ones
    # use voronoi (no velocity on freeze-frames). All partial/sparse — honest NULL.
    out = add_pressure_on_actor(out, frames, links=links)
    out = add_shape_graph(out, frames, links=links, home_team_id=home_team_id)
    out = add_ghost_gk(out, frames, model="default", links=links,
                       home_team_id=home_team_id, actions_for_context=out, kde_backend="fft-cic")
    out = add_gk_influence(out, frames, xt, links=links, home_team_id=home_team_id,
                           method="voronoi", zone_names=["six_yard_box", "near_post", "far_post"])
    out = add_obso(out, frames, links=links, home_team_id=home_team_id, pitch_control_method="voronoi")
    out = add_pausa(out, frames, links=links, home_team_id=home_team_id, pitch_control_method="voronoi")
    out = add_xshot_occurrence(out, frames, model=None, links=links, home_team_id=home_team_id)
    out["pitch_control_method"] = "voronoi"
```

- [ ] **Step 2: Run — expect PASS (GREEN):** `uv run pytest src/tests/action_context/test_sb360_coverage.py -v` → both PASS.

- [ ] **Step 3: No tracking regression:** `AC1_E2E=1 uv run pytest src/tests/action_context/test_e2e.py -v` → PASS.

- [ ] **Step 4: Stage:** `git add src/analytics/action_context/pipeline.py src/analytics/action_context/enrich.py`

---

## Phase C — governance, references, wheel, ADR, final gate + single commit

### Task C1: xShotOccurrence academic reference + dataset card

**Files:** `ARCHITECTURE.md`, `src/tests/test_architecture_md_appendix.py`, `NOTICE`, `workflow-cards/wf-action-context.yaml`, `docs/huggingface/dataset-cards/spadl-action-context.md`.

- [ ] **Step 1: Verify the canonical author** (M-NEW1) against arXiv:2512.00203:
`uv run python -c "import urllib.request; print(urllib.request.urlopen('https://arxiv.org/abs/2512.00203', timeout=30).read().decode())" 2>&1 | grep -io "pipping[- ]*gam[oó]n" | head` → confirms `Pipping-Gamón`.
- [ ] **Step 2:** Add the Appendix-D row to `ARCHITECTURE.md` § "D. Academic References": `Pipping-Gamón, Feng & Sabin (2026). "Beyond Expected Goals: A Probabilistic Framework for Shot Occurrences in Soccer." arXiv:2512.00203. — xShotOccurrence (xS) in fct_action_context.`
- [ ] **Step 3:** Add `"Pipping"` to `expected_authors` in `src/tests/test_architecture_md_appendix.py` (substring match).
- [ ] **Step 4:** Add the same citation to `NOTICE` (academic-references section).
- [ ] **Step 5:** Create a `references:` block in `workflow-cards/wf-action-context.yaml` (none today):
```yaml
references:
  - "Pipping-Gamón, Feng & Sabin (2026). Beyond Expected Goals: A Probabilistic Framework for Shot Occurrences in Soccer. arXiv:2512.00203"
```
- [ ] **Step 6:** `docs/huggingface/dataset-cards/spadl-action-context.md` — add the `pitch_control_method` provenance note (spearman=tracking / voronoi=SB360) + SB360 sparsity caveat (xshot ~4% on SB360; metrics are a non-random subsample — no naive provider averages).
- [ ] **Step 7: Run** `uv run pytest src/tests/test_architecture_md_appendix.py src/tests/test_ai_governance_md.py -v` → PASS. (H1: `wf-action-context` is NOT added to `PER_PLAYER_EVALUATIVE_CARDS` — xS is per-action; no governance change.)
- [ ] **Step 8: Stage:** `git add ARCHITECTURE.md NOTICE workflow-cards/wf-action-context.yaml src/tests/test_architecture_md_appendix.py docs/huggingface/dataset-cards/spadl-action-context.md`

### Task C2: ADR-039

- [ ] **Step 1:** Create `docs/superpowers/adrs/ADR-039-action-context-gk-metrics-and-sb360-coverage.md` (Nygard format per `ADR-TEMPLATE.md`): (a) xShotOccurrence adoption (Pipping-Gamón et al. 2026); (b) gk_influence zone expansion; (c) **cross-provider pitch-control method divergence (spearman tracking / voronoi SB360) persisted into shared columns + the `pitch_control_method` provenance column**; (d) SB360 coverage + sparsity caveat; (e) migration operator-applied + CI re-wiring deferred. Status: Accepted.
- [ ] **Step 2: Stage:** `git add docs/superpowers/adrs/ADR-039-action-context-gk-metrics-and-sb360-coverage.md`

### Task C3: wheel bump + boundary test

- [ ] **Step 1:** Bump `pyproject.toml` `version` 0.5.14 → 0.5.15, then `uv run python scripts/bump_wheel.py && uv run python scripts/bump_wheel.py --check` → "All files consistent at version 0.5.15."
- [ ] **Step 2:** `uv run pytest src/tests/test_silly_kicks_boundary.py -v` → PASS (add assertions if it enumerates the AC-1 silly-kicks surface, e.g. `add_xshot_occurrence`).
- [ ] **Step 3: Stage:** `git add pyproject.toml src/shared/wheel.py scripts/ deploy.sh terraform/ src/tests/test_silly_kicks_boundary.py`

### Task C4: full gate

- [ ] **Step 1:** `uv run ruff check src/ scripts/ && uv run ruff format --check src/ scripts/ && uv run pyright src/ && uv run pytest src/tests/` → all clean. (Includes `test_topandas_boundedness`; if an exempted `.toPandas()` line shifted in an edited file, update `_topandas_exemptions.yml` and re-run.)
- [ ] **Step 2:** `AC1_E2E=1 uv run pytest src/tests/action_context/test_e2e.py -v` → PASS.
- [ ] **Step 3: Stage any fixups:** `git add -A`

### Task C5: single feature commit (USER-GATED)

- [ ] **Step 1: Show the staged set** for review: `git status --short && git diff --cached --stat`
- [ ] **Step 2: PROPOSE the single commit and await explicit user approval** (do NOT auto-run). Proposed message:
```
feat(ac-1): add xShotOccurrence + gk_influence zones + SB360 freeze-frame coverage (ADR-039)
```
Only on the user's explicit go-ahead, create the one commit (with the `Co-Authored-By` trailer). Push / PR are separately gated.

---

## Post-merge runbook (operator — spec §13)
1. Merge → wait for post-merge CI (wheel 0.5.15 deploy).
2. **Apply the bronze migration** `ALTER TABLE bronze.spadl_action_context ADD COLUMNS (…)` (via `_runner.py`) — BEFORE steps 3–4.
3. Re-run AC-1 compute (writer schema now includes the 6 columns).
4. Live `dbt build` (staging/mart + contract). 5. Synced-table refresh + HF publish auto-include the columns.

## Out of scope (separate PRs)
- CI migration-runner re-wiring (spec §11 options 1/2).
- SB360 `pre_shot_gk_position/angle = 0` anomaly.
- Publishing the spadl-action-context HF dataset; the broader recompute (pending silly-kicks DAS-empty-batch fix).
