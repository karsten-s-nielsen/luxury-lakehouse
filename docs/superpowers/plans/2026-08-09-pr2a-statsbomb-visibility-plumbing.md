# PR-2a — StatsBomb visibility plumbing + GS signal closure Implementation Plan (rev 4)

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Thread a real per-match `visibility` from StatsBomb ingestion through SPADL to `dim_matches` so the PR-2b allowlist flip has something true to flip against — and, in the same cycle, close the Gradient Sports signal gap that R-6a would otherwise render invisible.

**Architecture:** Two units, two commits, one cycle. **Unit A** is semantically inert: StatsBomb rows are `public` before and after. **Unit B** is a deliberate semantic change with a measured before/after tier count. They are separate commits because a tier movement hidden inside a unit that promises inertness is the exact silent-change class this repo has been eliminating all week — not because the GS work is optional.

**Tech Stack:** Python 3.10, PySpark (Databricks serverless), dbt, pytest.

## Scope added during execution (operator-approved) — the cross-provider ADR-064 gap

Not in any revision of this plan; folded in on 2026-08-09 after measurement, at the operator's direction.

**Gradient Sports was not the only provider without a bronze-coverage gate.** `idsse` and `skillcorner` had none either — the same state that let GS's `visibility`/`access_tier` go undocumented. Measured against live bronze: **329 undocumented columns** across the two, zero phantoms.

Six of those are the ADR-064 redistribution/audit columns, and one is the sharpest instance in the repo:

| Table | Columns | Coupling |
|---|---|---|
| `skillcorner_matches` | `visibility`, `access_tier` | sources.yml only (no staging model) |
| `skillcorner_tracking` | `access_tier` | + staging SQL + `_models.yml` |
| `idsse_tracking` | `access_tier`, `_ingested_at` | + staging SQL + `_models.yml` |
| `idsse.elastic_sync_results` | `_ingested_at` | sources.yml only |

**`skillcorner_matches.visibility` is the column that decides whether a private Real Madrid match may reach a public HF repo** (ADR-049 / ADR-064), and nothing documented or watched it. That is the same defect as GS, in the provider where the consequence is worst — so it belongs in *this* PR, whose entire subject is making that signal real and gated.

All six landed; live re-measure **329 → 323**, four tables at `MISSING=0`. The residual 323 is exactly two bulk tables — `skillcorner_events` (288 of 295 undocumented) and `idsse_events` (35) — deliberately left as the next cycle's scope together with a `--check`-able `sources.yml` generator on the `scripts/sync_tf_env_pins.py` pattern (pure core so fixer ≡ checker, gated by a parity test). Bundling ~1,000 lines of unrelated provider documentation into a StatsBomb-visibility PR would make it unreviewable; the measurement is recorded so the next cycle starts sized.

> **A splice bug worth remembering.** The first attempt auto-detected the table indentation and matched `- name: skillcorner` — the **source** at indent 2 — instead of the table entries at indent 6. Block boundaries were wrong, so `idsse_tracking`'s two columns were appended at EOF under `elastic_sync_results` while the script reported "+2 col(s)". Both files were still valid YAML. It was caught only by reading placement back **structurally** (per-table membership + duplicate check), not by trusting the script's own success message — the same discipline that made the GS backfill's `SUCCESS` worth re-verifying against `_ingested_at`.

## Execution findings (2026-08-09) — defects the plan did not predict

Recorded here because they were found by *doing* the work, and the same traps recur in Unit B.

**EX-1 — the StatsBomb matches projection is an explicit column list, and it drops the signal.** `spadl_conversion.py:361` reads `spark.table(matches_table).select("match_id", "home_team").toPandas()`. Task 4's `visibility_map` is built from that frame, so as first written it was **permanently empty** and every StatsBomb match threaded `None` — the Finding-5 over-restriction, shipped silently. The projection now names `visibility` explicitly.

**This is the same defect class as Finding 3's GS `_gs_needed_bronze_columns()` backtick projection** — two providers, two explicit column lists, one trap — and it was walked into *while documenting the other one*. Regression-gated by `test_statsbomb_matches_projection_includes_visibility`.

**EX-2 — a tolerant guard converted a loud failure into a silent one.** The first draft wrapped the map build in `if "visibility" in all_matches_pdf.columns:`. That guard is what would have made EX-1 invisible: missing column → empty map → every row restricted, with no error. Removed; the projection now makes the column a hard precondition and `zip(..., strict=True)` keeps it that way. **A defensive guard around a signal you require is not defensive — it is a silent-degradation switch** (CLAUDE.md: fail loudly).

**EX-4 — the migration's own OQ-1 instruction was impossible, and only executing it revealed that.** It directed the operator to run `WHERE visibility IS NULL OR visibility <> 'public'` **after the ALTERs and before the UPDATEs**, expecting `0`. At that instant the column exists and every one of the 3,464 rows is NULL, so it returns **3,464** — the check was scheduled at the one moment it cannot pass. It conflated two distinct obligations:

- **Precondition (OQ-1)** — "no commercially-licensed rows exist". Pre-migration there is *no `visibility` column*, so this can never be a column query. It is a **provenance** question, answered by the competition inventory: every row must belong to a StatsBomb free/open release. Verified 2026-08-10: 3,464 rows / 21 competitions, all open-data, no club-subscription competition.
- **Postcondition** — the same count run *after* the UPDATEs, proving the stamp reached every row. Verified: **0** on both columns, 3,464 rows `visibility='public'`.

**Three reviews and a final review passed over this**, because every one of them read the migration and none executed it. That is the `dbt-live-ci` shim failure mode in miniature: an artifact whose correctness is only observable at run time is covered by no amount of reading.

**EX-5 — `_runner.py` failed at client construction, and the failure looked like a partial apply.** `~/.databrickscfg` holds both a `DEFAULT` and an `OAUTH` profile matching the workspace host, so a bare `WorkspaceClient()` raises `ValueError: … Use --profile`. Nothing had executed — but the operator sees it only *after* substituting `<CUTOFF>`, at the moment they believe the apply is under way. The runner now takes `--profile` (defaulting to `DATABRICKS_CONFIG_PROFILE`), **builds the client before reading the migration file**, and on that specific error exits with a message stating plainly that nothing was applied. Note the same bare construction exists in ~8 sibling operator scripts; fixing those is its own cycle.

**EX-3 — `N802` forbids emphasis-by-capitals in test names.** Every `test_..._REAL_...` / `..._BEFORE_...` name in this plan violated the enforced ruff naming rule. Names are lowercase in the shipped code; the emphasis lives in the docstrings.

**Two tests were added beyond the plan**, both guarding decisions the plan makes but did not gate: `test_stamp_access_tier_has_no_visibility_default` (R-6a at the *definition* — re-adding a default would leave every call-site assertion passing) and `test_statsbomb_is_not_a_confirmed_public_override` (D4 — the override that would defeat the PR-2b fail-safe).

## rev 4 — what review 3 changed (partial review: Task 2 Steps 4–6, Task 9 Step 4)

A **scoped** review of the two regions rev 3 introduced. Both carried defects; all four findings verified against the tree and adopted.

| # | Finding | Effect |
|---|---|---|
| **1** | **A sixth coupled edit, in no task.** `test_staging_coverage.py::test_bronze_col_coverage` reads the staging side from **`_statsbomb__models.yml`, not the SQL** (`coverage_utils.py:101`); `INITIAL_BRONZE_STAGING_GAPS = {}` (`:178`) is **locked empty** by `test_gaps_snapshot_is_empty` (`:241`); the case is registered at `:66`. rev 3 mentioned that file **zero times** | Task 2 Step 6 now edits the SQL **and** the models.yml. |
| **2** | Step 5's `sources.yml` edit turned that gate red at Task 2 and it **stayed red through Tasks 3–6**, surfacing only at Task 7's full gates. rev 3's Step 6 ran the two tests it was thinking about, not the ones the edit breaks | Staging edits **moved from Task 6 into Task 2**; every task boundary green. |
| **3** | Task 9's risk note warned about "tracking `STRING` vs events `INT`" — **tracking is not in this join** (it is metadata ↔ events), and the memory it came from is about `player_id`. The join is provably safe **statically** | Replaced with the real validator-asymmetry argument + its evidence chain. |
| **4** | **Step 2's re-ingest is a no-op whose stop condition records a false finding.** `_GradientSportsGuard` Phase A finds nothing missing; Phase B keys on **provider** re-processing, which a schema change on our side is not → `count=0`, guard skips, `visibility` stays NULL. rev 3 then read that as *"the feed supplies no visibility"* — impossible, since `MatchInfo.visibility: str` is **required** pydantic | Rewritten around `_backfill_artifacts` (*"skips the guard entirely"*), with the invocation surfaced as a fork and the stop condition corrected. |

**Finding 4 is the one that would have cost real time**, and it is the same defect class the plan had already caught once: Task 10 exists because StatsBomb *conversion* is incremental, and rev 3 then re-ingested GS as though *ingestion* were not. **Caught on one provider, missed on the other, in the same document.** A rule applied to one instance is not a rule applied.

**Surfaced, not scoped — needs your decision.** GS has **no bronze-coverage gate at all**: there is no `test_gradientsports_bronze_coverage.py`, and `visibility`/`access_tier` are absent from `_gradientsports__sources.yml` despite being stamped on bronze at ingest. That is why the drift survives while the identical class is rigorously enforced two directories over. It is a pre-existing gap PR-2a does not create or worsen. **Not added to this cycle unilaterally** — say whether it rides here or becomes a TODO row.

## rev 3 — what review 2 changed

Review 2's diagnosis is correct and is the organising fact of this revision:

> **The instruction that was verified is right; the instruction that was inferred from it is wrong.**

StatsBomb's *converter* was read → Q1's fix holds. StatsBomb's *ingest* was **not** read → rev 2 pointed at `ingest_competitions`. GS was assumed to have "the same shape" → rev 2 described a join that does not exist. **Four of the six anchors rev 2 introduced were wrong.** Before writing rev 3 the verification pass review 2 asked for was run over Unit B and Task 2, and it found one further defect **neither review caught** (D1-b).

| # | Finding | Effect |
|---|---|---|
| **D1** | `statsbomb.py:254` writes **`statsbomb_competitions`**, not matches; and the stamp was aimed at a **Spark** frame a pandas helper cannot take | Task 2 Step 4 now targets **`matches_pdf` before `finalize_bronze_df` at `:402`**. |
| **D1-b** | **NEW — found by the verification pass, in neither review.** `_STATSBOMB_MATCHES_EXPECTED_COLS` is **not a literal**: it is `expected_cols_from_snapshot(...)` over `src/tests/fixtures/statsbomb_bronze_schema_snapshot.json` (**25 cols, no `visibility`, no `access_tier`**), and `test_statsbomb_bronze_coverage.py` asserts snapshot ↔ `sources.yml` **both directions** (missing **and** phantom) | Task 2 is **four coupled edits in one commit**, not one. |
| **D2** | `_convert_gradientsports_from_bronze` (`:2524`) has **no `createDataFrame` lookup frame and no join** — `spark.table` → `filter` → backtick `.select()` → `groupBy.applyInPandas`. The cited `:782-786` is the **wyscout** converter | Task 9 specifies a **new** frame from `bronze.gradientsports_metadata`, joined **after** the projection. |
| **D3** | Task 3's AST gate requires *every* site to pass `visibility`, but GS is Unit B — so **Unit A ships a red suite**, contradicting "full gates before each commit" | Unit A sets the GS site to `visibility=None` with the R-6b comment: inert **and** green. |
| **D4** | Task 5 registered two preconditions against one override → `test_no_orphan_preconditions` fails. And adding a `statsbomb` override would be **actively wrong**: `default_tier_for_provider` returns the override *instead of* consulting the classifier, defeating the PR-2b fail-safe | `assert_no_commercial_statsbomb_rows` is **not registered**; its statement moves to Task 1's migration comment. |
| **E1** | `build_backfill_statements(*, ..., providers: tuple[str, ...])` is **keyword-only and takes a tuple** — the planned positional-list call raises `TypeError`. The test also asserted no precedence | One `build_backfill_plan(*, providers)`; precedence asserted on the emitted order. |
| **E2** | The B3 wiring test goes green if the helper appears **anywhere** in the 180-line function, including after the write | Compares `lineno`s against the `statsbomb_matches` write. |
| **E3** | A GS bronze re-ingest is the most consequential act in the plan and was a bare step | Added to the approval list, with the transient-window hazard stated. |
| **F1–F4** | Anchor drift (`:388` not `:387`; `:377-384` not `:376-383`), keyword-only idiom, `grep -c` counts lines, gate predicate too narrow | All corrected. |

## rev 2 — what review 1 changed

Reviewed by a parallel session against the tree; **every finding verified and adopted**. The corrections that changed the plan's shape:

| # | Finding | Effect |
|---|---|---|
| **A1** | Task 5's *recommended* branch (thread GS visibility) **moves tiers**, violating rev 1's own Global Constraint 1 | Split into Unit A (inert) + Unit B (measured). Neither deferred. |
| **A2** | rev 1 called `_scalar(conn, sql)` "this module's existing helper". **It does not exist.** `access_tier_backfill.py` imports only `shared.access_tier`, builds statement STRINGS, executes nothing, and has **no driver at all** | Preconditions become statement-builders, matching `build_backfill_statements`. |
| **A3** | `_PRECONDITIONS: frozenset[str]` binds a name to a name — delete the function, keep the name, test still passes. Same shape as the defect R-19 exists to close | Registry maps name → callable; both-directions coverage; a test that it is INVOKED. |
| **Q1** | rev 1 said "obtain visibility from the matches frame it already joins". `_make_sb_spadl_udf()` takes **no arguments**; `_udf(pdf)` gets the events group. That structure did not exist | Specified as a caller-side join, extending the **existing** `home_sdf` lookup at `:377-390` (anchor corrected in rev 3, F1). |
| **B1** | Task 4's assertion passed on `visibility=None` — which IS the Finding-5 defect it names | Added the negative assertion. |
| **B2** | Task 3's RED step cannot fire: the call sites are inside `_udf` closures that only run under Spark | One AST gate replaces three regexes, enumerating all six providers **both directions**. |
| **B3/B4** | Task 2 tested a pure helper the plan itself creates; the "survives the flip" test never constructed the flip | Wiring asserted; flip monkeypatched with both halves. |
| **B5** | The migration's `UPDATE ... WHERE visibility IS NULL` is idempotent by design, so it is **permanently re-runnable** — after PR-4 it would stamp a commercial row public | Bounded to a fixed row set. |
| **Q3** | Conversion is **incremental** (`filter(col("match_id").isin(new_game_ids))`), so existing SPADL rows never acquire the threaded value | Forced re-conversion named as Task 10, not left to the gate. |
| **C1–C4** | docstring enumeration, regex fragility, gate predicates, `SILLY_KICKS_ASSERT_INVARIANTS=1` | All folded in. |

**Also corrected: a stale premise in the spec itself.** R-6b says *"GS carries a real per-match visibility in bronze… so the GS SPADL leg discards a live signal"*. Measured 2026-08-09: `bronze.gradientsports_metadata` holds **64 rows, all `visibility=NULL`, all `access_tier=restricted`**. The pipeline carries the signal (`MatchInfo.visibility: str` is required-no-default and both callers pass it) but the stored rows predate the column. Threading alone would thread NULL. Unit B therefore **populates first, then threads** — Task 8 fixes the spec sentence so the next reader does not inherit it.

## Global Constraints

- **ONE feature branch, ONE commit, ONE PR.** *(Changed 2026-08-09 by operator decision; earlier revisions specified two commits.)* The Unit A / Unit B split remains a **narrative** structure — it orders the work and separates the inert plumbing from the measured signal change — but it is no longer a commit boundary. The reasoning for collapsing it: the inertness guarantee is delivered by the tests and the measured before/after counts, not by a commit boundary; and revert granularity was illusory anyway, because Unit B's substance is a **production bronze backfill** that reverting code does not unwind. Committing still requires **explicit user approval**; deciding the commit *shape* is not granting it.
- **Unit A is semantically inert.** Every StatsBomb row is `access_tier='public'` before and after. If a Unit A task changes a tier value, it is wrong.
- **Unit B deliberately moves tiers** and must report the count. A Unit B commit with no measured before/after is incomplete.
- **Do NOT flip `PUBLIC_BY_LICENSE_PROVIDERS`** — that is PR-2b, behind the live zero-NULL gate.
- **Migration ordering is load-bearing.** Bronze migrations have **no CI auto-apply**; apply **before or at merge, never after**. `dbt-live-ci.yml` is a daily scheduled live build and `dim_matches.sql` will select these columns. Verify with a live `DESCRIBE`.
- Line length 120. Ruff `E,W,F,I,N,UP,B,S,BLE,RUF` zero violations. `pyright` basic, zero errors.
  - **`N802` bites test names.** Emphasis-by-capitals (`test_..._REAL_...`, `..._BEFORE_...`) is a ruff violation, so every test name here is lowercase and the emphasis lives in the docstring instead. Names were corrected during execution; the blocks above reflect the shipped names.
- **Wheel bump required**: edit `pyproject.toml`, then `uv run python scripts/bump_wheel.py`. Never hand-edit consumers.
- Full gates before each commit, with `SILLY_KICKS_ASSERT_INVARIANTS=1` set (this repo sets it process-wide via `bootstrap.py` and CI sets it at job level, so a local run without it does not match CI):

```bash
export DATABRICKS_TOKEN="$(uv run --extra sdk python scripts/mint_databricks_oauth.py 2>/dev/null | tail -1)"
export SILLY_KICKS_ASSERT_INVARIANTS=1
uv run ruff check src/ scripts/ && uv run ruff format --check src/ scripts/
uv run pyright src/ && uv run lint-imports
uv run pytest src/tests/ -q          # never -p no:benchmark
```

---

# UNIT A — StatsBomb visibility plumbing (inert)

### Task 1: Migration — bounded, run-once, not permanently re-runnable

**Files:**
- Create: `scripts/migrations/2026-08-09-add-visibility-to-statsbomb-matches.sql`

**Interfaces:**
- Produces: `visibility STRING`, `access_tier STRING` on `soccer_analytics.bronze.statsbomb_matches`, existing rows `'public'`.

- [ ] **Step 1: Write the migration**

```sql
-- PR-2a (spec 2026-08-06 statsbomb-commercial-360-containment) — visibility plumbing.
--
-- SEMANTICALLY INERT. StatsBomb is in PUBLIC_BY_LICENSE_PROVIDERS, so every existing row
-- already resolves to access_tier='public'. This makes that implicit default EXPLICIT and
-- per-row, so the PR-2b flip has real data to flip against instead of an absence.
--
-- OPERATOR-APPLIED, WITH THE MERGE — never after. dbt-live-ci.yml is a daily scheduled live
-- build and PR-2a's dim_matches.sql selects these columns.
--
--   uv run --extra sdk python scripts/migrations/_runner.py \
--     scripts/migrations/2026-08-09-add-visibility-to-statsbomb-matches.sql
--
-- The ALTERs are idempotent (_runner.py DESCRIBE skip-if-exists).
--
-- THE UPDATES ARE DELIBERATELY *NOT* PERMANENTLY RE-RUNNABLE (review B5). A bare
-- `WHERE visibility IS NULL` is idempotent by design and therefore re-runnable forever —
-- so after PR-4 ingests commercial data, any row that reached bronze without a visibility
-- (the exact omission R-16 exists to prevent) would be stamped PUBLIC by a re-run of this
-- file. R-19's precondition guards the Python backfill, which is a different artifact.
-- Bounding on _ingested_at pins the statement to the row set whose OQ-1 premise was
-- verified, and makes a later re-run a no-op instead of a fail-open.
--
-- OPERATOR: replace <CUTOFF> with the UTC timestamp at which you verified OQ-1 — that this
-- table holds zero commercially-licensed rows.
--
-- [CORRECTED 2026-08-10 — see EX-4 below. This block originally specified ONE check, run
--  "after the ALTERs and before the UPDATEs, expect 0", which is impossible: at that moment
--  the column exists and every row is NULL, so it returns the full row count. The shipped
--  migration now separates the PROVENANCE precondition from the post-UPDATE postcondition.
--  Read the migration file, not this excerpt, for the authoritative wording.]
--
-- The predicate is `IS NULL OR <> 'public'`, NOT `= 'private'` (review A2). classify_access_tier
-- fail-safes on ANY non-'public' value — NULL, 'private', or an unrecognised string — so a
-- `= 'private'` check would report clean on a row the classifier would restrict.
--
-- This statement lives HERE, in the migration, rather than in access_tier_backfill.py's
-- _PRECONDITIONS (review D4). Registering it there would require a `statsbomb` entry in
-- _EXISTING_CONFIRMED_PUBLIC, and default_tier_for_provider returns an override INSTEAD of
-- consulting the classifier — so after the PR-2b flip a no-signal StatsBomb row would still
-- resolve 'public' from a hardcoded override, defeating the fail-safe PR-2b installs.

ALTER TABLE soccer_analytics.bronze.statsbomb_matches ADD COLUMNS (visibility STRING);

ALTER TABLE soccer_analytics.bronze.statsbomb_matches ADD COLUMNS (access_tier STRING);

UPDATE soccer_analytics.bronze.statsbomb_matches
   SET visibility = 'public'
 WHERE visibility IS NULL
   AND _ingested_at < TIMESTAMP '<CUTOFF>';

UPDATE soccer_analytics.bronze.statsbomb_matches
   SET access_tier = 'public'
 WHERE access_tier IS NULL
   AND _ingested_at < TIMESTAMP '<CUTOFF>';
```

- [ ] **Step 2: Verify the cutoff placeholder is still present**

Run: `grep -o "<CUTOFF>" scripts/migrations/2026-08-09-add-visibility-to-statsbomb-matches.sql | wc -l`
Expected: `3` (one in the comment, two in the UPDATEs). A `0` means someone substituted a value into the committed file — the cutoff belongs to the operator at apply time, not to the repo.

> **F3:** `grep -c` counts matching **lines**, not occurrences. It happens to agree today because the three placeholders sit on three separate lines; put two on one line and the check silently reads `2`. `grep -o … | wc -l` is what the step means.

> Do NOT apply yet. Applied with the merge (Task 6).

---

### Task 2: R-16 — stamp `visibility='public'` at open-path ingest, and test the WIRING

**Files:**
- Modify: `src/ingestion/statsbomb.py`
- Test: `src/tests/test_pr2a_visibility_plumbing.py` (create)

**Interfaces:**
- Produces: `stamp_open_match_visibility(df: pd.DataFrame) -> pd.DataFrame`.

Model: `src/ingestion/skillcorner_matches.py:134-136`.

- [ ] **Step 1: Write the failing tests**

```python
"""PR-2a visibility plumbing (spec 2026-08-06, §9 test table).

The over-restriction guard: without R-16's open-path stamp, R-6 threads visibility=None and
the PR-2b flip restricts the ENTIRE open corpus (spec Finding 5). RED before Task 2.

Review B3/B4: these test the WIRING and construct the FLIP. An earlier draft tested only the
pure helper the plan itself creates — which cannot be wrong — and asserted classifier
behaviour that already existed, so neither guarded the defect they were named for.
"""

from __future__ import annotations

import ast
from pathlib import Path

import pandas as pd
import pytest

_INGESTION = Path(__file__).resolve().parents[1] / "ingestion"


def test_open_statsbomb_ingest_stamps_public_visibility() -> None:
    from ingestion.statsbomb import stamp_open_match_visibility

    out = stamp_open_match_visibility(pd.DataFrame({"match_id": [1, 2]}))
    assert list(out["visibility"]) == ["public", "public"]
    assert list(out["access_tier"]) == ["public", "public"]


def test_the_stamp_precedes_the_statsbomb_matches_write() -> None:
    """B3 + E2 — the defect lives in the CALL, and in WHERE the call sits.

    A helper that is never invoked leaves every row NULL, and the PR-2b flip then restricts
    the whole corpus. Asserted via AST because the enclosing ingest function needs Spark.

    E2: an earlier draft set `wired = True` if the helper appeared ANYWHERE in a function
    that also mentioned the string "statsbomb_matches". That function mentions it three
    times across ~180 lines, so the test went green with the stamp placed AFTER the write —
    i.e. on the exact defect it is named for. Compare line numbers instead.
    """
    source = (_INGESTION / "statsbomb.py").read_text(encoding="utf-8")
    tree = ast.parse(source)

    stamp_lines = [
        n.lineno
        for n in ast.walk(tree)
        if isinstance(n, ast.Call)
        and isinstance(n.func, ast.Name)
        and n.func.id == "stamp_open_match_visibility"
    ]
    assert stamp_lines, (
        "stamp_open_match_visibility is never called. The helper alone stamps nothing (B3)."
    )

    write_lines = [
        n.lineno
        for n in ast.walk(tree)
        if isinstance(n, ast.Call)
        and isinstance(n.func, ast.Name)
        and n.func.id == "write_delta_table"
        and any(
            isinstance(a, ast.Constant) and a.value == "statsbomb_matches" for a in n.args
        )
    ]
    assert len(write_lines) == 1, (
        f"expected exactly one write_delta_table(..., 'statsbomb_matches', ...); found "
        f"{len(write_lines)}. If the write moved or was duplicated, re-verify this anchor "
        f"rather than loosening the assertion."
    )

    assert min(stamp_lines) < write_lines[0], (
        f"stamp_open_match_visibility is called at line(s) {stamp_lines} but the "
        f"statsbomb_matches write is at line {write_lines[0]}. Stamping after the write "
        f"leaves every persisted row NULL (E2)."
    )


def test_stamped_row_survives_the_pr2b_flip_and_unstamped_does_not(monkeypatch: pytest.MonkeyPatch) -> None:
    """B4 — actually CONSTRUCT the flip instead of restating classifier behaviour.

    Both halves matter: a stamped row stays public without the allowlist (that is why R-16
    is a precondition of PR-2b), and an UNSTAMPED row goes restricted (that is the Finding-5
    corpus withdrawal this whole unit exists to prevent).
    """
    import shared.access_tier as at

    monkeypatch.setattr(at, "PUBLIC_BY_LICENSE_PROVIDERS", frozenset({"wyscout", "idsse", "metrica"}))

    assert at.classify_access_tier(provider="statsbomb", visibility="public").value == "public"
    assert at.classify_access_tier(provider="statsbomb", visibility=None).value == "restricted"
```

- [ ] **Step 2: Run to verify it fails**

Run: `uv run pytest src/tests/test_pr2a_visibility_plumbing.py -q`
Expected: FAIL — `ImportError: cannot import name 'stamp_open_match_visibility'`.

- [ ] **Step 3: Implement the helper**

```python
def stamp_open_match_visibility(df: pd.DataFrame) -> pd.DataFrame:
    """Stamp the OPEN StatsBomb path's per-match redistribution signals (R-16).

    The open (free) StatsBomb feed has no per-match visibility field — every match in it is
    public by licence. This makes that an EXPLICIT per-row value rather than an implicit
    consequence of `statsbomb` sitting in PUBLIC_BY_LICENSE_PROVIDERS.

    PR-2b removes statsbomb from that allowlist so commercial 360 data fails safe to
    restricted. At that moment a row carrying visibility=None ALSO becomes restricted —
    silently withholding the entire open corpus (spec Finding 5). An explicit 'public'
    survives, because classify_access_tier returns PUBLIC on visibility == 'public' BEFORE
    it consults the allowlist.

    The COMMERCIAL path (PR-4) stamps 'private' at its own site and does not reuse this:
    "open feed ⇒ public" is a property of THIS path only.
    """
    from shared.access_tier import classify_access_tier

    df = df.copy()
    df["visibility"] = "public"
    df["access_tier"] = classify_access_tier(provider="statsbomb", visibility="public").value
    return df
```

- [ ] **Step 4: Wire it into the matches write — at the verified site**

> **D1.** rev 2 pointed at `:254`, which writes **`statsbomb_competitions`**. It also said "the frame passed to `write_delta_table`" — that is `matches_sdf`, a **Spark** DataFrame, and `stamp_open_match_visibility` does `df["visibility"] = "public"`, which raises on PySpark. Rev 2 declined to name a variable it had not read, then stated a line number it had not read either. Same failure, different token.

The verified chain in `ingest_matches_and_details` (`:335-515`):

```python
matches_pdf = finalize_bronze_df(matches_pdf, expected_cols=_STATSBOMB_MATCHES_EXPECTED_COLS, ...)  # :402
matches_sdf = spark.createDataFrame(matches_pdf)                                                    # :408
write_delta_table(matches_sdf, catalog, schema, "statsbomb_matches", ...)                           # :416
```

Insert the stamp on **`matches_pdf`, immediately before `:402`**:

```python
matches_pdf = stamp_open_match_visibility(matches_pdf)
```

Placement is load-bearing in both directions. `finalize_bronze_df`'s contract is *"call this in every bronze parser immediately before `spark.createDataFrame(df)`"*, and it **adds** any `expected_cols` missing from the frame as explicitly-typed all-NA columns. Stamping **before** it means a future path that forgets the stamp yields a typed NULL column rather than a silent NullType drop — and after PR-2b that NULL fails safe to restricted rather than vanishing.

- [ ] **Step 5: Extend the schema snapshot and `sources.yml` — two of the four contract files**

> **D1-b — found by rev 3's verification pass, in neither review.** `_STATSBOMB_MATCHES_EXPECTED_COLS` is **not a hand-edited tuple**:
> ```python
> _STATSBOMB_SNAPSHOT_TABLES = load_bronze_snapshot("statsbomb_bronze_schema_snapshot.json")          # :51
> _STATSBOMB_MATCHES_EXPECTED_COLS = expected_cols_from_snapshot(_STATSBOMB_SNAPSHOT_TABLES, "statsbomb_matches")  # :59
> ```
> The snapshot is `src/tests/fixtures/statsbomb_bronze_schema_snapshot.json` — a **test fixture that production code imports** — and `statsbomb_matches` in it has **25 columns, no `visibility`, no `access_tier`**. Review 2's instruction ("add them to `_STATSBOMB_MATCHES_EXPECTED_COLS`") assumed a literal, so following it verbatim would have edited a derived value.

`test_statsbomb_bronze_coverage.py` asserts snapshot ↔ `sources.yml` in **both** directions — every snapshot column must be documented (`missing`), and no documented column may be absent from the snapshot (`phantom`). So the two new columns must land in **both files, in this commit**:

1. `src/tests/fixtures/statsbomb_bronze_schema_snapshot.json` → add `visibility` and `access_tier` (type `string`) to `statsbomb_matches`, matching the migration's DDL exactly. **The type matters twice:** `_STATSBOMB_MATCHES_DTYPE_OVERRIDES` is derived from the same snapshot by `dtype_overrides_from_snapshot`, and `finalize_bronze_df` uses it to pick the nullable dtype for any column it has to synthesise (verified: `df[col] = pd.array([None] * n_rows, dtype=target)`). A wrong type here surfaces as an Arrow conversion error at `spark.createDataFrame`, not as a schema mismatch.
2. `dbt_project/models/staging/statsbomb/_statsbomb__sources.yml` → document both columns on the `statsbomb_matches` source.
3. Nothing to edit in `statsbomb.py` for the constant — it re-derives from the snapshot automatically. **That is the point:** the expected-cols list and the fixture cannot drift apart, because one is computed from the other.

Order matters against the daily live build. The snapshot is a checked-in `DESCRIBE TABLE` capture, so it must describe the table **after** the migration. Both edits are in-repo (neither test queries live bronze), so CI stays green on the PR — but `dim_matches.sql` (Task 6) *will* select these columns against live bronze, which is why Global Constraints require the migration applied **before or at merge, never after**.

- [ ] **Step 6: Propagate to staging — the two edits that close the second coverage gate**

> **Findings 1 + 2 (review 3).** Step 5's `sources.yml` edit trips a **second** gate that rev 3 did not know about. `test_staging_coverage.py::test_bronze_col_coverage` compares bronze columns (from `sources.yml`) against staging columns — and it reads the staging side from **`_statsbomb__models.yml`, not from the SQL** (`coverage_utils.py:101` `load_staging_cols_from_models_yml`). Three verified facts make it bite:
>
> 1. `INITIAL_BRONZE_STAGING_GAPS = {}` (`test_staging_coverage.py:178`) and is **locked empty** by `TestCoverageInvariants::test_gaps_snapshot_is_empty` (`:241`) — the escape hatch the failure message advertises is closed.
> 2. `("statsbomb_matches", "stg_statsbomb__matches")` is registered in `PROVIDER_COVERAGE` (`:66`), so the parametrised case exists today.
> 3. rev 3 mentioned `_statsbomb__models.yml` **zero times**. Editing only the SQL leaves the gate red.
>
> **Why these edits moved here rather than staying in Task 6.** rev 3 put the staging passthrough in Task 6, which meant Step 5 turned this gate red at Task 2 and it stayed red through Tasks 3, 4, 5 and most of 6 — surfacing only at Task 7's full gates. Keeping every task boundary green is the stronger option, and it gives better boundaries anyway: **Task 2 is now "the column exists and propagates to staging"; Task 6 is "the mart consumes it."** One schema-propagation unit, one task.

1. `dbt_project/models/staging/statsbomb/stg_statsbomb__matches.sql` → add `visibility` and `access_tier` to the explicit column list in the `cleaned` CTE. (`source` is `select *`, but `cleaned` enumerates — the mart cannot read what staging does not select.)
2. `dbt_project/models/staging/statsbomb/_statsbomb__models.yml` → document both columns on `stg_statsbomb__matches`. **The SQL edit alone does not satisfy the gate**; the test reads the YAML.

- [ ] **Step 7: Run — all four gates this commit touches**

Run:
```bash
uv run pytest src/tests/test_pr2a_visibility_plumbing.py \
              src/tests/test_statsbomb_bronze_coverage.py \
              src/tests/test_statsbomb_bronze_expected_cols.py \
              src/tests/test_staging_coverage.py -q
```
Expected: PASS.

The coverage tests are included **because Steps 5–6 edit the files they read**. rev 3 applied exactly this reasoning and still came up one gate short: it caught the snapshot↔`sources.yml` pair and missed the `sources.yml`↔`models.yml` pair, which the *same* edit also trips. When an edit adds a column to a contract file, enumerate every test that reads that file — do not reason from the one you were thinking about.

---

### Task 3: R-6a — required-no-default, guarded by ONE AST gate

**Files:**
- Modify: `src/ingestion/spadl_udf_shared.py:88-101`
- Modify: `src/ingestion/spadl_conversion.py` — call sites at `626`, `1140`, `1564`
- Test: `src/tests/test_pr2a_visibility_plumbing.py`

**Interfaces:**
- Produces: `stamp_access_tier(actions, *, source: str, visibility: str | None) -> pd.DataFrame` — **no default**.

> **Review B2:** rev 1 expected `pytest -k "spadl or conversion"` to RED with a `TypeError`. It will not — the call sites live inside `_udf` closures that only execute under Spark. R-6a's guarantee ("the next omission is a TypeError at the call site") therefore had **no CI guard at all**; it would fire on a Databricks run months later, which is the latency R-6a exists to remove. One AST gate replaces three per-provider regexes and is strictly stronger: it catches the seventh converter nobody has written.

- [ ] **Step 1: Write the AST gate**

```python
_PROVIDERS = frozenset({"statsbomb", "wyscout", "idsse", "metrica", "skillcorner", "gradientsports"})


def _stamp_calls() -> list[ast.Call]:
    tree = ast.parse((_INGESTION / "spadl_conversion.py").read_text(encoding="utf-8"))
    return [
        n
        for n in ast.walk(tree)
        if isinstance(n, ast.Call)
        and isinstance(n.func, ast.Name)
        and n.func.id in {"_stamp_tier", "stamp_access_tier"}
    ]


def test_every_stamp_call_passes_visibility_explicitly() -> None:
    """R-6a — the signature is required-no-default, but nothing in CI executes these calls.

    They live inside applyInPandas closures that only run under Spark, so a missing argument
    would surface as a TypeError on Databricks months later — the audit-finding latency R-6a
    exists to remove. This gate is the mechanical guard (review B2).
    """
    calls = _stamp_calls()
    assert calls, "found ZERO _stamp_tier calls — the parser is broken, not the source"
    missing = [
        c.lineno for c in calls if not any(k.arg == "visibility" for k in c.keywords)
    ]
    assert not missing, f"_stamp_tier call(s) at line(s) {missing} pass no visibility (R-6a)"


def test_stamp_call_providers_match_the_known_set_both_ways() -> None:
    """Both directions: a seventh converter must register here; a removed one must be dropped.

    A per-provider regex structurally cannot catch the provider nobody has written yet.
    """
    seen = {
        k.value.value
        for c in _stamp_calls()
        for k in c.keywords
        if k.arg == "source" and isinstance(k.value, ast.Constant)
    }
    assert seen == _PROVIDERS, f"provider drift: source= literals {seen} != known {_PROVIDERS}"
```

- [ ] **Step 2: Run to verify it fails**

Run: `uv run pytest src/tests/test_pr2a_visibility_plumbing.py -k stamp -q`
Expected: FAIL — call sites at 234, 626, 1140, 1564, 2400 pass no `visibility`.

- [ ] **Step 3: Remove the default; fix the docstring, keeping what was true**

```python
def stamp_access_tier(
    actions: pd.DataFrame,
    *,
    source: str,
    visibility: str | None,
) -> pd.DataFrame:
    """Stamp the per-match HF redistribution ``access_tier`` (spec 2026-06-29).

    DIRECT stamp from the match's ingestion-time signals — never a dim_matches join
    (unmatched→NULL→fail-safe-restricted silently drops public data, spec D3/M1). The value is
    constant per match, so a scalar assignment broadcasts across the rows.

    ``visibility`` is REQUIRED with NO DEFAULT (R-6a). It previously defaulted to ``None``,
    making "this converter forgot to thread the signal" indistinguishable from "this provider
    has no signal" — Finding 2's second site. Pass ``None`` EXPLICITLY where there is no feed.

    ``visibility=None`` yields the provider default: **public for statsbomb / wyscout / idsse /
    metrica**, **restricted for skillcorner and gradientsports**. (The previous docstring listed
    skillcorner as public — false since the P1 allowlist flip. The enumeration is kept, minus
    that error, because it is what made the error visible; review C1.)
    """
```

- [ ] **Step 4: Update the three no-feed providers**

At `626` (wyscout), `1140` (idsse), `1564` (metrica) — statsbomb is Task 4:

```python
        # visibility=None is EXPLICIT (R-6a): this provider has no per-match visibility feed.
        # NOT an omission — the required-no-default signature makes that distinction visible.
        actions = _stamp_tier(actions, source="wyscout", visibility=None)
```

- [ ] **Step 5: Set the gradientsports site to an explicit `None` — inert, and green**

> **D3.** The AST gate requires **every** site to pass `visibility`, but GS is Unit B. As rev 2 was written, Task 4 Step 4 said *"Expected: PASS except the gradientsports site (Unit B)"* while Global Constraints require full gates **before each commit**. Both cannot hold: commit 1 would ship a red suite.
>
> The split survives intact by making Unit A's GS edit **semantically inert but syntactically complete**. GS already defaults to `restricted`, and `visibility=None` yields exactly that — so no tier moves, the R-6a decision assertion is satisfiable at commit 1, and Unit B's diff becomes a **one-line, reviewable** change from `None` to the real signal. That is a better Unit B diff than one that introduces the whole keyword at once.

At `2400` (gradientsports):

```python
        # visibility=None is EXPLICIT (R-6a). GS *does* have a per-match visibility field in
        # bronze, but as of 2026-08-09 all 64 rows of bronze.gradientsports_metadata hold NULL
        # (spec R-6b's premise is stale — see Task 8). Threading it today would thread NULL.
        # Unit B populates bronze first, then replaces this None with the real signal.
        actions = _stamp_tier(actions, source="gradientsports", visibility=None)
```

- [ ] **Step 6: Run**

Run: `uv run pytest src/tests/test_pr2a_visibility_plumbing.py -k stamp -q`
Expected: still FAIL on line 234 only (statsbomb — Task 4).

---

### Task 4: R-6 — thread StatsBomb visibility via the EXISTING caller-side join

**Files:**
- Modify: `src/ingestion/spadl_conversion.py:365-390` (the `home_sdf` lookup) and `:234`
- Test: `src/tests/test_pr2a_visibility_plumbing.py`

> **Review Q1 — rev 1 pointed at a structure that does not exist.** `_make_sb_spadl_udf()` takes **no arguments** and `_udf(pdf)` receives only the events group; there is no matches frame inside the closure. But the caller **already builds a per-match lookup and joins it** before the `groupBy` — `home_team_map` at `:365`, `home_rows`/`home_schema` at `:377-384`, `home_sdf` at `:385`, and `events_sdf.filter(...).join(home_sdf, on="match_id", how="inner")` at `:388-390`. Extend **that** frame. Do not invent a new join, and never capture a driver-side dict inside the closure (serverless captures lazily; it will not be present on the executor).

- [ ] **Step 1: Add the negative assertion (review B1)**

```python
def test_statsbomb_threads_a_real_visibility_not_none() -> None:
    """R-6 — `visibility=None` at the statsbomb site IS the Finding-5 defect.

    rev 1's assertion only required the substring `visibility=`, which `visibility=None`
    satisfies — it passed on exactly the defect it named (review B1).
    """
    sb = [
        c
        for c in _stamp_calls()
        for k in c.keywords
        if k.arg == "source" and isinstance(k.value, ast.Constant) and k.value.value == "statsbomb"
    ]
    assert sb, "no statsbomb _stamp_tier call found"
    for call in sb:
        vis = next(k for k in call.keywords if k.arg == "visibility")
        assert not (isinstance(vis.value, ast.Constant) and vis.value.value is None), (
            "statsbomb passes visibility=None — after the PR-2b flip that fails safe to "
            "RESTRICTED and withholds the entire open corpus (spec Finding 5 / R-6)."
        )
```

- [ ] **Step 2: Run to verify it fails**

Run: `uv run pytest src/tests/test_pr2a_visibility_plumbing.py -k REAL -q`
Expected: FAIL — the site passes no `visibility` at all.

- [ ] **Step 3: Carry visibility on the existing lookup frame**

Extend `home_rows` / `home_schema` at `:377-384` to a third column sourced from `all_matches_pdf` (which already holds the matches rows, and after Task 1+2 carries `visibility`):

```python
    # R-6: carry the per-match visibility INTO the group. The UDF closure takes no arguments
    # and sees only its events group, so the signal must arrive via this existing caller-side
    # lookup join — not a driver-side capture, which serverless would not ship to the executor.
    home_rows = [(gid, home_team_map[gid], visibility_map.get(gid)) for gid in new_game_ids]
    home_schema = StructType(
        [
            StructField("match_id", LongType()),
            StructField("home_team_id", LongType()),
            StructField("visibility", StringType()),
        ]
    )
```

Build `visibility_map` from `all_matches_pdf` beside `home_team_map` at `:365`. Then at `:234`, read it off the group frame:

```python
        # Per-match HF redistribution tier (spec 2026-06-29). R-6: a REAL threaded signal,
        # arriving via the caller-side lookup join. Constant within the group (one match).
        actions = _stamp_tier(actions, source="statsbomb", visibility=_match_visibility)
```

where `_match_visibility` is read from `pdf["visibility"].iloc[0]` inside `_udf`, guarded for an empty frame exactly as the existing `match_id` / `season_id` reads are.

> **F4 — the row-granularity form of Finding 5.** `visibility_map.get(gid)` yields `None` for any `gid` absent from `all_matches_pdf`. The frame still builds (nullable `StringType`), the UDF stamps `visibility=None`, and after PR-2b that row fails safe to **restricted**. That is correct behaviour — but it means a *subset* of the open corpus can go dark without the whole-corpus symptom Finding 5 describes, which is precisely the kind of partial failure that reads as success. Task 7 Step 4's live gate is widened accordingly.

- [ ] **Step 4: Run**

Run: `uv run pytest src/tests/test_pr2a_visibility_plumbing.py -q`
Expected: **PASS, all tests.** (rev 2 said "PASS except the gradientsports site" — see D3; Task 3 Step 5 now sets that site explicitly, so Unit A's suite is green as a whole. A task whose expected outcome is a red suite cannot satisfy "full gates before each commit".)

---

### Task 5: R-19 — preconditions as STATEMENTS, in a module that has no driver

**Files:**
- Modify: `src/ingestion/access_tier_backfill.py:74-86`
- Test: `src/tests/test_pr2a_visibility_plumbing.py`

> **Review A2 — rev 1 fabricated an API.** It claimed `_scalar(conn, sql)` was "this module's existing single-value query helper". Verified: the module imports **only** `from shared.access_tier import AccessTier, classify_access_tier`, contains **zero** occurrences of `conn` or `_scalar`, builds statement strings via `build_backfill_statements`, and has **no driver anywhere in the repo** (`grep -rl` returns the module and its test). rev 1 added a SQL-executing function three paragraphs after arguing that module must stay I/O-free.
>
> **Review A3 —** a `frozenset[str]` of names binds a string to a string: delete the function, keep the name, the test still passes. That is the defect's own shape, one level up.

- [ ] **Step 1: Write the failing tests**

```python
def test_every_override_names_a_precondition_that_exists_as_a_callable() -> None:
    """R-19 / A3 — the entry cannot outlive the check.

    A name→name registry passes even after the function is deleted. Mapping to the callable
    makes deletion break the map.
    """
    from ingestion.access_tier_backfill import _EXISTING_CONFIRMED_PUBLIC, _PRECONDITIONS

    assert _EXISTING_CONFIRMED_PUBLIC, "override map is empty — the guard is vacuous"
    for provider, (tier, precondition) in _EXISTING_CONFIRMED_PUBLIC.items():
        assert tier in {"public", "restricted"}, f"{provider}: bad tier {tier!r}"
        assert precondition in _PRECONDITIONS, f"{provider}: {precondition!r} not registered"
        assert callable(_PRECONDITIONS[precondition]), (
            f"{precondition!r} maps to a name, not a callable — deleting the check would "
            "leave the override standing (review A3)"
        )


def test_no_orphan_preconditions() -> None:
    """A3 — the REVERSE direction: a registered check nothing references is dead on arrival."""
    from ingestion.access_tier_backfill import _EXISTING_CONFIRMED_PUBLIC, _PRECONDITIONS

    referenced = {p for _tier, p in _EXISTING_CONFIRMED_PUBLIC.values()}
    orphans = set(_PRECONDITIONS) - referenced
    assert not orphans, f"precondition(s) {orphans} are registered but referenced by no override"


def test_the_plan_emits_preconditions_before_the_backfills() -> None:
    """A3 / E1 — a registry nothing consults is decoration. Assert the ORDER.

    E1 killed two things in rev 2's version of this test. First, it called
    `build_backfill_statements(["skillcorner"])` — the real signature is keyword-only with a
    tuple, so that raises TypeError and the test errors rather than fails. Second, it asserted
    only that both builders return non-empty output; nothing asserted precedence. Since there
    is no driver in the repo, no caller exists to assert on either — so the coupling has to be
    a property of the CODE. One builder emitting an ordered plan gives it that.
    """
    from ingestion.access_tier_backfill import build_backfill_plan, build_precondition_statements

    pre = build_precondition_statements(providers=("skillcorner",))
    assert pre, "no precondition statements emitted for an override-carrying provider"
    assert all("count(*)" in s.lower() for s in pre), "a precondition must be an answerable query"

    plan = build_backfill_plan(providers=("skillcorner",))
    assert len(plan) > len(pre), "the plan must carry backfills as well as preconditions"
    assert plan[: len(pre)] == pre, (
        "preconditions must come FIRST in the plan. Two independent builders can be run "
        "independently; an ordered plan cannot be (E1)."
    )


def test_default_tier_for_provider_stays_pure() -> None:
    """A2 — the purity argument must survive R-19, not be made and then broken."""
    import inspect

    import ingestion.access_tier_backfill as m

    assert list(inspect.signature(m.default_tier_for_provider).parameters) == ["provider"]
    src = (_INGESTION / "access_tier_backfill.py").read_text(encoding="utf-8")
    for banned in ("import pyspark", "spark.sql", "def _scalar", "conn."):
        assert banned not in src, f"{banned!r} in access_tier_backfill.py — it executes nothing (A2)"
```

- [ ] **Step 2: Run to verify it fails**

Run: `uv run pytest src/tests/test_pr2a_visibility_plumbing.py -k "precondition or orphan or pure" -q`
Expected: FAIL — `ImportError: cannot import name '_PRECONDITIONS'`.

- [ ] **Step 3: Implement in the module's existing idiom**

```python
# R-19: an override MUST name a precondition, and that name MUST resolve to a callable —
# a name→name registry survives deleting the check, which is the very failure mode R-19
# exists to close (review A3).
_EXISTING_CONFIRMED_PUBLIC: dict[str, tuple[str, str]] = {
    "skillcorner": (AccessTier.PUBLIC.value, "assert_no_private_skillcorner_rows"),
}


def _no_private_skillcorner_rows() -> str:
    """Statement proving skillcorner's confirmed-public premise still holds."""
    return (
        "select count(*) from soccer_analytics.bronze.skillcorner_matches "
        "where visibility is null or visibility <> 'public'"
    )


# Preconditions are STATEMENT BUILDERS, matching build_backfill_statements. This module
# executes nothing and imports only shared.access_tier; adding a conn-taking function here
# would inject I/O into a layer that has none — the separation the purity argument rests on.
#
# There is deliberately NO statsbomb entry. See the note below Step 3.
_PRECONDITIONS: dict[str, Callable[[], str]] = {
    "assert_no_private_skillcorner_rows": _no_private_skillcorner_rows,
}


def build_precondition_statements(*, providers: tuple[str, ...]) -> list[str]:
    """Statements the operator MUST run — and see return 0 — before any backfill statement."""
    out: list[str] = []
    for provider in providers:
        entry = _EXISTING_CONFIRMED_PUBLIC.get(provider)
        if entry is None:
            continue
        _tier, precondition = entry
        out.append(_PRECONDITIONS[precondition]())
    return out


def build_backfill_plan(
    *,
    catalog: str = "soccer_analytics",
    bronze_schema: str = "bronze",
    tables: tuple[str, ...] = BACKFILL_TABLES,
    providers: tuple[str, ...] = EXISTING_PROVIDERS,
) -> list[str]:
    """The ordered plan: every precondition first, then every backfill (R-19, review E1).

    Two independent builders can be run independently — which makes "run the precondition
    first" an instruction the operator has to remember, and R-19 exists precisely because
    that kind of instruction decays. Emitting ONE ordered list makes the precedence a
    property of the code. Keyword-only with tuple defaults, matching this module's idiom.
    """
    return build_precondition_statements(providers=providers) + build_backfill_statements(
        catalog=catalog, bronze_schema=bronze_schema, tables=tables, providers=providers
    )
```

Update the lookup for the tuple shape:

```python
    if provider in _EXISTING_CONFIRMED_PUBLIC:
        tier, _precondition = _EXISTING_CONFIRMED_PUBLIC[provider]
        return tier
    return classify_access_tier(provider=provider, visibility=None).value
```

> **D4 — the decision, made.** rev 2 registered **two** preconditions against an override map holding only `skillcorner`, so `test_no_orphan_preconditions` fails on `assert_no_commercial_statsbomb_rows` — while Step 4 said "Expected: PASS". It then deferred the resolution to the implementer. An unmade decision that makes the plan's own test fail is not something to discover at Step 4.
>
> The deferral offered two branches, and **one of them is actively harmful.** Adding a `statsbomb` override entry would put `statsbomb` into `_EXISTING_CONFIRMED_PUBLIC`, and `default_tier_for_provider` returns the override **instead of** consulting the classifier:
>
> ```python
> if provider in _EXISTING_CONFIRMED_PUBLIC:
>     tier, _precondition = _EXISTING_CONFIRMED_PUBLIC[provider]
>     return tier          # <-- the classifier is never reached
> return classify_access_tier(provider=provider, visibility=None).value
> ```
>
> After PR-2b removes `statsbomb` from `PUBLIC_BY_LICENSE_PROVIDERS`, a no-signal StatsBomb row would **still** resolve `public` — from a hardcoded override, in the one module whose job is to encode confirmed-public facts. That defeats the exact fail-safe PR-2b exists to install.
>
> **So: `assert_no_commercial_statsbomb_rows` is NOT registered here**, and `_no_commercial_statsbomb_rows` is not defined in this module. Its statement is real and worth keeping — it moves into Task 1's migration comment as OQ-1 evidence, where an operator reads it at apply time. The `is null or <> 'public'` predicate rationale (review A2 — the classifier fail-safes on *any* non-`'public'` value, so `= 'private'` would pass a row the classifier restricts) travels with it.

- [ ] **Step 4: Run**

Run: `uv run pytest src/tests/test_pr2a_visibility_plumbing.py -q`
Expected: PASS.

---

### Task 6: R-5 — `dim_matches` reads the real columns (direct, no aggregation)

**Files:**
- Modify: `dbt_project/models/marts/dim_matches.sql:54-58`

> **Review Q2 — resolvable now, so it is stated, not branched.** The StatsBomb leg selects `from stg_statsbomb__matches m` with two `left join`s to `sb_event_team_ids` and **no `group by`**; SkillCorner's `max()` exists because roster rows fan out. Use direct columns. Copying `max()` without a `group by` is a syntax error — loud, but a wasted `dbt parse` cycle.
>
> **rev 4:** the staging passthrough that used to be this task's Step 1 has **moved into Task 2 Step 6**, together with the `_statsbomb__models.yml` documentation that review 3 found missing. Rationale below. This task is now purely "the mart consumes the column".

- [ ] **Step 1: Point the mart at them**

```sql
        -- Per-match HF redistribution tier (spec 2026-06-29 §6.4). R-5: StatsBomb now carries a
        -- REAL per-match visibility (PR-2a) — the open path stamps 'public' at ingest and the
        -- commercial path (PR-4) will stamp 'private'. Previously hardcoded NULL/'public', which
        -- had no way to express a restricted match.
        --
        -- DIRECT columns, no max(): this leg is row-per-match (left joins to sb_event_team_ids
        -- do not fan out) and has no `group by`, unlike the SkillCorner leg at :167-176 whose
        -- roster rows do. Do not copy that aggregation here.
        m.visibility                   as visibility,
        m.access_tier                  as access_tier
```

- [ ] **Step 2: Parse-check**

```bash
export DATABRICKS_TOKEN="$(uv run --extra sdk python scripts/mint_databricks_oauth.py 2>/dev/null | tail -1)"
cd dbt_project && uv run dbt parse
```
Expected: success. This does **not** prove the live columns exist — that needs Task 7's migration.

---

### Task 7: Unit A gates, migration, commit 1

- [ ] **Step 1: Bump the wheel**

Edit `pyproject.toml`, then `uv run python scripts/bump_wheel.py`. Expected: `Synced 30 file(s)`.

- [ ] **Step 2: Full gates** (the block in Global Constraints, including `SILLY_KICKS_ASSERT_INVARIANTS=1`)

- [ ] **Step 3: Apply the migration — with the merge**

Substitute `<CUTOFF>` with the UTC timestamp at which you verified zero commercial rows, then:

```bash
uv run --extra sdk python scripts/migrations/_runner.py \
  scripts/migrations/2026-08-09-add-visibility-to-statsbomb-matches.sql
```

Verify live: `DESCRIBE` shows both columns; `select count(*) ... where visibility is null` is 0; `group by access_tier` shows public only.

- [ ] **Step 4: Record the PR-2b gate numbers with explicit predicates (review C3)**

```sql
select count(*) from soccer_analytics.bronze.statsbomb_matches
 where visibility is null or visibility <> 'public';
select count(*) from soccer_analytics.dev_gold.fct_action_values
 where data_source = 'statsbomb' and (access_tier is null or access_tier <> 'public');
select count(*) from soccer_analytics.bronze.spadl_actions
 where data_source = 'statsbomb' and (access_tier is null or access_tier <> 'public');
```

The latter two will **not** be zero yet — see Task 10.

> **F4 — why `<> 'public'` and not just `is null`.** rev 2 checked only for NULLs. But `visibility_map.get(gid)` (Task 4) returns `None` for any match absent from `all_matches_pdf`, and after PR-2b *any* non-`'public'` value fails safe to restricted — NULL, an unrecognised string, or a stale value. A gate keyed on NULL alone reports clean while a subset of the open corpus is being withheld. This is Finding 5 at row granularity: the whole-corpus version is obvious, the subset version reads as success.

- [ ] **Step 5: STOP for commit 1 approval.** Diff + gate results. Do not commit unprompted.

---

# UNIT B — Gradient Sports signal closure (semantic, measured)

> **Why this is a separate commit and not deferred.** rev 1 recommended threading GS visibility *inside* PR-2a, which would have moved tiers under a banner promising inertness — the review's showstopper. It is in **this cycle**, as its own commit, where the movement is the headline and is counted.
>
> **The spec's premise is stale.** R-6b says the GS SPADL leg "discards a live signal". Measured 2026-08-09: `bronze.gradientsports_metadata` = **64 rows, all `visibility=NULL`, all `access_tier=restricted`**. `MatchInfo.visibility: str` is required-no-default and both `parse_metadata` callers pass it, so the pipeline carries the signal — the stored rows simply predate it (same shape as ADR-030's GS dedup, which also needed a re-ingest to reach stored data). **Threading alone would thread NULL.**

### Task 8: Correct the spec's R-6b premise

**Files:**
- Modify: `docs/superpowers/specs/2026-08-06-statsbomb-commercial-360-containment-design.md:175-177`

- [ ] **Step 1: Add the measured correction inline**

Append to R-6b, leaving the original text intact so the reasoning chain stays legible:

```markdown
  **Measured correction (2026-08-09, PR-2a rev 2).** `bronze.gradientsports_metadata` holds
  **64 rows, all `visibility=NULL`, all `access_tier=restricted`**. The pipeline carries the
  signal — `MatchInfo.visibility: str` is required-no-default and both `parse_metadata` call
  sites pass it — but the STORED rows predate the column (same shape as ADR-030's GS dedup,
  which also required a re-ingest to reach stored data). So "discards a live signal" is
  half-true: threading alone threads NULL. The signal must be POPULATED first. This is why
  R-6b ships as its own commit with a measured before/after tier count rather than inside the
  inert plumbing unit.
```

---

### Task 9: Populate GS visibility, then thread it — with the movement measured

**Files:**
- Modify: `src/ingestion/spadl_conversion.py:2201-2460` (the GS UDF caller) and `:2400`
- Test: `src/tests/test_pr2a_visibility_plumbing.py`

- [ ] **Step 1: Measure the BEFORE state**

```sql
describe table soccer_analytics.bronze.gradientsports_metadata;
describe table soccer_analytics.bronze.gradientsports_events;

select visibility, access_tier, count(*)
  from soccer_analytics.bronze.gradientsports_metadata group by 1, 2 order by 3 desc;
select access_tier, count(*) from soccer_analytics.bronze.spadl_actions
 where data_source = 'gradientsports' group by 1;
```

Record the counts in the PR description. **A Unit B commit without these numbers is incomplete** (Global Constraints).

> **Why two `DESCRIBE`s and not a sample join (review 3).** rev 3 proposed confirming the key shape with a live 5-row join. That is the **weakest** available instrument: Spark's implicit numeric-string coercion can make a STRING/INT join match anyway, so a green probe would not prove what it appears to prove. The `DESCRIBE`s read the **stored Delta types**, which is the one thing static reading genuinely cannot settle — the parsers determine what *new* writes look like, but a historical write under an older parser could have landed a different type. Unambiguous, and immune to coercion. Finding 3 settles the parser-level question; these settle the stored-state question.

- [ ] **Step 2: Re-ingest GS so bronze carries the real visibility — REQUIRES EXPLICIT APPROVAL**

> **E3 — this is the most consequential action in the plan, and rev 2 wrote it as a bare step.** Global Constraints require separate approval for commit / push / PR / merge and said nothing about mutating production data. **STOP here and obtain explicit approval before running it**, on the same footing as a commit.

> **FINDING 4 (review 3) — rev 3's version of this step was a no-op with a stop condition that would have recorded a false finding.** This is the sharpest defect any round has found, because it fails *quietly and confidently*.
>
> rev 3 said `run_now(job_id=302697362345215, only=["ingest_gradientsports"])`. Verified against `_GradientSportsGuard.check` (`gradientsports.py:60-100`):
> - **Phase A** anti-joins the API match list against `distinct match_id` from `bronze.gradientsports_events`. Every GS match is already there → `missing` is empty.
> - **Phase B** (only when A is empty) re-queries with `updatedSince = MAX(_ingested_at)`, and its docstring is explicit that this catches *"matches the **provider** re-processed"*. **A schema change on our side is not a provider re-process.**
>
> So the guard returns `count=0`, the workflow skips, **no row is rewritten, and `visibility` stays NULL.**
>
> Then rev 3's stop condition said: *"if every row is still NULL, the pining feed is not supplying visibility for GS."* That diagnosis **cannot be true**. `MatchInfo.visibility: str` is a **required** pydantic field (`gradientsports_common.py:35`) — `fetch_match_list` would raise at model construction if the discovery endpoint omitted it. The feed is structurally guaranteed to supply it. A still-NULL result means **the guard skipped**, not that the source is silent.
>
> **And note the symmetry that makes this embarrassing rather than merely wrong:** Task 10 exists precisely because StatsBomb *conversion* is incremental, so existing rows never acquire the threaded value. GS *ingestion* is incremental for the same reason, one unit later in the same document, and rev 3 re-ingested as if it were not. Caught on one provider, missed on the other.

**The right tool already exists.** `_backfill_artifacts` (`gradientsports.py:264`) — *"Skips the guard entirely. Reads the match ID list from existing `bronze.gradientsports_events`, fetches metadata + roster from the API (NOT events or tracking)"* — is exactly this step's intent. It is reachable via `main()`'s `--backfill-artifacts` flag (`:341`, `:360-362`), which branches and returns before any other argument is required.

> **Correction from the live job definition (2026-08-09).** Earlier revisions said the guard runs inside `ingest_gradientsports`. It does not. That task is a **`for_each_task`** fanning out over `{{tasks.preflight_gradientsports.values.gradientsports_matches}}` with `concurrency = 8`, and each iteration is a `python_wheel_task` taking `--match-json {{input}}`. The guard therefore runs one task **upstream**, in `preflight_gradientsports`, which emits the match list. The conclusion is unchanged — a re-run repopulates nothing because preflight emits an empty list — but the mechanism sits a level up from where the plan described it. **Read the deployed job, not the Terraform, when the question is "what actually runs."**

**But no Terraform task passes that flag** (verified: `grep -rn "backfill.artifacts" terraform/` returns nothing), so it is **not reachable** through `run_now(..., only=["ingest_gradientsports"])`.

- [ ] **Step 2a: Decide the invocation — this is a fork, not a detail**

Three options, none free:

| Option | Cost | Note |
|---|---|---|
| One-off `w.jobs.submit` with `--backfill-artifacts` | No repo change; ephemeral | Mirrors the ADR-074 diagnostic harness pattern (`/Shared/luxury-lakehouse-diag/`) |
| Add the flag to the existing TF task | Permanent repo change, needs `terraform apply` | Makes a one-time repair a standing task argument |
| Temporary TF task, removed after | Two applies | Most ceremony |

**Recommendation: the one-off submit.** This is a one-time historical repair, not recurring behaviour, and the other two put a migration-shaped action into the permanent job definition. **Surface the choice for approval — do not pick it silently.**

- [ ] **Step 2b: Run the backfill, then re-measure**

Then re-measure query 1. Corrected stop conditions:

- **Rows now carry a real `visibility`** → proceed to Step 3.
- **Rows still NULL** → **stop and investigate the backfill**, not the feed. Check that `_backfill_artifacts` actually ran (it logs the match-ID count it read from `gradientsports_events`) and that the write landed. A required-no-default pydantic field cannot arrive empty from a successful fetch, so a still-NULL result is a *our-side* failure by elimination.

> **The transient window, and why it is safe to cross.** Between this step (bronze carries real `visibility`, and its `access_tier` is re-derived by the classifier at ingest) and the merge of Step 4 (the SPADL leg still stamps `restricted`), `bronze.gradientsports_metadata.access_tier` can **disagree** with `bronze.spadl_actions.access_tier` for the same match.
>
> `assert_access_tier_visibility_consistency.sql` is a build-gating dbt test and `dbt-live-ci.yml` runs on a **daily schedule**, so this window can span a nightly build. Two facts bound the risk:
>
> 1. That test asserts `access_tier` agrees with `visibility` **within a row**, keyed on `PUBLIC_BY_LICENSE_PROVIDERS` — it does not join bronze metadata against SPADL. A GS metadata row with `visibility='private'` and `access_tier='restricted'` is internally consistent; so is a SPADL row with `visibility` absent and `access_tier='restricted'`. **The window does not by itself turn the nightly red.**
> 2. The disagreement can only ever be *more* restrictive on the SPADL side (GS defaults `restricted`), so no data becomes over-shared while it is open. The failure mode is under-sharing, which is the safe direction.
>
> If GS visibility comes back **all NULL** (the Step 2 stop condition), the window is empty — nothing moved. Land Step 4 in the same cycle regardless; do not leave the window open across a weekend.

- [ ] **Step 3: Write the R-6b decision assertion**

```python
def test_gradientsports_visibility_decision_is_explicit_and_reasoned() -> None:
    """R-6b — whichever branch was taken, it must be WRITTEN DOWN.

    After R-6a the omission is no longer visible as an omission, so the status quo must not
    be able to survive by silence.
    """
    src = (_INGESTION / "spadl_conversion.py").read_text(encoding="utf-8")
    gs = [
        c
        for c in _stamp_calls()
        for k in c.keywords
        if k.arg == "source" and isinstance(k.value, ast.Constant) and k.value.value == "gradientsports"
    ]
    assert gs, "no gradientsports _stamp_tier call found"
    assert any(k.arg == "visibility" for k in gs[0].keywords), "R-6b: must pass visibility explicitly"
    idx = src.index("source=\"gradientsports\"")
    assert "R-6b" in src[max(0, idx - 700) : idx], (
        "R-6b: the GS choice must cite R-6b in a comment stating WHY."
    )

    # Unit B specifically: the signal must now be REAL. Unit A parked this site at an explicit
    # None (Task 3 Step 5) to keep commit 1 green, and that placeholder satisfies both
    # assertions above — so without this one, Task 9's RED step could not be red and the test
    # would certify Unit A's inert placeholder as Unit B's deliverable. Mirrors Task 4's B1.
    vis = next(k for k in gs[0].keywords if k.arg == "visibility")
    assert not (isinstance(vis.value, ast.Constant) and vis.value.value is None), (
        "gradientsports still passes visibility=None — that is Unit A's placeholder, not "
        "Unit B's threaded signal (R-6b)."
    )
```

- [ ] **Step 4: Build a GS lookup frame — there is none to extend**

> **D2 — rev 2 asserted a structure that does not exist.** It said GS has "the same shape as the StatsBomb one" and pointed at `:782-786` for a lookup frame the caller "already joins". Verified: `:782-786` is inside the **wyscout** converter (its UDF factory is at `:490`, its `applyInPandas` at `:862`) — roughly 1,400 lines from the GS code. The real `_convert_gradientsports_from_bronze` (`:2524`) is:
>
> ```python
> events_sdf = spark.table(events_table)                                          # :2552
> new_events_sdf = events_sdf.filter(spark_fn.col("match_id").isin(new_match_ids))  # :2572
> needed = _gs_needed_bronze_columns()                                            # :2580
> new_events_sdf = new_events_sdf.select(<backtick-quoted projection>)            # :2583
> spadl_sdf = new_events_sdf.groupBy("match_id").applyInPandas(...)
> ```
>
> **No `createDataFrame` lookup frame and no join anywhere.** This is Q1 recurring one unit later: StatsBomb's converter was fixed by reading it; the GS instruction was inferred from that fix — in a plan whose own Self-Review names an invented structure as the cautionary case.

Two constraints follow, and the second is the one that bites.

`visibility` is **not** a GS bronze *events* column — it lives in `bronze.gradientsports_metadata`. So it cannot be added to `_gs_needed_bronze_columns()`: that set exists to project the ~264 dotted event columns by backtick-quoting them, and naming a column the events table does not have would fail the `.select()` outright. The join must therefore land **after** the projection at `:2583` and **before** the `groupBy`:

```python
    # R-6b: GS has no caller-side lookup frame (unlike statsbomb's home_sdf), so build one.
    # visibility lives in bronze.gradientsports_metadata, NOT in the events table — it cannot
    # ride _gs_needed_bronze_columns(), whose whole job is backtick-projecting dotted EVENT
    # columns. Join AFTER that projection so the added column is not dropped by it.
    vis_sdf = (
        spark.table(f"{catalog}.{schema}.gradientsports_metadata")
        .select(
            spark_fn.col("match_id").alias("match_id"),
            spark_fn.col("visibility").alias("visibility"),
        )
        .distinct()
    )
    new_events_sdf = new_events_sdf.join(vis_sdf, on="match_id", how="left")
```

`how="left"` is deliberate: an events match absent from metadata must still convert, arriving with `visibility=None` and defaulting to `restricted` — the fail-safe direction. An `inner` join would silently **drop** those matches from SPADL entirely, trading an over-restriction for data loss.

> **The parameter is `schema`, not `bronze_schema`.** `_convert_gradientsports_from_bronze(spark, catalog, schema, logger, existing_matches, match_id_filter)` — verified at `:2524`. A first draft of this step wrote `f"{catalog}.{bronze_schema}..."`, a name not in scope: the invented-identifier error, inside the fix for the invented-structure error. Per ADR-073 a bronze reader correctly uses the schema it was passed, so `{schema}` is right here — do **not** substitute `DEFAULT_BRONZE_SCHEMA`.

> **FINDING 3 (review 3) — rev 3's risk note named the wrong pair, and the join is provably safe.** rev 3 warned about "tracking `STRING` vs events `INT`". **Tracking is not in this join** — the join is **metadata ↔ events**. (Worse: the memory that warning came from, `project_gradientsports_player_id_space_bug`, is about **`player_id`**, not `match_id`. A misremembered column applied to a misidentified table pair.) A reader acting on that note would have checked the wrong two tables and concluded the join was fine for a reason that was not the reason.
>
> The real chain is fully determined **statically** — no live probe needed:
>
> | Step | Evidence |
> |---|---|
> | `MatchInfo.id: str` on a **pydantic** `BaseModel` with a `@field_validator("id")` | `gradientsports_common.py:26-38` — enforced at construction, not merely annotated |
> | `mid = match.id` | `gradientsports.py:163` |
> | events: `df["match_id"] = match_id` — the raw value | `gradientsports_events.py:69` |
> | metadata: `df["match_id"] = gradientsports_native_match_id(mid)` | `gradientsports_metadata.py:44, 68` |
> | that helper is **value-preserving**: `s = str(raw)`, pattern-check, `return s` | `shared/identifiers.py:240-245` |
> | the backfill path reads match ids **out of `gradientsports_events`** and passes them straight to `parse_metadata` | `gradientsports.py:279, 298` |
>
> Both sides are STRING carrying identical values, written from the same variable in the same loop iteration. **The `on="match_id"` left join is correct by construction.** The one genuine asymmetry — metadata routes the id through a validator, events does not — is benign *because* the validator preserves value and pydantic guarantees `str` upstream. That is a stronger argument than rev 3's, and it does not depend on a probe.

Then at `:2400`, read it off the group frame as StatsBomb does (`pdf["visibility"].iloc[0]`, guarded for an empty frame), replacing the explicit `None` that Task 3 Step 5 put there:

```python
        # Per-match HF redistribution tier (spec 2026-06-29). R-6b: thread the REAL GS signal,
        # arriving via the caller-side lookup join. This leg previously discarded it and
        # defaulted every GS action to `restricted`. MEASURED 2026-08-09: all 64 stored rows
        # were visibility=NULL, so the pre-re-ingest movement is zero — the count in the PR
        # description is the authority, not this comment.
        actions = _stamp_tier(actions, source="gradientsports", visibility=_gs_match_visibility)
```

- [x] **Step 5: Measure the AFTER state and decide re-materialization — DONE 2026-08-09**

Backfill run `644177291898073` (one-off `w.jobs.submit`, wheel 0.5.92, `--backfill-artifacts`) terminated `SUCCESS`. **SUCCESS was verified against the data, not trusted** — ADR-067/ADR-073 are this repo's standing evidence that a swallowed failure still reports SUCCESS.

| Measure | BEFORE | AFTER |
|---|---|---|
| `bronze.gradientsports_metadata` | 64 rows, `visibility=NULL`, `access_tier='restricted'` | **64 rows, `visibility='private'`, `access_tier='restricted'`** |
| `max(_ingested_at)` | (pre-run) | **2026-08-09T19:28:30Z** — proof the rows were actually rewritten |
| `metadata ⋈ events` on `match_id` | untested | **64 events matches / 64 joined — 100%, zero orphans** |
| `bronze.spadl_actions` (GS) | 90,831 rows, all `restricted` | unchanged, all `restricted` |

**Tier movement: ZERO rows.** GS was `restricted` by *provider default*; it is now `restricted` by an explicit `visibility='private'` signal. Identical value, real provenance. Three consequences, all good:

1. **No re-materialization is needed.** Nothing moved, so `fct_action_values` cannot disagree with bronze, and the GS SPADL rows' stored `restricted` is already the value the threaded signal produces. (Same reasoning that retired Task 10, arrived at independently.)
2. **The transient-window hazard (review E3) never opened.** Bronze now says `private`/`restricted`; SPADL says `restricted`. `assert_access_tier_visibility_consistency.sql` sees GS outside `PUBLIC_BY_LICENSE_PROVIDERS` with a non-public visibility resolving to restricted — consistent in both places, so no nightly build can straddle a disagreement.
3. **The R-6b premise is now fully settled.** The feed does supply visibility (all 64 `private`), confirming `MatchInfo.visibility: str` is honoured end-to-end; the stored NULLs were purely historical, exactly as the corrected spec stanza says.

The left join's fail-safe branch is unexercised (zero orphans today) but remains correct — `how="left"` is what keeps a future events-only match converting as `restricted` instead of vanishing.

- [ ] **Step 6: Run**

Run: `uv run pytest src/tests/test_pr2a_visibility_plumbing.py -q`
Expected: PASS (all).

---

### Task 10: Forced StatsBomb re-conversion — the PR-2b gate's real precondition

> **Review Q3, verified.** `spadl_conversion.py:388` filters `events_sdf.filter(col("match_id").isin(new_game_ids))` — conversion is **incremental**. Existing StatsBomb SPADL rows therefore never acquire the threaded visibility. rev 1 left this to be discovered at the gate.

- [x] **Step 1: Establish what a re-conversion requires — DONE, and the answer retires this task**

**Mechanism (traced, not guessed).** `spadl_vaep.py:848` computes `existing_spadl_matches = _read_existing_match_ids(spark, catalog, schema, _SPADL_TABLE, logger)` — the distinct `match_id` set already in `bronze.spadl_actions` — and passes it to every converter. `_run_chunk` (`:1161`) does the same, so **chunk mode does not bypass the skip either**. A forced re-conversion is therefore neither a flag nor a watermark reset: it requires **deleting the StatsBomb rows from `bronze.spadl_actions`** so they stop counting as existing.

**Measured 2026-08-09 (live), and this is what changes the plan:**

| Fact | Value |
|---|---|
| `bronze.spadl_actions` where `data_source='statsbomb'` | **7,151,519 rows / 3,463 matches — all `access_tier='public'`, 0 NULL** |
| `dev_gold.fct_action_values` where `data_source='statsbomb'` | **7,066,329 rows — all `access_tier='public'`, 0 NULL** |
| `bronze.spadl_actions.visibility` | **does not exist** — `stamp_access_tier` writes only `access_tier` |

**Conclusion: the forced re-conversion is NOT required, and this task is retired.** Three independent reasons, each sufficient:

1. **The PR-2b zero-NULL gate is already satisfied on both legs**, today, before any re-conversion. Task 7 Step 4's predicate (`access_tier is null or access_tier <> 'public'`) returns **0** for `spadl_actions` and `fct_action_values` alike.
2. **There is no SPADL-level `visibility` column to backfill.** "Existing rows never acquire the threaded visibility" is literally true and *inconsequential* — nothing downstream reads a `visibility` from SPADL; the redistribution decision is carried by `access_tier`.
3. **The stored `access_tier='public'` is already correct and does not decay.** It was materialized by `classify_access_tier('statsbomb', None)` under the current allowlist, and PR-2b does not re-derive materialized values. Open StatsBomb data is public before and after the flip.

What the threading *does* buy is forward correctness: after PR-2b, any **new** conversion must carry a real `visibility` or fail safe to restricted. That is delivered by Task 2 (R-16 ingest stamp) + Task 4 (threading) + Task 1 (the migration populating existing bronze), and the explicit `visibility` projection now fails **loud** if the migration has not been applied — closing the merge-to-migration window.

> **This is a scope reduction, and reductions are the user's call, not the implementer's.** It is recorded here with its evidence rather than acted on unilaterally: nothing was deleted, no re-conversion was run. If the preference is to re-derive anyway for provenance (so every row's tier traces to a threaded signal rather than a defaulted one), the operation is a `DELETE FROM bronze.spadl_actions WHERE data_source='statsbomb'` followed by a normal `compute_spadl_vaep` run — **7.15M rows / 3,463 matches**, plus the downstream `fct_action_values` rebuild. It does not ride on the upcoming silly-kicks action-context recompute, which regenerates `fct_action_context` but **not** `spadl_actions` (a different job).

---

## Self-Review

**1. Review coverage.** *Review 1:* A1 → Unit A/B split. A2 → Task 5 (statement builders, purity assertions). A3 → Task 5's tests. Q1 → Task 4 Step 3. Q2 → Task 6. Q3 → Task 10. B1 → Task 4 Step 1. B2 → Task 3's AST gate. B3/B4 → Task 2. B5 → Task 1. C1 → Task 3 Step 3. C2 → AST replaces regex. C3 → Task 7 Step 4. C4 → Global Constraints.
*Review 2:* D1 → Task 2 Step 4 (`matches_pdf` before `:402`). D1-b → Task 2 Step 5 (snapshot + `sources.yml`). D2 → Task 9 Step 4 (new frame, joined after the projection). D3 → Task 3 Step 5 + Task 4 Step 4. D4 → Task 5's note + Task 1's migration comment. E1 → `build_backfill_plan`. E2 → `lineno` comparison. E3 → Task 9 Step 2 approval + transient window. F1 → anchors re-verified. F2 → keyword-only tuples. F3 → `grep -o | wc -l`. F4 → Task 7 Step 4 predicate.

**2. Placeholder scan.** No "TBD"/"handle edge cases". **Rev 2 had three "read this before editing" steps; rev 3 has one** — Task 10 Step 1's re-conversion mechanism. The other two were removed by *doing the reading*: Task 2's write frame is now `matches_pdf` at a verified anchor, and Task 9's GS frame is now specified because the verification pass established that no frame exists to extend. A named unknown beats an invented name; a verified fact beats both.

**3. Type consistency.** `stamp_access_tier(actions, *, source: str, visibility: str | None)` — defined Task 3, called Tasks 3/4/9. `_EXISTING_CONFIRMED_PUBLIC: dict[str, tuple[str, str]]`, `_PRECONDITIONS: dict[str, Callable[[], str]]` (one entry), `build_precondition_statements(*, providers: tuple[str, ...]) -> list[str]`, `build_backfill_plan(*, catalog, bronze_schema, tables, providers) -> list[str]` — all keyword-only with tuple defaults, matching `build_backfill_statements`'s verified signature. `stamp_open_match_visibility(df: pd.DataFrame) -> pd.DataFrame` — Task 2 only. `_stamp_calls()` / `_PROVIDERS` — defined Task 3, reused Tasks 4 and 9.

**4. Ordering.** Task 6 reads columns that exist only after Task 1 is APPLIED; `dbt parse` passes without them, the daily live build does not. Task 2 Step 5's snapshot edit must ride the same commit as Step 4's stamp, or the both-directions coverage test fails. Task 9 Step 2 must precede Step 4, or threading threads NULL.

**5. Unit A is green as a whole.** Every Unit A task ends with a passing suite (D3). No task's expected outcome is a red test, because "full gates before each commit" and "expected: FAIL on the GS site" cannot both hold.

**6. What this cycle does NOT do.** It does not flip `PUBLIC_BY_LICENSE_PROVIDERS` (PR-2b), convert publisher registry modes, or touch `embedding_space_id` (PR-3). Unit A moves no tier. Unit B may — and reports the number.

**7. Approval points.** Five, each requiring explicit user approval: commit 1 (Task 7 Step 5), the **GS backfill invocation choice** (Task 9 Step 2a — rev 4), the **GS production backfill run** (Task 9 Step 2b — rev 3 per E3), commit 2 (Task 10 Step 3), and push/PR/merge. Plus one open scope question: the GS bronze-coverage gap surfaced in the rev-4 header.

**9. Contract-file edits and their gates (rev 4).** Adding a bronze column to StatsBomb touches **six** files, and three separate tests read them. Enumerated so the next reader does not rediscover this one gate at a time: `migrations/*.sql` (live DDL) → `statsbomb_bronze_schema_snapshot.json` (feeds `_STATSBOMB_MATCHES_EXPECTED_COLS` **and** `_STATSBOMB_MATCHES_DTYPE_OVERRIDES`) → `_statsbomb__sources.yml` (`test_statsbomb_bronze_coverage`, both directions) → `stg_statsbomb__matches.sql` + `_statsbomb__models.yml` (`test_staging_coverage`, which reads the **YAML**) → `dim_matches.sql`. Two review rounds each found one more link in this chain; the chain itself is the durable artifact.

**8. Anchors verified in rev 3** (against `main`, this working tree): `statsbomb.py` `:51` snapshot load, `:59` expected-cols derivation, `:402` `finalize_bronze_df`, `:408` `createDataFrame`, `:416` matches write, `:254` competitions write (the anchor rev 2 mistook for matches). `spadl_conversion.py` `:365` `home_team_map`, `:377-384` `home_rows`/`home_schema`, `:385` `home_sdf`, `:388-390` filter+join, `:2474` `_gs_needed_bronze_columns`, `:2524` GS converter, `:2552`/`:2572`/`:2583` its table/filter/projection. `access_tier_backfill.py` keyword-only signature. Snapshot fixture: 25 columns, neither new column present.
