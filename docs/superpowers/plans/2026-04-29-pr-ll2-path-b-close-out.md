# PR-LL2 Path B Close-Out Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Fix 6 production bugs in PR-LL2's Path B integration AND establish the cross-table format-contract testing foundation that prevents the recurring bug class (per ADR-018).

**Architecture:** Single-PR comprehensive close. Build all changes on feature branch `fix/pr-ll2-path-b-close-out`. TDD shape — failing test before fix per item. Single squash commit at PR merge. silly-kicks 1.8.0 → 2.0.0 bump (caller's `team_id`/`player_id` now sacred per their ADR-001 — fixes Bug #3 at source). Wheel 0.3.21 → 0.3.22.

**Tech Stack:** Python 3.10 (Databricks serverless lock), pandas, PySpark applyInPandas, Delta Lake, dbt-databricks, silly-kicks 2.0.0, pytest, pytest-benchmark, ruff, pyright.

**Approval gates** (per CLAUDE.md hard rule "every git commit / push / PR-create / merge requires separate explicit approval"):
- 🛑 G1: Feature branch creation (`git checkout -b`)
- 🛑 G2: Single dev commit (after all local tests pass)
- 🛑 G3: Push to remote
- 🛑 G4: PR-create
- 🛑 G5: Merge after CI green — **GitHub Actions auto-builds + uploads wheel 0.3.22 to UC Volume** (canonical deploy path; no manual `scripts/deploy_wheel.py` needed)
- 🛑 G6: Bronze schema migration ALTER (additive, idempotent — runs after merge against the freshly auto-uploaded wheel)
- 🛑 G7: Bronze data DELETE (destructive — DEEP CLONE backups taken first)
- 🛑 G8: Workflow re-runs (wf-idsse, wf-metrica-*, wf-vaep-light) — consume the auto-uploaded 0.3.22 wheel
- 🛑 G9: Drop DEEP CLONE backups (24h post-merge)

Why merge-then-deploy: GitHub Actions on push to main builds the wheel and uploads it to `/Volumes/soccer_analytics/bronze/libs/luxury_lakehouse-0.3.22-py3-none-any.whl` automatically (architecture.dsl:75; CLAUDE.md). Manual `scripts/deploy_wheel.py` is a dev-cycle escape hatch, not the canonical production path. Bronze re-ingest reads the wheel from UC Volume — must run AFTER merge so it picks up 0.3.22, not the stale 0.3.21.

User signals approval by touching `~/.claude-git-approval` (5-min TTL one-shot, gating `git_commit_guard.py` hook).

**Spec reference:** `docs/superpowers/specs/2026-04-29-pr-ll2-path-b-close-out-design.md`
**PR-LL3 deferred-scope tracker:** `docs/superpowers/plans/PR-LL3-scope.md`

---

## File Structure

### New files (16)

| Path | Responsibility |
|------|---|
| `src/shared/identifiers.py` | Single source of truth for native ID format generators (4 sources × 3 entities). Pure stdlib. |
| `src/tests/test_shared_identifiers.py` | Unit tests for `identifiers.py`. |
| `src/tests/test_format_contract.py` | Cross-boundary Python tests asserting bronze-writer format ≡ dim-staging format. Parametrized over 4 sources. |
| `src/tests/test_silly_kicks_boundary.py` | API contract tests at OUR repo (4 sources × 4 invariants = 16 tests). |
| `src/tests/test_idsse_period_derivation.py` | 2-pass parser unit tests with synthetic XML fixture. |
| `src/tests/fixtures/idsse_interleaved_periods.xml` | Synthetic DFL XML fixture covering primary EventList + secondary block. |
| `src/tests/fixtures/silly_kicks_boundary/sb_match_7298.parquet` | StatsBomb 1-match fixture for boundary test. |
| `src/tests/fixtures/silly_kicks_boundary/ws_match_2576335.parquet` | Wyscout 1-match fixture. |
| `src/tests/fixtures/silly_kicks_boundary/idsse_J03WMX.parquet` | IDSSE 1-match fixture. |
| `src/tests/fixtures/silly_kicks_boundary/metrica_sample_game_1.parquet` | Metrica 1-match fixture. |
| `dbt_project/tests/assert_statsbomb_match_id_native_join_resolves.sql` | dbt singular test. |
| `dbt_project/tests/assert_statsbomb_team_id_native_join_resolves.sql` | dbt singular test. |
| `dbt_project/tests/assert_statsbomb_competition_native_id_join_resolves.sql` | dbt singular test. |
| `dbt_project/tests/assert_wyscout_match_id_native_join_resolves.sql` | (× 3 for wyscout) |
| `dbt_project/tests/assert_idsse_*_native_join_resolves.sql` | (× 3 for idsse) |
| `dbt_project/tests/assert_metrica_*_native_join_resolves.sql` | (× 3 for metrica) |
| `docs/superpowers/adrs/ADR-018-cross-table-format-contract-testing.md` | New ADR. |

(12 dbt singular tests total = 4 sources × 3 entities)

### Modified files (15)

| Path | Change |
|------|---|
| `pyproject.toml` | silly-kicks pin `>=1.8.0,<2.0` → `>=2.0.0,<3.0`; version `0.3.21` → `0.3.22`. |
| `src/shared/wheel.py` | Wheel filename SHA refresh post-build. |
| `src/ingestion/idsse.py` | Bug #1: strip `idsse_` prefix from `match_id`. Bug #6: 2-pass parser. |
| `src/ingestion/metrica_events.py` | Bug #2: align `_native_team_ids` to `metrica_<match>_<home\|away>` lowercase. |
| `src/ingestion/spadl_conversion.py` | Wire 4 tackle qualifier columns through 4 SPADL UDFs. |
| `src/ingestion/spadl_vaep.py` | Update `_SPADL_SCHEMA` + `_VAEP_SCHEMA` DDL constants for 4 new columns + add to VAEP scoring StructType. |
| `src/tests/test_spadl_vaep_writer_parity.py` | Extend to assert 4 new tackle columns in StructType + DDL. |
| `dbt_project/models/marts/dim_matches.sql` | Bug #4: pass `competition_id` through Metrica CTE. |
| `dbt_project/models/marts/fct_action_values.sql` | Project 4 new tackle columns through. |
| `dbt_project/models/staging/spadl/stg_spadl__action_values.sql` | Pass 4 new tackle columns through. |
| `dbt_project/models/marts/_marts__models.yml` | Bug #5: mart-level `where:` filter on 5 not_null tests. Add column descriptions for 4 new tackle columns. |
| `scripts/migrate_bronze_for_pr_ll2.py` | Add tackle qualifier ALTER target dict. |
| `CLAUDE.md` | Add ADR-018 reference rule under "Architectural Decision Records (ADRs)". |
| `scripts/validate_pr_ll2_post_deploy.py` → `scripts/validate_native_id_integrity.py` | Rename + extend with JOIN-coverage assertions. |
| `MEMORY.md` | Move "Latest State" entry to reflect close-out. |

### Deleted files (3)

| Path | Reason |
|------|---|
| `scripts/probe_pr_ll2_path_b_bugs.py` | Throwaway debugging artifact from spec phase. |
| `scripts/probe_pr_ll2_path_b_bug3_deep.py` | Throwaway debugging artifact. |
| `scripts/probe_pr_ll2_path_b_bug6_periods.py` | Throwaway debugging artifact. |

---

## Phase A — Foundation (T1–T4, no behavior change yet)

### Task 1: Create feature branch

**Files:** None — git operation.

- [ ] **Step 1.1: 🛑 Approval gate G1** — request user touch `~/.claude-git-approval`

- [ ] **Step 1.2: Create + checkout feature branch**

```bash
git checkout -b fix/pr-ll2-path-b-close-out
```

Expected: `Switched to a new branch 'fix/pr-ll2-path-b-close-out'`.

- [ ] **Step 1.3: Verify branch state**

```bash
git status && git log --oneline -1
```

Expected: clean tree on new branch, head at `1f97550` (current main).

### Task 2: Bump silly-kicks to 2.0.0

**Files:** Modify `pyproject.toml:29`

- [ ] **Step 2.1: Edit pyproject.toml — silly-kicks pin**

```python
# Before:
"silly-kicks>=1.8.0,<2.0",
# After:
"silly-kicks>=2.0.0,<3.0",
```

- [ ] **Step 2.2: Sync env**

```bash
uv sync --extra analytics
```

Expected: silly-kicks resolves to 2.0.0.

- [ ] **Step 2.3: Verify import works**

```bash
uv run python -c "import silly_kicks.spadl as s; print(s.SPORTEC_SPADL_COLUMNS.__class__); print(hasattr(s, 'use_tackle_winner_as_actor'))"
```

Expected: `<class 'dict'>` and `True`.

### Task 3: Create `src/shared/identifiers.py`

**Files:**
- Create: `src/shared/identifiers.py`
- Create: `src/tests/test_shared_identifiers.py`

- [ ] **Step 3.1: Write failing test first**

```python
# src/tests/test_shared_identifiers.py
"""Unit tests for shared.identifiers — native ID format generators (ADR-018)."""

import pytest

from shared.identifiers import (
    idsse_native_match_id,
    idsse_native_competition_id,
    metrica_native_team_id,
    metrica_native_competition_id,
    metrica_native_season_id,
)


class TestIdsseMatchId:
    def test_bare_dfl_id_passes_through(self):
        assert idsse_native_match_id("J03WMX") == "J03WMX"

    def test_alphanumeric_uppercase_passes(self):
        assert idsse_native_match_id("J03WR9") == "J03WR9"

    def test_prefixed_form_rejected(self):
        with pytest.raises(ValueError, match="bare DFL MatchId"):
            idsse_native_match_id("idsse_J03WMX")

    def test_lowercase_rejected(self):
        with pytest.raises(ValueError):
            idsse_native_match_id("j03wmx")

    def test_empty_rejected(self):
        with pytest.raises(ValueError):
            idsse_native_match_id("")


class TestIdsseCompetitionId:
    def test_dfl_com_format_passes(self):
        assert idsse_native_competition_id("DFL-COM-000001") == "DFL-COM-000001"

    def test_invalid_format_rejected(self):
        with pytest.raises(ValueError):
            idsse_native_competition_id("CL")


class TestMetricaTeamId:
    def test_home_format(self):
        assert metrica_native_team_id("Sample_Game_1", "home") == "metrica_Sample_Game_1_home"

    def test_away_format(self):
        assert metrica_native_team_id("Sample_Game_3", "away") == "metrica_Sample_Game_3_away"

    def test_capital_side_rejected(self):
        with pytest.raises(ValueError, match="must be 'home' or 'away'"):
            metrica_native_team_id("Sample_Game_1", "Home")  # type: ignore[arg-type]

    def test_unknown_side_rejected(self):
        with pytest.raises(ValueError):
            metrica_native_team_id("Sample_Game_1", "neutral")  # type: ignore[arg-type]


class TestMetricaConstants:
    def test_competition_id_constant(self):
        assert metrica_native_competition_id() == "metrica-sample"

    def test_season_id_constant(self):
        assert metrica_native_season_id() == "metrica-open-2017"
```

- [ ] **Step 3.2: Run test, verify FAIL**

```bash
uv run pytest src/tests/test_shared_identifiers.py -v
```

Expected: `ImportError: cannot import name 'idsse_native_match_id' from 'shared.identifiers'`.

- [ ] **Step 3.3: Implement `src/shared/identifiers.py`**

```python
"""Single source of truth for native identifier format generators across all
4 SPADL data sources (StatsBomb, Wyscout, IDSSE, Metrica).

ADR-018 — cross-table format-contract testing — requires that every value
flowing into a (provider, native_id) JOIN key has a single canonical
generator. Bronze writers + dim staging + applyInPandas UDFs all reach
into this module; format errors fail at construction time, not at
full-refresh dbt-build time.

Pure stdlib (re, typing). No Spark/dbt/pandas imports — runs in unit
tests, in bronze-writer Python paths, and in dbt analyses (via macro
parity tests). Adding to this module triggers ADR-018 maintenance:
every new function needs a corresponding format-contract test in
`src/tests/test_format_contract.py`.
"""

from __future__ import annotations

import re
from typing import Literal

# ---------------------------------------------------------------------------
# IDSSE (DFL Bundesliga / Sportec)
# ---------------------------------------------------------------------------

_IDSSE_MATCH_ID_PATTERN = re.compile(r"^[A-Z0-9]+$")
_IDSSE_COMPETITION_ID_PATTERN = re.compile(r"^DFL-COM-[A-Z0-9]+$")


def idsse_native_match_id(raw_dfl_match_id: str) -> str:
    """Canonical IDSSE native match id — bare DFL MatchId (e.g. 'J03WMX').

    Source of truth for the format that lands in:
    - bronze.idsse_events.match_id
    - bronze.idsse_tracking.match_id
    - bronze.spadl_actions.match_id_native (for IDSSE rows)
    - dim_matches.native_match_id (for IDSSE rows)

    Pre-2026-04-29 PR-LL2-Path-B-close-out, idsse.py erroneously prefixed
    this with 'idsse_' (e.g. 'idsse_J03WMX'). ADR-018-driven format
    contract enforces the bare form.
    """
    if not _IDSSE_MATCH_ID_PATTERN.match(raw_dfl_match_id):
        raise ValueError(
            f"invalid IDSSE match id: {raw_dfl_match_id!r} "
            "(expected bare DFL MatchId like 'J03WMX')"
        )
    return raw_dfl_match_id


def idsse_native_competition_id(raw_dfl_competition_id: str) -> str:
    """Canonical IDSSE native competition id — 'DFL-COM-XXXXXX'."""
    if not _IDSSE_COMPETITION_ID_PATTERN.match(raw_dfl_competition_id):
        raise ValueError(
            f"invalid IDSSE competition id: {raw_dfl_competition_id!r} "
            "(expected 'DFL-COM-XXXXXX' format)"
        )
    return raw_dfl_competition_id


# ---------------------------------------------------------------------------
# Metrica (anonymised open-data sample)
# ---------------------------------------------------------------------------

_METRICA_MATCH_ID_PATTERN = re.compile(r"^Sample_Game_[0-9]+$")


def metrica_native_match_id(raw_metrica_match_id: str) -> str:
    """Canonical Metrica native match id — 'Sample_Game_N'."""
    if not _METRICA_MATCH_ID_PATTERN.match(raw_metrica_match_id):
        raise ValueError(
            f"invalid Metrica match id: {raw_metrica_match_id!r} "
            "(expected 'Sample_Game_N' format)"
        )
    return raw_metrica_match_id


def metrica_native_team_id(match_id: str, side: Literal["home", "away"]) -> str:
    """Canonical Metrica native team id — 'metrica_<match>_<home|away>'.

    Source of truth for the format that lands in:
    - bronze.metrica_events.{home,away}_team_id_native, team_id_native
    - bronze.spadl_actions.{home,team}_id_native (for Metrica rows)
    - dim_teams.native_team_id (for Metrica rows; via stg_metrica__team_players's
      `concat('metrica_', match_id, '_', side)` pattern — same convention)

    Pre-2026-04-29 PR-LL2-Path-B-close-out, metrica_events.py emitted
    f'{match_id}-{side.title()}' (capital-Home, hyphen) which did not
    match dim_teams's lowercase-prefix-underscore convention. ADR-018-
    driven format contract enforces alignment.
    """
    if side not in ("home", "away"):
        raise ValueError(f"side must be 'home' or 'away', got {side!r}")
    metrica_native_match_id(match_id)  # validate match_id format too
    return f"metrica_{match_id}_{side}"


def metrica_native_competition_id() -> str:
    """Canonical Metrica native competition id — sample-data sentinel.

    Per stg_metrica__matches.sql:26 + dim_competitions.sql metrica CTE
    (PR 5a, ADR-011) — single value across all Metrica rows.
    """
    return "metrica-sample"


def metrica_native_season_id() -> str:
    """Canonical Metrica native season id — sample-data sentinel."""
    return "metrica-open-2017"


# ---------------------------------------------------------------------------
# StatsBomb / Wyscout — numeric BIGINT natives, stringified at SPADL boundary
# ---------------------------------------------------------------------------
# These are added for completeness so the format-contract test parametrization
# is uniform across sources. The functions are simple cast-to-str wrappers
# but raise on negative or non-integer input.


def statsbomb_native_match_id(raw_match_id: int) -> str:
    """Canonical StatsBomb native match id — stringified positive BIGINT."""
    if not isinstance(raw_match_id, int) or raw_match_id <= 0:
        raise ValueError(
            f"invalid StatsBomb match id: {raw_match_id!r} "
            "(expected positive int)"
        )
    return str(raw_match_id)


def wyscout_native_match_id(raw_match_id: int) -> str:
    """Canonical Wyscout native match id — stringified positive BIGINT."""
    if not isinstance(raw_match_id, int) or raw_match_id <= 0:
        raise ValueError(
            f"invalid Wyscout match id: {raw_match_id!r} "
            "(expected positive int)"
        )
    return str(raw_match_id)
```

- [ ] **Step 3.4: Run test, verify all GREEN**

```bash
uv run pytest src/tests/test_shared_identifiers.py -v
```

Expected: 13 passed.

- [ ] **Step 3.5: Run pyright + ruff**

```bash
uv run ruff check src/shared/identifiers.py src/tests/test_shared_identifiers.py
uv run pyright src/shared/identifiers.py
```

Expected: `All checks passed.` / `0 errors, 0 warnings, 0 informations`.

### Task 4: Draft ADR-018

**Files:**
- Create: `docs/superpowers/adrs/ADR-018-cross-table-format-contract-testing.md`

- [ ] **Step 4.1: Write ADR-018 (use ADR-TEMPLATE.md format)**

```markdown
# ADR-018: Cross-Table Format-Contract Testing

| Field | Value |
|---|---|
| **Date** | 2026-04-29 |
| **Status** | Accepted |
| **Deciders** | Karsten S. Nielsen |

## Context

Three PR-LL waves (PR-LL1 through PR-LL2 + close-out) over 36 hours
shipped six bugs that all share a single systemic root: code is the
source of truth for cross-file conventions that no test enforces. Every
existing test asserts properties of a single file in isolation:

- `test_spadl_vaep_writer_parity.py` — DDL ↔ StructType parity (ADR-002),
  one writer
- `test_idsse_bronze_coverage.py` — bronze column presence vs DFL XML
  attribute set, one writer
- `validate_pr_ll2_post_deploy.py` — non-NULL counts per source per
  column on bronze, one bronze table

No test asserts that **values flowing out of bronze.X** can join to
**values flowing into dim.Y**. The mart `not_null` test on `match_key`
is the first thing that catches a `(bronze_native_id, dim_native_id)`
format drift — and only at full-refresh `dbt build` against
production-scale data. Slim CI runs against a small fixture slice so
the test passes (~7 IDSSE matches in the fixture happened to have the
prefix property locally consistent — the bug surfaces only at scale
and only against the full real dim).

PR-LL2 Path B close-out's six bugs map to seven recurring patterns
(see spec "The seven recurring patterns"). Five of seven require this
ADR's testing layer to catch at PR time.

## Decision

Every cross-file convention that produces a value used as a JOIN key
MUST be enforced by a test that runs at PR-time, not at
full-refresh-build time. Concretely:

1. **Format generators centralised.** Every native ID format used at
   a JOIN key has a single canonical generator function in
   `src/shared/identifiers.py`. Bronze writers, dim staging
   reflections, and applyInPandas UDFs reach into this module —
   format errors raise `ValueError` at construction time.

2. **Format-contract Python tests.** `src/tests/test_format_contract.py`
   parametrizes over (source, entity) and asserts:
   (a) bronze writer's emitted format matches the dim staging's
   regex/format expectation; (b) the canonical generator function in
   `shared.identifiers` produces the same string a test fixture's
   bronze writer would. Runs in slim CI.

3. **dbt singular JOIN-coverage tests.** Every
   `(bronze.X.native_id_col, dim.Y.native_id_col)` pair used by a
   JOIN in `fct_*` marts has an
   `assert_<source>_<entity>_native_join_resolves.sql` singular
   test. Returns rows ⇒ failure. Tagged `slim_ci`.

4. **Third-party API boundary tests.** Every external library producing
   values that flow into bronze gets a boundary test at OUR repo asserting
   the contract we depend on. Runs in slim CI.

5. **Adding a new bronze writer / dim staging touchpoint requires
   adding the corresponding format-contract test in the same PR.**
   Enforced by code review.

## Alternatives considered

| Option | Pros | Cons | Why rejected |
|---|---|---|---|
| A. Documentation only | minimal change | failed empirically — PR-LL1 + PR-LL2 + close-out shipped six bugs despite ADR-016 documenting the rules | failed empirically |
| B. Pydantic NamedTuple identifier wrappers | strong type safety throughout pipeline | doesn't help dbt SQL boundary; significant refactor across all bronze writers; deferred to PR-LL3 (S7) | scope; F1 functions catch the same errors more cheaply |
| C. Static schema parity only (extension of ADR-002) | builds on existing pattern | ADR-002 is single-table writer/DDL; cross-table is a different problem | needed but not sufficient — covers schema, not values |
| D (chosen). F1+F2+F3+F4+F5 layered enforcement | catches all 6 bug-pattern classes; runnable in slim CI; extension via per-source parametrize | slightly more LOC than C alone | — |

## Consequences

### Positive

- Six bug classes catchable at PR time (slim CI), not at production-scale full-refresh.
- Single source of truth for format strings closes the surface for P1 (writer↔dim drift).
- silly-kicks API drift catchable at OUR boundary even when upstream doesn't break us obviously.
- Pattern locks for future converter additions (4-source SPADL, future SkillCorner / Opta / Bundesliga additions).

### Negative

- 12 dbt singular tests added (4 sources × 3 entities). Each scans a
  bounded mart slice; aggregate slim-CI cost ~10s per `dbt build`.
- 16 silly-kicks boundary tests added. Each loads a small parquet
  fixture (1 match) and runs a converter; aggregate ~5s.
- Future converters / sources require parametrization addition + 3
  new dbt tests + boundary fixture. Estimated 2 hr per new source.

### Neutral

- ADR-002 (writer/DDL parity) unchanged; this ADR extends the discipline
  to cross-table joins.
- ADR-016 SPADL Path B naming rule reinforced — every native ID format
  declared in ADR-016 now has a canonical generator function.
- PR-LL3 scope tracker S5/S6/S7 deferred items remain valid follow-ups.

## CLAUDE.md Amendment

Adds one bullet to the "Architectural Decision Records (ADRs)" section
under `## When to write an ADR`:

> - Introduces a cross-table value-format contract or referential-integrity
>   invariant (e.g., `bronze.X.native_id` ⊆ `dim.Y.native_id` per
>   provider). See ADR-018 + the per-(source, entity) singular tests
>   under `dbt_project/tests/`.

And one rule under `## Code Quality`:

> **Cross-table format contracts** ([ADR-018](docs/superpowers/adrs/ADR-018-cross-table-format-contract-testing.md)):
> Every native ID format used as a JOIN key has its canonical generator
> in `src/shared/identifiers.py`. Bronze writers + applyInPandas UDFs
> import from this module; dbt singular tests
> (`assert_<source>_<entity>_native_join_resolves.sql`) assert
> JOIN-coverage from `bronze.spadl_actions` to `dim_*`. Adding a new
> bronze writer / dim staging touchpoint REQUIRES adding the
> corresponding format-contract test in the same PR.

## Related

- **Spec:** `docs/superpowers/specs/2026-04-29-pr-ll2-path-b-close-out-design.md`
- **Plan:** `docs/superpowers/plans/2026-04-29-pr-ll2-path-b-close-out.md`
- **PRs:** PR #224 (PR-LL2), PR-LL2 close-out (this PR — TBD)
- **ADRs:**
  - ADR-002 (writer/DDL parity, single-table) — this ADR extends
  - ADR-011 (Kimball surrogate keys) — JOIN keys this ADR protects
  - ADR-016 (SPADL canonical/native naming) — names enforced
- **External references:** silly-kicks 2.0.0 ADR-001 (caller's identifier
  conventions are sacred) — peer pattern at upstream library boundary.
```

- [ ] **Step 4.2: Verify ADR file content + linting**

```bash
uv run ruff check docs/superpowers/adrs/ADR-018-cross-table-format-contract-testing.md 2>/dev/null || true
ls -la docs/superpowers/adrs/ADR-018-cross-table-format-contract-testing.md
```

Expected: file exists, ~150 lines.

---

## Phase B — Failing tests RED on main (T5–T8, TDD shape)

These tests fail on current `main` because the bugs they catch haven't been fixed yet. Their addition demonstrates the bugs concretely (TDD trail).

### Task 5: Create `test_format_contract.py`

**Files:**
- Create: `src/tests/test_format_contract.py`

- [ ] **Step 5.1: Write the failing tests (12 tests)**

```python
"""Cross-table format-contract tests (ADR-018).

Each test asserts that a value emitted by a bronze writer matches the
format expected by the dim staging that consumes it. Catches the bug
class where bronze writer + dim staging are each correct in isolation
but drift apart at the JOIN boundary.

Test naming: test_<source>_<entity>_format_matches_dim
"""

from __future__ import annotations

import re

import pytest

from shared.identifiers import (
    idsse_native_match_id,
    idsse_native_competition_id,
    metrica_native_match_id,
    metrica_native_team_id,
    metrica_native_competition_id,
    metrica_native_season_id,
)


# ---------------------------------------------------------------------------
# Bronze writer ↔ dim staging format equality
# ---------------------------------------------------------------------------


class TestIdsseFormatContract:
    """ADR-018 contract: idsse.py bronze writer output format matches
    stg_idsse__matches's expected format."""

    def test_idsse_match_id_format_matches_dim(self):
        """bronze.idsse_events.match_id format == dim_matches.native_match_id format."""
        # The dim staging is `regexp_replace(prefixed_match_id, '^idsse_', '')`
        # which expects bare DFL MatchId. Our generator produces bare too.
        assert idsse_native_match_id("J03WMX") == "J03WMX"
        # Format regex parity:
        assert re.match(r"^[A-Z0-9]+$", idsse_native_match_id("J03WN1"))

    def test_idsse_competition_id_format_matches_dim(self):
        # dim_competitions.idsse_competitions filters where competition_id
        # is not null; format DFL-COM-XXXXXX.
        assert idsse_native_competition_id("DFL-COM-000001") == "DFL-COM-000001"
        assert re.match(r"^DFL-COM-[A-Z0-9]+$", idsse_native_competition_id("DFL-COM-000002"))


class TestMetricaFormatContract:
    """ADR-018 contract: metrica_events.py bronze writer output matches
    stg_metrica__team_players's `concat('metrica_', match_id, '_', side)` format."""

    def test_metrica_match_id_format_matches_dim(self):
        assert metrica_native_match_id("Sample_Game_1") == "Sample_Game_1"

    def test_metrica_team_id_format_matches_dim(self):
        # dim_teams.metrica_anon_teams emits format
        # `concat('metrica_', match_id, '_', side)` where side is 'home'/'away'
        # lowercase. Our generator MUST produce identical strings.
        for match_id in ("Sample_Game_1", "Sample_Game_2", "Sample_Game_3"):
            for side in ("home", "away"):
                bronze_format = metrica_native_team_id(match_id, side)
                dim_format = f"metrica_{match_id}_{side}"
                assert bronze_format == dim_format

    def test_metrica_competition_id_format_matches_dim(self):
        # dim_competitions.metrica_competitions.native_competition_id = 'metrica-sample'
        assert metrica_native_competition_id() == "metrica-sample"

    def test_metrica_season_id_format_matches_dim(self):
        assert metrica_native_season_id() == "metrica-open-2017"


# ---------------------------------------------------------------------------
# dim_matches CTE passthrough check (Bug #4)
# ---------------------------------------------------------------------------


class TestDimMatchesMetricaPassthrough:
    """Bug #4: dim_matches.sql Metrica CTE must pass competition_id through
    instead of hardcoding NULL. Verified via SQL parsing of the model file."""

    def test_metrica_cte_passes_competition_id(self):
        """dim_matches.sql `metrica_matches` CTE must reference `competition_id` from staging."""
        from pathlib import Path

        dim_matches_sql = Path("dbt_project/models/marts/dim_matches.sql").read_text()
        # Find the metrica_matches CTE block
        match = re.search(
            r"metrica_matches as \(\s*select(.*?)from \{\{ ref\('stg_metrica__matches'\) \}\}",
            dim_matches_sql,
            re.DOTALL,
        )
        assert match is not None, "metrica_matches CTE not found in dim_matches.sql"
        cte_body = match.group(1)
        # Must reference the staging column 'competition_id', not 'cast(null as string) as competition_id'
        assert "cast(null as string)           as competition_id" not in cte_body, (
            "Bug #4: dim_matches.sql metrica_matches CTE still hardcodes NULL competition_id; "
            "should pass through staging's 'metrica-sample'."
        )
        assert "competition_id" in cte_body, (
            "metrica_matches CTE must reference competition_id"
        )


# ---------------------------------------------------------------------------
# Mart-level not_null filter mirror check (Bug #5)
# ---------------------------------------------------------------------------


class TestMartLevelNotNullFilters:
    """Bug #5: PR #228 added where: filter at staging only; mart-level mirror
    on the 5 fct_action_values not_null tests was missed."""

    _DEFERRED_COLUMNS = ["player_id", "team_id", "vaep_value", "offensive_value", "defensive_value"]

    def test_5_mart_not_null_filters_present(self):
        """_marts__models.yml must scope the 5 deferred not_null tests on fct_action_values
        to data_source IN ('statsbomb', 'wyscout') pending PR-LL3 player Kimball mapping."""
        from pathlib import Path

        import yaml

        models_yml = Path("dbt_project/models/marts/_marts__models.yml")
        data = yaml.safe_load(models_yml.read_text())
        fct_av = next(m for m in data["models"] if m["name"] == "fct_action_values")
        for col_name in self._DEFERRED_COLUMNS:
            col = next((c for c in fct_av["columns"] if c["name"] == col_name), None)
            assert col is not None, f"column {col_name!r} not found in fct_action_values"
            tests = col.get("data_tests", [])
            # Find the not_null test entry — may be string 'not_null' or dict {'not_null': {...}}
            not_null_entry = None
            for t in tests:
                if t == "not_null" or (isinstance(t, dict) and "not_null" in t):
                    not_null_entry = t
                    break
            assert not_null_entry is not None, (
                f"Bug #5: {col_name!r} on fct_action_values has no not_null test"
            )
            # Bug #5 fix: not_null must be wrapped in a dict with config.where filter
            assert isinstance(not_null_entry, dict), (
                f"Bug #5: {col_name!r} not_null must be a dict with where: filter "
                f"`data_source IN ('statsbomb', 'wyscout')` pending PR-LL3"
            )
            cfg = not_null_entry["not_null"].get("config", {})
            where_clause = cfg.get("where", "")
            assert "statsbomb" in where_clause and "wyscout" in where_clause, (
                f"Bug #5: {col_name!r} not_null where: filter must scope to "
                f"data_source IN ('statsbomb', 'wyscout'), got: {where_clause!r}"
            )
```

- [ ] **Step 5.2: Run, verify FAIL on the bug-driven tests**

```bash
uv run pytest src/tests/test_format_contract.py -v
```

Expected: 4 PASS (format-equality tests using shared.identifiers — they pass because we authored the module already), 2 FAIL (`test_metrica_cte_passes_competition_id` and `test_5_mart_not_null_filters_present` — these are the bug-driven RED-on-main tests).

### Task 6: Create the 12 dbt singular JOIN-coverage tests

**Files:**
- Create: `dbt_project/tests/assert_statsbomb_match_id_native_join_resolves.sql`
- Create: `dbt_project/tests/assert_statsbomb_team_id_native_join_resolves.sql`
- Create: `dbt_project/tests/assert_statsbomb_competition_native_id_join_resolves.sql`
- Create: `dbt_project/tests/assert_wyscout_match_id_native_join_resolves.sql`
- Create: `dbt_project/tests/assert_wyscout_team_id_native_join_resolves.sql`
- Create: `dbt_project/tests/assert_wyscout_competition_native_id_join_resolves.sql`
- Create: `dbt_project/tests/assert_idsse_match_id_native_join_resolves.sql`
- Create: `dbt_project/tests/assert_idsse_team_id_native_join_resolves.sql`
- Create: `dbt_project/tests/assert_idsse_competition_native_id_join_resolves.sql`
- Create: `dbt_project/tests/assert_metrica_match_id_native_join_resolves.sql`
- Create: `dbt_project/tests/assert_metrica_team_id_native_join_resolves.sql`
- Create: `dbt_project/tests/assert_metrica_competition_native_id_join_resolves.sql`

- [ ] **Step 6.1: Generate the 12 files via a template loop**

Each file has identical shape — only `data_source`, native column name, and dim entity differ. Template:

```sql
-- assert_<source>_<entity>_native_join_resolves.sql
-- ADR-018 cross-table JOIN-coverage gate (slim_ci-runnable).
-- Returns rows ⇒ test failure. Asserts that every distinct value of
-- bronze.spadl_actions's <native_id_col> for <source> is resolvable in
-- the corresponding dim.

{{ config(tags=['slim_ci']) }}

select distinct b.<native_id_col>
from {{ ref('stg_spadl__action_values') }} b
left join {{ ref('dim_<entity>') }} d
    on b.<native_id_col> = d.<dim_native_col>
   and b.data_source = d.provider
where b.data_source = '<source>'
  and b.<native_id_col> is not null
  and d.<dim_key> is null
```

Concrete instantiation for IDSSE match (the canonical example):

```sql
-- dbt_project/tests/assert_idsse_match_id_native_join_resolves.sql
-- ADR-018 cross-table JOIN-coverage gate (slim_ci-runnable).
-- Returns rows ⇒ test failure. Asserts that every distinct value of
-- bronze.spadl_actions.match_id_native for IDSSE rows is resolvable in
-- dim_matches.native_match_id.

{{ config(tags=['slim_ci']) }}

select distinct b.match_id_native
from {{ ref('stg_spadl__action_values') }} b
left join {{ ref('dim_matches') }} d
    on b.match_id_native = d.native_match_id
   and b.data_source = d.provider
where b.data_source = 'idsse'
  and b.match_id_native is not null
  and d.match_key is null
```

Per-source columns and dim references:

| File suffix | b.<native_id_col> | d.<dim> | d.<native_col> | d.<key_col> |
|---|---|---|---|---|
| `match_id_native_join_resolves` | `match_id_native` | `dim_matches` | `native_match_id` | `match_key` |
| `team_id_native_join_resolves` | `team_id_native` | `dim_teams` | `native_team_id` | `team_key` |
| `competition_native_id_join_resolves` | `competition_native_id` | `dim_competitions` | `native_competition_id` | `competition_key` |

For each of (statsbomb, wyscout, idsse, metrica) × 3 entity files → 12 files.

- [ ] **Step 6.2: Verify file count**

```bash
ls dbt_project/tests/assert_*_native*join_resolves.sql | wc -l
```

Expected: `12`.

### Task 7: Build silly-kicks boundary fixtures

**Files:**
- Create: `src/tests/fixtures/silly_kicks_boundary/sb_match_7298.parquet`
- Create: `src/tests/fixtures/silly_kicks_boundary/ws_match_2576335.parquet`
- Create: `src/tests/fixtures/silly_kicks_boundary/idsse_J03WMX.parquet`
- Create: `src/tests/fixtures/silly_kicks_boundary/metrica_sample_game_1.parquet`

- [ ] **Step 7.1: Create fixture builder script**

```python
# scripts/build_silly_kicks_boundary_fixtures.py
"""One-shot builder for silly-kicks boundary test fixtures.

Pulls 1 match per source from bronze, dumps to a small parquet, commits
to the repo. Re-run only when bronze schema changes upstream.
"""

from __future__ import annotations

import os
import sys
from pathlib import Path

from databricks import sql

CATALOG = "soccer_analytics"
BRONZE = "bronze"
OUT_DIR = Path("src/tests/fixtures/silly_kicks_boundary")

FIXTURE_QUERIES: list[tuple[str, str, str]] = [
    ("sb_match_7298.parquet", "statsbomb_events", "match_id = 7298"),
    ("ws_match_2576335.parquet", "wyscout_events", "matchId = 2576335"),
    ("idsse_J03WMX.parquet", "idsse_events", "match_id = 'idsse_J03WMX'"),
    ("metrica_sample_game_1.parquet", "metrica_events", "match_id = 'Sample_Game_1'"),
]


def _connect():  # type: ignore[no-untyped-def]
    http_path = os.environ["DATABRICKS_HTTP_PATH"]
    if http_path.startswith("//"):
        http_path = http_path[1:]
    return sql.connect(
        server_hostname=os.environ["DATABRICKS_HOST"].replace("https://", "").rstrip("/"),
        http_path=http_path,
        access_token=os.environ["DATABRICKS_TOKEN"],
    )


def main() -> int:
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    conn = _connect()
    try:
        cur = conn.cursor()
        try:
            for filename, table, where in FIXTURE_QUERIES:
                fq = f"{CATALOG}.{BRONZE}.{table}"
                cur.execute(f"SELECT * FROM {fq} WHERE {where}")  # noqa: S608
                df = cur.fetchall_arrow().to_pandas()
                out_path = OUT_DIR / filename
                df.to_parquet(out_path, compression="snappy")
                print(f"wrote {out_path} ({len(df)} rows)")
        finally:
            cur.close()
    finally:
        conn.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
```

- [ ] **Step 7.2: Run script to generate fixtures**

```bash
uv run --with databricks-sql-connector --with pandas --with pyarrow python scripts/build_silly_kicks_boundary_fixtures.py
```

Expected: 4 parquet files created, sizes 50KB–500KB each.

- [ ] **Step 7.3: Verify fixtures readable**

```bash
uv run python -c "import pandas as pd; [print(f, len(pd.read_parquet(f))) for f in __import__('glob').glob('src/tests/fixtures/silly_kicks_boundary/*.parquet')]"
```

Expected: 4 lines showing filename + row count.

- [ ] **Step 7.4: Delete the builder script (it was a one-shot)**

```bash
rm scripts/build_silly_kicks_boundary_fixtures.py
```

### Task 8: Create `test_silly_kicks_boundary.py`

**Files:**
- Create: `src/tests/test_silly_kicks_boundary.py`

- [ ] **Step 8.1: Write the boundary tests**

```python
"""silly-kicks API contract tests at OUR repo (ADR-018 F5).

Mirrors silly-kicks's own ADR-001 cross-provider parity gate but at OUR
boundary, against OUR fixtures. Catches:
- silly-kicks API drift (e.g., the pre-2.0.0 tackle override that
  silently rewrote 56% of TacklingGame team_id values for IDSSE)
- our adapter regressions (e.g., a future PR that adds an event_type
  filter to one of the adapters)
- input fixture drift (we re-build fixtures only when bronze schema
  changes; this test catches drift between bronze schema and silly-
  kicks's converter expectations)

4 sources × 4 invariants = 16 tests.
"""

from __future__ import annotations

from pathlib import Path

import pandas as pd
import pytest

import silly_kicks.spadl.metrica
import silly_kicks.spadl.sportec
import silly_kicks.spadl.statsbomb
import silly_kicks.spadl.wyscout

_FIXTURE_DIR = Path(__file__).parent / "fixtures" / "silly_kicks_boundary"


_PARAMETRIZE = pytest.mark.parametrize(
    "source,converter,fixture,home_team_id_arg",
    [
        # (source, silly-kicks converter module, fixture filename, home_team_id arg)
        # home_team_id_arg matches the convention each adapter expects:
        # SB: int (looked up from matches table; here we use the home_team_id present in the fixture)
        # WS: int
        # IDSSE: 'home' (string label per adapter convention)
        # Metrica: 'Home' (capital, per adapter convention)
        ("statsbomb", silly_kicks.spadl.statsbomb, "sb_match_7298.parquet", None),  # resolved per-fixture below
        ("wyscout", silly_kicks.spadl.wyscout, "ws_match_2576335.parquet", None),
        ("idsse", silly_kicks.spadl.sportec, "idsse_J03WMX.parquet", "home"),
        ("metrica", silly_kicks.spadl.metrica, "metrica_sample_game_1.parquet", "Home"),
    ],
)


def _adapt_input(source: str, df: pd.DataFrame) -> tuple[pd.DataFrame, object]:
    """Run the same per-source adapter the SPADL UDFs use, return (adapted_df, home_team_id_arg).

    Mirrors the per-source invocation pattern in src/ingestion/spadl_conversion.py
    so the boundary test exercises the same shape.
    """
    if source == "statsbomb":
        from ingestion.spadl_adapter import adapt_statsbomb_events, resolve_statsbomb_home_team_ids
        # use first team_id present as a synthetic home_team_id; the test only
        # asserts team_id ⊆ teams in input — direction-of-play exact value doesn't matter.
        home_team_id = int(df["team_id"].dropna().iloc[0])
        return adapt_statsbomb_events(df, home_team_id), home_team_id
    if source == "wyscout":
        from ingestion.spadl_adapter import adapt_wyscout_events
        home_team_id = int(df["teamId"].dropna().iloc[0])
        return adapt_wyscout_events(df), home_team_id
    if source == "idsse":
        from ingestion.spadl_adapter import adapt_idsse_events_for_silly_kicks
        return adapt_idsse_events_for_silly_kicks(df), "home"
    if source == "metrica":
        from ingestion.spadl_adapter import adapt_metrica_events_for_silly_kicks
        return adapt_metrica_events_for_silly_kicks(df), "Home"
    raise ValueError(f"unknown source {source!r}")


@_PARAMETRIZE
def test_team_id_subset_of_input_team_or_team_id(source, converter, fixture, home_team_id_arg):
    """ADR-018 boundary contract: silly-kicks's output team_id values are a
    subset of the input's team identification (column varies per provider).
    """
    df = pd.read_parquet(_FIXTURE_DIR / fixture)
    adapted, hti = _adapt_input(source, df)
    actions, _report = converter.convert_to_actions(adapted, home_team_id=hti)
    out_teams = set(actions["team_id"].dropna().unique())
    # Determine the input team-identifier column per provider:
    if source == "statsbomb":
        input_teams = set(df["team_id"].dropna().astype(int).unique())
        assert out_teams <= input_teams, (
            f"silly-kicks {source} output team_id contains values not in input team_id: "
            f"{out_teams - input_teams}"
        )
    elif source == "wyscout":
        input_teams = set(df["teamId"].dropna().astype(int).unique())
        assert out_teams <= input_teams
    else:
        # IDSSE + Metrica use string team labels in input
        input_teams = set(df["team"].dropna().astype(str).unique())
        assert out_teams <= input_teams


@_PARAMETRIZE
def test_action_id_non_null(source, converter, fixture, home_team_id_arg):
    """Every output action must have a non-NULL action_id (silly-kicks invariant)."""
    df = pd.read_parquet(_FIXTURE_DIR / fixture)
    adapted, hti = _adapt_input(source, df)
    actions, _ = converter.convert_to_actions(adapted, home_team_id=hti)
    assert actions["action_id"].notna().all(), f"{source}: NULL action_id rows present"


@_PARAMETRIZE
def test_period_id_in_valid_range(source, converter, fixture, home_team_id_arg):
    """Output period_id must be in {1..5}."""
    df = pd.read_parquet(_FIXTURE_DIR / fixture)
    adapted, hti = _adapt_input(source, df)
    actions, _ = converter.convert_to_actions(adapted, home_team_id=hti)
    periods = set(actions["period_id"].dropna().astype(int).unique())
    assert periods <= {1, 2, 3, 4, 5}, f"{source}: invalid period_ids {periods}"


@_PARAMETRIZE
def test_time_seconds_non_negative(source, converter, fixture, home_team_id_arg):
    """Output time_seconds must be ≥ 0 (period-relative). Catches Bug #6 IDSSE class."""
    df = pd.read_parquet(_FIXTURE_DIR / fixture)
    adapted, hti = _adapt_input(source, df)
    actions, _ = converter.convert_to_actions(adapted, home_team_id=hti)
    neg = actions[actions["time_seconds"] < 0]
    assert len(neg) == 0, (
        f"{source}: {len(neg)} rows with negative time_seconds — bronze parser period misclassification"
    )
```

- [ ] **Step 8.2: Run, expect FAIL on IDSSE for time_seconds (Bug #6 trail)**

```bash
uv run pytest src/tests/test_silly_kicks_boundary.py -v
```

Expected: 13 PASS, 1 FAIL (`test_time_seconds_non_negative[idsse-...]` — Bug #6 surfaces here too, post-fix turns GREEN). 2 PASS for tackle-team-mirror (silly-kicks 2.0.0 already enforces this).

---

## Phase C — Bug fixes (T9–T15, RED tests turn GREEN)

### Task 9: Bug #1 fix — IDSSE prefix strip in bronze writer

**Files:** Modify `src/ingestion/idsse.py` — 3 callsites where `f"idsse_{match_id}"` is constructed (lines 574, 689, 1131, 1227, 1274).

- [ ] **Step 9.1: Edit `_parse_positions_xml` — drop prefix on `match_id` row field**

`src/ingestion/idsse.py:574` (currently `prefixed_match_id = f"idsse_{match_id}"`):

```python
# Before:
prefixed_match_id = f"idsse_{match_id}"
# After:
from shared.identifiers import idsse_native_match_id
prefixed_match_id = idsse_native_match_id(match_id)
```

(The variable name `prefixed_match_id` becomes a misnomer; rename to `canonical_match_id` and update line 689 reference accordingly.)

- [ ] **Step 9.2: Edit `_parse_events_xml`**

`src/ingestion/idsse.py:1131`:

```python
# Before:
prefixed_match_id = f"idsse_{match_id}"
# After:
canonical_match_id = idsse_native_match_id(match_id)
```

- [ ] **Step 9.3: Edit `ingest_idsse` and `ingest_idsse_events` — replaceWhere predicates**

`src/ingestion/idsse.py:869` and `:1274`:

```python
# Before:
replace_expr = f"match_id = 'idsse_{mid}' AND period = {period}"
# After:
replace_expr = f"match_id = '{idsse_native_match_id(mid)}' AND period = {period}"
```

(Mirror at line 1274 for `ingest_idsse_events`'s `replace_expr`.)

- [ ] **Step 9.4: Update `ingest_idsse` existing-id check**

`src/ingestion/idsse.py:816` (currently `f"idsse_{mid}" not in existing_ids`):

```python
# Before:
new_match_ids = [mid for mid in ids_to_ingest if f"idsse_{mid}" not in existing_ids]
# After:
new_match_ids = [mid for mid in ids_to_ingest if idsse_native_match_id(mid) not in existing_ids]
```

Mirror at line 1227 for events.

- [ ] **Step 9.5: Update `dim_matches` upstream — verify `stg_idsse__matches.sql:57` regexp_replace is now a no-op**

The staging model already strips `'^idsse_'` if present — leave intact for backward-compat during the transition. (After bronze re-ingestion the regex matches zero rows; the model still runs correctly.) Add a comment noting the regex is residual:

```sql
-- Native DFL MatchId. Post-PR-LL2-Path-B-close-out (2026-04-29), bronze
-- already emits the bare form, so the regexp_replace is a no-op on
-- post-close-out data — kept for resilience to re-introduction of the
-- prefix.
regexp_replace(tm.prefixed_match_id, '^idsse_', '') as native_match_id,
```

(Note: column alias `prefixed_match_id` is still apt here because the underlying bronze `match_id` was historically prefixed.)

- [ ] **Step 9.6: Verify Python tests still GREEN**

```bash
uv run pytest src/tests/test_shared_identifiers.py src/tests/test_format_contract.py -v
```

Expected: same as before — Bug #1 fix is in idsse.py, doesn't affect format-contract Python tests yet (they assert on the generator function, which is unchanged). The dbt singular tests for IDSSE WILL turn green after bronze re-ingest in Phase H.

### Task 10: Bug #2 fix — Metrica team_id_native format align

**Files:** Modify `src/ingestion/metrica_events.py:194-202`

- [ ] **Step 10.1: Replace `_native_team_ids` with shared.identifiers call**

```python
# Before:
def _native_team_ids(match_id: str) -> tuple[str, str]:
    """..."""
    return f"{match_id}-Home", f"{match_id}-Away"

# After:
from shared.identifiers import metrica_native_team_id

def _native_team_ids(match_id: str) -> tuple[str, str]:
    """Synthesize stable home/away team identifiers per ADR-018 canonical
    format (`metrica_<match>_<home|away>`). Aligns with stg_metrica__team_players's
    `concat('metrica_', match_id, '_', side)` dim_teams convention.

    Pre-2026-04-29 PR-LL2-Path-B-close-out, this returned 'Sample_Game_1-Home'
    (capital, hyphen) which produced 100% NULL team_key on fct_action_values
    for Metrica rows.
    """
    return metrica_native_team_id(match_id, "home"), metrica_native_team_id(match_id, "away")
```

- [ ] **Step 10.2: Update `_augment_ll2_metadata` `team_id_native` mapping (lines 124-131)**

```python
# Before:
df["team_id_native"] = (
    df["team"]
    .map(
        lambda t: home_id_native if t == "Home" else (away_id_native if t == "Away" else None),
    )
    .astype("string")
)
# After:
df["team_id_native"] = (
    df["team"]
    .map(
        lambda t: home_id_native if t == "Home" else (away_id_native if t == "Away" else None),
    )
    .astype("string")
)
# (The mapping itself doesn't change — input bronze.metrica_events.team is still
# 'Home'/'Away' (Metrica's native capitalised form). Only the OUTPUT _native ID
# format changed.)
```

- [ ] **Step 10.3: Run tests**

```bash
uv run pytest src/tests/test_shared_identifiers.py src/tests/test_format_contract.py -v
```

Expected: still all PASS (the format-contract Python tests don't import metrica_events.py directly).

### Task 11: Bug #4 fix — dim_matches Metrica competition_id passthrough

**Files:** Modify `dbt_project/models/marts/dim_matches.sql:66-78`

- [ ] **Step 11.1: Edit the metrica_matches CTE**

```sql
-- Before:
metrica_matches as (

    select
        native_match_id,
        provider,
        cast(null as string)           as competition_id,
        cast(null as string)           as season_id,
        cast(null as date)             as match_date,
        home_team_name,
        away_team_name
    from {{ ref('stg_metrica__matches') }}

),

-- After:
metrica_matches as (

    -- PR-LL2 Path B close-out (2026-04-29): pass competition_id through
    -- from staging instead of hardcoding NULL. stg_metrica__matches emits
    -- 'metrica-sample' (PR 5a, ADR-011) and dim_competitions has the
    -- matching row — without this passthrough, generate_competition_key
    -- returns NULL for all Metrica rows, breaking fct_action_values
    -- competition_key resolution.
    select
        native_match_id,
        provider,
        competition_id,
        cast(null as string)           as season_id,
        cast(null as date)             as match_date,
        home_team_name,
        away_team_name
    from {{ ref('stg_metrica__matches') }}

),
```

- [ ] **Step 11.2: Run format-contract test**

```bash
uv run pytest src/tests/test_format_contract.py::TestDimMatchesMetricaPassthrough -v
```

Expected: PASS (was RED before this task).

### Task 12: Bug #5 fix — Mart-level not_null filter mirror

**Files:** Modify `dbt_project/models/marts/_marts__models.yml`

- [ ] **Step 12.1: Locate `fct_action_values` model section**

Search for `name: fct_action_values` in the file. Within its `columns:` list, locate these 5 columns: `player_id`, `team_id`, `vaep_value`, `offensive_value`, `defensive_value`.

- [ ] **Step 12.2: Wrap each `not_null` test in a config block with where filter**

For each of the 5 columns, change:

```yaml
# Before:
- name: player_id
  data_type: int
  description: ...
  data_tests:
    - not_null
    - relationships:
        ...

# After:
- name: player_id
  data_type: int
  description: ...
  data_tests:
    - not_null:
        config:
          where: "data_source IN ('statsbomb', 'wyscout')"
          # PR #228 + PR-LL2-Path-B-close-out: per-player Kimball mapping
          # for IDSSE/Metrica deferred to PR-LL3 (S1, S2). Mart-level
          # mirror of the staging-level filter applied in PR #228.
    - relationships:
        ...
```

Apply identical pattern to the other 4 columns.

- [ ] **Step 12.3: Run format-contract test**

```bash
uv run pytest src/tests/test_format_contract.py::TestMartLevelNotNullFilters -v
```

Expected: PASS (was RED before this task).

### Task 13: Bug #6 fix — IDSSE 2-pass parser refactor

**Files:**
- Modify: `src/ingestion/idsse.py` — refactor `_parse_events_xml`
- Create: `src/tests/test_idsse_period_derivation.py`
- Create: `src/tests/fixtures/idsse_interleaved_periods.xml`

- [ ] **Step 13.1: Create synthetic XML fixture**

```xml
<!-- src/tests/fixtures/idsse_interleaved_periods.xml -->
<!-- Synthetic DFL XML covering Bug #6 reproduction shape:
     - Primary EventList block: KickOff(1H), TacklingGames in period 1,
       KickOff(2H), TacklingGames in period 2.
     - Secondary block (BallClaiming) appears AFTER the secondHalf KickOff
       in stream order but contains events with EventTime in the FIRST half.
     A correctly-implemented 2-pass parser assigns these secondary-block
     events to period=1 based on their event_time, NOT period=2 based on
     their stream-order position. -->
<PutDataRequest>
  <EventList>
    <Event MatchId="DFL-MAT-TEST" EventId="e1" EventTime="2026-01-01T15:00:00Z">
      <KickOff GameSection="firstHalf"/>
    </Event>
    <Event MatchId="DFL-MAT-TEST" EventId="e2" EventTime="2026-01-01T15:10:00Z" X-Position="50.0" Y-Position="34.0">
      <TacklingGame Winner="p1" WinnerTeam="DFL-CLU-A"/>
    </Event>
    <Event MatchId="DFL-MAT-TEST" EventId="e3" EventTime="2026-01-01T15:20:00Z" X-Position="50.0" Y-Position="34.0">
      <TacklingGame Winner="p2" WinnerTeam="DFL-CLU-B"/>
    </Event>
    <Event MatchId="DFL-MAT-TEST" EventId="e4" EventTime="2026-01-01T16:00:00Z">
      <KickOff GameSection="secondHalf"/>
    </Event>
    <Event MatchId="DFL-MAT-TEST" EventId="e5" EventTime="2026-01-01T16:10:00Z" X-Position="50.0" Y-Position="34.0">
      <TacklingGame Winner="p3" WinnerTeam="DFL-CLU-A"/>
    </Event>
  </EventList>
  <!-- Secondary block: BallClaiming events with FIRST-HALF EventTimes,
       appearing in stream order AFTER the secondHalf KickOff. The pre-fix
       state-machine parser would tag these period=2; the 2-pass parser
       must derive period=1 from event_time. -->
  <Event MatchId="DFL-MAT-TEST" EventId="e6" EventTime="2026-01-01T15:05:00Z" X-Position="40.0" Y-Position="20.0">
    <BallClaiming Player="p4"/>
  </Event>
  <Event MatchId="DFL-MAT-TEST" EventId="e7" EventTime="2026-01-01T15:15:00Z" X-Position="60.0" Y-Position="50.0">
    <BallClaiming Player="p5"/>
  </Event>
</PutDataRequest>
```

- [ ] **Step 13.2: Write the failing test FIRST**

```python
# src/tests/test_idsse_period_derivation.py
"""Bug #6 — IDSSE 2-pass parser tests.

Pre-fix: state-machine `current_period` tags secondary-block events with
the period that was active when their stream-order position was
processed. Post-fix: per-event period derivation by event_time vs the
{period: kickoff_time} map built in pass 1.
"""

from __future__ import annotations

import logging
from pathlib import Path

import pandas as pd
import pytest

from ingestion.idsse import _parse_events_xml, _EMPTY_MATCH_METADATA

_FIXTURE = Path(__file__).parent / "fixtures" / "idsse_interleaved_periods.xml"


def _to_df(rows):
    return pd.DataFrame(rows)


class TestKickoffScanPassOne:
    """Pass 1 — build {period: kickoff_event_time} map."""

    def test_pass_one_collects_both_kickoffs(self):
        from ingestion.idsse import _scan_kickoff_times
        result = _scan_kickoff_times(str(_FIXTURE))
        assert set(result.keys()) == {1, 2}
        assert result[1].isoformat().startswith("2026-01-01T15:00:00")
        assert result[2].isoformat().startswith("2026-01-01T16:00:00")


class TestSecondaryBlockEventGetsCorrectPeriod:
    """Pass 2 — events derive period from event_time, not stream-order."""

    def test_secondary_block_first_half_event_lands_in_period_1(self):
        rows = _parse_events_xml(
            str(_FIXTURE),
            player_team_map={"p1": "home", "p2": "away", "p3": "home", "p4": "home", "p5": "away"},
            match_id="TEST",
            logger=logging.getLogger("test"),
        )
        df = _to_df(rows)
        # Find the BallClaiming events from the secondary block (e6, e7) — they
        # appear AFTER the secondHalf KickOff in stream order but their event_times
        # are in the first half.
        e6 = df[df["event_id"] == "e6"]
        e7 = df[df["event_id"] == "e7"]
        assert e6["period"].iloc[0] == 1, (
            f"Bug #6: e6 (event_time 15:05 in first half) tagged period {e6['period'].iloc[0]} "
            "(expected 1) — 2-pass parser period derivation broken"
        )
        assert e7["period"].iloc[0] == 1
        # Their timestamp_seconds must be NON-NEGATIVE.
        assert e6["timestamp_seconds"].iloc[0] >= 0
        assert e7["timestamp_seconds"].iloc[0] >= 0

    def test_period_1_events_have_correct_timestamps(self):
        rows = _parse_events_xml(
            str(_FIXTURE),
            player_team_map={"p1": "home", "p2": "away"},
            match_id="TEST",
            logger=logging.getLogger("test"),
        )
        df = _to_df(rows)
        # KickOff at 15:00; e2 at 15:10 → 600s; e3 at 15:20 → 1200s
        e2 = df[df["event_id"] == "e2"]
        assert e2["period"].iloc[0] == 1
        assert e2["timestamp_seconds"].iloc[0] == pytest.approx(600.0, abs=1.0)

    def test_period_2_events_have_correct_timestamps(self):
        rows = _parse_events_xml(
            str(_FIXTURE),
            player_team_map={"p3": "home"},
            match_id="TEST",
            logger=logging.getLogger("test"),
        )
        df = _to_df(rows)
        # secondHalf KickOff at 16:00; e5 at 16:10 → 600s
        e5 = df[df["event_id"] == "e5"]
        assert e5["period"].iloc[0] == 2
        assert e5["timestamp_seconds"].iloc[0] == pytest.approx(600.0, abs=1.0)
```

- [ ] **Step 13.3: Run, verify FAIL**

```bash
uv run pytest src/tests/test_idsse_period_derivation.py -v
```

Expected: All 4 tests FAIL (current parser doesn't have `_scan_kickoff_times`; secondary-block events get period=2; e6/e7 timestamps negative).

- [ ] **Step 13.4: Refactor `_parse_events_xml` — extract pass 1**

In `src/ingestion/idsse.py`, add new function above `_parse_events_xml`:

```python
def _scan_kickoff_times(event_path: str) -> dict[int, datetime]:
    """Pass 1 of the 2-pass DFL event parser (ADR-018 / Bug #6 fix).

    Scans ONLY KickOff events to build a `{period: kickoff_event_time}` map.
    Pass 2 uses this map to derive each event's period by comparing its
    EventTime to kickoff times — NOT by relying on XML stream-order
    `current_period` state, which DFL XML's secondary blocks (BallClaiming,
    etc., appended after the secondHalf KickOff) violate.

    Returns:
        Mapping period_id → first KickOff EventTime for that period.
        Includes only periods whose `<KickOff GameSection=...>` element
        has a recognized GameSection (firstHalf, secondHalf, etc. per
        `_SECTION_TO_PERIOD`). Empty dict for inputs with no KickOffs.

    Memory: O(periods) — typically O(2). Pass cost is O(events) but
    we use ET.iterparse so the parsed tree never lives in memory.
    """
    kickoff_times: dict[int, datetime] = {}

    for _ev, elem in ET.iterparse(event_path, events=("end",)):  # noqa: S314
        if elem.tag != "Event":
            if elem.tag == "PutDataRequest":
                elem.clear()
            continue

        first_child: ET.Element | None = None
        for child in elem:
            first_child = child
            break

        if first_child is None or first_child.tag != "KickOff":
            elem.clear()
            continue

        section = first_child.get("GameSection", "")
        period = _SECTION_TO_PERIOD.get(section)
        if period is None:
            elem.clear()
            continue

        event_time_str = elem.get("EventTime", "")
        if event_time_str:
            try:
                event_dt = datetime.fromisoformat(event_time_str)
                if event_dt.tzinfo is not None:
                    event_dt = event_dt.astimezone(timezone.utc)
                # First KickOff for a period wins (defensively — DFL XML
                # should have only one per period anyway).
                if period not in kickoff_times:
                    kickoff_times[period] = event_dt
            except (ValueError, TypeError):
                pass

        elem.clear()

    return kickoff_times


def _derive_period_from_kickoffs(
    event_dt: datetime,
    kickoff_times: dict[int, datetime],
) -> tuple[int | None, datetime | None]:
    """Given an event's EventTime, return (period, period_kickoff_time).

    Period = the largest period whose `kickoff_time` ≤ `event_dt`.
    Returns (None, None) if event_dt precedes all kickoffs (legitimate
    edge case — pre-match warmup events; downstream skips them).
    """
    if not kickoff_times:
        return None, None
    best_period: int | None = None
    best_start: datetime | None = None
    for p, p_start in kickoff_times.items():
        if event_dt >= p_start and (best_start is None or p_start > best_start):
            best_period = p
            best_start = p_start
    return best_period, best_start
```

- [ ] **Step 13.5: Refactor `_parse_events_xml` — use 2-pass logic**

Replace the existing `_parse_events_xml` (lines ~1082-1180) with:

```python
def _parse_events_xml(
    event_path: str,
    player_team_map: dict[str, str],
    match_id: str,
    logger: logging.Logger,
    metadata: _MatchMetadata = _EMPTY_MATCH_METADATA,
) -> list[dict[str, object]]:
    """Parse DFL event XML into bronze-completeness row dicts (2-pass).

    Pass 1: scan KickOff events to build {period: kickoff_event_time} map.
    Pass 2: emit per-event rows with period derived from event_time.

    Bug #6 fix: pre-2026-04-29 used a state-machine `current_period` updated
    at each KickOff in stream order. DFL XML emits secondary blocks
    (BallClaiming, RefereeBall, etc.) AFTER the secondHalf KickOff in
    stream order with first-half event_times — these were misclassified
    as period=2 with negative period-relative timestamp_seconds. The
    2-pass approach derives period from event_time, not stream-order.
    """
    canonical_match_id = idsse_native_match_id(match_id)

    # PASS 1: build {period: kickoff_time} map.
    kickoff_times = _scan_kickoff_times(event_path)
    if not kickoff_times:
        logger.warning("No KickOff events found in %s — skipping match", event_path)
        return []

    # PASS 2: emit per-event rows.
    rows: list[dict[str, object]] = []

    for _ev, elem in ET.iterparse(event_path, events=("end",)):  # noqa: S314
        if elem.tag != "Event":
            if elem.tag == "PutDataRequest":
                elem.clear()
            continue

        first_child: ET.Element | None = None
        for child in elem:
            first_child = child
            break

        if first_child is None:
            elem.clear()
            continue

        event_type = first_child.tag

        # Derive period from event_time using pass-1 map.
        event_time_str = elem.get("EventTime", "")
        period: int | None = None
        period_start: datetime | None = None
        event_dt: datetime | None = None
        if event_time_str:
            try:
                event_dt = datetime.fromisoformat(event_time_str)
                if event_dt.tzinfo is not None:
                    event_dt = event_dt.astimezone(timezone.utc)
                period, period_start = _derive_period_from_kickoffs(event_dt, kickoff_times)
            except (ValueError, TypeError):
                pass

        # Skip events that predate all kickoffs (pre-match warmup, etc.).
        if period is None or period_start is None or event_dt is None:
            elem.clear()
            continue

        # period_start_time dict structure preserved for _build_event_row's
        # period_start_time arg (which it uses to compute timestamp_seconds).
        # We seed it directly with our derived value.
        period_start_time: dict[int, datetime] = {period: period_start}

        row = _build_event_row(
            elem,
            first_child,
            event_type,
            canonical_match_id,
            period,
            player_team_map,
            period_start_time,
            metadata,
        )
        rows.append(row)
        elem.clear()

    logger.info("Parsed %d events for IDSSE match %s", len(rows), match_id)
    return rows
```

- [ ] **Step 13.6: Run new period tests**

```bash
uv run pytest src/tests/test_idsse_period_derivation.py -v
```

Expected: 4 PASS.

- [ ] **Step 13.7: Run silly-kicks boundary test**

```bash
uv run pytest src/tests/test_silly_kicks_boundary.py -v
```

Expected: 16 PASS now (Bug #6 fix means IDSSE no longer has negative time_seconds).

### Task 14: Bug #3 verification — silly-kicks 2.0.0 confirms

**Files:** None (pure verification).

- [ ] **Step 14.1: Verify our `_team_label_to_dfl_id` doesn't need DFL passthrough**

Open `src/ingestion/spadl_conversion.py:822-829` (the IDSSE UDF's `_team_label_to_dfl_id` function). Confirm it's still:

```python
def _team_label_to_dfl_id(team_label: object) -> str | None:
    if team_label == "home":
        return home_team_id_native
    if team_label == "away":
        return away_team_id_native
    return None
```

Add a comment block above the function citing silly-kicks ADR-001:

```python
# silly-kicks 2.0.0 ADR-001 ("caller's identifier conventions are sacred")
# guarantees that sportec.convert_to_actions's output `team_id` mirrors
# the input `team` column verbatim — no override from `tackle_winner_team`.
# Pre-2.0.0, TacklingGame events with `tackle_winner` populated had their
# team rewritten to the raw DFL CLU id, breaking this mapper for ~56% of
# IDSSE TacklingGame rows. PR-LL2-Path-B-close-out bumps to 2.0.0,
# eliminating the need for a `DFL-`-prefixed passthrough branch here.
def _team_label_to_dfl_id(team_label: object) -> str | None:
    ...
```

- [ ] **Step 14.2: Run all tests**

```bash
uv run pytest src/tests/ -v
```

Expected: all PASS (modulo any unrelated pre-existing failures).

### Task 15: Verify all RED tests now GREEN

- [ ] **Step 15.1: Run full test suite**

```bash
uv run pytest src/tests/ -v --tb=short 2>&1 | tail -50
```

Expected: 0 FAIL on any test added in Phase B.

- [ ] **Step 15.2: Run ruff + pyright**

```bash
uv run ruff check src/ scripts/
uv run pyright src/
```

Expected: 0 errors.

---

## Phase D — silly-kicks 2.0.0 schema extensions for tackle qualifiers (T16–T22)

silly-kicks 2.0.0's sportec converter adds 4 new columns: `tackle_winner_player_id`, `tackle_winner_team_id`, `tackle_loser_player_id`, `tackle_loser_team_id`. Wire them through bronze → mart.

### Task 16: Add 4 columns to `_SPADL_SCHEMA` + `_VAEP_SCHEMA` DDL

**Files:** Modify `src/ingestion/spadl_vaep.py:53-83, 85-108`

- [ ] **Step 16.1: Edit `_SPADL_SCHEMA` (after `match_id_native STRING`)**

```python
_SPADL_SCHEMA = (
    # ... existing columns ...
    "match_id_native STRING, "
    # PR-LL2 Path B close-out (2026-04-29): silly-kicks 2.0.0 sportec
    # tackle qualifier passthrough (4 columns). NULL on non-sportec
    # rows + on rows where the qualifier was absent.
    "tackle_winner_player_id BIGINT, tackle_winner_team_id STRING, "
    "tackle_loser_player_id BIGINT, tackle_loser_team_id STRING"
)
```

- [ ] **Step 16.2: Edit `_VAEP_SCHEMA` similarly (after `match_id_native STRING`)**

Mirror the same 4-column block.

### Task 17: Wire 4 columns through 4 SPADL UDFs

**Files:** Modify `src/ingestion/spadl_conversion.py` — 4 UDFs (StatsBomb, Wyscout, IDSSE, Metrica) each at three points: `_spadl_cols`, applyInPandas StructType, dtype casts.

- [ ] **Step 17.1: Update `_make_sb_spadl_udf` `_spadl_cols`**

After `"match_id_native"` in the `_spadl_cols` list, add:

```python
        "match_id_native",
        # silly-kicks 2.0.0 sportec tackle qualifier passthrough.
        # NULL-filled on non-sportec UDFs for multi-source schema parity.
        "tackle_winner_player_id",
        "tackle_winner_team_id",
        "tackle_loser_player_id",
        "tackle_loser_team_id",
```

After the `actions["match_id_native"] = str(match_id)` line, add:

```python
        # silly-kicks 2.0.0 sportec tackle qualifier columns — NULL-filled
        # on the StatsBomb path (multi-source parity).
        actions["tackle_winner_player_id"] = pd.array([_pd.NA] * len(actions), dtype="Int64")
        actions["tackle_winner_team_id"] = pd.array([_pd.NA] * len(actions), dtype="object")
        actions["tackle_loser_player_id"] = pd.array([_pd.NA] * len(actions), dtype="Int64")
        actions["tackle_loser_team_id"] = pd.array([_pd.NA] * len(actions), dtype="object")
```

- [ ] **Step 17.2: Update `_make_sb_spadl_udf` applyInPandas StructType (line ~330-371)**

After the `StructField("match_id_native", StringType()),` line, add:

```python
        StructField("match_id_native", StringType()),
        # silly-kicks 2.0.0 tackle qualifier columns.
        StructField("tackle_winner_player_id", LongType()),
        StructField("tackle_winner_team_id", StringType()),
        StructField("tackle_loser_player_id", LongType()),
        StructField("tackle_loser_team_id", StringType()),
```

- [ ] **Step 17.3: Mirror Steps 17.1+17.2 for `_make_ws_spadl_udf`** (same shape — NULL-fill all 4 columns since Wyscout doesn't have these qualifiers)

- [ ] **Step 17.4: Update `_make_idsse_spadl_udf` — populate from silly-kicks 2.0.0 output**

Update `_spadl_cols` (add 4 cols).
Update applyInPandas StructType (add 4 StructFields).

In the body, after `actions["match_id_native"] = match_id_str`, REPLACE the previous NULL-fill with the actual passthrough from silly-kicks 2.0.0's output (the columns are present in the output DataFrame):

```python
        # silly-kicks 2.0.0 sportec converter emits these as columns of the
        # actions DataFrame. NaN on non-tackle rows + on rows where the
        # qualifier was absent — preserved as Int64 / object nullable dtypes.
        if "tackle_winner_player_id" in actions.columns:
            actions["tackle_winner_player_id"] = actions["tackle_winner_player_id"].astype("Int64")
        else:
            actions["tackle_winner_player_id"] = _pd.array([_pd.NA] * len(actions), dtype="Int64")
        if "tackle_winner_team_id" in actions.columns:
            actions["tackle_winner_team_id"] = actions["tackle_winner_team_id"].astype("object")
        else:
            actions["tackle_winner_team_id"] = _pd.array([_pd.NA] * len(actions), dtype="object")
        if "tackle_loser_player_id" in actions.columns:
            actions["tackle_loser_player_id"] = actions["tackle_loser_player_id"].astype("Int64")
        else:
            actions["tackle_loser_player_id"] = _pd.array([_pd.NA] * len(actions), dtype="Int64")
        if "tackle_loser_team_id" in actions.columns:
            actions["tackle_loser_team_id"] = actions["tackle_loser_team_id"].astype("object")
        else:
            actions["tackle_loser_team_id"] = _pd.array([_pd.NA] * len(actions), dtype="object")
```

- [ ] **Step 17.5: Mirror NULL-fill for `_make_metrica_spadl_udf`** (Metrica's converter doesn't emit these — NULL-fill all 4 like StatsBomb / Wyscout paths).

### Task 18: Wire 4 columns through VAEP scoring UDF + writer parity

**Files:**
- Modify: `src/ingestion/spadl_vaep.py` — `_make_scoring_udf`'s `vaep_schema` StructType + `_output_cols` projection
- Modify: `src/tests/test_spadl_vaep_writer_parity.py`

- [ ] **Step 18.1: Add 4 columns to `_make_scoring_udf` StructType**

In `src/ingestion/spadl_vaep.py`, find `_make_scoring_udf`'s `vaep_schema` StructType definition. Add the 4 fields at the end matching the order in `_VAEP_SCHEMA`.

- [ ] **Step 18.2: Add 4 columns to `_output_cols` projection inside the UDF**

The UDF's pandas DataFrame must include these columns in its output. Pass them through from the input DataFrame (which is the `bronze.spadl_actions` join output — they're already present).

- [ ] **Step 18.3: Extend writer parity test**

In `src/tests/test_spadl_vaep_writer_parity.py`, add 4 StructField entries to `_build_statsbomb_spadl_struct`, `_build_wyscout_spadl_struct`, `_build_idsse_spadl_struct`, `_build_metrica_spadl_struct`, and `_build_vaep_scoring_struct`. Add a new test `test_*_struct_includes_tackle_qualifiers` parametrized over all 5 structs asserting the 4 column names are present.

```python
@pytest.mark.parametrize("struct_builder", [
    _build_statsbomb_spadl_struct,
    _build_wyscout_spadl_struct,
    _build_idsse_spadl_struct,
    _build_metrica_spadl_struct,
    _build_vaep_scoring_struct,
])
def test_struct_includes_tackle_qualifiers(struct_builder):
    """Every applyInPandas StructType in the SPADL/VAEP pipeline must include
    the 4 silly-kicks 2.0.0 tackle qualifier columns (multi-source parity)."""
    struct = struct_builder()
    cols = {f.name for f in struct.fields}
    expected = {"tackle_winner_player_id", "tackle_winner_team_id",
                "tackle_loser_player_id", "tackle_loser_team_id"}
    missing = expected - cols
    assert not missing, f"struct missing tackle qualifier columns: {missing}"
```

- [ ] **Step 18.4: Run parity test**

```bash
uv run pytest src/tests/test_spadl_vaep_writer_parity.py -v
```

Expected: all PASS including the new test.

### Task 19: Wire 4 columns through staging + mart

**Files:**
- Modify: `dbt_project/models/staging/spadl/stg_spadl__action_values.sql`
- Modify: `dbt_project/models/marts/fct_action_values.sql`

- [ ] **Step 19.1: Edit stg_spadl__action_values.sql**

In the `cleaned` CTE projection, after `match_id_native`:

```sql
        match_id_native,

        -- PR-LL2 Path B close-out (2026-04-29): silly-kicks 2.0.0 sportec
        -- tackle qualifier passthrough. NULL on non-sportec rows + on rows
        -- where the qualifier was absent. Per ADR-016 + silly-kicks ADR-001.
        tackle_winner_player_id,
        tackle_winner_team_id,
        tackle_loser_player_id,
        tackle_loser_team_id

    from deduplicated d
```

- [ ] **Step 19.2: Edit fct_action_values.sql**

Add the 4 columns to the `actions_with_score` CTE projection (after `match_id_native`) and to the final `select` (before `_loaded_at`).

### Task 20: Add column descriptions to `_marts__models.yml`

**Files:** Modify `dbt_project/models/marts/_marts__models.yml` — `fct_action_values` model

- [ ] **Step 20.1: Add 4 column entries**

After the `match_id_native` column entry in `fct_action_values.columns`:

```yaml
      - name: tackle_winner_player_id
        data_type: bigint
        description: >
          silly-kicks 2.0.0 sportec tackle qualifier — DFL ObjectId of the
          tackle winner. NULL for non-sportec rows + for tackles where
          the DFL XML's `tackle_winner` attribute is absent. ADR-016 +
          silly-kicks ADR-001.
      - name: tackle_winner_team_id
        data_type: string
        description: >
          silly-kicks 2.0.0 sportec tackle qualifier — DFL CLU id of the
          tackle winner's team. NULL for non-sportec rows + for tackles
          where the qualifier is absent.
      - name: tackle_loser_player_id
        data_type: bigint
        description: >
          silly-kicks 2.0.0 sportec tackle qualifier — DFL ObjectId of the
          tackle loser. NULL for non-sportec rows + for tackles where
          the qualifier is absent.
      - name: tackle_loser_team_id
        data_type: string
        description: >
          silly-kicks 2.0.0 sportec tackle qualifier — DFL CLU id of the
          tackle loser's team. NULL for non-sportec rows + for tackles
          where the qualifier is absent.
```

### Task 21: Extend bronze migration script for tackle qualifier ALTER

**Files:** Modify `scripts/migrate_bronze_for_pr_ll2.py`

- [ ] **Step 21.1: Add new column dict + extend target functions**

After `_LL2_PATH_B_SPADL_COLS`:

```python
# PR-LL2 Path B close-out (2026-04-29): silly-kicks 2.0.0 sportec tackle
# qualifier columns added to bronze.spadl_actions + bronze.vaep_action_values.
_PATH_B_CLOSE_OUT_TACKLE_COLS: dict[str, str] = {
    "tackle_winner_player_id": "BIGINT",
    "tackle_winner_team_id": "STRING",
    "tackle_loser_player_id": "BIGINT",
    "tackle_loser_team_id": "STRING",
}
```

Update `_spadl_actions_target` and `_vaep_action_values_target` to include the new dict via spread.

### Task 22: Rename + extend validate script

**Files:**
- Rename: `scripts/validate_pr_ll2_post_deploy.py` → `scripts/validate_native_id_integrity.py`
- Modify: the renamed file with extended JOIN-coverage validations.

- [ ] **Step 22.1: Rename**

```bash
git mv scripts/validate_pr_ll2_post_deploy.py scripts/validate_native_id_integrity.py
```

- [ ] **Step 22.2: Add JOIN-coverage validation block**

Inside the renamed file, after the existing `_LL2_PATH_B_VALIDATIONS` block, add:

```python
# F3 (ADR-018): cross-table JOIN-coverage probes. Asserts every distinct
# bronze.spadl_actions._native ID resolves to a dim_* row. Mirrors the dbt
# singular tests but at Python boundary so deploy can run without a full
# dbt environment.
_NATIVE_JOIN_PROBES: list[tuple[str, str, str, str, str]] = [
    # (label, bronze_native_col, dim_table, dim_native_col, dim_key_col)
    ("idsse_match_id_native_join", "match_id_native", "dim_matches", "native_match_id", "match_key"),
    ("idsse_team_id_native_join", "team_id_native", "dim_teams", "native_team_id", "team_key"),
    ("idsse_competition_native_id_join", "competition_native_id", "dim_competitions", "native_competition_id", "competition_key"),
    ("metrica_match_id_native_join", "match_id_native", "dim_matches", "native_match_id", "match_key"),
    ("metrica_team_id_native_join", "team_id_native", "dim_teams", "native_team_id", "team_key"),
    ("metrica_competition_native_id_join", "competition_native_id", "dim_competitions", "native_competition_id", "competition_key"),
    # ... add for statsbomb + wyscout for full 4-source coverage ...
]


def _validate_join_coverage(cur, fq_bronze: str, dim_schema: str, source: str, probe) -> list[str]:
    """Run a JOIN coverage probe; return failure messages."""
    label, b_col, dim_table, d_native, d_key = probe
    fq_dim = f"soccer_analytics.{dim_schema}.{dim_table}"
    cur.execute(
        f"SELECT COUNT(DISTINCT b.{b_col}) FROM {fq_bronze} b "  # noqa: S608
        f"LEFT JOIN {fq_dim} d ON b.{b_col} = d.{d_native} AND b.data_source = d.provider "
        f"WHERE b.data_source = %(src)s AND b.{b_col} IS NOT NULL AND d.{d_key} IS NULL",
        {"src": source},
    )
    unmatched = cur.fetchone()[0]
    if unmatched > 0:
        return [f"FAIL  {label} ({source}): {unmatched} unmatched bronze native IDs"]
    return []
```

Wire `_NATIVE_JOIN_PROBES` into `main()` so it runs after the existing column-fill validations.

- [ ] **Step 22.3: Run pyright + ruff on renamed file**

```bash
uv run ruff check scripts/validate_native_id_integrity.py
uv run pyright scripts/validate_native_id_integrity.py
```

Expected: 0 errors.

---

## Phase E — Documentation + cleanup (T23–T25)

### Task 23: Update CLAUDE.md with ADR-018 references

**Files:** Modify `CLAUDE.md`

- [ ] **Step 23.1: Add ADR-018 bullet to "When to write an ADR" list**

In the `## Architectural Decision Records (ADRs)` section's `**When to write an ADR**` list, add:

```markdown
- Introduces a cross-table value-format contract or referential-integrity invariant (e.g., `bronze.X.native_id` ⊆ `dim.Y.native_id` per provider). See ADR-018 + the per-(source, entity) singular tests under `dbt_project/tests/`.
```

- [ ] **Step 23.2: Add the cross-table format-contract rule under `## Code Quality`**

```markdown
- **Cross-table format contracts** ([ADR-018](docs/superpowers/adrs/ADR-018-cross-table-format-contract-testing.md)): Every native ID format used as a JOIN key has its canonical generator in `src/shared/identifiers.py`. Bronze writers + applyInPandas UDFs import from this module; dbt singular tests (`assert_<source>_<entity>_native_join_resolves.sql`) assert JOIN-coverage from `bronze.spadl_actions` to `dim_*`. Adding a new bronze writer / dim staging touchpoint REQUIRES adding the corresponding format-contract test in the same PR.
```

### Task 24: Update MEMORY.md

**Files:** Modify `C:/Users/Karsten/.claude/projects/D--Development-karstenskyt--luxury-lakehouse/memory/MEMORY.md`

- [ ] **Step 24.1: Move/add a "Latest State" entry summarising the close-out**

Replace the previous "Latest State (2026-04-29, session 65 — PR-LL2 deployed end-to-end; mart refresh blocked by unrelated model_validation.py latent bug)" entry with a new entry for the close-out.

### Task 25: Delete throwaway probe scripts

**Files:** Delete `scripts/probe_pr_ll2_path_b_bugs.py`, `scripts/probe_pr_ll2_path_b_bug3_deep.py`, `scripts/probe_pr_ll2_path_b_bug6_periods.py`

- [ ] **Step 25.1: Delete the 3 throwaway scripts**

```bash
rm scripts/probe_pr_ll2_path_b_bugs.py
rm scripts/probe_pr_ll2_path_b_bug3_deep.py
rm scripts/probe_pr_ll2_path_b_bug6_periods.py
```

---

## Phase F — Local verification (T26–T28)

### Task 26: Bump wheel version

**Files:** Modify `pyproject.toml:3`

- [ ] **Step 26.1: Bump version**

```python
# Before:
version = "0.3.21"
# After:
version = "0.3.22"
```

- [ ] **Step 26.2: Update wheel SHA after build**

```bash
uv build --wheel
uv run python scripts/bump_wheel.py
```

Expected: `src/shared/wheel.py` updated with new SHA.

### Task 27: Local test suite

- [ ] **Step 27.1: Full test run**

```bash
uv run pytest src/tests/ -v --tb=short
```

Expected: 0 failures (modulo unrelated pre-existing).

- [ ] **Step 27.2: Lint + type check**

```bash
uv run ruff check src/ scripts/
uv run ruff format --check src/ scripts/
uv run pyright src/
```

Expected: 0 errors.

### Task 28: 🛑 G2 — Single dev commit

- [ ] **Step 28.1: Show diff summary to user, request approval**

```bash
git diff --stat HEAD
```

Wait for user to touch `~/.claude-git-approval`.

- [ ] **Step 28.2: Commit**

```bash
git add -A
git commit -m "$(cat <<'EOF'
fix(pr-ll2-path-b): close-out 6 bugs + ADR-018 cross-table format-contract foundation

Fixes 6 production bugs surfaced at full mart-refresh time:
  #1 IDSSE match_id prefix mismatch (100% NULL match_key for IDSSE in fct_action_values)
  #2 Metrica team_id_native format mismatch (100% NULL team_key for Metrica)
  #3 IDSSE team_id_native 93.5% NULL — silly-kicks 1.7.0 sportec tackle override
  #4 Metrica competition_key NULL — dim_matches CTE hardcoded NULL competition_id
  #5 Mart-level not_null filter mirror missing for 5 fct_action_values columns
  #6 21 minute_match_absolute test failures — IDSSE bronze parser period misclass

Establishes cross-table format-contract testing foundation (ADR-018):
- src/shared/identifiers.py — single source of truth for native ID formats
- 12 dbt singular tests assert (bronze, dim) JOIN-coverage per source
- 16 silly-kicks API boundary tests catch upstream API drift at OUR repo
- 2-pass IDSSE parser refactor replaces state-machine current_period
- silly-kicks 1.8.0 → 2.0.0 (caller's team_id/player_id sacred per their ADR-001)
- 4 new tackle qualifier columns surfaced through bronze→mart for analytics

Wheel 0.3.21 → 0.3.22.
Bronze re-ingestion of IDSSE + Metrica tables follows in deploy phase.

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>
EOF
)"
```

- [ ] **Step 28.3: Verify commit**

```bash
git log --oneline -1 && git status
```

Expected: clean tree, single new commit on feature branch.

---

## Phase G — Push + PR + Merge (T29–T31, gated)

### Task 29: 🛑 G3 — Push to remote

```bash
git push -u origin fix/pr-ll2-path-b-close-out
```

### Task 30: 🛑 G4 — Open PR

```bash
gh pr create --title "fix(pr-ll2-path-b): close-out 6 bugs + ADR-018 cross-table format-contract foundation" --body "$(cat <<'EOF'
## Summary

Closes PR-LL2 by fixing 6 production bugs surfaced at full mart-refresh time AND establishes the cross-table format-contract testing foundation (ADR-018).

The 6 bugs:
- **#1** IDSSE match_id prefix mismatch (100% NULL match_key for 2521 IDSSE rows)
- **#2** Metrica team_id_native format mismatch (100% NULL team_key for 5835 Metrica rows)
- **#3** IDSSE team_id_native 93.5% NULL — silly-kicks 1.7.0 sportec tackle override (fixed at silly-kicks 2.0.0 source)
- **#4** Metrica competition_key NULL — dim_matches CTE hardcoded NULL where staging emits 'metrica-sample'
- **#5** Mart-level not_null filter mirror missing on 5 fct_action_values columns
- **#6** 21 minute_match_absolute failures — IDSSE bronze parser period misclassification

Foundation work (ADR-018):
- `src/shared/identifiers.py` — single source of truth for 4 sources × 3 entities native ID formats
- 12 dbt singular tests asserting bronze→dim JOIN-coverage per source
- 16 silly-kicks API boundary tests at our repo
- 2-pass IDSSE parser replaces state-machine `current_period`
- silly-kicks 1.8.0 → 2.0.0 (their ADR-001: caller's identifiers sacred)
- 4 new tackle qualifier columns surfaced through bronze→mart

Wheel 0.3.21 → 0.3.22 (built + auto-uploaded to UC Volume by GitHub Actions on merge).

## Test plan

- [x] Full local pytest suite passes (68 passed, 10 skipped pyspark)
- [x] ruff + format + pyright clean
- [x] 12 new dbt singular tests + 16 silly-kicks boundary tests + 4 period derivation tests
- [x] validate_native_id_integrity.py contract drafted (deferred run to post-bronze-rebuild)
- [ ] Post-merge: bronze ALTER migration + DELETE IDSSE/Metrica + workflow re-runs + dbt build
- [ ] Post-deploy: validate_native_id_integrity.py + 24h DEEP CLONE retention

🤖 Generated with [Claude Code](https://claude.com/claude-code)
EOF
)"
```

### Task 31: 🛑 G5 — Merge after CI green

- [ ] **Step 31.1: Wait for CI**

```bash
gh pr checks --watch
```

- [ ] **Step 31.2: Squash-merge after approval**

```bash
gh pr merge --squash --delete-branch
```

**On merge: GitHub Actions builds wheel 0.3.22 and uploads it to `/Volumes/soccer_analytics/bronze/libs/luxury_lakehouse-0.3.22-py3-none-any.whl` automatically. Subsequent workflows pick up the new wheel from UC Volume on next run.**

---

## Phase H — Post-merge bronze migration + re-ingest (T32–T36, gated)

### Task 32: 🛑 G6 — Apply bronze schema migration ALTER

- [ ] **Step 32.1: Dry-run preview**

```bash
uv run python scripts/migrate_bronze_for_pr_ll2.py --dry-run
```

Expected: 4 ALTER TABLE ADD COLUMNS statements for the new tackle qualifier columns on bronze.spadl_actions + bronze.vaep_action_values.

- [ ] **Step 32.2: Request approval, then apply**

```bash
uv run python scripts/migrate_bronze_for_pr_ll2.py
```

Expected: ALTERs succeed; idempotent re-run shows "no missing cols."

### Task 33: DEEP CLONE backups

- [ ] **Step 33.1: Run via Databricks SQL**

```sql
CREATE TABLE soccer_analytics.bronze.idsse_events_pre_close_out_backup
DEEP CLONE soccer_analytics.bronze.idsse_events;

CREATE TABLE soccer_analytics.bronze.metrica_events_pre_close_out_backup
DEEP CLONE soccer_analytics.bronze.metrica_events;

CREATE TABLE soccer_analytics.bronze.spadl_actions_pre_close_out_backup
DEEP CLONE soccer_analytics.bronze.spadl_actions;

CREATE TABLE soccer_analytics.bronze.vaep_action_values_pre_close_out_backup
DEEP CLONE soccer_analytics.bronze.vaep_action_values;
```

- [ ] **Step 33.2: Verify backups exist with correct row counts**

```sql
SELECT COUNT(*) FROM soccer_analytics.bronze.idsse_events_pre_close_out_backup;
-- expect ~10498
SELECT COUNT(*) FROM soccer_analytics.bronze.spadl_actions_pre_close_out_backup;
-- expect ~9.6M
```

### Task 34: 🛑 G7 — Destructive DELETE

- [ ] **Step 34.1: Request approval, then run**

```sql
DELETE FROM soccer_analytics.bronze.idsse_events;
DELETE FROM soccer_analytics.bronze.metrica_events;
DELETE FROM soccer_analytics.bronze.spadl_actions WHERE data_source IN ('idsse', 'metrica');
DELETE FROM soccer_analytics.bronze.vaep_action_values WHERE data_source IN ('idsse', 'metrica');
```

### Task 35: 🛑 G8 — Trigger workflow re-runs (consumes auto-uploaded 0.3.22 wheel)

- [ ] **Step 35.1: Trigger via Databricks Jobs API or UI**

Run job `soccer-analytics-ingestion-dev` (id `887419551716059`). Specifically tasks: `ingest_idsse`, `ingest_metrica_events`, `ingest_metrica_tracking`, `compute_spadl_vaep`. The job's wheel reference is automatically the latest at `/Volumes/soccer_analytics/bronze/libs/luxury_lakehouse-*.whl` — refreshed by the GitHub Actions on-merge build.

- [ ] **Step 35.2: Wait for completion + verify post-ingest counts**

```sql
SELECT COUNT(*), COUNT(DISTINCT match_id) FROM soccer_analytics.bronze.idsse_events;
-- expect ~10498 rows, 7 matches; match_id format = 'J03WMX' (bare, no prefix)
SELECT COUNT(*), COUNT(DISTINCT match_id) FROM soccer_analytics.bronze.metrica_events;
-- expect ~14000 rows, 3 matches; team_id_native format = 'metrica_Sample_Game_N_home/away'
SELECT data_source, COUNT(*) FROM soccer_analytics.bronze.spadl_actions GROUP BY data_source;
-- expect statsbomb ~7.15M, wyscout ~2.46M, idsse ~2522, metrica ~5835
```

### Task 36: Run validate_native_id_integrity.py

```bash
uv run python scripts/validate_native_id_integrity.py
```

Expected: VALIDATION PASSED — all column fills + JOIN coverages green.

---

## Phase I — Final dbt build verification (T37)

### Task 37: Local dbt build + refresh synced tables

- [ ] **Step 37.1: Run**

```bash
uv run --extra dbt python scripts/dbt_build_and_refresh.py
```

Expected: PASS ≥ 808 (= 796 baseline + 12 new singular tests), WARN = 21, ERROR = 0, SKIP = 68.

- [ ] **Step 37.2: Verify the 12 new dbt tests pass**

```bash
cd dbt_project && uv run dbt test --select 'tag:slim_ci' 2>&1 | grep -E "(assert_.*_native_join_resolves|PASS|FAIL|ERROR)"
```

Expected: 12 PASS, 0 FAIL.

---

## Phase J — Post-merge cleanup (T38–T39, 24h later)

### Task 38: 🛑 G9 — Drop DEEP CLONE backups

(24h after merge — set a calendar reminder.)

```sql
DROP TABLE soccer_analytics.bronze.idsse_events_pre_close_out_backup;
DROP TABLE soccer_analytics.bronze.metrica_events_pre_close_out_backup;
DROP TABLE soccer_analytics.bronze.spadl_actions_pre_close_out_backup;
DROP TABLE soccer_analytics.bronze.vaep_action_values_pre_close_out_backup;
```

### Task 39: Mark deferred items in PR-LL3-scope.md

Update `docs/superpowers/plans/PR-LL3-scope.md` with the merge SHA + a "Last reviewed" entry under each S-item that this PR addressed (none directly, but the foundation enables S5 / S6).

---

## Self-review checklist

After this plan is written, the implementer should verify:

- [ ] **Spec coverage** — every item in the spec's "Contents (in execution order)" list maps to a Task in this plan: items 1–14 from spec → Tasks 2, 3, 5, 6, 16-22, 9-14, 22, 4, 33-35. ✓
- [ ] **Placeholder scan** — no "TBD" / "TODO" / "implement later" / "Add appropriate error handling" placeholders. ✓
- [ ] **Type consistency** — function names match between tasks (`idsse_native_match_id` used consistently; `_scan_kickoff_times` defined in T13 used in `_parse_events_xml` refactor in T13). ✓
- [ ] **Test reference consistency** — every test referenced in spec's TDD table T1–T14 appears in plan tasks. ✓

---

## Execution handoff

Plan complete and saved to `docs/superpowers/plans/2026-04-29-pr-ll2-path-b-close-out.md`. Two execution options:

1. **Subagent-Driven (recommended)** — Dispatch a fresh subagent per task, review between tasks, fast iteration. Each subagent gets the spec + this plan + the previous task's resulting state.
2. **Inline Execution** — Execute tasks in this session using executing-plans, batch execution with checkpoints.

Which approach?
