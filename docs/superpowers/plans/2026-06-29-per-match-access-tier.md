# Per-Match HF Redistribution Restriction (`access_tier`) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Move the HF redistribution boundary from provider-level (`RESTRICTED_HF_PROVIDERS`) to **per-match**, driven by the pining `visibility` ingestion property, so restricted SkillCorner matches never reach a public HF repo — raw or derived.

**Architecture:** A pure stdlib domain core (`src/shared/access_tier.py`) classifies `(provider, visibility) → AccessTier`. Ingestion stamps `access_tier` (+ raw `visibility`) on bronze; it rides per-row through the SPADL/AC/tracking passthrough (ADR-016) to the marts; `split_restricted` splits on `access_tier` (fail-safe: anything not exactly `public` → restricted); every public publisher is registry-gated by an enumerate-all, fail-closed leak guard. Derived aggregates (football2vec) are rebuilt public-only upstream.

**Tech Stack:** Python 3.10, pandas, PySpark/Delta, dbt, pytest, HuggingFace Hub. Spec: `docs/superpowers/specs/2026-06-29-per-match-hf-redistribution-restriction.md` (Rev 4). Precedent for the mechanical schema passthrough: the silly-kicks 4.36.0 `xt_gk_*` coords add (PR #413) — `access_tier` follows the identical RESULT_COLUMNS/DDL/StructType/staging/mart/contract/migration pattern.

**⚠ LEAK-CRITICAL ORDERING (spec §8):** the pining owner token is already live, so restricted SkillCorner can ingest at any manual trigger. **Phases 1–9 must be deployed AND verified (Task 22) BEFORE the restricted matches are allowed to flow (Task 23).** Until then, do not ingest the private SkillCorner matches and do not run the SkillCorner-carrying publishes. Execute phase-by-phase with a review gate between phases.

---

## Plan Revision 2.1 — pining-for-the-data reviews (2026-06-29)

**Round-3 residuals (all resolved):** **R1** Task 7 ↔ 8b NULL contract — Task 7 carves out NULL→value (populate allowed; only non-NULL→different-non-NULL raises) + Task 8b re-ingests match-info so no NULL exists (decisive side). **R2** publishers drop `access_tier` after split+guard, before upload (Task 13 Step 3b) — avoids the Hyrum schema change to public datasets. **R3** `MatchInfo.visibility` required-no-default invariant test (Task 5 Step 5) — keeps `skillcorner+None→PUBLIC` unreachable in ingestion. **R4** Task 23 pre-states the two expected deltas (public football2vec excludes GS; public schema unchanged because R2 drops the column). Round-3 verdict: ship-ready.

**Round-2 resolutions:**

Three blockers + 5 mediums resolved (a data-owner decision + two code-verified facts):
- **B1 — backfill (regression):** `ADD COLUMNS` leaves every historical row `access_tier = NULL` → the fail-safe routes NULL→restricted, hard-blocking every public publish and emptying the public datasets of StatsBomb/Wyscout/IDSSE/Metrica. **Fix: new Task 8b** one-time backfill of existing bronze rows to their provider-default tier (derived from `classify_access_tier`, not hand-encoded) + a **zero-NULL-tier-in-any-mart** test gating rollout.
- **B2 — publish path (verified):** the pyproject `[project.scripts]` entry points are the **`src/ingestion/` twins** (`ingestion.publish_spadl_vaep_hf:main` :183–185), while the ADR-049 *split* versions are in `scripts/` (run via `hf jobs`). Three twins: `spadl_vaep`, `xg_shots`, `freeze_frame`. **Fix: Task 4 registry globs BOTH dirs; Task 15 covers all three twins + determines/consolidates the canonical production path** (the migration + guard must target whichever runs).
- **B3 — RESOLVED (data owner): split-to-both is licence-clean for SkillCorner.** Licence rationale (operator): *the data may be stored locally + backed up to cloud; it may NOT be shared.* A private HF `-restricted` repo (org-members-only) is permitted **storage/backup**; a **public** repo is **sharing** and is prohibited. So restricted SkillCorner → private `-restricted` repo (same as GS), and **the public-repo leak guard IS the "do-not-share" enforcement** — the boundary the licence draws maps exactly onto public-vs-private repo. The uniform split mechanism stands; no drop-private path.
- **M1:** Task 10 stamps `access_tier` **directly** (converter knows provider+match), never a join (avoids NULL-on-unmatched → silent public-data drop). **M2:** aggregates (football2vec career/season) get a **daily dbt DATA test** (public aggregate == recompute-from-public-rows), not just a grep; gate rollout on `dbt build`, not `dbt parse`. **M3:** grep all `parse_match_json` callers before the required-kwarg break. **M4:** migration-completeness checklist tied to B1's zero-NULL test. **M5:** Task 22 drills **one publisher per mode** (split / fail_closed / derived).

**Recorded assumption (operator):** **pining is the SOLE lakehouse ingestion source for SkillCorner** — Dropbox is a parallel delivery channel to the team, never used for ingestion. So every SkillCorner row that enters the lakehouse carries a pining `visibility` signal, and the classifier (keyed on `visibility`) is complete with no unclassified-source gap. *If a non-pining SkillCorner ingestion path (e.g. Dropbox import) is ever added, this assumption breaks — such a source carries no `visibility`, and the `skillcorner + None → PUBLIC` default would mis-classify restricted matches as public. Re-open this decision before adding any such path (default unknown-source SkillCorner to RESTRICTED).*

---

## File Structure

**New files:**
- `src/shared/access_tier.py` — `AccessTier` enum, `RESTRICTED_HF_PROVIDERS`, `classify_access_tier`. Stdlib only.
- `src/tests/test_access_tier.py` — pure classifier truth table.
- `src/ingestion/hf_leak_guard.py` — `PUBLISHER_REGISTRY` + `assert_no_private_leak(df, publisher)` (enumerate-all, fail-closed).
- `src/tests/test_hf_leak_guard.py` — registry completeness + fail-closed behavior.
- `src/tests/test_pining_visibility_contract.py` — env-gated live + recorded-fixture vocabulary contract.
- `scripts/migrations/2026-06-30-add-access-tier-columns.sql` — bronze `ADD COLUMNS`.

**Modified (by phase):**
- Core/split: `src/ingestion/hf_publish.py` (relocate constant, swap split mask).
- Ingestion: `src/ingestion/skillcorner_matches.py`, `skillcorner.py`, `gradientsports_metadata.py`, `gradientsports.py`; the SPADL/AC/tracking bronze writers.
- Schema: `src/analytics/action_context/schema.py`, `src/ingestion/spadl_enrichments.py`, the applyInPandas StructTypes in `spadl_conversion.py`/`spadl_udf_shared.py`/`spadl_vaep.py`, the tracking-frames writer.
- dbt: `dbt_project/models/marts/dim_matches.sql`, `fct_action_values.sql`, `fct_action_context.sql`, `fct_shot_psxg.sql`, `fct_tracking_frames.sql`, `fct_player_embeddings_{career,season}.sql`, `_marts__models.yml`, the staging passthroughs.
- Publishers: `scripts/publish_{spadl_vaep,action_context,psxg_shots,pitch_control_tracking,line_breaking_passes,xg_shots,football2vec_embeddings}_hf.py`; `src/ingestion/publish_spadl_vaep_hf.py` (C5).
- Trainer: `scripts/train_vaep_model_hf.py`, `scripts/train_football2vec*.py`.
- Tests: `test_hf_publish.py`, `test_hf_publish_parity.py`, `test_gradientsports_hf_exclusion.py`, `test_spadl_vaep_writer_parity.py`.

---

## Phase 1 — Domain core (no behavior change)

### Task 1: `AccessTier` + `classify_access_tier` (the pure policy)

**Files:**
- Create: `src/shared/access_tier.py`
- Test: `src/tests/test_access_tier.py`

- [ ] **Step 1: Write the failing test** (`src/tests/test_access_tier.py`)

```python
"""Pure truth table for the per-match access-tier policy (spec §4 / D2)."""

from __future__ import annotations

import pytest

from shared.access_tier import AccessTier, RESTRICTED_HF_PROVIDERS, classify_access_tier


@pytest.mark.parametrize(
    ("provider", "visibility", "expected"),
    [
        # Literal pining values (pining models.py:60 — ^(public|private)$).
        ("skillcorner", "private", AccessTier.RESTRICTED),
        ("skillcorner", "public", AccessTier.PUBLIC),
        ("gradientsports", "private", AccessTier.RESTRICTED),
        ("gradientsports", "public", AccessTier.PUBLIC),
        # No feed → provider default.
        ("gradientsports", None, AccessTier.RESTRICTED),  # in RESTRICTED_HF_PROVIDERS
        ("statsbomb", None, AccessTier.PUBLIC),
        ("wyscout", None, AccessTier.PUBLIC),
        ("idsse", None, AccessTier.PUBLIC),
        ("metrica", None, AccessTier.PUBLIC),
        ("skillcorner", None, AccessTier.PUBLIC),  # NOT in the default set
        # Fail-safe: any unknown value → RESTRICTED (D1).
        ("skillcorner", "embargoed", AccessTier.RESTRICTED),
        ("skillcorner", "", AccessTier.RESTRICTED),
    ],
)
def test_classify_access_tier(provider: str, visibility: str | None, expected: AccessTier) -> None:
    assert classify_access_tier(provider=provider, visibility=visibility) is expected


def test_enum_values_are_the_canonical_strings() -> None:
    assert AccessTier.PUBLIC.value == "public"
    assert AccessTier.RESTRICTED.value == "restricted"


def test_restricted_default_providers_is_frozenset_lowercase() -> None:
    assert isinstance(RESTRICTED_HF_PROVIDERS, frozenset)
    assert all(p == p.lower() for p in RESTRICTED_HF_PROVIDERS)
```

- [ ] **Step 2: Run it — expect failure**

Run: `uv run pytest src/tests/test_access_tier.py -v -o addopts="" -p no:xdist`
Expected: FAIL — `ModuleNotFoundError: No module named 'shared.access_tier'`.

- [ ] **Step 3: Implement the core** (`src/shared/access_tier.py`)

```python
"""Per-match HF redistribution policy — the SINGLE source of truth (spec §4).

Pure: stdlib only (sits beside src/shared/identifiers.py; src/shared/ has zero external deps).
No Spark, no HF, no I/O. Inputs are ingestion-time signals; output is the redistribution tier.
"""

from __future__ import annotations

from enum import Enum


class AccessTier(str, Enum):
    PUBLIC = "public"
    RESTRICTED = "restricted"


# Providers whose matches default to RESTRICTED when they carry NO per-match visibility signal.
# (GradientSports today; SkillCorner has a real `visibility` feed so it is NOT defaulted.)
# Relocated here from ingestion.hf_publish (spec D5): hf_publish.py imports it FROM this pure core,
# so the stdlib-only core never imports the pandas/HF adapter (no zero-dep violation, no cycle).
RESTRICTED_HF_PROVIDERS: frozenset[str] = frozenset({"gradientsports"})


def classify_access_tier(*, provider: str, visibility: str | None) -> AccessTier:
    """Classify a match's redistribution tier from its ingestion-time signals.

    pining `visibility` is `"public" | "private"` (pining canonical models.py:60). Mapping:
        "private"           -> RESTRICTED   (the positive trigger — match the LITERAL value)
        "public"            -> PUBLIC
        None (no feed)      -> provider default (RESTRICTED if in RESTRICTED_HF_PROVIDERS else PUBLIC)
        anything else       -> RESTRICTED   (fail-safe — never leak an unrecognized value, spec D1)
    """
    if visibility is None:
        return AccessTier.RESTRICTED if provider in RESTRICTED_HF_PROVIDERS else AccessTier.PUBLIC
    if visibility == "public":
        return AccessTier.PUBLIC
    # "private" AND any unrecognized value both route to RESTRICTED (fail-safe).
    return AccessTier.RESTRICTED
```

- [ ] **Step 4: Run it — expect pass**

Run: `uv run pytest src/tests/test_access_tier.py -v -o addopts="" -p no:xdist`
Expected: PASS (15 cases).

- [ ] **Step 5: Verify import-linter layering** (src/shared must stay dependency-free)

Run: `uv run lint-imports` (or `uv run ruff check src/shared/access_tier.py`)
Expected: PASS — no new third-party import in `src/shared/`.

- [ ] **Step 6: Commit**

```bash
git add src/shared/access_tier.py src/tests/test_access_tier.py
git commit -m "feat(access-tier): pure per-match redistribution policy core (spec phase 1)"
```

### Task 2: Relocate `RESTRICTED_HF_PROVIDERS` into the core (D5)

**Files:**
- Modify: `src/ingestion/hf_publish.py:86` (the constant) — re-export from the core.
- Test: `src/tests/test_hf_publish.py` (existing import-site test still passes).

- [ ] **Step 1: Edit `hf_publish.py`** — replace the literal definition with a re-export shim:

```python
# src/ingestion/hf_publish.py — replace the `RESTRICTED_HF_PROVIDERS = frozenset(...)` definition with:
from shared.access_tier import RESTRICTED_HF_PROVIDERS  # noqa: F401 — re-export shim (spec D5)

# Deliberate shim: the set's single source of truth is shared.access_tier (stdlib-only core). Existing
# publishers/trainers that `from ingestion.hf_publish import RESTRICTED_HF_PROVIDERS` keep working.
```

- [ ] **Step 2: Run the existing parity + restriction tests — expect pass**

Run: `uv run pytest src/tests/test_hf_publish.py src/tests/test_hf_publish_parity.py -o addopts="" -p no:xdist -q`
Expected: PASS (the re-export keeps every import site working).

- [ ] **Step 3: Commit**

```bash
git add src/ingestion/hf_publish.py
git commit -m "refactor(access-tier): relocate RESTRICTED_HF_PROVIDERS to the pure core (spec D5)"
```

---

## Phase 2 — Split on `access_tier` + the leak guard

### Task 3: `split_restricted` splits on `access_tier` (fail-safe)

**Files:**
- Modify: `src/ingestion/hf_publish.py` (`split_restricted`, ~line 94).
- Test: `src/tests/test_hf_publish.py::TestRestrictedPublishing`.

- [ ] **Step 1: Write the failing tests** (append to `TestRestrictedPublishing` in `src/tests/test_hf_publish.py`)

```python
def test_split_restricted_access_tier_mode_fail_safe() -> None:
    import pandas as pd
    from ingestion.hf_publish import split_restricted

    df = pd.DataFrame(
        {
            "data_source": ["skillcorner", "skillcorner", "gradientsports", "statsbomb"],
            "access_tier": ["public", "restricted", "restricted", None],  # None must fail-safe to restricted
            "v": [1, 2, 3, 4],
        }
    )
    public_df, restricted_df = split_restricted(df, column="access_tier")
    # Only the explicit "public" row is public; restricted + NULL/unknown are held back (fail-safe, D1).
    assert sorted(public_df["v"].tolist()) == [1]
    assert sorted(restricted_df["v"].tolist()) == [2, 3, 4]
    # Disjoint + complete.
    assert len(public_df) + len(restricted_df) == len(df)


def test_split_restricted_same_provider_in_both_partitions() -> None:
    import pandas as pd
    from ingestion.hf_publish import split_restricted

    df = pd.DataFrame({"data_source": ["skillcorner", "skillcorner"], "access_tier": ["public", "restricted"], "v": [1, 2]})
    public_df, restricted_df = split_restricted(df, column="access_tier")
    # The new capability: one provider appears in BOTH repos.
    assert public_df["data_source"].tolist() == ["skillcorner"]
    assert restricted_df["data_source"].tolist() == ["skillcorner"]
```

- [ ] **Step 2: Run — expect failure**

Run: `uv run pytest "src/tests/test_hf_publish.py::TestRestrictedPublishing" -v -o addopts="" -p no:xdist`
Expected: FAIL — current mask uses `isin(RESTRICTED_HF_PROVIDERS)` on `access_tier`, so no row matches → restricted_df empty.

- [ ] **Step 3: Implement** — replace the body of `split_restricted` in `src/ingestion/hf_publish.py`:

```python
def split_restricted(df: pd.DataFrame, column: str = "access_tier") -> tuple[pd.DataFrame, pd.DataFrame]:
    """Split a publish DataFrame into ``(public_df, restricted_df)`` (ADR-049 / spec §6).

    Default column is now ``access_tier`` (per-match). A row is PUBLIC only if its tier is exactly
    ``"public"``; restricted AND NULL/unknown route to the restricted partition (fail-safe, spec D1 —
    never leak an unclassified row). The legacy provider mode is retained for any un-migrated caller.
    """
    from shared.access_tier import AccessTier, RESTRICTED_HF_PROVIDERS

    if column == "access_tier":
        is_public = df[column] == AccessTier.PUBLIC.value  # NaN/None/unknown -> not public -> restricted
        return df[is_public], df[~is_public]
    mask = df[column].isin(RESTRICTED_HF_PROVIDERS)  # legacy provider-level mode
    return df[~mask], df[mask]
```

- [ ] **Step 4: Run — expect pass** (and re-run the existing `data_source`-mode tests)

Run: `uv run pytest "src/tests/test_hf_publish.py::TestRestrictedPublishing" -v -o addopts="" -p no:xdist`
Expected: PASS (new + existing legacy-mode tests).

- [ ] **Step 5: Commit**

```bash
git add src/ingestion/hf_publish.py src/tests/test_hf_publish.py
git commit -m "feat(access-tier): split_restricted splits on access_tier, fail-safe to restricted (spec D1)"
```

### Task 4: The enumerate-all, fail-closed leak guard

**Files:**
- Create: `src/ingestion/hf_leak_guard.py`
- Test: `src/tests/test_hf_leak_guard.py`

- [ ] **Step 1: Write the failing test** (`src/tests/test_hf_leak_guard.py`)

```python
"""Every-run leak guard: no public artifact may carry a non-public row; registry is exhaustive (spec §9.7)."""

from __future__ import annotations

import glob
import os

import pandas as pd
import pytest

from ingestion.hf_leak_guard import PUBLISHER_REGISTRY, LeakDetected, assert_no_private_leak


def test_public_frame_with_private_row_fails_closed() -> None:
    df = pd.DataFrame({"access_tier": ["public", "restricted"], "v": [1, 2]})
    with pytest.raises(LeakDetected):
        assert_no_private_leak(df, publisher="publish_action_context_hf")


def test_null_tier_fails_closed() -> None:
    df = pd.DataFrame({"access_tier": ["public", None], "v": [1, 2]})
    with pytest.raises(LeakDetected):
        assert_no_private_leak(df, publisher="publish_action_context_hf")


def test_all_public_passes() -> None:
    df = pd.DataFrame({"access_tier": ["public", "public"], "v": [1, 2]})
    assert_no_private_leak(df, publisher="publish_action_context_hf")  # no raise


def test_registry_covers_every_publisher_module() -> None:
    """A publisher in EITHER scripts/ or src/ingestion/ with no registry entry FAILS (B2 — the
    src/ingestion/ twins are the wired pyproject entry points; the guard must not be blind to them)."""
    paths = glob.glob("scripts/publish_*_hf.py") + glob.glob("src/ingestion/publish_*_hf.py")
    modules = {os.path.basename(p)[: -len(".py")] for p in paths}  # basename de-dupes scripts/ vs src/ twins
    missing = modules - set(PUBLISHER_REGISTRY)
    assert not missing, f"publishers missing from PUBLISHER_REGISTRY (leak guard would skip them): {sorted(missing)}"
```

- [ ] **Step 2: Run — expect failure** (`ModuleNotFoundError`).

Run: `uv run pytest src/tests/test_hf_leak_guard.py -v -o addopts="" -p no:xdist`

- [ ] **Step 3: Implement** (`src/ingestion/hf_leak_guard.py`)

```python
"""Fail-closed leak guard for public HF publishers (spec §9.7).

Every public-HF publisher calls assert_no_private_leak(public_df, publisher=<name>) immediately before
uploading its PUBLIC artifact. The registry enumerates every publisher + its tier-handling mode, so a
new publisher with no entry fails test_registry_covers_every_publisher_script (it cannot be silently
omitted). ERROR-level + raise on any non-public row (spec C3 / CLAUDE.md telemetry rule).
"""

from __future__ import annotations

import logging

import pandas as pd

from shared.access_tier import AccessTier

logger = logging.getLogger(__name__)


class LeakDetected(RuntimeError):
    """A public artifact contains a non-public (restricted/NULL/unknown) row."""


# mode: "split"        — row-level, publishes both repos; public_df must be all-public.
#       "fail_closed"   — safe-by-absence today (no restricted provider in its mart); still asserted.
#       "derived"       — built public-only upstream; the publisher asserts its source separately (§6.8).
PUBLISHER_REGISTRY: dict[str, str] = {
    "publish_spadl_vaep_hf": "split",
    "publish_action_context_hf": "split",
    "publish_psxg_shots_hf": "split",
    "publish_pitch_control_tracking_hf": "split",
    "publish_line_breaking_passes_hf": "fail_closed",
    "publish_xg_shots_hf": "fail_closed",
    "publish_freeze_frame_hf": "fail_closed",
    "publish_obso_pausa_inputs_hf": "fail_closed",
    "publish_shots_on_target_hf": "fail_closed",
    "publish_football2vec_embeddings_hf": "derived",
}


def assert_no_private_leak(public_df: pd.DataFrame, *, publisher: str) -> None:
    """Raise LeakDetected if ``public_df`` contains any row whose access_tier is not exactly 'public'."""
    if publisher not in PUBLISHER_REGISTRY:
        raise LeakDetected(f"publisher {publisher!r} not in PUBLISHER_REGISTRY — add it (fail-closed)")
    if "access_tier" not in public_df.columns:
        raise LeakDetected(f"{publisher}: public frame has no access_tier column — cannot prove it is public")
    non_public = public_df[public_df["access_tier"] != AccessTier.PUBLIC.value]
    if len(non_public) > 0:
        by_tier = non_public["access_tier"].fillna("<null>").value_counts().to_dict()
        logger.error("LEAK BLOCKED: %s public artifact has %d non-public rows: %s", publisher, len(non_public), by_tier)
        raise LeakDetected(f"{publisher}: {len(non_public)} non-public rows in public artifact: {by_tier}")
    logger.info("leak guard OK: %s public artifact is all-public (%d rows)", publisher, len(public_df))
```

- [ ] **Step 4: Run — expect pass.** Run: `uv run pytest src/tests/test_hf_leak_guard.py -v -o addopts="" -p no:xdist`

- [ ] **Step 5: Commit**

```bash
git add src/ingestion/hf_leak_guard.py src/tests/test_hf_leak_guard.py
git commit -m "feat(access-tier): enumerate-all fail-closed HF leak guard (spec C3/§9.7)"
```

---

## Phase 3 — Ingestion stamp (visibility → bronze)

> Pattern for all stamp tasks: thread the provider's `MatchInfo.visibility` into the bronze match-info
> writer, persist it **raw** AND stamp `access_tier = classify_access_tier(provider, visibility)` (spec
> C1). Then carry `access_tier` onto each per-action/per-frame bronze fact.

### Task 5: SkillCorner match-info stamps `visibility` + `access_tier`

**Files:**
- Modify: `src/ingestion/skillcorner_matches.py:47` (`parse_match_json`), `:134` (`write_matches`), and `skillcorner.py` (pass `MatchInfo.visibility` in).
- Test: `src/tests/test_silly_kicks_boundary.py` (or a new `test_skillcorner_access_tier.py`).

- [ ] **Step 1: Write the failing test** (`src/tests/test_skillcorner_access_tier.py`)

```python
"""SkillCorner match-info carries raw visibility + derived access_tier (spec §6.2 / C1)."""

from __future__ import annotations

from ingestion.skillcorner_matches import parse_match_json

_MATCH_JSON = '{"id": 1886347, "home_team": {"id": 1, "name": "A"}, "away_team": {"id": 2, "name": "B"}}'


def test_parse_stamps_public_visibility_and_tier() -> None:
    df = parse_match_json(_MATCH_JSON, match_id="1886347", visibility="public")
    assert (df["visibility"] == "public").all()
    assert (df["access_tier"] == "public").all()


def test_parse_stamps_private_to_restricted() -> None:
    df = parse_match_json(_MATCH_JSON, match_id="1886347", visibility="private")
    assert (df["visibility"] == "private").all()
    assert (df["access_tier"] == "restricted").all()
```

(Adjust `_MATCH_JSON` to the real minimal shape `parse_match_json` expects — copy from the existing
`src/tests/fixtures/skillcorner/match.json` used elsewhere in the suite.)

- [ ] **Step 2: Run — expect failure** (`parse_match_json() got an unexpected keyword argument 'visibility'`).

- [ ] **Step 2b (M3): grep ALL callers before the signature break** — `parse_match_json` becoming a
  required kwarg breaks every caller. Run `grep -rn "parse_match_json" src/ scripts/` and update each
  call site (ingestion, tests, any backfill) in this task, so nothing breaks on a missed site.

- [ ] **Step 3: Implement** — add a required `visibility` kwarg to `parse_match_json`, stamp both columns, and pass it through from `write_matches` / `skillcorner.py`:

```python
# skillcorner_matches.py
from shared.access_tier import classify_access_tier

def parse_match_json(source: str, *, match_id: str, visibility: str) -> pd.DataFrame:
    df = ...  # existing parse
    df["visibility"] = visibility
    df["access_tier"] = classify_access_tier(provider="skillcorner", visibility=visibility).value
    return df

def write_matches(..., visibility: str) -> int:  # thread through
    df = parse_match_json(source, match_id=match_id, visibility=visibility)
    ...  # write_delta_table unchanged (now carries the two new columns)
```

```python
# skillcorner.py — at the call site that has the MatchInfo:
parse_match_json(match_resp.text, match_id=mid, visibility=match_info.visibility)
```

- [ ] **Step 4: Run — expect pass.** `uv run pytest src/tests/test_skillcorner_access_tier.py -v -o addopts="" -p no:xdist`

- [ ] **Step 5 (R3 — enforce the no-silent-default invariant):** add a test that `MatchInfo.visibility`
  is a **required** pydantic field with **no default** (both `skillcorner_common.MatchInfo` and
  `gradientsports_common.MatchInfo`). This is the technical lynchpin that keeps `skillcorner + None →
  PUBLIC` unreachable in ingestion — a pining response missing `visibility` must hard-error, never
  silently become `None → public`. The test fails if anyone later adds a convenience default:

```python
# src/tests/test_visibility_required.py
import pytest
from ingestion.skillcorner_common import MatchInfo as ScMatchInfo
from ingestion.gradientsports_common import MatchInfo as GsMatchInfo

@pytest.mark.parametrize("model", [ScMatchInfo, GsMatchInfo])
def test_visibility_is_required_no_default(model) -> None:
    field = model.model_fields["visibility"]
    assert field.is_required(), f"{model.__module__}.MatchInfo.visibility must stay REQUIRED (no default) — "
    "a silent default re-opens the skillcorner+None->public leak (plan R3)"
```

- [ ] **Step 6: Migration** — add the columns to `bronze.skillcorner_matches` (see Task 8's migration file; include them there).

- [ ] **Step 7: Commit**

```bash
git add src/ingestion/skillcorner_matches.py src/ingestion/skillcorner.py src/tests/test_skillcorner_access_tier.py src/tests/test_visibility_required.py
git commit -m "feat(access-tier): SkillCorner match-info stamps visibility + access_tier; visibility-required invariant (spec §6.2 / R3)"
```

### Task 6: GradientSports match-info stamps `visibility` + `access_tier` (D7)

**Files:** Modify `src/ingestion/gradientsports_metadata.py`, `gradientsports.py`. Test: `src/tests/test_gradientsports_ingestion.py`.

- [ ] **Step 1–4:** Mirror Task 5 exactly for GradientSports — thread `MatchInfo.visibility` from `gradientsports.py` into the metadata writer; `df["visibility"] = visibility`; `df["access_tier"] = classify_access_tier(provider="gradientsports", visibility=visibility).value`. Test both `"public"→public` and `"private"→restricted`, plus the **`None` → restricted** provider-default case (GS is in `RESTRICTED_HF_PROVIDERS`).

- [ ] **Step 5: Commit**

```bash
git add src/ingestion/gradientsports_metadata.py src/ingestion/gradientsports.py src/tests/test_gradientsports_ingestion.py
git commit -m "feat(access-tier): GradientSports match-info stamps visibility + access_tier (spec D7)"
```

### Task 7: Re-ingest immutability assertion (A3)

**Files:** Modify the SkillCorner + GS match-info writers. Test: `test_skillcorner_access_tier.py`.

- [ ] **Step 1: Write the failing tests** — a real flip raises; a NULL→value populate does NOT (R1):

```python
def test_visibility_flip_raises(monkeypatch) -> None:
    # stored non-NULL "public", new "private" -> raise (pining forbids re-tiering; spec A3).
    ...

def test_null_to_value_is_allowed(monkeypatch) -> None:
    # stored NULL/absent (a backfilled or first-seen row), new "public" -> MUST NOT raise (R1).
    # This is exactly the Task 24 re-ingest path after the Task 8b backfill — it must not trip.
    ...
```

- [ ] **Step 2–4 (R1 — explicit NULL carve-out):** In `write_matches` (both providers), before the
  `replace_where` write, read the existing `visibility` for the `match_id` (via
  `ingestion.utils.tolerate_missing_table`). **Raise ONLY when a non-NULL stored value changes to a
  DIFFERENT non-NULL value:** `raise RuntimeError(f"visibility flip for {provider} match {match_id}: {old!r} -> {new!r}")`.
  Treat **stored NULL / absent row / absent table as "unset" → allowed to populate** (so the Task 8b
  backfill's NULL is reconciled by the Task 24 re-ingest without tripping the assert). Equal values are
  no-ops.

- [ ] **Step 5: Commit** `feat(access-tier): visibility immutability with NULL-populate carve-out (spec A3 / R1)`

> **R1 resolution (the Task 7 ↔ 8b contract):** Task 8b **re-ingests** SkillCorner/GS match-info to
> populate real `visibility` (its preferred option), so in the normal path there is no NULL to
> reconcile. The NULL carve-out above is the defense-in-depth that keeps a backfilled-NULL row (e.g. a
> fact table that can't be cheaply re-ingested) from blocking a later real-visibility populate. Both
> together remove the latent tension the reviewer flagged.

---

## Phase 4 — Schema passthrough (bronze facts → marts)

> Mechanical, but parity-tested (the LL1 silent-drop class). `access_tier` is a provider-native
> passthrough → canonical name, no `<provider>_` prefix. **This mirrors the just-merged `xt_gk_*`
> coords add (PR #413) step-for-step** — use that diff as the reference for each touchpoint.

### Task 8: `access_tier` through the AC schema + bronze migration

**Files:**
- Modify: `src/analytics/action_context/schema.py` (`RESULT_COLUMNS`, `ACTION_CONTEXT_DDL`, the count comment).
- Create: `scripts/migrations/2026-06-30-add-access-tier-columns.sql`.
- Test: `src/tests/action_context/test_schema.py`, `src/tests/test_action_context_schema_parity.py`.

- [ ] **Step 1: Write/extend the failing parity test** — assert `"access_tier"` is in `RESULT_COLUMNS` and the DDL field list, and that `len(RESULT_COLUMNS)` equals the DDL field count (the existing parity test already checks count equality; adding to one side fails until both updated).

- [ ] **Step 2: Run — expect failure.**

- [ ] **Step 3: Implement** — add `"access_tier"` to `RESULT_COLUMNS` (a provenance/identity column; place it next to `data_source` near the top of the identity block) and `"access_tier STRING,"` to `ACTION_CONTEXT_DDL` in the same position; bump the count comment (`= 139` → `= 140`).

- [ ] **Step 4: Write the migration** (`scripts/migrations/2026-06-30-add-access-tier-columns.sql`):

```sql
-- Per-match access_tier (spec phase 4). Adds raw visibility + derived access_tier to every bronze
-- fact a restricted-aware publisher reads. Operator-applied (no CI auto-apply). Idempotent: the runner
-- skips ADD COLUMNS when the leading column already exists.
ALTER TABLE soccer_analytics.bronze.spadl_action_context ADD COLUMNS (access_tier STRING);
ALTER TABLE soccer_analytics.bronze.spadl_actions ADD COLUMNS (access_tier STRING);
ALTER TABLE soccer_analytics.bronze.skillcorner_matches ADD COLUMNS (visibility STRING, access_tier STRING);
ALTER TABLE soccer_analytics.bronze.gradientsports_metadata ADD COLUMNS (visibility STRING, access_tier STRING);
-- tracking frames + psxg shots bronze: add as confirmed in Task 10/11.
```

- [ ] **Step 5: Run parity tests — expect pass.** `uv run pytest src/tests/action_context/test_schema.py src/tests/test_action_context_schema_parity.py -o addopts="" -p no:xdist -q`

- [ ] **Step 6: Commit** `feat(access-tier): AC schema + bronze migration for access_tier (spec §6.3)`

### Task 8b: One-time backfill of existing rows (BLOCKER 1 — must precede any publish)

**Why:** `ADD COLUMNS` leaves every historical row `access_tier = NULL`; ingestion stamps only NEW
pulls. Without a backfill, the marts rebuild all-NULL for StatsBomb/Wyscout/IDSSE/Metrica/public-SkillCorner,
the split fail-safe routes NULL→restricted, and **every public publish hard-fails / the public datasets
empty.** Backfill existing bronze rows to their **provider-default** tier — values DERIVED from
`classify_access_tier` (don't hand-encode the policy in SQL).

**Files:**
- Create: `scripts/migrations/2026-06-30-backfill-access-tier.py` (a PEP-723/SDK script — it must derive
  the per-provider default from `classify_access_tier`, so it is Python, not raw SQL).
- Test: `src/tests/test_access_tier_backfill.py`.

- [ ] **Step 1: Write the failing test** — the backfill plan maps each existing provider to its default tier via the core, and produces no NULL:

```python
# src/tests/test_access_tier_backfill.py
from shared.access_tier import classify_access_tier
from ingestion.access_tier_backfill import default_tier_for_provider, EXISTING_PROVIDERS

def test_default_tier_matches_the_classifier() -> None:
    # No hand-encoding: the backfill default for a no-feed provider IS classify_access_tier(provider, None).
    for p in EXISTING_PROVIDERS:
        assert default_tier_for_provider(p) == classify_access_tier(provider=p, visibility=None).value

def test_gradientsports_defaults_restricted_others_public() -> None:
    assert default_tier_for_provider("gradientsports") == "restricted"
    for p in ["statsbomb", "wyscout", "idsse", "metrica", "skillcorner"]:
        assert default_tier_for_provider(p) == "public"  # existing SkillCorner rows are the public A-League
```

- [ ] **Step 2: Run — expect failure** (module missing).

- [ ] **Step 3: Implement** `src/ingestion/access_tier_backfill.py`:

```python
"""One-time backfill of access_tier on EXISTING bronze rows (BLOCKER 1). Idempotent (WHERE access_tier IS NULL)."""
from __future__ import annotations
from shared.access_tier import classify_access_tier

EXISTING_PROVIDERS = ("statsbomb", "wyscout", "idsse", "metrica", "skillcorner", "gradientsports")

def default_tier_for_provider(provider: str) -> str:
    # existing rows have no per-match visibility signal → the no-feed provider default.
    return classify_access_tier(provider=provider, visibility=None).value
```

The migration script issues, per affected **per-action / per-frame** bronze table that has a
`data_source` column:
`UPDATE <table> SET access_tier = '<default_tier_for_provider(p)>' WHERE access_tier IS NULL AND data_source = '<p>'`
for each provider (string-interpolate the *derived* tier, never a hand-typed literal).

For the **match-info** bronze tables (`skillcorner_matches`, `gradientsports_metadata`), **re-ingest the
match-info** rather than NULL-backfilling — re-pull the existing matches' metadata so each row carries
its **real** `visibility` (`access_tier` then derives from it). This populates the real value up front,
so there is no NULL to reconcile and the Task 7 immutability assert (with its NULL carve-out) never
trips on the Task 24 re-ingest. (R1: this is the decisive side of the Task 7 ↔ 8b contract.)

- [ ] **Step 4: Run — expect pass.** `uv run pytest src/tests/test_access_tier_backfill.py -o addopts="" -p no:xdist -q`

- [ ] **Step 5: Add the zero-NULL mart guard** (gates rollout, Task 22) — a daily dbt test (or the Task 12
  SQL-text suite's runtime check) asserting **`COUNT(*) WHERE access_tier IS NULL = 0`** on every mart a
  registered publisher reads. The fail-safe must only ever fire on a genuine anomaly, never on the backlog.

- [ ] **Step 6: Commit** `feat(access-tier): one-time backfill of historical rows to provider-default tier (BLOCKER 1)`

### Task 9: `access_tier` through the SPADL/VAEP writer schema + parity

**Files:** Modify `src/ingestion/spadl_enrichments.py` (`_SPADL_SCHEMA`, `_VAEP_SCHEMA`), the applyInPandas StructTypes in `spadl_conversion.py` / `spadl_udf_shared.py` / `spadl_vaep.py`. Test: `src/tests/test_spadl_vaep_writer_parity.py`.

- [ ] **Step 1–4:** Add `access_tier STRING` to `_SPADL_SCHEMA` and `_VAEP_SCHEMA` and to every applyInPandas output `StructType` that mirrors them (the parity test enumerates them). The AC enrichment stamp (Task 10) populates the value; the writer just passes it through. **TDD via `test_spadl_vaep_writer_parity.py`** — it will fail until all mirrored schemas include `access_tier`, then pass.

- [ ] **Step 5: Commit** `feat(access-tier): SPADL/VAEP writer schema passthrough + parity (spec §6.3)`

### Task 10: AC/SPADL writers populate `access_tier` per row

**Files:** Modify the AC enrich/build path (`src/analytics/action_context/schema.py::build_output` already selects RESULT_COLUMNS; the value must be present in the frame) and the SPADL conversion that joins per-match metadata. Test: `src/tests/action_context/test_mini_golden.py` (column presence) + a new value test.

- [ ] **Step 1–4 (M1 — DIRECT stamp, never a join):** The per-action frame must carry `access_tier`.
  **Stamp it directly** in `build_output`/the SPADL conversion from the match's tier — the converter
  already knows provider + match, exactly like the existing `data_source` stamp. **Do NOT join**
  `dim_matches` for it: an unmatched join key → NULL → fail-safe-restricted → **silently drops public
  data** (an availability bug the spec rejected for the publish path, D3). The classifier is the source;
  the converter stamps the resolved value as a constant per (provider, match). Write a test that a
  converted frame for a `visibility="private"` match has `access_tier == "restricted"` on every row;
  for `None`-feed providers (StatsBomb/Wyscout/IDSSE/Metrica), `"public"`.

- [ ] **Step 5: Commit** `feat(access-tier): AC/SPADL writers stamp access_tier per row (spec §6.2)`

### Task 11: `access_tier` onto `fct_tracking_frames` bronze + psxg shots bronze

**Files:** Modify the tracking-frames bronze writer (the one feeding `fct_tracking_frames`) and the psxg shots bronze writer; extend the Task 8 migration. Test: the relevant writer/boundary tests.

- [ ] **Step 1–4:** Stamp `access_tier` per frame/shot from the match tier (same join/stamp as Task 10). Add the `ADD COLUMNS (access_tier STRING)` lines for these bronze tables to the migration. TDD via a column-presence + value test on a private-match fixture.

- [ ] **Step 5: Commit** `feat(access-tier): tracking-frames + psxg-shots bronze carry access_tier (spec §6.5/D9)`

---

## Phase 5 — dbt marts + contracts

### Task 12: `access_tier` (+ `visibility`) through staging → marts → `dim_matches`

**Files:** Modify `dim_matches.sql`, the staging passthroughs, `fct_action_values.sql`, `fct_action_context.sql`, `fct_shot_psxg.sql`, `fct_tracking_frames.sql`, `fct_player_embeddings.sql`, `_marts__models.yml`.

- [ ] **Step 1: Write the failing SQL-text guard test** (dbt builds run only in daily CI — assert on the model SQL text in python-ci, per the project's `reference_dbt_ci_parse_only_tests_daily`):

```python
# src/tests/test_access_tier_mart_sql.py
from pathlib import Path
import pytest

_MARTS = ["fct_action_values", "fct_action_context", "fct_shot_psxg", "fct_tracking_frames", "fct_player_embeddings"]

@pytest.mark.parametrize("model", _MARTS)
def test_mart_selects_access_tier(model: str) -> None:
    sql = Path(f"dbt_project/models/marts/{model}.sql").read_text(encoding="utf-8")
    assert "access_tier" in sql, f"{model}.sql must carry access_tier per-row (spec §6.4)"

def test_dim_matches_has_access_tier_and_visibility() -> None:
    sql = Path("dbt_project/models/marts/dim_matches.sql").read_text(encoding="utf-8")
    assert "access_tier" in sql and "visibility" in sql
```

- [ ] **Step 2: Run — expect failure.**

- [ ] **Step 3: Implement** — add `access_tier` to each staging passthrough + mart SELECT; add `access_tier` + raw `visibility` to `dim_matches` (aggregate from the bronze match-info rows, like `competition_id`); add column entries to `_marts__models.yml` for every contract-enforced mart.

- [ ] **Step 4: Run the SQL-text guard + dbt parse — expect pass.** `uv run pytest src/tests/test_access_tier_mart_sql.py -o addopts="" -p no:xdist -q` and `uv run --extra dbt dbt parse --project-dir dbt_project --profiles-dir dbt_project`.

- [ ] **Step 5: Commit** `feat(access-tier): carry access_tier through staging/marts + dim_matches (spec §6.4)`

---

## Phase 6 — Publisher migration (row-level + fail-closed)

### Task 13: The 4 row-level dataset publishers split on `access_tier` + call the leak guard

**Files:** Modify `scripts/publish_spadl_vaep_hf.py`, `publish_action_context_hf.py`, `publish_psxg_shots_hf.py`, `publish_pitch_control_tracking_hf.py`. Test: `src/tests/test_gradientsports_hf_exclusion.py`, `src/tests/test_hf_publish_parity.py`.

- [ ] **Step 1: Write/extend the failing contract test** — assert all four import `split_restricted` and call `split_restricted(df, column="access_tier")` (grep the SQL has **no** `data_source !=` filter; the call passes `column="access_tier"`), and call `assert_no_private_leak(public_df, publisher=...)` before the public upload.

- [ ] **Step 2: Run — expect failure** (pitch_control_tracking has no split; the three datasets call `split_restricted(df)` without the column arg).

- [ ] **Step 3: Implement** — in each: `public_df, restricted_df = split_restricted(df, column="access_tier")`; `assert_no_private_leak(public_df, publisher="<name>")`; publish both repos (the three already do; `pitch_control_tracking` gains the restricted companion repo `restricted_repo_id(DATASET_REPO)` + its `-restricted` card). `pitch_control_tracking` also needs an in-repo `-restricted` card file (`docs/huggingface/dataset-cards/pitch-control-tracking-restricted.md`).

- [ ] **Step 3b (R2 — drop the internal column before upload):** AFTER the split + `assert_no_private_leak` (which need it), **drop `access_tier` from both frames before upload** — it is a constant per repo (`"public"` in every public artifact), carries no consumer value, and keeping it is a Hyrum additive-schema change to existing public datasets. Order is strict: split → guard → `df.drop(columns=["access_tier"], errors="ignore")` → upload. Add a contract-test assertion that the uploaded frame has **no** `access_tier` column. (Apply the same drop in Task 14 + Task 18's per-match split.)

- [ ] **Step 4: Run — expect pass.**

- [ ] **Step 5: Commit** `feat(access-tier): 4 row-level publishers split on access_tier + leak guard, drop col pre-upload (spec §6.5/D9/R2)`

### Task 14: `line_breaking_passes` + `xg_shots` made fail-closed (D11)

**Files:** Modify `scripts/publish_line_breaking_passes_hf.py`, `publish_xg_shots_hf.py`. Test: `test_gradientsports_hf_exclusion.py`.

- [ ] **Step 1–4:** These carry no SkillCorner today (safe-by-absence). Add `access_tier` to their SELECT (so the column exists) and call `assert_no_private_leak(df, publisher="<name>")` before publishing. If a private row ever appears, the guard fails the run (rather than leaking). TDD: assert the call exists + the guard raises on a synthetic private row.

- [ ] **Step 5: Commit** `feat(access-tier): line_breaking/xg_shots fail-closed leak guard (spec §6.7/D11)`

### Task 15: Resolve ALL THREE `src/ingestion/` publisher twins (B2/C5)

**Files:** `src/ingestion/publish_spadl_vaep_hf.py`, `src/ingestion/publish_xg_shots_hf.py`,
`src/ingestion/publish_freeze_frame_hf.py` (the wired pyproject entry points :183–185), and their
`scripts/` counterparts.

- [ ] **Step 1: Determine the canonical PRODUCTION publish path** (B2 — the entry points are the
  `src/ingestion/` twins, the `scripts/` ones run via `hf jobs`): `grep -rn "publish_spadl_vaep\|publish_xg_shots\|publish_freeze_frame" pyproject.toml terraform/ .github/ workflow-cards/`
  to find which is actually invoked (Databricks job task vs HF Jobs). The migration + guard must target
  whichever RUNS; consolidate the divergent twins to **one canonical publisher per artifact**.
- [ ] **Step 2: Migrate/consolidate each of the three** — the canonical publisher uses
  `split_restricted(df, column="access_tier")` + `assert_no_private_leak(public_df, publisher=...)`; delete
  the redundant twin (only after Step 1 proves it unwired). Critically: `src/ingestion/publish_spadl_vaep_hf.py`
  reads `fct_action_values` (carries SkillCorner) with **no split** today — it must not survive as a
  no-split path on a contaminated mart.
- [ ] **Step 3: Test** — `test_gradientsports_hf_exclusion.py`: **no publisher (in `scripts/` OR
  `src/ingestion/`) partitions on `data_source` for the restriction decision**; exactly one canonical
  publisher per artifact; and `test_hf_leak_guard.py::test_registry_covers_every_publisher_module`
  (Task 4) now globs both dirs so any surviving twin without a split + registry entry fails.
- [ ] **Step 4: Commit** `fix(access-tier): consolidate the three src/ingestion publisher twins to canonical split paths (B2/C5)`

### Task 16: Migrate `publish_tracking_context_hf.py` off the legacy SQL filter (D6)

**Files:** `scripts/publish_tracking_context_hf.py`, `test_gradientsports_hf_exclusion.py`.

- [ ] **Step 1–4:** Replace `WHERE data_source != 'gradientsports'` with the `access_tier` split + leak guard (requires `access_tier` on `fct_tracking_context`; add to its mart + the migration). Remove the publisher from `_GS_GATED_PUBLISHERS` and add to the ADR-049 split set in the two-mode guard test. (If genuinely being deprecated, deprecate in this change instead.)
- [ ] **Step 5: Commit** `feat(access-tier): migrate tracking_context publisher off legacy SQL gate (spec D6)`

---

## Phase 7 — football2vec (derived, public-only upstream)

### Task 17: Public-only career/season aggregate (upstream fix)

**Files:** Modify `dbt_project/models/marts/fct_player_embeddings_career.sql`, `fct_player_embeddings_season.sql` (or the publisher's re-aggregation). Test: SQL-text guard + a value test.

- [ ] **Step 1: Write the failing test** — the career/season aggregate must be computable from public-tier rows only:

```python
# src/tests/test_football2vec_public_only.py
from pathlib import Path

def test_career_season_aggregate_filters_to_public_tier() -> None:
    for m in ["fct_player_embeddings_career", "fct_player_embeddings_season"]:
        sql = Path(f"dbt_project/models/marts/{m}.sql").read_text(encoding="utf-8")
        assert "access_tier = 'public'" in sql or "access_tier='public'" in sql, (
            f"{m} must aggregate from public-tier rows only (spec §6.8 — pre-mix cannot be filtered at publish)"
        )
```

- [ ] **Step 2–4:** Add `WHERE access_tier = 'public'` (public-only) to the career/season aggregation grain, OR build a parallel public aggregate the publisher reads. The per-match `fct_player_embeddings` keeps all tiers (it splits at publish, Task 18). Requires `access_tier` on `fct_player_embeddings` (Task 12).

- [ ] **Step 5 (M2 — semantics, not grep): add a daily dbt DATA test** (`dbt_project/tests/assert_career_season_public_only.sql`). The grep in Step 1 only proves the string exists; it cannot prove the *aggregation* is public-only. The data test takes a player with BOTH public and private matches and asserts the published public career/season vector **equals the vector recomputed from that player's public rows only** (zero rows where they differ). Runs in the **daily dbt build** (parse-only PR CI can't execute it); **gate rollout (Task 22) on a `dbt build` of these models, not just `dbt parse`.**

- [ ] **Step 6: Commit** `feat(access-tier): football2vec career/season aggregate is public-only upstream + dbt data test (spec §6.8/M2)`

### Task 18: football2vec publisher — public corpus + input/output assertions (D10)

**Files:** `scripts/publish_football2vec_embeddings_hf.py`, `scripts/train_football2vec*.py` (corpus filter). Test: `src/tests/test_football2vec_public_only.py`.

- [ ] **Step 1: Write the failing assertions test** — the publisher must (a) assert its materialized input has zero `access_tier != 'public'` rows; (b) assert the published vocabulary ⊆ players with ≥1 public row (private-only player ID absent).

- [ ] **Step 2–4:** Implement: filter the training corpus read (`fct_action_values`) to `access_tier='public'`; in the publisher, `assert_no_private_leak`-style input assertion on the per-match source + an output-vocabulary assertion (the set of `canonical_player_id`s published is a subset of players appearing in `access_tier='public'` rows). **Fail-closed:** if the public career/season aggregate isn't provably public-recomputed, raise and publish only the per-match (split) embeddings. (Recommend: add `torch.manual_seed(...)` to the trainer to restore reproducibility — enables the §9.8 differential test.)

- [ ] **Step 5: Commit** `feat(access-tier): football2vec public-only corpus + input/output leak assertions (spec D10)`

---

## Phase 8 — Trainer gate

### Task 19: VAEP trainer gate keys on policy-can-produce-restricted (D4/C6)

**Files:** `scripts/train_vaep_model_hf.py`. Test: `src/tests/test_sk3_mig_b_orchestrator_invariants.py` or a new trainer-gate test.

- [ ] **Step 1–4:** Change the "restricted repo required" gate from "observed rows" to **policy-can-produce-restricted** (`RESTRICTED_HF_PROVIDERS` non-empty OR any feed can emit `private`). Add a comment marking the **token-misconfig canary** property (C6) so it is not softened. TDD: a test asserting the gate raises when the restricted repo is empty while the policy expects restricted.

- [ ] **Step 5: Commit** `feat(access-tier): VAEP trainer gate = policy-can-produce-restricted (canary, spec D4/C6)`

---

## Phase 9 — Cross-repo contract + final guard wiring

### Task 20: pining `visibility` vocabulary contract (C2)

**Files:** Create `src/tests/test_pining_visibility_contract.py`.

- [ ] **Step 1–4:** A hermetic test over a recorded fixture of a real `/skillcorner/matches` response asserting `visibility ∈ {"public","private"}` and that `classify_access_tier` maps each explicitly (unknown → the test that an unrecognized value would route to RESTRICTED, never PUBLIC). An **env-gated** (`@pytest.mark.skipif(not os.getenv("PINING_LIVE_CONTRACT"))`) live variant hits the API with the owner token. Mirrors pining's producer-side schema test.

- [ ] **Step 5: Commit** `test(access-tier): cross-repo pining visibility vocabulary contract (spec C2)`

### Task 21: Observability — per-tier counts + restricted-zero alert (C7)

**Files:** the 4 split publishers + football2vec.

- [ ] **Step 1–4:** Each publisher logs per-tier row counts to each repo at INFO; emits an ERROR-level log when restricted-count == 0 while the policy expects restricted (token-misconfig backstop). TDD via caplog assertions.

- [ ] **Step 5: Commit** `feat(access-tier): per-tier publish counts + restricted-zero alert (spec C7)`

---

## Phase 10 — Rollout (ops; AFTER 1–9 merged + deployed)

**Operational sequencing (operator):** *backfill the existing data and prove the whole mechanism works
on it BEFORE ingesting any new private SkillCorner.* This validates the split + leak guard + public-repo
integrity on **known-safe data** (existing SkillCorner is the 10 public A-League matches; no restricted
SkillCorner exists yet), so the first time real restricted data flows, the machinery is already proven.

### Task 22: Deploy + backfill (no private data exists yet)

- [ ] Bump the wheel (every-change rule) + deploy; full CI green incl. `test_access_tier`,
  `test_hf_leak_guard` (registry globs both dirs), `test_access_tier_backfill`, the parity tests.
- [ ] **M4 — migration completeness checklist:** confirm `2026-06-30-add-access-tier-columns.sql` covers
  **every** bronze table a registered publisher reads (AC, spadl_actions, tracking-frames, psxg-shots,
  tracking-context source, the two match-info tables). Apply: `uv run --extra sdk python
  scripts/migrations/_runner.py scripts/migrations/2026-06-30-add-access-tier-columns.sql`; `DESCRIBE` each.
- [ ] **Run the Task 8b backfill** on existing bronze; then rebuild the affected marts (rederive).
- [ ] **B1 zero-NULL gate (must pass):** assert `COUNT(*) WHERE access_tier IS NULL = 0` on every mart a
  registered publisher reads. If any NULL remains, STOP — the fail-safe would block/empty publishes.

### Task 23: Prove it works on SAFE data — the "we know it works" gate (§8.6, operator)

- [ ] **Full real publish cycle on the existing (all-public-SkillCorner) data.** Run every registered
  publisher and verify the **public** repos are **intact**: StatsBomb / Wyscout / IDSSE / Metrica / the
  10 public A-League SkillCorner matches all present in public; **GradientSports present only in the
  `-restricted` repos**; per-tier counts (C7) sane; the every-run leak guard green for all.
- [ ] **M5 — synthetic leak drill, one publisher PER MODE** (not just one): inject a synthetic
  `access_tier='restricted'` row into (a) a **split** publisher (e.g. action_context), (b) a
  **fail_closed** publisher (e.g. xg_shots), (c) the **derived** path (football2vec input) — confirm each
  raises `LeakDetected` and the row never reaches a public artifact. Combined with the Task 4 registry
  test ("every publisher calls the guard"), this makes coverage **provable, not assumed**.
- [ ] **R4 — pre-state the EXPECTED (non-regression) deltas so "intact" is unambiguous.** "Intact"
  means *intact modulo these two known, intended changes*:
  (a) the public **football2vec** embeddings change — the GradientSports contribution is now excluded
  (public-only sourcing, Task 17). That is the fix working, **not** data loss; assert GS players are
  absent from the public embedding vocabulary, present in the restricted/internal one.
  (b) the public **datasets' schema** loses no column and gains none — because R2 drops `access_tier`
  before upload (Task 13 Step 3b). Assert the published public schema equals today's (no new
  `access_tier` column). (If R2 were ever reverted, this delta would reappear and must be carded.)
- [ ] Gate: do NOT proceed to Task 24 until this cycle is clean (public datasets unchanged from today
  **except** the GS→restricted move + the two deltas above; zero leak; zero NULL).

### Task 24: Controlled private ingestion (§8.7–8.8 — only after Task 23 green)

- [ ] Run SkillCorner ingestion (the owner token already returns the 98 private RM matches) + scoped
  AC / SPADL / tracking re-materialize → the private matches carry `access_tier='restricted'` end-to-end.
- [ ] Republish HF; **verify**: a known private RM match appears ONLY in the `-restricted` repos and NOT
  in any public repo (datasets + pitch-control + the embeddings vocabulary); a known public A-League
  match appears only in public. Confirm per-tier publish counts (C7).
- [ ] Ping the producer session to confirm the boundary holds end-to-end.

---

## Self-Review

**Spec coverage:** §4 core → Task 1; D5 → Task 2; split/D1 → Task 3; C3/§9.7 leak guard (registry globs both dirs) → Task 4 (+ wired in 13/14/18); §6.2 ingestion stamp (M3 caller-grep) → Tasks 5–6; A3 → Task 7; §6.3 schema passthrough → Tasks 8–11; **B1 backfill → Task 8b**; §6.4 marts/dim → Task 12; §6.5 row-level (incl. D9 pitch_control) → Task 13; D11 fail-closed → Task 14; **B2/C5 all three twins + canonical path → Task 15**; D6 → Task 16; §6.8 football2vec/D10 (+ M2 dbt data test) → Tasks 17–18; D4/C6 trainer → Task 19; C2 contract → Task 20; C7 observability → Task 21; §8 rollout (M4 checklist, M5 per-mode drill, B1 zero-NULL gate, backfill-and-verify-on-safe-data-first) → Tasks 22–24. **No gap found.**

**Review-2 resolutions:** B1 (backfill) → Task 8b + Task 22 zero-NULL gate; B2 (publish path) → Task 4 both-dir glob + Task 15 three twins; B3 (SkillCorner HF policy) → operator: split-to-both, private repo = permitted backup, public repo = prohibited sharing (the leak guard is the do-not-share enforcement); M1 → Task 10 direct stamp; M2 → Task 17 dbt data test; M3 → Task 5 Step 2b; M4/M5 → Task 22/23.

**Review-3 resolutions:** R1 (Task 7 NULL carve-out + Task 8b re-ingest) → no Task 7↔8b tension; R2 (drop `access_tier` pre-upload) → Task 13 Step 3b (+ 14/18); R3 (`MatchInfo.visibility` required-no-default test) → Task 5 Step 5; R4 (expected-delta acceptance) → Task 23. The self-review's earlier "no gap found" missed only R2 (the published-column question) — now covered.

**Placeholder scan:** Tasks 5/10/11/12 reference "the existing parse shape" / "the converter's match-metadata join" — these are real, codebase-specific anchors (file:line given) rather than code I can invent without reading the exact writer; the reviewer/implementer fills the literal SQL/columns from those anchors. All novel logic (core, split, leak guard, classifier, immutability, football2vec assertions) has complete code.

**Type consistency:** `AccessTier` (PUBLIC/RESTRICTED, `.value` = "public"/"restricted"), `classify_access_tier(*, provider, visibility)`, `RESTRICTED_HF_PROVIDERS`, `split_restricted(df, column="access_tier")`, `assert_no_private_leak(df, *, publisher)`, `PUBLISHER_REGISTRY`, `LeakDetected` — names used consistently across all tasks.
