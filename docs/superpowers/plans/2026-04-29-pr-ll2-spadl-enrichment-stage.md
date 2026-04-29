# PR-LL2 Implementation Plan — SPADL Enrichment Stage + 4-Source Coverage + LL1 Cleanup

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Establish a named SPADL post-conversion enrichment stage as the canonical home for provider-agnostic silly-kicks helpers, fix three latent gaps from PR-LL1, expand SPADL coverage from 2 sources to 4 (StatsBomb / Wyscout / IDSSE / Metrica), apply β-consistent canonical-vs-native naming to `fct_action_values`, and modernize `fct_funnel_stages_agg` + Taipy funnel to use canonical heuristic possessions.

**Architecture:** New module `src/ingestion/spadl_enrichments.py` with pure-pandas `apply_spadl_enrichments(actions, *, source)` runs silly-kicks's `add_possessions` + `add_gk_role` + `add_pre_shot_gk_context` per-match inside the existing `groupBy(match_id).applyInPandas(...)` UDFs. Each of 4 source UDFs (StatsBomb / Wyscout dedicated; IDSSE / Metrica via silly-kicks 1.7.0's new dedicated DataFrame converters) emits canonical + provider-namespaced columns. Bronze schemas evolved via idempotent ALTER script. `fct_action_values` gets β-consistent rename in one PR; `fct_funnel_stages_agg` retires its Wyscout-synthetic-possession workaround.

**Tech Stack:** Python 3.10, pandas, silly-kicks 1.7.0 (`>=1.7.0,<2.0`), PySpark on Databricks serverless, Delta Lake, dbt (incremental + contract: enforced), Databricks SQL connector, ruff/pyright/pytest, hatchling. luxury-lakehouse main @ `e88b35b` (wheel 0.3.20).

**Spec source of truth:** [docs/superpowers/specs/2026-04-29-pr-ll2-spadl-enrichment-stage-design.md](../specs/2026-04-29-pr-ll2-spadl-enrichment-stage-design.md). Decisions log in §Decisions.

**Commit policy:** Local TDD commits per task for execution checkpoints. **All commits squashed at PR-merge time** per silly-kicks single-commit-per-branch convention. Branch: `feat/spadl-enrichment-stage`. Wheel bump 0.3.20 → 0.3.21 in the dedicated commit per Phase 16.

**Pre-merge gates that need explicit user approval:** Phase 17 (run bronze ALTER against live Databricks), Phase 22 (open PR), Phase 22 (squash-merge). **Post-merge gates:** Phase 23 (destructive DELETE + wf-vaep), Phase 24 (dbt full-refresh), Phase 25 (Taipy deploy).

---

## Phase 0 — Branch setup

### Task 0.1: Verify clean working tree and create feature branch

**Files:** none modified.

- [ ] **Step 1: Verify clean tree**

Run: `git status --short`

Expected: empty output (no uncommitted changes other than the spec we already wrote — if the spec is uncommitted that's fine, it gets bundled into the squash later).

If anything else uncommitted, stop and ask the user before proceeding.

- [ ] **Step 2: Verify on main and up-to-date**

Run: `git log --oneline -1`

Expected: `e88b35b feat(spadl): silly-kicks 1.5.0 preserve_native + Kimball alignment for action-values (PR-LL1) (#223)` or later.

- [ ] **Step 3: Create the feature branch**

Run: `git checkout -b feat/spadl-enrichment-stage`

Expected: `Switched to a new branch 'feat/spadl-enrichment-stage'`

---

## Phase 1 — `apply_spadl_enrichments` module + contract tests

### Task 1.1: Write contract tests for `apply_spadl_enrichments`

**Files:**
- Create: `src/tests/test_spadl_enrichments.py`

- [ ] **Step 1: Write failing contract tests**

Create `src/tests/test_spadl_enrichments.py`:

```python
"""Tests for src/ingestion/spadl_enrichments.py.

Layered test strategy (see PR-LL2 spec §Test plan):
    - Contract tests (this file, TestContract): shape/dtype/empty/mutation
    - Plausibility tests (TestPlausibility): real-fixture sanity checks
    - Boundary-F1 test (TestBoundaryF1): heuristic vs StatsBomb native, F1≥0.85

silly-kicks owns the algorithm-level golden tests for the 3 helpers (verified
comprehensive: 597 LOC test_add_possessions.py + 438 LOC test_add_gk_role.py
+ 422 LOC test_add_pre_shot_gk_context.py in silly-kicks repo).
"""

from __future__ import annotations

import pandas as pd
import pytest

from ingestion.spadl_enrichments import apply_spadl_enrichments

_PASS_TYPE_ID = 0   # silly_kicks.spadl.config.actiontype_id["pass"] in 1.7.0
_FOOT_BODY_ID = 0   # silly_kicks.spadl.config.bodypart_id["foot"]
_SUCCESS_RES_ID = 1 # silly_kicks.spadl.config.result_id["success"]


def _build_minimal_spadl_fixture(n: int = 5, *, team_id: int = 100) -> pd.DataFrame:
    """Build a minimal SPADL-shaped fixture for unit tests.

    Mirrors the shape produced by silly-kicks's per-provider convert_to_actions
    (game_id / period_id / time_seconds / action_id / team_id / type_id / etc.)
    so silly-kicks's add_possessions / add_gk_role / add_pre_shot_gk_context
    helpers can run on it.
    """
    return pd.DataFrame(
        [
            {
                "game_id": 1,
                "match_id": 1,
                "original_event_id": str(i),
                "action_id": i,
                "period_id": 1,
                "time_seconds": float(i),
                "team_id": team_id,
                "player_id": 200,
                "type_id": _PASS_TYPE_ID,
                "result_id": _SUCCESS_RES_ID,
                "bodypart_id": _FOOT_BODY_ID,
                "start_x": 50.0,
                "start_y": 34.0,
                "end_x": 60.0,
                "end_y": 34.0,
            }
            for i in range(n)
        ]
    )


class TestContract:
    def test_returns_dataframe(self):
        actions = _build_minimal_spadl_fixture()
        result = apply_spadl_enrichments(actions, source="statsbomb")
        assert isinstance(result, pd.DataFrame)

    def test_adds_six_new_columns(self):
        actions = _build_minimal_spadl_fixture()
        result = apply_spadl_enrichments(actions, source="statsbomb")
        for col in [
            "possession_id_heuristic",
            "gk_role",
            "gk_was_distributing",
            "gk_was_engaged",
            "gk_actions_in_possession",
            "defending_gk_player_id",
        ]:
            assert col in result.columns, f"missing {col}"

    def test_preserves_input_columns(self):
        actions = _build_minimal_spadl_fixture()
        result = apply_spadl_enrichments(actions, source="statsbomb")
        for col in actions.columns:
            assert col in result.columns

    def test_does_not_mutate_input(self):
        actions = _build_minimal_spadl_fixture()
        cols_before = list(actions.columns)
        apply_spadl_enrichments(actions, source="statsbomb")
        assert list(actions.columns) == cols_before

    def test_handles_empty_input(self):
        actions = _build_minimal_spadl_fixture(n=0)
        result = apply_spadl_enrichments(actions, source="statsbomb")
        assert "possession_id_heuristic" in result.columns
        assert len(result) == 0

    def test_invalid_source_raises_value_error(self):
        actions = _build_minimal_spadl_fixture()
        with pytest.raises(ValueError, match=r"source"):
            apply_spadl_enrichments(actions, source="unknown_provider")

    def test_action_id_preserved(self):
        actions = _build_minimal_spadl_fixture(n=10)
        original_ids = set(actions["action_id"].tolist())
        result = apply_spadl_enrichments(actions, source="statsbomb")
        # silly-kicks's add_dribbles may insert synthetic rows with new
        # action_ids — original ones must still be present.
        assert original_ids.issubset(set(result["action_id"].tolist()))
```

- [ ] **Step 2: Run tests to verify failure**

Run: `uv run pytest src/tests/test_spadl_enrichments.py::TestContract -v`

Expected: All tests FAIL with `ModuleNotFoundError: No module named 'ingestion.spadl_enrichments'`.

### Task 1.2: Implement `apply_spadl_enrichments`

**Files:**
- Create: `src/ingestion/spadl_enrichments.py`

- [ ] **Step 1: Implement the module**

Create `src/ingestion/spadl_enrichments.py`:

```python
"""SPADL post-conversion enrichment stage.

Provider-agnostic helpers from silly-kicks applied to the canonical SPADL
output of any per-provider converter. Establishes the named architectural
home for these enrichments — see ADR-016 for the design rationale.

First occupants (LL2):
    - silly_kicks.spadl.utils.add_possessions      → possession_id_heuristic
    - silly_kicks.spadl.utils.add_gk_role          → gk_role
    - silly_kicks.spadl.utils.add_pre_shot_gk_context
                                                   → gk_was_distributing,
                                                     gk_was_engaged,
                                                     gk_actions_in_possession,
                                                     defending_gk_player_id

Future helpers (e.g., add_gk_distribution_metrics) plug in by extending
``apply_spadl_enrichments`` and adding their column declarations to the
schema constants in ``ingestion.spadl_vaep``.
"""

from __future__ import annotations

from typing import Final

import pandas as pd

_VALID_SOURCES: Final[frozenset[str]] = frozenset(
    {"statsbomb", "wyscout", "idsse", "metrica"}
)


def apply_spadl_enrichments(
    actions: pd.DataFrame,
    *,
    source: str,
) -> pd.DataFrame:
    """Apply provider-agnostic SPADL post-conversion enrichments.

    Runs silly-kicks's ``add_possessions``, ``add_gk_role``, and
    ``add_pre_shot_gk_context`` in order. The helpers themselves are
    provider-agnostic — they read the canonical SPADL columns and don't
    branch on ``source``. The parameter is kept here for telemetry and
    for any future helper that needs source-specific behavior.

    Args:
        actions: SPADL action DataFrame from any provider's
            ``convert_to_actions``. Must include ``action_id`` (silly-kicks's
            helpers require it as input — silly-kicks's converters emit it
            automatically; luxury-lakehouse's UDFs surface it through the
            output StructType).
        source: One of ``"statsbomb"``, ``"wyscout"``, ``"idsse"``,
            ``"metrica"``.

    Returns:
        A copy of ``actions`` with 6 new columns appended:
        ``possession_id_heuristic``, ``gk_role``, ``gk_was_distributing``,
        ``gk_was_engaged``, ``gk_actions_in_possession``,
        ``defending_gk_player_id``.

    Raises:
        ValueError: If ``source`` is not in the valid set.
    """
    if source not in _VALID_SOURCES:
        raise ValueError(
            f"apply_spadl_enrichments: unknown source {source!r}. "
            f"Valid sources: {sorted(_VALID_SOURCES)}"
        )

    # Imports inside function — silly-kicks is a heavy dep we don't want at
    # module-import time for tests of unrelated modules.
    from silly_kicks.spadl.utils import (
        add_gk_role,
        add_possessions,
        add_pre_shot_gk_context,
    )

    enriched = add_possessions(actions)
    enriched = add_gk_role(enriched)
    enriched = add_pre_shot_gk_context(enriched)

    # silly-kicks emits ``possession_id`` from add_possessions. Rename to
    # ``possession_id_heuristic`` to make provenance explicit at the bronze
    # layer. The mart-level canonical ``possession_id`` is sourced from this
    # column via a SELECT alias (see fct_action_values.sql).
    enriched = enriched.rename(columns={"possession_id": "possession_id_heuristic"})

    return enriched
```

- [ ] **Step 2: Run tests to verify pass**

Run: `uv run pytest src/tests/test_spadl_enrichments.py::TestContract -v`

Expected: All 7 contract tests PASS.

- [ ] **Step 3: Run lint + types**

Run: `uv run ruff check src/ingestion/spadl_enrichments.py src/tests/test_spadl_enrichments.py && uv run pyright src/ingestion/spadl_enrichments.py`

Expected: Both clean (no warnings, no errors).

- [ ] **Step 4: Commit**

```bash
git add src/ingestion/spadl_enrichments.py src/tests/test_spadl_enrichments.py
git commit -m "feat(spadl): apply_spadl_enrichments named stage + contract tests"
```

### Task 1.3: Add plausibility tests against a synthetic GK fixture

**Files:**
- Modify: `src/tests/test_spadl_enrichments.py` (append `TestPlausibility` class)

- [ ] **Step 1: Append plausibility tests**

Append to `src/tests/test_spadl_enrichments.py`:

```python
def _build_match_with_gk_actions() -> pd.DataFrame:
    """Build a fixture with mixed GK + outfield + shot actions for plausibility checks.

    Match structure: 6 outfield passes by team A, then a shot by team A,
    a save by team B's GK, GK distribution (goalkick) by GK, then 4 more
    passes by team B. Two distinct teams (A=100, B=200), GK player_id=999.
    """
    rows = [
        # Team A possession 1: 3 passes
        {"action_id": 0, "type_id": 0,  "team_id": 100, "player_id": 200, "time_seconds": 0.0,  "start_x": 50.0, "start_y": 34.0, "end_x": 60.0, "end_y": 34.0},
        {"action_id": 1, "type_id": 0,  "team_id": 100, "player_id": 201, "time_seconds": 1.0,  "start_x": 60.0, "start_y": 34.0, "end_x": 70.0, "end_y": 34.0},
        {"action_id": 2, "type_id": 0,  "team_id": 100, "player_id": 202, "time_seconds": 2.0,  "start_x": 70.0, "start_y": 34.0, "end_x": 90.0, "end_y": 34.0},
        # Team A shot
        {"action_id": 3, "type_id": 11, "team_id": 100, "player_id": 202, "time_seconds": 3.0,  "start_x": 95.0, "start_y": 34.0, "end_x": 105.0, "end_y": 34.0},
        # Team B GK save (in box) — shot_stopping
        {"action_id": 4, "type_id": 14, "team_id": 200, "player_id": 999, "time_seconds": 4.0,  "start_x": 5.0,  "start_y": 34.0, "end_x": 5.0,  "end_y": 34.0},
        # Team B GK distribution (goalkick by same player) — distribution role
        {"action_id": 5, "type_id": 22, "team_id": 200, "player_id": 999, "time_seconds": 6.0,  "start_x": 5.0,  "start_y": 34.0, "end_x": 50.0, "end_y": 34.0},
        # Team B regular passes
        {"action_id": 6, "type_id": 0,  "team_id": 200, "player_id": 300, "time_seconds": 8.0,  "start_x": 50.0, "start_y": 34.0, "end_x": 60.0, "end_y": 34.0},
        {"action_id": 7, "type_id": 0,  "team_id": 200, "player_id": 301, "time_seconds": 9.0,  "start_x": 60.0, "start_y": 34.0, "end_x": 70.0, "end_y": 34.0},
        # Team A shot (defending GK 999 was engaged 5+ actions ago)
        {"action_id": 8, "type_id": 11, "team_id": 100, "player_id": 202, "time_seconds": 13.0, "start_x": 95.0, "start_y": 34.0, "end_x": 105.0, "end_y": 34.0},
    ]
    # Common columns for every row
    for r in rows:
        r["game_id"] = 1
        r["match_id"] = 1
        r["original_event_id"] = str(r["action_id"])
        r["period_id"] = 1
        r["result_id"] = _SUCCESS_RES_ID
        r["bodypart_id"] = _FOOT_BODY_ID
    return pd.DataFrame(rows)


class TestPlausibility:
    def test_gk_role_assigned_on_keeper_actions(self):
        actions = _build_match_with_gk_actions()
        result = apply_spadl_enrichments(actions, source="statsbomb")
        # Action 4 is the keeper_save — must get a non-null gk_role
        save_row = result[result["action_id"] == 4].iloc[0]
        assert pd.notna(save_row["gk_role"])
        assert save_row["gk_role"] in {"shot_stopping", "sweeping"}

    def test_distribution_tagged_after_keeper_action(self):
        actions = _build_match_with_gk_actions()
        result = apply_spadl_enrichments(actions, source="statsbomb")
        # Action 5 is the goalkick by the same player who just made the save
        distribution_row = result[result["action_id"] == 5].iloc[0]
        assert distribution_row["gk_role"] == "distribution"

    def test_outfield_pass_gets_null_gk_role(self):
        actions = _build_match_with_gk_actions()
        result = apply_spadl_enrichments(actions, source="statsbomb")
        # Actions 0-2 are outfield passes — no gk_role
        for action_id in [0, 1, 2]:
            row = result[result["action_id"] == action_id].iloc[0]
            assert pd.isna(row["gk_role"])

    def test_gk_was_engaged_only_on_shot_rows(self):
        actions = _build_match_with_gk_actions()
        result = apply_spadl_enrichments(actions, source="statsbomb")
        # Non-shot rows must always have gk_was_engaged == False
        non_shots = result[~result["type_id"].isin({11})]  # 11 = shot
        for engaged in non_shots["gk_was_engaged"]:
            assert bool(engaged) is False

    def test_possession_id_heuristic_starts_at_zero(self):
        actions = _build_match_with_gk_actions()
        result = apply_spadl_enrichments(actions, source="statsbomb")
        sorted_result = result.sort_values(["game_id", "period_id", "action_id"]).reset_index(drop=True)
        assert sorted_result["possession_id_heuristic"].iloc[0] == 0

    def test_possession_id_heuristic_monotonic(self):
        actions = _build_match_with_gk_actions()
        result = apply_spadl_enrichments(actions, source="statsbomb")
        sorted_result = result.sort_values(["game_id", "period_id", "action_id"]).reset_index(drop=True)
        ids = sorted_result["possession_id_heuristic"].to_numpy()
        for i in range(1, len(ids)):
            assert ids[i] >= ids[i - 1], f"possession_id_heuristic not monotonic at row {i}"
```

- [ ] **Step 2: Run tests**

Run: `uv run pytest src/tests/test_spadl_enrichments.py::TestPlausibility -v`

Expected: All 6 plausibility tests PASS.

- [ ] **Step 3: Commit**

```bash
git add src/tests/test_spadl_enrichments.py
git commit -m "test(spadl): plausibility checks for apply_spadl_enrichments"
```

---

## Phase 2 — Build the StatsBomb fixture for the boundary-F1 test

### Task 2.1: Write `scripts/build_test_fixtures.py`

**Files:**
- Create: `scripts/build_test_fixtures.py`

- [ ] **Step 1: Create the fixture-builder script**

Create `scripts/build_test_fixtures.py`:

```python
#!/usr/bin/env python3
"""One-shot builder for luxury-lakehouse test fixtures.

Currently builds:
    src/tests/fixtures/spadl_3match_statsbomb_for_f1.parquet
        — 3 StatsBomb open-data matches converted to SPADL with native
          ``possession`` preserved. Used by
          test_spadl_enrichments.py::TestBoundaryF1 to validate the
          add_possessions heuristic against StatsBomb's native
          possession_id (boundary-F1 ≥ 0.85).

Re-run only when silly-kicks's StatsBomb converter output shape changes
or fixture matches are swapped.

Usage:
    uv run python scripts/build_test_fixtures.py

Requires network access to GitHub (statsbomb/open-data raw URLs).
"""

from __future__ import annotations

import io
import json
from pathlib import Path

import pandas as pd
import requests

# Three matches across competition classes for diversity:
#   7298  — Women's World Cup 2023 group stage
#   7584  — Champions League knockout (also used as silly-kicks's e2e fixture path)
#   3855  — Premier League regular season
# If any are unavailable, swap before running. List taken from the public
# StatsBomb open-data competitions index.
_MATCH_IDS: list[int] = [7298, 7584, 3855]

_SB_OPEN_DATA_BASE = "https://raw.githubusercontent.com/statsbomb/open-data/master/data"
_FIXTURE_OUT = Path(__file__).resolve().parent.parent / "src" / "tests" / "fixtures" / "spadl_3match_statsbomb_for_f1.parquet"


def _fetch_match_events(match_id: int) -> list[dict]:
    """Fetch raw StatsBomb event JSON for one match."""
    url = f"{_SB_OPEN_DATA_BASE}/events/{match_id}.json"
    resp = requests.get(url, timeout=(10, 30))
    resp.raise_for_status()
    return json.loads(resp.text)


def _adapt_to_silly_kicks_input(events_raw: list[dict], match_id: int) -> pd.DataFrame:
    """Flatten StatsBomb open-data event JSON into silly-kicks's expected DataFrame shape.

    Mirrors the adapter pattern used in silly-kicks's tests/spadl/test_add_possessions.py
    (the @pytest.mark.e2e fixture loader at test_add_possessions.py:547-596).
    """
    _top_level_keys = {"id", "period", "timestamp", "team", "player", "type", "location"}
    return pd.DataFrame(
        [
            {
                "game_id": match_id,
                "event_id": e.get("id"),
                "period_id": e.get("period"),
                "timestamp": e.get("timestamp"),
                "team_id": (e.get("team") or {}).get("id"),
                "player_id": (e.get("player") or {}).get("id"),
                "type_name": (e.get("type") or {}).get("name"),
                "location": e.get("location"),
                "extra": {k: v for k, v in e.items() if k not in _top_level_keys},
                # preserve_native target — top-level possession sequence number
                "possession": e.get("possession"),
            }
            for e in events_raw
        ]
    )


def _build_one_match(match_id: int) -> pd.DataFrame:
    """Convert one StatsBomb match to SPADL with native possession preserved."""
    from silly_kicks.spadl import statsbomb

    events_raw = _fetch_match_events(match_id)
    adapted = _adapt_to_silly_kicks_input(events_raw, match_id)
    if len(adapted) == 0:
        raise RuntimeError(f"empty events for match_id={match_id}")
    home_team_id = int(adapted["team_id"].dropna().iloc[0])
    actions, _report = statsbomb.convert_to_actions(
        adapted,
        home_team_id=home_team_id,
        preserve_native=["possession"],
    )
    actions["match_id"] = match_id
    return actions


def main() -> None:
    print(f"Building {_FIXTURE_OUT.name} from {len(_MATCH_IDS)} matches...")
    all_actions: list[pd.DataFrame] = []
    for mid in _MATCH_IDS:
        print(f"  fetching match {mid}...")
        df = _build_one_match(mid)
        print(f"    -> {len(df):,} SPADL actions")
        all_actions.append(df)

    combined = pd.concat(all_actions, ignore_index=True)
    print(f"  combined: {len(combined):,} actions across {combined['match_id'].nunique()} matches")

    _FIXTURE_OUT.parent.mkdir(parents=True, exist_ok=True)
    combined.to_parquet(_FIXTURE_OUT, index=False)
    print(f"  wrote {_FIXTURE_OUT}")
    print(f"  size: {_FIXTURE_OUT.stat().st_size / 1024:.1f} KB")


if __name__ == "__main__":
    main()
```

- [ ] **Step 2: Run lint**

Run: `uv run ruff check scripts/build_test_fixtures.py`

Expected: clean.

### Task 2.2: Run the script and produce the fixture

**Files:**
- Create (binary): `src/tests/fixtures/spadl_3match_statsbomb_for_f1.parquet`

- [ ] **Step 1: Verify the fixtures directory exists**

Run: `ls src/tests/fixtures/ 2>/dev/null || mkdir -p src/tests/fixtures/`

- [ ] **Step 2: Run the fixture builder**

Run: `uv run --extra analytics python scripts/build_test_fixtures.py`

Expected output (approximate):
```
Building spadl_3match_statsbomb_for_f1.parquet from 3 matches...
  fetching match 7298...
    -> ~2,000 SPADL actions
  fetching match 7584...
    -> ~2,000 SPADL actions
  fetching match 3855...
    -> ~2,000 SPADL actions
  combined: ~6,000 actions across 3 matches
  wrote src/tests/fixtures/spadl_3match_statsbomb_for_f1.parquet
  size: ~500-1500 KB
```

If a match is unavailable (404), open the script and swap that match_id for another from the StatsBomb open-data competitions index, then re-run.

- [ ] **Step 3: Verify the parquet is valid**

Run:
```bash
uv run python -c "
import pandas as pd
df = pd.read_parquet('src/tests/fixtures/spadl_3match_statsbomb_for_f1.parquet')
print(f'rows={len(df):,} matches={df[\"match_id\"].nunique()} possession_native_populated={df[\"possession\"].notna().sum():,}')
print(f'columns: {list(df.columns)}')
"
```

Expected: rows >5,000, matches=3, `possession` column present and largely populated (synthetic dribble rows are NaN — that's fine).

- [ ] **Step 4: Commit fixture + script**

```bash
git add scripts/build_test_fixtures.py src/tests/fixtures/spadl_3match_statsbomb_for_f1.parquet
git commit -m "test(spadl): boundary-F1 fixture (3 StatsBomb matches) + builder script"
```

---

## Phase 3 — Boundary-F1 test

### Task 3.1: Add boundary-F1 test against the fixture

**Files:**
- Modify: `src/tests/test_spadl_enrichments.py` (append `TestBoundaryF1` class + helper)

- [ ] **Step 1: Append the boundary-F1 test**

Append to `src/tests/test_spadl_enrichments.py`:

```python
import numpy as np
from pathlib import Path

_FIXTURE_PATH = Path(__file__).resolve().parent / "fixtures" / "spadl_3match_statsbomb_for_f1.parquet"


def _boundary_f1(heuristic: pd.Series, native: pd.Series) -> float:
    """F1 score on possession boundaries between two id sequences.

    Boundaries are invariant under counter relabeling, so heuristic
    possession_id (0-indexed) and native possession_id (provider's
    offset) compare directly on where they emit a boundary.
    """
    h_changes = heuristic.ne(heuristic.shift(1)).iloc[1:].to_numpy()
    n_changes = native.ne(native.shift(1)).iloc[1:].to_numpy()
    tp = int((h_changes & n_changes).sum())
    fp = int((h_changes & ~n_changes).sum())
    fn = int((~h_changes & n_changes).sum())
    precision = tp / (tp + fp) if (tp + fp) > 0 else 0.0
    recall = tp / (tp + fn) if (tp + fn) > 0 else 0.0
    if precision + recall == 0:
        return 0.0
    return 2 * precision * recall / (precision + recall)


@pytest.mark.skipif(
    not _FIXTURE_PATH.exists(),
    reason=f"boundary-F1 fixture missing — run scripts/build_test_fixtures.py",
)
class TestBoundaryF1:
    def test_boundary_f1_against_native_statsbomb(self):
        """Validate add_possessions heuristic against StatsBomb's native possession_id.

        Threshold: F1 ≥ 0.85 (conservative; silly-kicks claims ~0.90 on
        published team-change-with-carve-outs heuristics). Refine to
        (measured - 0.02) per silly-kicks convention if the empirical
        number suggests recalibration.
        """
        all_actions = pd.read_parquet(_FIXTURE_PATH)

        # Drop synthetic dribble rows — they have NaN ``possession`` because
        # silly-kicks's _add_dribbles inserts them with no native counterpart.
        non_synthetic = all_actions[all_actions["possession"].notna()].copy()

        # Compute per-match F1, then average across the 3 matches.
        f1_per_match: dict[int, float] = {}
        for match_id, match_df in non_synthetic.groupby("match_id"):
            enriched = apply_spadl_enrichments(match_df.copy(), source="statsbomb")
            # Re-attach native possession (apply_spadl_enrichments preserves it
            # because it's part of the input DataFrame — but sort consistently).
            enriched = enriched.sort_values(
                ["game_id", "period_id", "action_id"]
            ).reset_index(drop=True)
            heuristic = enriched["possession_id_heuristic"]
            native = enriched["possession"].astype(np.int64)
            f1_per_match[int(match_id)] = _boundary_f1(heuristic, native)

        avg_f1 = float(np.mean(list(f1_per_match.values())))
        per_match_str = ", ".join(f"{m}={f:.3f}" for m, f in f1_per_match.items())
        assert avg_f1 >= 0.85, (
            f"boundary-F1 {avg_f1:.4f} below 0.85 threshold. "
            f"Per-match: {per_match_str}. "
            f"If empirically lower with no algorithm regression, "
            f"lower threshold to (measured - 0.02) per silly-kicks convention."
        )
```

- [ ] **Step 2: Run the boundary-F1 test**

Run: `uv run pytest src/tests/test_spadl_enrichments.py::TestBoundaryF1 -v -s`

Expected: PASS with average F1 ~0.85-0.92. The `-s` flag shows the per-match values via the assertion message on success-or-failure.

If F1 < 0.85: stop and investigate. Either (a) the fixture matches have unusual possession patterns, (b) silly-kicks's `add_possessions` algorithm regressed, or (c) the threshold needs adjustment per the silly-kicks `(measured - 0.02)` convention. Surface the empirical number to the user and ask before adjusting.

- [ ] **Step 3: Commit**

```bash
git add src/tests/test_spadl_enrichments.py
git commit -m "test(spadl): boundary-F1 ≥ 0.85 against StatsBomb native possession_id"
```

---

## Phase 4 — Update DDL constants in `spadl_vaep.py`

### Task 4.1: Add 6 enrichment columns to `_SPADL_SCHEMA` and `action_id` + 6 enrichment to `_VAEP_SCHEMA`

**Files:**
- Modify: `src/ingestion/spadl_vaep.py:51-75`

- [ ] **Step 1: Read the current constants for reference**

Run: `grep -n "_SPADL_SCHEMA\|_VAEP_SCHEMA" src/ingestion/spadl_vaep.py | head -10`

Expected output identifies the constants at approximately line 51 and line 62.

- [ ] **Step 2: Update `_SPADL_SCHEMA` and `_VAEP_SCHEMA`**

Use Edit on `src/ingestion/spadl_vaep.py`:

Find:
```python
_SPADL_SCHEMA = (
    "game_id BIGINT, original_event_id STRING, period_id BIGINT, time_seconds DOUBLE, "
    "team_id BIGINT, player_id BIGINT, start_x DOUBLE, start_y DOUBLE, end_x DOUBLE, end_y DOUBLE, "
    "type_id BIGINT, result_id BIGINT, bodypart_id BIGINT, action_id BIGINT, "
    "competition_id BIGINT, season_id BIGINT, data_source STRING, _ingested_at TIMESTAMP, match_id BIGINT, "
    # Provider-namespaced StatsBomb-native fields surfaced via silly-kicks 1.5.0+
    # ``preserve_native`` kwarg on convert_to_actions. NULL for non-StatsBomb sources.
    "statsbomb_possession_id BIGINT, statsbomb_possession_team_id BIGINT, "
    "statsbomb_play_pattern STRING, statsbomb_under_pressure BOOLEAN"
)
_VAEP_TABLE = "vaep_action_values"
_VAEP_SCHEMA = (
    "game_id BIGINT, match_id BIGINT, original_event_id STRING, period_id BIGINT, "
    "time_seconds DOUBLE, team_id BIGINT, player_id BIGINT, start_x DOUBLE, start_y DOUBLE, "
    "end_x DOUBLE, end_y DOUBLE, type_id BIGINT, action_type STRING, result_id BIGINT, "
    "action_result STRING, bodypart_id BIGINT, bodypart STRING, offensive_value DOUBLE, "
    "defensive_value DOUBLE, vaep_value DOUBLE, competition_id BIGINT, season_id BIGINT, "
    "data_source STRING, _ingested_at TIMESTAMP, "
    # Provider-namespaced StatsBomb-native fields (carried through from spadl_actions).
    # NULL for non-StatsBomb sources. See ADR-011 dual-column window for `possession_team_*`
    # naming: gold mart resolves `possession_team_key` via dim_teams; legacy
    # `possession_team_id` retained until 2026-07-22 cutover.
    "statsbomb_possession_id BIGINT, statsbomb_possession_team_id BIGINT, "
    "statsbomb_play_pattern STRING, statsbomb_under_pressure BOOLEAN"
)
```

Replace with:
```python
_SPADL_SCHEMA = (
    "game_id BIGINT, original_event_id STRING, period_id BIGINT, time_seconds DOUBLE, "
    "team_id BIGINT, player_id BIGINT, start_x DOUBLE, start_y DOUBLE, end_x DOUBLE, end_y DOUBLE, "
    "type_id BIGINT, result_id BIGINT, bodypart_id BIGINT, action_id BIGINT, "
    "competition_id BIGINT, season_id BIGINT, data_source STRING, _ingested_at TIMESTAMP, match_id BIGINT, "
    # Provider-namespaced StatsBomb-native fields surfaced via silly-kicks 1.5.0+
    # ``preserve_native`` kwarg on convert_to_actions. NULL for non-StatsBomb sources.
    "statsbomb_possession_id BIGINT, statsbomb_possession_team_id BIGINT, "
    "statsbomb_play_pattern STRING, statsbomb_under_pressure BOOLEAN, "
    # LL2: 6 post-conversion enrichment columns from apply_spadl_enrichments.
    # add_possessions → possession_id_heuristic
    # add_gk_role → gk_role
    # add_pre_shot_gk_context → 4 columns. See ADR-016.
    "possession_id_heuristic BIGINT, gk_role STRING, "
    "gk_was_distributing BOOLEAN, gk_was_engaged BOOLEAN, "
    "gk_actions_in_possession BIGINT, defending_gk_player_id BIGINT"
)
_VAEP_TABLE = "vaep_action_values"
_VAEP_SCHEMA = (
    "game_id BIGINT, match_id BIGINT, original_event_id STRING, period_id BIGINT, "
    "time_seconds DOUBLE, team_id BIGINT, player_id BIGINT, start_x DOUBLE, start_y DOUBLE, "
    "end_x DOUBLE, end_y DOUBLE, type_id BIGINT, action_type STRING, result_id BIGINT, "
    "action_result STRING, bodypart_id BIGINT, bodypart STRING, offensive_value DOUBLE, "
    "defensive_value DOUBLE, vaep_value DOUBLE, competition_id BIGINT, season_id BIGINT, "
    "data_source STRING, _ingested_at TIMESTAMP, "
    # LL2: action_id surfaced through to vaep_action_values (was never carried
    # through pre-LL2 — bronze.spadl_actions.action_id existed but was 100% NULL).
    "action_id BIGINT, "
    # Provider-namespaced StatsBomb-native fields (carried through from spadl_actions).
    # NULL for non-StatsBomb sources.
    "statsbomb_possession_id BIGINT, statsbomb_possession_team_id BIGINT, "
    "statsbomb_play_pattern STRING, statsbomb_under_pressure BOOLEAN, "
    # LL2: 6 post-conversion enrichment columns. See ADR-016.
    "possession_id_heuristic BIGINT, gk_role STRING, "
    "gk_was_distributing BOOLEAN, gk_was_engaged BOOLEAN, "
    "gk_actions_in_possession BIGINT, defending_gk_player_id BIGINT"
)
```

- [ ] **Step 3: Verify lint + types still clean**

Run: `uv run ruff check src/ingestion/spadl_vaep.py && uv run pyright src/ingestion/spadl_vaep.py`

Expected: clean.

- [ ] **Step 4: Run existing writer parity tests — they should still pass**

Run: `uv run pytest src/tests/test_spadl_vaep_writer_parity.py -v`

Expected: All current tests PASS. The existing `test_statsbomb_struct_matches_spadl_ddl` checks that the StatsBomb StructType is a SUBSET of `_SPADL_SCHEMA` (writer columns present in DDL); adding more columns to DDL doesn't break that direction.

- [ ] **Step 5: Commit**

```bash
git add src/ingestion/spadl_vaep.py
git commit -m "feat(spadl): extend _SPADL_SCHEMA + _VAEP_SCHEMA with action_id + 6 LL2 enrichment columns"
```

---

## Phase 5 — StatsBomb writer parity tests + UDF update

### Task 5.1: Extend the StatsBomb writer parity test for the new columns

**Files:**
- Modify: `src/tests/test_spadl_vaep_writer_parity.py:53-87` (the `_build_statsbomb_spadl_struct` helper)
- Modify: `src/tests/test_spadl_vaep_writer_parity.py:111-141` (extend `test_spadl_ddl_includes_*` and `test_vaep_ddl_includes_*`)

- [ ] **Step 1: Extend the StatsBomb StructType replay to include `action_id` + 6 enrichment columns**

Use Edit on `src/tests/test_spadl_vaep_writer_parity.py`:

Find:
```python
    return StructType(
        [
            StructField("game_id", LongType()),
            StructField("match_id", LongType()),
            StructField("original_event_id", StringType()),
            StructField("period_id", LongType()),
            StructField("time_seconds", DoubleType()),
            StructField("team_id", LongType()),
            StructField("player_id", LongType()),
            StructField("start_x", DoubleType()),
            StructField("start_y", DoubleType()),
            StructField("end_x", DoubleType()),
            StructField("end_y", DoubleType()),
            StructField("type_id", LongType()),
            StructField("result_id", LongType()),
            StructField("bodypart_id", LongType()),
            StructField("competition_id", LongType()),
            StructField("season_id", LongType()),
            StructField("data_source", StringType()),
            StructField("statsbomb_possession_id", LongType()),
            StructField("statsbomb_possession_team_id", LongType()),
            StructField("statsbomb_play_pattern", StringType()),
            StructField("statsbomb_under_pressure", BooleanType()),
        ]
    )
```

Replace with:
```python
    return StructType(
        [
            StructField("game_id", LongType()),
            StructField("match_id", LongType()),
            StructField("original_event_id", StringType()),
            StructField("period_id", LongType()),
            StructField("time_seconds", DoubleType()),
            StructField("team_id", LongType()),
            StructField("player_id", LongType()),
            StructField("start_x", DoubleType()),
            StructField("start_y", DoubleType()),
            StructField("end_x", DoubleType()),
            StructField("end_y", DoubleType()),
            StructField("type_id", LongType()),
            StructField("result_id", LongType()),
            StructField("bodypart_id", LongType()),
            StructField("action_id", LongType()),  # LL2: surfaced from convert_to_actions
            StructField("competition_id", LongType()),
            StructField("season_id", LongType()),
            StructField("data_source", StringType()),
            StructField("statsbomb_possession_id", LongType()),
            StructField("statsbomb_possession_team_id", LongType()),
            StructField("statsbomb_play_pattern", StringType()),
            StructField("statsbomb_under_pressure", BooleanType()),
            # LL2: 6 post-conversion enrichment columns
            StructField("possession_id_heuristic", LongType()),
            StructField("gk_role", StringType()),
            StructField("gk_was_distributing", BooleanType()),
            StructField("gk_was_engaged", BooleanType()),
            StructField("gk_actions_in_possession", LongType()),
            StructField("defending_gk_player_id", LongType()),
        ]
    )
```

- [ ] **Step 2: Add a test asserting the 6 enrichment columns are in `_SPADL_SCHEMA`**

In the same file, append after the existing `test_vaep_ddl_includes_new_preserve_native_columns` method (within the `TestSpadlVaepWriterDdlParity` class):

```python
    def test_spadl_ddl_includes_action_id(self) -> None:
        """LL2: action_id must be declared in _SPADL_SCHEMA (currently is — test
        guards against accidental removal)."""
        from ingestion import spadl_vaep

        ddl = _parse_ddl(spadl_vaep._SPADL_SCHEMA)
        assert "action_id" in ddl, "_SPADL_SCHEMA missing action_id (LL2)"

    def test_spadl_ddl_includes_enrichment_columns(self) -> None:
        """LL2: 6 enrichment columns from apply_spadl_enrichments must be in _SPADL_SCHEMA."""
        from ingestion import spadl_vaep

        ddl = _parse_ddl(spadl_vaep._SPADL_SCHEMA)
        for col in (
            "possession_id_heuristic",
            "gk_role",
            "gk_was_distributing",
            "gk_was_engaged",
            "gk_actions_in_possession",
            "defending_gk_player_id",
        ):
            assert col in ddl, f"_SPADL_SCHEMA missing LL2 enrichment column {col!r}"

    def test_vaep_ddl_includes_action_id(self) -> None:
        """LL2: action_id surfaced through to vaep_action_values (was 100% NULL pre-LL2)."""
        from ingestion import spadl_vaep

        ddl = _parse_ddl(spadl_vaep._VAEP_SCHEMA)
        assert "action_id" in ddl, "_VAEP_SCHEMA missing action_id (LL2 surfaces it)"

    def test_vaep_ddl_includes_enrichment_columns(self) -> None:
        """LL2: 6 enrichment columns must propagate through to vaep_action_values."""
        from ingestion import spadl_vaep

        ddl = _parse_ddl(spadl_vaep._VAEP_SCHEMA)
        for col in (
            "possession_id_heuristic",
            "gk_role",
            "gk_was_distributing",
            "gk_was_engaged",
            "gk_actions_in_possession",
            "defending_gk_player_id",
        ):
            assert col in ddl, f"_VAEP_SCHEMA missing LL2 enrichment column {col!r}"

    def test_spadl_dtypes_match_vaep_dtypes_for_enrichment_columns(self) -> None:
        """LL2: type parity for enrichment columns across both DDLs (mirrors the existing
        statsbomb_* parity test)."""
        from ingestion import spadl_vaep

        spadl = _parse_ddl(spadl_vaep._SPADL_SCHEMA)
        vaep = _parse_ddl(spadl_vaep._VAEP_SCHEMA)
        for col in (
            "action_id",
            "possession_id_heuristic",
            "gk_role",
            "gk_was_distributing",
            "gk_was_engaged",
            "gk_actions_in_possession",
            "defending_gk_player_id",
        ):
            assert spadl[col] == vaep[col], (
                f"LL2 column {col!r} type drift: _SPADL_SCHEMA={spadl[col]!r} vs _VAEP_SCHEMA={vaep[col]!r}"
            )
```

- [ ] **Step 3: Run tests to verify failure**

Run: `uv run pytest src/tests/test_spadl_vaep_writer_parity.py::TestSpadlVaepWriterDdlParity -v`

Expected: `test_statsbomb_struct_matches_spadl_ddl` FAILS — the static StructType replay now includes the 7 new columns, but the actual UDF in `spadl_conversion.py` does not. The 4 other DDL-only tests (`test_spadl_ddl_includes_*`, `test_vaep_ddl_includes_*`) PASS because Phase 4 added the columns to the DDL constants.

### Task 5.2: Update the StatsBomb UDF to surface `action_id` + 6 enrichment columns

**Files:**
- Modify: `src/ingestion/spadl_conversion.py:104-199` (the `_make_sb_spadl_udf` function)
- Modify: `src/ingestion/spadl_conversion.py:286-312` (the `spadl_schema` StructType inside `_convert_statsbomb_from_bronze`)

- [ ] **Step 1: Update `_make_sb_spadl_udf` — surface action_id + call apply_spadl_enrichments + project new columns**

Use Edit on `src/ingestion/spadl_conversion.py`:

Find the `_spadl_cols` list inside `_make_sb_spadl_udf`:
```python
        _spadl_cols = _pd.Index(
            [
                "game_id",
                "match_id",
                "original_event_id",
                "period_id",
                "time_seconds",
                "team_id",
                "player_id",
                "start_x",
                "start_y",
                "end_x",
                "end_y",
                "type_id",
                "result_id",
                "bodypart_id",
                "competition_id",
                "season_id",
                "data_source",
                # Provider-namespaced StatsBomb-native fields (silly-kicks 1.5.0+).
                "statsbomb_possession_id",
                "statsbomb_possession_team_id",
                "statsbomb_play_pattern",
                "statsbomb_under_pressure",
            ]
        )
```

Replace with:
```python
        _spadl_cols = _pd.Index(
            [
                "game_id",
                "match_id",
                "original_event_id",
                "period_id",
                "time_seconds",
                "team_id",
                "player_id",
                "start_x",
                "start_y",
                "end_x",
                "end_y",
                "type_id",
                "result_id",
                "bodypart_id",
                "action_id",  # LL2: surfaced from silly-kicks convert_to_actions output
                "competition_id",
                "season_id",
                "data_source",
                # Provider-namespaced StatsBomb-native fields (silly-kicks 1.5.0+).
                "statsbomb_possession_id",
                "statsbomb_possession_team_id",
                "statsbomb_play_pattern",
                "statsbomb_under_pressure",
                # LL2: 6 post-conversion enrichment columns from apply_spadl_enrichments.
                "possession_id_heuristic",
                "gk_role",
                "gk_was_distributing",
                "gk_was_engaged",
                "gk_actions_in_possession",
                "defending_gk_player_id",
            ]
        )
```

- [ ] **Step 2: Insert `apply_spadl_enrichments` call after the existing rename block**

In the same UDF, find:
```python
        # Provider-namespace the preserved fields. silly-kicks returns them with
        # their input names (``possession``, ``possession_team_id``, etc.); the
        # bronze + mart conventions use ``statsbomb_*`` per the multi-provider
        # symmetry argument (Wyscout/IDSSE/SkillCorner produce NULL here).
        actions = actions.rename(
            columns={
                "possession": "statsbomb_possession_id",
                "possession_team_id": "statsbomb_possession_team_id",
                "play_pattern": "statsbomb_play_pattern",
                "under_pressure": "statsbomb_under_pressure",
            }
        )

        # Cast original_event_id to str for Spark/PyArrow serialization
        # (silly-kicks outputs object dtype; Spark needs explicit string)
        actions["original_event_id"] = actions["original_event_id"].astype(str)
```

Replace with:
```python
        # Provider-namespace the preserved fields. silly-kicks returns them with
        # their input names (``possession``, ``possession_team_id``, etc.); the
        # bronze + mart conventions use ``statsbomb_*`` per the multi-provider
        # symmetry argument (Wyscout/IDSSE/SkillCorner produce NULL here).
        actions = actions.rename(
            columns={
                "possession": "statsbomb_possession_id",
                "possession_team_id": "statsbomb_possession_team_id",
                "play_pattern": "statsbomb_play_pattern",
                "under_pressure": "statsbomb_under_pressure",
            }
        )

        # LL2: provider-agnostic post-conversion enrichments. Adds 6 columns:
        # possession_id_heuristic, gk_role, gk_was_distributing, gk_was_engaged,
        # gk_actions_in_possession, defending_gk_player_id. See ADR-016.
        from ingestion.spadl_enrichments import apply_spadl_enrichments as _enrich

        actions = _enrich(actions, source="statsbomb")

        # Cast original_event_id to str for Spark/PyArrow serialization
        # (silly-kicks outputs object dtype; Spark needs explicit string)
        actions["original_event_id"] = actions["original_event_id"].astype(str)
```

- [ ] **Step 3: Cast new BIGINT columns to nullable Int64**

In the same UDF, find:
```python
        actions["statsbomb_possession_id"] = actions["statsbomb_possession_id"].astype("Int64")
        actions["statsbomb_possession_team_id"] = actions["statsbomb_possession_team_id"].astype("Int64")
        actions["statsbomb_under_pressure"] = actions["statsbomb_under_pressure"].astype("boolean")
        # statsbomb_play_pattern stays object (string with NaN) — fine for StringType.
```

Replace with:
```python
        actions["statsbomb_possession_id"] = actions["statsbomb_possession_id"].astype("Int64")
        actions["statsbomb_possession_team_id"] = actions["statsbomb_possession_team_id"].astype("Int64")
        actions["statsbomb_under_pressure"] = actions["statsbomb_under_pressure"].astype("boolean")
        # statsbomb_play_pattern stays object (string with NaN) — fine for StringType.

        # LL2: cast enrichment columns to nullable dtypes for clean PyArrow conversion.
        # action_id and possession_id_heuristic come back as int64 from silly-kicks but
        # synthetic dribble rows can introduce NaN — use Int64 to be safe.
        actions["action_id"] = actions["action_id"].astype("Int64")
        actions["possession_id_heuristic"] = actions["possession_id_heuristic"].astype("Int64")
        # gk_role is pd.Categorical from silly-kicks — convert to object (string) for StringType.
        actions["gk_role"] = actions["gk_role"].astype("object")
        # GK context booleans default to False on non-shot rows (silly-kicks contract).
        actions["gk_was_distributing"] = actions["gk_was_distributing"].astype("boolean")
        actions["gk_was_engaged"] = actions["gk_was_engaged"].astype("boolean")
        actions["gk_actions_in_possession"] = actions["gk_actions_in_possession"].astype("Int64")
        # defending_gk_player_id comes back as float64-with-NaN from silly-kicks; convert to Int64.
        actions["defending_gk_player_id"] = actions["defending_gk_player_id"].astype("Int64")
```

- [ ] **Step 4: Update the `spadl_schema` StructType inside `_convert_statsbomb_from_bronze`**

Find:
```python
    spadl_schema = StructType(
        [
            StructField("game_id", LongType()),
            StructField("match_id", LongType()),
            StructField("original_event_id", StringType()),
            StructField("period_id", LongType()),
            StructField("time_seconds", DoubleType()),
            StructField("team_id", LongType()),
            StructField("player_id", LongType()),
            StructField("start_x", DoubleType()),
            StructField("start_y", DoubleType()),
            StructField("end_x", DoubleType()),
            StructField("end_y", DoubleType()),
            StructField("type_id", LongType()),
            StructField("result_id", LongType()),
            StructField("bodypart_id", LongType()),
            StructField("competition_id", LongType()),
            StructField("season_id", LongType()),
            StructField("data_source", StringType()),
            # Provider-namespaced StatsBomb-native fields (silly-kicks 1.5.0+).
            # Wyscout / IDSSE / SkillCorner produce NULL.
            StructField("statsbomb_possession_id", LongType()),
            StructField("statsbomb_possession_team_id", LongType()),
            StructField("statsbomb_play_pattern", StringType()),
            StructField("statsbomb_under_pressure", BooleanType()),
        ]
    )
```

Replace with:
```python
    spadl_schema = StructType(
        [
            StructField("game_id", LongType()),
            StructField("match_id", LongType()),
            StructField("original_event_id", StringType()),
            StructField("period_id", LongType()),
            StructField("time_seconds", DoubleType()),
            StructField("team_id", LongType()),
            StructField("player_id", LongType()),
            StructField("start_x", DoubleType()),
            StructField("start_y", DoubleType()),
            StructField("end_x", DoubleType()),
            StructField("end_y", DoubleType()),
            StructField("type_id", LongType()),
            StructField("result_id", LongType()),
            StructField("bodypart_id", LongType()),
            StructField("action_id", LongType()),  # LL2: surfaced from silly-kicks
            StructField("competition_id", LongType()),
            StructField("season_id", LongType()),
            StructField("data_source", StringType()),
            # Provider-namespaced StatsBomb-native fields (silly-kicks 1.5.0+).
            # Wyscout / IDSSE / SkillCorner produce NULL.
            StructField("statsbomb_possession_id", LongType()),
            StructField("statsbomb_possession_team_id", LongType()),
            StructField("statsbomb_play_pattern", StringType()),
            StructField("statsbomb_under_pressure", BooleanType()),
            # LL2: 6 post-conversion enrichment columns from apply_spadl_enrichments.
            StructField("possession_id_heuristic", LongType()),
            StructField("gk_role", StringType()),
            StructField("gk_was_distributing", BooleanType()),
            StructField("gk_was_engaged", BooleanType()),
            StructField("gk_actions_in_possession", LongType()),
            StructField("defending_gk_player_id", LongType()),
        ]
    )
```

- [ ] **Step 5: Run the writer parity tests**

Run: `uv run pytest src/tests/test_spadl_vaep_writer_parity.py::TestSpadlVaepWriterDdlParity::test_statsbomb_struct_matches_spadl_ddl -v`

Expected: PASS.

- [ ] **Step 6: Run all updated tests**

Run: `uv run pytest src/tests/test_spadl_vaep_writer_parity.py -v && uv run pytest src/tests/test_spadl_enrichments.py -v && uv run ruff check src/ingestion/spadl_conversion.py && uv run pyright src/ingestion/spadl_conversion.py`

Expected: all PASS, lint + types clean.

- [ ] **Step 7: Commit**

```bash
git add src/ingestion/spadl_conversion.py src/tests/test_spadl_vaep_writer_parity.py
git commit -m "feat(spadl): StatsBomb UDF surfaces action_id + calls apply_spadl_enrichments + writer parity tests"
```

---

## Phase 6 — Wyscout writer parity tests + UDF update

### Task 6.1: Add Wyscout writer parity test (was untested in LL1) and update Wyscout UDF

**Files:**
- Modify: `src/tests/test_spadl_vaep_writer_parity.py` (add `_build_wyscout_spadl_struct` helper + test)
- Modify: `src/ingestion/spadl_conversion.py:355-429` (the `_make_ws_spadl_udf` function)
- Modify: `src/ingestion/spadl_conversion.py:541-565` (the `spadl_schema` inside `_convert_wyscout_from_bronze`)

- [ ] **Step 1: Add Wyscout struct helper + parity test**

Append to `src/tests/test_spadl_vaep_writer_parity.py` (after `_build_statsbomb_spadl_struct`):

```python
def _build_wyscout_spadl_struct():  # type: ignore[no-untyped-def]
    """Replay the Wyscout applyInPandas StructType in spadl_conversion.py."""
    pyspark_types = pytest.importorskip("pyspark.sql.types")
    BooleanType = pyspark_types.BooleanType  # noqa: N806
    DoubleType = pyspark_types.DoubleType  # noqa: N806
    LongType = pyspark_types.LongType  # noqa: N806
    StringType = pyspark_types.StringType  # noqa: N806
    StructField = pyspark_types.StructField  # noqa: N806
    StructType = pyspark_types.StructType  # noqa: N806

    return StructType(
        [
            StructField("game_id", LongType()),
            StructField("match_id", LongType()),
            StructField("original_event_id", StringType()),
            StructField("period_id", LongType()),
            StructField("time_seconds", DoubleType()),
            StructField("team_id", LongType()),
            StructField("player_id", LongType()),
            StructField("start_x", DoubleType()),
            StructField("start_y", DoubleType()),
            StructField("end_x", DoubleType()),
            StructField("end_y", DoubleType()),
            StructField("type_id", LongType()),
            StructField("result_id", LongType()),
            StructField("bodypart_id", LongType()),
            StructField("action_id", LongType()),
            StructField("competition_id", LongType()),
            StructField("season_id", LongType()),
            StructField("data_source", StringType()),
            StructField("statsbomb_possession_id", LongType()),
            StructField("statsbomb_possession_team_id", LongType()),
            StructField("statsbomb_play_pattern", StringType()),
            StructField("statsbomb_under_pressure", BooleanType()),
            StructField("possession_id_heuristic", LongType()),
            StructField("gk_role", StringType()),
            StructField("gk_was_distributing", BooleanType()),
            StructField("gk_was_engaged", BooleanType()),
            StructField("gk_actions_in_possession", LongType()),
            StructField("defending_gk_player_id", LongType()),
        ]
    )
```

In the same file, add a new test method to `TestSpadlVaepWriterDdlParity`:

```python
    def test_wyscout_struct_matches_spadl_ddl(self) -> None:
        """LL2: Wyscout writer parity (was untested in LL1 — gap closed)."""
        from ingestion import spadl_vaep

        ddl = _parse_ddl(spadl_vaep._SPADL_SCHEMA)
        out = {f.name: f.dataType.simpleString() for f in _build_wyscout_spadl_struct().fields}

        missing = [c for c in out if c not in ddl]
        assert not missing, f"Wyscout writer emits columns absent from _SPADL_SCHEMA: {missing}"

        mismatched = {c: (out[c], ddl[c]) for c in out if out[c] != ddl[c]}
        assert not mismatched, f"Wyscout writer/DDL type drift {mismatched}"
```

- [ ] **Step 2: Run tests to verify failure**

Run: `uv run pytest src/tests/test_spadl_vaep_writer_parity.py::TestSpadlVaepWriterDdlParity::test_wyscout_struct_matches_spadl_ddl -v`

Expected: FAIL — Wyscout's actual UDF doesn't yet emit `action_id` + 6 enrichment columns.

- [ ] **Step 3: Update `_make_ws_spadl_udf` similarly to StatsBomb**

In `src/ingestion/spadl_conversion.py`, find the Wyscout `_spadl_cols` list:
```python
        _spadl_cols = _pd.Index(
            [
                "game_id",
                "match_id",
                "original_event_id",
                "period_id",
                "time_seconds",
                "team_id",
                "player_id",
                "start_x",
                "start_y",
                "end_x",
                "end_y",
                "type_id",
                "result_id",
                "bodypart_id",
                "competition_id",
                "season_id",
                "data_source",
                # Multi-source schema parity: Wyscout has no analogues to the
                # StatsBomb-native ``possession`` / ``play_pattern`` /
                # ``under_pressure`` fields, so these columns are NULL on the
                # Wyscout code path.
                "statsbomb_possession_id",
                "statsbomb_possession_team_id",
                "statsbomb_play_pattern",
                "statsbomb_under_pressure",
            ]
        )
```

Replace with:
```python
        _spadl_cols = _pd.Index(
            [
                "game_id",
                "match_id",
                "original_event_id",
                "period_id",
                "time_seconds",
                "team_id",
                "player_id",
                "start_x",
                "start_y",
                "end_x",
                "end_y",
                "type_id",
                "result_id",
                "bodypart_id",
                "action_id",  # LL2: surfaced from silly-kicks convert_to_actions
                "competition_id",
                "season_id",
                "data_source",
                # Multi-source schema parity: Wyscout has no analogues to the
                # StatsBomb-native ``possession`` / ``play_pattern`` /
                # ``under_pressure`` fields, so these columns are NULL on the
                # Wyscout code path.
                "statsbomb_possession_id",
                "statsbomb_possession_team_id",
                "statsbomb_play_pattern",
                "statsbomb_under_pressure",
                # LL2: 6 post-conversion enrichment columns (provider-agnostic — populated for Wyscout).
                "possession_id_heuristic",
                "gk_role",
                "gk_was_distributing",
                "gk_was_engaged",
                "gk_actions_in_possession",
                "defending_gk_player_id",
            ]
        )
```

In the same UDF, find:
```python
        actions["match_id"] = match_id
        actions["competition_id"] = competition_id
        actions["season_id"] = season_id
        actions["data_source"] = "wyscout"

        # Cast original_event_id to str for Spark/PyArrow serialization
        actions["original_event_id"] = actions["original_event_id"].astype(str)

        # NULL-fill the StatsBomb-namespaced fields for multi-source parity.
```

Replace with:
```python
        actions["match_id"] = match_id
        actions["competition_id"] = competition_id
        actions["season_id"] = season_id
        actions["data_source"] = "wyscout"

        # LL2: provider-agnostic post-conversion enrichments (populated for Wyscout).
        from ingestion.spadl_enrichments import apply_spadl_enrichments as _enrich

        actions = _enrich(actions, source="wyscout")

        # Cast original_event_id to str for Spark/PyArrow serialization
        actions["original_event_id"] = actions["original_event_id"].astype(str)

        # NULL-fill the StatsBomb-namespaced fields for multi-source parity.
```

In the same UDF, find:
```python
        n = len(actions)
        actions["statsbomb_possession_id"] = _pd.array([_pd.NA] * n, dtype="Int64")
        actions["statsbomb_possession_team_id"] = _pd.array([_pd.NA] * n, dtype="Int64")
        actions["statsbomb_play_pattern"] = _pd.array([_pd.NA] * n, dtype="object")
        actions["statsbomb_under_pressure"] = _pd.array([_pd.NA] * n, dtype="boolean")
```

Replace with:
```python
        n = len(actions)
        actions["statsbomb_possession_id"] = _pd.array([_pd.NA] * n, dtype="Int64")
        actions["statsbomb_possession_team_id"] = _pd.array([_pd.NA] * n, dtype="Int64")
        actions["statsbomb_play_pattern"] = _pd.array([_pd.NA] * n, dtype="object")
        actions["statsbomb_under_pressure"] = _pd.array([_pd.NA] * n, dtype="boolean")

        # LL2: cast enrichment columns to nullable dtypes (same pattern as StatsBomb path).
        actions["action_id"] = actions["action_id"].astype("Int64")
        actions["possession_id_heuristic"] = actions["possession_id_heuristic"].astype("Int64")
        actions["gk_role"] = actions["gk_role"].astype("object")
        actions["gk_was_distributing"] = actions["gk_was_distributing"].astype("boolean")
        actions["gk_was_engaged"] = actions["gk_was_engaged"].astype("boolean")
        actions["gk_actions_in_possession"] = actions["gk_actions_in_possession"].astype("Int64")
        actions["defending_gk_player_id"] = actions["defending_gk_player_id"].astype("Int64")
```

- [ ] **Step 4: Update the Wyscout `spadl_schema` StructType in `_convert_wyscout_from_bronze`**

Find:
```python
    spadl_schema = StructType(
        [
            StructField("game_id", LongType()),
            StructField("match_id", LongType()),
            StructField("original_event_id", StringType()),
            StructField("period_id", LongType()),
            StructField("time_seconds", DoubleType()),
            StructField("team_id", LongType()),
            StructField("player_id", LongType()),
            StructField("start_x", DoubleType()),
            StructField("start_y", DoubleType()),
            StructField("end_x", DoubleType()),
            StructField("end_y", DoubleType()),
            StructField("type_id", LongType()),
            StructField("result_id", LongType()),
            StructField("bodypart_id", LongType()),
            StructField("competition_id", LongType()),
            StructField("season_id", LongType()),
            StructField("data_source", StringType()),
            StructField("statsbomb_possession_id", LongType()),
            StructField("statsbomb_possession_team_id", LongType()),
            StructField("statsbomb_play_pattern", StringType()),
            StructField("statsbomb_under_pressure", BooleanType()),
        ]
    )
```

Replace with:
```python
    spadl_schema = StructType(
        [
            StructField("game_id", LongType()),
            StructField("match_id", LongType()),
            StructField("original_event_id", StringType()),
            StructField("period_id", LongType()),
            StructField("time_seconds", DoubleType()),
            StructField("team_id", LongType()),
            StructField("player_id", LongType()),
            StructField("start_x", DoubleType()),
            StructField("start_y", DoubleType()),
            StructField("end_x", DoubleType()),
            StructField("end_y", DoubleType()),
            StructField("type_id", LongType()),
            StructField("result_id", LongType()),
            StructField("bodypart_id", LongType()),
            StructField("action_id", LongType()),  # LL2
            StructField("competition_id", LongType()),
            StructField("season_id", LongType()),
            StructField("data_source", StringType()),
            StructField("statsbomb_possession_id", LongType()),
            StructField("statsbomb_possession_team_id", LongType()),
            StructField("statsbomb_play_pattern", StringType()),
            StructField("statsbomb_under_pressure", BooleanType()),
            # LL2: 6 enrichment columns
            StructField("possession_id_heuristic", LongType()),
            StructField("gk_role", StringType()),
            StructField("gk_was_distributing", BooleanType()),
            StructField("gk_was_engaged", BooleanType()),
            StructField("gk_actions_in_possession", LongType()),
            StructField("defending_gk_player_id", LongType()),
        ]
    )
```

- [ ] **Step 5: Run the Wyscout parity test**

Run: `uv run pytest src/tests/test_spadl_vaep_writer_parity.py::TestSpadlVaepWriterDdlParity::test_wyscout_struct_matches_spadl_ddl -v`

Expected: PASS.

- [ ] **Step 6: Run all parity tests + lint + types**

Run: `uv run pytest src/tests/test_spadl_vaep_writer_parity.py -v && uv run ruff check src/ingestion/spadl_conversion.py && uv run pyright src/ingestion/spadl_conversion.py`

Expected: all PASS, lint + types clean.

- [ ] **Step 7: Commit**

```bash
git add src/ingestion/spadl_conversion.py src/tests/test_spadl_vaep_writer_parity.py
git commit -m "feat(spadl): Wyscout UDF surfaces action_id + calls apply_spadl_enrichments + writer parity test"
```

---

## Phase 7 — VAEP scoring UDF parity (closes LL1 latent bug class)

### Task 7.1: Add the VAEP scoring UDF parity test

**Files:**
- Modify: `src/tests/test_spadl_vaep_writer_parity.py` (add `_build_vaep_scoring_struct` + test class)

- [ ] **Step 1: Append the VAEP scoring StructType replay + test class**

Append to `src/tests/test_spadl_vaep_writer_parity.py`:

```python
def _build_vaep_scoring_struct():  # type: ignore[no-untyped-def]
    """Replay the VAEP scoring applyInPandas StructType from spadl_vaep._make_scoring_udf.

    LL2: This test closes the LL1 latent-bug class — the original LL1 vaep_schema
    omitted statsbomb_* columns, which silently dropped them at the applyInPandas
    boundary. 0 of 7,151,510 StatsBomb rows ended up with non-NULL
    statsbomb_possession_id post-LL1. This struct must agree column-for-column
    with _VAEP_SCHEMA so the same drift cannot recur.
    """
    pyspark_types = pytest.importorskip("pyspark.sql.types")
    BooleanType = pyspark_types.BooleanType  # noqa: N806
    DoubleType = pyspark_types.DoubleType  # noqa: N806
    LongType = pyspark_types.LongType  # noqa: N806
    StringType = pyspark_types.StringType  # noqa: N806
    StructField = pyspark_types.StructField  # noqa: N806
    StructType = pyspark_types.StructType  # noqa: N806

    return StructType(
        [
            StructField("game_id", LongType()),
            StructField("match_id", LongType()),
            StructField("original_event_id", StringType()),
            StructField("period_id", LongType()),
            StructField("time_seconds", DoubleType()),
            StructField("team_id", LongType()),
            StructField("player_id", LongType()),
            StructField("start_x", DoubleType()),
            StructField("start_y", DoubleType()),
            StructField("end_x", DoubleType()),
            StructField("end_y", DoubleType()),
            StructField("type_id", LongType()),
            StructField("action_type", StringType()),
            StructField("result_id", LongType()),
            StructField("action_result", StringType()),
            StructField("bodypart_id", LongType()),
            StructField("bodypart", StringType()),
            StructField("offensive_value", DoubleType()),
            StructField("defensive_value", DoubleType()),
            StructField("vaep_value", DoubleType()),
            StructField("competition_id", LongType()),
            StructField("season_id", LongType()),
            StructField("data_source", StringType()),
            # LL2: action_id surfaced through to vaep_action_values
            StructField("action_id", LongType()),
            # PR-LL1 statsbomb_* (closes LL1 latent bug — must be in vaep_schema)
            StructField("statsbomb_possession_id", LongType()),
            StructField("statsbomb_possession_team_id", LongType()),
            StructField("statsbomb_play_pattern", StringType()),
            StructField("statsbomb_under_pressure", BooleanType()),
            # LL2: 6 enrichment columns
            StructField("possession_id_heuristic", LongType()),
            StructField("gk_role", StringType()),
            StructField("gk_was_distributing", BooleanType()),
            StructField("gk_was_engaged", BooleanType()),
            StructField("gk_actions_in_possession", LongType()),
            StructField("defending_gk_player_id", LongType()),
        ]
    )


class TestVaepScoringWriterDdlParity:
    """spadl_vaep._make_scoring_udf vaep_schema must match _VAEP_SCHEMA DDL.

    LL2: closes the LL1 latent-bug class. The original LL1 release shipped
    _VAEP_SCHEMA with 4 statsbomb_* columns + DDL-side ALTERed
    bronze.vaep_action_values to add them, but the actual applyInPandas
    StructType inside _make_scoring_udf did not include statsbomb_*. Spark
    silently dropped them on every write — 0 of 7M rows ended up populated.
    This class makes the failure visible at unit-test time.
    """

    def test_vaep_scoring_struct_matches_vaep_ddl(self) -> None:
        from ingestion import spadl_vaep

        ddl = _parse_ddl(spadl_vaep._VAEP_SCHEMA)
        out = {f.name: f.dataType.simpleString() for f in _build_vaep_scoring_struct().fields}

        missing = [c for c in out if c not in ddl]
        assert not missing, (
            f"VAEP scoring writer emits columns absent from _VAEP_SCHEMA: {missing}. "
            "DELTA_FAILED_TO_MERGE_FIELDS will fire on next replaceWhere write."
        )

        ddl_only = [c for c in ddl if c not in out and c != "_ingested_at"]
        assert not ddl_only, (
            f"_VAEP_SCHEMA declares columns absent from VAEP scoring writer: {ddl_only}. "
            "These columns will be silently NULL-filled on every write — closing this gap is "
            "exactly the LL1 latent-bug class. Add them to vaep_schema in spadl_vaep._make_scoring_udf."
        )

        mismatched = {c: (out[c], ddl[c]) for c in out if out[c] != ddl[c]}
        assert not mismatched, f"VAEP scoring writer/DDL type drift {mismatched}"
```

- [ ] **Step 2: Run test to verify failure**

Run: `uv run pytest src/tests/test_spadl_vaep_writer_parity.py::TestVaepScoringWriterDdlParity -v`

Expected: FAIL. The current `vaep_schema` in `_make_scoring_udf` is missing `action_id`, the 4 statsbomb_*, AND the 6 enrichment columns relative to `_VAEP_SCHEMA`.

### Task 7.2: Update `_make_scoring_udf` to project all required columns + update `vaep_schema`

**Files:**
- Modify: `src/ingestion/spadl_vaep.py:274-440` (the `_make_scoring_udf` function)
- Modify: `src/ingestion/spadl_vaep.py:534-559` (the `vaep_schema` StructType in `run_pipeline`)

- [ ] **Step 1: Update `_output_cols` projection in `_make_scoring_udf`**

Find:
```python
        _output_cols = _pd.Index(
            [
                "game_id",
                "match_id",
                "original_event_id",
                "period_id",
                "time_seconds",
                "team_id",
                "player_id",
                "start_x",
                "start_y",
                "end_x",
                "end_y",
                "type_id",
                "action_type",
                "result_id",
                "action_result",
                "bodypart_id",
                "bodypart",
                "offensive_value",
                "defensive_value",
                "vaep_value",
                "competition_id",
                "season_id",
                "data_source",
                # Provider-namespaced StatsBomb-native fields carried through from
                # spadl_actions. NULL on Wyscout / IDSSE / SkillCorner code paths.
                "statsbomb_possession_id",
                "statsbomb_possession_team_id",
                "statsbomb_play_pattern",
                "statsbomb_under_pressure",
            ]
        )
```

Replace with:
```python
        _output_cols = _pd.Index(
            [
                "game_id",
                "match_id",
                "original_event_id",
                "period_id",
                "time_seconds",
                "team_id",
                "player_id",
                "start_x",
                "start_y",
                "end_x",
                "end_y",
                "type_id",
                "action_type",
                "result_id",
                "action_result",
                "bodypart_id",
                "bodypart",
                "offensive_value",
                "defensive_value",
                "vaep_value",
                "competition_id",
                "season_id",
                "data_source",
                # LL2: action_id surfaced through (was 100% NULL pre-LL2).
                "action_id",
                # Provider-namespaced StatsBomb-native fields carried through from
                # spadl_actions. NULL on Wyscout / IDSSE / Metrica code paths.
                "statsbomb_possession_id",
                "statsbomb_possession_team_id",
                "statsbomb_play_pattern",
                "statsbomb_under_pressure",
                # LL2: 6 post-conversion enrichment columns from apply_spadl_enrichments.
                "possession_id_heuristic",
                "gk_role",
                "gk_was_distributing",
                "gk_was_engaged",
                "gk_actions_in_possession",
                "defending_gk_player_id",
            ]
        )
```

- [ ] **Step 2: Update the per-game projection list inside the UDF**

Find inside `_make_scoring_udf`:
```python
                game_out = _pd.DataFrame(
                    game_actions[
                        [
                            c
                            for c in [
                                "game_id",
                                "match_id",
                                "original_event_id",
                                "period_id",
                                "time_seconds",
                                "team_id",
                                "player_id",
                                "start_x",
                                "start_y",
                                "end_x",
                                "end_y",
                                "type_id",
                                "type_name",
                                "result_id",
                                "result_name",
                                "bodypart_id",
                                "bodypart_name",
                                # Carry through provider-namespaced StatsBomb-native
                                # fields (NULL for non-StatsBomb sources). The ``c in
                                # game_actions.columns`` guard above tolerates the
                                # Wyscout-only path where these columns may be absent
                                # from the per-game frame; the post-concat schema
                                # alignment fills them as NULL.
                                "statsbomb_possession_id",
                                "statsbomb_possession_team_id",
                                "statsbomb_play_pattern",
                                "statsbomb_under_pressure",
                            ]
                            if c in game_actions.columns
                        ]
                    ].copy()
                )
```

Replace with:
```python
                game_out = _pd.DataFrame(
                    game_actions[
                        [
                            c
                            for c in [
                                "game_id",
                                "match_id",
                                "original_event_id",
                                "period_id",
                                "time_seconds",
                                "team_id",
                                "player_id",
                                "start_x",
                                "start_y",
                                "end_x",
                                "end_y",
                                "type_id",
                                "type_name",
                                "result_id",
                                "result_name",
                                "bodypart_id",
                                "bodypart_name",
                                # LL2: action_id carried through from spadl_actions.
                                "action_id",
                                # Carry through provider-namespaced StatsBomb-native
                                # fields (NULL for non-StatsBomb sources).
                                "statsbomb_possession_id",
                                "statsbomb_possession_team_id",
                                "statsbomb_play_pattern",
                                "statsbomb_under_pressure",
                                # LL2: 6 enrichment columns (populated for ALL sources).
                                "possession_id_heuristic",
                                "gk_role",
                                "gk_was_distributing",
                                "gk_was_engaged",
                                "gk_actions_in_possession",
                                "defending_gk_player_id",
                            ]
                            if c in game_actions.columns
                        ]
                    ].copy()
                )
```

- [ ] **Step 3: Update the `vaep_schema` StructType in `run_pipeline`**

Find:
```python
    vaep_schema = StructType(
        [
            StructField("game_id", LongType()),
            StructField("match_id", LongType()),
            StructField("original_event_id", StringType()),
            StructField("period_id", LongType()),
            StructField("time_seconds", DoubleType()),
            StructField("team_id", LongType()),
            StructField("player_id", LongType()),
            StructField("start_x", DoubleType()),
            StructField("start_y", DoubleType()),
            StructField("end_x", DoubleType()),
            StructField("end_y", DoubleType()),
            StructField("type_id", LongType()),
            StructField("action_type", StringType()),
            StructField("result_id", LongType()),
            StructField("action_result", StringType()),
            StructField("bodypart_id", LongType()),
            StructField("bodypart", StringType()),
            StructField("offensive_value", DoubleType()),
            StructField("defensive_value", DoubleType()),
            StructField("vaep_value", DoubleType()),
            StructField("competition_id", LongType()),
            StructField("season_id", LongType()),
            StructField("data_source", StringType()),
        ]
    )
```

Replace with:
```python
    from pyspark.sql.types import BooleanType  # noqa: PLC0415 — local for this StructType

    vaep_schema = StructType(
        [
            StructField("game_id", LongType()),
            StructField("match_id", LongType()),
            StructField("original_event_id", StringType()),
            StructField("period_id", LongType()),
            StructField("time_seconds", DoubleType()),
            StructField("team_id", LongType()),
            StructField("player_id", LongType()),
            StructField("start_x", DoubleType()),
            StructField("start_y", DoubleType()),
            StructField("end_x", DoubleType()),
            StructField("end_y", DoubleType()),
            StructField("type_id", LongType()),
            StructField("action_type", StringType()),
            StructField("result_id", LongType()),
            StructField("action_result", StringType()),
            StructField("bodypart_id", LongType()),
            StructField("bodypart", StringType()),
            StructField("offensive_value", DoubleType()),
            StructField("defensive_value", DoubleType()),
            StructField("vaep_value", DoubleType()),
            StructField("competition_id", LongType()),
            StructField("season_id", LongType()),
            StructField("data_source", StringType()),
            # LL2: action_id carried through (closes pre-LL2 100%-NULL gap).
            StructField("action_id", LongType()),
            # PR-LL1 statsbomb_* — must be in this schema, otherwise applyInPandas
            # silently drops them at the boundary. Closes the LL1 latent-bug class.
            StructField("statsbomb_possession_id", LongType()),
            StructField("statsbomb_possession_team_id", LongType()),
            StructField("statsbomb_play_pattern", StringType()),
            StructField("statsbomb_under_pressure", BooleanType()),
            # LL2: 6 enrichment columns from apply_spadl_enrichments.
            StructField("possession_id_heuristic", LongType()),
            StructField("gk_role", StringType()),
            StructField("gk_was_distributing", BooleanType()),
            StructField("gk_was_engaged", BooleanType()),
            StructField("gk_actions_in_possession", LongType()),
            StructField("defending_gk_player_id", LongType()),
        ]
    )
```

- [ ] **Step 4: Run the parity test**

Run: `uv run pytest src/tests/test_spadl_vaep_writer_parity.py -v`

Expected: ALL pass — both writer-parity classes, including `TestVaepScoringWriterDdlParity::test_vaep_scoring_struct_matches_vaep_ddl`.

- [ ] **Step 5: Run lint + types**

Run: `uv run ruff check src/ingestion/spadl_vaep.py && uv run pyright src/ingestion/spadl_vaep.py`

Expected: clean.

- [ ] **Step 6: Commit**

```bash
git add src/ingestion/spadl_vaep.py src/tests/test_spadl_vaep_writer_parity.py
git commit -m "feat(spadl): VAEP scoring UDF carries through statsbomb_* + action_id + 6 enrichment cols (closes LL1 vaep_schema gap)"
```

---

## Phase 8 — IDSSE adapter + UDF

### Task 8.1: Add IDSSE writer parity test (red)

**Files:**
- Modify: `src/tests/test_spadl_vaep_writer_parity.py` (add `_build_idsse_spadl_struct` helper + test)

- [ ] **Step 1: Append the IDSSE struct + test**

Append to `src/tests/test_spadl_vaep_writer_parity.py`:

```python
def _build_idsse_spadl_struct():  # type: ignore[no-untyped-def]
    """Replay the IDSSE applyInPandas StructType in spadl_conversion.py (LL2).

    IDSSE/Sportec via silly_kicks.spadl.sportec.convert_to_actions (silly-kicks
    1.7.0+). Output schema mirrors StatsBomb / Wyscout — same canonical SPADL
    columns + action_id surfaced + 6 LL2 enrichment columns. statsbomb_* are
    NULL-filled for multi-source parity.
    """
    pyspark_types = pytest.importorskip("pyspark.sql.types")
    BooleanType = pyspark_types.BooleanType  # noqa: N806
    DoubleType = pyspark_types.DoubleType  # noqa: N806
    LongType = pyspark_types.LongType  # noqa: N806
    StringType = pyspark_types.StringType  # noqa: N806
    StructField = pyspark_types.StructField  # noqa: N806
    StructType = pyspark_types.StructType  # noqa: N806

    return StructType(
        [
            StructField("game_id", LongType()),
            StructField("match_id", LongType()),
            StructField("original_event_id", StringType()),
            StructField("period_id", LongType()),
            StructField("time_seconds", DoubleType()),
            StructField("team_id", LongType()),
            StructField("player_id", LongType()),
            StructField("start_x", DoubleType()),
            StructField("start_y", DoubleType()),
            StructField("end_x", DoubleType()),
            StructField("end_y", DoubleType()),
            StructField("type_id", LongType()),
            StructField("result_id", LongType()),
            StructField("bodypart_id", LongType()),
            StructField("action_id", LongType()),
            StructField("competition_id", LongType()),
            StructField("season_id", LongType()),
            StructField("data_source", StringType()),
            StructField("statsbomb_possession_id", LongType()),
            StructField("statsbomb_possession_team_id", LongType()),
            StructField("statsbomb_play_pattern", StringType()),
            StructField("statsbomb_under_pressure", BooleanType()),
            StructField("possession_id_heuristic", LongType()),
            StructField("gk_role", StringType()),
            StructField("gk_was_distributing", BooleanType()),
            StructField("gk_was_engaged", BooleanType()),
            StructField("gk_actions_in_possession", LongType()),
            StructField("defending_gk_player_id", LongType()),
        ]
    )
```

Add to `TestSpadlVaepWriterDdlParity`:

```python
    def test_idsse_struct_matches_spadl_ddl(self) -> None:
        """LL2: IDSSE/Sportec writer parity (NEW source — silly-kicks 1.7.0)."""
        from ingestion import spadl_vaep

        ddl = _parse_ddl(spadl_vaep._SPADL_SCHEMA)
        out = {f.name: f.dataType.simpleString() for f in _build_idsse_spadl_struct().fields}

        missing = [c for c in out if c not in ddl]
        assert not missing, f"IDSSE writer emits columns absent from _SPADL_SCHEMA: {missing}"

        mismatched = {c: (out[c], ddl[c]) for c in out if out[c] != ddl[c]}
        assert not mismatched, f"IDSSE writer/DDL type drift {mismatched}"
```

- [ ] **Step 2: Run test to verify failure**

Run: `uv run pytest src/tests/test_spadl_vaep_writer_parity.py::TestSpadlVaepWriterDdlParity::test_idsse_struct_matches_spadl_ddl -v`

Expected: PASSES the parity check (the static replay is consistent with `_SPADL_SCHEMA`), but no actual IDSSE UDF exists yet — that comes in 8.2 / 8.3.

### Task 8.2: Add IDSSE adapter functions to `spadl_adapter.py`

**Files:**
- Modify: `src/ingestion/spadl_adapter.py` (append IDSSE adapter section)

- [ ] **Step 1: Append IDSSE adapter functions**

Append to `src/ingestion/spadl_adapter.py`:

```python
# ---------------------------------------------------------------------------
# IDSSE (Sportec / Bundesliga) adapters — LL2
# ---------------------------------------------------------------------------
#
# luxury-lakehouse's bronze.idsse_events stores DFL-flattened event rows
# (~210 columns including action data + ~200 per-event-type qualifiers).
# silly-kicks 1.7.0's silly_kicks.spadl.sportec.convert_to_actions accepts
# this shape directly — the brief targeted exactly luxury-lakehouse's
# bronze schema. The adapter below is mostly an identity passthrough plus
# the small home_team_id resolution.
#
# Required cols on input to silly-kicks (1.7.0 spec):
#   match_id, event_id, event_type, period, timestamp_seconds,
#   player_id, team, x, y
# All present in bronze.idsse_events. Optional qualifier columns (pass_*,
# shot_*, tackle_*, etc.) pass through silly-kicks's _RECOGNIZED_QUALIFIER_COLUMNS
# silently when present.


def adapt_idsse_events_for_silly_kicks(
    events_pdf: pd.DataFrame,
    home_team_id: str,
) -> pd.DataFrame:
    """Convert bronze ``idsse_events`` rows to silly-kicks 1.7.0 sportec input.

    LL2: bronze.idsse_events already exposes the column names silly-kicks's
    sportec converter expects (match_id, event_id, event_type, period,
    timestamp_seconds, player_id, team, x, y plus qualifier columns).
    This adapter is therefore a near-identity passthrough — present for
    consistency with the StatsBomb / Wyscout adapter pattern and to host
    any future column-rename divergences.

    Args:
        events_pdf: DataFrame read from the ``bronze.idsse_events`` table.
        home_team_id: Team string (e.g., "DFL-CLU-XXXXX") for direction-of-play
            normalization. silly-kicks's ``_fix_direction_of_play`` (unified in
            1.7.0 across all 6 converters) flips away-team coords.

    Returns:
        Adapted DataFrame ready for ``silly_kicks.spadl.sportec.convert_to_actions``.
    """
    # Currently identity passthrough. If silly-kicks's expected schema diverges
    # from the bronze shape in a future release, place the rename map here.
    return events_pdf.copy()


def resolve_idsse_home_team_ids(
    events_pdf: pd.DataFrame,
) -> dict[str, str]:
    """Derive ``home_team_id`` per match from IDSSE event metadata.

    The DFL `KickOff` event's ``kickoff_team_left`` field identifies the
    team starting on the left of the pitch in period 1, which luxury-lakehouse
    treats as the home team (consistent with StatsBomb's home-team-attacks-left
    convention after silly-kicks's ``_fix_direction_of_play`` normalization).

    Args:
        events_pdf: DataFrame read from ``bronze.idsse_events``.

    Returns:
        Mapping of ``match_id`` -> ``team`` (string), one entry per match.
        Matches without a kickoff record receive the first team observed
        in their event stream as a fallback.
    """
    home_map: dict[str, str] = {}
    if "kickoff_team_left" in events_pdf.columns:
        kickoff_rows = events_pdf[events_pdf["kickoff_team_left"].notna()]
        for match_id, group in kickoff_rows.groupby("match_id"):
            home_map[str(match_id)] = str(group["kickoff_team_left"].iloc[0])

    # Fallback for matches without a recognized KickOff event
    for match_id, group in events_pdf.groupby("match_id"):
        if str(match_id) not in home_map:
            non_null_team = group["team"].dropna()
            if len(non_null_team) > 0:
                home_map[str(match_id)] = str(non_null_team.iloc[0])
    return home_map
```

- [ ] **Step 2: Run lint + types on adapter**

Run: `uv run ruff check src/ingestion/spadl_adapter.py && uv run pyright src/ingestion/spadl_adapter.py`

Expected: clean.

- [ ] **Step 3: Commit (test + adapter, UDF coming next)**

```bash
git add src/tests/test_spadl_vaep_writer_parity.py src/ingestion/spadl_adapter.py
git commit -m "feat(spadl): IDSSE adapter functions + writer parity test (LL2)"
```

### Task 8.3: Add IDSSE UDF + `_convert_idsse_from_bronze` orchestrator

**Files:**
- Modify: `src/ingestion/spadl_conversion.py` (append IDSSE section)

- [ ] **Step 1: Add the IDSSE replace_where helper, UDF factory, and orchestrator**

Append to `src/ingestion/spadl_conversion.py`:

```python
# ---------------------------------------------------------------------------
# IDSSE (Sportec / Bundesliga) SPADL conversion — LL2
# ---------------------------------------------------------------------------


def _make_idsse_replace_where(new_game_ids: list[str]) -> str:
    """Build a replaceWhere predicate scoped to specific IDSSE matches being processed.

    IDSSE match_ids are strings (e.g. "J03WMX") — quote them in the SQL list.
    """
    if not new_game_ids:
        msg = "replace_where predicate requires at least one match_id"
        raise ValueError(msg)
    ids_sql = ", ".join(f"'{gid}'" for gid in sorted(new_game_ids))
    return f"data_source = 'idsse' AND match_id IN ({ids_sql})"


def _make_idsse_spadl_udf() -> object:
    """Build the ``applyInPandas`` UDF closure for IDSSE SPADL conversion.

    Uses silly-kicks 1.7.0+ ``silly_kicks.spadl.sportec.convert_to_actions``
    (DataFrame-based). All silly-kicks library imports happen inside the
    closure so they are available on Spark executors.
    """

    def _udf(pdf: pd.DataFrame) -> pd.DataFrame:
        """Convert one IDSSE match's events to SPADL actions."""
        import pandas as _pd

        from ingestion.spadl_adapter import adapt_idsse_events_for_silly_kicks as _adapt

        _spadl_cols = _pd.Index(
            [
                "game_id",
                "match_id",
                "original_event_id",
                "period_id",
                "time_seconds",
                "team_id",
                "player_id",
                "start_x",
                "start_y",
                "end_x",
                "end_y",
                "type_id",
                "result_id",
                "bodypart_id",
                "action_id",
                "competition_id",
                "season_id",
                "data_source",
                # statsbomb_* are NULL-filled for IDSSE (multi-source parity).
                "statsbomb_possession_id",
                "statsbomb_possession_team_id",
                "statsbomb_play_pattern",
                "statsbomb_under_pressure",
                # LL2: 6 enrichment columns (populated for IDSSE).
                "possession_id_heuristic",
                "gk_role",
                "gk_was_distributing",
                "gk_was_engaged",
                "gk_actions_in_possession",
                "defending_gk_player_id",
            ]
        )

        if pdf.empty:
            return _pd.DataFrame(columns=_spadl_cols)

        import silly_kicks.spadl.sportec as _spadl_sportec

        match_id = str(pdf["match_id"].iloc[0])
        home_team_id = str(pdf["home_team_id"].iloc[0])
        # IDSSE events use string team IDs and competition codes
        competition_id_val = pdf["competition_id"].iloc[0]
        season_id_val = pdf["season_id"].iloc[0]

        try:
            adapted = _adapt(pdf, home_team_id)
            actions, _report = _spadl_sportec.convert_to_actions(
                adapted,
                home_team_id=home_team_id,
            )
        except Exception as exc:
            msg = f"IDSSE SPADL conversion failed for match_id={match_id}"
            raise RuntimeError(msg) from exc

        # game_id / match_id are strings in IDSSE bronze; for Delta/Spark we
        # need consistent BIGINT typing in the SPADL pipeline. Hash the string
        # match_id deterministically to a stable int. silly-kicks's converter
        # uses match_id for grouping; we substitute on output for downstream
        # Delta/dim_matches consistency.
        import hashlib

        def _stable_int(s: str) -> int:
            return int(hashlib.sha256(s.encode("utf-8")).hexdigest()[:15], 16)

        actions["match_id"] = _stable_int(match_id)
        actions["game_id"] = actions["match_id"]
        # Wrap any string team/player IDs the converter passed through with the same hash.
        for col in ("team_id", "player_id"):
            if col in actions.columns and actions[col].dtype == "object":
                actions[col] = actions[col].astype(str).map(_stable_int)

        actions["competition_id"] = _stable_int(str(competition_id_val))
        actions["season_id"] = int(season_id_val) if str(season_id_val).isdigit() else _stable_int(str(season_id_val))
        actions["data_source"] = "idsse"

        # LL2 post-conversion enrichments
        from ingestion.spadl_enrichments import apply_spadl_enrichments as _enrich

        actions = _enrich(actions, source="idsse")

        actions["original_event_id"] = actions["original_event_id"].astype(str)

        # NULL-fill the StatsBomb-namespaced fields for multi-source parity.
        n = len(actions)
        actions["statsbomb_possession_id"] = _pd.array([_pd.NA] * n, dtype="Int64")
        actions["statsbomb_possession_team_id"] = _pd.array([_pd.NA] * n, dtype="Int64")
        actions["statsbomb_play_pattern"] = _pd.array([_pd.NA] * n, dtype="object")
        actions["statsbomb_under_pressure"] = _pd.array([_pd.NA] * n, dtype="boolean")

        # Cast enrichment columns
        actions["action_id"] = actions["action_id"].astype("Int64")
        actions["possession_id_heuristic"] = actions["possession_id_heuristic"].astype("Int64")
        actions["gk_role"] = actions["gk_role"].astype("object")
        actions["gk_was_distributing"] = actions["gk_was_distributing"].astype("boolean")
        actions["gk_was_engaged"] = actions["gk_was_engaged"].astype("boolean")
        actions["gk_actions_in_possession"] = actions["gk_actions_in_possession"].astype("Int64")
        actions["defending_gk_player_id"] = actions["defending_gk_player_id"].astype("Int64")

        return _pd.DataFrame(actions[_spadl_cols])

    return _udf


def _convert_idsse_from_bronze(
    spark: SparkSession,
    catalog: str,
    schema: str,
    logger: logging.Logger,
    existing_matches: set[int],
) -> bool:
    """Read IDSSE events from bronze, adapt, convert to SPADL via silly-kicks 1.7.0 sportec, write Delta.

    IDSSE bronze uses STRING match_ids (e.g., "J03WMX"). We hash to BIGINT
    inside the UDF to keep the spadl_actions pipeline BIGINT-consistent —
    same pattern used by other STRING-id sources downstream.
    """
    from pyspark.sql import functions as spark_fn
    from pyspark.sql.types import (
        BooleanType,
        DoubleType,
        LongType,
        StringType,
        StructField,
        StructType,
    )

    from ingestion.spadl_adapter import resolve_idsse_home_team_ids

    events_table = f"{catalog}.{schema}.idsse_events"

    try:
        events_sdf = spark.table(events_table)
    except Exception:
        logger.exception("Cannot read IDSSE events bronze table")
        return False

    all_match_rows = events_sdf.select("match_id").distinct().collect()
    all_match_ids: list[str] = [str(row["match_id"]) for row in all_match_rows]

    # IDSSE match_ids are STRING; existing_matches is set[int] of HASHED ids.
    import hashlib

    def _stable_int(s: str) -> int:
        return int(hashlib.sha256(s.encode("utf-8")).hexdigest()[:15], 16)

    new_match_ids: list[str] = [
        mid for mid in all_match_ids if _stable_int(mid) not in existing_matches
    ]

    if not new_match_ids:
        logger.info("IDSSE: all %d matches already converted — skipping", len(all_match_ids))
        return False

    logger.info("IDSSE: converting %d new matches (of %d total)", len(new_match_ids), len(all_match_ids))

    # Pull events for new matches to driver to resolve home_team_id (small data — 7 matches * ~1.5K = ~10K rows)
    new_events_pdf = (
        events_sdf.filter(spark_fn.col("match_id").isin(new_match_ids))
        .select("match_id", "team", "kickoff_team_left")
        .toPandas()
    )
    home_team_map = resolve_idsse_home_team_ids(new_events_pdf)

    # Build lookup DataFrame
    lookup_rows = [(mid, home_team_map[mid]) for mid in new_match_ids if mid in home_team_map]
    lookup_schema = StructType(
        [
            StructField("match_id", StringType()),
            StructField("home_team_id", StringType()),
        ]
    )
    lookup_sdf = spark.createDataFrame(lookup_rows, schema=lookup_schema)

    new_events_sdf = (
        events_sdf.filter(spark_fn.col("match_id").isin(new_match_ids))
        .join(lookup_sdf, on="match_id", how="inner")
    )

    spadl_schema = StructType(
        [
            StructField("game_id", LongType()),
            StructField("match_id", LongType()),
            StructField("original_event_id", StringType()),
            StructField("period_id", LongType()),
            StructField("time_seconds", DoubleType()),
            StructField("team_id", LongType()),
            StructField("player_id", LongType()),
            StructField("start_x", DoubleType()),
            StructField("start_y", DoubleType()),
            StructField("end_x", DoubleType()),
            StructField("end_y", DoubleType()),
            StructField("type_id", LongType()),
            StructField("result_id", LongType()),
            StructField("bodypart_id", LongType()),
            StructField("action_id", LongType()),
            StructField("competition_id", LongType()),
            StructField("season_id", LongType()),
            StructField("data_source", StringType()),
            StructField("statsbomb_possession_id", LongType()),
            StructField("statsbomb_possession_team_id", LongType()),
            StructField("statsbomb_play_pattern", StringType()),
            StructField("statsbomb_under_pressure", BooleanType()),
            StructField("possession_id_heuristic", LongType()),
            StructField("gk_role", StringType()),
            StructField("gk_was_distributing", BooleanType()),
            StructField("gk_was_engaged", BooleanType()),
            StructField("gk_actions_in_possession", LongType()),
            StructField("defending_gk_player_id", LongType()),
        ]
    )

    udf_fn = _make_idsse_spadl_udf()
    spadl_sdf = new_events_sdf.groupBy("match_id").applyInPandas(
        udf_fn,  # type: ignore[arg-type]
        schema=spadl_schema,
    )

    # Use HASHED match_ids in the replaceWhere predicate so it matches the
    # match_id column type written to Delta.
    hashed_new_ids = sorted(_stable_int(mid) for mid in new_match_ids)
    ids_sql = ", ".join(str(int(h)) for h in hashed_new_ids)
    write_delta_table(
        spadl_sdf,
        catalog,
        schema,
        _SPADL_TABLE,
        replace_where=f"data_source = 'idsse' AND match_id IN ({ids_sql})",
        logger=logger,
    )

    logger.info("IDSSE: SPADL conversion complete for %d matches", len(new_match_ids))
    return True
```

- [ ] **Step 2: Run writer parity test for IDSSE**

Run: `uv run pytest src/tests/test_spadl_vaep_writer_parity.py::TestSpadlVaepWriterDdlParity::test_idsse_struct_matches_spadl_ddl -v`

Expected: PASS.

- [ ] **Step 3: Run lint + types**

Run: `uv run ruff check src/ingestion/spadl_conversion.py && uv run pyright src/ingestion/spadl_conversion.py`

Expected: clean.

- [ ] **Step 4: Commit**

```bash
git add src/ingestion/spadl_conversion.py
git commit -m "feat(spadl): IDSSE UDF + _convert_idsse_from_bronze (silly-kicks 1.7.0 sportec converter)"
```

---

## Phase 9 — Metrica adapter + UDF

### Task 9.1: Add Metrica writer parity test + adapter + UDF

**Files:**
- Modify: `src/tests/test_spadl_vaep_writer_parity.py` (add `_build_metrica_spadl_struct` + test)
- Modify: `src/ingestion/spadl_adapter.py` (append Metrica adapter)
- Modify: `src/ingestion/spadl_conversion.py` (append Metrica UDF + orchestrator)

- [ ] **Step 1: Add Metrica writer parity test (mirror IDSSE)**

Append to `src/tests/test_spadl_vaep_writer_parity.py`:

```python
def _build_metrica_spadl_struct():  # type: ignore[no-untyped-def]
    """Replay the Metrica applyInPandas StructType in spadl_conversion.py (LL2)."""
    pyspark_types = pytest.importorskip("pyspark.sql.types")
    BooleanType = pyspark_types.BooleanType  # noqa: N806
    DoubleType = pyspark_types.DoubleType  # noqa: N806
    LongType = pyspark_types.LongType  # noqa: N806
    StringType = pyspark_types.StringType  # noqa: N806
    StructField = pyspark_types.StructField  # noqa: N806
    StructType = pyspark_types.StructType  # noqa: N806

    return StructType(
        [
            StructField("game_id", LongType()),
            StructField("match_id", LongType()),
            StructField("original_event_id", StringType()),
            StructField("period_id", LongType()),
            StructField("time_seconds", DoubleType()),
            StructField("team_id", LongType()),
            StructField("player_id", LongType()),
            StructField("start_x", DoubleType()),
            StructField("start_y", DoubleType()),
            StructField("end_x", DoubleType()),
            StructField("end_y", DoubleType()),
            StructField("type_id", LongType()),
            StructField("result_id", LongType()),
            StructField("bodypart_id", LongType()),
            StructField("action_id", LongType()),
            StructField("competition_id", LongType()),
            StructField("season_id", LongType()),
            StructField("data_source", StringType()),
            StructField("statsbomb_possession_id", LongType()),
            StructField("statsbomb_possession_team_id", LongType()),
            StructField("statsbomb_play_pattern", StringType()),
            StructField("statsbomb_under_pressure", BooleanType()),
            StructField("possession_id_heuristic", LongType()),
            StructField("gk_role", StringType()),
            StructField("gk_was_distributing", BooleanType()),
            StructField("gk_was_engaged", BooleanType()),
            StructField("gk_actions_in_possession", LongType()),
            StructField("defending_gk_player_id", LongType()),
        ]
    )
```

Add test method to `TestSpadlVaepWriterDdlParity`:
```python
    def test_metrica_struct_matches_spadl_ddl(self) -> None:
        """LL2: Metrica writer parity (NEW source — silly-kicks 1.7.0)."""
        from ingestion import spadl_vaep

        ddl = _parse_ddl(spadl_vaep._SPADL_SCHEMA)
        out = {f.name: f.dataType.simpleString() for f in _build_metrica_spadl_struct().fields}

        missing = [c for c in out if c not in ddl]
        assert not missing, f"Metrica writer emits columns absent from _SPADL_SCHEMA: {missing}"

        mismatched = {c: (out[c], ddl[c]) for c in out if out[c] != ddl[c]}
        assert not mismatched, f"Metrica writer/DDL type drift {mismatched}"
```

- [ ] **Step 2: Add Metrica adapter functions to `spadl_adapter.py`**

Append to `src/ingestion/spadl_adapter.py`:

```python
# ---------------------------------------------------------------------------
# Metrica adapters — LL2
# ---------------------------------------------------------------------------
#
# luxury-lakehouse's bronze.metrica_events stores normalized event rows
# (~20 columns: team / type / subtype / period / start_frame / start_time_s /
# end_frame / end_time_s / player / to / start_x / start_y / end_x / end_y /
# event_id / match_id / pitch_length_m / pitch_width_m / subtypes_all_json).
# silly-kicks 1.7.0's silly_kicks.spadl.metrica.convert_to_actions accepts
# this shape directly.


def adapt_metrica_events_for_silly_kicks(
    events_pdf: pd.DataFrame,
    home_team_id: str,
) -> pd.DataFrame:
    """Convert bronze ``metrica_events`` rows to silly-kicks 1.7.0 metrica input.

    Identity passthrough — bronze.metrica_events column names match silly-kicks
    1.7.0's required input schema directly.
    """
    return events_pdf.copy()


def resolve_metrica_home_team_ids(
    events_pdf: pd.DataFrame,
) -> dict[str, str]:
    """Derive ``home_team_id`` per match for Metrica events.

    Metrica open-data uses team string identifiers (e.g., "Home" / "Away" or
    numeric IDs depending on the game). For Sample_Game_1/2 (CSVs), the team
    column carries "Home" / "Away" — we use "Home" as home_team_id. For
    Sample_Game_3 (EPTS), the convention may differ; fallback to first-team-
    observed.
    """
    home_map: dict[str, str] = {}
    for match_id, group in events_pdf.groupby("match_id"):
        teams = group["team"].dropna().unique()
        if "Home" in teams:
            home_map[str(match_id)] = "Home"
        elif len(teams) > 0:
            home_map[str(match_id)] = str(teams[0])
    return home_map
```

- [ ] **Step 3: Add Metrica UDF + `_convert_metrica_from_bronze` orchestrator**

Append to `src/ingestion/spadl_conversion.py`:

```python
# ---------------------------------------------------------------------------
# Metrica SPADL conversion — LL2
# ---------------------------------------------------------------------------


def _make_metrica_replace_where(new_game_ids: list[str]) -> str:
    """Build a replaceWhere predicate scoped to specific Metrica matches."""
    if not new_game_ids:
        msg = "replace_where predicate requires at least one match_id"
        raise ValueError(msg)
    ids_sql = ", ".join(f"'{gid}'" for gid in sorted(new_game_ids))
    return f"data_source = 'metrica' AND match_id IN ({ids_sql})"


def _make_metrica_spadl_udf() -> object:
    """Build the ``applyInPandas`` UDF closure for Metrica SPADL conversion.

    Uses silly-kicks 1.7.0+ ``silly_kicks.spadl.metrica.convert_to_actions``.
    """

    def _udf(pdf: pd.DataFrame) -> pd.DataFrame:
        """Convert one Metrica match's events to SPADL actions."""
        import hashlib

        import pandas as _pd

        from ingestion.spadl_adapter import adapt_metrica_events_for_silly_kicks as _adapt

        _spadl_cols = _pd.Index(
            [
                "game_id",
                "match_id",
                "original_event_id",
                "period_id",
                "time_seconds",
                "team_id",
                "player_id",
                "start_x",
                "start_y",
                "end_x",
                "end_y",
                "type_id",
                "result_id",
                "bodypart_id",
                "action_id",
                "competition_id",
                "season_id",
                "data_source",
                "statsbomb_possession_id",
                "statsbomb_possession_team_id",
                "statsbomb_play_pattern",
                "statsbomb_under_pressure",
                "possession_id_heuristic",
                "gk_role",
                "gk_was_distributing",
                "gk_was_engaged",
                "gk_actions_in_possession",
                "defending_gk_player_id",
            ]
        )

        if pdf.empty:
            return _pd.DataFrame(columns=_spadl_cols)

        import silly_kicks.spadl.metrica as _spadl_metrica

        match_id = str(pdf["match_id"].iloc[0])
        home_team_id = str(pdf["home_team_id"].iloc[0])
        competition_id_val = pdf["competition_id"].iloc[0] if "competition_id" in pdf.columns else "metrica_open_data"
        season_id_val = pdf["season_id"].iloc[0] if "season_id" in pdf.columns else 0

        try:
            adapted = _adapt(pdf, home_team_id)
            actions, _report = _spadl_metrica.convert_to_actions(
                adapted,
                home_team_id=home_team_id,
            )
        except Exception as exc:
            msg = f"Metrica SPADL conversion failed for match_id={match_id}"
            raise RuntimeError(msg) from exc

        def _stable_int(s: str) -> int:
            return int(hashlib.sha256(s.encode("utf-8")).hexdigest()[:15], 16)

        actions["match_id"] = _stable_int(match_id)
        actions["game_id"] = actions["match_id"]
        for col in ("team_id", "player_id"):
            if col in actions.columns and actions[col].dtype == "object":
                actions[col] = actions[col].astype(str).map(_stable_int)

        actions["competition_id"] = _stable_int(str(competition_id_val))
        actions["season_id"] = int(season_id_val) if str(season_id_val).isdigit() else _stable_int(str(season_id_val))
        actions["data_source"] = "metrica"

        from ingestion.spadl_enrichments import apply_spadl_enrichments as _enrich

        actions = _enrich(actions, source="metrica")

        actions["original_event_id"] = actions["original_event_id"].astype(str)

        n = len(actions)
        actions["statsbomb_possession_id"] = _pd.array([_pd.NA] * n, dtype="Int64")
        actions["statsbomb_possession_team_id"] = _pd.array([_pd.NA] * n, dtype="Int64")
        actions["statsbomb_play_pattern"] = _pd.array([_pd.NA] * n, dtype="object")
        actions["statsbomb_under_pressure"] = _pd.array([_pd.NA] * n, dtype="boolean")

        actions["action_id"] = actions["action_id"].astype("Int64")
        actions["possession_id_heuristic"] = actions["possession_id_heuristic"].astype("Int64")
        actions["gk_role"] = actions["gk_role"].astype("object")
        actions["gk_was_distributing"] = actions["gk_was_distributing"].astype("boolean")
        actions["gk_was_engaged"] = actions["gk_was_engaged"].astype("boolean")
        actions["gk_actions_in_possession"] = actions["gk_actions_in_possession"].astype("Int64")
        actions["defending_gk_player_id"] = actions["defending_gk_player_id"].astype("Int64")

        return _pd.DataFrame(actions[_spadl_cols])

    return _udf


def _convert_metrica_from_bronze(
    spark: SparkSession,
    catalog: str,
    schema: str,
    logger: logging.Logger,
    existing_matches: set[int],
) -> bool:
    """Read Metrica events from bronze, adapt, convert via silly-kicks 1.7.0 metrica, write Delta."""
    from pyspark.sql import functions as spark_fn
    from pyspark.sql.types import (
        BooleanType,
        DoubleType,
        LongType,
        StringType,
        StructField,
        StructType,
    )

    from ingestion.spadl_adapter import resolve_metrica_home_team_ids

    events_table = f"{catalog}.{schema}.metrica_events"

    try:
        events_sdf = spark.table(events_table)
    except Exception:
        logger.exception("Cannot read Metrica events bronze table")
        return False

    all_match_rows = events_sdf.select("match_id").distinct().collect()
    all_match_ids: list[str] = [str(row["match_id"]) for row in all_match_rows]

    import hashlib

    def _stable_int(s: str) -> int:
        return int(hashlib.sha256(s.encode("utf-8")).hexdigest()[:15], 16)

    new_match_ids: list[str] = [
        mid for mid in all_match_ids if _stable_int(mid) not in existing_matches
    ]

    if not new_match_ids:
        logger.info("Metrica: all %d matches already converted — skipping", len(all_match_ids))
        return False

    logger.info("Metrica: converting %d new matches (of %d total)", len(new_match_ids), len(all_match_ids))

    new_events_pdf = (
        events_sdf.filter(spark_fn.col("match_id").isin(new_match_ids))
        .select("match_id", "team")
        .toPandas()
    )
    home_team_map = resolve_metrica_home_team_ids(new_events_pdf)

    lookup_rows = [(mid, home_team_map[mid]) for mid in new_match_ids if mid in home_team_map]
    lookup_schema = StructType(
        [
            StructField("match_id", StringType()),
            StructField("home_team_id", StringType()),
        ]
    )
    lookup_sdf = spark.createDataFrame(lookup_rows, schema=lookup_schema)

    new_events_sdf = (
        events_sdf.filter(spark_fn.col("match_id").isin(new_match_ids))
        .join(lookup_sdf, on="match_id", how="inner")
    )

    spadl_schema = StructType(
        [
            StructField("game_id", LongType()),
            StructField("match_id", LongType()),
            StructField("original_event_id", StringType()),
            StructField("period_id", LongType()),
            StructField("time_seconds", DoubleType()),
            StructField("team_id", LongType()),
            StructField("player_id", LongType()),
            StructField("start_x", DoubleType()),
            StructField("start_y", DoubleType()),
            StructField("end_x", DoubleType()),
            StructField("end_y", DoubleType()),
            StructField("type_id", LongType()),
            StructField("result_id", LongType()),
            StructField("bodypart_id", LongType()),
            StructField("action_id", LongType()),
            StructField("competition_id", LongType()),
            StructField("season_id", LongType()),
            StructField("data_source", StringType()),
            StructField("statsbomb_possession_id", LongType()),
            StructField("statsbomb_possession_team_id", LongType()),
            StructField("statsbomb_play_pattern", StringType()),
            StructField("statsbomb_under_pressure", BooleanType()),
            StructField("possession_id_heuristic", LongType()),
            StructField("gk_role", StringType()),
            StructField("gk_was_distributing", BooleanType()),
            StructField("gk_was_engaged", BooleanType()),
            StructField("gk_actions_in_possession", LongType()),
            StructField("defending_gk_player_id", LongType()),
        ]
    )

    udf_fn = _make_metrica_spadl_udf()
    spadl_sdf = new_events_sdf.groupBy("match_id").applyInPandas(
        udf_fn,  # type: ignore[arg-type]
        schema=spadl_schema,
    )

    hashed_new_ids = sorted(_stable_int(mid) for mid in new_match_ids)
    ids_sql = ", ".join(str(int(h)) for h in hashed_new_ids)
    write_delta_table(
        spadl_sdf,
        catalog,
        schema,
        _SPADL_TABLE,
        replace_where=f"data_source = 'metrica' AND match_id IN ({ids_sql})",
        logger=logger,
    )

    logger.info("Metrica: SPADL conversion complete for %d matches", len(new_match_ids))
    return True
```

- [ ] **Step 4: Run all writer parity tests + lint + types**

Run: `uv run pytest src/tests/test_spadl_vaep_writer_parity.py -v && uv run ruff check src/ingestion/spadl_conversion.py src/ingestion/spadl_adapter.py && uv run pyright src/ingestion/spadl_conversion.py src/ingestion/spadl_adapter.py`

Expected: all PASS, lint + types clean.

- [ ] **Step 5: Commit**

```bash
git add src/ingestion/spadl_conversion.py src/ingestion/spadl_adapter.py src/tests/test_spadl_vaep_writer_parity.py
git commit -m "feat(spadl): Metrica UDF + adapter + writer parity test (silly-kicks 1.7.0 metrica)"
```

---

## Phase 10 — Update `_VaepGuard.check()` + `run_pipeline` for 4 sources

### Task 10.1: Expand the guard's Stage 1 to query 4 source tables

**Files:**
- Modify: `src/ingestion/spadl_vaep.py:78-143` (the `_VaepGuard` class)
- Modify: `src/ingestion/spadl_vaep.py:451-520` (the `run_pipeline` function — call sequence)

- [ ] **Step 1: Update `_VaepGuard.check()`**

Find:
```python
        # Stage 1: Source events not yet in SPADL (two sources, union results)
        sb_new = find_new_ids(
            spark,
            f"{catalog}.{schema}.statsbomb_events",
            spadl_table,
        )
        ws_new = find_new_ids(
            spark,
            f"{catalog}.{schema}.wyscout_events",
            spadl_table,
            id_column="matchId",
            results_id_column="match_id",
        )
        new_spadl = sorted(set(sb_new) | set(ws_new))
```

Replace with:
```python
        # Stage 1: Source events not yet in SPADL (four sources after LL2,
        # union results). IDSSE + Metrica use STRING match_ids; the SPADL UDFs
        # hash to BIGINT inside the per-match closure for Delta consistency.
        # find_new_ids handles type coercion based on the configured columns.
        sb_new = find_new_ids(
            spark,
            f"{catalog}.{schema}.statsbomb_events",
            spadl_table,
        )
        ws_new = find_new_ids(
            spark,
            f"{catalog}.{schema}.wyscout_events",
            spadl_table,
            id_column="matchId",
            results_id_column="match_id",
        )
        # IDSSE: STRING match_id. find_new_ids returns the string IDs; the
        # SPADL UDF hashes them. We track string IDs in the metadata for
        # observability; the UDF orchestrators handle hashing.
        idsse_new = find_new_ids(
            spark,
            f"{catalog}.{schema}.idsse_events",
            spadl_table,
            id_column="match_id",
            results_id_column="match_id",
            results_filter="data_source = 'idsse'",
        )
        metrica_new = find_new_ids(
            spark,
            f"{catalog}.{schema}.metrica_events",
            spadl_table,
            id_column="match_id",
            results_id_column="match_id",
            results_filter="data_source = 'metrica'",
        )
        new_spadl = sorted(set(sb_new) | set(ws_new) | set(idsse_new) | set(metrica_new))
```

NOTE: This step assumes `find_new_ids` accepts a `results_filter` parameter. **Verify before applying** — read `src/ingestion/guards.py::find_new_ids` to confirm or add the parameter. If the helper doesn't support a filter clause and the IDSSE/Metrica match_id namespace doesn't collide with the StatsBomb/Wyscout BIGINT space (it shouldn't — they're greenfield), the simple form without `results_filter` is fine. Adjust the call to match the helper's actual signature.

- [ ] **Step 2: Update `run_pipeline` to call all four converters**

Find:
```python
    sb_wrote = _convert_statsbomb_from_bronze(spark, catalog, schema, logger, existing_spadl_matches)
    ws_wrote = _convert_wyscout_from_bronze(spark, catalog, schema, logger, existing_spadl_matches)

    if not sb_wrote and not ws_wrote and not existing_spadl_matches:
        msg = "No SPADL actions produced from either StatsBomb or Wyscout"
        logger.error(msg)
        raise RuntimeError(msg)
```

Replace with:
```python
    sb_wrote = _convert_statsbomb_from_bronze(spark, catalog, schema, logger, existing_spadl_matches)
    ws_wrote = _convert_wyscout_from_bronze(spark, catalog, schema, logger, existing_spadl_matches)
    idsse_wrote = _convert_idsse_from_bronze(spark, catalog, schema, logger, existing_spadl_matches)
    metrica_wrote = _convert_metrica_from_bronze(spark, catalog, schema, logger, existing_spadl_matches)

    if (
        not sb_wrote
        and not ws_wrote
        and not idsse_wrote
        and not metrica_wrote
        and not existing_spadl_matches
    ):
        msg = "No SPADL actions produced from any of the 4 sources (StatsBomb / Wyscout / IDSSE / Metrica)"
        logger.error(msg)
        raise RuntimeError(msg)
```

- [ ] **Step 3: Update the imports at the top of `spadl_vaep.py`**

Find:
```python
from ingestion.spadl_conversion import (
    _SPADL_TABLE,
    _convert_statsbomb_from_bronze,
    _convert_wyscout_from_bronze,
    _read_existing_match_ids,
)
```

Replace with:
```python
from ingestion.spadl_conversion import (
    _SPADL_TABLE,
    _convert_idsse_from_bronze,
    _convert_metrica_from_bronze,
    _convert_statsbomb_from_bronze,
    _convert_wyscout_from_bronze,
    _read_existing_match_ids,
)
```

- [ ] **Step 4: Run lint + types + existing tests**

Run: `uv run ruff check src/ingestion/spadl_vaep.py && uv run pyright src/ingestion/spadl_vaep.py && uv run pytest src/tests/test_spadl_vaep_writer_parity.py src/tests/test_spadl_enrichments.py -v`

Expected: clean lint + types; all writer parity + enrichment tests pass.

- [ ] **Step 5: Run any existing guard conformance tests**

Run: `uv run pytest src/tests/test_guard_conformance.py -v 2>&1 | head -40`

Expected: existing guard conformance tests still pass (we expanded the guard's Stage 1 query but didn't change its public contract — it still returns `FilterResult` with the same metadata shape).

If new guard conformance tests for IDSSE/Metrica are needed (4-source coverage), they would be added in a separate test or extended within `test_guard_conformance.py`. Document in PR description if added.

- [ ] **Step 6: Commit**

```bash
git add src/ingestion/spadl_vaep.py
git commit -m "feat(spadl): _VaepGuard + run_pipeline cover 4 sources (StatsBomb / Wyscout / IDSSE / Metrica)"
```

---

## Phase 11 — dbt staging passthrough

### Task 11.1: Update `stg_spadl__action_values.sql` to passthrough new bronze columns

**Files:**
- Modify: `dbt_project/models/staging/spadl/stg_spadl__action_values.sql:111-117`

- [ ] **Step 1: Add new column passthroughs in the `cleaned` CTE**

Use Edit on `dbt_project/models/staging/spadl/stg_spadl__action_values.sql`:

Find:
```sql
        -- Provider-namespaced StatsBomb-native fields (silly-kicks 1.5.0+
        -- preserve_native passthrough). NULL for non-StatsBomb sources.
        statsbomb_possession_id,
        statsbomb_possession_team_id,
        statsbomb_play_pattern,
        statsbomb_under_pressure

    from deduplicated d
```

Replace with:
```sql
        -- Provider-namespaced StatsBomb-native fields (silly-kicks 1.5.0+
        -- preserve_native passthrough). NULL for non-StatsBomb sources.
        statsbomb_possession_id,
        statsbomb_possession_team_id,
        statsbomb_play_pattern,
        statsbomb_under_pressure,

        -- LL2: action_id surfaced from silly-kicks convert_to_actions output
        -- (was 100% NULL pre-LL2). Per-match counter starting at 0.
        action_id,

        -- LL2: 6 post-conversion enrichment columns from apply_spadl_enrichments
        -- (provider-agnostic — populated for all 4 sources). See ADR-016.
        possession_id_heuristic,
        gk_role,
        gk_was_distributing,
        gk_was_engaged,
        gk_actions_in_possession,
        defending_gk_player_id

    from deduplicated d
```

- [ ] **Step 2: Verify dbt compiles**

Run: `uv run dbt compile --select stg_spadl__action_values 2>&1 | tail -20`

Expected: `Done.` (model compiles successfully — no SQL syntax errors).

If it fails with "column not found" for any of the new columns, that means bronze hasn't been ALTERed yet (Phase 17 covers that). For local dev, dbt will fail to compile against the live source until ALTER runs. **Acceptable**: continue plan tasks; the pre-merge ALTER step (Phase 17 / 21) makes this resolve. Note in commit message that the staging model anticipates the post-ALTER bronze schema.

- [ ] **Step 3: Update `_spadl__sources.yml` with column docs**

Use Edit on `dbt_project/models/staging/spadl/_spadl__sources.yml`:

Find:
```yaml
          - name: statsbomb_under_pressure
            description: >
              StatsBomb-native pressure flag — True iff a defender was
              within ~1m of the actor at the moment of the event.
              NULL for non-StatsBomb sources.
```

Replace with:
```yaml
          - name: statsbomb_under_pressure
            description: >
              StatsBomb-native pressure flag — True iff a defender was
              within ~1m of the actor at the moment of the event.
              NULL for non-StatsBomb sources.
          - name: action_id
            description: >
              Per-match SPADL action sequence number (silly-kicks
              convert_to_actions output, surfaced through bronze in LL2).
              Was 100% NULL pre-LL2 (writer dropped at projection); fixed in
              LL2 across all 4 sources. Monotonic non-decreasing within
              ``(match_id, period_id)``.
          - name: possession_id_heuristic
            description: >
              Heuristic possession sequence number (silly-kicks
              ``add_possessions``, LL2). Per-match counter resetting at
              new ``game_id``. Always populated across all 4 sources. The
              canonical mart-level ``possession_id`` in
              ``fct_action_values`` is sourced from this column.
          - name: gk_role
            description: >
              Goalkeeper role tag (silly-kicks ``add_gk_role``, LL2). One of
              ``shot_stopping`` / ``cross_collection`` / ``sweeping`` /
              ``pick_up`` / ``distribution``. NULL on non-GK action rows.
              Provider-agnostic — populated for all 4 sources.
          - name: gk_was_distributing
            description: >
              True iff the defending goalkeeper had a non-keeper action in
              the lookback window before this shot (silly-kicks
              ``add_pre_shot_gk_context``, LL2). False on non-shot rows by
              construction.
          - name: gk_was_engaged
            description: >
              True iff the defending goalkeeper had a ``keeper_*`` action in
              the lookback window before this shot. False on non-shot rows.
          - name: gk_actions_in_possession
            description: >
              Count of ``keeper_*`` actions by the defending GK in the
              lookback window. 0 on non-shot rows.
          - name: defending_gk_player_id
            description: >
              Player ID of the defending GK identified from the most recent
              ``keeper_*`` action by a team OTHER than the shooter's team
              within the lookback window. NULL when no defending GK
              identifiable or non-shot row.
```

- [ ] **Step 4: Verify dbt parses + compiles cleanly**

Run: `uv run dbt parse 2>&1 | tail -5 && uv run dbt compile --select stg_spadl__action_values 2>&1 | tail -5`

Expected: parse `Done.` and compile `Done.`.

- [ ] **Step 5: Commit**

```bash
git add dbt_project/models/staging/spadl/stg_spadl__action_values.sql dbt_project/models/staging/spadl/_spadl__sources.yml
git commit -m "feat(dbt): stg_spadl__action_values passthrough action_id + 6 enrichment cols (LL2)"
```

---

## Phase 12 — `fct_action_values` β-consistent rewrite

### Task 12.1: Rewrite `fct_action_values.sql` SELECT for β-consistent shape

**Files:**
- Modify: `dbt_project/models/marts/fct_action_values.sql:64-228`

- [ ] **Step 1: Update the `actions_with_score` CTE to expose new columns + rename existing aliases**

Use Edit on `dbt_project/models/marts/fct_action_values.sql`:

Find the `actions_with_score` SELECT block:
```sql
        -- Possession context (StatsBomb-only; NULL for Wyscout / IDSSE / SkillCorner).
        -- Sourced upstream via silly-kicks 1.5.0 preserve_native kwarg (LL1).
        av.statsbomb_possession_id                  as possession_id,
        -- Legacy `possession_team_id` retained inside the ADR-011 dual-column
        -- window (sunset 2026-07-22 alongside team_id / match_id / competition_id).
        -- The Kimball surrogate `possession_team_key` is the canonical FK.
        av.statsbomb_possession_team_id             as possession_team_id,
        -- Kimball surrogate FK for the possession team — same dim_teams
        -- resolution pattern as `team_key` / `player_key` above.
        dt_poss.team_key                            as possession_team_key,

        -- Pure descriptors (no FK semantics) — StatsBomb-only.
        av.statsbomb_play_pattern                   as play_pattern,
        av.statsbomb_under_pressure                 as under_pressure,
```

Replace with:
```sql
        -- LL2 β-consistent naming: provider-native passthroughs keep their
        -- statsbomb_* prefix all the way to the mart (no alias drop). See ADR-016.
        -- NULL for non-StatsBomb sources (Wyscout / IDSSE / Metrica).
        av.statsbomb_possession_id,
        av.statsbomb_possession_team_id,
        av.statsbomb_play_pattern,
        av.statsbomb_under_pressure,

        -- Kimball surrogate FK for the StatsBomb-native possession team.
        -- Plain-named because Kimball surrogate naming (`*_key`) wins over the
        -- canonical/native rule; population follows the underlying
        -- statsbomb_possession_team_id, so it's NULL on non-StatsBomb sources.
        dt_poss.team_key                            as possession_team_key,

        -- LL2 canonical columns (post-conversion enrichments — populated for
        -- all 4 sources). possession_id is heuristic (silly-kicks
        -- add_possessions); semantically distinct from the LL1
        -- StatsBomb-only-NULL-elsewhere version. Consumers that want strict
        -- StatsBomb semantics use statsbomb_possession_id (above).
        av.possession_id_heuristic                  as possession_id,
        av.action_id,
        av.gk_role,
        av.gk_was_distributing,
        av.gk_was_engaged,
        av.gk_actions_in_possession,
        av.defending_gk_player_id,
```

- [ ] **Step 2: Update the `final` CTE SELECT list**

Find:
```sql
final as (

    select
        action_value_id,
        match_key,
        competition_key,
        match_id,
        competition_id,
        player_id,
        team_id,
        team_key,
        player_key,
        season_id,
        period,
        time_seconds,
        minute,
        second,
        start_x,
        start_y,
        end_x,
        end_y,
        action_type,
        action_result,
        bodypart,
        offensive_value,
        defensive_value,
        vaep_value,
        possession_id,
        possession_team_id,
        possession_team_key,
        play_pattern,
        under_pressure,
        case
```

Replace with:
```sql
final as (

    select
        action_value_id,
        match_key,
        competition_key,
        match_id,
        competition_id,
        player_id,
        team_id,
        team_key,
        player_key,
        season_id,
        period,
        time_seconds,
        minute,
        second,
        start_x,
        start_y,
        end_x,
        end_y,
        action_type,
        action_result,
        bodypart,
        offensive_value,
        defensive_value,
        vaep_value,
        -- LL2 canonical post-conversion enrichments
        action_id,
        possession_id,                     -- canonical heuristic (was StatsBomb-only alias pre-LL2)
        gk_role,
        gk_was_distributing,
        gk_was_engaged,
        gk_actions_in_possession,
        defending_gk_player_id,
        -- LL2 provider-native passthroughs (StatsBomb-only, NULL elsewhere)
        statsbomb_possession_id,
        statsbomb_possession_team_id,
        statsbomb_play_pattern,
        statsbomb_under_pressure,
        -- Kimball surrogate (plain-named per Kimball convention)
        possession_team_key,
        case
```

- [ ] **Step 3: Update the header comment block to reflect β-consistent shape**

Find:
```sql
-- PR 4b (2026-04-23): Kimball-conformed per ADR-011. Emits `match_key` +
-- `competition_key` (BIGINT Kimball surrogates, resolved via LEFT JOIN
-- dim_matches on (native_match_id, provider)). Retains legacy `match_id`
-- and `competition_id` (both BIGINT nullable) for the 90-day dual-column
-- window — removed on or after 2026-07-22 per ADR-011 policy.
--
-- Coordinate system: 105x68 meters (SPADL academic standard).
-- One row per action.
```

Replace with:
```sql
-- PR 4b (2026-04-23): Kimball-conformed per ADR-011. Emits `match_key` +
-- `competition_key` (BIGINT Kimball surrogates, resolved via LEFT JOIN
-- dim_matches on (native_match_id, provider)). Retains legacy `match_id`
-- and `competition_id` (both BIGINT nullable) for the 90-day dual-column
-- window — removed on or after 2026-07-22 per ADR-011 policy.
--
-- LL2 (2026-04-29): β-consistent canonical-vs-native naming convention
-- (see ADR-016). Post-conversion enrichments use plain canonical names
-- (`possession_id` is now heuristic-based and populated for all sources;
-- `gk_role`, `gk_was_engaged`, etc.). Provider-native passthroughs use
-- `statsbomb_*` prefix everywhere (`statsbomb_possession_id`,
-- `statsbomb_play_pattern`, etc.; NULL on Wyscout / IDSSE / Metrica).
-- The legacy `possession_team_id` alias was closed early (column added in
-- LL1, renamed in LL2 24h later, no consumers). 4 source coverage:
-- StatsBomb / Wyscout / IDSSE / Metrica.
--
-- Coordinate system: 105x68 meters (SPADL academic standard).
-- One row per action.
```

- [ ] **Step 4: Verify dbt compiles**

Run: `uv run dbt compile --select fct_action_values 2>&1 | tail -10`

Expected: `Done.` — compiles successfully.

If it fails with `data_source IN ('statsbomb', 'wyscout')` accepted_values mismatch (because we'll add idsse/metrica to the source list in Phase 12.2), continue past for now — the YAML update in 12.2 makes that resolve.

### Task 12.2: Update `_marts__models.yml` `fct_action_values` contract

**Files:**
- Modify: `dbt_project/models/marts/_marts__models.yml:621-915` (the `fct_action_values` block)

- [ ] **Step 1: Read the current `fct_action_values` YAML block**

Run: `awk '/- name: fct_action_values/,/- name: fct_match_summary/' dbt_project/models/marts/_marts__models.yml | head -200`

Read the output to confirm structure before editing.

- [ ] **Step 2: Update `data_source` accepted_values to 4**

Find:
```yaml
      - name: data_source
        data_type: string
        description: Data provider (statsbomb, wyscout)
        data_tests:
          - not_null
          - accepted_values:
              arguments:
                values: ['statsbomb', 'wyscout']
```

Replace with:
```yaml
      - name: data_source
        data_type: string
        description: Data provider (statsbomb, wyscout, idsse, metrica)
        data_tests:
          - not_null
          - accepted_values:
              arguments:
                values: ['statsbomb', 'wyscout', 'idsse', 'metrica']
```

- [ ] **Step 3: Rename 4 columns and drop legacy `possession_team_id` entry**

In the same block, find:
```yaml
      - name: possession_id
        data_type: bigint
        description: >
          StatsBomb possession sequence number identifying the possession
          chain this action belongs to. NULL for Wyscout (no possession
          tracking in open data). LL1 (silly-kicks 1.5.0+): sourced
          upstream via the silly-kicks ``preserve_native`` kwarg, replacing
          the prior late-join to ``stg_statsbomb__events``.
      - name: possession_team_id
        data_type: bigint
        description: >
          StatsBomb-native team ID of the team in possession (LEGACY raw
          provider ID). Inside the ADR-011 dual-column window — sunset
          2026-07-22 alongside ``team_id`` / ``match_id`` /
          ``competition_id``. Use ``possession_team_key`` for new
          consumers. NULL for Wyscout.
```

Replace with:
```yaml
      - name: possession_id
        data_type: bigint
        description: >
          LL2 canonical possession sequence number — heuristic-based via
          silly-kicks ``add_possessions``. Per-match counter resetting at
          new game_id; populated for ALL data sources (StatsBomb / Wyscout
          / IDSSE / Metrica). Semantic flip from LL1 (was StatsBomb-only
          alias of statsbomb_possession_id). Consumers needing strict
          StatsBomb semantics should use ``statsbomb_possession_id``.
        data_tests:
          - not_null
      - name: action_id
        data_type: bigint
        description: >
          LL2: per-match SPADL action sequence number from silly-kicks
          ``convert_to_actions`` output (was 100% NULL pre-LL2 because the
          writer dropped it at the projection boundary). Monotonic
          non-decreasing within ``(match_id, period)``. Populated across
          all 4 sources.
        data_tests:
          - not_null
      - name: gk_role
        data_type: string
        description: >
          LL2: goalkeeper role tag (silly-kicks ``add_gk_role``). One of
          shot_stopping / cross_collection / sweeping / pick_up /
          distribution. NULL on non-GK rows. Provider-agnostic.
        data_tests:
          - accepted_values:
              arguments:
                values: ['shot_stopping', 'cross_collection', 'sweeping', 'pick_up', 'distribution']
              config:
                where: "gk_role is not null"
      - name: gk_was_distributing
        data_type: boolean
        description: >
          LL2 (silly-kicks ``add_pre_shot_gk_context``): true iff the
          defending GK had a non-keeper action in the lookback window before
          this shot. False on non-shot rows by construction.
        data_tests:
          - not_null
      - name: gk_was_engaged
        data_type: boolean
        description: >
          LL2: true iff the defending GK had a keeper_* action in the
          lookback window before this shot. False on non-shot rows.
        data_tests:
          - not_null
      - name: gk_actions_in_possession
        data_type: bigint
        description: >
          LL2: count of keeper_* actions by the defending GK in the
          lookback window. 0 on non-shot rows.
        data_tests:
          - not_null
      - name: defending_gk_player_id
        data_type: bigint
        description: >
          LL2: player ID of the defending GK identified from the most recent
          keeper_* action by a team OTHER than the shooter's team within
          the lookback window. NULL when defending GK absent or non-shot row.
      - name: statsbomb_possession_id
        data_type: bigint
        description: >
          StatsBomb-native possession sequence number (LL1 + LL2 — sourced
          via silly-kicks ``preserve_native`` from
          ``bronze.statsbomb_events``). NULL on non-StatsBomb sources.
          Pre-LL2 this column was named ``possession_id`` in the mart;
          renamed in LL2 for β-consistent canonical-vs-native naming.
      - name: statsbomb_possession_team_id
        data_type: bigint
        description: >
          StatsBomb-native team_id of the team in possession (raw provider
          int — NOT a Kimball surrogate). The ``possession_team_key``
          Kimball surrogate is the canonical FK. NULL on non-StatsBomb
          sources. Pre-LL2 this column was named ``possession_team_id``;
          renamed in LL2 (legacy ADR-011 alias closed early — column
          existed for 24h before rename, no consumers built up dependence).
      - name: statsbomb_play_pattern
        data_type: string
        description: >
          StatsBomb-native broader phase tag — one of "Regular Play",
          "From Throw In", "From Free Kick", "From Corner", "From
          Goal Kick", "From Counter", "From Keeper", "From Kick Off",
          "Other". NULL on non-StatsBomb sources. Pre-LL2 this column was
          named ``play_pattern``.
      - name: statsbomb_under_pressure
        data_type: boolean
        description: >
          StatsBomb-native pressure flag — True iff a defender was
          within ~1m of the actor at the moment of the event. NULL on
          non-StatsBomb sources. Pre-LL2 this column was named
          ``under_pressure``.
```

- [ ] **Step 4: Find and remove the existing standalone `play_pattern` and `under_pressure` entries (post-LL1)**

Search for them with:

Run: `grep -n "      - name: play_pattern\|      - name: under_pressure" dbt_project/models/marts/_marts__models.yml | head -10`

If they exist as separate entries (post-LL1 they should), delete those entries — they're now covered by the new `statsbomb_play_pattern` / `statsbomb_under_pressure` entries above. Use Edit to remove the old `play_pattern` block (lines from `- name: play_pattern` down to just before the next `- name:` entry) and the old `under_pressure` block.

- [ ] **Step 5: Verify dbt parse + contract enforcement**

Run: `uv run dbt parse 2>&1 | tail -5 && uv run dbt compile --select fct_action_values 2>&1 | tail -10`

Expected: parse `Done.`, compile `Done.`. The contract is enforced — if any column declared in YAML is absent from the SELECT, dbt fails compile.

- [ ] **Step 6: Commit**

```bash
git add dbt_project/models/marts/fct_action_values.sql dbt_project/models/marts/_marts__models.yml
git commit -m "feat(dbt): fct_action_values β-consistent shape — canonical possession_id + statsbomb_* native passthroughs + 5 GK enrichment cols + action_id (LL2)"
```

---

## Phase 13 — `fct_funnel_stages_agg` Option ii reconciliation

### Task 13.1: Replace Wyscout-synthetic-possession workaround with canonical heuristic possession_id

**Files:**
- Modify: `dbt_project/models/marts/fct_funnel_stages_agg.sql:39-52` (header comments)
- Modify: `dbt_project/models/marts/fct_funnel_stages_agg.sql:60-130` (CTEs)
- Modify: `dbt_project/models/marts/fct_funnel_stages_agg.sql:134-160` (final SELECT)

- [ ] **Step 1: Update the header comments (Wyscout synthetic-possession narrative)**

Use Edit on `dbt_project/models/marts/fct_funnel_stages_agg.sql`:

Find:
```sql
-- Wyscout handling:
--   Wyscout actions have possession_id = NULL. Current Python treats those
--   as 1 synthetic possession per match (at gs=All) or per (match, gs) at
--   gs-filter. wy_match_flag=1 if a team had any NULL-possession row during
--   THIS specific (match, team, gs); flag is per-gs (not match-level). The
--   app dedups at the driver via
--   COUNT(DISTINCT CASE WHEN wy_match_flag=1 THEN match_id END), which works
--   correctly for both gs-filtered (sees only that gs's flags) and gs=All
--   (unions all gs's flags and dedups on match_id via nunique).
--
--   Earlier design had wy_match_flag at match-level (max across gs rows),
--   but that over-counted: a match with Wyscout rows only during drawing
--   would register flag=1 on ALL gs rows, so a winning-gs query would
--   erroneously count it as a Wyscout match during winning.
```

Replace with:
```sql
-- LL2 (2026-04-29) — canonical heuristic possession_id:
--   Pre-LL2, possession_id was NULL on Wyscout rows. The Wyscout-handling
--   workaround treated each Wyscout match as 1 synthetic possession via the
--   wy_match_flag column + driver-side dedup. LL2's canonical possession_id
--   (heuristic-based, populated for all 4 sources) retires this workaround:
--   pos_in_gs and pos_in_match now COUNT(DISTINCT possession_id) directly
--   for every source. The column previously named wy_match_flag is renamed
--   to heuristic_possession_flag and retargeted on
--   statsbomb_possession_id IS NULL, capturing "this match's possessions
--   are heuristic, not provider-native" — a more general signal that also
--   covers IDSSE and Metrica matches.
--
--   Behavior change for users: the Conversion Funnel page on Wyscout
--   matches transitions from showing pos_in_gs = 0 (with synthetic
--   compensation at the driver) to showing real heuristic possession
--   counts. IDSSE / Metrica matches show in the funnel correctly out of
--   the box.
```

- [ ] **Step 2: Update the `base` CTE to select renamed columns**

Find:
```sql
with base as (

    select
        ms.match_key,
        av.match_id,
        av.competition_id,
        av.team_id,
        av.game_state,
        av.possession_id,
        av.possession_team_id,
        av.start_x,
        av.end_x,
        av.action_type,
        av.action_result,
        av.data_source,
        ms.home_team_id,
        ms.away_team_id
    from {{ ref('fct_action_values') }} av
```

Replace with:
```sql
with base as (

    select
        ms.match_key,
        av.match_id,
        av.competition_id,
        av.team_id,
        av.game_state,
        -- LL2: canonical heuristic possession_id (populated for all 4 sources)
        av.possession_id,
        -- LL2: provider-native StatsBomb-only column (NULL on non-StatsBomb)
        -- — used for the own-possession filter in own_possession CTE below
        av.statsbomb_possession_id,
        av.statsbomb_possession_team_id,
        av.start_x,
        av.end_x,
        av.action_type,
        av.action_result,
        av.data_source,
        ms.home_team_id,
        ms.away_team_id
    from {{ ref('fct_action_values') }} av
```

- [ ] **Step 3: Update the `own_possession` CTE filter to use `statsbomb_possession_team_id`**

Find:
```sql
own_possession as (

    select
        *,
        case
            when team_id = home_team_id then away_team_id
            else home_team_id
        end as opponent_team_id
    from base
    where possession_team_id is null or possession_team_id = team_id

),
```

Replace with:
```sql
own_possession as (

    select
        *,
        case
            when team_id = home_team_id then away_team_id
            else home_team_id
        end as opponent_team_id
    from base
    -- LL2: own-possession filter uses statsbomb_possession_team_id (renamed
    -- from possession_team_id pre-LL2). Branch semantics preserved:
    --   StatsBomb rows: filter to actions where this team is the one in possession
    --   Non-StatsBomb (Wyscout / IDSSE / Metrica): NULL passthrough — all rows kept,
    --   since these sources don't carry a provider-native team-of-possession concept
    where statsbomb_possession_team_id is null
       or statsbomb_possession_team_id = team_id

),
```

- [ ] **Step 4: Update `per_gs` CTE — drop the workaround, rename flag**

Find:
```sql
per_gs as (

    select
        match_key,
        match_id,
        competition_id,
        team_id,
        opponent_team_id,
        data_source,
        game_state,
        count(distinct case when possession_id is not null then possession_id end)      as pos_in_gs,
        max(case when possession_id is null then 1 else 0 end)                           as wy_match_flag,
        sum(case when start_x <= 70 and end_x > 70 then 1 else 0 end)                    as a3_entries,
        sum(case when action_type in ('shot','shot_penalty','shot_freekick') then 1 else 0 end) as shots,
        sum(case
                when action_type in ('shot','shot_penalty','shot_freekick')
                 and action_result = 'success'
                then 1 else 0
            end)                                                                         as goals
    from own_possession
    group by match_key, match_id, competition_id, team_id, opponent_team_id, data_source, game_state

),
```

Replace with:
```sql
per_gs as (

    select
        match_key,
        match_id,
        competition_id,
        team_id,
        opponent_team_id,
        data_source,
        game_state,
        -- LL2: canonical heuristic possession_id (populated for all sources)
        count(distinct possession_id)                                                    as pos_in_gs,
        -- LL2: rename of wy_match_flag, retargeted on the StatsBomb-native NULL pattern
        -- so it captures "this match's possessions are heuristic" for ALL non-StatsBomb
        -- sources (Wyscout / IDSSE / Metrica), not just Wyscout.
        max(case when statsbomb_possession_id is null then 1 else 0 end)                 as heuristic_possession_flag,
        sum(case when start_x <= 70 and end_x > 70 then 1 else 0 end)                    as a3_entries,
        sum(case when action_type in ('shot','shot_penalty','shot_freekick') then 1 else 0 end) as shots,
        sum(case
                when action_type in ('shot','shot_penalty','shot_freekick')
                 and action_result = 'success'
                then 1 else 0
            end)                                                                         as goals
    from own_possession
    group by match_key, match_id, competition_id, team_id, opponent_team_id, data_source, game_state

),
```

- [ ] **Step 5: Update `per_match` CTE**

Find:
```sql
per_match as (

    select
        match_id,
        team_id,
        count(distinct case when possession_id is not null then possession_id end) as pos_in_match
    from own_possession
    group by match_id, team_id

),
```

Replace with:
```sql
per_match as (

    select
        match_id,
        team_id,
        -- LL2: canonical heuristic possession_id (populated for all sources)
        count(distinct possession_id) as pos_in_match
    from own_possession
    group by match_id, team_id

),
```

- [ ] **Step 6: Update `final` CTE — rename flag column**

Find:
```sql
        cast(g.wy_match_flag as smallint)               as wy_match_flag,
```

Replace with:
```sql
        cast(g.heuristic_possession_flag as smallint)   as heuristic_possession_flag,
```

- [ ] **Step 7: Update `_marts__models.yml` `fct_funnel_stages_agg` block**

Find the `fct_funnel_stages_agg` YAML block and look for:
```yaml
      - name: wy_match_flag
        data_type: smallint
```

Replace with:
```yaml
      - name: heuristic_possession_flag
        data_type: smallint
        description: >
          LL2: 1 if any row for this (match, team, game_state) had
          ``statsbomb_possession_id IS NULL`` — i.e., the match's possessions
          are heuristic-based rather than provider-native. Captures Wyscout,
          IDSSE, and Metrica matches uniformly. Renamed from ``wy_match_flag``
          (Wyscout-specific) — semantics generalized as part of the
          fct_action_values β-consistent migration.
```

(If the YAML block has a description for `wy_match_flag` that mentions Wyscout-only, replace the entire entry — don't just rename — to keep the description accurate.)

- [ ] **Step 8: Update `data_source` accepted_values in `fct_funnel_stages_agg` block**

If the YAML lists `accepted_values: ['statsbomb', 'wyscout']` for `data_source`, expand to all 4 sources (mirror Phase 12.2):

Replace with:
```yaml
      - name: data_source
        data_type: string
        description: Data provider (statsbomb, wyscout, idsse, metrica)
        data_tests:
          - not_null
          - accepted_values:
              arguments:
                values: ['statsbomb', 'wyscout', 'idsse', 'metrica']
```

- [ ] **Step 9: Verify dbt compiles + tests parse**

Run: `uv run dbt parse 2>&1 | tail -5 && uv run dbt compile --select fct_funnel_stages_agg 2>&1 | tail -10`

Expected: parse `Done.`, compile `Done.`.

- [ ] **Step 10: Commit**

```bash
git add dbt_project/models/marts/fct_funnel_stages_agg.sql dbt_project/models/marts/_marts__models.yml
git commit -m "feat(dbt): fct_funnel_stages_agg Option ii reconciliation — drop Wyscout-synthetic-possession workaround, use canonical heuristic possession_id, rename wy_match_flag → heuristic_possession_flag (LL2)"
```

---

## Phase 14 — Taipy app cleanup (`hf_taipy_app/src/queries/funnel.py`)

### Task 14.1: Remove driver-side synthetic-possession compensation, rename column reference, update doc

**Files:**
- Modify: `hf_taipy_app/src/queries/funnel.py:88` (docstring) and any logic referencing `wy_match_flag`

- [ ] **Step 1: Read the current funnel.py to identify all wy_match_flag / synthetic-compensation references**

Run: `grep -n "wy_match_flag\|synthetic\|possession_id.*null\|null.*possession" hf_taipy_app/src/queries/funnel.py`

Expected: matches at line 88 (doc) plus zero or more code-level references. Read ~30 lines around each match to understand the surrounding logic.

Run: `cat hf_taipy_app/src/queries/funnel.py | head -200 | tail -150`

- [ ] **Step 2: Update the line-88 docstring**

Find:
```python
    possessions (Wyscout data has possession_id = NULL for 28.27 % of rows per V05).
```

Replace with:
```python
    possessions (LL2 canonical: heuristic possession_id populated for all sources;
    statsbomb_possession_id NULL fraction tracks "non-native possession" matches via
    the heuristic_possession_flag column on the funnel mart).
```

- [ ] **Step 3: Rename any `wy_match_flag` references to `heuristic_possession_flag`**

Use Edit with `replace_all=true` on `hf_taipy_app/src/queries/funnel.py`:

- old_string: `wy_match_flag`
- new_string: `heuristic_possession_flag`

This catches all references (column SELECTs, dictionary keys, comments).

- [ ] **Step 4: Remove driver-side synthetic-possession compensation logic**

If the file contains a code block that adds 1 synthetic possession per Wyscout match (look for keywords like `+= 1`, `synthetic`, or `wy_match_flag == 1` — the exact shape depends on the implementation), remove it. The new mart returns real heuristic possession counts, so no driver-side adjustment is needed.

If unclear what to remove, **ask the user before deleting** — be conservative; a wrong delete could regress the funnel chart on the production app. The grep at Step 1 is the discovery phase. Bring its output to the user if any non-obvious code references show up.

- [ ] **Step 5: Run any Taipy tests**

Run: `uv run pytest hf_taipy_app/tests/ -v 2>&1 | tail -30` (or whatever test path the Taipy app uses).

Expected: tests pass. If a Taipy test checks driver-side compensation logic, it'll fail and need updating to match the new (no-compensation) behavior. Update + re-run.

- [ ] **Step 6: Lint**

Run: `uv run ruff check hf_taipy_app/src/queries/funnel.py && uv run pyright hf_taipy_app/src/queries/funnel.py 2>&1 | tail -10`

Expected: clean.

- [ ] **Step 7: Commit**

```bash
git add hf_taipy_app/src/queries/funnel.py
git commit -m "feat(taipy): drop synthetic-possession compensation in funnel.py — uses canonical heuristic possession_id (LL2)"
```

---

## Phase 15 — `test_marts_live_schema.py` update

### Task 15.1: Update `_FCT_ACTION_VALUES_EXPECTED_COLS` for the post-LL2 mart shape

**Files:**
- Modify: `src/tests/test_marts_live_schema.py:62-93`

- [ ] **Step 1: Update the expected columns dict**

Use Edit on `src/tests/test_marts_live_schema.py`:

Find:
```python
_FCT_ACTION_VALUES_EXPECTED_COLS: dict[str, str] = {
    "action_value_id": "string",
    # Kimball surrogates + legacy BIGINT match_id (PR 4b, 2026-04-23).
    "match_key": "bigint",
    "competition_key": "bigint",
    "match_id": "bigint",
    # Legacy competition_id / player/team/season IDs are int per contract.
    "competition_id": "int",
    "player_id": "int",
    "team_id": "int",
    "season_id": "int",
    "period": "int",
    "time_seconds": "double",
    "minute": "int",
    "second": "int",
    "start_x": "double",
    "start_y": "double",
    "end_x": "double",
    "end_y": "double",
    "action_type": "string",
    "action_result": "string",
    "bodypart": "string",
    "offensive_value": "double",
    "defensive_value": "double",
    "vaep_value": "double",
    "possession_id": "bigint",
    "possession_team_id": "int",
    "game_state": "string",
    "data_source": "string",
    "original_event_id": "string",
    "_loaded_at": "timestamp",
}
```

Replace with:
```python
_FCT_ACTION_VALUES_EXPECTED_COLS: dict[str, str] = {
    "action_value_id": "string",
    # Kimball surrogates + legacy BIGINT match_id (PR 4b, 2026-04-23).
    "match_key": "bigint",
    "competition_key": "bigint",
    "match_id": "bigint",
    # Legacy competition_id / player/team/season IDs are int per contract.
    "competition_id": "int",
    "player_id": "int",
    "team_id": "int",
    "team_key": "bigint",
    "player_key": "bigint",
    "season_id": "int",
    "period": "int",
    "time_seconds": "double",
    "minute": "int",
    "second": "int",
    "start_x": "double",
    "start_y": "double",
    "end_x": "double",
    "end_y": "double",
    "action_type": "string",
    "action_result": "string",
    "bodypart": "string",
    "offensive_value": "double",
    "defensive_value": "double",
    "vaep_value": "double",
    # LL2 canonical post-conversion enrichments
    "action_id": "bigint",
    "possession_id": "bigint",
    "gk_role": "string",
    "gk_was_distributing": "boolean",
    "gk_was_engaged": "boolean",
    "gk_actions_in_possession": "bigint",
    "defending_gk_player_id": "bigint",
    # LL2 provider-native passthroughs (renamed from non-prefixed aliases pre-LL2)
    "statsbomb_possession_id": "bigint",
    "statsbomb_possession_team_id": "bigint",
    "statsbomb_play_pattern": "string",
    "statsbomb_under_pressure": "boolean",
    # Kimball surrogate (plain-named per Kimball convention)
    "possession_team_key": "bigint",
    "game_state": "string",
    "data_source": "string",
    "original_event_id": "string",
    "_loaded_at": "timestamp",
}
```

- [ ] **Step 2: Run the live mart test**

Run: `uv run pytest src/tests/test_marts_live_schema.py -v`

Expected: skips when `DATABRICKS_*` env vars absent. With env vars present, **the test will FAIL until Phases 17 + 21 (run ALTER + dbt full-refresh against live Databricks)** because the live mart still has the pre-LL2 shape. This is expected — the test asserts the post-LL2 target shape; achieving it requires the migration in Phase 17 and dbt rebuild in Phase 24.

Document this in the commit message — the test going green is the explicit success metric for Phase 24's validation.

- [ ] **Step 3: Lint + types**

Run: `uv run ruff check src/tests/test_marts_live_schema.py && uv run pyright src/tests/test_marts_live_schema.py`

Expected: clean.

- [ ] **Step 4: Commit**

```bash
git add src/tests/test_marts_live_schema.py
git commit -m "test(marts): update _FCT_ACTION_VALUES_EXPECTED_COLS for β-consistent LL2 mart shape (will pass post-backfill)"
```

---

## Phase 16 — Wheel + dep bump

### Task 16.1: Bump version + silly-kicks dep, sync wheel consumers

**Files:**
- Modify: `pyproject.toml`
- Auto-modified: 22 PEP 723 + Terraform consumers + `src/shared/wheel.py` (via `scripts/bump_wheel.py`)

- [ ] **Step 1: Update version + silly-kicks pin**

Use Edit on `pyproject.toml`:

Find: `version = "0.3.20"`

Replace with: `version = "0.3.21"`

Find: `"silly-kicks>=1.5.0,<2.0",`

Replace with: `"silly-kicks>=1.7.0,<2.0",`

- [ ] **Step 2: Run `bump_wheel.py` to sync 22 consumers**

Run: `uv run python scripts/bump_wheel.py`

Expected output: prints the list of files updated (PEP 723 script headers, Terraform `wheel_path` strings, `deploy.sh`, `src/shared/wheel.py`). Should report ~22-23 files updated.

- [ ] **Step 3: Verify the sync was complete**

Run: `git diff --stat | tail -30`

Expected: `pyproject.toml` plus ~22 other files modified, all with version-string changes only (no logic changes).

Run: `git grep -l "0.3.20" -- ":!docs/**" ":!*.md" 2>&1 | head` (should return nothing other than CHANGELOG-style historical references in spec/plan docs).

- [ ] **Step 4: Verify `uv sync` resolves the new dep**

Run: `uv sync --extra analytics 2>&1 | tail -10`

Expected: resolves silly-kicks 1.7.0 from PyPI.

- [ ] **Step 5: Re-run unit tests under the new dep**

Run: `uv run pytest src/tests/test_spadl_enrichments.py src/tests/test_spadl_vaep_writer_parity.py -v`

Expected: all pass.

- [ ] **Step 6: Commit**

```bash
git add -A
git commit -m "chore: wheel 0.3.20 → 0.3.21, silly-kicks dep ≥1.5 → ≥1.7 (sync 22 consumers)"
```

---

## Phase 17 — Bronze migration script

### Task 17.1: Write `scripts/migrate_bronze_for_pr_ll2.py`

**Files:**
- Create: `scripts/migrate_bronze_for_pr_ll2.py`

- [ ] **Step 1: Create the idempotent ALTER script**

Create `scripts/migrate_bronze_for_pr_ll2.py`:

```python
#!/usr/bin/env python3
"""PR-LL2 bronze schema migration — idempotent ALTER-or-noop.

Pattern lifted from scripts/maintain_synced_tables.py (CLAUDE.md-blessed
for idempotent post-creation schema repair).

Adds to bronze.spadl_actions (10 columns):
    - 4 statsbomb_* columns missed by PR-LL1's ALTER (vaep_action_values
      received them; spadl_actions did not)
    - 6 LL2 enrichment columns from apply_spadl_enrichments

Adds to bronze.vaep_action_values (7 columns):
    - action_id (newly surfaced from silly-kicks convert_to_actions)
    - 6 LL2 enrichment columns

Re-runnable: queries DESCRIBE TABLE first; only ALTERs columns absent
from the live table. Safe to run during PR development.

Usage:
    uv run python scripts/migrate_bronze_for_pr_ll2.py
        [--catalog soccer_analytics]
        [--schema bronze]
        [--dry-run]

Required env: DATABRICKS_HOST, DATABRICKS_TOKEN.
"""

from __future__ import annotations

import argparse
import os
import sys
from typing import Final

from databricks import sql

# Target column lists (column_name -> Spark type DDL fragment)
_SPADL_ACTIONS_TARGET: Final[dict[str, str]] = {
    # PR-LL1 statsbomb_* — should be on vaep_action_values already, missing on spadl_actions
    "statsbomb_possession_id": "BIGINT",
    "statsbomb_possession_team_id": "BIGINT",
    "statsbomb_play_pattern": "STRING",
    "statsbomb_under_pressure": "BOOLEAN",
    # LL2 enrichment columns
    "possession_id_heuristic": "BIGINT",
    "gk_role": "STRING",
    "gk_was_distributing": "BOOLEAN",
    "gk_was_engaged": "BOOLEAN",
    "gk_actions_in_possession": "BIGINT",
    "defending_gk_player_id": "BIGINT",
}

_VAEP_ACTION_VALUES_TARGET: Final[dict[str, str]] = {
    # LL2: action_id surfaced through to vaep_action_values
    "action_id": "BIGINT",
    # LL2 enrichment columns
    "possession_id_heuristic": "BIGINT",
    "gk_role": "STRING",
    "gk_was_distributing": "BOOLEAN",
    "gk_was_engaged": "BOOLEAN",
    "gk_actions_in_possession": "BIGINT",
    "defending_gk_player_id": "BIGINT",
}


def _describe_columns(cur: object, table_fqn: str) -> set[str]:
    """Return the set of column names currently in the live Delta table."""
    cur.execute(f"DESCRIBE TABLE {table_fqn}")  # type: ignore[attr-defined]
    rows = cur.fetchall()  # type: ignore[attr-defined]
    return {r[0] for r in rows if r[0] and not r[0].startswith("#")}


def _alter_table(
    cur: object,
    table_fqn: str,
    target_columns: dict[str, str],
    *,
    dry_run: bool,
) -> int:
    """Add any missing columns from target_columns to table_fqn. Returns count added."""
    actual = _describe_columns(cur, table_fqn)
    missing = {col: ddl for col, ddl in target_columns.items() if col not in actual}

    if not missing:
        print(f"  {table_fqn}: already at target schema (no changes)")
        return 0

    cols_sql = ", ".join(f"{col} {ddl_type}" for col, ddl_type in missing.items())
    alter_sql = f"ALTER TABLE {table_fqn} ADD COLUMNS ({cols_sql})"
    print(f"  {table_fqn}: ADD COLUMNS — {len(missing)} columns: {sorted(missing.keys())}")

    if dry_run:
        print(f"    [DRY RUN] would execute: {alter_sql}")
        return 0

    cur.execute(alter_sql)  # type: ignore[attr-defined]
    print(f"    ALTER applied successfully")
    return len(missing)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    parser.add_argument("--catalog", default="soccer_analytics")
    parser.add_argument("--schema", default="bronze")
    parser.add_argument("--http-path", default="/sql/1.0/warehouses/6c3b36ca64d183fe")
    parser.add_argument("--dry-run", action="store_true", help="Show planned ALTERs without executing them")
    args = parser.parse_args()

    host_env = os.environ.get("DATABRICKS_HOST", "")
    token = os.environ.get("DATABRICKS_TOKEN", "")
    if not host_env or not token:
        print("ERROR: DATABRICKS_HOST and DATABRICKS_TOKEN must be set", file=sys.stderr)
        return 1

    host = host_env.replace("https://", "").rstrip("/")

    print(f"PR-LL2 bronze migration — catalog={args.catalog} schema={args.schema} dry_run={args.dry_run}")
    total_added = 0
    with sql.connect(server_hostname=host, http_path=args.http_path, access_token=token) as conn:
        with conn.cursor() as cur:
            for table_name, target in [
                ("spadl_actions", _SPADL_ACTIONS_TARGET),
                ("vaep_action_values", _VAEP_ACTION_VALUES_TARGET),
            ]:
                fqn = f"{args.catalog}.{args.schema}.{table_name}"
                added = _alter_table(cur, fqn, target, dry_run=args.dry_run)
                total_added += added

            # Verify post-alter
            print("\nPost-migration verification:")
            for table_name, target in [
                ("spadl_actions", _SPADL_ACTIONS_TARGET),
                ("vaep_action_values", _VAEP_ACTION_VALUES_TARGET),
            ]:
                fqn = f"{args.catalog}.{args.schema}.{table_name}"
                actual = _describe_columns(cur, fqn)
                missing = sorted(set(target.keys()) - actual)
                if missing and not args.dry_run:
                    print(f"  ERROR: {fqn} still missing {missing} after ALTER", file=sys.stderr)
                    return 1
                print(f"  {fqn}: {len(actual)} cols, all target columns present")

    print(f"\nTotal columns added: {total_added}")
    if args.dry_run and total_added == 0:
        print("(Dry-run mode — no changes applied)")
    return 0


if __name__ == "__main__":
    sys.exit(main())
```

- [ ] **Step 2: Run lint + types**

Run: `uv run ruff check scripts/migrate_bronze_for_pr_ll2.py && uv run pyright scripts/migrate_bronze_for_pr_ll2.py`

Expected: clean.

- [ ] **Step 3: Test dry-run mode against live Databricks**

Run: `uv run --with databricks-sql-connector python scripts/migrate_bronze_for_pr_ll2.py --dry-run`

Expected output (approximate):
```
PR-LL2 bronze migration — catalog=soccer_analytics schema=bronze dry_run=True
  soccer_analytics.bronze.spadl_actions: ADD COLUMNS — 10 columns: ['defending_gk_player_id', 'gk_actions_in_possession', 'gk_role', 'gk_was_distributing', 'gk_was_engaged', 'possession_id_heuristic', 'statsbomb_play_pattern', 'statsbomb_possession_id', 'statsbomb_possession_team_id', 'statsbomb_under_pressure']
    [DRY RUN] would execute: ALTER TABLE soccer_analytics.bronze.spadl_actions ADD COLUMNS (...)
  soccer_analytics.bronze.vaep_action_values: ADD COLUMNS — 7 columns: ['action_id', 'defending_gk_player_id', 'gk_actions_in_possession', 'gk_role', 'gk_was_distributing', 'gk_was_engaged', 'possession_id_heuristic']

Post-migration verification:
  soccer_analytics.bronze.spadl_actions: 19 cols, all target columns present
    [WARNING — dry-run completed without applying ALTER]
```

Note: in dry-run mode, the post-verification step will report missing columns since no ALTER fired. That's fine for dry-run; the actual run in Phase 21 verifies clean.

- [ ] **Step 4: Commit**

```bash
git add scripts/migrate_bronze_for_pr_ll2.py
git commit -m "feat(scripts): idempotent bronze migration script for PR-LL2 (10 + 7 columns)"
```

---

## Phase 18 — Post-deploy validation scripts

### Task 18.1: Write `scripts/validate_pr_ll2_post_deploy.py`

**Files:**
- Create: `scripts/validate_pr_ll2_post_deploy.py`

- [ ] **Step 1: Create the validation script**

Create `scripts/validate_pr_ll2_post_deploy.py`:

```python
#!/usr/bin/env python3
"""PR-LL2 post-deploy validation — non-NULL counts of all 11 new columns.

Runs after the combined backfill (Phase 23 of the runbook). Asserts the
expected-population semantics for each new column on the live
bronze.vaep_action_values table:
    - statsbomb_*  : populated for StatsBomb rows; NULL for non-StatsBomb
    - action_id    : populated on every row (was 100% NULL pre-LL2)
    - possession_id_heuristic : populated on every row across all 4 sources
    - gk_role      : populated on a small fraction (only GK action rows)
    - gk_was_*     : populated on shot rows (False default elsewhere)

Exits 0 on success, 1 on any expected-population check failing. Logs a
per-source breakdown for observability.

Usage:
    uv run python scripts/validate_pr_ll2_post_deploy.py
"""

from __future__ import annotations

import os
import sys

from databricks import sql

_TABLE = "soccer_analytics.bronze.vaep_action_values"


def _query_counts(cur: object) -> list[tuple]:
    """Return (data_source, total, sb_pop, action_id_pop, hpos_pop, gk_role_pop)."""
    sql_query = f"""
    SELECT
      data_source,
      COUNT(*) total,
      COUNT(statsbomb_possession_id) sb_pop,
      COUNT(action_id) action_id_pop,
      COUNT(possession_id_heuristic) hpos_pop,
      COUNT(gk_role) gk_role_pop,
      SUM(CASE WHEN gk_was_engaged THEN 1 ELSE 0 END) engaged_count
    FROM {_TABLE}
    GROUP BY data_source
    ORDER BY data_source
    """
    cur.execute(sql_query)  # type: ignore[attr-defined]
    return cur.fetchall()  # type: ignore[attr-defined]


def main() -> int:
    host_env = os.environ.get("DATABRICKS_HOST", "")
    token = os.environ.get("DATABRICKS_TOKEN", "")
    if not host_env or not token:
        print("ERROR: DATABRICKS_HOST and DATABRICKS_TOKEN required", file=sys.stderr)
        return 1
    host = host_env.replace("https://", "").rstrip("/")

    print(f"PR-LL2 post-deploy validation against {_TABLE}\n")
    failures: list[str] = []
    with sql.connect(
        server_hostname=host,
        http_path="/sql/1.0/warehouses/6c3b36ca64d183fe",
        access_token=token,
    ) as conn:
        with conn.cursor() as cur:
            rows = _query_counts(cur)

    print(f"{'source':12s} {'total':>12s} {'sb_pop':>12s} {'action_id_pop':>14s} {'hpos_pop':>12s} {'gk_role_pop':>12s} {'engaged':>10s}")
    print("-" * 86)

    for row in rows:
        source, total, sb_pop, action_id_pop, hpos_pop, gk_role_pop, engaged = row
        print(
            f"{source:12s} {total:>12,d} {sb_pop:>12,d} {action_id_pop:>14,d} "
            f"{hpos_pop:>12,d} {gk_role_pop:>12,d} {engaged:>10,d}"
        )

        # Per-source assertions
        if total == 0:
            failures.append(f"{source}: zero rows (expected >0)")
            continue

        # action_id should be populated on every row (LL1 latent gap closed)
        if action_id_pop < total * 0.99:
            failures.append(
                f"{source}: action_id populated {action_id_pop:,}/{total:,} (<99%) — "
                f"LL1 latent gap not closed"
            )

        # possession_id_heuristic: populated on every row (canonical, all sources)
        if hpos_pop < total * 0.99:
            failures.append(
                f"{source}: possession_id_heuristic populated {hpos_pop:,}/{total:,} (<99%)"
            )

        # statsbomb_possession_id: populated only on StatsBomb rows
        if source == "statsbomb":
            if sb_pop < total * 0.95:
                failures.append(
                    f"statsbomb: statsbomb_possession_id populated {sb_pop:,}/{total:,} (<95%) — "
                    f"LL1 vaep_schema gap not closed (was 0/7M pre-LL2)"
                )
        elif source in ("wyscout", "idsse", "metrica"):
            if sb_pop > 0:
                failures.append(
                    f"{source}: statsbomb_possession_id populated {sb_pop:,} on non-StatsBomb source (expected 0)"
                )

    print()
    if failures:
        print("VALIDATION FAILED:")
        for msg in failures:
            print(f"  - {msg}")
        return 1

    print("VALIDATION PASSED — all expected-population semantics hold")
    return 0


if __name__ == "__main__":
    sys.exit(main())
```

- [ ] **Step 2: Lint + types**

Run: `uv run ruff check scripts/validate_pr_ll2_post_deploy.py && uv run pyright scripts/validate_pr_ll2_post_deploy.py`

Expected: clean.

### Task 18.2: Write `scripts/measure_boundary_f1_full_corpus.py`

**Files:**
- Create: `scripts/measure_boundary_f1_full_corpus.py`

- [ ] **Step 1: Create the empirical F1 measurement script**

Create `scripts/measure_boundary_f1_full_corpus.py`:

```python
#!/usr/bin/env python3
"""PR-LL2 empirical boundary-F1 on the full StatsBomb corpus.

Runs post-deploy (one-shot, not gating). Loads every StatsBomb match's
SPADL actions from bronze.vaep_action_values, computes per-match
boundary-F1 between possession_id_heuristic and statsbomb_possession_id,
prints per-competition breakdown, and writes a timestamped log.

Re-run quarterly or after any silly-kicks add_possessions algorithm change
to detect drift.

Usage:
    uv run python scripts/measure_boundary_f1_full_corpus.py
    [--out logs/boundary_f1_<timestamp>.json]
"""

from __future__ import annotations

import argparse
import json
import os
import sys
from datetime import datetime, timezone
from pathlib import Path

import numpy as np
import pandas as pd
from databricks import sql


def _boundary_f1(heuristic: pd.Series, native: pd.Series) -> float:
    """Boundary F1 between two possession-id sequences. Invariant under counter relabeling."""
    h_changes = heuristic.ne(heuristic.shift(1)).iloc[1:].to_numpy()
    n_changes = native.ne(native.shift(1)).iloc[1:].to_numpy()
    tp = int((h_changes & n_changes).sum())
    fp = int((h_changes & ~n_changes).sum())
    fn = int((~h_changes & n_changes).sum())
    precision = tp / (tp + fp) if (tp + fp) > 0 else 0.0
    recall = tp / (tp + fn) if (tp + fn) > 0 else 0.0
    if precision + recall == 0:
        return 0.0
    return 2 * precision * recall / (precision + recall)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    parser.add_argument("--out", default=None, help="Output JSON path; defaults to logs/boundary_f1_<timestamp>.json")
    parser.add_argument("--catalog", default="soccer_analytics")
    parser.add_argument("--schema", default="bronze")
    args = parser.parse_args()

    host_env = os.environ.get("DATABRICKS_HOST", "")
    token = os.environ.get("DATABRICKS_TOKEN", "")
    if not host_env or not token:
        print("ERROR: DATABRICKS_HOST and DATABRICKS_TOKEN required", file=sys.stderr)
        return 1
    host = host_env.replace("https://", "").rstrip("/")

    fetch_sql = f"""
    SELECT
        match_id,
        competition_id,
        period_id,
        action_id,
        possession_id_heuristic,
        statsbomb_possession_id
    FROM {args.catalog}.{args.schema}.vaep_action_values
    WHERE data_source = 'statsbomb'
      AND statsbomb_possession_id IS NOT NULL
      AND possession_id_heuristic IS NOT NULL
    ORDER BY match_id, period_id, action_id
    """

    print("Fetching StatsBomb SPADL actions from Databricks...")
    with sql.connect(
        server_hostname=host,
        http_path="/sql/1.0/warehouses/6c3b36ca64d183fe",
        access_token=token,
    ) as conn:
        df = pd.read_sql(fetch_sql, conn)
    print(f"  fetched {len(df):,} rows / {df['match_id'].nunique():,} matches / "
          f"{df['competition_id'].nunique():,} competitions")

    f1_per_match: dict[int, float] = {}
    f1_per_competition: dict[int, list[float]] = {}

    for match_id, mdf in df.groupby("match_id"):
        f1 = _boundary_f1(
            mdf["possession_id_heuristic"],
            mdf["statsbomb_possession_id"].astype(np.int64),
        )
        f1_per_match[int(match_id)] = f1
        comp = int(mdf["competition_id"].iloc[0])
        f1_per_competition.setdefault(comp, []).append(f1)

    print("\nPer-competition boundary-F1 (median across matches):")
    print(f"{'competition_id':>15s} {'matches':>10s} {'median_f1':>12s} {'min_f1':>10s} {'max_f1':>10s}")
    print("-" * 60)
    for comp in sorted(f1_per_competition):
        vals = f1_per_competition[comp]
        median = float(np.median(vals))
        print(f"{comp:>15d} {len(vals):>10d} {median:>12.4f} {min(vals):>10.4f} {max(vals):>10.4f}")

    overall_median = float(np.median(list(f1_per_match.values())))
    overall_mean = float(np.mean(list(f1_per_match.values())))
    print(f"\nOverall median: {overall_median:.4f}")
    print(f"Overall mean:   {overall_mean:.4f}")

    # Write JSON log
    out_path = Path(args.out) if args.out else Path("logs") / f"boundary_f1_{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ')}.json"
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(json.dumps({
        "timestamp_utc": datetime.now(timezone.utc).isoformat(),
        "total_matches": len(f1_per_match),
        "total_actions": len(df),
        "overall_median_f1": overall_median,
        "overall_mean_f1": overall_mean,
        "per_competition_median": {str(c): float(np.median(v)) for c, v in f1_per_competition.items()},
        "per_match_f1": {str(m): f for m, f in f1_per_match.items()},
    }, indent=2))
    print(f"\nLog written to {out_path}")

    if overall_median < 0.80:
        print(f"WARN: overall median F1 {overall_median:.4f} < 0.80 — investigate before next training cycle")
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
```

- [ ] **Step 2: Lint + types**

Run: `uv run ruff check scripts/measure_boundary_f1_full_corpus.py && uv run pyright scripts/measure_boundary_f1_full_corpus.py`

Expected: clean.

- [ ] **Step 3: Commit both validation scripts**

```bash
git add scripts/validate_pr_ll2_post_deploy.py scripts/measure_boundary_f1_full_corpus.py
git commit -m "feat(scripts): post-deploy validation + empirical F1 measurement scripts (LL2)"
```

---

## Phase 19 — ADRs + documentation

### Task 19.1: Write ADR-016

**Files:**
- Create: `docs/superpowers/adrs/ADR-016-spadl-enrichment-stage-canonical-naming.md`

- [ ] **Step 1: Read the ADR template**

Run: `cat docs/superpowers/adrs/ADR-TEMPLATE.md`

Use the template's section headers (Context / Decision / Consequences) as the structure.

- [ ] **Step 2: Create ADR-016**

Create `docs/superpowers/adrs/ADR-016-spadl-enrichment-stage-canonical-naming.md`:

```markdown
# ADR-016: SPADL post-conversion enrichment stage and canonical/native naming convention

**Status:** Accepted (PR-LL2, 2026-04-29)
**Deciders:** Karsten S. Nielsen
**Related:** ADR-002 (silent exception swallow elimination, writer/DDL parity), ADR-011 (Kimball surrogate key migration), PR-LL1 (silly-kicks 1.5.0 preserve_native), PR-LL2

## Context

silly-kicks (luxury-lakehouse's SPADL/VAEP/xT toolkit) ships a family of
provider-agnostic post-conversion enrichment helpers in
``silly_kicks.spadl.utils``: ``add_possessions``, ``add_names``,
``add_gk_role``, ``add_gk_distribution_metrics``,
``add_pre_shot_gk_context``. Each helper takes a canonical SPADL action
DataFrame and returns it with new columns appended. They are pure pandas,
have no provider-specific branching, and silly-kicks owns their unit tests.

Pre-LL2, luxury-lakehouse called ``silly_kicks.spadl.add_names`` ad-hoc
inside the VAEP scoring UDF (``spadl_vaep.py:352``). Each new helper added
later (``add_possessions``, the GK suite) faced the same scaffolding
problem: where to call it, where to declare its output columns in the
schema, where to project them in each provider's UDF, and how to ensure
all 4 source UDFs (StatsBomb / Wyscout / IDSSE / Metrica) stay in sync.

PR-LL2 needs to wire in 3 helpers (``add_possessions``, ``add_gk_role``,
``add_pre_shot_gk_context``). Without an architectural pattern, each one
would touch every provider UDF independently, schemas would drift across
the 4 paths, and writer/DDL parity would have to be re-verified per
helper.

PR-LL2 also discovered three latent gaps from PR-LL1:
1. ``bronze.spadl_actions.action_id`` declared but 100% NULL on every row
   (writer dropped the column at projection)
2. ``vaep_schema`` StructType in ``_make_scoring_udf`` did not include the
   PR-LL1 ``statsbomb_*`` columns — ``applyInPandas`` silently dropped them
   at the boundary; 0 of 7,151,510 StatsBomb rows had non-NULL
   ``statsbomb_possession_id``
3. ``bronze.spadl_actions`` was missing the 4 ``statsbomb_*`` columns
   physically (PR-LL1's ALTER touched only ``vaep_action_values``)

These gaps share a root cause: no test enforces parity between the
applyInPandas StructType emitted by each writer and the DDL constants
declared in ``_SPADL_SCHEMA`` / ``_VAEP_SCHEMA``. ADR-002 §4 establishes
the writer/target schema drift guard pattern for telemetry writers; the
SPADL/VAEP pipeline did not yet have it.

A separate but parallel decision: PR-LL1 introduced 4 StatsBomb-native
columns (``statsbomb_possession_id``, ``statsbomb_possession_team_id``,
``statsbomb_play_pattern``, ``statsbomb_under_pressure``) at the bronze
layer with provider-namespaced names, but aliased them to non-prefixed
names at the mart layer (``possession_id``, ``possession_team_id``,
``play_pattern``, ``under_pressure``). This created a naming inconsistency:
provider-agnostic enrichments and provider-native passthroughs were
indistinguishable in the mart by name alone. Future cross-source
consumers would have to memorize "possession_id is StatsBomb-only,
possession_id_heuristic is multi-source" semantics.

## Decision

### Decision 1 — Named SPADL post-conversion enrichment stage

Establish ``apply_spadl_enrichments(actions: pd.DataFrame, *, source: str) -> pd.DataFrame``
as a dedicated module at ``src/ingestion/spadl_enrichments.py``. The
function calls silly-kicks's provider-agnostic enrichment helpers in a
defined order. Every per-provider SPADL UDF in
``src/ingestion/spadl_conversion.py`` calls it after
``convert_to_actions(...)`` and before the column projection. Future
helpers slot into the function with a one-line addition; the column
declaration goes into ``_SPADL_SCHEMA`` and ``_VAEP_SCHEMA`` once and is
enforced by writer/DDL parity tests across all 4 source UDFs.

The module is pure pandas (silly-kicks dep only), testable without Spark.

### Decision 2 — Canonical/native naming convention

Apply the following naming rule across luxury-lakehouse's SPADL pipeline
(bronze, staging, mart):

| Origin of column value | Naming convention | Population |
|---|---|---|
| Computed post-conversion enrichment (deterministic from canonical SPADL) | Plain canonical name: ``possession_id``, ``gk_role``, ``action_id`` | Always populated for all sources |
| Provider-native passthrough | ``<provider>_<field>``: ``statsbomb_possession_id``, ``statsbomb_play_pattern`` | NULL on sources without that provider's native concept |
| Kimball surrogate FK | ``<entity>_key``: ``match_key``, ``possession_team_key`` | Plain (Kimball convention wins). Population follows underlying native data |
| Legacy native ID inside ADR-011 dual-column window | ``<entity>_id``: ``match_id``, ``team_id`` | Always populated; sunset 2026-07-22 |

The mart drops all aliasing — provider-namespaced columns surface to the
mart with their full prefix (``statsbomb_possession_id``, etc.). PR-LL1's
``possession_id``/``possession_team_id``/``play_pattern``/``under_pressure``
mart-level aliases are renamed to their bronze names; LL2 introduces a
new canonical ``possession_id`` (heuristic-based, populated for all sources).

### Decision 3 — Writer/DDL parity tests across all 4 source UDFs and the VAEP scoring UDF

``test_spadl_vaep_writer_parity.py`` is extended to assert the
applyInPandas StructType emitted by each of the 4 source SPADL UDFs
(StatsBomb / Wyscout / IDSSE / Metrica) and the VAEP scoring UDF
(``_make_scoring_udf``) match the corresponding DDL constants
(``_SPADL_SCHEMA``, ``_VAEP_SCHEMA``) column-for-column with type parity.
This closes the LL1 latent-bug class — applyInPandas can no longer
silently drop columns declared in DDL but absent from the StructType.

## Consequences

### Positive

- New silly-kicks helpers integrate via a one-line addition to
  ``apply_spadl_enrichments`` plus column declarations in the schema
  constants. No per-provider UDF surgery beyond initial wiring.
- All 4 source UDFs produce semantically equivalent SPADL output
  (silly-kicks 1.7.0's ``_fix_direction_of_play`` unification supports
  this; the enrichment stage operates on the unified output).
- Writer/DDL parity tests prevent the LL1 latent-bug class from
  recurring. Schema drift between code and storage is visible at unit-test
  time, not at production-deployment time.
- Provider-namespaced columns and computed enrichments are
  distinguishable by name alone — consumers never need a glossary to
  understand a column's source.
- Future providers (e.g., Opta when re-enabled, or SkillCorner if events
  are ingested) follow the same pattern: dedicated UDF + adapter +
  apply_spadl_enrichments call. No bespoke logic per provider.

### Negative

- The LL1 ``possession_id`` mart-level alias was renamed 24 hours after
  it landed. ADR-011 footnote captures the early sunset of
  ``possession_team_id``. This is a Hyrum's-law concession but acceptable
  given the column had no time to accrue downstream consumers (verified
  via grep across luxury-lakehouse, hf_taipy_app/, and dbt_project/).
- Per-helper output column dtypes (e.g., silly-kicks's
  ``defending_gk_player_id`` as float64-with-NaN, ``gk_role`` as
  ``pd.Categorical``) require explicit casting in each UDF before the
  Spark applyInPandas boundary. Documented in
  ``apply_spadl_enrichments``'s call sites.
- ``apply_spadl_enrichments`` adds a small per-match overhead inside the
  applyInPandas closure (silly-kicks's helpers run on the per-match
  group). For 9.6M rows across ~5,400 matches, the overhead is
  measurable but acceptable; benchmarked during TDD.

### Neutral

- silly-kicks owns the algorithm-level golden tests for each helper;
  luxury-lakehouse's tests focus on integration (writer parity, smoke +
  plausibility, boundary-F1 against StatsBomb's native possession_id as
  ground truth for ``add_possessions`` specifically). Test coverage is
  layered, not duplicated.

## Implementation reference

- New module: ``src/ingestion/spadl_enrichments.py``
- Wired in: ``src/ingestion/spadl_conversion.py`` (4 UDFs), ``src/ingestion/spadl_vaep.py`` (VAEP scoring UDF + DDL constants)
- Mart shape: ``dbt_project/models/marts/fct_action_values.sql`` + ``dbt_project/models/marts/_marts__models.yml``
- Writer parity tests: ``src/tests/test_spadl_vaep_writer_parity.py``
- Funnel mart Option ii reconciliation: ``dbt_project/models/marts/fct_funnel_stages_agg.sql`` + ``hf_taipy_app/src/queries/funnel.py``
- Bronze migration: ``scripts/migrate_bronze_for_pr_ll2.py``
- Spec: ``docs/superpowers/specs/2026-04-29-pr-ll2-spadl-enrichment-stage-design.md``
```

### Task 19.2: Append ADR-011 footnote

**Files:**
- Modify: `docs/superpowers/adrs/ADR-011-unified-kimball-match-dimension.md`

- [ ] **Step 1: Append the LL2 footnote**

Open `docs/superpowers/adrs/ADR-011-unified-kimball-match-dimension.md`, scroll to the end, and append:

```markdown

---

## 2026-04-29 (PR-LL2) — `possession_team_id` legacy alias closed early

PR-LL1 (2026-04-28) introduced `possession_team_id` in `fct_action_values`
as an alias of the bronze column `statsbomb_possession_team_id`, intended
to live inside the standard 90-day dual-column window through 2026-07-22
alongside the original Kimball-migration legacy columns (`team_id`,
`match_id`, `competition_id`, `player_id`, `season_id`).

PR-LL2 (2026-04-29) renamed the mart-level alias to its bronze name
`statsbomb_possession_team_id` as part of the β-consistent SPADL
post-conversion enrichment naming rule (see ADR-016), dropping the alias
24 hours after introduction. Acceptable in this specific case because the
column had no time to accrue downstream consumers — `hf_taipy_app/`,
`src/`, and `dbt_project/` greps confirm zero matches at the time of rename.

The 90-day window remains in force for the original ADR-011 legacy columns;
sunset date 2026-07-22 unchanged.
```

### Task 19.3: Update `docs/engineering/conventions.md`

**Files:**
- Modify: `docs/engineering/conventions.md`

- [ ] **Step 1: Add a SPADL Pipeline subsection**

Find an appropriate insertion point in `conventions.md` (likely near the existing dbt or workflow sections). Append:

```markdown

### SPADL Pipeline

- **Post-conversion enrichments live in `src/ingestion/spadl_enrichments.py`.**
  New silly-kicks helpers (e.g., `add_possessions`, `add_gk_role`) are added
  by extending the `apply_spadl_enrichments` function, declaring their output
  columns in `_SPADL_SCHEMA` + `_VAEP_SCHEMA` (in `src/ingestion/spadl_vaep.py`),
  and adding them to each provider UDF's `_spadl_cols` projection +
  applyInPandas StructType. Writer/DDL parity tests in
  `src/tests/test_spadl_vaep_writer_parity.py` enforce the column-for-column
  match across all 4 source UDFs + the VAEP scoring UDF — this is the
  defense against the LL1 latent-bug class (silent applyInPandas drops).
- **Naming rule (ADR-016)**: provider-native passthroughs use
  `<provider>_<field>` everywhere (bronze, staging, mart) — e.g.,
  `statsbomb_possession_id`. Computed enrichments use plain canonical
  names (`possession_id`, `gk_role`). Kimball surrogate FKs use
  `<entity>_key` (`match_key`, `possession_team_key`) per Kimball
  convention. Mart never aliases columns — what's in bronze surfaces with
  the same name in the mart.
```

### Task 19.4: Add CLAUDE.md reference to ADR-016

**Files:**
- Modify: `CLAUDE.md` (project root)

- [ ] **Step 1: Add a one-line reference to ADR-016**

Find an appropriate location in `CLAUDE.md` § Architecture Principles or § ADR-related sections. Append:

```markdown
- **SPADL post-conversion enrichments use the named `apply_spadl_enrichments` stage** in `src/ingestion/spadl_enrichments.py` per [ADR-016](docs/superpowers/adrs/ADR-016-spadl-enrichment-stage-canonical-naming.md). Canonical-vs-native naming convention enforced: computed enrichments use plain canonical names; provider-native passthroughs use `<provider>_<field>` everywhere.
```

- [ ] **Step 2: Commit all docs changes**

```bash
git add docs/superpowers/adrs/ADR-016-spadl-enrichment-stage-canonical-naming.md docs/superpowers/adrs/ADR-011-unified-kimball-match-dimension.md docs/engineering/conventions.md CLAUDE.md
git commit -m "docs: ADR-016 (SPADL enrichment stage + naming convention) + ADR-011 footnote + conventions/CLAUDE.md updates"
```

---

## Phase 20 — Final pre-merge verification

### Task 20.1: Run the complete local test + lint + types + dbt parse

**Files:** none modified — verification only.

- [ ] **Step 1: Full ruff check**

Run: `uv run ruff check src/ scripts/`

Expected: clean.

- [ ] **Step 2: Full ruff format check**

Run: `uv run ruff format --check src/ scripts/`

Expected: clean.

- [ ] **Step 3: Pyright type check**

Run: `uv run pyright src/`

Expected: clean (basic mode, 0 errors).

- [ ] **Step 4: Full pytest sweep**

Run: `uv run pytest src/tests/ -v --tb=short 2>&1 | tail -40`

Expected: all PASS. Note that `test_marts_live_schema.py` will SKIP without `DATABRICKS_*` env vars; with them present, it FAILS until Phase 21+24 (this is documented and expected).

- [ ] **Step 5: dbt parse + compile**

Run: `uv run dbt parse 2>&1 | tail -5 && uv run dbt compile --select fct_action_values+ fct_funnel_stages_agg+ stg_spadl__action_values 2>&1 | tail -5`

Expected: parse `Done.`, compile `Done.` for all selected models.

- [ ] **Step 6: Verify clean working tree**

Run: `git status --short`

Expected: empty (all changes committed across the prior phases).

- [ ] **Step 7: Verify the branch's commit log**

Run: `git log --oneline main..HEAD`

Expected: ~17 commits, one per task that ended in commit. (These will be squash-merged at PR merge time per the silly-kicks single-commit-per-branch convention.)

If any step fails, stop, fix the issue, re-run that step, then continue.

---

## Phase 21 — Pre-merge: run bronze ALTER against live Databricks

### Task 21.1: Run the migration script — REQUIRES EXPLICIT USER APPROVAL

**Files:** none in repo — modifies live Databricks bronze schema.

- [ ] **Step 1: Confirm with user**

Surface to the user:
> "About to run `scripts/migrate_bronze_for_pr_ll2.py` against the live
> `soccer_analytics.bronze` schema. This adds 10 columns to
> `bronze.spadl_actions` and 7 columns to `bronze.vaep_action_values`. The
> ALTERs are non-destructive (column adds only); existing rows get NULL
> in the new columns until the combined backfill in Phase 23 repopulates
> them. The script is idempotent and has been dry-run-verified.
> Approve to proceed?"

Wait for explicit user approval.

- [ ] **Step 2: Run the migration script**

Run: `uv run --with databricks-sql-connector python scripts/migrate_bronze_for_pr_ll2.py`

Expected output:
```
PR-LL2 bronze migration — catalog=soccer_analytics schema=bronze dry_run=False
  soccer_analytics.bronze.spadl_actions: ADD COLUMNS — 10 columns: [...]
    ALTER applied successfully
  soccer_analytics.bronze.vaep_action_values: ADD COLUMNS — 7 columns: [...]
    ALTER applied successfully

Post-migration verification:
  soccer_analytics.bronze.spadl_actions: 29 cols, all target columns present
  soccer_analytics.bronze.vaep_action_values: 35 cols, all target columns present

Total columns added: 17
```

- [ ] **Step 3: Verify the live schema directly**

Run:
```bash
uv run --with databricks-sql-connector python -c "
from databricks import sql
import os
host = os.environ['DATABRICKS_HOST'].replace('https://','')
with sql.connect(server_hostname=host, http_path='/sql/1.0/warehouses/6c3b36ca64d183fe', access_token=os.environ['DATABRICKS_TOKEN']) as c:
    for tbl in ['spadl_actions', 'vaep_action_values']:
        with c.cursor() as cur:
            cur.execute(f'DESCRIBE TABLE soccer_analytics.bronze.{tbl}')
            cols = [r[0] for r in cur.fetchall() if r[0] and not r[0].startswith('#')]
            print(f'{tbl}: {len(cols)} cols')
"
```

Expected: `spadl_actions: 29 cols`, `vaep_action_values: 35 cols`.

- [ ] **Step 4: Re-run dbt CI against the now-evolved bronze schema**

Run: `uv run dbt compile --select fct_action_values+ stg_spadl__action_values 2>&1 | tail -10`

Expected: compile clean — staging models can resolve all referenced bronze columns.

(Phase 21 is a Databricks-side action; nothing to commit in the repo.)

---

## Phase 22 — Open PR + squash-merge

### Task 22.1: Push branch + open PR — REQUIRES USER APPROVAL

**Files:** GitHub PR created.

- [ ] **Step 1: Push the branch**

Run: `git push -u origin feat/spadl-enrichment-stage`

Expected: branch pushed, GitHub returns the URL to create a PR.

- [ ] **Step 2: Open the PR via `gh pr create`**

Compose a PR body covering:
- Goal (1-paragraph from spec executive summary)
- LL1 latent gaps closed (3 items)
- 4-source coverage added (StatsBomb / Wyscout / IDSSE / Metrica)
- β-consistent mart shape rename (column-rename table)
- fct_funnel_stages_agg Option ii reconciliation note (Wyscout funnel UX change)
- Test coverage matrix (writer parity / boundary-F1 / live schema)
- Migration runbook (link to spec)
- silly-kicks 1.6.0 + 1.7.0 dependencies

Run (after composing):
```bash
gh pr create \
    --title "feat: SPADL enrichment stage + 4-source coverage + LL1 cleanup (PR-LL2)" \
    --body "$(cat <<'EOF'
## Summary

PR-LL2 establishes the named SPADL post-conversion enrichment stage
(`apply_spadl_enrichments`), wires in 3 silly-kicks helpers
(`add_possessions`, `add_gk_role`, `add_pre_shot_gk_context`) for all
4 data sources, fixes 3 latent gaps from PR-LL1, and applies β-consistent
canonical-vs-native naming to `fct_action_values`.

See [the design spec](docs/superpowers/specs/2026-04-29-pr-ll2-spadl-enrichment-stage-design.md)
and [ADR-016](docs/superpowers/adrs/ADR-016-spadl-enrichment-stage-canonical-naming.md)
for full design rationale.

## LL1 latent gaps closed

1. `bronze.spadl_actions.action_id` was 100% NULL — writer dropped at projection. Fixed by surfacing in 4 UDFs.
2. `vaep_schema` in `_make_scoring_udf` did not include `statsbomb_*` — applyInPandas dropped them. 0/7M rows had non-NULL values pre-LL2. Fixed by extending the StructType.
3. `bronze.spadl_actions` missing 4 `statsbomb_*` columns physically — PR-LL1's ALTER touched only `vaep_action_values`. Fixed via migration script.

## Source coverage

Pre-LL2: StatsBomb + Wyscout (2 sources, ~9.6M rows). Post-LL2: + IDSSE + Metrica (~17K greenfield rows).

## β-consistent column renames in `fct_action_values`

| Pre-LL2 (alias) | Post-LL2 |
|---|---|
| `possession_id` (StatsBomb-only NULL elsewhere) | `statsbomb_possession_id` (provider-native) + new canonical `possession_id` (heuristic, all sources) |
| `possession_team_id` | `statsbomb_possession_team_id` |
| `play_pattern` | `statsbomb_play_pattern` |
| `under_pressure` | `statsbomb_under_pressure` |

Plus 7 NEW columns: `action_id`, `gk_role`, `gk_was_distributing`, `gk_was_engaged`, `gk_actions_in_possession`, `defending_gk_player_id`, plus the canonical `possession_id`.

## fct_funnel_stages_agg Option ii reconciliation

Drops the Wyscout-synthetic-possession workaround. The Conversion Funnel
page on Wyscout matches transitions from `pos_in_gs = 0` (with synthetic
driver compensation) to real heuristic possession counts. IDSSE and
Metrica matches show in the funnel correctly out of the box.

`wy_match_flag` renamed to `heuristic_possession_flag` (more general —
captures non-StatsBomb sources uniformly).

## Pre-merge migration

`scripts/migrate_bronze_for_pr_ll2.py` was run pre-merge, adding 10 cols
to `bronze.spadl_actions` and 7 cols to `bronze.vaep_action_values`.
dbt CI compiles cleanly against the evolved schema.

## Post-merge runbook

Combined backfill: `DELETE` StatsBomb + Wyscout from `spadl_actions` +
`vaep_action_values` + run wf-vaep + dbt full-refresh + Taipy deploy.
Estimated 30–60 min total operational window. Defensive Delta clones
provide rollback. Detailed in [spec §Migration runbook](docs/superpowers/specs/2026-04-29-pr-ll2-spadl-enrichment-stage-design.md#migration-runbook).

## Test plan

- 14 new unit/plausibility tests in `test_spadl_enrichments.py`
- Boundary-F1 ≥ 0.85 against StatsBomb native (3-match fixture)
- 5 writer parity tests (StatsBomb / Wyscout / IDSSE / Metrica + VAEP scoring) — closes LL1 latent-bug class
- `test_marts_live_schema.py` updated for β-consistent shape (gates on Phase 24 dbt full-refresh)

## Dependencies

- silly-kicks 1.7.0 (PyPI; commit `45ef2f8`) — dedicated `silly_kicks.spadl.sportec` + `silly_kicks.spadl.metrica` DataFrame converters; `_fix_direction_of_play` unification
- Wheel: 0.3.20 → 0.3.21

🤖 Generated with [Claude Code](https://claude.com/claude-code)
EOF
)"
```

**ASK USER FOR APPROVAL** before running the gh command.

- [ ] **Step 3: Wait for CI green**

Run: `gh pr checks --watch 2>&1 | tail -20` (after merging the URL output)

Expected: all checks PASS.

If any CI check fails, stop, investigate, push a fix commit to the branch, re-run CI watch.

### Task 22.2: Squash-merge — REQUIRES USER APPROVAL

- [ ] **Step 1: Confirm with user before merge**

Once CI is green, surface to the user:
> "CI green on PR-LL2. Ready to squash-merge. The squash collapses the
> ~17 task-level commits into a single commit on main. Approve?"

- [ ] **Step 2: Squash-merge**

Run: `gh pr merge --squash --delete-branch`

Expected: squash-merge succeeds; remote feature branch deleted; local branch may need explicit deletion.

- [ ] **Step 3: Update local main**

```bash
git checkout main
git pull origin main
git branch -D feat/spadl-enrichment-stage 2>/dev/null || true
```

- [ ] **Step 4: Verify the squash commit**

Run: `git log --oneline -3`

Expected: top commit is the LL2 squash with the PR title; `e88b35b` (PR-LL1) is one or two commits below.

---

## Phase 23 — Defensive snapshot + destructive backfill

### Task 23.1: Snapshot bronze tables — REQUIRES USER APPROVAL

- [ ] **Step 1: Confirm with user**

Surface:
> "About to take Delta deep-clone snapshots of `bronze.spadl_actions` and
> `bronze.vaep_action_values` to `*_pre_ll2_backfill` table names. Cost
> is metadata-only (no data copy until source is rewritten). Approve?"

- [ ] **Step 2: Run the snapshot SQL**

Use `uv run --with databricks-sql-connector python` (one-off SQL):

```python
from databricks import sql
import os
host = os.environ['DATABRICKS_HOST'].replace('https://','')
with sql.connect(server_hostname=host, http_path='/sql/1.0/warehouses/6c3b36ca64d183fe', access_token=os.environ['DATABRICKS_TOKEN']) as c:
    with c.cursor() as cur:
        for src, dst in [
            ('soccer_analytics.bronze.spadl_actions', 'soccer_analytics.bronze.spadl_actions_pre_ll2_backfill'),
            ('soccer_analytics.bronze.vaep_action_values', 'soccer_analytics.bronze.vaep_action_values_pre_ll2_backfill'),
        ]:
            cur.execute(f"CREATE TABLE {dst} DEEP CLONE {src}")
            print(f"  cloned {src} -> {dst}")
```

Expected: 2 clone tables created in <30 seconds.

### Task 23.2: Destructive DELETE + wf-vaep manual trigger — REQUIRES USER APPROVAL

- [ ] **Step 1: Confirm with user before DELETE**

Surface:
> "About to DELETE all StatsBomb + Wyscout rows from `bronze.spadl_actions`
> (~9.6M rows) and `bronze.vaep_action_values` (~9.6M rows). This is the
> destructive step of the LL2 combined backfill — restorable from the
> Phase 23.1 snapshot if anything goes wrong. After DELETE, wf-vaep is
> manually triggered to re-convert + re-score everything with the new
> writer schemas. ETA: 10–20 min for re-conversion + scoring. Approve?"

- [ ] **Step 2: Execute the DELETEs**

```python
from databricks import sql
import os
host = os.environ['DATABRICKS_HOST'].replace('https://','')
with sql.connect(server_hostname=host, http_path='/sql/1.0/warehouses/6c3b36ca64d183fe', access_token=os.environ['DATABRICKS_TOKEN']) as c:
    with c.cursor() as cur:
        for tbl in ['spadl_actions', 'vaep_action_values']:
            cur.execute(f"DELETE FROM soccer_analytics.bronze.{tbl} WHERE data_source IN ('statsbomb', 'wyscout')")
            print(f"  DELETE applied on {tbl}")
```

Expected: both DELETE operations complete successfully.

- [ ] **Step 3: Trigger wf-vaep**

Run: `databricks jobs run-now --job-id <wf-vaep-job-id> 2>&1 | tail -10`

(Get the actual job_id via `databricks jobs list 2>&1 | grep -i vaep` if not known.)

- [ ] **Step 4: Monitor the run progress**

Run: `databricks jobs list-runs --job-id <wf-vaep-job-id> --limit 1 2>&1 | tail -5`

Repeat every 60-120 seconds until status is `TERMINATED` with `result_state=SUCCESS`. Per CLAUDE.md "Never disappear into long-running commands": report progress to the user at each poll.

If the run fails: investigate logs (`databricks jobs runs get-output --run-id <id>`), fix root cause if possible, re-trigger. Per `_read_existing_match_ids` skip-already-converted-games logic, partial state recovers naturally on re-run.

---

## Phase 24 — dbt full-refresh

### Task 24.1: Rebuild fct_action_values + fct_funnel_stages_agg

**Files:** none in repo modified — rebuilds live mart tables.

- [ ] **Step 1: Run dbt full-refresh**

Run: `uv run dbt run --full-refresh --select fct_action_values+ fct_funnel_stages_agg+ 2>&1 | tail -30`

Expected: dbt rebuilds `fct_action_values`, `fct_funnel_stages_agg`, and any downstream marts. Runtime ~5–15 minutes for the ~9.6M-row rebuild.

- [ ] **Step 2: Run dbt tests**

Run: `uv run dbt test --select fct_action_values fct_funnel_stages_agg 2>&1 | tail -30`

Expected: all data tests PASS. Failures here would indicate data-quality issues from the backfill — investigate before proceeding.

- [ ] **Step 3: Run the live mart schema test**

Run: `uv run pytest src/tests/test_marts_live_schema.py -v`

Expected: all PASS. The test asserts the new β-consistent shape on the live `dev_gold.fct_action_values`.

If FAIL: investigate column-by-column diff between live mart and `_FCT_ACTION_VALUES_EXPECTED_COLS`. Likely indicates dbt build was incomplete or a contract mismatch.

---

## Phase 25 — Taipy deploy + final post-deploy validation

### Task 25.1: Deploy Taipy app — REQUIRES USER APPROVAL

- [ ] **Step 1: Confirm with user**

Surface:
> "About to deploy the new Taipy app code (drops the synthetic-possession
> compensation in funnel.py; references the new mart shape with renamed
> columns). Mart is in the new shape post-Phase-24 so the deploy lands
> cleanly. Approve?"

- [ ] **Step 2: Deploy**

Run: `uv run python scripts/manage_space.py deploy production 2>&1 | tail -20`

Expected: deploy succeeds; HF Space rebuilds with the new code.

### Task 25.2: Post-deploy validation

- [ ] **Step 1: Run the validation script**

Run: `uv run --with databricks-sql-connector python scripts/validate_pr_ll2_post_deploy.py`

Expected: `VALIDATION PASSED — all expected-population semantics hold`.

If FAIL: investigate the specific column / source combination flagged. Most likely failure mode: wf-vaep didn't write a new column for one source path (UDF or vaep_schema mismatch — but writer parity tests catch this at unit level, so would be a surprise).

- [ ] **Step 2: Run the empirical boundary-F1 measurement**

Run: `uv run --with databricks-sql-connector python scripts/measure_boundary_f1_full_corpus.py`

Expected: per-competition median F1 ~0.85–0.92; overall median above 0.85. JSON log written to `logs/boundary_f1_<timestamp>.json`.

If overall median <0.80: investigate before next training cycle (script returns exit code 1 in that case).

- [ ] **Step 3: Manual smoke test on Conversion Funnel page**

Open the Taipy production URL: `https://huggingface.co/spaces/luxury-lakehouse/soccer-analytics-app`

Navigate to the Conversion Funnel page. Pick:
- A StatsBomb match — confirm `pos_in_gs` numbers look similar to pre-LL2 (regression check)
- A Wyscout match — confirm `pos_in_gs > 0` (was 0 pre-LL2; should now show real heuristic possessions)
- An IDSSE match (if visible in the match selector) — confirm it shows in the funnel
- A Metrica match — same

Visually verify the funnel chart renders correctly without errors.

- [ ] **Step 4: Drop the defensive snapshots (after 24+ hours hold time)**

Wait at least 24 hours after Phase 25.2 passes before dropping the snapshots — gives time for any latent issue to surface from scheduled runs / user feedback.

After 24-hour hold:

```python
from databricks import sql
import os
host = os.environ['DATABRICKS_HOST'].replace('https://','')
with sql.connect(server_hostname=host, http_path='/sql/1.0/warehouses/6c3b36ca64d183fe', access_token=os.environ['DATABRICKS_TOKEN']) as c:
    with c.cursor() as cur:
        for tbl in ['spadl_actions_pre_ll2_backfill', 'vaep_action_values_pre_ll2_backfill']:
            cur.execute(f"DROP TABLE soccer_analytics.bronze.{tbl}")
            print(f"  dropped soccer_analytics.bronze.{tbl}")
```

This is a USER-APPROVAL gate — surface before dropping.

---

## Self-review checklist (post-plan)

Re-read the plan against the spec and verify:

- [ ] Spec coverage — every item in the spec's "File scope" + "Decisions log" maps to at least one task.
- [ ] No placeholders — every step has actual code / commands / expected output (no "TODO", no "implement later").
- [ ] Type consistency — `apply_spadl_enrichments` signature is consistent across all call sites; `_VALID_SOURCES` is consistent; column types in writer parity tests match DDL constants.
- [ ] Pre-merge gates clearly require user approval (Phase 21).
- [ ] Post-merge gates clearly require user approval (Phases 22.2, 23.1, 23.2, 25.1, 25.2 cleanup).
- [ ] Rollback path documented (Phase 23.1 snapshot exists; Phase 25.2 retains it 24h post-validation).

---

## Execution choice

**Plan complete and saved.** Two execution options:

**1. Subagent-Driven (recommended)** — I dispatch a fresh subagent per task, review between tasks, fast iteration. Each subagent gets a self-contained brief with the relevant task content; main session reviews + checkpoints.

**2. Inline Execution** — Execute tasks in this session using `superpowers:executing-plans`, batch execution with checkpoints for review.

Which approach?
