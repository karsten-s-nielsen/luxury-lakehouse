# Match Summary Redesign Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use `superpowers:subagent-driven-development` (recommended) or `superpowers:executing-plans` to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

> **Implementation notes (2026-04-19, post-execution):** The discipline-events data layer (Tasks 1a–1g) was added after Task 1 schema discovery surfaced two realities the initial plan did not anticipate:
> 1. StatsBomb card data is NOT flattened to a bronze column by statsbombpy — it lives in `_raw_extra_json`. `stg_statsbomb__events.card_name` now coalesces `bad_behaviour.card.name` and `foul_committed.card.name` (cards ride on BOTH event types — 13,403 yellow / 342 red / 357 second-yellow across all StatsBomb matches).
> 2. A new gold mart `fct_discipline_events` was created (~14K rows, liquid-clustered on `match_id`) and synced to Lakebase as `fct_discipline_events_synced`. The synced table creation is Terraform-declared but per ADR-005 Path A requires UI-create + `terraform import` (provider limitation on Autoscaling projects).
>
> Query pattern uses `execute_query` + `t()` + `%s` from `queries/common` (NOT the raw Databricks SQL cursor shown in the plan draft). Shot-level xG column is `statsbomb_xg`, aliased to `xg` in `fetch_shots_timeline`. State-module imports are unqualified (`from state.match_summary_verdict import derive_verdict`), matching the Taipy sys.path convention.
>
> The state module's discipline fetch is wrapped in a non-fatal try/except: if `fct_discipline_events_synced` is unavailable (e.g. during rollout before the UI-create step is done), the page renders without red cards instead of failing the whole refresh. The VAEP + shots fetches remain fatal — without them there is no Row 1 / Row 2.

**Goal:** Rewrite the Match Summary Taipy page from the current 4-grouped-bar scorecard into a dashboard-layout editorial page — top tile strip (Final · xG · xG · Our Verdict), Row 1 "Big Story" decisive actions ranked by VAEP, Row 2 Plotly xG-race with hover, Row 3 ranked delta table — per `docs/superpowers/specs/2026-04-19-match-summary-redesign-design.md`.

**Architecture:** Single-page rewrite. No new marts (queries `fct_action_values`, existing shot-level and event marts). No new dependencies — silly-kicks + VAEP are already in production. Row 2 migrates from matplotlib PNG to Plotly figure (native hover). Rendering helpers split into a new module (`state/match_summary_render.py`) and a pure-function verdict module (`state/match_summary_verdict.py`) for testability. Mobile responsive CSS added at 768px breakpoint in `style_v2.css`.

**Tech Stack:** Taipy 4.1, Plotly 5.x, pandas, pytest + pytest-mock, Databricks SQL Connector, Puppeteer (for E2E on deployed staging Space).

**Commit policy** (per user rule — overrides skill default): NO commits after individual tasks. All work stays uncommitted until staging E2E passes. Final task commits everything as one squash-merge-able commit.

---

## File Structure

**Files created:**

- `hf_taipy_app/src/state/match_summary_verdict.py` — pure function `derive_verdict(home_xg, away_xg, home_score, away_score) -> tuple[phrase, detail]`. No Taipy/Spark dependencies — trivially testable.
- `hf_taipy_app/src/state/match_summary_render.py` — rendering helpers: `render_moments_html()`, `build_xg_race_figure()` (Plotly), `render_delta_table_html()`. All pure functions taking DataFrames and returning strings or Figures.
- `src/tests/test_match_summary_verdict.py` — unit tests for verdict vocabulary.
- `src/tests/test_match_summary_render.py` — unit tests for rendering helpers.

**Files modified:**

- `hf_taipy_app/src/pages/match_summary.py` — page config: `metrics=[...]` replaced by `stats=[StatCard(...)]`, content rows restructured, citations updated.
- `hf_taipy_app/src/state/match_summary.py` — refresh callback rewritten to orchestrate new helpers; new state variables; old `_render_stat_bars` removed.
- `hf_taipy_app/src/queries/match.py` — add `fetch_vaep_decisive_actions`, `fetch_shots_timeline`, `fetch_discipline_events`.
- `hf_taipy_app/src/template.py` — `PAGE_TERMS["Match-Summary"]` extended; `GLOSSARY` additions for new terms.
- `hf_taipy_app/src/style_v2.css` — mobile responsive rules at 768px, new class styles for `ll-match-moments` and `ll-delta-table`.
- `NOTICE` — add silly-kicks entry if missing (verify first).

---

## Task 1: Data discovery — confirm mart and column names

**Goal:** Confirm the exact mart/column names for shot-level xG timeline data and discipline events (red/yellow cards) before writing queries. The spec deliberately left these TBD ("Exact mart name to be confirmed during implementation").

**Files:**
- Read-only inspection: `dbt_project/models/marts/*.sql`, `dbt_project/models/staging/**/*.sql`
- Reference: `hf_taipy_app/src/queries/match.py`, `shots.py`, `tracking.py`

- [ ] **Step 1: Find shot-level mart**

Run:
```bash
ls dbt_project/models/marts/*.sql | xargs grep -l "xg\b\|expected_goals\|is_goal"
```
Expected: at least one file. If `fct_shots.sql` exists, inspect it for columns `match_id`, `minute`, `second`, `period`, `team_id`, `xg`, `is_goal`. If columns are named differently (e.g. `expected_goals`, `goal_flag`), record the actual names.

If multiple candidates, pick the one already used by `hf_taipy_app/src/queries/` — follow precedent.

- [ ] **Step 2: Find discipline events source**

Run:
```bash
ls dbt_project/models/**/*.sql | xargs grep -l "red.?card\|yellow.?card\|card_type\|Bad Behaviour"
```
Expected: at least one staging or mart file referencing cards. Candidates: `stg_statsbomb__events.sql`, `fct_events.sql`. Note the table name, and the column that identifies red vs yellow (e.g. `card_type`, `event_subtype`).

If no card-specific field surfaces, inspect `stg_statsbomb__events.sql` for raw event type/subtype fields — StatsBomb events encode cards under `event_type='Bad Behaviour'` with `card_type` subtype.

- [ ] **Step 3: Record findings in the plan**

Add a short "Discovered schema" block to this plan (edit Task 2 below) with the confirmed table names and column names. This unblocks query implementation.

- [ ] **Step 4: Quick connectivity check**

Run:
```bash
uv run python -c "from hf_taipy_app.src.queries import match; print(dir(match))"
```
Expected: no import error; exposes current `fetch_*` functions. Confirms the module is importable before we extend it.

---

## Task 2: Verdict derivation pure function

**Goal:** Implement the five-phrase verdict vocabulary as a pure function, fully tested. Per spec §8.

**Files:**
- Create: `hf_taipy_app/src/state/match_summary_verdict.py`
- Create: `src/tests/test_match_summary_verdict.py`

- [ ] **Step 1: Write the failing tests**

Create `src/tests/test_match_summary_verdict.py`:

```python
"""Tests for Match Summary verdict derivation (spec §8)."""

from __future__ import annotations

import pytest

from hf_taipy_app.src.state.match_summary_verdict import derive_verdict

# Each case: (home_xg, away_xg, home_score, away_score, expected_phrase, detail_contains)
VERDICT_CASES = [
    # Fully merited — winner xG >= loser xG + 0.5
    (2.4, 0.8, 2, 1, "Fully merited", "+1.6"),
    (0.8, 2.4, 1, 2, "Fully merited", "+1.6"),  # symmetric — away winner
    # Fair result — within 0.3
    (1.2, 1.0, 1, 1, "Fair result", None),
    (1.4, 1.5, 2, 2, "Fair result", None),
    # Fortunate — winner xG < loser xG, loser xG < 2.0
    (0.6, 1.2, 1, 0, "Fortunate", None),
    # Smash & grab — loser xG >= winner xG + 1.5
    (0.4, 2.1, 1, 0, "Smash & grab", None),
    (2.1, 0.4, 0, 1, "Smash & grab", None),
    # Flattered by scoreline — winner xG >= 2x winner goals
    (4.2, 0.5, 2, 0, "Flattered by scoreline", None),
    # 0-0 with tiny xG gap — Fair result
    (0.3, 0.5, 0, 0, "Fair result", None),
    # 0-0 with large xG gap — Fair result + detail carries the nuance
    (2.1, 0.4, 0, 0, "Fair result", "2.1"),
]


@pytest.mark.parametrize(
    "home_xg, away_xg, home_score, away_score, phrase, detail_fragment",
    VERDICT_CASES,
)
def test_derive_verdict_vocabulary(
    home_xg: float, away_xg: float, home_score: int, away_score: int,
    phrase: str, detail_fragment: str | None,
) -> None:
    result_phrase, result_detail = derive_verdict(home_xg, away_xg, home_score, away_score)
    assert result_phrase == phrase, (
        f"Expected phrase '{phrase}' for xG {home_xg}-{away_xg}, score {home_score}-{away_score}, "
        f"got '{result_phrase}' with detail '{result_detail}'"
    )
    if detail_fragment is not None:
        assert detail_fragment in result_detail, (
            f"Expected detail to contain '{detail_fragment}', got '{result_detail}'"
        )


def test_resolution_order_smash_and_grab_wins_over_fortunate() -> None:
    """When both Fortunate (winner xG < loser) and Smash & grab (gap >= 1.5) apply,
    Smash & grab wins (higher priority per spec §8)."""
    # home wins 1-0 with xG 0.4 vs 2.1 — gap of 1.7 means Smash & grab
    phrase, _ = derive_verdict(0.4, 2.1, 1, 0)
    assert phrase == "Smash & grab"


def test_resolution_order_flattered_wins_over_fully_merited() -> None:
    """xG 4.2 vs 0.5, score 2-0 — Fully merited applies (gap >= 0.5) and Flattered
    applies (winner xG >= 2 * goals). Flattered has higher priority."""
    phrase, _ = derive_verdict(4.2, 0.5, 2, 0)
    assert phrase == "Flattered by scoreline"


def test_detail_always_has_xg_gap() -> None:
    """Detail annotation must carry the xG-gap number in every case."""
    _, detail = derive_verdict(2.4, 0.8, 2, 1)
    # Should contain something like "+1.6" or "1.6"
    assert "1.6" in detail


def test_pure_function_no_side_effects() -> None:
    """Calling derive_verdict twice with same inputs returns same output."""
    a = derive_verdict(2.4, 0.8, 2, 1)
    b = derive_verdict(2.4, 0.8, 2, 1)
    assert a == b
```

- [ ] **Step 2: Run tests — confirm they fail**

Run:
```bash
uv run pytest src/tests/test_match_summary_verdict.py -v
```
Expected: FAIL — `ModuleNotFoundError: No module named 'hf_taipy_app.src.state.match_summary_verdict'` (or equivalent).

- [ ] **Step 3: Implement the pure function**

Create `hf_taipy_app/src/state/match_summary_verdict.py`:

```python
"""Match Summary verdict derivation — pure function per spec §8.

Maps (home_xg, away_xg, home_score, away_score) to a verdict phrase from
a closed vocabulary of five, plus an xG-gap detail annotation.

Resolution order (first-match-wins):
    1. Smash & grab   — loser xG >= winner xG + 1.5
    2. Flattered      — winner xG >= 2 * winner goals
    3. Fortunate      — winner xG < loser xG (winner wins against the xG)
    4. Fair result    — |xG delta| < 0.3 (including all draws unless large gap)
    5. Fully merited  — winner xG >= loser xG + 0.5 (default for clear wins)
"""

from __future__ import annotations


def derive_verdict(
    home_xg: float, away_xg: float,
    home_score: int, away_score: int,
) -> tuple[str, str]:
    """Return (phrase, detail). Phrase is one of the five vocabulary entries.
    Detail is a compact xG-gap annotation suitable for a tile subtitle."""

    xg_gap = abs(home_xg - away_xg)

    # Determine winner / loser by actual score. Draws have no winner.
    if home_score > away_score:
        winner_xg, loser_xg, winner_goals = home_xg, away_xg, home_score
    elif away_score > home_score:
        winner_xg, loser_xg, winner_goals = away_xg, home_xg, away_score
    else:
        # Draw — evaluate xG balance
        winner_xg = loser_xg = winner_goals = None  # type: ignore[assignment]

    # Detail always carries the gap. Higher-xG team mentioned in the detail.
    higher_xg_team = "home" if home_xg > away_xg else "away"
    detail = _format_detail(home_xg, away_xg, xg_gap, higher_xg_team)

    # Draws always get Fair result regardless of xG gap — the xG context is in detail.
    if winner_xg is None:
        return "Fair result", detail

    # Resolution order — first match wins.
    if loser_xg >= winner_xg + 1.5:
        return "Smash & grab", detail
    if winner_xg >= 2.0 * winner_goals and winner_goals > 0:
        return "Flattered by scoreline", detail
    if winner_xg < loser_xg:
        return "Fortunate", detail
    if xg_gap < 0.3:
        return "Fair result", detail
    return "Fully merited", detail


def _format_detail(home_xg: float, away_xg: float, xg_gap: float, higher_xg_team: str) -> str:
    """Compose a compact detail string that always includes the xG-gap number."""
    higher_label = "Home" if higher_xg_team == "home" else "Away"
    return f"{higher_label} +{xg_gap:.1f} xG gap (Home {home_xg:.1f} vs Away {away_xg:.1f})"
```

- [ ] **Step 4: Run tests — confirm they pass**

Run:
```bash
uv run pytest src/tests/test_match_summary_verdict.py -v
```
Expected: all tests pass (15 parameterized + 4 standalone = 19 tests).

- [ ] **Step 5: Lint + type check**

Run:
```bash
uv run ruff check hf_taipy_app/src/state/match_summary_verdict.py src/tests/test_match_summary_verdict.py
uv run ruff format --check hf_taipy_app/src/state/match_summary_verdict.py src/tests/test_match_summary_verdict.py
uv run pyright hf_taipy_app/src/state/match_summary_verdict.py
```
Expected: no issues. If format check fails, run `uv run ruff format <file>` to fix.

---

## Task 3: SQL queries — VAEP decisive actions, shots timeline, discipline events

**Goal:** Add three query functions to `hf_taipy_app/src/queries/match.py`. Each validates inputs, returns a typed pandas DataFrame, has a LIMIT, and follows the existing connection pattern in the module.

**Files:**
- Modify: `hf_taipy_app/src/queries/match.py`
- Create: `src/tests/test_queries_match_extended.py` (or extend existing test file if present)

**Discovered schema** (fill in from Task 1 findings before starting):

- Shot-level mart name: `___________` (e.g. `fct_shots`)
- Shot xG column: `___________` (e.g. `xg`, `expected_goals`)
- Shot goal flag column: `___________` (e.g. `is_goal`)
- Discipline events source: `___________` (e.g. `stg_statsbomb__events`)
- Card type identification: `___________` (e.g. `event_type='Bad Behaviour' AND card_type IN ('Red Card', 'Second Yellow')`)

- [ ] **Step 1: Read the existing queries/match.py to learn the connection pattern**

Read `hf_taipy_app/src/queries/match.py` in full. Note how `fetch_match_summary` opens a connection, parameterises `match_id`, returns a DataFrame. All new queries follow the same pattern.

- [ ] **Step 2: Write failing tests for the three new functions**

Create `src/tests/test_queries_match_extended.py`:

```python
"""Tests for the three new match-level query functions added in the Match Summary redesign."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pandas as pd
import pytest


def _mock_connection_returning(df: pd.DataFrame) -> MagicMock:
    """Helper: a mock connection context manager whose cursor returns `df`."""
    mock_conn = MagicMock()
    mock_cursor = MagicMock()
    mock_cursor.fetchall_arrow.return_value = MagicMock(to_pandas=MagicMock(return_value=df))
    mock_conn.cursor.return_value.__enter__.return_value = mock_cursor
    return mock_conn


@patch("hf_taipy_app.src.queries.match._open_connection")
def test_fetch_vaep_decisive_actions_returns_top_n_sorted(mock_open) -> None:
    from hf_taipy_app.src.queries.match import fetch_vaep_decisive_actions

    # Simulated mart response — deliberately unsorted
    fake = pd.DataFrame({
        "match_id": [1, 1, 1],
        "minute": [23, 67, 84],
        "second": [12, 5, 40],
        "period": [1, 2, 2],
        "player_id": [100, 200, 300],
        "team_id": [10, 20, 10],
        "action_type": ["shot", "shot", "keeper_save"],
        "action_result": ["success", "fail", "success"],
        "vaep_value": [0.18, -0.04, 0.35],
        "offensive_value": [0.18, 0.0, 0.0],
        "defensive_value": [0.0, -0.04, 0.35],
        "start_x": [80, 85, 5],
        "start_y": [30, 34, 34],
        "end_x": [99, 90, 30],
        "end_y": [34, 30, 34],
    })
    mock_open.return_value.__enter__.return_value = _mock_connection_returning(fake)

    df = fetch_vaep_decisive_actions(match_id=1, n=3)
    assert len(df) == 3
    # Top-of-result should be sorted by |vaep_value| desc — query does the sort
    # (test ensures the function trusts the query's ORDER BY)
    assert df.iloc[0]["vaep_value"] == pytest.approx(0.18)


@patch("hf_taipy_app.src.queries.match._open_connection")
def test_fetch_vaep_decisive_actions_validates_match_id(mock_open) -> None:
    from hf_taipy_app.src.queries.match import fetch_vaep_decisive_actions
    with pytest.raises((ValueError, TypeError)):
        fetch_vaep_decisive_actions(match_id=None)  # type: ignore[arg-type]


@patch("hf_taipy_app.src.queries.match._open_connection")
def test_fetch_shots_timeline_returns_ordered_shots(mock_open) -> None:
    from hf_taipy_app.src.queries.match import fetch_shots_timeline

    fake = pd.DataFrame({
        "match_id": [1, 1, 1, 1],
        "minute": [5, 23, 45, 72],
        "second": [10, 12, 30, 5],
        "period": [1, 1, 1, 2],
        "team_id": [10, 20, 10, 20],
        "xg": [0.03, 0.12, 0.20, 0.48],
        "is_goal": [False, True, False, False],
    })
    mock_open.return_value.__enter__.return_value = _mock_connection_returning(fake)

    df = fetch_shots_timeline(match_id=1)
    assert len(df) == 4
    # Query must ORDER BY period, minute, second — test asserts result is ordered
    df_sorted_check = df.sort_values(["period", "minute", "second"]).reset_index(drop=True)
    pd.testing.assert_frame_equal(df.reset_index(drop=True), df_sorted_check)


@patch("hf_taipy_app.src.queries.match._open_connection")
def test_fetch_discipline_events_red_cards_only(mock_open) -> None:
    from hf_taipy_app.src.queries.match import fetch_discipline_events

    fake = pd.DataFrame({
        "match_id": [1, 1],
        "minute": [58, 77],
        "second": [20, 10],
        "period": [2, 2],
        "player_id": [999, 888],
        "team_id": [20, 10],
        "card_type": ["Red Card", "Second Yellow"],
    })
    mock_open.return_value.__enter__.return_value = _mock_connection_returning(fake)

    df = fetch_discipline_events(match_id=1)
    assert len(df) == 2
    assert set(df["card_type"].tolist()) == {"Red Card", "Second Yellow"}
```

- [ ] **Step 3: Run tests — confirm they fail**

Run:
```bash
uv run pytest src/tests/test_queries_match_extended.py -v
```
Expected: FAIL — the three new functions don't exist yet.

- [ ] **Step 4: Implement the three query functions**

Append to `hf_taipy_app/src/queries/match.py` (use the existing `_open_connection` / `_CATALOG` / `_SCHEMA` helpers present in the module — discover exact names by reading the file):

```python
# ... existing imports and functions above ...

_VAEP_DECISIVE_SQL = f"""
SELECT match_id, minute, second, period,
       player_id, team_id, action_type, action_result,
       start_x, start_y, end_x, end_y,
       offensive_value, defensive_value, vaep_value
FROM {_CATALOG}.{_SCHEMA_GOLD}.fct_action_values
WHERE match_id = ?
ORDER BY ABS(vaep_value) DESC
LIMIT ?
"""


def fetch_vaep_decisive_actions(match_id: int, n: int = 3) -> pd.DataFrame:
    """Return the top-N on-ball actions in a match ranked by |VAEP value|.

    Used by Match Summary Row 1. Per spec §5.3 + §7.4.
    """
    if match_id is None or not isinstance(match_id, int):
        raise ValueError(f"match_id must be int, got {type(match_id).__name__}")
    if n <= 0 or n > 20:
        raise ValueError(f"n must be in 1..20, got {n}")

    with _open_connection() as conn:
        with conn.cursor() as cursor:
            cursor.execute(_VAEP_DECISIVE_SQL, [match_id, n])
            return cursor.fetchall_arrow().to_pandas()


# Replace <SHOTS_MART> and column names with the values discovered in Task 1.
_SHOTS_TIMELINE_SQL = f"""
SELECT match_id, minute, second, period, team_id,
       xg, is_goal
FROM {_CATALOG}.{_SCHEMA_GOLD}.fct_shots
WHERE match_id = ?
ORDER BY period, minute, second
LIMIT 200
"""


def fetch_shots_timeline(match_id: int) -> pd.DataFrame:
    """Return all shots for a match, ordered chronologically. Used by Row 2 xG race."""
    if match_id is None or not isinstance(match_id, int):
        raise ValueError(f"match_id must be int, got {type(match_id).__name__}")

    with _open_connection() as conn:
        with conn.cursor() as cursor:
            cursor.execute(_SHOTS_TIMELINE_SQL, [match_id])
            return cursor.fetchall_arrow().to_pandas()


# Replace <EVENTS_SOURCE> and card predicate with values discovered in Task 1.
_DISCIPLINE_SQL = f"""
SELECT match_id, minute, second, period, player_id, team_id, card_type
FROM {_CATALOG}.{_SCHEMA_GOLD}.fct_events  -- placeholder; confirm in Task 1
WHERE match_id = ?
  AND card_type IN ('Red Card', 'Second Yellow')
ORDER BY period, minute, second
LIMIT 10
"""


def fetch_discipline_events(match_id: int) -> pd.DataFrame:
    """Return red cards and second-yellow cards for a match. Used by Row 1 auto-include + Row 2 markers."""
    if match_id is None or not isinstance(match_id, int):
        raise ValueError(f"match_id must be int, got {type(match_id).__name__}")

    with _open_connection() as conn:
        with conn.cursor() as cursor:
            cursor.execute(_DISCIPLINE_SQL, [match_id])
            return cursor.fetchall_arrow().to_pandas()
```

- [ ] **Step 5: Run tests — confirm they pass**

Run:
```bash
uv run pytest src/tests/test_queries_match_extended.py -v
```
Expected: all 4 tests pass.

- [ ] **Step 6: Integration sanity check against Databricks** (skip if Databricks env unavailable)

Run a quick REPL query against a known good match (find one via `SELECT match_id FROM fct_match_summary LIMIT 1`):

```bash
uv run python -c "
from hf_taipy_app.src.queries.match import fetch_vaep_decisive_actions, fetch_shots_timeline, fetch_discipline_events
match_id = <choose one>
print('VAEP top-3:'); print(fetch_vaep_decisive_actions(match_id, 3))
print('Shots:'); print(fetch_shots_timeline(match_id).head())
print('Discipline:'); print(fetch_discipline_events(match_id))
"
```
Expected: non-empty DataFrames. If the discipline query returns zero for a match with a known red card, revisit Task 1 column identification.

- [ ] **Step 7: Lint + type**

Run:
```bash
uv run ruff check hf_taipy_app/src/queries/match.py src/tests/test_queries_match_extended.py
uv run pyright hf_taipy_app/src/queries/match.py
```
Expected: no issues.

---

## Task 4: Big Story HTML rendering helper

**Goal:** Implement `render_moments_html` that takes decisive actions + red-card events and produces the Row 1 HTML block. Per spec §5.3.

**Files:**
- Create: `hf_taipy_app/src/state/match_summary_render.py` (new module, will also hold the next two helpers)
- Extend: `src/tests/test_match_summary_render.py` (new file)

- [ ] **Step 1: Write failing tests**

Create `src/tests/test_match_summary_render.py`:

```python
"""Tests for Match Summary rendering helpers (spec §5.3, §5.4, §5.5)."""

from __future__ import annotations

import pandas as pd
import pytest


def _sample_decisive() -> pd.DataFrame:
    """Three decisive actions: a shot goal, a missed shot, a keeper save."""
    return pd.DataFrame({
        "minute": [23, 67, 84],
        "second": [12, 5, 40],
        "period": [1, 2, 2],
        "player_id": [100, 200, 300],
        "player_name": ["Palmer", "Saka", "Sánchez"],
        "team_id": [10, 20, 10],
        "team_name": ["Chelsea", "Arsenal", "Chelsea"],
        "action_type": ["shot", "shot", "keeper_save"],
        "action_result": ["success", "fail", "success"],
        "vaep_value": [0.18, -0.04, 0.35],
        "offensive_value": [0.18, 0.0, 0.0],
        "defensive_value": [0.0, -0.04, 0.35],
    })


def _no_red_cards() -> pd.DataFrame:
    return pd.DataFrame(columns=["minute", "second", "period", "player_id", "player_name",
                                  "team_id", "team_name", "card_type"])


def _one_red_card() -> pd.DataFrame:
    return pd.DataFrame({
        "minute": [58], "second": [20], "period": [2],
        "player_id": [999], "player_name": ["Saliba"],
        "team_id": [20], "team_name": ["Arsenal"],
        "card_type": ["Red Card"],
    })


def test_moments_html_contains_hero_card() -> None:
    from hf_taipy_app.src.state.match_summary_render import render_moments_html
    html = render_moments_html(_sample_decisive(), _no_red_cards(), scope_plain="Chelsea vs Arsenal")
    # Hero is the highest |VAEP|: Sánchez save (0.35)
    assert "Sánchez" in html
    assert "ll-big-story-hero" in html or "big-story-hero" in html  # class name used for styling


def test_moments_html_contains_two_secondary_cards() -> None:
    from hf_taipy_app.src.state.match_summary_render import render_moments_html
    html = render_moments_html(_sample_decisive(), _no_red_cards(), scope_plain="Chelsea vs Arsenal")
    # Secondary cards: Palmer (0.18) and Saka (-0.04)
    assert "Palmer" in html
    assert "Saka" in html


def test_moments_html_auto_includes_red_card() -> None:
    from hf_taipy_app.src.state.match_summary_render import render_moments_html
    html = render_moments_html(_sample_decisive(), _one_red_card(), scope_plain="Chelsea vs Arsenal")
    assert "Saliba" in html
    assert "58'" in html
    # Red-card card uses a distinctive class or marker
    assert "red-card" in html.lower() or "card-red" in html.lower()


def test_moments_html_empty_decisive_renders_empty_state() -> None:
    from hf_taipy_app.src.state.match_summary_render import render_moments_html
    empty = pd.DataFrame(columns=_sample_decisive().columns)
    html = render_moments_html(empty, _no_red_cards(), scope_plain="X vs Y")
    # Empty state is a short message, not a crash
    assert "No decisive actions" in html or len(html) > 0


def test_moments_html_caveat_line_present() -> None:
    from hf_taipy_app.src.state.match_summary_render import render_moments_html
    html = render_moments_html(_sample_decisive(), _no_red_cards(), scope_plain="Chelsea vs Arsenal")
    # Spec §5.3 — caveat mentions VAEP + off-ball limitation
    assert "VAEP" in html
    assert "off-ball" in html.lower() or "on-ball" in html.lower()
```

- [ ] **Step 2: Run tests — confirm they fail**

Run:
```bash
uv run pytest src/tests/test_match_summary_render.py -v
```
Expected: FAIL — module does not exist.

- [ ] **Step 3: Implement `render_moments_html`**

Create `hf_taipy_app/src/state/match_summary_render.py`:

```python
"""Match Summary rendering helpers — Row 1 Big Story, Row 2 xG race, Row 3 delta table.

All helpers are pure functions over DataFrames; no Taipy state handled here.
"""

from __future__ import annotations

from html import escape

import pandas as pd


# Human-readable SPADL action-type labels for Row 1 prose.
_ACTION_TYPE_LABELS: dict[str, str] = {
    "shot": "Shot",
    "pass": "Pass",
    "cross": "Cross",
    "dribble": "Dribble",
    "take_on": "Take-on",
    "tackle": "Tackle",
    "interception": "Interception",
    "clearance": "Clearance",
    "keeper_save": "Keeper save",
    "keeper_claim": "Keeper claim",
    "keeper_punch": "Keeper punch",
    "foul": "Foul",
    "freekick_crossed": "Cross from free kick",
    "freekick_short": "Short free kick",
    "corner_crossed": "Corner cross",
    "corner_short": "Short corner",
}


def _action_label(row: pd.Series) -> str:
    """Human label for a decisive-action row — e.g. 'Shot, scored' or 'Tackle, possession won'."""
    atype = _ACTION_TYPE_LABELS.get(str(row.get("action_type")), str(row.get("action_type", "action")).title())
    result = str(row.get("action_result", ""))
    result_suffix = ""
    if atype == "Shot" and result == "success":
        result_suffix = ", scored"
    elif atype == "Shot" and result == "fail":
        result_suffix = ", missed / saved"
    elif result == "success":
        result_suffix = ""
    elif result == "fail":
        result_suffix = ", failed"
    return f"{atype}{result_suffix}"


def _fmt_minute(row: pd.Series) -> str:
    return f"{int(row['minute'])}'"


def _vaep_sign(val: float) -> str:
    return f"{val:+.2f}"


def _card_html(row: pd.Series) -> str:
    minute = _fmt_minute(row)
    player = escape(str(row.get("player_name", "Unknown")))
    team = escape(str(row.get("team_name", "")))
    card = escape(str(row.get("card_type", "Card")))
    return (
        '<div class="ll-moment-card ll-moment-card-red-card">'
        f'<span class="ll-moment-minute">{minute}</span>'
        f'<span class="ll-moment-body"><strong>{card}</strong>: {player} ({team}) — down to 10</span>'
        "</div>"
    )


def _moment_card_html(row: pd.Series, *, hero: bool = False) -> str:
    cls = "ll-moment-card ll-big-story-hero" if hero else "ll-moment-card ll-moment-card-secondary"
    minute = _fmt_minute(row)
    player = escape(str(row.get("player_name", "Unknown")))
    team = escape(str(row.get("team_name", "")))
    action = escape(_action_label(row))
    vaep = _vaep_sign(float(row["vaep_value"]))
    return (
        f'<div class="{cls}">'
        f'<span class="ll-moment-minute">{minute}</span>'
        f'<div class="ll-moment-body">'
        f'<strong>{player}</strong> <span class="ll-moment-team">{team}</span><br>'
        f'{action}'
        f'</div>'
        f'<span class="ll-moment-vaep">VAEP {vaep}</span>'
        "</div>"
    )


def render_moments_html(
    decisive: pd.DataFrame,
    red_cards: pd.DataFrame,
    *,
    scope_plain: str,
) -> str:
    """Build the Row 1 HTML block. Expects `decisive` sorted desc by |vaep_value|."""

    if decisive.empty:
        return (
            '<div class="ll-match-moments ll-match-moments-empty">'
            "<p>No decisive actions available for this match.</p>"
            "</div>"
        )

    hero = decisive.iloc[0]
    secondary = decisive.iloc[1:3] if len(decisive) > 1 else decisive.iloc[0:0]

    parts: list[str] = ['<div class="ll-match-moments">']
    parts.append('<div class="ll-big-story-label">★ Big story</div>')
    parts.append(_moment_card_html(hero, hero=True))
    parts.append('<div class="ll-moments-secondary">')
    for _, row in secondary.iterrows():
        parts.append(_moment_card_html(row, hero=False))
    if not red_cards.empty:
        for _, row in red_cards.iterrows():
            parts.append(_card_html(row))
    parts.append("</div>")
    parts.append(
        '<p class="ll-moments-caveat">'
        "Decisive on-ball actions ranked by VAEP impact "
        "(Decroos et al. 2019, computed via silly-kicks). "
        "Off-ball runs and positioning are not valued."
        "</p>"
    )
    parts.append("</div>")
    return "\n".join(parts)
```

- [ ] **Step 4: Run tests — confirm they pass**

Run:
```bash
uv run pytest src/tests/test_match_summary_render.py -v -k moments
```
Expected: 5 tests pass.

- [ ] **Step 5: Lint + type**

Run:
```bash
uv run ruff check hf_taipy_app/src/state/match_summary_render.py src/tests/test_match_summary_render.py
uv run pyright hf_taipy_app/src/state/match_summary_render.py
```
Expected: no issues.

---

## Task 5: Plotly xG race chart builder

**Goal:** Implement `build_xg_race_figure()` returning a Plotly `Figure` with stepped xG lines, shot ticks, gold decisive rings, goal icons, red-card markers, and half-time divider. Per spec §5.4.

**Files:**
- Extend: `hf_taipy_app/src/state/match_summary_render.py`
- Extend: `src/tests/test_match_summary_render.py`

- [ ] **Step 1: Write failing tests for Plotly figure properties**

Append to `src/tests/test_match_summary_render.py`:

```python
# --- Row 2: xG race chart tests ---

def _sample_shots() -> pd.DataFrame:
    return pd.DataFrame({
        "minute": [5, 23, 45, 67, 72, 84],
        "second": [10, 12, 30, 5, 15, 40],
        "period": [1, 1, 1, 2, 2, 2],
        "team_id": [10, 10, 10, 20, 20, 10],
        "team_name": ["Chelsea", "Chelsea", "Chelsea", "Arsenal", "Arsenal", "Chelsea"],
        "xg": [0.03, 0.12, 0.20, 0.48, 0.08, 0.30],
        "is_goal": [False, True, False, False, False, True],
    })


def test_xg_race_figure_has_expected_traces() -> None:
    """Figure must contain: 2 cumulative xG lines, 2 sets of shot ticks, decisive rings,
    goal icons, red-card markers. Test for minimum trace count."""
    from hf_taipy_app.src.state.match_summary_render import build_xg_race_figure
    fig = build_xg_race_figure(
        shots=_sample_shots(),
        decisive=_sample_decisive(),
        red_cards=_no_red_cards(),
        home_team_id=10, home_team_name="Chelsea",
        away_team_id=20, away_team_name="Arsenal",
        home_color="#5a9999", away_color="#a55555",
    )
    # At least 2 cumulative lines + 2 shot tick traces + decisive rings + goal icons = 6 traces
    assert len(fig.data) >= 6


def test_xg_race_figure_has_halftime_line() -> None:
    from hf_taipy_app.src.state.match_summary_render import build_xg_race_figure
    fig = build_xg_race_figure(
        shots=_sample_shots(),
        decisive=_sample_decisive(),
        red_cards=_no_red_cards(),
        home_team_id=10, home_team_name="Chelsea",
        away_team_id=20, away_team_name="Arsenal",
        home_color="#5a9999", away_color="#a55555",
    )
    shapes = fig.layout.shapes or ()
    halftime_shapes = [s for s in shapes if s.type == "line" and s.x0 == 45]
    assert len(halftime_shapes) >= 1, "Expected a half-time divider line at minute 45"


def test_xg_race_figure_cumulative_ends_at_total_xg() -> None:
    """The last point of each team's cumulative trace should equal the team's total xG."""
    from hf_taipy_app.src.state.match_summary_render import build_xg_race_figure
    shots = _sample_shots()
    fig = build_xg_race_figure(
        shots=shots, decisive=_sample_decisive(), red_cards=_no_red_cards(),
        home_team_id=10, home_team_name="Chelsea",
        away_team_id=20, away_team_name="Arsenal",
        home_color="#5a9999", away_color="#a55555",
    )
    home_total_expected = shots.loc[shots["team_id"] == 10, "xg"].sum()
    # Find the home cumulative trace by name
    home_traces = [t for t in fig.data if getattr(t, "name", None) == "Chelsea xG"]
    assert len(home_traces) == 1
    last_y = home_traces[0].y[-1]
    assert last_y == pytest.approx(home_total_expected, abs=1e-6)


def test_xg_race_figure_red_card_marker_present() -> None:
    from hf_taipy_app.src.state.match_summary_render import build_xg_race_figure
    fig = build_xg_race_figure(
        shots=_sample_shots(),
        decisive=_sample_decisive(),
        red_cards=_one_red_card(),
        home_team_id=10, home_team_name="Chelsea",
        away_team_id=20, away_team_name="Arsenal",
        home_color="#5a9999", away_color="#a55555",
    )
    red_traces = [t for t in fig.data if "red card" in str(getattr(t, "name", "")).lower()]
    assert len(red_traces) >= 1
```

- [ ] **Step 2: Run tests — confirm they fail**

Run:
```bash
uv run pytest src/tests/test_match_summary_render.py -v -k xg_race
```
Expected: FAIL — `build_xg_race_figure` not defined.

- [ ] **Step 3: Implement `build_xg_race_figure`**

Append to `hf_taipy_app/src/state/match_summary_render.py`:

```python
# --- Row 2: Plotly xG race chart ---

import plotly.graph_objects as go


def _cumulative_xg_by_team(shots: pd.DataFrame, team_id: int) -> pd.DataFrame:
    """Return a DataFrame with columns (minute_absolute, cumulative_xg) for one team.
    Prepends a (0, 0) anchor so the stepped trace starts at the origin."""
    team_shots = shots.loc[shots["team_id"] == team_id].copy()
    # minute_absolute accounts for second-half offset — 2nd period starts at minute 45.
    # For simplicity, assume the given `minute` column is already match-minute (0-90+);
    # if not, adjust: team_shots["minute_absolute"] = team_shots["minute"] + (team_shots["period"] - 1) * 45.
    team_shots = team_shots.sort_values(["period", "minute", "second"]).reset_index(drop=True)
    cum = team_shots["xg"].cumsum()
    df = pd.DataFrame({"minute": team_shots["minute"].astype(float), "cumulative_xg": cum.astype(float)})
    # Prepend anchor
    anchor = pd.DataFrame({"minute": [0.0], "cumulative_xg": [0.0]})
    return pd.concat([anchor, df], ignore_index=True)


def build_xg_race_figure(
    *,
    shots: pd.DataFrame,
    decisive: pd.DataFrame,
    red_cards: pd.DataFrame,
    home_team_id: int, home_team_name: str,
    away_team_id: int, away_team_name: str,
    home_color: str, away_color: str,
) -> go.Figure:
    """Build the Row 2 Plotly figure per spec §5.4."""

    fig = go.Figure()

    # 1. Cumulative xG stepped traces.
    for team_id, team_name, color in [
        (home_team_id, home_team_name, home_color),
        (away_team_id, away_team_name, away_color),
    ]:
        cum = _cumulative_xg_by_team(shots, team_id)
        fig.add_trace(go.Scatter(
            x=cum["minute"], y=cum["cumulative_xg"],
            mode="lines",
            line=dict(shape="hv", width=2.5, color=color),
            name=f"{team_name} xG",
            hovertemplate=f"<b>{team_name}</b><br>Minute %{{x:.0f}}'<br>Cumulative xG %{{y:.2f}}<extra></extra>",
        ))

    # 2. Shot ticks (small markers) — one trace per team.
    for team_id, team_name, color in [
        (home_team_id, home_team_name, home_color),
        (away_team_id, away_team_name, away_color),
    ]:
        team_shots = shots.loc[shots["team_id"] == team_id]
        if team_shots.empty:
            continue
        fig.add_trace(go.Scatter(
            x=team_shots["minute"], y=[-0.1] * len(team_shots),
            mode="markers",
            marker=dict(symbol="line-ns-open", size=team_shots["xg"] * 20 + 4,
                        color=color, line=dict(width=1.5)),
            name=f"{team_name} shots",
            hovertemplate=f"<b>{team_name} shot</b><br>Minute %{{x:.0f}}'<br>xG %{{customdata:.2f}}<extra></extra>",
            customdata=team_shots["xg"],
            showlegend=False,
        ))

    # 3. Gold decisive-action rings.
    if not decisive.empty:
        decisive_minutes = decisive["minute"].astype(float)
        # Y-coordinate: plot at the cumulative xG value of the action's team at that minute,
        # so the ring sits on the curve. Approximation: project onto the cumulative trace.
        ring_y = _project_to_curves(shots, decisive, home_team_id, away_team_id)
        ring_colors = [home_color if tid == home_team_id else away_color for tid in decisive["team_id"]]
        fig.add_trace(go.Scatter(
            x=decisive_minutes, y=ring_y,
            mode="markers",
            marker=dict(symbol="circle-open", size=14,
                        line=dict(color="#d9a300", width=2.5),  # AMBER ring
                        color=ring_colors),
            name="Decisive moments",
            hovertemplate="<b>Decisive</b><br>Minute %{x:.0f}'<br>VAEP %{customdata:+.2f}<extra></extra>",
            customdata=decisive["vaep_value"],
        ))

    # 4. Goal icons — star marker at goal minutes on the team's cumulative curve.
    goals = shots.loc[shots["is_goal"]]
    if not goals.empty:
        goal_y = _project_to_curves(shots, goals, home_team_id, away_team_id)
        goal_colors = [home_color if tid == home_team_id else away_color for tid in goals["team_id"]]
        fig.add_trace(go.Scatter(
            x=goals["minute"].astype(float), y=goal_y,
            mode="markers",
            marker=dict(symbol="star", size=14, color=goal_colors,
                        line=dict(color="#ffffff", width=1.5)),
            name="Goals",
            hovertemplate="<b>Goal</b><br>Minute %{x:.0f}'<br>xG of shot %{customdata:.2f}<extra></extra>",
            customdata=goals["xg"],
        ))

    # 5. Red-card markers — distinct symbol below axis.
    if not red_cards.empty:
        fig.add_trace(go.Scatter(
            x=red_cards["minute"].astype(float), y=[-0.2] * len(red_cards),
            mode="markers",
            marker=dict(symbol="square-open", size=12, color="#c84444", line=dict(width=2)),
            name="Red card",
            hovertemplate="<b>Red card</b><br>Minute %{x:.0f}'<extra></extra>",
        ))

    # Half-time divider as a shape (non-trace, so it doesn't appear in legend).
    fig.add_shape(type="line", x0=45, x1=45, y0=0, y1=1, yref="paper",
                   line=dict(color="#888888", width=1, dash="dash"))

    # Layout.
    fig.update_layout(
        xaxis=dict(title="Minute", range=[0, 95], showgrid=False),
        yaxis=dict(title="Cumulative xG", showgrid=True, gridcolor="#333333"),
        plot_bgcolor="#1a1a1a", paper_bgcolor="#1a1a1a",
        font=dict(color="#cccccc"),
        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
        margin=dict(l=40, r=20, t=40, b=40),
        hovermode="closest",
        height=320,
    )
    return fig


def _project_to_curves(
    shots: pd.DataFrame, events: pd.DataFrame,
    home_team_id: int, away_team_id: int,
) -> list[float]:
    """For each event in `events`, return the cumulative xG value of that event's team
    at the event's minute. Used to position rings / goal icons on the stepped curves."""
    ys: list[float] = []
    for _, ev in events.iterrows():
        tid = int(ev["team_id"])
        cum = _cumulative_xg_by_team(shots, tid)
        # Last cum value at or before ev.minute.
        mask = cum["minute"] <= float(ev["minute"])
        y = float(cum.loc[mask, "cumulative_xg"].iloc[-1]) if mask.any() else 0.0
        ys.append(y)
    return ys
```

- [ ] **Step 4: Run tests — confirm they pass**

Run:
```bash
uv run pytest src/tests/test_match_summary_render.py -v -k xg_race
```
Expected: 4 xG-race tests pass.

- [ ] **Step 5: Smoke test rendering — no visual regression**

Run:
```bash
uv run python -c "
import pandas as pd
from hf_taipy_app.src.state.match_summary_render import build_xg_race_figure
shots = pd.DataFrame({'minute':[5,23,45,67,72,84],'second':[10,12,30,5,15,40],'period':[1,1,1,2,2,2],
                       'team_id':[10,10,10,20,20,10],'team_name':['Chelsea']*3+['Arsenal']*2+['Chelsea'],
                       'xg':[0.03,0.12,0.20,0.48,0.08,0.30],'is_goal':[False,True,False,False,False,True]})
decisive = pd.DataFrame({'minute':[23,67,84],'second':[12,5,40],'period':[1,2,2],
                          'player_id':[100,200,300],'player_name':['Palmer','Saka','Sanchez'],
                          'team_id':[10,20,10],'team_name':['Chelsea','Arsenal','Chelsea'],
                          'action_type':['shot','shot','keeper_save'],'action_result':['success','fail','success'],
                          'vaep_value':[0.18,-0.04,0.35],'offensive_value':[0.18,0.0,0.0],'defensive_value':[0.0,-0.04,0.35]})
red = pd.DataFrame(columns=['minute','second','period','player_id','player_name','team_id','team_name','card_type'])
fig = build_xg_race_figure(shots=shots, decisive=decisive, red_cards=red,
                            home_team_id=10, home_team_name='Chelsea',
                            away_team_id=20, away_team_name='Arsenal',
                            home_color='#5a9999', away_color='#a55555')
print(f'Figure has {len(fig.data)} traces, {len(fig.layout.shapes or ())} shapes')
fig.write_html('/tmp/xg_race_smoke.html')
print('Smoke HTML written to /tmp/xg_race_smoke.html — inspect in browser')
"
```
Expected: prints trace/shape counts, writes smoke HTML. Open the HTML and visually confirm: stepped lines for both teams, shot ticks, gold rings, goal icons, red square (if red cards present), half-time dashed line.

---

## Task 6: Delta table HTML rendering helper

**Goal:** Implement `render_delta_table_html()` producing a ranked, direction-labelled HTML table. Per spec §5.5.

**Files:**
- Extend: `hf_taipy_app/src/state/match_summary_render.py`
- Extend: `src/tests/test_match_summary_render.py`

- [ ] **Step 1: Write failing tests**

Append to `src/tests/test_match_summary_render.py`:

```python
# --- Row 3: delta table tests ---

def _sample_stats() -> dict:
    return {
        "xG": 2.4, "Progressive passes": 47, "Shots": 16,
        "PPDA (lower = more press)": 8.2, "Possession %": 58, "Pass completion %": 87,
    }


def _sample_stats_away() -> dict:
    return {
        "xG": 0.8, "Progressive passes": 28, "Shots": 9,
        "PPDA (lower = more press)": 14.5, "Possession %": 42, "Pass completion %": 83,
    }


def test_delta_table_rows_sorted_by_abs_delta() -> None:
    from hf_taipy_app.src.state.match_summary_render import render_delta_table_html
    html = render_delta_table_html(
        home_stats=_sample_stats(), away_stats=_sample_stats_away(),
        home_name="Chelsea", away_name="Arsenal",
        league_avgs={"xG": 1.3, "Possession %": 50, "Pass completion %": 82},
    )
    # Progressive passes has the largest absolute delta (47 - 28 = 19) and should be near top
    # xG has the most editorial weight (gold star) — top row by our rules
    assert "Progressive passes" in html
    # Find row positions in the HTML
    idx_progressive = html.index("Progressive passes")
    idx_pass_completion = html.index("Pass completion")
    assert idx_progressive < idx_pass_completion, "Higher |delta| rows must precede lower-|delta| rows"


def test_delta_table_top_row_has_gold_star() -> None:
    from hf_taipy_app.src.state.match_summary_render import render_delta_table_html
    html = render_delta_table_html(
        home_stats=_sample_stats(), away_stats=_sample_stats_away(),
        home_name="Chelsea", away_name="Arsenal", league_avgs={},
    )
    # Gold star marker on the top row
    assert "★" in html or "gold-star" in html.lower() or "ll-delta-star" in html


def test_delta_table_ppda_direction_label_is_inverted() -> None:
    """PPDA: lower = more aggressive press. Direction label should communicate this."""
    from hf_taipy_app.src.state.match_summary_render import render_delta_table_html
    html = render_delta_table_html(
        home_stats=_sample_stats(), away_stats=_sample_stats_away(),
        home_name="Chelsea", away_name="Arsenal", league_avgs={},
    )
    # Chelsea PPDA 8.2 vs Arsenal 14.5 — Chelsea pressed harder (lower PPDA = more press)
    assert ("Chelsea pressed" in html) or ("pressed higher" in html) or ("more press" in html.lower())


def test_delta_table_handles_missing_league_avgs() -> None:
    from hf_taipy_app.src.state.match_summary_render import render_delta_table_html
    html = render_delta_table_html(
        home_stats=_sample_stats(), away_stats=_sample_stats_away(),
        home_name="Chelsea", away_name="Arsenal", league_avgs={},
    )
    # Should render without errors even with empty league_avgs
    assert "Chelsea" in html and "Arsenal" in html
```

- [ ] **Step 2: Run tests — confirm they fail**

Run:
```bash
uv run pytest src/tests/test_match_summary_render.py -v -k delta_table
```
Expected: FAIL — `render_delta_table_html` not defined.

- [ ] **Step 3: Implement `render_delta_table_html`**

Append to `hf_taipy_app/src/state/match_summary_render.py`:

```python
# --- Row 3: ranked delta table ---

# Metric-specific direction logic.
# For each stat, a function takes (home_val, away_val, home_name, away_name) and returns a prose direction label.
# PPDA is the canonical inverted case (lower = more press).
_DIRECTION_LABELS: dict[str, callable] = {
    "xG": lambda h, a, hn, an: f"{hn if h > a else an} created more",
    "Progressive passes": lambda h, a, hn, an: f"{hn if h > a else an} progressed the ball more",
    "Shots": lambda h, a, hn, an: f"{hn if h > a else an} shot more",
    "PPDA (lower = more press)": lambda h, a, hn, an: f"{hn if h < a else an} pressed higher",
    "Possession %": lambda h, a, hn, an: f"{hn if h > a else an} held the ball more",
    "Pass completion %": lambda h, a, hn, an: f"{hn if h > a else an} passed more accurately",
}


def render_delta_table_html(
    *,
    home_stats: dict[str, float],
    away_stats: dict[str, float],
    home_name: str,
    away_name: str,
    league_avgs: dict[str, float],
) -> str:
    """Build the Row 3 HTML: ranked delta table with directional labels."""

    rows: list[tuple[str, float, float, float, str]] = []
    for metric in home_stats:
        if metric not in away_stats:
            continue
        h = float(home_stats[metric])
        a = float(away_stats[metric])
        delta = h - a
        direction_fn = _DIRECTION_LABELS.get(metric, lambda h_, a_, hn_, an_: "")
        direction = direction_fn(h, a, home_name, away_name)
        rows.append((metric, h, a, delta, direction))

    # Sort by absolute delta descending. Ties broken by metric name for determinism.
    rows.sort(key=lambda r: (-abs(r[3]), r[0]))

    parts: list[str] = ['<table class="ll-delta-table">']
    parts.append(
        "<thead><tr>"
        "<th>Dimension</th>"
        f'<th class="ll-delta-home">{escape(home_name)}</th>'
        f'<th class="ll-delta-away">{escape(away_name)}</th>'
        "<th>Δ</th>"
        "<th>Direction</th>"
        "</tr></thead>"
    )
    parts.append("<tbody>")
    for i, (metric, h, a, delta, direction) in enumerate(rows):
        row_cls = "ll-delta-star" if i == 0 else ""
        star = "★ " if i == 0 else ""
        avg_annot = ""
        if metric in league_avgs:
            avg_annot = f' <span class="ll-delta-avg">(league avg {league_avgs[metric]:.1f})</span>'
        parts.append(
            f'<tr class="{row_cls}">'
            f'<td>{star}{escape(metric)}{avg_annot}</td>'
            f'<td class="ll-delta-home">{h:.2f}</td>'
            f'<td class="ll-delta-away">{a:.2f}</td>'
            f'<td class="ll-delta-delta">{delta:+.2f}</td>'
            f'<td class="ll-delta-direction">{escape(direction)}</td>'
            "</tr>"
        )
    parts.append("</tbody></table>")
    return "\n".join(parts)
```

- [ ] **Step 4: Run tests — confirm they pass**

Run:
```bash
uv run pytest src/tests/test_match_summary_render.py -v -k delta_table
```
Expected: 4 tests pass.

- [ ] **Step 5: Full render module test sweep**

Run:
```bash
uv run pytest src/tests/test_match_summary_render.py -v
```
Expected: all tests pass (moments + xg_race + delta = ~13 tests).

---

## Task 7: State module refresh callback rewrite

**Goal:** Replace `ms_refresh` in `hf_taipy_app/src/state/match_summary.py` with the new orchestration. Remove all 4 bar-chart rendering (`_render_stat_bars`). Wire the new helpers.

**Files:**
- Modify: `hf_taipy_app/src/state/match_summary.py`

- [ ] **Step 1: Read current state module**

Read `hf_taipy_app/src/state/match_summary.py` in full. Identify:
- The `ms_refresh` signature (expects a `state` object).
- The existing `ms_scope_*` setup via `build_scope_label_plain`.
- The existing `fetch_match_summary`, `fetch_league_averages` calls.
- The existing empty/warning state handling.

These patterns are preserved; only the render stage is replaced.

- [ ] **Step 2: Replace state variable declarations**

At the top of `hf_taipy_app/src/state/match_summary.py`, replace the existing declarations:

```python
# OLD — remove these:
ms_home_score: str = "--"
ms_away_score: str = "--"
ms_shooting_chart: str = ""
ms_passing_chart: str = ""
ms_possession_chart: str = ""
ms_ppda_chart: str = ""
ms_shooting_chart_alt: str = ""
ms_passing_chart_alt: str = ""
ms_possession_chart_alt: str = ""
ms_ppda_chart_alt: str = ""
```

with the new state variables per spec §7.2:

```python
# Tile strip
ms_home_name: str = ""
ms_away_name: str = ""
ms_final_score: str = "--"
ms_home_xg: str = "--"
ms_away_xg: str = "--"
ms_home_xg_delta: str = ""
ms_away_xg_delta: str = ""
ms_verdict_phrase: str = ""
ms_verdict_detail: str = ""

# Row 1
ms_moments_html: str = ""
ms_moments_height: str = "280px"

# Row 2 (Plotly figure, NOT a PNG path)
ms_xg_race_fig: Any = None
ms_xg_race_alt: str = ""

# Row 3
ms_delta_table_html: str = ""
ms_delta_table_height: str = "260px"
```

Update `__all__` to reflect the new names.

- [ ] **Step 3: Replace `ms_refresh` orchestration with concrete code**

Keep the existing imports at the top plus add these:

```python
from hf_taipy_app.src.state.match_summary_verdict import derive_verdict
from hf_taipy_app.src.state.match_summary_render import (
    render_moments_html, build_xg_race_figure, render_delta_table_html,
)
from hf_taipy_app.src.queries.match import (
    fetch_match_summary, fetch_league_averages,
    fetch_vaep_decisive_actions, fetch_shots_timeline, fetch_discipline_events,
)
from hf_taipy_app.src.render import HOME_COLOR, AWAY_COLOR
```

Replace the full `ms_refresh` body with:

```python
def ms_refresh(state: Any) -> None:
    """Reload match, derive verdict, fetch VAEP decisive actions + shot timeline + cards,
    render Row 1 HTML, build Row 2 Plotly figure, render Row 3 HTML."""

    comp_id = get_comp_id(state.selected_competition)
    match_id = get_match_id(state.selected_match)

    def _clear_all() -> None:
        for attr, default in [
            ("ms_home_name", ""), ("ms_away_name", ""),
            ("ms_final_score", "--"), ("ms_home_xg", "--"), ("ms_away_xg", "--"),
            ("ms_home_xg_delta", ""), ("ms_away_xg_delta", ""),
            ("ms_verdict_phrase", ""), ("ms_verdict_detail", ""),
            ("ms_moments_html", ""), ("ms_xg_race_fig", None),
            ("ms_xg_race_alt", ""), ("ms_delta_table_html", ""),
            ("ms_scope_comp", ""), ("ms_scope_team", ""), ("ms_scope_match", ""),
            ("ms_warning_text", ""), ("ms_data_freshness", ""),
            ("ms_league_averages", ""),
        ]:
            setattr(state, attr, default)

    if match_id is None:
        _clear_all()
        return

    # Scope line
    comp_label = state.selected_competition or ""
    team_label = state.selected_team if state.selected_team not in (None, _ALL_LABEL) else "All teams"
    match_label = state.selected_match if state.selected_match not in (None, _ALL_LABEL) else "—"
    state.ms_scope_comp = comp_label
    state.ms_scope_team = team_label
    state.ms_scope_match = match_label
    scope_plain = build_scope_label_plain([
        ("Competition", comp_label), ("Team", team_label), ("Match", match_label),
    ])

    # Fetch match summary
    match_data = fetch_match_summary(match_id)
    if match_data.empty:
        _clear_all()
        state.ms_warning_text = build_warning(domain="match data", suggestions=["choosing a different match"])
        return

    m = match_data.iloc[0]
    state.ms_warning_text = ""

    # Scorecard
    home_name = str(m["home_team_name"])
    away_name = str(m["away_team_name"])
    home_score = int(m["home_score"] or 0)
    away_score = int(m["away_score"] or 0)
    home_xg = float(m["home_xg"] or 0)
    away_xg = float(m["away_xg"] or 0)
    home_team_id = int(m["home_team_id"])
    away_team_id = int(m["away_team_id"])

    state.ms_home_name = home_name
    state.ms_away_name = away_name
    state.ms_final_score = f"{home_score} \u2014 {away_score}"
    state.ms_home_xg = f"{home_xg:.2f}"
    state.ms_away_xg = f"{away_xg:.2f}"
    state.ms_home_xg_delta = f"{home_score - home_xg:+.2f} vs actual"
    state.ms_away_xg_delta = f"{away_score - away_xg:+.2f} vs actual"

    # Verdict
    phrase, detail = derive_verdict(home_xg, away_xg, home_score, away_score)
    state.ms_verdict_phrase = phrase
    state.ms_verdict_detail = detail

    # Fetch decisive actions + shots + discipline
    try:
        decisive = fetch_vaep_decisive_actions(match_id, n=3)
        shots = fetch_shots_timeline(match_id)
        discipline = fetch_discipline_events(match_id)
    except Exception:
        # ADR-002 §2 — ERROR-level, not silent. Raises to surface in observability.
        logger.exception("Match Summary data fetch failed for match_id=%s", match_id)
        _clear_all()
        state.ms_warning_text = build_warning(
            domain="match data", suggestions=["choosing a different match", "retrying"],
        )
        return

    # Row 1 HTML
    state.ms_moments_html = render_moments_html(decisive, discipline, scope_plain=scope_plain)

    # Row 2 Plotly figure
    state.ms_xg_race_fig = build_xg_race_figure(
        shots=shots, decisive=decisive, red_cards=discipline,
        home_team_id=home_team_id, home_team_name=home_name,
        away_team_id=away_team_id, away_team_name=away_name,
        home_color=HOME_COLOR, away_color=AWAY_COLOR,
    )
    state.ms_xg_race_alt = f"xG race and decisive moments — {scope_plain}"

    # Row 3 delta table — assemble stats dicts from the match-summary row
    home_stats = {
        "xG": home_xg,
        "Progressive passes": float(m.get("home_progressive_passes", 0) or 0),
        "Shots": float(m.get("home_shots", 0) or 0),
        "PPDA (lower = more press)": float(m.get("home_ppda", 0) or 0),
        "Possession %": float(m.get("home_possession_pct", 50) or 50),
        "Pass completion %": float(m.get("home_pass_completion_pct", 0) or 0),
    }
    away_stats = {
        "xG": away_xg,
        "Progressive passes": float(m.get("away_progressive_passes", 0) or 0),
        "Shots": float(m.get("away_shots", 0) or 0),
        "PPDA (lower = more press)": float(m.get("away_ppda", 0) or 0),
        "Possession %": 100.0 - float(m.get("home_possession_pct", 50) or 50),
        "Pass completion %": float(m.get("away_pass_completion_pct", 0) or 0),
    }

    # League averages for reference
    league_avgs: dict[str, float] = {}
    if comp_id is not None:
        try:
            avg_df = fetch_league_averages(comp_id)
            if not avg_df.empty:
                avg = avg_df.iloc[0]
                league_avgs = {
                    "xG": float(avg.get("avg_xg_per_team", 0) or 0),
                    "Possession %": float(avg.get("avg_possession", 50) or 50),
                    "Pass completion %": float(avg.get("avg_pass_completion", 0) or 0),
                }
                state.ms_league_averages = (
                    f"League avg: {league_avgs['xG']:.2f} xG/team · "
                    f"{league_avgs['Possession %']:.0f}% possession · "
                    f"{league_avgs['Pass completion %']:.0f}% pass completion"
                )
        except Exception:
            logger.exception("Failed to fetch league averages for comp_id=%s", comp_id)
            state.ms_league_averages = ""

    state.ms_delta_table_html = render_delta_table_html(
        home_stats=home_stats, away_stats=away_stats,
        home_name=home_name, away_name=away_name,
        league_avgs=league_avgs,
    )

    state.ms_data_freshness = fetch_data_freshness()

    logger.info(
        "Match Summary redesigned refreshed: %s %d-%d %s (xG: %.2f-%.2f, verdict: %s)",
        home_name, home_score, away_score, away_name, home_xg, away_xg, phrase,
    )
```

Delete the old `_render_stat_bars` helper function entirely.

**Note on player/team name enrichment for Row 1:** the current `fetch_vaep_decisive_actions` query (Task 3) returns `player_id` and `team_id` only. `render_moments_html` in Task 4 expects `player_name` and `team_name` columns. Choose one of two enrichment approaches during implementation:

- **Approach A (preferred)** — extend the SQL in Task 3 to JOIN `dim_players` and `dim_teams` and return `player_name` / `team_name` directly. Cleanest; makes the query self-sufficient.
- **Approach B** — enrich in Python after fetching, using existing name-lookup helpers if they exist in `queries/` or `state/shared.py`. If no helper exists, Approach A is the only sensible option.

Verify which dimension tables exist (`dim_players`, `dim_teams`, or equivalent) before adding the JOIN.

- [ ] **Step 4: Smoke test the refreshed state module**

Run:
```bash
uv run python -c "
from hf_taipy_app.src.state.match_summary import ms_refresh
# Minimal mock state
class S: pass
state = S()
state.selected_competition = '<choose comp>'
state.selected_team = '<All>'
state.selected_match = '<choose match>'
# Populate the state shell with default ms_ vars as the module's module-level declarations set them.
for attr in ['ms_home_name','ms_away_name','ms_final_score','ms_home_xg','ms_away_xg',
             'ms_verdict_phrase','ms_verdict_detail','ms_moments_html','ms_xg_race_fig',
             'ms_delta_table_html','ms_scope_comp','ms_scope_team','ms_scope_match',
             'ms_warning_text','ms_data_freshness','ms_league_averages','ms_home_xg_delta',
             'ms_away_xg_delta','ms_moments_height','ms_delta_table_height','ms_xg_race_alt']:
    setattr(state, attr, '' if 'html' in attr or 'alt' in attr or 'scope' in attr or 'freshness' in attr or 'averages' in attr or 'delta' in attr or 'phrase' in attr or 'detail' in attr or 'name' in attr or 'warning' in attr or 'height' in attr else '--')
setattr(state, 'ms_xg_race_fig', None)
ms_refresh(state)
print('score:', state.ms_final_score)
print('verdict:', state.ms_verdict_phrase, '-', state.ms_verdict_detail)
print('moments html length:', len(state.ms_moments_html))
print('figure type:', type(state.ms_xg_race_fig).__name__)
print('delta table length:', len(state.ms_delta_table_html))
"
```
Expected: non-empty values for score / verdict / moments / figure / delta. If empty, check warning_text for the source.

- [ ] **Step 5: Lint + type**

Run:
```bash
uv run ruff check hf_taipy_app/src/state/match_summary.py
uv run pyright hf_taipy_app/src/state/match_summary.py
```
Expected: no issues.

---

## Task 8: Page config rewrite

**Goal:** Switch `PageConfig` from `metrics` (sidebar layout) to `stats` (dashboard layout). Update `ContentRow`s to reference the new state vars. Update citations.

**Files:**
- Modify: `hf_taipy_app/src/pages/match_summary.py`

- [ ] **Step 1: Rewrite the PageConfig**

Replace the entire `page_config = PageConfig(...)` block with:

```python
page_config = PageConfig(
    title="Match Summary",
    icon="scoreboard",
    nav_section=NAV_MATCH_ANALYSIS,
    description=(
        "Match scorecard with editorial verdict + decisive-action narrative. "
        "xG per Robberechts & Davis (2020). VAEP per Decroos et al. (2019), computed via silly-kicks. "
        "PPDA per Trainor & Chassy (2021)."
    ),
    citations=[
        Citation(
            "Robberechts & Davis (2020) — How Data Availability Affects the Ability to Learn Good xG Models",
            "https://dtai.cs.kuleuven.be/sports/blog/how-data-availability-affects-the-ability-to-learn-good-xg-models",
        ),
        Citation(
            "Decroos et al. (2019) — Actions Speak Louder than Goals: Valuing Player Actions in Soccer",
            "https://doi.org/10.1145/3292500.3330758",
        ),
        Citation("silly-kicks", "https://github.com/karsten-s-nielsen/silly-kicks"),
        Citation("Trainor & Chassy (2021)", "https://doi.org/10.3389/fpsyg.2020.531688"),
    ],
    scope_dims=[
        ScopeDim("Competition", "ms_scope_comp"),
        ScopeDim("Team", "ms_scope_team"),
        ScopeDim("Match", "ms_scope_match"),
    ],
    stats=[
        StatCard(
            label="Final",
            var="ms_final_score",
            help_text="Full-time score. Home team listed first.",
        ),
        StatCard(
            label="Home xG",
            var="ms_home_xg",
            detail_var="ms_home_xg_delta",
            help_text="Expected goals from shot locations and context. Delta = goals minus xG; positive = overperformed.",
        ),
        StatCard(
            label="Away xG",
            var="ms_away_xg",
            detail_var="ms_away_xg_delta",
            help_text="Expected goals from shot locations and context. Delta = goals minus xG; positive = overperformed.",
        ),
        StatCard(
            label="Our Verdict",
            var="ms_verdict_phrase",
            detail_var="ms_verdict_detail",
            help_text=(
                "Editorial interpretation of whether the scoreline reflected the run of play, by xG margin. "
                "Phrase set: Fully merited / Fair result / Fortunate / Smash & grab / Flattered by scoreline."
            ),
        ),
    ],
    content=[
        ContentRow([
            ContentBlock(
                "html", "ms_moments_html",
                height_var="ms_moments_height",
                container_class="ll-match-moments-wrap",
            ),
        ]),
        ContentRow([
            ContentBlock(
                "chart", "ms_xg_race_fig",
                chart_height="320px",
                alt_var="ms_xg_race_alt",
            ),
        ]),
        ContentRow([
            ContentBlock(
                "html", "ms_delta_table_html",
                height_var="ms_delta_table_height",
                container_class="ll-delta-table-wrap",
            ),
        ]),
    ],
    empty_message="Select a competition and match to begin.",
    empty_condition="len(ms_home_name) == 0",
    warning_var="ms_warning_text",
    scope_vars=["ms_league_averages"],
    freshness_var="ms_data_freshness",
)
page_md = build_page(page_config)
```

- [ ] **Step 2: Update the import**

Verify `StatCard` is imported at the top:

```python
from page_template import (
    NAV_MATCH_ANALYSIS,
    Citation,
    ContentBlock,
    ContentRow,
    PageConfig,
    ScopeDim,
    StatCard,  # <- new import if not already present
    build_page,
)
```

- [ ] **Step 3: Remove the old `metrics=[...]` block**

Confirm `metrics=` is entirely absent from the new config (dashboard layout is triggered by `stats` being non-empty, per template dispatcher at `page_template.py:884`).

- [ ] **Step 4: Lint + type**

Run:
```bash
uv run ruff check hf_taipy_app/src/pages/match_summary.py
uv run pyright hf_taipy_app/src/pages/match_summary.py
```
Expected: no issues.

---

## Task 9: Glossary, PAGE_TERMS, NOTICE updates

**Goal:** Match Summary's domain terms reach the glossary; silly-kicks NOTICE entry added if missing.

**Files:**
- Modify: `hf_taipy_app/src/template.py` (GLOSSARY + PAGE_TERMS)
- Modify: `NOTICE` (silly-kicks if not present)

- [ ] **Step 1: Read current `PAGE_TERMS` and `GLOSSARY`**

Open `hf_taipy_app/src/template.py` and locate `GLOSSARY` and `PAGE_TERMS`. Identify the existing `"Match-Summary"` entry in `PAGE_TERMS` (add if missing).

- [ ] **Step 2: Add Match Summary glossary terms**

Extend `GLOSSARY` with entries (if not already present):

```python
GLOSSARY: dict[str, str] = {
    # ... existing entries ...
    "VAEP": "Valuing Actions by Estimating Probabilities. Values every on-ball action by its effect on scoring probability. Decroos et al. 2019.",
    "Our Verdict": "Editorial interpretation of whether the scoreline reflected the run of play, derived from xG margin.",
    "Big Story": "The single most decisive action of the match by VAEP impact — highlighted as the hero card of Row 1.",
    "Smash & grab": "A win where the winner's xG was significantly below the loser's — result against the run of play.",
    "Fully merited": "A win where the winner's xG exceeded the loser's by at least 0.5 — clear xG dominance.",
    "Fortunate": "A win where the winner's xG was below the loser's but by a small margin.",
    "Flattered by scoreline": "A win where the final score underrepresents how much the winner created — xG suggests they should have scored more.",
    "Fair result": "A match where the two teams' xG were within 0.3 — no clear xG advantage either way.",
    # ... keep existing xG, PPDA, progressive pass entries if already present, else add:
    "xG": "Expected goals — probability of a shot resulting in a goal, learned from shot context (location, angle, body part, defenders).",
    "PPDA": "Passes Per Defensive Action. Pressing-intensity metric — LOWER = more aggressive press.",
    "progressive pass": "A pass that advances the ball substantially toward goal, per StatsBomb/SPADL definition.",
}
```

Update `PAGE_TERMS["Match-Summary"]`:

```python
PAGE_TERMS: dict[str, list[str]] = {
    # ... existing entries ...
    "Match-Summary": [
        "xG", "PPDA", "VAEP", "progressive pass",
        "Our Verdict", "Big Story",
        "Fully merited", "Fair result", "Fortunate", "Smash & grab", "Flattered by scoreline",
    ],
    # ... etc
}
```

- [ ] **Step 3: Verify + add NOTICE entries**

Run:
```bash
grep -n "silly-kicks\|silly_kicks" NOTICE
```
If silly-kicks entry exists under Third-Party Libraries, no change needed. If missing, add:

```
silly-kicks (https://github.com/karsten-s-nielsen/silly-kicks) is used for SPADL action-data conversion and VAEP model computation. Replaces the unmaintained socceraction library. Apache 2.0 licensed.
```

Append to the existing Third-Party Libraries section, preserving file ordering.

- [ ] **Step 4: Lint**

Run:
```bash
uv run ruff check hf_taipy_app/src/template.py
```
Expected: no issues.

---

## Task 10: Mobile responsive CSS

**Goal:** Add `@media (max-width: 768px)` rules per spec §5.7. Platform-level changes for `ll-stats-bar`; page-specific for `ll-match-moments`, `ll-delta-table`.

**Files:**
- Modify: `hf_taipy_app/src/style_v2.css`

- [ ] **Step 1: Read current style_v2.css for the relevant sections**

Run:
```bash
grep -n "ll-stats-bar\|@media\|768\|ll-match-moments\|ll-delta" hf_taipy_app/src/style_v2.css
```
Expected: existing rules for `.ll-stats-bar` (likely `display: grid; grid-template-columns: repeat(4, 1fr)`). Note the current selector.

- [ ] **Step 2: Append new Row 1 / Row 3 / mobile CSS**

Append to `hf_taipy_app/src/style_v2.css`:

```css
/* ===========================
 * Match Summary — Row 1 Big Story + moments
 * =========================== */
.ll-match-moments-wrap { padding: 0 8px; }
.ll-match-moments { display: grid; gap: 10px; }
.ll-big-story-label { color: #d9a300; font-size: 11px; text-transform: uppercase; letter-spacing: 0.6px; margin-bottom: 4px; }
.ll-moment-card { background: rgba(255,255,255,0.04); border-left: 2px solid #d9a300; border-radius: 4px; padding: 10px 14px; display: grid; grid-template-columns: 50px 1fr auto; gap: 12px; align-items: center; }
.ll-big-story-hero { background: rgba(255,215,0,0.08); border: 1px solid rgba(255,215,0,0.25); border-left: 3px solid #d9a300; padding: 14px 18px; }
.ll-moments-secondary { display: grid; grid-template-columns: 1fr 1fr; gap: 8px; }
.ll-moment-card-secondary { padding: 8px 12px; font-size: 12px; }
.ll-moment-card-red-card { border-left-color: #c84444; background: rgba(200,68,68,0.08); }
.ll-moment-minute { color: #d9a300; font-weight: 600; font-size: 13px; }
.ll-moment-card-red-card .ll-moment-minute { color: #e77; }
.ll-moment-vaep { font-family: monospace; color: #9bb; font-size: 11px; }
.ll-moments-caveat { color: #888; font-size: 10px; font-style: italic; margin-top: 6px; padding: 0 8px; }

/* ===========================
 * Match Summary — Row 3 delta table
 * =========================== */
.ll-delta-table-wrap { padding: 0 8px; }
.ll-delta-table { width: 100%; border-collapse: collapse; font-size: 12px; }
.ll-delta-table th, .ll-delta-table td { padding: 6px 10px; border-bottom: 1px solid #2a2a2a; text-align: left; }
.ll-delta-table th { color: #888; font-size: 10px; text-transform: uppercase; letter-spacing: 0.5px; font-weight: 500; }
.ll-delta-home { color: #5a9; }
.ll-delta-away { color: #a55; }
.ll-delta-delta { font-family: monospace; font-weight: 600; }
.ll-delta-direction { color: #ccc; font-size: 11px; }
.ll-delta-star { background: rgba(255,215,0,0.04); }
.ll-delta-star td:first-child { color: #d9a300; font-weight: 600; }
.ll-delta-avg { color: #666; font-size: 10px; margin-left: 4px; }

/* ===========================
 * Mobile responsive — 768px breakpoint
 * (platform-wide for .ll-stats-bar; page-specific for ll-match-moments + ll-delta-table)
 * =========================== */
@media (max-width: 768px) {
  .ll-stats-bar { grid-template-columns: repeat(2, 1fr); gap: 8px; }
  .ll-big-story-hero { padding: 10px 12px; }
  .ll-moments-secondary { grid-template-columns: 1fr; }
  .ll-moment-card { grid-template-columns: 40px 1fr auto; padding: 8px 10px; }
  .ll-moment-card-secondary { font-size: 11px; }
  .ll-delta-table th, .ll-delta-table td { padding: 5px 7px; }
  .ll-delta-direction { font-size: 10px; }
}
```

- [ ] **Step 3: Visual smoke test locally** (if possible)

If Taipy can be run locally:

```bash
cd hf_taipy_app && uv run python src/main.py
```

Open `http://localhost:7860/Match-Summary`; use browser devtools to toggle device emulation to iPhone SE (375px) and iPad (768px). Verify:
- 4 tiles stack 2×2 on mobile
- Secondary cards stack vertically
- Delta table remains readable

If local Taipy run is not feasible (missing env vars), skip — staging E2E in Task 12 covers this.

- [ ] **Step 4: Lint CSS (if there is a CSS linter in the project)**

Run:
```bash
grep -r "csslint\|stylelint" hf_taipy_app/
```
If no CSS linter is configured, skip.

---

## Task 11: Local verification sweep

**Goal:** Full-suite local sign-off before staging deploy.

- [ ] **Step 1: Ruff lint + format check**

Run:
```bash
uv run ruff check hf_taipy_app/src/pages/match_summary.py \
                   hf_taipy_app/src/state/match_summary.py \
                   hf_taipy_app/src/state/match_summary_verdict.py \
                   hf_taipy_app/src/state/match_summary_render.py \
                   hf_taipy_app/src/queries/match.py \
                   hf_taipy_app/src/template.py \
                   src/tests/test_match_summary_verdict.py \
                   src/tests/test_match_summary_render.py \
                   src/tests/test_queries_match_extended.py
uv run ruff format --check hf_taipy_app/src/pages/match_summary.py \
                            hf_taipy_app/src/state/match_summary.py \
                            hf_taipy_app/src/state/match_summary_verdict.py \
                            hf_taipy_app/src/state/match_summary_render.py \
                            hf_taipy_app/src/queries/match.py \
                            hf_taipy_app/src/template.py \
                            src/tests/test_match_summary_verdict.py \
                            src/tests/test_match_summary_render.py \
                            src/tests/test_queries_match_extended.py
```
Expected: zero violations. If format check fails, run the format command to fix.

- [ ] **Step 2: Pyright type check**

Run:
```bash
uv run pyright hf_taipy_app/src/pages/match_summary.py \
                hf_taipy_app/src/state/match_summary.py \
                hf_taipy_app/src/state/match_summary_verdict.py \
                hf_taipy_app/src/state/match_summary_render.py \
                hf_taipy_app/src/queries/match.py
```
Expected: zero errors.

- [ ] **Step 3: Full pytest for new tests + affected existing tests**

Run:
```bash
uv run pytest src/tests/test_match_summary_verdict.py \
               src/tests/test_match_summary_render.py \
               src/tests/test_queries_match_extended.py \
               -v
```
Expected: all new tests pass.

Also run any existing Taipy-related tests that touch Match Summary:

```bash
uv run pytest src/tests/ -v -k "match_summary or taipy"
```
Expected: no regressions.

---

## Task 12: Staging deploy + Puppeteer E2E

**Goal:** Deploy the branch to the staging HF Space and verify the redesigned page renders correctly for 3 representative matches.

**Files:**
- No code changes in this task — deploy + test only.

- [ ] **Step 1: Deploy to staging**

Run:
```bash
uv run python scripts/manage_space.py deploy staging
```
Expected: staging deploy completes, Space boots. Note the staging URL.

- [ ] **Step 2: Pick three representative matches for testing**

Using Databricks SQL, pick three matches:
- (a) One with clear xG dominance (e.g. home xG ≥ 2.0, away xG ≤ 0.8 — should produce `Fully merited`)
- (b) One with a close result (`|xG Δ| < 0.3` — should produce `Fair result`)
- (c) One with a red card (validates Row 1 auto-include + Row 2 red-card marker)

Record the competition/team/match selections for each.

- [ ] **Step 3: Puppeteer click-through — match (a)**

Write a short Puppeteer script (or extend existing) that:
1. Navigates to the staging Space Match-Summary page.
2. Selects the match via the sidebar dropdowns (use the `ll_ext.combobox` interaction pattern per `reference_puppeteer_taipy_dropdowns` memory: click `.MuiSelect-select` to open, click `li.MuiMenuItem-root` to select, await round-trip).
3. Waits for the page to render.
4. Asserts:
   - All 4 tiles display values (not `--`)
   - Verdict tile phrase is `"Fully merited"` (case (a) expectation)
   - Row 1 contains 3 cards (hero + 2 secondary)
   - Row 2 chart has loaded (non-empty canvas / SVG)
   - Row 3 table has ≥ 5 rows
5. Captures a screenshot to `.superpowers/brainstorm/` or a test artifact dir.

Expected: all assertions pass.

- [ ] **Step 4: Puppeteer click-through — match (b)**

Repeat for the close-result match. Expected verdict phrase: `"Fair result"`.

- [ ] **Step 5: Puppeteer click-through — match (c) with red card**

Repeat for the red-card match. Additional assertions:
- Row 1 contains 4 cards (hero + 2 secondary + red-card card)
- The red-card card is visually distinct (has `ll-moment-card-red-card` class)
- Row 2 chart contains a red-card marker at the red-card minute

Expected: all assertions pass.

- [ ] **Step 6: Mobile viewport test**

Using Puppeteer's viewport emulation, reload the Match Summary page at 375×812 (iPhone SE) and 768×1024 (iPad edge). Assert:
- Tile strip stacks to 2×2 at <768px
- No horizontal scroll on the page overall
- Hero card is full-width; secondary cards stack vertically
- Delta table remains readable

Expected: visual screenshots captured; no broken layout.

- [ ] **Step 7: Sanity-check all preserved behaviours**

- Scope line still shows "Competition · Team · Match" correctly
- Data freshness footer present
- Warning/empty state when a match has no data (test by trying a match without full StatsBomb data)
- Citations panel lists Decroos, silly-kicks, Robberechts & Davis, Trainor & Chassy

Expected: no regressions on preserved behaviours.

---

## Task 13: Final commit + push + PR

**Goal:** After all prior tasks green, commit everything and open the PR. No commits before this point.

**Files:** all accumulated changes across Tasks 2–10.

- [ ] **Step 1: Confirm all work is ready**

Run:
```bash
git status
```
Expected output should show:
- Modified: `hf_taipy_app/src/pages/match_summary.py`, `hf_taipy_app/src/state/match_summary.py`, `hf_taipy_app/src/queries/match.py`, `hf_taipy_app/src/template.py`, `hf_taipy_app/src/style_v2.css`, `NOTICE`
- Created: `hf_taipy_app/src/state/match_summary_verdict.py`, `hf_taipy_app/src/state/match_summary_render.py`, `src/tests/test_match_summary_verdict.py`, `src/tests/test_match_summary_render.py`, `src/tests/test_queries_match_extended.py`, `docs/superpowers/specs/2026-04-19-match-summary-redesign-design.md`, `docs/superpowers/plans/2026-04-19-match-summary-redesign.md`
- Modified: `TODO.md` (U7 entry)

- [ ] **Step 2: Stage all files explicitly (never `git add -A`)**

Run (adjust paths to match actual changes):

```bash
git add \
  docs/superpowers/specs/2026-04-19-match-summary-redesign-design.md \
  docs/superpowers/plans/2026-04-19-match-summary-redesign.md \
  hf_taipy_app/src/pages/match_summary.py \
  hf_taipy_app/src/state/match_summary.py \
  hf_taipy_app/src/state/match_summary_verdict.py \
  hf_taipy_app/src/state/match_summary_render.py \
  hf_taipy_app/src/queries/match.py \
  hf_taipy_app/src/template.py \
  hf_taipy_app/src/style_v2.css \
  NOTICE \
  TODO.md \
  src/tests/test_match_summary_verdict.py \
  src/tests/test_match_summary_render.py \
  src/tests/test_queries_match_extended.py
```

- [ ] **Step 3: Await user commit approval**

Before running `git commit`, surface the staged diff to the user and await explicit approval per user rule "every commit needs separate explicit approval".

- [ ] **Step 4: Commit (after approval)**

```bash
git commit -m "$(cat <<'EOF'
feat: match summary redesign — scorecard + Big Story + xG race + delta table

Rewrites the Match Summary page from four grouped horizontal bar charts into
a dashboard-layout editorial page per the 2026-04-19 design spec:

- Top tile strip: Final · Home xG · Away xG · Our Verdict (5-phrase vocabulary
  mapped to xG-delta thresholds: Fully merited / Fair result / Fortunate /
  Smash & grab / Flattered by scoreline).
- Row 1 Big Story: hero card + 2 secondary cards ranked by |VAEP value| from
  fct_action_values. Red cards auto-included when present. Decisive on-ball
  actions per Decroos et al. 2019 (silly-kicks implementation).
- Row 2: Plotly xG race with stepped cumulative lines, shot ticks sized by xG,
  gold decisive-action rings, goal stars, red-card markers, half-time divider.
  Native hover tooltips per spec §5.4.
- Row 3: ranked delta table sorted by |Δ| with directional labels. PPDA's
  inverted direction (lower = more press) correctly communicated.
- Mobile responsive CSS at 768px breakpoint. ll-stats-bar changes are
  platform-level; Conversion Funnel and Workflows verified non-regressing at
  desktop sizes.
- Citations updated: Decroos et al. (2019) with verified ACM KDD DOI, plus
  silly-kicks repo reference.

Spec: docs/superpowers/specs/2026-04-19-match-summary-redesign-design.md
Plan: docs/superpowers/plans/2026-04-19-match-summary-redesign.md
TODO.md U7: verdict-vocabulary expansion parked as Dunkin' On-Deck.

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>
EOF
)"
```

- [ ] **Step 5: Push branch**

```bash
git push -u origin ui/match-summary-redesign
```

- [ ] **Step 6: Open PR (after user approval)**

```bash
gh pr create --title "feat: match summary redesign — scorecard + Big Story + xG race + delta table" --body "$(cat <<'EOF'
## Summary

Rewrites Match Summary from a 4-grouped-bar stat dump into an editorial scorecard page with tile strip, Big Story decisive actions, Plotly xG race, and ranked delta table.

See full design spec: `docs/superpowers/specs/2026-04-19-match-summary-redesign-design.md`
Implementation plan: `docs/superpowers/plans/2026-04-19-match-summary-redesign.md`

## What changed

- Layout family: Standard → Dashboard (top tiles + full-width content)
- Row 1: Big Story hero + 2 secondary cards, VAEP-ranked (silly-kicks / Decroos et al. 2019)
- Row 2: Plotly xG race with native hover
- Row 3: Ranked delta table with editorial direction labels
- Mobile: responsive at 768px (2×2 tiles, stacked cards)
- Citations updated, glossary extended, NOTICE maintained

## Test plan

- [ ] CI green (lint, type, pytest)
- [ ] Staging E2E (Puppeteer) against 3 representative matches already run — see task 12 artifacts
- [ ] Verdict phrase test matrix covered in test_match_summary_verdict.py
- [ ] No regression on Conversion Funnel / Workflows (platform-level CSS change at <768px)

## Follow-ups (out of scope)

- TODO.md U7: verdict vocabulary expansion
- Click-through linking (v2)
- NOTICE citation sweep (tracked as `project_notice_citation_sweep_pending`)

🤖 Generated with [Claude Code](https://claude.com/claude-code)
EOF
)"
```

- [ ] **Step 7: Monitor CI**

Run:
```bash
gh pr checks <pr-number> --watch
```
Expected: all checks green. If not, do NOT merge — investigate and push fixes on the same branch.

---

## Self-review checklist (to be completed by the executor, not the planner)

Before marking the plan complete, executor verifies:

- [ ] Every spec section (§1–§17) maps to at least one task.
- [ ] Every new state variable in spec §7.2 is assigned in the refresh callback (Task 7).
- [ ] Every helper contract in spec §7.3 is implemented (Tasks 2, 4, 5, 6).
- [ ] Every query in spec §7.4 is implemented (Task 3).
- [ ] Verdict vocabulary in spec §8 is fully tested (Task 2).
- [ ] Citations update in spec §9 is applied (Task 8).
- [ ] Glossary additions in spec §10 are made (Task 9).
- [ ] Testing plan in spec §13 is executed (Tasks 11 + 12).
- [ ] Success criteria in spec §14 are verified (Task 12).
- [ ] Deferred items in spec §15 are NOT accidentally implemented (scope discipline).
- [ ] No commits before Task 13.
