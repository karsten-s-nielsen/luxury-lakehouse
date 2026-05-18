# SPADL Team ID Resolution Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Fix NULL `team_id` in tracking-provider SPADL actions by hashing `team_id_native`, preventing NaN VAEP values.

**Architecture:** Replace the intentional `team_id = NULL` fill in 3 SPADL conversion UDFs (IDSSE, Metrica, SkillCorner) with `hash_native_id_to_bigint(team_id_native)`. Add defense-in-depth guard in the VAEP scoring UDF. Handle NULL `team_id_native` edge cases (4 IDSSE freekick_short rows) with a deterministic sentinel hash.

**Tech Stack:** Python 3.10, pandas, XGBoost, silly-kicks, pytest

---

## File Structure

| File | Responsibility |
|------|---------------|
| `src/ingestion/spadl_adapter.py` | Add `UNKNOWN_TEAM_SENTINEL` constant (single source of truth) |
| `src/ingestion/spadl_conversion.py` | Replace NULL-fill with hash in 3 UDFs; use sentinel constant |
| `src/ingestion/spadl_vaep.py` | Defense-in-depth NULL team_id guard in scoring UDF |
| `src/tests/test_spadl_team_resolution.py` | **New**: unit tests for team_id hash resolution |
| `src/tests/test_spadl_vaep_tracking_providers.py` | **New**: integration test for VAEP on tracking-provider data |
| `src/tests/test_spadl_vaep.py` | Add structural regression guard (AST/grep) for NULL team_id |

---

### Task 1: Sentinel Constant + Unit Tests

**Files:**
- Modify: `src/ingestion/spadl_adapter.py:37-38`
- Create: `src/tests/test_spadl_team_resolution.py`

- [ ] **Step 1: Add sentinel constant to spadl_adapter.py**

In `src/ingestion/spadl_adapter.py`, immediately before `hash_native_id_to_bigint` (after line 38), add:

```python
UNKNOWN_TEAM_SENTINEL = "__UNKNOWN_TEAM__"
"""Deterministic sentinel for rows where ``team_id_native`` is NULL.

Used by tracking-provider SPADL UDFs (IDSSE, Metrica, SkillCorner) when
silly-kicks emits a team label that the home/away mapper cannot resolve
(e.g. freekick_short events). Hashed via ``hash_native_id_to_bigint`` to
produce a stable BIGINT that differs from all real team hashes.

Single source of truth — imported by all 3 UDFs and test assertions.
"""
```

- [ ] **Step 2: Write the unit test file**

Create `src/tests/test_spadl_team_resolution.py`:

```python
"""Unit tests for team_id hash resolution in tracking-provider SPADL converters.

Validates that the hash_native_id_to_bigint function produces correct, consistent,
and distinct team_id values from team_id_native strings — the core invariant that
VAEP scoring depends on (same-team equality comparison).
"""

from __future__ import annotations

import logging

import pandas as pd
import pytest

from ingestion.spadl_adapter import UNKNOWN_TEAM_SENTINEL, hash_native_id_to_bigint


# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

# Realistic native team IDs per provider
_IDSSE_HOME = "DFL-CLU-000008"
_IDSSE_AWAY = "DFL-CLU-00000G"
_METRICA_HOME = "metrica_Sample_Game_1_home"
_METRICA_AWAY = "metrica_Sample_Game_1_away"
_SKILLCORNER_HOME = "1805"
_SKILLCORNER_AWAY = "1806"


# ---------------------------------------------------------------------------
# Helper: replicates the production team_id resolution pattern
# ---------------------------------------------------------------------------


def _apply_team_id_hash(actions: pd.DataFrame, match_id_str: str) -> pd.DataFrame:
    """Replicate the team_id resolution logic applied in spadl_conversion.py.

    WARNING: This is a test-local copy of the production pattern. If the
    production logic in _make_idsse_spadl_udf / _make_metrica_spadl_udf /
    _make_skillcorner_spadl_udf changes, this helper must be updated too.
    The structural regression guard in test_spadl_vaep.py catches divergence
    by asserting the NULL-fill pattern is absent from production code.
    """
    _logger = logging.getLogger(__name__)
    null_team_mask = actions["team_id_native"].isna()
    if null_team_mask.any():
        _logger.warning(
            "NULL team_id_native in %d rows for match_id=%s (type_ids=%s). "
            "Filling with sentinel hash.",
            null_team_mask.sum(),
            match_id_str,
            actions.loc[null_team_mask, "type_id"].unique().tolist(),
        )
        actions.loc[null_team_mask, "team_id_native"] = UNKNOWN_TEAM_SENTINEL
    actions["team_id"] = actions["team_id_native"].map(hash_native_id_to_bigint).astype("Int64")
    return actions


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


class TestTeamIdPopulated:
    """team_id is non-NULL and correctly hashed for each provider."""

    def test_team_id_populated_for_idsse_actions(self) -> None:
        """IDSSE DFL-CLU-* native IDs produce non-NULL team_id hashes."""
        actions = pd.DataFrame(
            {
                "team_id_native": pd.array(
                    [_IDSSE_HOME, _IDSSE_AWAY, _IDSSE_HOME, _IDSSE_AWAY],
                    dtype="string",
                ),
                "type_id": [0, 0, 1, 8],
            }
        )
        result = _apply_team_id_hash(actions, "J03WMX")
        assert result["team_id"].notna().all()
        # Same native → same hash
        home_ids = result.loc[result["team_id_native"] == _IDSSE_HOME, "team_id"]
        assert home_ids.nunique() == 1

    def test_team_id_populated_for_metrica_actions(self) -> None:
        """Metrica synthetic native IDs produce non-NULL team_id hashes."""
        actions = pd.DataFrame(
            {
                "team_id_native": pd.array(
                    [_METRICA_HOME, _METRICA_AWAY, _METRICA_HOME],
                    dtype="string",
                ),
                "type_id": [0, 0, 11],
            }
        )
        result = _apply_team_id_hash(actions, "Sample_Game_1")
        assert result["team_id"].notna().all()

    def test_team_id_populated_for_skillcorner_actions(self) -> None:
        """SkillCorner numeric-string native IDs produce non-NULL team_id hashes."""
        actions = pd.DataFrame(
            {
                "team_id_native": pd.array(
                    [_SKILLCORNER_HOME, _SKILLCORNER_AWAY, _SKILLCORNER_HOME],
                    dtype="string",
                ),
                "type_id": [0, 1, 11],
            }
        )
        result = _apply_team_id_hash(actions, "1234567")
        assert result["team_id"].notna().all()


class TestSentinelFill:
    """NULL team_id_native rows get sentinel hash instead of crash."""

    def test_team_id_null_native_fills_sentinel(self, caplog: pytest.LogCaptureFixture) -> None:
        """NULL team_id_native rows get deterministic sentinel hash with warning."""
        actions = pd.DataFrame(
            {
                "team_id_native": pd.array(
                    [_IDSSE_HOME, pd.NA, _IDSSE_AWAY, pd.NA],
                    dtype="string",
                ),
                "type_id": [0, 4, 0, 4],  # type_id=4 = freekick_short
            }
        )
        with caplog.at_level(logging.WARNING):
            result = _apply_team_id_hash(actions, "J03WMX")

        # No NULL team_id in output
        assert result["team_id"].notna().all()

        # Sentinel hash is deterministic
        sentinel_hash = hash_native_id_to_bigint(UNKNOWN_TEAM_SENTINEL)
        sentinel_rows = result.loc[result["team_id"] == sentinel_hash]
        assert len(sentinel_rows) == 2

        # Sentinel differs from both real team hashes
        home_hash = hash_native_id_to_bigint(_IDSSE_HOME)
        away_hash = hash_native_id_to_bigint(_IDSSE_AWAY)
        assert sentinel_hash != home_hash
        assert sentinel_hash != away_hash

        # Warning logged with match context
        assert "NULL team_id_native in 2 rows" in caplog.text
        assert "J03WMX" in caplog.text
        assert "4" in caplog.text  # type_id in warning


class TestHashProperties:
    """Hash function produces correct equality semantics for VAEP."""

    def test_two_teams_produce_distinct_hashes(self) -> None:
        """Different team_id_native values produce different team_id hashes."""
        pairs = [
            (_IDSSE_HOME, _IDSSE_AWAY),
            (_METRICA_HOME, _METRICA_AWAY),
            (_SKILLCORNER_HOME, _SKILLCORNER_AWAY),
        ]
        for native_a, native_b in pairs:
            assert hash_native_id_to_bigint(native_a) != hash_native_id_to_bigint(native_b), (
                f"collision: {native_a} == {native_b}"
            )

    def test_hash_is_deterministic(self) -> None:
        """Same team_id_native always produces the same team_id hash."""
        for native in [_IDSSE_HOME, _METRICA_AWAY, _SKILLCORNER_HOME, UNKNOWN_TEAM_SENTINEL]:
            h1 = hash_native_id_to_bigint(native)
            h2 = hash_native_id_to_bigint(native)
            assert h1 == h2, f"non-deterministic hash for {native}"

    def test_hash_is_positive_bigint(self) -> None:
        """Hash values are positive integers that fit in a BIGINT column."""
        for native in [_IDSSE_HOME, _METRICA_AWAY, _SKILLCORNER_HOME, UNKNOWN_TEAM_SENTINEL]:
            h = hash_native_id_to_bigint(native)
            assert isinstance(h, int)
            assert h > 0
            assert h < 2**63  # fits in signed BIGINT
```

- [ ] **Step 3: Run tests**

Run: `uv run pytest src/tests/test_spadl_team_resolution.py -v`
Expected: All 8 tests PASS.

- [ ] **Step 4: Commit**

```bash
git add src/ingestion/spadl_adapter.py src/tests/test_spadl_team_resolution.py
git commit -m "test: add sentinel constant + unit tests for team_id hash resolution"
```

---

### Task 2: SPADL Conversion Fix — IDSSE

**Files:**
- Modify: `src/ingestion/spadl_conversion.py:960-970`

- [ ] **Step 1: Replace IDSSE NULL-fill with hash + sentinel guard**

In `src/ingestion/spadl_conversion.py`, the block at lines 962-969 currently reads:

```python
        match_id_hashed = _hash_id(match_id_str)
        actions["match_id"] = match_id_hashed
        actions["game_id"] = match_id_hashed
        n = len(actions)
        actions["team_id"] = _pd.array([_pd.NA] * n, dtype="Int64")
        actions["player_id"] = _pd.array([_pd.NA] * n, dtype="Int64")
        actions["competition_id"] = _pd.array([_pd.NA] * n, dtype="Int64")
        actions["season_id"] = _pd.array([_pd.NA] * n, dtype="Int64")
```

Replace with:

```python
        match_id_hashed = _hash_id(match_id_str)
        actions["match_id"] = match_id_hashed
        actions["game_id"] = match_id_hashed
        n = len(actions)
        # team_id: hash from team_id_native (populated at line 938).
        # Edge case: silly-kicks emits non-"home"/"away" labels for some
        # freekick_short events → NULL team_id_native. Fill with sentinel.
        null_team_mask = actions["team_id_native"].isna()
        if null_team_mask.any():
            logger.warning(
                "NULL team_id_native in %d rows for match_id=%s (type_ids=%s). "
                "Filling with sentinel hash.",
                null_team_mask.sum(),
                match_id_str,
                actions.loc[null_team_mask, "type_id"].unique().tolist(),
            )
            actions.loc[null_team_mask, "team_id_native"] = _SENTINEL
        actions["team_id"] = actions["team_id_native"].map(_hash_id).astype("Int64")
        actions["player_id"] = _pd.array([_pd.NA] * n, dtype="Int64")
        actions["competition_id"] = _pd.array([_pd.NA] * n, dtype="Int64")
        actions["season_id"] = _pd.array([_pd.NA] * n, dtype="Int64")
```

**Import placement**: Combine the sentinel import with the existing `_hash_id` import at line 811. The existing import reads:

```python
        from ingestion.spadl_adapter import (
            hash_native_id_to_bigint as _hash_id,
        )
```

Change to:

```python
        from ingestion.spadl_adapter import (
            UNKNOWN_TEAM_SENTINEL as _SENTINEL,
            hash_native_id_to_bigint as _hash_id,
        )
```

This keeps both imports from the same module together at the same nesting depth within the UDF closure.

- [ ] **Step 2: Run existing tests**

Run: `uv run pytest src/tests/test_spadl_vaep.py src/tests/test_spadl_team_resolution.py -v`
Expected: All PASS.

- [ ] **Step 3: Commit**

```bash
git add src/ingestion/spadl_conversion.py
git commit -m "fix(spadl): resolve IDSSE team_id NULL → hash(team_id_native)"
```

---

### Task 3: SPADL Conversion Fix — Metrica + SkillCorner

**Files:**
- Modify: `src/ingestion/spadl_conversion.py:1305-1314` (Metrica)
- Modify: `src/ingestion/spadl_conversion.py:1591-1600` (SkillCorner)

- [ ] **Step 1: Replace Metrica NULL-fill with hash + sentinel guard**

In `src/ingestion/spadl_conversion.py`, the Metrica block at lines 1307-1314 currently reads:

```python
        match_id_hashed = _hash_id(match_id_str)
        actions["match_id"] = match_id_hashed
        actions["game_id"] = match_id_hashed
        n = len(actions)
        actions["team_id"] = _pd.array([_pd.NA] * n, dtype="Int64")
        actions["player_id"] = _pd.array([_pd.NA] * n, dtype="Int64")
        actions["competition_id"] = _pd.array([_pd.NA] * n, dtype="Int64")
        actions["season_id"] = _pd.array([_pd.NA] * n, dtype="Int64")
```

Replace with:

```python
        match_id_hashed = _hash_id(match_id_str)
        actions["match_id"] = match_id_hashed
        actions["game_id"] = match_id_hashed
        n = len(actions)
        # team_id: hash from team_id_native (populated at line 1292).
        null_team_mask = actions["team_id_native"].isna()
        if null_team_mask.any():
            logger.warning(
                "NULL team_id_native in %d rows for match_id=%s (type_ids=%s). "
                "Filling with sentinel hash.",
                null_team_mask.sum(),
                match_id_str,
                actions.loc[null_team_mask, "type_id"].unique().tolist(),
            )
            actions.loc[null_team_mask, "team_id_native"] = _SENTINEL
        actions["team_id"] = actions["team_id_native"].map(_hash_id).astype("Int64")
        actions["player_id"] = _pd.array([_pd.NA] * n, dtype="Int64")
        actions["competition_id"] = _pd.array([_pd.NA] * n, dtype="Int64")
        actions["season_id"] = _pd.array([_pd.NA] * n, dtype="Int64")
```

**Import placement (Metrica)**: Same as IDSSE — combine with the existing `_hash_id` import at line 1193:

```python
        from ingestion.spadl_adapter import (
            UNKNOWN_TEAM_SENTINEL as _SENTINEL,
            hash_native_id_to_bigint as _hash_id,
        )
```

- [ ] **Step 2: Replace SkillCorner NULL-fill with hash + sentinel guard**

In `src/ingestion/spadl_conversion.py`, the SkillCorner block at lines 1592-1600 currently reads:

```python
        match_id_hashed = _hash_id(match_id_str)
        actions["match_id"] = match_id_hashed
        actions["game_id"] = match_id_hashed
        n = len(actions)
        actions["team_id"] = _pd.array([_pd.NA] * n, dtype="Int64")
        actions["player_id"] = _pd.array([_pd.NA] * n, dtype="Int64")
        actions["competition_id"] = _pd.array([_pd.NA] * n, dtype="Int64")
        actions["season_id"] = _pd.array([_pd.NA] * n, dtype="Int64")
```

Replace with:

```python
        match_id_hashed = _hash_id(match_id_str)
        actions["match_id"] = match_id_hashed
        actions["game_id"] = match_id_hashed
        n = len(actions)
        # team_id: hash from team_id_native (populated at line 1574-1578).
        null_team_mask = actions["team_id_native"].isna()
        if null_team_mask.any():
            _udf_logger = logging.getLogger(__name__)
            _udf_logger.warning(
                "NULL team_id_native in %d rows for match_id=%s (type_ids=%s). "
                "Filling with sentinel hash.",
                null_team_mask.sum(),
                match_id_str,
                actions.loc[null_team_mask, "type_id"].unique().tolist(),
            )
            actions.loc[null_team_mask, "team_id_native"] = _SENTINEL
        actions["team_id"] = actions["team_id_native"].map(_hash_id).astype("Int64")
        actions["player_id"] = _pd.array([_pd.NA] * n, dtype="Int64")
        actions["competition_id"] = _pd.array([_pd.NA] * n, dtype="Int64")
        actions["season_id"] = _pd.array([_pd.NA] * n, dtype="Int64")
```

**Import placement (SkillCorner)**: Same pattern — combine with the existing `_hash_id` import at line 1495:

```python
        from ingestion.spadl_adapter import (
            UNKNOWN_TEAM_SENTINEL as _SENTINEL,
            hash_native_id_to_bigint as _hash_id,
        )
```

Note: The SkillCorner UDF uses a local `_udf_logger` (not the module-level `logger`) because it's inside a deeply nested closure. This matches the existing pattern at line 1561.

- [ ] **Step 3: Run tests**

Run: `uv run pytest src/tests/test_spadl_vaep.py src/tests/test_spadl_team_resolution.py -v`
Expected: All PASS.

- [ ] **Step 4: Commit**

```bash
git add src/ingestion/spadl_conversion.py
git commit -m "fix(spadl): resolve Metrica + SkillCorner team_id NULL → hash(team_id_native)"
```

---

### Task 4: VAEP Scoring Guard (defense-in-depth)

**Files:**
- Modify: `src/ingestion/spadl_vaep.py:589-592`

**Executor note (TDD ordering):** Task 5's `test_vaep_raises_on_null_team_id` tests this guard. In strict TDD you'd write the test first. Since we squash all commits, the ordering is execution-convenience (guard first → test compiles). If you prefer TDD order, write Task 5 first, watch it fail, then implement this task.

- [ ] **Step 1: Add NULL team_id guard before VAEP formula call**

In `src/ingestion/spadl_vaep.py`, the per-game loop at line 589-592 currently reads:

```python
        for game_id in game_ids:
            game_actions = _game_groups.get(game_id, _pd.DataFrame()).reset_index(drop=True)
            if len(game_actions) < 2:
                continue
```

Replace with:

```python
        for game_id in game_ids:
            game_actions = _game_groups.get(game_id, _pd.DataFrame()).reset_index(drop=True)
            if len(game_actions) < 2:
                continue
            # Defense-in-depth: NULL team_id produces NaN VAEP silently.
            # Fail loud if SPADL conversion didn't resolve team_id.
            _null_team_count = game_actions["team_id"].isna().sum()
            if _null_team_count > 0:
                raise RuntimeError(
                    f"VAEP scoring received {_null_team_count} NULL team_id rows "
                    f"for game_id={game_id}. "
                    f"SPADL conversion must resolve team_id before scoring."
                )
```

- [ ] **Step 2: Run tests**

Run: `uv run pytest src/tests/test_spadl_vaep.py -v`
Expected: All PASS.

- [ ] **Step 3: Commit**

```bash
git add src/ingestion/spadl_vaep.py
git commit -m "fix(vaep): add defense-in-depth guard for NULL team_id in scoring UDF"
```

---

### Task 5: Integration Test — VAEP Non-NULL on Tracking Provider Fixture

**Files:**
- Create: `src/tests/test_spadl_vaep_tracking_providers.py`

- [ ] **Step 1: Write the integration test file**

```python
"""Integration test: VAEP scoring produces non-NULL values for tracking-provider
SPADL actions with correctly resolved team_id.

Validates the full pipeline: team_id hash → feature extraction (fs.team) →
VAEP formula (sameteam comparison) → non-NULL offensive/defensive values.

Uses a test-trained XGBoost model on fixture feature dimensionality (not the
production Champion) for test isolation — no MLflow/UC dependency.
"""

from __future__ import annotations

import numpy as np
import pandas as pd
import pytest
from xgboost import XGBClassifier

from ingestion.spadl_adapter import hash_native_id_to_bigint


# ---------------------------------------------------------------------------
# Fixture: realistic 2-team SPADL actions
# ---------------------------------------------------------------------------

_TEAM_A_NATIVE = "DFL-CLU-000008"
_TEAM_B_NATIVE = "DFL-CLU-00000G"
_TEAM_A_ID = hash_native_id_to_bigint(_TEAM_A_NATIVE)
_TEAM_B_ID = hash_native_id_to_bigint(_TEAM_B_NATIVE)
_MATCH_ID = hash_native_id_to_bigint("J03WMX")


def _build_fixture(n_actions: int = 100) -> pd.DataFrame:
    """Build a realistic SPADL fixture with 2 teams, alternating possession.

    Action type distribution mirrors real matches:
    - pass (0): 50%
    - dribble (1): 15%
    - tackle (8): 10%
    - interception (9): 10%
    - shot (11): 5%
    - clearance (18): 10%

    Note: For n_actions < 50, only period 1 is populated (period 2 has 0
    actions). The function handles this correctly but the fixture will not
    exercise cross-period boundary behavior.
    """
    rng = np.random.default_rng(42)

    type_ids = rng.choice(
        [0, 0, 0, 0, 0, 1, 1, 8, 9, 11, 18],
        size=n_actions,
    )
    # Result: success (1) for 70%, fail (0) for 30%
    result_ids = rng.choice([1, 1, 1, 1, 1, 1, 1, 0, 0, 0], size=n_actions)
    # Bodypart: foot (0) 80%, head (1) 15%, other (2) 5%
    bodypart_ids = rng.choice([0, 0, 0, 0, 0, 0, 0, 0, 1, 2], size=n_actions)

    # Alternating possession: ~5-8 consecutive actions per team
    team_ids = []
    current_team = _TEAM_A_ID
    streak = 0
    for i in range(n_actions):
        team_ids.append(current_team)
        streak += 1
        # Turnover after 5-8 actions (or on tackle/interception)
        if streak >= rng.integers(5, 9) or type_ids[i] in (8, 9):
            current_team = _TEAM_B_ID if current_team == _TEAM_A_ID else _TEAM_A_ID
            streak = 0

    # Time progression: 2 periods, split at midpoint
    split = n_actions // 2
    period_ids = np.array([1] * split + [2] * (n_actions - split))
    time_seconds = np.zeros(n_actions)
    for period in [1, 2]:
        mask = period_ids == period
        count = mask.sum()
        if count > 0:
            time_seconds[mask] = np.sort(rng.uniform(0, 2700, size=count))

    # Coordinates: attacking direction LTR (x: 0-105, y: 0-68)
    start_x = rng.uniform(10, 95, size=n_actions)
    start_y = rng.uniform(5, 63, size=n_actions)
    end_x = start_x + rng.uniform(-5, 10, size=n_actions)
    end_y = start_y + rng.uniform(-5, 5, size=n_actions)
    end_x = np.clip(end_x, 0, 105)
    end_y = np.clip(end_y, 0, 68)

    return pd.DataFrame(
        {
            "game_id": _MATCH_ID,
            "match_id": _MATCH_ID,
            "action_id": range(n_actions),
            "original_event_id": [f"evt_{i}" for i in range(n_actions)],
            "period_id": period_ids,
            "time_seconds": time_seconds,
            "team_id": pd.array(team_ids, dtype="Int64"),
            "player_id": pd.array([pd.NA] * n_actions, dtype="Int64"),
            "start_x": start_x,
            "start_y": start_y,
            "end_x": end_x,
            "end_y": end_y,
            "type_id": type_ids,
            "result_id": result_ids,
            "bodypart_id": bodypart_ids,
            # Columns required by _make_scoring_udf's output projection:
            "competition_id": pd.array([pd.NA] * n_actions, dtype="Int64"),
            "season_id": pd.array([pd.NA] * n_actions, dtype="Int64"),
            "data_source": "idsse",
        }
    )


def _train_test_models(x: pd.DataFrame) -> tuple[bytes, bytes]:
    """Train trivial XGBoost models on the fixture's feature dimensionality.

    Returns raw model bytes (scores, concedes) matching the production
    serialization format: model.get_booster().save_raw("json").
    Production loads via XGBClassifier().load_model(bytearray(...)).
    """
    rng = np.random.default_rng(123)
    n = len(x)
    # Random binary targets — model accuracy doesn't matter, just non-NULL output
    y_scores = (rng.random(n) > 0.5).astype(int)
    y_concedes = (rng.random(n) > 0.7).astype(int)

    m_scores = XGBClassifier(n_estimators=5, max_depth=2, random_state=42)
    m_scores.fit(x, y_scores)

    m_concedes = XGBClassifier(n_estimators=5, max_depth=2, random_state=42)
    m_concedes.fit(x, y_concedes)

    return (
        m_scores.get_booster().save_raw("json"),
        m_concedes.get_booster().save_raw("json"),
    )


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


class TestVaepTrackingProviders:
    """VAEP scoring produces non-NULL values for tracking-provider data."""

    def test_vaep_non_null_for_two_team_fixture(self) -> None:
        """Full pipeline: hash team_id → features → formula → non-NULL VAEP."""
        import silly_kicks.spadl as _spadl
        import silly_kicks.vaep.features as _fs
        import silly_kicks.vaep.formula as _vaepformula

        fixture = _build_fixture(100)
        named = _spadl.add_names(fixture)

        # Extract features (same feature functions as production)
        _feature_fns = [
            _fs.actiontype_onehot,
            _fs.result_onehot,
            _fs.bodypart_onehot,
            _fs.time,
            _fs.startlocation,
            _fs.endlocation,
            _fs.startpolar,
            _fs.endpolar,
            _fs.movement,
            _fs.team,
            _fs.time_delta,
        ]

        game_actions = named.reset_index(drop=True)
        gamestates = _fs.gamestates(game_actions, nb_prev_actions=3)
        x = pd.concat([fn(gamestates) for fn in _feature_fns], axis=1)

        # Train test models on this exact feature shape
        scores_raw, concedes_raw = _train_test_models(x)

        # Score with test models (same loading pattern as production: XGBClassifier)
        m_scores = XGBClassifier()
        m_scores.load_model(bytearray(scores_raw))
        m_concedes = XGBClassifier()
        m_concedes.load_model(bytearray(concedes_raw))

        p_scores = pd.Series(m_scores.predict_proba(x)[:, 1])
        p_concedes = pd.Series(m_concedes.predict_proba(x)[:, 1])
        values = _vaepformula.value(game_actions, p_scores, p_concedes)

        # Assert: VAEP values are non-NULL.
        # Expected NaN: last action per game (boundary effect from _prev shift).
        # With 1 game and 2 periods, expect at most ~3 NaN boundary rows.
        null_count = values["vaep_value"].isna().sum()
        assert null_count <= 4, (
            f"Too many NULL VAEP values: {null_count}/100. "
            f"Expected ≤4 (period boundaries only)."
        )

        # The team_1/team_2/team_3 features should NOT be all-False
        # (which is what happens with NULL team_id)
        team_cols = [c for c in x.columns if c.startswith("team_")]
        team_features = x[team_cols]
        assert team_features.any(axis=None), (
            "All team features are False — team_id equality comparison is broken"
        )

    def test_vaep_raises_on_null_team_id(self) -> None:
        """VAEP scoring guard raises RuntimeError on NULL team_id.

        Tests the defense-in-depth guard added in spadl_vaep.py. Calls
        _make_scoring_udf directly (signature: scores_raw, concedes_raw →
        callable). Verified at spadl_vaep.py:460.
        """
        import silly_kicks.spadl as _spadl
        import silly_kicks.vaep.features as _fs

        fixture = _build_fixture(20)

        # First, get valid features to train a model with correct shape
        named_valid = _spadl.add_names(fixture)
        game_actions_valid = named_valid.reset_index(drop=True)
        _feature_fns = [
            _fs.actiontype_onehot,
            _fs.result_onehot,
            _fs.bodypart_onehot,
            _fs.time,
            _fs.startlocation,
            _fs.endlocation,
            _fs.startpolar,
            _fs.endpolar,
            _fs.movement,
            _fs.team,
            _fs.time_delta,
        ]
        gamestates = _fs.gamestates(game_actions_valid, nb_prev_actions=3)
        x = pd.concat([fn(gamestates) for fn in _feature_fns], axis=1)
        scores_raw, concedes_raw = _train_test_models(x)

        # Now corrupt team_id to NULL — simulating the pre-fix state
        fixture["team_id"] = pd.array([pd.NA] * len(fixture), dtype="Int64")

        # Call the actual production scoring UDF factory
        from ingestion.spadl_vaep import _make_scoring_udf

        scoring_udf = _make_scoring_udf(scores_raw, concedes_raw)

        with pytest.raises(RuntimeError, match="NULL team_id rows"):
            scoring_udf(fixture)
```

- [ ] **Step 2: Run integration tests**

Run: `uv run pytest src/tests/test_spadl_vaep_tracking_providers.py -v`
Expected:
- `test_vaep_non_null_for_two_team_fixture` — PASS
- `test_vaep_raises_on_null_team_id` — PASS (requires Task 4's guard)

- [ ] **Step 3: Commit**

```bash
git add src/tests/test_spadl_vaep_tracking_providers.py
git commit -m "test: add VAEP integration test for tracking-provider team_id resolution"
```

---

### Task 6: Structural Regression Guard

**Files:**
- Modify: `src/tests/test_spadl_vaep.py`

This is a structural assertion that verifies the NULL-fill pattern (`_pd.array([_pd.NA] * n, dtype="Int64")` assigned to `team_id`) does NOT appear in the tracking-provider UDF code. If someone reverts the fix, this test catches it — unlike a unit test of the hash function, which would pass regardless of production code state.

- [ ] **Step 1: Add imports to top of file, then append test class**

First, add `import re` and `from pathlib import Path` to the top-of-file import block in `src/tests/test_spadl_vaep.py` (after the existing `from unittest.mock import MagicMock, patch` line). This satisfies ruff's isort rule.

Then append the test class at the bottom of the file:

```python
# ---------------------------------------------------------------------------
# Structural regression guard: team_id NULL-fill must not exist in
# tracking-provider UDFs. Catches reverts of the hash-resolution fix.
# ---------------------------------------------------------------------------

_SRC_ROOT = Path(__file__).resolve().parent.parent  # src/
_SPADL_CONVERSION_PATH = _SRC_ROOT / "ingestion" / "spadl_conversion.py"


class TestTeamIdNullFillAbsent:
    """Structural guard: tracking-provider UDFs must NOT NULL-fill team_id."""

    def test_no_team_id_null_fill_in_tracking_udfs(self) -> None:
        """The pattern `actions["team_id"] = _pd.array([_pd.NA] * n` must not
        appear after the IDSSE/Metrica/SkillCorner team_id_native population.

        This is a structural assertion — it reads the source file and checks
        that the old NULL-fill pattern was replaced with the hash pattern.
        If someone reverts the fix, this test fails immediately.
        """
        source = _SPADL_CONVERSION_PATH.read_text(encoding="utf-8")

        # The old pattern: actions["team_id"] = _pd.array([_pd.NA] * n, dtype="Int64")
        null_fill_pattern = re.compile(
            r'actions\["team_id"\]\s*=\s*_pd\.array\(\[_pd\.NA\]'
        )

        matches = null_fill_pattern.findall(source)
        assert len(matches) == 0, (
            f"Found {len(matches)} occurrence(s) of team_id NULL-fill pattern in "
            f"spadl_conversion.py. The tracking-provider UDFs must use "
            f"hash(team_id_native) instead. Locations:\n"
            + "\n".join(
                f"  line {i + 1}: {line.strip()}"
                for i, line in enumerate(source.splitlines())
                if null_fill_pattern.search(line)
            )
        )

    def test_hash_pattern_present_in_tracking_udfs(self) -> None:
        """Positive check: the hash assignment pattern must appear 3 times
        (once per tracking-provider UDF: IDSSE, Metrica, SkillCorner)."""
        source = _SPADL_CONVERSION_PATH.read_text(encoding="utf-8")

        # The fix pattern: actions["team_id"] = actions["team_id_native"].map(_hash_id)
        hash_pattern = re.compile(
            r'actions\["team_id"\]\s*=\s*actions\["team_id_native"\]\.map\(_hash_id\)'
        )

        matches = hash_pattern.findall(source)
        assert len(matches) == 3, (
            f"Expected 3 occurrences of team_id hash pattern — one per tracking-"
            f"provider UDF (IDSSE, Metrica, SkillCorner). Found {len(matches)}. "
            f"Update this count when adding/removing tracking-provider UDFs or "
            f"extracting the hash logic to a shared helper."
        )
```

- [ ] **Step 2: Run the regression test**

Run: `uv run pytest src/tests/test_spadl_vaep.py::TestTeamIdNullFillAbsent -v`
Expected: Both tests PASS (after Tasks 2-3 have been applied).

- [ ] **Step 3: Commit**

```bash
git add src/tests/test_spadl_vaep.py
git commit -m "test: add structural regression guard for team_id hash resolution"
```

---

### Task 7: Full Test Suite Verification

**Files:** None (verification only)

- [ ] **Step 1: Run full test suite**

Run: `uv run pytest src/tests/ -v --tb=short`
Expected: All tests PASS, including:
- `test_spadl_team_resolution.py` (8 tests)
- `test_spadl_vaep_tracking_providers.py` (2 tests)
- `test_spadl_vaep.py::TestTeamIdNullFillAbsent` (2 tests)
- All pre-existing tests unchanged

- [ ] **Step 2: Run ruff + pyright**

Run: `uv run ruff check src/ingestion/spadl_conversion.py src/ingestion/spadl_vaep.py src/ingestion/spadl_adapter.py src/tests/test_spadl_team_resolution.py src/tests/test_spadl_vaep_tracking_providers.py src/tests/test_spadl_vaep.py`
Expected: No violations.

Run: `uv run pyright src/ingestion/spadl_conversion.py src/ingestion/spadl_vaep.py src/ingestion/spadl_adapter.py`
Expected: No errors.

- [ ] **Step 3: Squash commits**

Squash all commits on the branch into one:

```bash
git rebase -i main
# Squash all into first commit with message:
# fix(spadl): resolve tracking-provider team_id NULL → hash(team_id_native)
#
# IDSSE, Metrica, and SkillCorner SPADL converters set team_id = NULL,
# breaking VAEP feature extraction (fs.team) and the value formula
# (sameteam comparison). Fix: hash team_id_native via the same
# SHA-256[:15] pure function used for match_id on these providers.
#
# - Add UNKNOWN_TEAM_SENTINEL constant in spadl_adapter.py (DRY)
# - Replace NULL-fill with hash in 3 UDFs
# - Handle NULL team_id_native edge-case rows (freekick_short) with
#   deterministic sentinel hash + structured warning
# - Add defense-in-depth guard in VAEP scoring UDF (loud fail on NULL)
# - Add unit tests, integration test, and structural regression guard
#
# Closes: 20 matches with 23,686 NaN VAEP values (90% of actions unscored)
```

---

### Task 8: Backfill (post-merge, operator action)

**Files:** None (Databricks SQL + job trigger)

- [ ] **Step 1: Wipe tracking-provider data**

Execute via Databricks SQL statement execution (after code is deployed):

```sql
-- Step 1: Collect match_ids while spadl_actions still has the rows
CREATE OR REPLACE TEMPORARY VIEW _tracking_match_ids AS
SELECT DISTINCT match_id FROM soccer_analytics.bronze.spadl_actions
WHERE data_source IN ('idsse', 'metrica', 'skillcorner');

-- Step 2: Delete VAEP values for those matches
DELETE FROM soccer_analytics.bronze.vaep_action_values
WHERE match_id IN (SELECT match_id FROM _tracking_match_ids);

-- Step 3: Delete SPADL actions
DELETE FROM soccer_analytics.bronze.spadl_actions
WHERE data_source IN ('idsse', 'metrica', 'skillcorner');
```

- [ ] **Step 2: Trigger selective re-run**

```bash
databricks jobs run-now --json '{"job_id": 302697362345215, "only": ["preflight_spadl_vaep", "compute_spadl_vaep"]}' --no-wait
```

- [ ] **Step 3: Validate results**

After job completes, verify:

```sql
-- No NULL team_id in tracking-provider actions
SELECT data_source, COUNT(*) AS null_team_id_count
FROM soccer_analytics.bronze.spadl_actions
WHERE data_source IN ('idsse', 'metrica', 'skillcorner')
  AND team_id IS NULL
GROUP BY data_source;
-- Expected: 0 rows

-- No NULL VAEP for tracking-provider matches
SELECT data_source, COUNT(*) AS null_vaep_count
FROM soccer_analytics.bronze.vaep_action_values v
JOIN (
    SELECT DISTINCT match_id, data_source
    FROM soccer_analytics.bronze.spadl_actions
    WHERE data_source IN ('idsse', 'metrica', 'skillcorner')
) sa ON v.match_id = sa.match_id
WHERE v.vaep_value IS NULL
GROUP BY data_source;
-- Expected: 0 rows (or small count from period-boundary NaN — ≤4 per match)
```
