# Canonical-SPADL Pre-Shot xG Unification — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Deliver a calibrated pre-shot xG on every shot for all providers, computed in canonical SPADL 105×68, keyed `(match_key, action_id)`, via a retrained SPADL-native set encoder (`xg_model_v3`) trained on the tracking cohorts' freeze frames, landed in a new `fct_shot_xg` mart — unblocking silly-kicks' xT-GK v2 SP1.

**Architecture:** Retrain the Deep-Sets set encoder in canonical SPADL space, including the 1,588 GS/SkillCorner full-22 freeze frames in training (GroupKFold-by-match holdout) + a set-cardinality feature, so tracking scoring is in-distribution rather than OOD (fixes review-B2 at the source). Score each tracking provider in **two modes** (context-aware freeze-frame vs tabular-only zero-context); a per-provider OOS discrimination gate — relative to StatsBomb — ships the winner, with tabular-only as the robust floor. Land all providers in `fct_shot_xg` (ADR-013 writer → bronze → staging → contract-enforced mart); retire `fct_xg_predictions_v2` to a back-compat view.

**Tech Stack:** Python 3.10, PySpark (Databricks serverless), pure-NumPy set-encoder inference (`analytics.set_encoder`), PyTorch training on HF Jobs (`scripts/train_xg_*_hf.py`), dbt (contract-enforced marts), MLflow UC registry + UC Volume (ADR-012 delivery), silly-kicks `link_actions_to_frames` / `sk_frame_adapters`.

**Design spec (WHY):** [`docs/superpowers/specs/2026-07-05-canonical-spadl-preshot-xg-unification-design.md`](../specs/2026-07-05-canonical-spadl-preshot-xg-unification-design.md) (v3). Section tags below (§N) reference it.

**ADRs in force:** ADR-013 (writer→bronze→staging→mart), ADR-012 (training→prod delivery), ADR-016 (SPADL enrichments), ADR-018 (cross-table join contracts), ADR-035 (frame orientation), ADR-043 (strand-safe re-derive), ADR-046 (serverless env pins), ADR-064 (access tier), AI-governance (CLAUDE.md). **A new ADR is authored in Task 0.1.**

---

## Conventions (apply to every task)

- **Test-first.** Each task lists its failing test FIRST; write it, watch it fail, implement, watch it pass. No implementation lands without its test.
- **Local gate before "done":** `uv run ruff check src/ scripts/` · `uv run ruff format --check src/ scripts/` · `uv run pyright src` · the task's `pytest`. dbt models: `uv run --extra dbt dbt build --select <model> --project-dir dbt_project --profiles-dir dbt_project` + contract test (parse-only in PR CI — assert SQL invariants in python-ci; build runs in the daily live job).
- **Commit boundaries:** one logical task per commit; **NO commits/PRs without explicit user approval** (CLAUDE.md). Bronze migrations applied with the merge via `scripts/migrations/_runner.py`.
- **Subagent model routing:** reading/search → `haiku`; exploration → `sonnet`; implementation → `opus`.
- **Done-when** per task = test green + local gate clean + the task's acceptance bullet satisfied.
- **silly-kicks version sentinels:** no code here bumps silly-kicks, but if a rebase pulls one, run the full suite (4 sentinels move in lockstep).

---

## Pre-flight (verify live before/early — do NOT assume)

> **Run order (m1):** P-3 (goal label) resolves the `<goal_label>` column that P-1 (population) and the calibration tasks use — **run P-3 before P-1.** P-7 (feasibility) should run before committing to Task 0.6 (the expensive retrain).

- [ ] **P-0 (m1, VERIFIED — record only):** `train_xg_v2_hf.py:256` appends an empty `(0,4)` array for freeze-frame-less shots and `:321` handles size-0 sets → the zero-context/tabular-only path is *processed* in training. **This verifies the code path, NOT the AUC (m3):** the tabular-only ~0.82-AUC robustness claim (R1) is only *measured* at Task 0.6 Step 5 / Task 1.3. Do not treat 0.82 as given — if the measured tabular-only OOS AUC is low, the "robust floor" narrative weakens and P-7 matters more.
- [ ] **P-1 / V-1 (shot population):** run a live `SELECT data_source, action_type, count(*), avg(CAST(<goal_label> AS INT))` over `fct_action_values` for `action_type IN ('shot','shot_penalty','shot_freekick')`, per provider (the `<goal_label>` column itself is resolved by P-3). Report counts + goal-rate per subtype vs the current `xg-shot-data` HF dataset population. **Decision output:** the exact shot-family filter for training + scoring, and the penalty-handling choice (§4.1.1). Gate for Task 0.6 (trainer) and Task 1.2 (scorer).
- [ ] **P-2 / V-2 (frame linkage):** for one GS + one SC match, confirm `action_type='shot'` rows resolve a linked frame with a non-empty player set, and that `is_goalkeeper` + `team_id` resolve on GS frames (GS `player_id` STRING/INT caveat; silly-kicks 4.27.0 `add_gradientsports_player_ids`). Report coverage vs ~100% of 1,588. Gate for Task 1.2.
- [ ] **P-3 / V-3 (goal label):** confirm the goal-label column + values for tracking shots (the SPADL shot `action_result`/`is_goal`), per-cohort goal rate. Gate for the calibration/gate tasks.
- [ ] **P-4 / V-4 (counts):** exact live shot counts per cohort (expect ≈1,363 GS / ≈225 SC).
- [ ] **P-5 / V-5 (consumers):** enumerate runtime consumers of `fct_xg_predictions_v2` (grep + live) — Taipy shot-map (`hf_taipy_app/src/queries/shots.py`, `state/shot_map.py`), HF `xg-shots` publisher, `refresh_synced_tables.py`, `create_indexes.py`. Flag any latency-sensitive one (informs Task 6.x view-vs-materialized). Gate for Task 6.4.
- [ ] **P-6 / V-6 (set-distribution + COMPOSITION diagnostic, M3):** measure not only set-**cardinality** but set-**composition** of SB-360 training freeze frames vs C1 tracking sets: spatial coverage, **actor in/out** (does the shooter appear in the set?), teammate/opponent balance, near-side vs far-side. `set_cardinality` (R3) disentangles *count* but not *composition* — two sets with equal cardinality but different composition still shift the summed context. **Pin the tracking snapshot convention to match SB-360** (Task 0.4): if StatsBomb freeze frames include the actor as a teammate, `build_tracking_snapshots` must too (and vice-versa). If composition diverges materially, `set_cardinality` alone won't rescue discrimination — the two-mode gate is the backstop, but know it *before* the retrain.
- [ ] **P-7 (feasibility pre-check, M2 — before the expensive retrain):** fit a cheap geometry-only logistic (v1-style: SPADL `distance_to_goal` + `shot_angle` only) GroupKFold-OOS on GS/SC; report AUC (with bootstrap CI). **Reference (N3):** the v3 relative floor (`sb_auc − margin`) does **not** exist yet (v3 `sb_auc` is produced at Task 0.6 Step 5) — so P-7 compares against the **current v2 StatsBomb geometry-only OOS AUC** (already known) *or* an **absolute ~0.72** threshold, explicitly as a *rough* feasibility signal, not the v3 gate. If geometry-only comfortably clears that, the **tabular-only floor is likely safe and the retrain is derisked**; if hopeless on tracking, you learn it in minutes (not after an HF-Jobs run) — the honest early signal for the `ood_flag ⇒ silly-kicks drops cohort` conversation. Minutes of local compute; no HF Jobs.
- [ ] **P-8 (committed-fixture policy — two rules):** (a) **GS** — the existing real `gradientsports/10517_p3` slice is a **non-issue** (license-clarity ambiguity, user 2026-07-06); no remediation; new GS fixtures may be real but synthetic preferred. (b) **SkillCorner Real Madrid (Soccermatics Pro)** — **HARD NO-COMMIT (user 2026-07-06):** never commit real RM/Soccermatics-Pro slices; the Task 1.5 SC fixture MUST be synthetic. Verify no RM data reaches any committed fixture before Task 1.5 lands.

> Report P-1…P-8 findings to the user before starting Phase 0 implementation (Investigation Discipline — findings before fixes). P-7 in particular is a go/no-go signal for the retrain.

---

## File Structure

**New files:**
- `src/analytics/xg_freeze_frame.py` — C2: pure freeze-frame normalization port (shared train + serve).
- `src/analytics/action_context/tracking_snapshots.py` — C1: `build_tracking_snapshots` (GS/SC per-shot player sets).
- `src/analytics/xg_calibration.py` — C5: per-provider Platt + discrimination gate + n-aware calibration test (pure).
- `src/ingestion/xg_shot_scorer.py` — C6: ADR-013 writer (two-mode scoring + coordinate guard).
- `scripts/train_xg_v3_hf.py` — C4: SPADL-native retrain (PEP 723, HF Jobs).
- `dbt_project/models/staging/xg/stg_xg__shot_predictions.sql` — staging view.
- `dbt_project/models/marts/fct_shot_xg.sql` — C7: contract-enforced gold mart.
- `scripts/migrations/2026-07-05-fct-xg-predictions-v2-backcompat.sql` — repoint/view migration notes (operator-applied).
- `docs/superpowers/adrs/ADR-066-canonical-spadl-preshot-xg-fct-shot-xg.md` — the ADR.
- Tests under `src/tests/` (one per task, listed inline).

**Modified files:**
- `src/analytics/xg_model.py` — `build_features`: add `set_cardinality` numeric feature + SPADL-native geometry (goal `(105,34)`, width `7.32`).
- `dbt_project/models/staging/xg/_xg__sources.yml` — add `xg_shot_predictions` bronze source + `shot_freeze_frames`.
- `dbt_project/models/marts/_marts__models.yml` — `fct_shot_xg` contract; `fct_xg_predictions_v2` becomes a view.
- `dbt_project/models/marts/fct_xg_predictions_v2.sql` — rewrite as back-compat view/table over `fct_shot_xg`.
- `docs/huggingface/model-cards/xg-v2-model-card.md`, `AI_GOVERNANCE.md`, `workflow-cards/wf-xg-v2.yaml` — governance.
- `src/ingestion/refresh_synced_tables.py` + `dbt_project.yml` (`triggered_synced_marts`) — if `fct_shot_xg` is synced.

---

## Phase 0 — Snapshots + SPADL-native retrain (governed) [spec §4]

### Task 0.1: Author the ADR

**Files:**
- Create: `docs/superpowers/adrs/ADR-066-canonical-spadl-preshot-xg-fct-shot-xg.md`

- [ ] **Step 1:** Copy `docs/superpowers/adrs/ADR-TEMPLATE.md`; fill Context/Decision/Consequences from spec §3, §7. Cover: (a) model coordinate contract → SPADL 105×68; (b) `fct_shot_xg` `(match_key, action_id)` replacing `fct_xg_predictions_v2`; (c) `bronze.shot_freeze_frames` reusable feature-layer fact; (d) tracking-in-training + two-mode gate; (e) decoupling (feature mart vs governed prediction).
- [ ] **Step 2:** Add ADR-066 to the ADR index if one exists; reference ADR-013/012/018/035/064.
- [ ] **Step 3: Commit** — `docs: add ADR-066 canonical-SPADL pre-shot xG + fct_shot_xg`.

**Acceptance:** ADR-066 exists, Nygard format, references the superseded `fct_xg_predictions_v2` path.

---

### Task 0.2: C2 — freeze-frame normalization port (pure)

**Files:**
- Create: `src/analytics/xg_freeze_frame.py`
- Test: `src/tests/test_xg_freeze_frame.py`

- [ ] **Step 1: Write the failing test.**

```python
# src/tests/test_xg_freeze_frame.py
import numpy as np
from analytics.xg_freeze_frame import normalize_freeze_frame, PitchDims, SPADL_PITCH

def _players():
    # (x, y, is_keeper, is_teammate) in SPADL meters, home-LTR
    return np.array([
        [105.0, 34.0, 1.0, 0.0],   # opponent keeper on goal line, centre
        [90.0,  20.0, 0.0, 0.0],   # opponent defender
        [95.0,  40.0, 0.0, 1.0],   # teammate
    ], dtype=np.float64)

def test_spadl_normalization_matches_statsbomb_fractional_position():
    # SPADL ÷105,÷68 lands on the same fractional [0,1] point StatsBomb ÷120,÷80 would.
    out = normalize_freeze_frame(_players(), SPADL_PITCH, shooter_attacks_high_x=True)
    assert out.shape == (3, 4)
    # keeper at x=105 -> x_norm 1.0 ; y=34 -> 0.5
    np.testing.assert_allclose(out[0, :2], [1.0, 0.5], atol=1e-9)
    # flags preserved
    np.testing.assert_array_equal(out[:, 2], [1.0, 0.0, 0.0])
    np.testing.assert_array_equal(out[:, 3], [0.0, 0.0, 1.0])

def test_away_shooter_is_point_reflected_to_attack_high_x():
    out = normalize_freeze_frame(_players(), SPADL_PITCH, shooter_attacks_high_x=False)
    # x -> (105-x)/105 ; keeper x=105 -> 0.0 ; y -> (68-y)/68 ; y=34 -> 0.5
    np.testing.assert_allclose(out[0, :2], [0.0, 0.5], atol=1e-9)

def test_empty_set_returns_zero_by_four():
    out = normalize_freeze_frame(np.empty((0, 4)), SPADL_PITCH, shooter_attacks_high_x=True)
    assert out.shape == (0, 4)
```

- [ ] **Step 2: Run to verify it fails.** `uv run pytest src/tests/test_xg_freeze_frame.py -v` → FAIL (module not found).
- [ ] **Step 3: Implement.**

```python
# src/analytics/xg_freeze_frame.py
"""Pure freeze-frame normalization port (shared by xG v3 training-export and scoring).

Normalizes a player set to [0,1] fractional pitch position + role flags, oriented so
the shooter always attacks toward high x. Coordinate-invariant by construction: dividing
by the system's own pitch dims yields identical fractional positions across conventions
(spec §1.1). NEVER rescales to StatsBomb units.
"""
from __future__ import annotations

from dataclasses import dataclass

import numpy as np
import numpy.typing as npt


@dataclass(frozen=True)
class PitchDims:
    length: float
    width: float


SPADL_PITCH = PitchDims(105.0, 68.0)


def normalize_freeze_frame(
    players: npt.NDArray[np.floating],
    pitch: PitchDims,
    *,
    shooter_attacks_high_x: bool,
) -> npt.NDArray[np.floating]:
    """(N,4) [x, y, is_keeper, is_teammate] in metric coords -> (N,4) [x_norm, y_norm, is_keeper, is_teammate].

    x_norm/y_norm in [0,1]; when ``shooter_attacks_high_x`` is False the frame is
    point-reflected (x->L-x, y->W-y) so the shooter attacks high x in every sample.
    """
    if players.shape[0] == 0:
        return np.empty((0, 4), dtype=np.float64)
    xy = players[:, :2].astype(np.float64)
    flags = players[:, 2:4].astype(np.float64)
    if not shooter_attacks_high_x:
        xy = np.column_stack([pitch.length - xy[:, 0], pitch.width - xy[:, 1]])
    norm = np.column_stack([xy[:, 0] / pitch.length, xy[:, 1] / pitch.width])
    return np.column_stack([norm, flags])
```

- [ ] **Step 4: Run to verify it passes.** `uv run pytest src/tests/test_xg_freeze_frame.py -v` → PASS.
- [ ] **Step 5: Commit** — `feat(xg): add coordinate-invariant freeze-frame normalization port (C2)`.

**Acceptance:** SPADL normalization equals StatsBomb fractional position; away-shooter reflection correct; empty set safe.

---

### Task 0.3: `build_features` — set-cardinality feature + SPADL-native geometry [spec §4.1, R3]

**Files:**
- Modify: `src/analytics/xg_model.py` (`_NUMERIC_FEATURES`, `build_features`, add `spadl_shot_geometry`)
- Test: `src/tests/test_xg_model.py` (extend)

- [ ] **Step 1: Write the failing test.**

```python
# src/tests/test_xg_model.py (add)
import pandas as pd
from analytics.xg_model import build_features, XGModelConfig, spadl_shot_geometry

def test_set_cardinality_is_a_feature_column():
    df = pd.DataFrame({"distance_to_goal": [10.0], "shot_angle": [0.5], "set_cardinality": [22], "is_goal": [0]})
    x, _ = build_features(df, XGModelConfig())
    assert "set_cardinality" in x.columns
    assert float(x.iloc[0]["set_cardinality"]) == 22.0

def test_spadl_shot_geometry_uses_105x34_goal_and_732_width():
    # penalty spot ~ (94, 34): distance 11m; subtended angle = 2*atan(3.66/11) ≈ 0.6424 rad
    # (VERIFIED numerically — do NOT assert > 0.9; the true value is ~0.64 and an executor
    #  "fixing" a failing > 0.9 would corrupt the goal-width normalization).
    dist, ang = spadl_shot_geometry(94.0, 34.0)
    assert abs(dist - 11.0) < 0.5
    assert 0.60 < ang < 0.70  # penalty-spot subtended angle ≈ 0.64 rad
    # acute corner shot (105, 0) -> angle ≈ 0 rad
    _, ang_corner = spadl_shot_geometry(105.0, 0.0)
    assert ang_corner < 0.05
    assert ang_corner < ang
```

- [ ] **Step 2: Run to verify it fails.** `uv run pytest src/tests/test_xg_model.py -k "set_cardinality or spadl_shot_geometry" -v` → FAIL.
- [ ] **Step 3: Implement.** In `xg_model.py`: add `"set_cardinality"` to `_NUMERIC_FEATURES`; add:

```python
_SPADL_GOAL_X = 105.0
_SPADL_GOAL_Y = 34.0
_SPADL_GOAL_HALF_WIDTH = 7.32 / 2.0

def spadl_shot_geometry(x: float, y: float) -> tuple[float, float]:
    """Distance-to-goal (m) and subtended shot angle (rad) in canonical SPADL 105x68."""
    import math
    dx = _SPADL_GOAL_X - x
    dist = math.hypot(dx, _SPADL_GOAL_Y - y)
    post_a = math.hypot(dx, (_SPADL_GOAL_Y - _SPADL_GOAL_HALF_WIDTH) - y)
    post_b = math.hypot(dx, (_SPADL_GOAL_Y + _SPADL_GOAL_HALF_WIDTH) - y)
    goal_w = 2 * _SPADL_GOAL_HALF_WIDTH
    cos_ang = (post_a**2 + post_b**2 - goal_w**2) / (2 * post_a * post_b + 1e-12)
    return dist, float(math.acos(max(-1.0, min(1.0, cos_ang))))
```

(`build_features` picks up `set_cardinality` automatically via `_NUMERIC_FEATURES`.)

- [ ] **Step 4: Run to verify it passes.** `uv run pytest src/tests/test_xg_model.py -k "set_cardinality or spadl_shot_geometry" -v` → PASS.
- [ ] **Step 5:** Full `build_features` regression: `uv run pytest src/tests/test_xg_model.py -v` → PASS (existing tests still green; reindex handles the new column).
- [ ] **Step 6: Commit** — `feat(xg): SPADL-native shot geometry + set-cardinality feature (R3)`.

**Acceptance:** `set_cardinality` is a model feature; SPADL geometry uses 105×34 goal + 7.32 m width; existing feature tests unregressed.

---

### Task 0.4: C1 — `build_tracking_snapshots` (GS/SC per-shot player sets) [spec §5.1]

**Files:**
- Create: `src/analytics/action_context/tracking_snapshots.py`
- Test: `src/tests/action_context/test_tracking_snapshots.py`
- Reference: `src/analytics/action_context/sb360_snapshots.py` (pattern), `sk_frame_adapters.py` (`_AC_FRAME_COLUMNS`), `enrich.py:283` (`link_actions_to_frames`).

- [ ] **Step 1: Write the failing test** (fixture-driven; uses a synthetic linked-frame + one shot action).

```python
# src/tests/action_context/test_tracking_snapshots.py
import numpy as np
import pandas as pd
from analytics.action_context.tracking_snapshots import build_tracking_snapshots

def _shot_row(team_id=1):
    return pd.DataFrame([{"action_id": 7, "match_key": 100, "team_id": team_id,
                          "type_name": "shot", "period_id": 1, "data_source": "gradientsports",
                          "team_attacking_direction": "ltr"}])

def _frames():
    # one frame linked to action_id 7: shooter team 1 attacks high-x
    return pd.DataFrame([
        {"action_id": 7, "player_id": "a", "team_id": 1, "is_goalkeeper": False, "x": 95.0, "y": 40.0},
        {"action_id": 7, "player_id": "b", "team_id": 2, "is_goalkeeper": True,  "x": 105.0, "y": 34.0},
        {"action_id": 7, "player_id": "c", "team_id": 2, "is_goalkeeper": False, "x": 90.0, "y": 20.0},
    ])

def test_builds_per_player_rows_with_flags_and_cardinality():
    snaps = build_tracking_snapshots(_shot_row(), _frames())
    row = snaps[snaps.action_id == 7]
    assert len(row) == 3
    kpr = row[row.player_id == "b"].iloc[0]
    assert kpr.is_keeper == 1 and kpr.is_teammate == 0     # opponent GK
    tm = row[row.player_id == "a"].iloc[0]
    assert tm.is_teammate == 1
    # coords in SPADL range
    assert row.x.between(0, 105).all() and row.y.between(0, 68).all()

def test_cardinality_matches_player_count():
    snaps = build_tracking_snapshots(_shot_row(), _frames())
    assert snaps.set_cardinality.iloc[0] == 3
```

- [ ] **Step 2: Run to verify it fails.** `uv run pytest src/tests/action_context/test_tracking_snapshots.py -v` → FAIL.
- [ ] **Step 3: Implement** `build_tracking_snapshots(shot_actions, frames) -> DataFrame[action_id, match_key, data_source, player_id, x, y, is_keeper, is_teammate, set_cardinality]`: for each shot action, take the linked frame's player rows, `is_teammate = (frame.team_id == shot.team_id)`, `is_keeper = is_goalkeeper`, keep SPADL x/y (already home-LTR per ADR-035), attach `set_cardinality = len(players)`. Orientation flag (`shooter_attacks_high_x`) derived from `team_attacking_direction` vs shooter team — the C2 port is applied later at feature-build time (store raw SPADL x/y + the orientation bool here). Follow `sb360_snapshots.py` column/return conventions. Full 22-player set (no visibility filter — §5.1.1). **M3 convention (from P-6):** match the SB-360 **actor-inclusion** convention — if `build_sb360_snapshots` includes the shooter in the set (as a teammate), `build_tracking_snapshots` must too; if it excludes the shooter, exclude here. Add a test asserting the actor-inclusion matches `build_sb360_snapshots` on an equivalent synthetic shot.
- [ ] **Step 4: Run to verify it passes.** → PASS.
- [ ] **Step 5:** Add a Spark integration entry (`build_tracking_snapshots_spark`) that runs per-match via the same linkage the AC pipeline uses (`link_actions_to_frames`), writing `bronze.shot_freeze_frames` (`replaceWhere` per `match_key`, `_ingested_at`). Unit-test the row-shape of the pandas core only; the Spark path is exercised in Task 0.5 + the e2e (Task 4.3).
- [ ] **Step 6: Commit** — `feat(xg): build_tracking_snapshots per-shot player sets for GS/SC (C1)`.

**Acceptance:** per-player rows with correct keeper/teammate flags + cardinality, SPADL coords; mirrors `build_sb360_snapshots` shape.

---

### Task 0.5: `bronze.shot_freeze_frames` writer + population run [spec §5.1, C3]

**Files:**
- Modify: `src/analytics/action_context/tracking_snapshots.py` (Spark writer)
- Migration: `scripts/migrations/2026-07-05-shot-freeze-frames-ddl.sql` (CREATE TABLE IF NOT EXISTS)
- Test: `src/tests/test_shot_freeze_frames_writer.py` (DDL↔writer column parity, mirror `test_bronze_live_schema.py` style)

- [ ] **Step 1: Write the failing test** — module-level `_SHOT_FF_COLUMNS` constant vs the CREATE TABLE DDL column list (ADR-002 §4 schema-drift guard).
- [ ] **Step 2: Run** → FAIL.
- [ ] **Step 3: Implement** the DDL migration + `_SHOT_FF_COLUMNS` constant + lazy `StructType` factory; writer uses `write_delta_table(..., replace_where="match_key IN (...)", row_count=...)`.
- [ ] **Step 4: Run** → PASS.
- [ ] **Step 5 (live, gated on P-2/P-4):** apply migration via `_runner.py`; populate `bronze.shot_freeze_frames` for the 64 GS + 10 SC matches; verify row count ≈ Σ(shots × players) and `SELECT DISTINCT` coverage = 1,588 shots. Report coverage.
- [ ] **Step 6: Commit** — `feat(xg): bronze.shot_freeze_frames writer + DDL (C3)`.

**Acceptance:** DDL↔writer parity test green; live table populated for GS/SC at target coverage (P-2 gap justified if any).

---

### Task 0.6: `train_xg_v3_hf.py` — SPADL-native retrain incl. tracking [spec §4, R2/R3]

**Files:**
- Create: `scripts/train_xg_v3_hf.py` (from `train_xg_v2_hf.py`)
- Test: `src/tests/test_train_xg_v3.py` (pure-logic units: population filter, feature-vector assembly, envelope contract, GroupKFold split)

- [ ] **Step 1: Write failing tests** for the extractable pure functions (do NOT run HF Jobs in unit tests):
  - population filter = the P-1-resolved shot family (`{shot, shot_penalty, shot_freekick}`), penalties handled per P-1 decision;
  - feature assembly calls the shared `build_features` (Task 0.3) + C2 port (Task 0.2) — the same functions the serving scorer calls (the M2 parity seam; the cross-entry-point parity assertion lives in Task 1.2 Step 6);
  - envelope carries `feature_names`, `tabular_dim`, `coordinate_system="spadl_105x68"` (ADR-012 §2);
  - GroupKFold-by-`match_key` produces disjoint fold groups (no same-match leakage) across the mixed SB-360 + tracking + zero-context set.
- [ ] **Step 2: Run** → FAIL.
- [ ] **Step 3: Implement** `train_xg_v3_hf.py`:
  - PEP 723 single-file; **install `luxury-lakehouse[spadl] @ wheel`** so the trainer imports the SAME package `build_features` (Task 0.3) + `normalize_freeze_frame` (Task 0.2) the scorer uses — do **NOT** re-inline copies of the feature-assembly (that would defeat the M2 parity gate by allowing silent drift). Only genuinely trainer-only glue is inlined. Runtime `_REQUIRED_SK_MIN` assert.
  - Training set = SB-360 SPADL freeze frames (via `build_sb360_snapshots`) + **GS/SC full-22** (`build_tracking_snapshots`, GroupKFold holdout) + zero-context (Wyscout/non-360-SB) shots (as v2 already does, P-0). SPADL-native tabular features + `set_cardinality`. **B2 invariant:** `set_cardinality` = the number of players actually encoded into the context vector, so **every zero-context training row has `set_cardinality = 0`** (context zeros ⇔ card 0). This is the exact shape the scorer's tabular-only mode must reproduce (Task 1.2 Step 1). Assert it in a trainer unit test.
  - ADR-012 delivery: `require_mlflow_env()` at top of `main()`; `mlflow.pyfunc.log_model`; `set_and_verify_mlflow_champion(... "xg_model_v3" ...)`; `upload_weights_to_uc_volume(...)`; secrets via `--secrets`.
  - Report **both scoring modes** OOS (context-aware + tabular-only) per provider via GroupKFold: ROC-AUC, Brier, Brier-skill, ECE.
- [ ] **Step 4: Run** → unit tests PASS.
- [ ] **Step 5 (live):** launch on HF Jobs (`hf jobs uv run --secrets ...`); verify new `xg_model_v3@Champion` alias + UC Volume weights + sidecar hash. Record StatsBomb OOS AUC (the §5.3 relative-floor reference), and GS/SC OOS AUC in both modes.
- [ ] **Step 6: Commit** — `feat(xg): train_xg_v3_hf SPADL-native retrain incl. tracking freeze frames (R2/R3)`.

**Acceptance:** `xg_model_v3@Champion` registered + UC Volume + sidecar; envelope records SPADL coord system; OOS metrics (both modes, per provider) reported; StatsBomb discrimination not regressed vs v2 (§4.3).

---

### Task 0.7: Governance — model card + AI_GOVERNANCE + workflow card [spec §4.3]

**Files:**
- Modify: `docs/huggingface/model-cards/xg-v2-model-card.md`, `AI_GOVERNANCE.md` (§5), `workflow-cards/wf-xg-v2.yaml` (`governance:` block, phase/model refs)
- Test: `src/tests/test_ai_governance_md.py` (must stay green)

- [ ] **Step 1:** Run `uv run pytest src/tests/test_ai_governance_md.py -v` → GREEN baseline.
- [ ] **Step 2:** Update the model card: coordinate system → SPADL 105×68; all-provider intended use; **per-provider calibration + two-mode scoring section**; refresh eval-metrics/model-index to the v3 OOS numbers; refresh **Next review** date; **add a one-line note (m4) explaining the workflow-name/model-version decoupling** — the governance card stays `wf-xg-v2` (evolve-in-place, no inventory churn) while the model artifact is `xg_model_v3`, to avoid future "why does wf-xg-v2 govern v3?" confusion. Update `AI_GOVERNANCE.md` §5 scope wording; keep `wf-xg-v2` in the inventory (evolve in place — no add/remove). Update `wf-xg-v2.yaml` `governance:` block + any `dbt_model:` / model reference.
- [ ] **Step 3:** Re-run `uv run pytest src/tests/test_ai_governance_md.py -v` → GREEN.
- [ ] **Step 4: Commit** — `docs(gov): xG v3 SPADL retrain — model card + AI_GOVERNANCE + wf-xg-v2`.

**Acceptance:** governance test green (inventory parity holds); model-index metrics current; 30-day review fresh.

> **CHECKPOINT A (review):** new SPADL-native champion trained (incl. tracking), governed, snapshots populated. Pause for review before building the scorer/mart.

---

## Phase 1 — Two-mode scoring, gate, calibration → `fct_shot_xg` (GS/SC) [spec §5]

### Task 1.1: C5 — per-provider calibration + discrimination gate (pure) [spec §5.3, R1/R4/M1]

**Files:**
- Create: `src/analytics/xg_calibration.py`
- Test: `src/tests/test_xg_calibration.py`

- [ ] **Step 1: Write failing tests:**
  - `fit_platt(xg_raw, is_goal)` returns params; applying it is monotone + maps to [0,1].
  - `groupkfold_auc(xg, y, groups)` — leakage guard: fit-group ∩ measure-group = ∅.
  - `bootstrap_auc_ci(xg, y, *, n_boot=2000, alpha=0.05, seed=0)` — returns `(auc, lo, hi)` via **percentile bootstrap** (resample shots with replacement, recompute AUC, take 2.5/97.5 percentiles). **N1: bootstrap, not hand-rolled DeLong** — trivially correct at n≈225, no variance-formula risk, speed irrelevant at this n. Deterministic via `seed` (numpy `default_rng(seed)`) so the test is reproducible. **Correctness test:** on a separable fixture AUC→1.0 with a tight CI; on a random-label fixture AUC≈0.5 with the CI bracketing 0.5; CI lower < point < upper always. The CI is **wide at small n** (SC n≈225/~25 goals → lo ≈ auc − ~0.11, see N2).
  - `select_scoring_mode(context_ci, tabular_ci, sb_auc, margin, floor)` — **n-aware (M1)**: takes AUC **CI tuples**, not point estimates. Ships `context_aware` iff `context_ci.lo ≥ max(sb_auc − margin, floor)` **and** `context_ci.lo > tabular_ci.lo`; else `tabular_only`. Symmetric statistical rigor with the calibration gate.
  - `is_mode_certified(shipped_ci, sb_auc, margin, floor)` — **(m2)** the floor check applied to the **shipped** mode independently. `select_scoring_mode` may return `tabular_only` as the *less-bad* option even when tabular is *below* the floor — **selecting a mode is not certifying it.** Certification (→ `ood_flag`) is this separate check on whichever mode ships.
  - `calibration_ok_n_aware(sum_xg, sum_goals, n)` — binomial test; True within sampling noise (SC n≈225, Σgoals≈25 does NOT fail for well-calibrated input); False on gross miscalibration.

```python
# src/tests/test_xg_calibration.py (excerpt)
from analytics.xg_calibration import select_scoring_mode, is_mode_certified, calibration_ok_n_aware, AucCi

def test_gate_uses_ci_lower_bound_not_point_estimate():
    # context point AUC 0.80 but wide CI (lo 0.74) at small n; sb 0.82, floor max(0.77,0.65)=0.77
    ctx = AucCi(auc=0.80, lo=0.74, hi=0.86); tab = AucCi(auc=0.78, lo=0.72, hi=0.84)
    # ctx.lo 0.74 < 0.77 -> does NOT clear relative floor on the CI -> tabular
    assert select_scoring_mode(ctx, tab, sb_auc=0.82, margin=0.05, floor=0.65) == "tabular_only"
    # tighter, higher context CI clears it and beats tabular
    ctx2 = AucCi(auc=0.82, lo=0.79, hi=0.85)
    assert select_scoring_mode(ctx2, tab, sb_auc=0.82, margin=0.05, floor=0.65) == "context_aware"

def test_mode_selection_is_not_certification():
    # both modes below floor -> select_scoring_mode still returns the less-bad one...
    weak = AucCi(auc=0.63, lo=0.58, hi=0.68); weaker = AucCi(auc=0.60, lo=0.55, hi=0.65)
    shipped = select_scoring_mode(weak, weaker, sb_auc=0.82, margin=0.05, floor=0.65)
    assert shipped == "tabular_only"
    # ...but it is NOT certified -> ood_flag
    assert is_mode_certified(weak, sb_auc=0.82, margin=0.05, floor=0.65) is False

def test_n_aware_calibration_tolerates_small_n_noise():
    assert calibration_ok_n_aware(sum_xg=25.0, sum_goals=28, n=225) is True
    assert calibration_ok_n_aware(sum_xg=25.0, sum_goals=60, n=225) is False
```

- [ ] **Step 2: Run** → FAIL.
- [ ] **Step 3: Implement** `xg_calibration.py` — Platt (sklearn logistic on one feature); `GroupKFold`; `bootstrap_auc_ci` (percentile bootstrap, seeded — N1); `binomtest` for calibration; `AucCi` dataclass; `select_scoring_mode` (CI-lower-bound gate) + `is_mode_certified` (independent shipped-mode floor check). Platt default; isotonic only if it beats Platt OOS (`choose_calibrator`).
- [ ] **Step 4: Run** → PASS.
- [ ] **Step 5: Commit** — `feat(xg): Platt + n-aware (bootstrap-CI) discrimination gate + independent certification (C5)`.

**Acceptance:** the discrimination gate uses the AUC **CI lower bound** (n-aware, symmetric with calibration); mode-selection and certification are **separate** (a shipped mode below the floor is `ood_flag`); leakage guard enforced.

---

### Task 1.2: C6 — `xg_shot_scorer` writer (two-mode + coordinate guard) [spec §5.3/§5.5]

**Files:**
- Create: `src/ingestion/xg_shot_scorer.py`
- Migration: `scripts/migrations/2026-07-05-xg-shot-predictions-ddl.sql`
- Test: `src/tests/test_xg_shot_scorer.py`

- [ ] **Step 1: Write failing tests:**
  - `_assert_coordinate_system(df, envelope)` raises when x/y outside `[0,105]×[0,68]` (+tol) for a `spadl_105x68` envelope (M3).
  - `_score_group(pdf, weights, mode)` returns `xg_set_encoder` + CI per row; `mode="tabular_only"` uses a zero context vector (no NaN — emits a value, fixing the v2 `continue`); `mode="context_aware"` uses the freeze frame.
  - **B2 mode↔features contract:** a tracking shot scored `mode="tabular_only"` produces **`set_cardinality = 0`** (NOT 22) *and* a zero context vector — so its feature vector matches the shape of a zero-context *training* row. Assert: for the same shot, the tabular-only feature vector equals `build_features` with `set_cardinality=0` + zero context; and it is byte-identical to how a Wyscout (zero-context) row is built. This is the trap the M2 parity test cannot catch — the mode-dependent zeroing lives in the scorer, downstream of `build_features`.

```python
# src/tests/test_xg_shot_scorer.py (B2 excerpt)
def test_tabular_only_mode_zeros_context_and_cardinality():
    feats = assemble_features(shot_row_with_22_player_frame(), mode="tabular_only")
    assert feats["set_cardinality"] == 0            # NOT 22 — must match zero-context training rows
    assert (context_vector_for(feats) == 0).all()   # zero context
```

  - DDL↔writer column parity (`_XG_SHOT_PRED_COLUMNS` incl. `scoring_mode`, `model_version`, `calibration_version`, `ood_flag`).
- [ ] **Step 2: Run** → FAIL.
- [ ] **Step 3: Implement** `ingestion.xg_shot_scorer`: guard (`_XgShotGuard` / `find_new_ids` per `match_key`), load `xg_model_v3` (MLflow @Champion → UC Volume fallback + SEC2 hash), load freeze frames from `bronze.shot_freeze_frames`, assemble features via the **shared** `build_features` (Task 0.3) + C2 port (Task 0.2) — the same functions the trainer calls — coordinate guard, `applyInPandas(groupBy match_key)` scoring in the provider's gated mode, apply provider calibrator, `write_delta_table` (`replaceWhere` per `match_key`). Hard-fail-first UDF (ADR-002 §5). `@workflow("wf-xg-v2", phase="inference")`.
- [ ] **Step 4: Run** → PASS.
- [ ] **Step 5: Commit** — `feat(xg): xg_shot_scorer two-mode ADR-013 writer + coordinate guard (C6/M3)`.
- [ ] **Step 6: M2 train/serve parity test (scoped, m2).** Write `src/tests/test_xg_v3_train_serve_parity.py`: build a fixture of shot rows; assert the **serving** feature vector (from `xg_shot_scorer`'s feature-assembly helper) is **identical** to the **training** feature vector (from `train_xg_v3_hf`'s extracted feature-assembly helper) for the same rows — for the **shared components** (tabular + the C2 seam). The provider-specific builders (`build_sb360_snapshots` vs `build_tracking_snapshots`) differ by design and are NOT cross-builder-compared. Run → PASS. Commit — `test(xg): train/serve feature parity gate (M2)`.

**Acceptance:** tabular-only emits a value (not NaN); coordinate guard raises on wrong-scale input; DDL↔writer parity green; train/serve feature vectors identical on the shared seam.

---

### Task 1.3: Score GS/SC + fit gate/calibration + report [spec §5.3, silly-kicks §4.3]

**Files:**
- Use: `xg_shot_scorer.py`, `xg_calibration.py`
- Output: `bronze.xg_shot_predictions` (GS/SC), a committed calibration report at `docs/reports/2026-07-05-xg-v3-tracking-calibration.md`

- [ ] **Step 1 (live):** for GS + SC, score **both modes** OOS (GroupKFold), compute **`bootstrap_auc_ci`**/Brier-skill/ECE + Σxg-vs-Σgoals; run `select_scoring_mode(context_ci, tabular_ci, sb_auc, ...)` per provider (v3 StatsBomb OOS AUC from Task 0.6 = `sb_auc`) to pick the mode, then **independently** run `is_mode_certified(shipped_ci, ...)` + `calibration_ok_n_aware(...)` on the shipped mode — a shipped mode failing either → `ood_flag=true`. Record shipped `scoring_mode` + certification per provider (selection ≠ certification, m2).
- [ ] **Step 1b (N2 — SkillCorner realism, make explicit):** at SC n≈225/~25 goals the bootstrap AUC CI lower bound sits **~0.11 below** the point AUC, so clearing a relative floor of ~0.77 on the *lower bound* needs a point AUC ≥ ~0.88 — unrealistic for xG. **SC is therefore *expected* to be `ood_flag` at its n, regardless of true model quality — this is anticipated, not a failure.** Consequences to carry: **WC2022 (gradientsports, ~1,363 shots) is the realistic primary cohort; SkillCorner-RM is a "certify if it can, expected not to" bonus.** The report MUST present **point AUC alongside the CI** (and calibration) so the Checkpoint-B user override is possible — do NOT let `ood_flag` collapse the decision to a silent drop. silly-kicks plans SP1 **WC2022-only as the baseline**, SC as a bonus.
- [ ] **Step 2 (live):** fit the shipped calibrator on all labeled shots for that provider; write `bronze.xg_shot_predictions` for GS/SC in the shipped mode (per-row CI populated in both modes — m2/m3).
- [ ] **Step 3:** write the **calibration report** (reliability curve description + per-provider AUC/Brier-skill vs base rate + n-aware calibration verdict + shipped mode + `ood_flag`), committed to `docs/reports/`.
- [ ] **Step 4:** distribution sanity assertions (median ≈ 0.05, p99 < ~0.75, max < 1.0) logged in the report.
- [ ] **Step 5: Commit** — `feat(xg): score GS/SC, gate scoring mode, per-provider calibration + report`.

**Acceptance:** each cohort has a shipped mode + calibrator; report attached; a cohort failing discrimination *or* n-aware calibration is `ood_flag=true`/uncertified (loud). Report the `ood_flag ⇒ silly-kicks drops cohort` contract explicitly.

---

### Task 1.4: `stg_xg__shot_predictions` + `fct_shot_xg` mart [spec §5.4, C7]

**Files:**
- Create: `dbt_project/models/staging/xg/stg_xg__shot_predictions.sql`, `dbt_project/models/marts/fct_shot_xg.sql`
- Modify: `dbt_project/models/staging/xg/_xg__sources.yml` (+ `xg_shot_predictions`, `shot_freeze_frames`), `dbt_project/models/marts/_marts__models.yml` (`fct_shot_xg` contract)
- Test: `src/tests/test_fct_shot_xg_sql.py` (SQL-invariant assertions — grain, join keys, columns; python-ci merge-time guard per parse-only CI)

- [ ] **Step 1: Write failing tests** (assert on model SQL text — grain `(match_key, action_id)`; INNER JOIN to `fct_action_values`/`fct_action_context` on `(match_key, action_id)`; presence of `scoring_mode`/`ood_flag`/CI columns; dedup by `_ingested_at`).
- [ ] **Step 2: Run** → FAIL.
- [ ] **Step 3: Implement** staging (dedup `row_number() over (partition by match_key, action_id order by _ingested_at desc)`) + mart (grain `(match_key, action_id)`, `contract: enforced: true`, `liquid_clustered_by=['match_key']`, Kimball FKs via INNER JOIN on `(match_key, action_id)`, `tags=['marts','output_mart']`). `enabled=var('xg_shot_enabled', false)` per the gated-mart pattern.
- [ ] **Step 4: Run** → PASS; then `uv run --extra dbt dbt build --select fct_shot_xg --project-dir dbt_project --profiles-dir dbt_project` (live/daily) → contract holds.
- [ ] **Step 5: Commit** — `feat(xg): stg_xg__shot_predictions + fct_shot_xg mart (C7, ADR-013)`.

**Acceptance:** grain uniqueness `(match_key, action_id)`; contract enforced; FKs resolve; SQL-invariant tests green.

---

### Task 1.5: Golden orientation test + cross-provider e2e — SYNTHETIC fixtures [spec §5.2/§5.6, M4, B3]

**Files:**
- Create: `src/tests/action_context/test_xg_freeze_frame_orientation_golden.py`, `src/tests/test_xg_shot_xg_e2e.py`
- Fixtures: **SYNTHETIC** GS + SC slices (hand-authored frames, as Task 0.4 does). The **SC slice MUST be synthetic** — real RM/Soccermatics-Pro data is hard no-commit (B3). GS may be real if convenient, but synthetic is preferred.
- Owner-gated (non-committed) real validation: `scripts/validate_xg_v3_tracking.py` + `@pytest.mark.e2e` (reads live restricted bronze incl. RM; never writes fixtures).

> **B3 (fixture policy — two different rules by source):**
> - **Gradient Sports (GS):** restriction is a **license-clarity ambiguity, not a hard barrier** (user 2026-07-06) — small committed real slices are acceptable; the existing `gradientsports/10517_p3` fixture needs no remediation (P-8).
> - **SkillCorner Real Madrid (RM), source = Soccermatics Pro:** **HARD NO-COMMIT (user 2026-07-06).** Do **NOT** commit any real RM / Soccermatics Pro data into repo fixtures — the SC slice in Task 1.5 **MUST be synthetic**. Real RM validation is owner-gated + non-committed only (Step 6).
> - **Default for all new committed tests:** prefer synthetic frames (hermetic, deterministic) regardless of source.

- [ ] **Step 1: Write failing golden** — a **synthetic** frame: GK at low x in the shooter-attacks-→ frame for a known shot; `[0,1]` normalization; handedness (near/far-post not mirrored).
- [ ] **Step 2: Write failing e2e** — **synthetic** GS + SC slices: raw shot → `build_tracking_snapshots` → `xg_shot_scorer` (fixture model) → `fct_shot_xg` row lands on the expected `(match_key, action_id)`, **1:1 cardinality**, sane value; assert ≥1 GS-shaped and ≥1 SC-shaped row.
- [ ] **Step 3: Run** → FAIL. **Step 4: Implement fixtures/wiring (synthetic).** **Step 5: Run** → PASS.
- [ ] **Step 6:** Add `scripts/validate_xg_v3_tracking.py` (owner-gated, reads live restricted bronze, NOT committed data) + a `@pytest.mark.e2e` test that runs it — the *real*-data confidence check, excluded from default CI (mirrors silly-kicks' owner-gated e2e pattern).
- [ ] **Step 7: Commit** — `test(xg): synthetic orientation golden + cross-provider e2e + owner-gated real validation`.

**Acceptance:** committed regression floors use synthetic fixtures only; real-GS/SC validation is owner-gated + non-committed; no new restricted data enters git.

---

### Task 1.6: Sync + publish (GS/SC restricted) [spec §7, ADR-064/049]

**Files:**
- Modify: `src/ingestion/refresh_synced_tables.py` (`SYNCED_TABLES`) + `dbt_project.yml` (`triggered_synced_marts`) if `fct_shot_xg` is synced; `scripts/create_indexes.py`
- Test: `src/tests/test_strand_safe_rederive.py` (parity), leak-guard tests if a publisher is added

- [ ] **Step 1 (decision):** silly-kicks reads gold directly → **no HF publish on the critical path.** If `fct_shot_xg` is synced to Lakebase for app use, add to both `SYNCED_TABLES` + `triggered_synced_marts` (parity test), index `match_key`, heal grants via `gh workflow run lakebase-grants.yml`.
- [ ] **Step 2:** If a publisher is later added, GS + restricted-SC rows split to the private companion via `split_restricted` + `assert_no_private_leak` (register in `PUBLISHER_REGISTRY`); `access_tier` rides per-row. (Out of the unblock path — note only.)
- [ ] **Step 3: Commit** — `feat(xg): fct_shot_xg synced-table registration + index (if synced)`.

**Acceptance:** if synced — parity test green, grants/index healed; publishing (if any) leak-guarded.

> **CHECKPOINT B (review):** GS + SC calibrated pre-shot xG in `fct_shot_xg`, joinable to `action_id`, with the calibration report + shipped-mode + `ood_flag`. **silly-kicks handoff possible here.** The `ood_flag ⇒ drop cohort` go/no-go is decided with the user — **with WC2022 (GS) as the expected primary cohort and SkillCorner-RM as a likely-CI-uncertified bonus (N2)**; the report surfaces point AUC + CI + calibration so the user can consciously include SC with a documented caveat (e.g. point AUC ~0.78) rather than a silent drop. Pause for review.

---

## Phase 2 — Consolidate StatsBomb + Wyscout; retire `fct_xg_predictions_v2` [spec §6]

### Task 2.1: Extend scorer to StatsBomb-360 + zero-context (Wyscout/non-360) [spec §6.1, B3]

**Files:**
- Modify: `src/ingestion/xg_shot_scorer.py`
- Test: `src/tests/test_xg_shot_scorer.py` (extend)

- [ ] **Step 1: Write failing tests** — StatsBomb-360 shots score context-aware via `build_sb360_snapshots` freeze frames (into `bronze.shot_freeze_frames`); Wyscout/non-360-SB score **tabular-only** (zero context) → a value, not NaN (B3 fix); the tabular-only population is reported with its own discrimination/calibration.
- [ ] **Step 2: Run** → FAIL. **Step 3: Implement** the all-provider scoring path. **Step 4: Run** → PASS.
- [ ] **Step 5 (live):** score StatsBomb + Wyscout into `bronze.xg_shot_predictions`; **scope guard** — if the zero-context population can't clear its own calibration/discrimination validation this cycle, scope Wyscout/non-360-SB out explicitly (documented) rather than ship uncertified.
- [ ] **Step 6: Commit** — `feat(xg): all-provider scoring incl. zero-context tabular-only path (B3)`.

**Acceptance:** StatsBomb-360 context-aware + Wyscout/non-360 tabular-only (trained, m1) both emit values; tabular-only population validated or explicitly scoped out.

---

### Task 2.2: StatsBomb/Wyscout bridge into `fct_shot_xg` + parity [spec §6]

**Files:**
- Modify: `dbt_project/models/marts/fct_shot_xg.sql` (union all providers)
- Test: `src/tests/test_xg_shot_xg_bridge.py`

- [ ] **Step 1: Write failing test** — resolve `fct_shot_xg.(match_key, action_id)` ↔ `fct_action_values.original_event_id` ↔ `fct_shots.event_id` **by JOIN** (no MD5 recompute); assert 1:1 cardinality + zero unresolved on the StatsBomb shot set.
- [ ] **Step 2: Run** → FAIL. **Step 3: Implement** the union + verify the bridge. **Step 4: Run** → PASS.
- [ ] **Step 5: Commit** — `feat(xg): consolidate SB+WS into fct_shot_xg via action-stream bridge`.

**Acceptance:** provider-as-column single fact; bridge 1:1, zero unresolved.

---

### Task 2.3: `fct_xg_predictions_v2` → back-compat view/table [spec §6, m5, Hyrum]

**Files:**
- Modify: `dbt_project/models/marts/fct_xg_predictions_v2.sql`, `_marts__models.yml`
- Test: `src/tests/test_fct_xg_predictions_v2_backcompat.py`

- [ ] **Step 1: Write failing test** — the rewritten model reproduces the old column set (`shot_id`, `xg_set_encoder`, `xg_ci_lower/upper`, Kimball FKs) from `fct_shot_xg` bridged to `fct_shots`.
- [ ] **Step 2: Run** → FAIL. **Step 3: Implement** as a **view** by default (materialized table only if P-5 finds a latency-sensitive consumer — m5). Keep `data_source` restricted to `statsbomb`/`wyscout` for exact back-compat.
- [ ] **Step 4: Run** → PASS; `dbt build --select fct_xg_predictions_v2` (live) → consumers unaffected.
- [ ] **Step 5:** Verify each P-5 consumer (Taipy shot-map, HF publisher, synced/index scripts) still resolves.
- [ ] **Step 6: Commit** — `refactor(xg): fct_xg_predictions_v2 -> back-compat over fct_shot_xg`.

**Acceptance:** old shape reproduced; P-5 consumers work; view-vs-materialized per measured latency.

---

### Task 2.4: UX methodology caption + user-facing value-change note [spec §6, UX]

**Files:**
- Modify: `hf_taipy_app/src/**` shot-map page (caption), per `hf_taipy_app/CLAUDE.md`
- Test: existing Taipy tests

- [ ] **Step 1:** Add a methodology caption on the shot-map noting xG was recomputed on the SPADL-native `model_version` (never silently substitute). Apply cross-cutting per `hf_taipy_app/CLAUDE.md` (all pages showing xG).
- [ ] **Step 2: Commit** — `feat(ux): shot-map xG recalibration methodology caption`.

**Acceptance:** caption live; value change documented, not silent.

> **CHECKPOINT C (review):** one all-provider pre-shot xG source of truth in `fct_shot_xg`; `fct_xg_predictions_v2` is a compat shim; governance current.

---

## Cross-cutting

- **Rollback:** Phase 0 reversal = re-point `wf-xg-v2` to the prior model (`xg_model_v2@Champion` untouched) — nothing consumed `xg_model_v3` until Phase 1's `fct_shot_xg`, which is a new gated mart (`xg_shot_enabled=false` by default). Phase 2 reversal = restore the materialized `fct_xg_predictions_v2` from its prior definition (git revert) + re-derive strand-safe (ADR-043). `bronze.shot_freeze_frames` + `bronze.xg_shot_predictions` are additive.
- **Dual-model window:** `xg_model_v2` (yards) and `xg_model_v3` (SPADL) coexist until Task 2.3. The Task 1.2 coordinate guard (M3) is the guard against a mixup.
- **Performance:** snapshot builder + scorer run on the shot subset — trivial; no benchmark gate. Decoupled from AC (retraining xG never triggers an AC recompute).
- **Terraform env pins (ADR-046):** no new serverless dep expected; if one appears, mirror `==` pins + `uv.lock` + terraform together.
- **Docs:** ARCHITECTURE Appendix D unchanged (Deep Sets / MC dropout already cited — no new author). NOTICE unchanged.

---

## Acceptance (done-when, whole plan) — maps to spec §9

1. GS + SkillCorner shots resolve non-null `xg_set_encoder` at ≥ StatsBomb coverage (~100% of 1,588); gaps justified.
2. Certification = discrimination (OOS AUC **bootstrap CI lower bound** ≥ StatsBomb−margin, absolute ≥0.65, Brier-skill>0) **and** n-aware calibration; failure → `ood_flag`/uncertified. Mode-selection and certification are separate (a shipped mode below the floor is `ood_flag`). Report presents **point AUC + CI + calibration** so the Checkpoint-B override is possible (N2). SkillCorner is *expected* CI-uncertified at its n — WC2022 is the primary cohort.
3. Per-provider shipped `scoring_mode` gated (context never worse than tabular baseline); V-6 set-cardinality + **composition** diagnostic attached; snapshot actor-inclusion convention matches SB-360.
4. Documented, cardinality-tested join `fct_shot_xg.(match_key, action_id)` → `fct_action_values.action_id`; committed GS+SC e2e.
5. Distribution sanity per provider; per-row CI populated (both modes).
6. `ood_flag ⇒ silly-kicks drops cohort` contract explicit (Checkpoint B go/no-go).
7. Provenance (`model_version`, `calibration_version`, `scoring_mode`, CI, `ood_flag`) on every row.
8. One source of truth: `fct_xg_predictions_v2` is a compat shim over `fct_shot_xg`; all providers, one SPADL-native model; zero-context path built+validated or explicitly scoped out.
9. Train/serve parity (M2) + coordinate guard (M3) + governance + orientation golden + bridge tests green.

## Explicitly NOT in this plan (deferred)

- Pressure-stratified calibration covariate (single pooled per-provider calibrator; silly-kicks stratifies by pressure itself) — spec O-3.
- Public HF publishing of `fct_shot_xg` (both cohorts restricted; silly-kicks reads gold) — added later under ADR-064 if wanted.
- Retiring the legacy `fct_tracking_context` path (unrelated).
