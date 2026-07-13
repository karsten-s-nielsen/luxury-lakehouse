# Action-Context Coverage Completeness Implementation Plan (v2)

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Finish the half-done delete-and-depend on silly-kicks' tracking preprocess — whose dropped `len <= 1` guard silently zeroed a half-match of SkillCorner action-context — and make any future unit failure impossible to ship as SUCCESS.

**Architecture:** The lakehouse already delegates frame *building* to silly-kicks (TF-23) but bolts a hand-maintained *velocity* port on afterwards. GS and IDSSE already pass `preprocess=` to the builder; SkillCorner and Metrica do not. This plan collapses SC + Metrica onto that same seam and deletes both copies of the port. Plus: the drain must fail its task on `failed > 0`, and the completeness invariant must be anchored on bronze action counts rather than the frame clock it is meant to police.

**Tech Stack:** Python 3.10, pandas 2.2.3, silly-kicks 4.43.0, PySpark (production driver only), pytest, Databricks.

**Spec:** `docs/superpowers/specs/2026-07-12-action-context-coverage-completeness-design.md` (v3)

**Git flow:** ONE feature branch → ONE commit → ONE PR. Spec + plan + ADR bundled into that commit. Commit / push / PR / merge each need separate explicit user approval.

---

## Context the engineer needs

**What broke.** `analytics/action_context/convert.py:79-88` re-implements silly-kicks' short-group velocity fallback but omits its `len(x_vals) <= 1` early-return. A one-frame player track reaches `np.gradient` (needs ≥ 2 points) and raises. That happened in SkillCorner `1552423`, period 2, frame batch 184.

**Why it cost 550 rows.** The UDF correctly re-raised with the group key (ADR-002 §5), but one raising batch fails the whole `applyInPandas` write, so the unit emitted **zero** rows. `drain.py:170-181` caught it (`failed += 1`, `continue`); `main_drain_worker` logged and returned. Task → SUCCESS. Job → SUCCESS.

**The velocity wiring is half-migrated.** silly-kicks' builders take `preprocess: PreprocessConfig | None` and run **three** passes internally — `interpolate_frames` → `smooth_frames` → `derive_velocities` (`skillcorner.py:281-291`).

| provider | builder | velocity source (today) |
|---|---|---|
| GS, IDSSE | silly-kicks | `preprocess=` on the builder (`pipeline.py:160,248`) |
| SkillCorner, Metrica | silly-kicks (`sk_frame_adapters.py:142`) | **lakehouse port**, bolted on in `_finalize` |
| — | legacy TC-1 (`tracking_context.py:1050,1169`) | **lakehouse port** (second copy) |

Both ports are `_derive_velocities_savgol`, welded by an AST-equality test (`test_convert_drift.py:46`).

**⚠️ The `is_default()` trap — do NOT hand-build a config.** `resolve_preprocess` promotes to per-provider tuning **only if** `cfg.is_default()`, which is **flag-based** and set solely by the `.default()` factory (`_config_dataclass.py:74,113`; `_resolve.py:36-39`). A hand-built `PreprocessConfig(derive_velocity=True)` has the flag `False`, is passed through **unpromoted**, and silently uses the **universal** `sg_window_seconds=0.4` instead of SkillCorner's tuned **1.0**. Always pass `PreprocessConfig.default()`.

> **Pre-existing bug, DO NOT fix here:** `pipeline.py:248` hand-builds its config, so **GS is running at 0.4 instead of its tuned 0.333** (window 13 vs 11 at 30 Hz). Real, but fixing it in this PR would move GS values and ambush the Task 12 golden diff, which asserts GS does **not** move. **File it separately** (Task 11 Step 2).

**Verified non-issue (do not "fix" it):** the port groups with pandas' default `dropna=True`, silly-kicks uses `dropna=False`. This does **not** drop the ball group: the port iterates `.groups.items()`, and `.groups` includes NaN keys even under `dropna=True` (verified, pandas 2.2.3). Ball velocities are populated today and will not appear "for the first time".

**Verification commands** (never pipe to `tail` — it masks the exit code):
```bash
uv run ruff check src/ scripts/
uv run pyright src/
uv run pytest src/tests/
```

---

## Task 1: Branch and baseline

- [ ] **Step 1: Sync and branch**

```bash
git fetch origin && git checkout main && git pull --ff-only origin main
git checkout -b fix/ac-coverage-completeness
```

- [ ] **Step 2: Green baseline BEFORE any change**

```bash
uv run pytest src/tests/ -q > /tmp/baseline.txt 2>&1; echo "EXIT=$?" >> /tmp/baseline.txt
```
Read the final `EXIT=` line. Must be `EXIT=0`. If red, STOP and report.

---

## Task 2: D2 — the drain must fail its task when units failed

Lands first: everything downstream (especially the Task 12 recompute) depends on a failing unit being loud.

**Files:**
- Modify: `src/ingestion/action_context.py` (import at `:27`; `main_drain_worker` at `:1300-1308`)
- Test: `src/tests/action_context/test_drain_failure_surfacing.py` (create)

> `DrainSummary.worker_id` is a **required** field (`drain.py:44`) — `DrainSummary()` raises `TypeError`.
> It already carries `worker_id`, so the helper must NOT take a second one (the message could then
> disagree with the object it describes).

- [ ] **Step 1: Write the failing test**

```python
"""D2: a drain that swallowed a unit failure must NOT report success.

2026-07-11: skillcorner:1552423:2 raised inside the UDF; drain_worker caught it (failed=1) and
continued; main_drain_worker logged the summary and returned -- so the task exited 0 and the job
reported SUCCESS while 550 actions were missing. The swallow itself is CORRECT (one bad unit must
not destroy a 5.5h drain, and the worker's slice rolls forward). Exiting 0 afterwards is not.
"""

from __future__ import annotations

import pytest

from analytics.action_context.drain import DrainSummary
from ingestion.action_context import raise_on_failed_units


def test_failed_units_raise_with_labels() -> None:
    summary = DrainSummary(
        worker_id=5, processed=46, failed=1, failed_units=["skillcorner:1552423:2"]
    )
    with pytest.raises(RuntimeError, match="skillcorner:1552423:2"):
        raise_on_failed_units(summary, run_id="85619159042760")


def test_clean_drain_does_not_raise() -> None:
    raise_on_failed_units(DrainSummary(worker_id=0, processed=47), run_id="r1")


def test_timeouts_alone_do_not_raise() -> None:
    """Timeouts roll forward to the next run BY DESIGN -- a capacity signal, not a correctness
    one. Only `failed` means a unit produced no rows and never will."""
    summary = DrainSummary(
        worker_id=1, processed=40, timed_out=7, timed_out_units=["gradientsports:10502:1"]
    )
    raise_on_failed_units(summary, run_id="r1")
```

- [ ] **Step 2: Run and confirm it fails**

```bash
uv run pytest src/tests/action_context/test_drain_failure_surfacing.py -v
```
Expected: `ImportError: cannot import name 'raise_on_failed_units'`.

- [ ] **Step 3: Implement**

In `src/ingestion/action_context.py`, extend the import at `:27` to include `DrainSummary`
(it currently imports only `WATCHDOG_BUDGET_S, assign_workers, drain_worker` — pyright will fail otherwise):

```python
from analytics.action_context.drain import WATCHDOG_BUDGET_S, DrainSummary, assign_workers, drain_worker
```

Add the helper near `main_drain_worker`:

```python
def raise_on_failed_units(summary: DrainSummary, *, run_id: str) -> None:
    """Fail the task if any unit failed (D2 -- ADR-002 §5 applied at the ORCHESTRATION layer).

    The per-unit ``except Exception`` in ``drain.py:170-181`` is deliberate and STAYS: one bad
    unit must not destroy a multi-hour drain. The defect it enabled was that the TASK then exited
    0 -- so a raised guard and a silent pass were indistinguishable from the mart, and
    skillcorner:1552423:2 shipped 0 of 550 actions inside a "successful" run.

    Timeouts are deliberately EXCLUDED: they roll forward by design (a capacity signal).
    """
    if not summary.failed:
        return
    units = ", ".join(summary.failed_units)
    raise RuntimeError(
        f"action-context drain worker {summary.worker_id} (run_id={run_id}) had {summary.failed} "
        f"FAILED unit(s): {units}. Each failed unit wrote ZERO rows -- its actions are missing from "
        "fct_action_context. Do NOT accept this run: fix the cause and re-drain. "
        "(Timeouts roll forward and are not counted here.)"
    )
```

At the end of `main_drain_worker`, AFTER the existing `task_logger.info("Drain worker %d complete: ...")`
call (the summary must always be logged before the raise):

```python
    raise_on_failed_units(summary, run_id=run_id)
```

- [ ] **Step 4: Verify**

```bash
uv run pytest src/tests/action_context/test_drain_failure_surfacing.py -v
```
Expected: 3 passed.

---

## Task 3: D1 — collapse SkillCorner + Metrica onto the builder's `preprocess=` seam

Do **not** hand-wire `smooth_frames`/`derive_velocities`. The builder already does it, GS and IDSSE
already use it, and hand-wiring would silently skip `interpolate_frames`.

**Files:**
- Modify: `src/analytics/action_context/sk_frame_adapters.py` (`_finalize` `:93-103`; import `:27`; builder calls `:142`, `:~205`)
- Modify: `src/analytics/action_context/convert.py` (delete `_derive_velocities_savgol`)
- Modify: `src/tests/action_context/test_sk_frame_adapters.py` — **`test_skillcorner_adapter_derives_velocities_and_matches_schema` (`:96`) changes behaviour** (velocities now come from the builder's three-pass preprocess, and interpolation fills NaN x/y). Update its expectations.
- Test: `src/tests/action_context/test_velocity_single_frame.py` (create)

> **⚠️ Test the REAL path, not `_finalize`.** Adopting the `preprocess=` seam moves velocity derivation
> **into the builder**; `_finalize` no longer computes it. A test that hand-builds a frame and calls
> `_finalize` directly can never go green — and worse, `_AC_FRAME_COLUMNS` **contains `vx`/`vy`/`speed`**
> (`sk_frame_adapters.py:15,23,24`), so any "pad missing columns with NaN" helper *fabricates* the NaN the
> test then asserts on. That assertion would pass with the velocity code deleted entirely.
>
> Drive `convert_skillcorner_bronze_to_frames` instead — builder → preprocess → `_finalize`, the actual
> production path. Reuse the synthetic bronze builder `_sc_bronze()` already in
> `test_sk_frame_adapters.py:25`; real bronze also exists at
> `src/tests/fixtures/action_context/skillcorner/1886347_p2/frames.parquet`.

- [ ] **Step 1: Write the failing test — on the production path**

```python
"""D1: a 1-frame player track must yield NaN velocity, not a crash.

skillcorner 1552423, period 2, frame batch 184 contained a player with exactly ONE frame. The
lakehouse's ported velocity helper dropped silly-kicks' `len(x_vals) <= 1` guard, so np.gradient
(needs >= 2 points) raised, the UDF re-raised, and the WHOLE unit wrote 0 of 550 actions.

Drives convert_skillcorner_bronze_to_frames -- builder -> preprocess -> _finalize -- because after
the delete-and-depend the velocity step lives in the BUILDER, not in _finalize. Asserting on a
hand-built frame passed to _finalize would assert on padded NaN, not on a guard.
"""

from __future__ import annotations

import numpy as np
import pandas as pd
import pytest

from analytics.action_context.sk_frame_adapters import (
    _AC_FRAME_COLUMNS,
    _finalize,
    convert_skillcorner_bronze_to_frames,
)
from src.tests.action_context.test_sk_frame_adapters import _sc_bronze  # synthetic SC bronze


def _one_frame_player_bronze() -> pd.DataFrame:
    """Take the standard synthetic SC bronze and give one player EXACTLY ONE frame."""
    bronze = _sc_bronze()
    victim = bronze["player_id"].dropna().iloc[0]
    keep_one = bronze[bronze["player_id"] == victim].index[1:]  # drop all but the first row
    return bronze.drop(index=keep_one).reset_index(drop=True)


def _prt(bronze: pd.DataFrame) -> pd.DataFrame:
    """Period-relative clock map the adapter requires (frame_id, period_id, time_seconds)."""
    fr = bronze[["frame", "period"]].drop_duplicates()
    return pd.DataFrame(
        {
            "frame_id": fr["frame"].to_numpy(),
            "period_id": fr["period"].to_numpy(),
            "time_seconds": fr["frame"].to_numpy() / 10.0,
        }
    )


def test_single_frame_track_yields_nan_velocity_not_a_crash() -> None:
    bronze = _one_frame_player_bronze()

    frames, _report = convert_skillcorner_bronze_to_frames(
        bronze, game_id=99, home_team_id="H", period_relative_time=_prt(bronze)
    )

    victim = _sc_bronze()["player_id"].dropna().iloc[0]
    once = frames[frames["player_id"].astype(str) == str(victim)]
    assert len(once) == 1, "fixture must give the victim exactly one frame"
    assert np.isnan(once["vx"].iloc[0]), "a 1-frame track has no velocity -- must be NaN"
    assert np.isnan(once["vy"].iloc[0])

    # Non-degenerate tracks must still get REAL velocities -- this is what makes the NaN above
    # meaningful rather than an artefact of an unfilled column.
    others = frames[frames["player_id"].astype(str) != str(victim)]
    assert others["vx"].notna().any(), "multi-frame tracks must carry real velocities"


def test_finalize_drops_the_preprocess_scratch_columns() -> None:
    """The scratch columns must be dropped before the symmetric-diff check, or EVERY unit fails.

    Inject them explicitly -- a frame padded to exactly _AC_FRAME_COLUMNS carries no scratch
    columns, so the drop would never be exercised and the test would pass vacuously.
    """
    base = pd.DataFrame({col: [np.nan] for col in _AC_FRAME_COLUMNS})
    base["frame_id"] = [1]
    base["is_ball"] = [False]
    dirty = base.assign(x_smoothed=1.0, y_smoothed=2.0, _preprocessed_with="savgol")

    out = _finalize(dirty, derive_velocities=True)

    assert set(out.columns) == set(_AC_FRAME_COLUMNS)
    for leaked in ("x_smoothed", "y_smoothed", "_preprocessed_with"):
        assert leaked not in out.columns


def test_empty_bronze_does_not_raise() -> None:
    """H3 -- the hazard MOVED. silly-kicks reads `frames["frame_rate"].dropna().iloc[0]`
    (_smoothing.py:110) -> IndexError on a zero-row frame, and the SkillCorner builder has NO
    empty-bronze early return. So the guard belongs BEFORE the builder call, not in _finalize.
    """
    empty = _sc_bronze().iloc[0:0]

    frames, _report = convert_skillcorner_bronze_to_frames(
        empty, game_id=99, home_team_id="H", period_relative_time=_prt(_sc_bronze()).iloc[0:0]
    )

    assert len(frames) == 0
    assert set(frames.columns) == set(_AC_FRAME_COLUMNS)
```

> If importing `_sc_bronze` across test modules is awkward, move it to
> `src/tests/action_context/conftest.py` as a fixture and have both modules use it. Do not duplicate it.

- [ ] **Step 2: Run — it MUST fail with the production error**

```bash
uv run pytest src/tests/action_context/test_velocity_single_frame.py -v
```
Expected: `test_single_frame_track_yields_nan_velocity_not_a_crash` FAILS with
`ValueError: Shape of array too small to calculate a numerical gradient` — today's `_finalize` still calls
the port, so the RED is real.
**If it does not fail with that message, STOP — the fixture does not reproduce the bug.**

- [ ] **Step 3: Pass `preprocess=` to the builders**

In `src/analytics/action_context/sk_frame_adapters.py`, in `convert_skillcorner_bronze_to_frames`
(`:142`) and `convert_metrica_bronze_to_frames` (`:~205`), pass the config to the builder:

```python
    from silly_kicks.tracking.preprocess import PreprocessConfig

    frames, report = convert_to_frames(
        bronze,
        home_team_id=str(home_team_id),
        output_convention="ltr",
        preprocess=PreprocessConfig.default(),  # auto-promotes to the per-provider tuning
    )
```

> `PreprocessConfig.default()` — **never** `PreprocessConfig(derive_velocity=True)`. See the
> `is_default()` trap in the Context section. `.default()` promotes to SkillCorner's 1.0s / Metrica's
> 0.4s window; a hand-built config silently gets the universal 0.4s.

- [ ] **Step 4: Strip the velocity branch from `_finalize`; put the empty guard where the hazard is**

Delete the import at `:27` and replace `_finalize` (`:93-103`) with:

```python
def _finalize(frames: pd.DataFrame, *, derive_velocities: bool) -> pd.DataFrame:
    """Drop silly-kicks' preprocess scratch columns and assert the AC result-frame schema.

    Delete-and-depend (ADR-055 precedent): velocities now come from the BUILDER's `preprocess=`
    seam (the same one GS and IDSSE already use), which runs interpolate -> smooth -> derive.
    The lakehouse's own `_derive_velocities_savgol` is DELETED: it was a single SG pass on RAW
    positions that had dropped silly-kicks' `len(x_vals) <= 1` guard, crashing on 1-frame tracks
    and zeroing a whole work unit (2026-07-11, skillcorner:1552423:2).

    ``provider`` is gone -- it was only ever used to pick SG parameters, which the builder now
    resolves itself.
    """
    scratch = [c for c in ("x_smoothed", "y_smoothed", "_preprocessed_with") if c in frames.columns]
    if scratch:
        frames = frames.drop(columns=scratch)
    if not derive_velocities:
        # The builder emits `speed`/`speed_source` unconditionally but NOT vx/vy (skillcorner.py:185,214),
        # so without the preprocess pass the AC schema is short two columns and the check below would
        # raise. Materialise them as NaN -- "no velocity derived" is exactly what NaN means here.
        for col in ("vx", "vy"):
            if col not in frames.columns:
                frames[col] = np.nan
    if not frames.empty:
        frames = frames.sort_values(["frame_id", "is_ball"]).reset_index(drop=True)
    drift = set(frames.columns) ^ _AC_FRAME_COLUMNS
    if drift:
        raise ValueError(f"sk_frame_adapters: AC result-frame schema drift (symmetric diff): {sorted(drift)}")
    return frames
```

**Both call sites lose the `provider=` argument** (`_finalize(frames, derive_velocities=derive_velocities)`).

**Guard the empty case BEFORE the builder, not after.** The `IndexError` hazard is inside
`smooth_frames` (`_smoothing.py:110` reads `frames["frame_rate"].dropna().iloc[0]`), and the SkillCorner
builder has **no empty-bronze early return** — so an empty slice raises *upstream of `_finalize`*. In each
converter, pass `preprocess=None` when the bronze is empty:

```python
    _cfg = None if bronze.empty else PreprocessConfig.default()
    frames, report = convert_to_frames(
        bronze, home_team_id=str(home_team_id), output_convention="ltr", preprocess=_cfg,
    )
```

`_finalize`'s `derive_velocities=False` branch then supplies the NaN `vx`/`vy`, so an empty slice still
comes back in the AC schema.

- [ ] **Step 5: Delete the port**

In `src/analytics/action_context/convert.py`, delete `_derive_velocities_savgol` entirely (`:22` to the
end of its body) and the `scipy.signal` import inside it. Update the module docstring (`:1-8`) to drop
the reference to "the shared `_derive_velocities_savgol` helper".

- [ ] **Step 6: Verify**

```bash
uv run pytest src/tests/action_context/test_velocity_single_frame.py -v
```
Expected: 3 passed.

---

## Task 4: D1 — migrate the LEGACY TC-1 copy

**⚠️ TC-1 IS LIVE.** `ingestion/tracking_context.py` writes `bronze.spadl_tracking_context`, and the
`fct_tracking_context` retirement is still open (blocked on re-homing the GK-identity and IDSSE-minutes
consumers). So this is **not** a test-mechanics chore: **it changes a second production mart**, and that
mart needs its own recompute (Task 12 Step 3).

**Files:**
- Modify: `src/ingestion/tracking_context.py` (delete `_derive_velocities_savgol` `:840`; call sites `:1050`, `:1169`)
- Modify: `src/tests/action_context/test_convert_drift.py`

- [ ] **Step 1: Confirm the required columns exist, and ASSERT `frame_rate`**

`derive_velocities` needs `period_id`, `is_ball`, `player_id`, `frame_id`. **`frame_rate` is OPTIONAL
upstream and silently defaults to 25.0 Hz** (`_velocity.py:53`) — an absent or all-NaN `frame_rate` gives
**wrong velocities with no error**, the worst failure mode in this PR. Assert it.

- [ ] **Step 2: Replace the helper with a silly-kicks call**

Delete `_derive_velocities_savgol` (`:840`…) and add:

```python
def _apply_sk_velocities(frames: pd.DataFrame, *, provider: str) -> pd.DataFrame:
    """Derive vx/vy/speed via silly-kicks (delete-and-depend; see sk_frame_adapters._finalize).

    Returns a NEW frame (the old helper mutated in place and returned None). Scratch columns are
    dropped so the TC-1 schema is unchanged.
    """
    from silly_kicks.tracking.preprocess import PreprocessConfig, derive_velocities, interpolate_frames, smooth_frames

    if frames.empty:
        return frames
    if "frame_rate" not in frames.columns or frames["frame_rate"].dropna().empty:
        raise ValueError(
            f"_apply_sk_velocities({provider}): frames carry no usable `frame_rate`. silly-kicks "
            "silently defaults to 25.0 Hz, which would produce WRONG velocities with no error."
        )
    # PUBLIC API. Do NOT reach into silly_kicks.tracking.preprocess._resolve -- it is private, not in
    # __all__, and its owner would refactor it without notice (Hyrum's Law). `for_provider` performs
    # exactly the per-provider promotion the builder does internally.
    cfg = PreprocessConfig.for_provider(provider)
    out = frames
    if cfg.interpolation_method is not None:
        out = interpolate_frames(out, config=cfg)
    if cfg.smoothing_method is not None:
        out = smooth_frames(out, config=cfg)
    out = derive_velocities(out, config=cfg)
    return out.drop(columns=[c for c in ("x_smoothed", "y_smoothed", "_preprocessed_with") if c in out.columns])
```

> This mirrors the builder's three passes exactly (`skillcorner.py:286-291`) — TC-1 does not go through a
> silly-kicks builder, so the passes must be run explicitly. `resolve_preprocess(PreprocessConfig.default(), ...)`
> reproduces the builder's promotion.

**Signature change:** the old helper mutated in place and returned `None`; this returns a NEW frame.
Both call sites must now **assign**:

```python
    frames = _apply_sk_velocities(frames, provider="metrica")      # :1050
    frames = _apply_sk_velocities(frames, provider="skillcorner")  # :1169
```
Check the lines immediately after each call for code assuming in-place mutation.

- [ ] **Step 3: Update the drift test**

Delete `test_velocity_helper_no_drift` (the AST-equality assertion — there is no second copy to compare
against any more) and add:

```python
def test_velocity_helper_is_deleted_and_depended() -> None:
    """D1: BOTH lakehouse copies are deleted; silly-kicks owns velocity derivation.

    The port dropped upstream's `len(x_vals) <= 1` guard and crashed on 1-frame tracks, zeroing a
    whole work unit (2026-07-11). A comment claiming a copy "matches silly-kicks" is not a
    contract -- deletion is.
    """
    assert not hasattr(new, "_derive_velocities_savgol"), (
        "analytics.action_context.convert re-grew _derive_velocities_savgol (delete-and-depend regression)"
    )
    assert not hasattr(tc_legacy, "_derive_velocities_savgol"), (
        "ingestion.tracking_context re-grew _derive_velocities_savgol (delete-and-depend regression)"
    )
```

- [ ] **Step 4: Verify**

```bash
uv run pytest src/tests/action_context/ -v
```

---

## Task 5: D5 — time-base guards: lower bound + NaN

**Files:** `src/analytics/action_context/time_base_guard.py`; test `src/tests/action_context/test_time_base_guard.py`

- [ ] **Step 1: Failing tests**

```python
import math
import pytest
from analytics.action_context.time_base_guard import assert_frames_time_base, assert_work_unit_time_base


def test_negative_clock_raises() -> None:
    """The documented -2700s double-subtraction (ADR-040) passes a one-sided floor."""
    with pytest.raises(ValueError):
        assert_frames_time_base({2: -2700.0})


def test_nan_clock_raises() -> None:
    """`NaN >= 1800.0` is False -- a NaN min slipped through the floor silently."""
    with pytest.raises(ValueError):
        assert_frames_time_base({2: math.nan})


def test_healthy_clock_does_not_raise() -> None:
    assert_frames_time_base({1: 0.0, 2: 1.2})
    assert_work_unit_time_base({1: 0.0, 2: 3.4})


def test_small_negative_float_noise_is_tolerated() -> None:
    assert_frames_time_base({2: -0.3})
```

- [ ] **Step 2: Run — expect the first two to FAIL (no raise).**

- [ ] **Step 3: Implement**

Add `import math`, then below `_ABSOLUTE_CLOCK_MIN_FLOOR_SECONDS`:

```python
# Lower bound. A period-relative period starts at t ~= 0; small negative float noise is fine, but a
# large negative min means an OVER-SUBTRACTED clock -- the documented -2700 s SkillCorner P2
# double-subtraction (ADR-040), which a one-sided floor passes trivially. NaN is rejected explicitly
# because `nan >= floor` is False, so a NaN min slipped through silently.
_PERIOD_RELATIVE_MIN_FLOOR_SECONDS: float = -60.0


def _offending_periods(period_min: dict[int, float]) -> dict[int, float]:
    return {
        p: m
        for p, m in period_min.items()
        if math.isnan(m) or m < _PERIOD_RELATIVE_MIN_FLOOR_SECONDS or m >= _ABSOLUTE_CLOCK_MIN_FLOOR_SECONDS
    }
```

Use it in BOTH guards, and extend each message: too HIGH ⇒ absolute match clock; too LOW ⇒
over-subtracted re-base (the −2700 s class); NaN ⇒ missing timestamps.

- [ ] **Step 4: Verify** — 4 passed.

---

## Task 6: D7 — make the vacuous e2e assertion real

**Files:** `src/tests/action_context/test_e2e.py`

`test_gs_e2e_convert_and_enrich_does_not_crash` asserts only the column set — but `_empty_result()`
(`pipeline.py:119-123`) returns a **zero-row frame carrying the full schema**, so it passes on an empty
emit: exactly the failure we are chasing.

- [ ] **Step 1: Add the row assertion**

```python
    assert len(result) > 0, (
        "GS e2e resolved zero actions -- _empty_result() carries the FULL schema, so a column-set "
        "assertion alone passes on an empty emit (the silent-zero class this suite exists to catch)."
    )
```

- [ ] **Step 2: Name the CI gap instead of leaving it implicit**

Above the `AC1_E2E` skip marker (`:21`):

```python
# NOTE (D7): these row-count assertions do NOT run in CI (AC1_E2E=1 gate, ~5 min, DAS-dominated).
# The always-on guard for the single-frame / silent-zero class is
# src/tests/action_context/test_velocity_single_frame.py, which runs in the default suite.
```

- [ ] **Step 3:** `uv run pytest src/tests/action_context/ -v`

---

## Task 7: D3 + D4 — re-anchor the completeness invariant, and WIRE THE EXEMPTION

**This task reds the whole fixture suite if `is_slice` is not wired. Do not skip Step 1.**

**Files:**
- Modify: `src/analytics/action_context/completeness.py` (API rewrite)
- Modify: `src/analytics/action_context/pipeline.py` (`run_work_unit` signature + caller `:600-603`; imports `:26-28`)
- Modify: `src/ingestion/action_context.py` (caller `:1803`; imports `:1793-1796`)
- **Rewrite:** `src/tests/action_context/test_completeness.py`
- Modify: fixture-driven callers of `run_work_unit` (see Step 1)

**Why the exemption is load-bearing.** `scripts/extract_action_context_fixture.py` slices **frames**
(`--frame-start/--frame-end`) but `_pull_actions` has **no time filter** — a fixture is seconds of frames
against a **full-match** actions parquet. Under the new invariant `bronze_expected` is hundreds while
`emitted` is a handful, and the frame window overlaps ~1–2 % of the actions → below the floor →
`RuntimeError: ...UNEXPLAINED...`. **`MIN_EXPECTED_ACTIONS_FOR_CHECK` was the fence holding this up**;
`test_completeness.py` says so ("the dead-ball fixtures emit 0 of 1 by design"). Removing it without
replacing what it carried turns every fixture red — and with D2 live, that becomes a *task* failure.

- [ ] **Step 1: Wire `is_slice` through the local hexagon FIRST**

`WorkUnit.frame_range` is `None` in fixtures too, so it cannot be the signal. Add an explicit parameter
to `run_work_unit` — only tests and the golden builders call it; the Spark driver never does:

```python
def run_work_unit(wu, *, frames, actions, xt, meta, sink, is_slice: bool = False) -> int:
```

Pass `is_slice=is_slice` into `assert_unit_action_completeness`. Then set `is_slice=True` at every
fixture-driven call site: `test_dead_ball_batches.py`, `test_e2e.py`, `test_pipeline_dispatch.py`, and
**both golden builders** (`scripts/build_ac1_mini_golden.py`, `scripts/build_ac1_full_golden.py`).

Find them all:
```bash
grep -rn "run_work_unit(" src/ scripts/ | grep -v "def run_work_unit"
```

> **Record in the ADR:** `is_slice` is an INTERIM fence. The real fix is for the extractor to slice
> **actions to the frame window** too, making a fixture a faithful miniature of a production unit and
> `is_slice` unnecessary. That means regenerating committed parquet + re-baselining goldens — out of
> scope here, but it must not look like the permanent answer.

- [ ] **Step 2: REWRITE `test_completeness.py`**

The existing module imports `MIN_EXPECTED_ACTIONS_FOR_CHECK` (`:7-12`) which the rewrite deletes →
**ImportError at collection**, killing the whole module including the new tests. Six of its eight tests
also pass the removed `expected=` keyword. Migrate every one.

**Keep `test_silent_drop_raises_with_unit_key_and_counts`** — it matches `r"65 of 536"`, and the new
message preserves those numbers. It is the incident; do not lose it.

New tests:

```python
from analytics.action_context.completeness import (
    MISMATCH_OVERLAP_FLOOR,
    assert_unit_action_completeness,
    coverage_overlap_fraction,
)


def test_corrupted_clock_with_intact_bronze_count_raises() -> None:
    """D3: the class the old invariant could not see. `expected` used to collapse together with
    `emitted` (both frame-derived), holding the ratio at ~1.0. Now `expected` is the bronze count."""
    with pytest.raises(RuntimeError, match="UNEXPLAINED"):   # case-sensitive re.search!
        assert_unit_action_completeness(
            emitted=0,
            bronze_expected=60,
            action_times_by_period={2: [float(t) for t in range(0, 600, 10)]},
            frame_window_by_period={2: (5000.0, 5600.0)},   # absolute clock: overlaps nothing
            unit_desc="skillcorner:1552423:2",
        )


def test_genuine_sparse_coverage_is_excused() -> None:
    """H4 regression guard. time_base_guard.py:15-24 records that an OVERLAP metric was tried and
    REJECTED for the primary guard, because it cannot tell an offset clock from legitimately sparse
    coverage. Our use is narrower -- an excuse-BOUND, not a primary raise -- so sparse-but-correct
    broadcast coverage must still pass."""
    assert_unit_action_completeness(  # must NOT raise
        emitted=30,
        bronze_expected=60,
        action_times_by_period={1: [float(t) for t in range(0, 600, 10)]},
        frame_window_by_period={1: (0.0, 300.0)},   # frames cover ~half the period
        unit_desc="skillcorner:1:1",
    )


def test_nine_covered_actions_zero_emitted_raises() -> None:
    """D4: the old `expected < 10` skip band let this through silently."""
    with pytest.raises(RuntimeError):
        assert_unit_action_completeness(
            emitted=0, bronze_expected=9,
            action_times_by_period={1: [float(t) for t in range(9)]},
            frame_window_by_period={1: (-1.0, 100.0)},
            unit_desc="p:m:1",
        )


def test_slice_fixture_is_exempt_by_flag_not_by_size() -> None:
    """Fixtures are exempted EXPLICITLY. A size threshold also exempts a 9-action clip of a REAL
    half-match -- precisely the silent skip this guard exists to prevent."""
    assert_unit_action_completeness(  # must NOT raise
        emitted=0, bronze_expected=3,
        action_times_by_period={1: [1.0, 2.0, 3.0]},
        frame_window_by_period={1: (0.0, 5.0)},
        unit_desc="idsse:mini:1", is_slice=True,
    )


def test_overlap_fraction_uses_the_unbuffered_window() -> None:
    assert coverage_overlap_fraction({1: [1.0, 2.0, 3.0, 4.0]}, {1: (0.0, 2.5)}) == 0.5
    assert coverage_overlap_fraction({1: [1.0, 2.0]}, {1: (900.0, 1000.0)}) == 0.0
    assert MISMATCH_OVERLAP_FLOOR == 0.2
```

- [ ] **Step 3: Rewrite `completeness.py`**

Replace everything below the module docstring:

```python
from __future__ import annotations

MIN_UNIT_ACTION_COVERAGE: float = 0.95

# Minimum fraction of a unit's actions the frame window must overlap before a window-based EXCUSE
# for a shortfall is BELIEVED. Adopted from silly-kicks' MISMATCH_OVERLAP_FLOOR
# (silly_kicks/tracking/utils.py:28).
#
# NOTE (H4): time_base_guard.py:15-24 records that an overlap metric was tried and REJECTED as a
# PRIMARY guard -- it cannot distinguish an offset clock from legitimately sparse coverage; both
# yield low overlap. This use is different and safe: it never raises on its own. It only decides
# whether a window is credible enough to EXCUSE a shortfall that `bronze_expected` has already
# proven real. Sparse-but-correct coverage still passes, because a unit that emitted everything
# its frames cover never reaches the excuse path. Pinned by test_genuine_sparse_coverage_is_excused.
MISMATCH_OVERLAP_FLOOR: float = 0.2


def coverage_overlap_fraction(
    action_times_by_period: dict[int, list[float]],
    frame_window_by_period: dict[int, tuple[float, float]],
) -> float:
    """Fraction of the unit's actions inside the frames' UNBUFFERED per-period window.

    A credibility check on the WINDOW -- never the expectation. A window on a broken clock overlaps
    ~0% of the actions and must not be allowed to excuse the shortfall it caused.
    """
    total = sum(len(t) for t in action_times_by_period.values())
    if total == 0:
        return 1.0
    inside = 0
    for period, times in action_times_by_period.items():
        window = frame_window_by_period.get(period)
        if window is None:
            continue
        lo, hi = window
        inside += sum(1 for t in times if lo <= t <= hi)
    return inside / total


def expected_actions_within_coverage(
    action_times_by_period: dict[int, list[float]],
    frame_window_by_period: dict[int, tuple[float, float]],
    buffer_s: float = 0.5,
) -> int:
    """Actions strictly INTERIOR to the frames' coverage. Used only to EXPLAIN a shortfall, never
    to define the expectation (that is ``bronze_expected``)."""
    expected = 0
    for period, times in action_times_by_period.items():
        window = frame_window_by_period.get(period)
        if window is None:
            continue
        lo, hi = window[0] + buffer_s, window[1] - buffer_s
        expected += sum(1 for t in times if lo <= t <= hi)
    return expected


def assert_unit_action_completeness(
    *,
    emitted: int,
    bronze_expected: int,
    action_times_by_period: dict[int, list[float]],
    frame_window_by_period: dict[int, tuple[float, float]],
    unit_desc: str,
    buffer_s: float = 0.5,
    min_coverage: float = MIN_UNIT_ACTION_COVERAGE,
    is_slice: bool = False,
) -> None:
    """Raise ``RuntimeError`` when a unit emitted fewer rows than its BRONZE action count allows.

    1. ``bronze_expected`` -- the unit's SPADL action count -- is the expectation. It is independent
       of the frame clock, so a corrupted clock cannot shrink it in lockstep with ``emitted`` (which
       is exactly how the previous frame-derived expectation went blind).
    2. A shortfall MAY be excused when the missing actions lie outside frame coverage -- but only if
       the window is CREDIBLE (overlaps >= MISMATCH_OVERLAP_FLOOR of the unit's actions). Otherwise
       the shortfall is UNEXPLAINED and we raise.

    ``is_slice`` exempts test fixtures EXPLICITLY (see the plan: fixture actions are not sliced to
    the frame window). There is deliberately NO size threshold -- one that exempts a 9-action fixture
    also exempts a 9-action clip of a real half-match.
    """
    if is_slice or bronze_expected == 0:
        return
    if emitted >= min_coverage * bronze_expected:
        return

    overlap = coverage_overlap_fraction(action_times_by_period, frame_window_by_period)
    if overlap < MISMATCH_OVERLAP_FLOOR:
        raise RuntimeError(
            f"action-context completeness violated for {unit_desc}: emitted {emitted} of "
            f"{bronze_expected} bronze SPADL actions, and the shortfall is UNEXPLAINED -- the frame "
            f"window overlaps only {overlap:.1%} of the unit's actions (floor "
            f"{MISMATCH_OVERLAP_FLOOR:.0%}). The tracking clock is not trustworthy, so its coverage "
            "cannot excuse the missing rows. Check the dispatch time-base (ADR-040) before "
            "re-running; do NOT accept this unit's output."
        )

    covered = expected_actions_within_coverage(action_times_by_period, frame_window_by_period, buffer_s=buffer_s)
    if covered and emitted >= min_coverage * covered:
        return  # excused: the missing actions genuinely lie outside credible frame coverage

    coverage = emitted / covered if covered else 0.0
    raise RuntimeError(
        f"action-context completeness violated for {unit_desc}: emitted {emitted} of {covered} "
        f"SPADL actions ({coverage:.1%} < {min_coverage:.0%}; bronze total {bronze_expected}). The "
        "unit silently dropped actions -- check the dispatch time-base (ADR-040) and batch window "
        "filters before re-running; do NOT accept this unit's output."
    )
```

- [ ] **Step 4: Update both callers — and drop the now-unused import (ruff F401)**

`pipeline.py` (~`:600`) — note the names in scope are on `wu`, not loose locals:

```python
        assert_unit_action_completeness(
            emitted=written,
            bronze_expected=sum(len(t) for t in _times.values()),
            action_times_by_period=_times,
            frame_window_by_period=_windows,
            unit_desc=f"{wu.provider}:{wu.match_id}:{wu.period}",
            buffer_s=_ACTION_TIME_BUFFER_SECONDS,
            is_slice=is_slice,
        )
```

`ingestion/action_context.py` (~`:1803`) — the driver's f-string is already correct:

```python
        assert_unit_action_completeness(
            emitted=written,
            bronze_expected=sum(len(t) for t in _times.values()),
            action_times_by_period=_times,
            frame_window_by_period=_frame_windows,
            unit_desc=f"{provider}:{match_id}:{period_filter}",
            buffer_s=_ACTION_TIME_BUFFER_SECONDS,
        )
```

Both modules still import `expected_actions_within_coverage` and no longer call it directly
(`pipeline.py:26-28`, `action_context.py:1793-1796`) → **remove those imports** or ruff fails Task 9.

> `_times` is the unit's period-filtered, NaN-dropped action times and is **not** frame-derived
> (`pipeline.py:589-599`) — so its total IS the bronze action count. No new query needed.
> **The check is still skipped entirely when `"timestamp" not in frames.columns`** (`pipeline.py:588`).
> That is now the only remaining silent skip; leave it, but name it in the ADR.

- [ ] **Step 5: Verify — the FULL action-context suite, not just this file**

```bash
uv run pytest src/tests/action_context/ -v
uv run pytest src/tests/ -q > /tmp/t7.txt 2>&1; echo "EXIT=$?" >> /tmp/t7.txt
```
`EXIT=0`. Any fixture raising `UNEXPLAINED` means a `run_work_unit` call site is missing `is_slice=True`.

---

## Task 8: D6 — pin the all-or-nothing unit write

**Files:** `src/tests/action_context/test_unit_write_atomicity.py` (create)

Use the stub ports that already exist in `src/tests/action_context/test_pipeline_dispatch.py`:
`_Actions` (`:19`), `_Xt` (`:24`), `_Meta` (`:29`), `_Sink` (`:34`), multi-batch `_Frames` (`:99-110`).
Do **not** hand-roll them — `run_work_unit` calls `xt.grid()` unconditionally (`pipeline.py:487`), so
`xt=None` raises `AttributeError` before any batch runs, and the Sink port is `write(wu, result_df)`.

- [ ] **Step 1: The test must DISCRIMINATE**

A `_boom` that raises on the **first** batch is vacuous: `sink.write` is unreachable either way, so the
test would pass identically even if someone moved the write *inside* the batch loop — the exact
regression it claims to pin. **Batch 1 must succeed and batch 2 must raise.**

```python
"""D6: a work unit is written ALL-OR-NOTHING. One raising batch => zero rows for the unit.

A DELIBERATE contract, not an accident: a partially-written unit is the silent-corruption case
ADR-040 exists to prevent -- downstream cannot tell "this half has 12 actions" from "this half HAD
550 and we lost 538". Failing loudly (D2) and writing nothing is the safer half of the trade.

Scope: this pins the LOCAL HEXAGON (run_work_unit). Production writes via _process_tracking_match
-> mapInPandas -> a single write_delta_table(replace_where=...), where all-or-nothing is Spark plus
the atomic Delta transaction -- a DIFFERENT mechanism.

If this fails, the blast radius of one bad batch has CHANGED. Decide that consciously; do not
paper over it.
"""
```

Build a two-batch `_Frames`, let batch 1 return a real row and batch 2 raise, assert the sink received
**nothing**. Name it `test_run_work_unit_writes_all_or_nothing`.

- [ ] **Step 2: Run it.** Expected: PASS (pins current behaviour). If it FAILS, the 550-row blast-radius
analysis in the spec is wrong — **report, do not "fix" the test.**

---

## Task 9: ADR, lessons, and the GS bug ticket

**Files:**
- Create: `docs/superpowers/adrs/ADR-067-velocity-delete-and-depend-and-unit-write-atomicity.md`
- Modify: `docs/engineering/conventions.md`

- [ ] **Step 1: Write ADR-067** (Nygard format, `docs/superpowers/adrs/ADR-TEMPLATE.md`). It MUST record:

- **Context:** two lakehouse copies of a silly-kicks function, welded by an AST-equality test, one of which
  dropped upstream's `len(x_vals) <= 1` guard → `skillcorner:1552423:2` wrote 0 of 550 actions inside a
  SUCCESS run (2026-07-11).
- **Decision 1 (D1):** delete both copies; SC + Metrica adopt the builder's `preprocess=` seam (the one GS
  and IDSSE already use). Precedent: ADR-055.
  - **We are adopting all THREE passes** (interpolate → smooth → derive), not two. Interpolation
    (linear, `max_gap` 0.6 s SC / 0.56 s Metrica) is NEW for SC/Metrica and is a deliberate choice.
  - **`PreprocessConfig.default()` only.** `is_default()` is flag-based; a hand-built config is passed
    through unpromoted and silently uses the universal 0.4 s window.
  - **⚠️ Interpolation moves POSITIONS, not just velocities.** `interpolate_frames` **writes back to `x`
    and `y`** (`_interpolation.py:97-98`), filling previously-NaN positions for gaps ≤ `max_gap`. So
    **every position-derived feature moves** for SC + Metrica — pitch control, DAS, ghost-GK, xT-GK,
    defensive line, team shape, nearest-defender distances — not merely the velocity columns. Rows that
    were previously NaN-excluded now **participate** in those computations. SkillCorner is *broadcast*
    tracking and therefore NaN-heavy by construction, so this is a large change for SC specifically.
    This is the single most under-predicted consequence of the migration: state it plainly so the Task 11
    golden diff is neither stopped unnecessarily nor rubber-stamped as "just the velocity change".
  - **Rows in groups shorter than the SG window will NOT move** — `_smoothing.py:30-31` passes short
    groups through un-smoothed, so `derive_velocities` runs `np.gradient` on raw positions,
    byte-identical to the old port. Unmoved rows are EXPECTED, not evidence of a failed migration.
  - **TC-1 is live** → `bronze.spadl_tracking_context` changes too and needs its own recompute.
  - `_AC_FRAME_COLUMNS` guards only SC/Metrica; GS and IDSSE never pass through `_finalize` and carry the
    scratch columns downstream today.
- **Decision 2 (D6):** a work unit is written all-or-nothing. Pinned in the local hexagon; production
  relies on Spark + the atomic Delta transaction.
- **Interim fences to remove later:** `is_slice` (the real fix is slicing fixture *actions* to the frame
  window); the `"timestamp" not in frames.columns` silent skip.
- **Consequences:** a comment claiming a copy "matches silly-kicks" is not a contract. Deletion is.

- [ ] **Step 2: File the GS `sg_window_seconds` bug SEPARATELY**

`pipeline.py:248` hand-builds `_PreprocessConfig(derive_velocity=True)`, which never promotes, so **GS runs
at the universal 0.4 s instead of its tuned 0.333 s**. Real, pre-existing, small. **Do not fix it in this
PR** — Task 12 Step 4 asserts GS does not move, and fixing this *would* move GS. Open a separate issue.

- [ ] **Step 3: Promote the process lessons** — append to `docs/engineering/conventions.md`:

```markdown
### Diagnosing a silent pipeline failure

- **Read the driver logs before theorising.** The AC drain swallows per-unit exceptions by design
  (`drain.py:170-181`); until D2 the task still exited 0. A raised guard and a silently-passing invariant
  therefore produce an IDENTICAL signature in the mart. The 2026-07-11 SkillCorner incident was diagnosed
  in minutes from `ac1_drain_unit_failed` in the for-each iteration logs — after a spec had reasoned at
  length from the mart alone and reached the wrong conclusion.
- **Prose is not a contract.** A stale docstring (`completeness.py` claimed "`0` skips the check" while the
  code skipped `< 10`) propagated straight into a design document; so did a comment claiming a copied
  function "matches silly-kicks" while omitting the guard upstream had added. Verify against the code, and
  prefer deletion over a comment promising parity.
```

---

## Task 10: Full verification, then commit + PR (APPROVAL GATES)

- [ ] **Step 1: Every gate. Capture exit codes — never `| tail`.**

```bash
uv run ruff check src/ scripts/ > /tmp/ruff.txt 2>&1;         echo "EXIT=$?" >> /tmp/ruff.txt
uv run ruff format --check src/ scripts/ > /tmp/fmt.txt 2>&1; echo "EXIT=$?" >> /tmp/fmt.txt
uv run pyright src/ > /tmp/pyright.txt 2>&1;                  echo "EXIT=$?" >> /tmp/pyright.txt
uv run pytest src/tests/ > /tmp/pytest.txt 2>&1;              echo "EXIT=$?" >> /tmp/pytest.txt
uv run lint-imports > /tmp/imports.txt 2>&1;                  echo "EXIT=$?" >> /tmp/imports.txt
```
All five must read `EXIT=0`. FULL suite — not a subset, not `--collect-only`.

- [ ] **Step 2: STOP — request explicit approval to commit.** Plan approval is NOT commit authority
(`CLAUDE.md`). Show the diff summary and wait.

- [ ] **Step 3: Commit (after approval)** — one commit: code + tests + spec + plan + ADR + conventions,
via `git commit -F <msg-file>`.

- [ ] **Step 4: STOP — separate approvals for push and for opening the PR.**

---

## Task 11: Post-merge operator sequence (NOT in the PR)

Do not start until post-merge CI is green (no wheel-consuming job before that).

- [ ] **Step 1:** bump `pyproject.toml` version, then `uv run python scripts/bump_wheel.py` (never hand-edit
the ~31 consumers). Deploy.

- [ ] **Step 2: Recompute action-context (SC + Metrica values changed).** This **subsumes the 550-row
backfill** — there is no separate backfill step. Follow `reference_ac_recompute_for_new_column`: wipe the
affected bronze slice → `run_now` `only=[preflight_action_context, compute_action_context]` → rebuild the
staging view → `scripts/rederive_synced_marts.py`.

**D2 is live: a failing unit now FAILS THE TASK.** That is the point. Read `ac1_drain_unit_failed` and fix
the cause; never re-run blindly.

- [ ] **Step 3: Recompute TC-1 as well.** Task 4 changed `bronze.spadl_tracking_context` velocities. Its
consumers (GK identity, IDSSE minutes) must not be left on mixed-vintage data.

- [ ] **Step 4: Verify the recovery**

```sql
select count(*) from soccer_analytics.dev_gold.fct_action_context c
join soccer_analytics.dev_gold.dim_matches dm on dm.match_key = c.match_key
where dm.provider = 'skillcorner' and dm.native_match_id = '1552423' and c.period_id = 2;
```
Expected: **550**. Then re-run the spec §2 gap query: the SkillCorner shortfall must be **0**, leaving only
the 891 GS extra-time actions (no ET frames — unrecoverable).

**Pin the snapshot** (spec §6) so the unexplained 899-vs-922 delta cannot silently re-open:

```sql
select data_source, min(_ingested_at) lo, max(_ingested_at) hi, count(*) rows
from soccer_analytics.bronze.spadl_actions
where data_source in ('gradientsports','skillcorner') group by data_source;
```
Record the bounds in ADR-067 as the basis for every figure quoted.

- [ ] **Step 5: Re-baseline BOTH goldens — deliberately**

```bash
uv run python scripts/build_ac1_mini_golden.py
uv run python scripts/build_ac1_full_golden.py
```

**Diff old vs new before accepting.** Predicted, from ADR-067:

- **SC + Metrica: POSITION-derived features move too, not only velocity-derived ones.** `interpolate_frames`
  writes back `x`/`y`, filling previously-NaN positions (gaps ≤ 0.6 s SC / 0.56 s Metrica), so pitch control,
  DAS, ghost-GK, xT-GK, defensive line, team shape and nearest-defender distances all shift — and rows that
  were NaN-excluded now participate. Expect a **wide** field of moves for SkillCorner especially (broadcast
  tracking is NaN-heavy). This is expected; it is not a failed migration.
- **Rows in groups shorter than the SG window do NOT move** (short groups pass through un-smoothed →
  `np.gradient` on raw positions → byte-identical to the old port). Unmoved rows are expected too.
- **GS and IDSSE must NOT move at all.** If they do, **STOP** — the most likely cause is someone "fixing"
  the GS `sg_window_seconds` promotion bug that was deliberately deferred (Task 9 Step 2).

A re-baseline must never paper over an unexpected delta. "It's all just the velocity change" is now a
**wrong** explanation — say what actually moved and why.

- [ ] **Step 6: Benchmarks.** The velocity path is on the tracking hot path; interpolation is NEW work for
SC/Metrica. Confirm no regression against the `CLAUDE.md` budgets: `uv run pytest src/tests/ -k benchmark -v`

---

## Task 12: D8 — mart gate, ONLY if planner parity holds

With D2 live this is a **backstop**, not the fix. Run its query read-only first.

The planner anti-joins **bronze** `spadl_action_context` at **match** grain; a naive gate would assert on
**gold** `fct_action_context` (incremental, INNER JOIN `dim_matches`) at **period** grain, on an independent
daily cron, while the drain legitimately rolls slices forward. Backlog, a rolled-over slice, or a
newly-ingested match would each turn it red on a **correct** state.

**If the gate cannot be made to agree with the planner's predicate on the current corpus — DROP IT and say
so.** A guard that cries wolf gets muted within a month, and D1 + D2 + D3 already close this incident.

---

## Deferred (explicitly NOT in this PR)

- **D9 — work-queue terminal state.** No update/merge method on `DeltaWorkQueue`; `skipped_no_frames` has no
  decision point (frames are the driving table); a status stamped only at unit end cannot answer "how far
  did the drain get" (needs `queued`/`running` + `started_at`). With D2 shipped this is operability, not
  detection.
- **Fixture extractor should slice actions to the frame window** — removes the need for `is_slice`.
- **GS `sg_window_seconds` 0.4 → 0.333** (the `is_default()` promotion bug) — would move GS values.
- **The 38-action residual** — characterise after D3 lands.
- **GS 10510 / 10511 extra time (891 actions)** — unrecoverable (no ET frames). Raise with the provider.
