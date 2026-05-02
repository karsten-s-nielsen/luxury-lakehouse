# SK3-MIG (Group A) — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Migrate the lakehouse to silly-kicks 3.0.0's corrected direction-of-play handling — bump pin, adapt call sites, force-rebuild `bronze.spadl_actions` and all coord-dependent downstream marts under the new converter behavior, wipe + recompute `expected_threat_grids`, and verify end-to-end with explicit provider-coverage and coord-correctness gates.

**Architecture:** silly-kicks 3.0.0 deletes the dual-mirror bug at the library layer (`_fix_direction_of_play` removed; per-converter `to_spadl_ltr(input_convention=...)` dispatch). Lakehouse-side, this is primarily a data-rebuild migration: pin bump + behaviour-preserving tracking-adapter pinning + ADR-012 §2 grace-period closure (remove the v2→v1 XGBoost feature-list fallback), then a one-shot orchestrated rebuild of `bronze.spadl_actions` and downstream marts via a new `scripts/sk3_mig_rebuild.py` script with hard verification gates at `scripts/sk3_mig_verify.py`. **Group B** (model retraining + HF dataset republishing) is explicitly deferred to a separate single-PR follow-up cycle.

**Tech Stack:** Python 3.10, silly-kicks 3.0.0, PySpark on Databricks Serverless, dbt, Delta Lake, MLflow, Lakebase synced tables, GitHub Actions CI, Terraform.

**Spec:** `docs/superpowers/specs/2026-05-02-sk3-mig-direction-of-play-migration-design.md`

**Branching policy:** ONE commit, ONE PR. The user's standing rule (`feedback_no_commits_without_explicit_approval.md` + `feedback_no_micro_approvals_in_execution.md`): execute Phases 1–3 through TDD without per-task approval gates; ONE explicit approval gate at the meaningful checkpoint (Task 17 commit + Task 18 PR creation + Task 21 merge).

**Live-system risk:** Phase 5 deletes from `bronze.spadl_actions`, `bronze.vaep_action_values`, and `dev_gold.expected_threat_grids` on dev_gold. Pre-deletion Delta versions captured in a JSON sidecar for rollback via `RESTORE TABLE ... TO VERSION AS OF`.

---

## File Structure

### Files Created

| Path | Responsibility |
|------|----------------|
| `scripts/sk3_mig_rebuild.py` | Orchestrator script issuing Databricks job triggers in dependency order (11 steps from spec §2). Idempotent + resumable via `--start-at <step>`. |
| `scripts/sk3_mig_verify.py` | Verification script — Gate A (provider coverage), Gate B (coord-correctness), xT sanity probe, §6 xG v1/v2 pre-flight. Re-runnable post-merge for regression checking. |
| `src/tests/test_sk3_coord_correctness.py` | Unit-test equivalent of Gate B. Synthetic 2-team fixture, asserts post-conversion per-team start_x split. |
| `docs/superpowers/adrs/ADR-022-direction-of-play-migration.md` | Lakehouse-side ADR documenting the bug, the silly-kicks 3.0.0 fix, the Group A/B split, and tracking-adapter `output_convention` posture. |
| `C:\Users\Karsten\.claude\projects\D--Development-karstenskyt--luxury-lakehouse\memory\project_sk3_mig_complete.md` | Post-merge memory entry capturing what shipped + what's queued for Group B. |

### Files Modified

| Path | What changes |
|------|--------------|
| `pyproject.toml` | `silly-kicks>=2.5.0,<3.0` → `silly-kicks>=3.0.0,<4` |
| `terraform/modules/workflows/main.tf` | silly-kicks pin in every env-spec block matching pyproject pin |
| Various PEP 723 scripts + `deploy.sh` (auto-discovered by `bump_wheel.py`) | Wheel version 0.3.29 → 0.3.30 |
| `src/ingestion/xg_model_v2.py` (lines 240-261) | Delete the v2→v1 XGBoost feature-list fallback (ADR-012 §2 grace-period closure) |
| `src/ingestion/pitch_control_batch.py`, `line_breaking.py`, `off_ball_xt.py`, `formations_efpi.py`, `formations_shape_graph.py`, `defcon_lite.py`, `elastic_sync.py` | Pin tracking-adapter calls to `output_convention="absolute_frame"` to preserve current behaviour |
| `.github/workflows/python-ci.yml`, `.github/workflows/dbt-live-ci.yml` | Add `SILLY_KICKS_ASSERT_INVARIANTS: "1"` to env block |
| `terraform/modules/workflows/main.tf` (env spec environment_variables) | Add `SILLY_KICKS_ASSERT_INVARIANTS = "1"` to all coord-dependent job env declarations |
| `src/tests/test_xg_model_v2.py` (or wherever the fallback path is exercised) | Update tests to assert raise on missing `feature_names` instead of asserting fallback to v1 features |
| `src/tests/test_spadl_vaep.py`, `test_spadl_vaep_writer_parity.py`, `test_silly_kicks_boundary.py` | Run under `SILLY_KICKS_ASSERT_INVARIANTS=1`; regenerate any fixture that produces different output post-3.0.0 |
| `docs/superpowers/adrs/ADR-012-training-to-production-delivery-hardening.md` (§2) | Record fallback removal — change the grace-period sentence to reference this PR |
| `TODO.md` | Add `XG1-RETIRE` row in On Deck section; update SK3-MIG row to point at this PR's spec/plan |
| `C:\Users\Karsten\.claude\projects\D--Development-karstenskyt--luxury-lakehouse\memory\project_silly_kicks_direction_of_play_bug.md` | Mark as historical; add pointer to `project_sk3_mig_complete.md` |
| `C:\Users\Karsten\.claude\projects\D--Development-karstenskyt--luxury-lakehouse\memory\MEMORY.md` | Index entry for `project_sk3_mig_complete.md` |

---

## Phase 1 — Code adaptation (local, TDD)

### Task 1: Branch setup + pre-flight verification

**Files:**
- No file changes — environment setup only

- [ ] **Step 1: Sync local main with origin**

```bash
git fetch origin
git checkout main
git pull --ff-only origin main
```

Per `feedback_pull_origin_main_before_branching.md`: branching from a stale local main produces an unresolvable PR base on GitHub (CI doesn't fire).

- [ ] **Step 2: Verify clean working tree (the spec is already committed in this state from the brainstorm — should be clean)**

```bash
git status
```

Expected: working tree clean, on `main`, up to date with `origin/main`. If the spec doc shows as untracked/modified, that's expected — it was written during the brainstorming phase.

- [ ] **Step 3: Create the feature branch**

```bash
git checkout -b feat/sk3-mig-direction-of-play
```

- [ ] **Step 4: Verify silly-kicks 3.0.0 is installable from PyPI**

```bash
curl -s --max-time 15 https://pypi.org/pypi/silly-kicks/json | python -c "import sys, json; d = json.load(sys.stdin); print('latest_pypi:', d.get('info', {}).get('version'))"
```

Expected output: `latest_pypi: 3.0.0` (or higher).

- [ ] **Step 5: Verify the local silly-kicks repo at `D:\Development\karstenskyt__silly-kicks` is at v3.0.0 + post-release CI follow-up**

```bash
cd "D:\Development\karstenskyt__silly-kicks" && git log --oneline -3
```

Expected first two lines:
```
fd742d8 ci: enable SILLY_KICKS_ASSERT_INVARIANTS=1 in test job (post-PR-S22 follow-up) (#29)
a1ebfa0 feat(spadl): direction-of-play correctness refactor -- silly-kicks 3.0.0 (PR-S22) (#28)
```

If your output differs, stop and reconcile — the silly-kicks parallel session may have advanced further.

---

### Task 2: Pin bump + wheel bump

**Files:**
- Modify: `pyproject.toml` (line 29)
- Auto-modify (via `bump_wheel.py`): various PEP 723 scripts in `scripts/` + `deploy.sh` + Terraform files containing the wheel filename

- [ ] **Step 1: Read the current silly-kicks pin to confirm baseline**

```bash
grep -nE '^\s*"silly-kicks' pyproject.toml
```

Expected: `29:    "silly-kicks>=2.5.0,<3.0",`

- [ ] **Step 2: Edit `pyproject.toml` line 29 — change the silly-kicks pin**

Change:
```toml
"silly-kicks>=2.5.0,<3.0",
```
to:
```toml
"silly-kicks>=3.0.0,<4",
```

- [ ] **Step 3: Bump the wheel version in `pyproject.toml`**

Find the `version = "0.3.29"` line in `[project]` and change to `version = "0.3.30"`.

```bash
grep -nE '^version' pyproject.toml | head -1
```

Expected after edit: `version = "0.3.30"`.

- [ ] **Step 4: Run `bump_wheel.py` to propagate the new wheel version to all consumer files**

```bash
uv run python scripts/bump_wheel.py
```

Expected: script reports a list of files updated (PEP 723 scripts, deploy.sh, Terraform). No errors.

- [ ] **Step 5: Verify `bump_wheel.py --check` is now clean**

```bash
uv run python scripts/bump_wheel.py --check
```

Expected: exit code 0 (no stale references).

- [ ] **Step 6: Sync local venv to the new dependency set**

```bash
uv sync
```

Expected: silly-kicks 3.0.0 installed. Check:

```bash
uv run python -c "import silly_kicks; print(silly_kicks.__version__)"
```

Expected: `3.0.0`.

- [ ] **Step 7: Verify the new silly-kicks 3.0.0 module surface is importable**

```bash
uv run python -c "from silly_kicks.spadl.orientation import InputConvention, to_spadl_ltr, POSSESSION_PERSPECTIVE, ABSOLUTE_FRAME_HOME_RIGHT; print('OK')"
```

Expected: `OK`. Confirms the new orientation module is reachable from our venv.

---

### Task 3: TF env-spec parity update

**Files:**
- Modify: `terraform/modules/workflows/main.tf` (multiple env-spec blocks; exact lines surfaced by the parity test)

- [ ] **Step 1: Run the parity test to surface the exact failing env-spec blocks**

```bash
uv run pytest src/tests/test_terraform_env_dep_parity.py -v
```

Expected: FAIL — pyproject pin is now `>=3.0.0,<4` but TF env specs still carry `>=2.5.0,<3.0`. The failure message lists each (env_key, package, pyproject_spec, tf_spec) row that has empty intersection.

- [ ] **Step 2: For each failing env-spec block reported, update the silly-kicks line in `terraform/modules/workflows/main.tf`**

Search for `silly-kicks` in `terraform/modules/workflows/main.tf`:

```bash
grep -nE 'silly-kicks' terraform/modules/workflows/main.tf
```

For each match, change `"silly-kicks>=2.5.0,<3.0"` to `"silly-kicks>=3.0.0,<4"`. There should be 3 occurrences per the OPT-1 cycle's prior parity-fix pattern; verify by counting matches before AND after the edit.

- [ ] **Step 3: Re-run the parity test — expect PASS**

```bash
uv run pytest src/tests/test_terraform_env_dep_parity.py -v
```

Expected: PASS.

---

### Task 4: silly-kicks 3.0.0 import-site verification

**Files:**
- Verify only (no edits expected): `src/ingestion/spadl_vaep.py`, `src/ingestion/spadl_conversion.py`, `src/ingestion/spadl_enrichments.py`, `src/ingestion/vaep_training.py`, `src/tests/test_silly_kicks_boundary.py`, `src/tests/test_spadl_vaep.py`, `src/tests/test_spadl_conversion.py`, `src/tests/test_spadl_enrichments.py`

silly-kicks 3.0.0's per-converter `convert_to_actions(adapted, home_team_id, ...)` signature is preserved (the `to_spadl_ltr` dispatch happens INSIDE each converter). `silly_kicks.vaep.features.gamestates(actions, nb_prev_actions=N)` signature is preserved. So our consumer-side call sites should require ZERO API changes — but we must verify, not assume.

- [ ] **Step 1: Grep for any reference to the deleted `_fix_direction_of_play` symbol**

```bash
grep -rnE "_fix_direction_of_play" src/ scripts/
```

Expected: no matches. If any match appears, replace with the appropriate new pattern (silly-kicks 3.0.0 made this internal — consumers should never have called it directly; if any consumer did, it was an error we now must remove).

- [ ] **Step 2: Grep for direct references to `play_left_to_right`**

```bash
grep -rnE "play_left_to_right" src/ scripts/
```

Expected: no matches in production code. If any test file references it as a fixture-construction helper, leave alone — silly-kicks 3.0.0 still provides the function as a public API per `tracking.utils`; only the implicit call inside `vaep.compute_features` was removed.

- [ ] **Step 3: Verify `convert_to_actions` call-site signatures are unchanged**

For each of the 4 sources, the call site in `src/ingestion/spadl_conversion.py` looks like (StatsBomb example, line 175):
```python
actions, _report = _spadl_sb.convert_to_actions(
    adapted,
    home_team_id,
    preserve_native=[...],
)
```

Check that this signature still accepts the same kwargs in silly-kicks 3.0.0:

```bash
uv run python -c "import inspect, silly_kicks.spadl.statsbomb as m; print(inspect.signature(m.convert_to_actions))"
```

Expected: signature includes `home_team_id` (positional or keyword) + `preserve_native=` kwarg. If the signature has changed (e.g., gained a required `input_convention` kwarg), you'll need to update each of the 4 call sites in `spadl_conversion.py` to pass the per-source `InputConvention` value:

| Source | Call site (file:line) | Add kwarg if required |
|---|---|---|
| StatsBomb | `spadl_conversion.py:175` | `input_convention=POSSESSION_PERSPECTIVE` |
| Wyscout | `spadl_conversion.py:522` (search for `_spadl_ws.convert_to_actions`) | `input_convention=POSSESSION_PERSPECTIVE` |
| Sportec/IDSSE | `spadl_conversion.py:903` (search for `_spadl_sportec.convert_to_actions`) | `input_convention=ABSOLUTE_FRAME_HOME_RIGHT` |
| Metrica | `spadl_conversion.py:1253` (search for `_spadl_metrica.convert_to_actions`) | `input_convention=ABSOLUTE_FRAME_HOME_RIGHT` |

If signatures are unchanged (most likely outcome — the silly-kicks 3.0.0 release notes say converters self-route), no edits are needed.

- [ ] **Step 4: Verify `gamestates` and feature functions preserve their signatures**

```bash
uv run python -c "import inspect, silly_kicks.vaep.features as fs; print('gamestates:', inspect.signature(fs.gamestates)); print('startlocation:', inspect.signature(fs.startlocation))"
```

If `gamestates` gained a required kwarg (e.g., `frames_convention`), update `src/ingestion/spadl_vaep.py:524` and `src/ingestion/vaep_training.py:70` accordingly. Most likely outcome: signatures unchanged for the standard SPADL path.

- [ ] **Step 5: Run the existing silly-kicks boundary test — discover any drift this way too**

```bash
uv run pytest src/tests/test_silly_kicks_boundary.py -v
```

Expected: tests pass (boundary test asserts surface-level imports). If failures occur, update the test to match the new boundary surface and proceed.

---

### Task 5: Tracking-adapter audit + opt-out for `output_convention="absolute_frame"`

**Files:**
- Modify (potentially): `src/ingestion/pitch_control_batch.py`, `src/ingestion/line_breaking.py`, `src/ingestion/off_ball_xt.py`, `src/ingestion/formations_efpi.py`, `src/ingestion/formations_shape_graph.py`, `src/ingestion/defcon_lite.py`, `src/ingestion/elastic_sync.py`

silly-kicks 3.0.0 makes tracking adapters output SPADL-LTR by default. We preserve current behaviour by passing `output_convention="absolute_frame"` to every tracking-adapter call. Per the spec, default in this PR is to preserve current behaviour; LTR migration is a follow-up PR if any consumer would benefit.

- [ ] **Step 1: Find every call to a silly-kicks tracking adapter**

```bash
grep -rnE 'silly_kicks\.tracking|from silly_kicks\.tracking' src/ingestion/
```

For each match, identify whether it calls a tracking-adapter function that now accepts `output_convention=`. (Per the silly-kicks 3.0.0 commit message, the kwarg is added to `tracking/kloppy.py`, `tracking/pff.py`, `tracking/sportec.py`.)

- [ ] **Step 2: Inspect the new kwarg surface**

```bash
uv run python -c "import inspect; from silly_kicks.tracking import kloppy, sportec, pff; [print(name + ':', inspect.signature(getattr(m, fname))) for m, name in [(kloppy, 'kloppy'), (sportec, 'sportec'), (pff, 'pff')] for fname in dir(m) if not fname.startswith('_') and callable(getattr(m, fname))]"
```

Expected: each public converter shows an `output_convention=` kwarg with default value. Check the default — silly-kicks 3.0.0 may default to `"ltr"` (silently breaking) OR may default to `"absolute_frame"` with `DeprecationWarning` (per spec §1.3). The plan diverges:

- If default is `"absolute_frame"` with `DeprecationWarning`: pin every call site to the explicit kwarg `output_convention="absolute_frame"` to silence the warning AND make the intent explicit. This also future-proofs against the eventual default flip.
- If default is `"ltr"` (silent breaking change): pin every call site to `output_convention="absolute_frame"` to preserve current behaviour. Without this, consumers receive transposed coords and silently produce wrong analytics output.

In either case, the action is the same: pin every call site explicitly.

- [ ] **Step 3: For each call site found in Step 1, edit to pass `output_convention="absolute_frame"`**

Example transformation:
```python
# Before
frames = silly_kicks.tracking.kloppy.convert(raw_frames, ...)
# After
frames = silly_kicks.tracking.kloppy.convert(raw_frames, ..., output_convention="absolute_frame")
```

Every call site gets the kwarg. Verify after edits:

```bash
grep -rnE 'silly_kicks\.tracking.*\(' src/ingestion/ | grep -v 'output_convention'
```

Expected: no matches (every call now passes the kwarg).

- [ ] **Step 4: Run the tracking-related unit tests to confirm no behaviour shift**

```bash
uv run pytest src/tests/ -k "tracking or pitch_control or line_breaking or off_ball" -v
```

Expected: PASS. If any test fails, the cause is either (a) a fixture that pre-dated absolute-frame-default behavior — regenerate, OR (b) a real regression — investigate before proceeding.

---

### Task 6: Set `SILLY_KICKS_ASSERT_INVARIANTS=1` in CI + production

**Files:**
- Modify: `.github/workflows/python-ci.yml`, `.github/workflows/dbt-live-ci.yml`
- Modify: `terraform/modules/workflows/main.tf` — every coord-dependent Databricks job environment_variables block

- [ ] **Step 1: Identify the env block in `python-ci.yml`**

```bash
grep -nE '^\s*env:' .github/workflows/python-ci.yml
```

Find the appropriate job-level `env:` block (top-level or inside a job). Add:

```yaml
env:
  SILLY_KICKS_ASSERT_INVARIANTS: "1"
```

If a job-level env block already exists, append the new key. Otherwise, add a new env block.

- [ ] **Step 2: Same for `dbt-live-ci.yml`**

```bash
grep -nE '^\s*env:' .github/workflows/dbt-live-ci.yml
```

Repeat the addition.

- [ ] **Step 3: Identify Databricks job environment_variables blocks in `terraform/modules/workflows/main.tf`**

```bash
grep -nE 'environment_variables\s*=' terraform/modules/workflows/main.tf
```

For every coord-dependent job (any job that runs `compute_spadl_vaep`, `compute_xg_predictions`, `compute_xg_predictions_v2`, `compute_defcon_lite`, `compute_pausa`, `compute_obso`, `compute_player_embeddings_*`, `compute_expected_threat`, F2V batch inference workflows), add:

```hcl
environment_variables = merge(
  <existing map or empty {}>,
  {
    SILLY_KICKS_ASSERT_INVARIANTS = "1"
  }
)
```

If the block already merges multiple maps, slot the new key into the appropriate map. Match the existing formatting style.

- [ ] **Step 4: Run `terraform fmt` to normalize formatting**

```bash
cd terraform && terraform fmt -recursive && cd ..
```

Expected: no errors. Files reformatted in place if needed.

- [ ] **Step 5: Run `terraform validate`**

```bash
cd terraform/environments/dev && terraform validate && cd ../../..
```

Expected: success.

---

### Task 7: Remove `xg_model_v2.py` legacy fallback (ADR-012 §2 grace-period closure)

**Files:**
- Modify: `src/ingestion/xg_model_v2.py` lines 240-261
- Modify: `src/tests/test_xg_model_v2.py` (or wherever the fallback path is exercised — discover via grep)

This is a strict TDD task: write the failing test first, then delete the fallback code, then update or delete any test that previously asserted the fallback path.

- [ ] **Step 1: Find existing tests that exercise the fallback path**

```bash
grep -rnE "xgb_features|legacy.*envelope|fallback.*v1|v2.*fallback" src/tests/
```

Note any test that constructs a v2 envelope WITHOUT `feature_names` and asserts that inference falls back to v1's feature list.

- [ ] **Step 2: Write the failing test asserting RuntimeError on missing `feature_names`**

Add a new test to `src/tests/test_xg_model_v2.py` (create the file if it doesn't exist):

```python
import json
import pytest


def test_v2_inference_raises_on_envelope_missing_feature_names():
    """ADR-012 §2 grace-period closure (SK3-MIG, 2026-05-02).

    The v2 → v1 XGBoost feature-list fallback was removed. v2 envelopes
    that lack `feature_names` must now raise RuntimeError with a clear
    pointer to the retraining script.
    """
    legacy_envelope_bytes = json.dumps({
        "tabular_dim": 41,
        # Note: no "feature_names" key — this is the legacy shape.
        # In a real envelope these would be present; we only need
        # the absence to trigger the new strict path.
    }).encode("utf-8")

    # Import the module-level helper that performs the strict envelope read.
    # Concrete import path depends on the refactor in Step 4 — the function
    # being tested is the new envelope parser. If the parser is inlined in
    # the UDF closure, expose it as a module-level function for testability.
    from ingestion.xg_model_v2 import _parse_v2_envelope_features

    with pytest.raises(RuntimeError, match="missing 'feature_names'"):
        _parse_v2_envelope_features(legacy_envelope_bytes)
```

- [ ] **Step 3: Run the new test — expect FAIL**

```bash
uv run pytest src/tests/test_xg_model_v2.py::test_v2_inference_raises_on_envelope_missing_feature_names -v
```

Expected: FAIL — either `_parse_v2_envelope_features` doesn't exist yet, or it returns the v1 fallback list instead of raising.

- [ ] **Step 4: Refactor the inline envelope-parsing block into a module-level helper, then implement the strict semantics**

In `src/ingestion/xg_model_v2.py`, replace lines 240-261 (the existing fallback block inside the UDF closure) with the new helper + a clean call. New module-level helper:

```python
def _parse_v2_envelope_features(v2_weights_bytes: bytes) -> tuple[list[str], int]:
    """Parse v2 weights envelope and return (feature_names, tabular_dim).

    ADR-012 §2 grace-period closure (SK3-MIG, 2026-05-02): legacy envelopes
    without `feature_names` raise RuntimeError. Inference must run on weights
    produced by 2026-04-22+ training (PR #177 `ecf2551` and later).
    """
    import json

    envelope = json.loads(v2_weights_bytes.decode("utf-8"))
    v2_features = envelope.get("feature_names")
    if not v2_features:
        raise RuntimeError(
            "v2 weights envelope is missing 'feature_names'. "
            "ADR-012 §2 grace-period removal — refresh @Champion via "
            "scripts/train_xg_v2_hf.py before re-running."
        )
    v2_tabular_dim = envelope["tabular_dim"]
    if len(v2_features) != v2_tabular_dim:
        raise AssertionError(
            f"v2 envelope is inconsistent: feature_names={len(v2_features)} "
            f"!= tabular_dim={v2_tabular_dim}. Envelope corrupted at training time."
        )
    return list(v2_features), v2_tabular_dim
```

Then in the UDF closure (around the old line 240-255), replace the legacy block:

```python
# OLD lines 240-255 — DELETE ENTIRELY
cc = next(iter(cache["xgboost"].calibrated_classifiers_))
xgb_estimator = cc.estimator  # type: ignore[union-attr]
cache["xgb_features"] = list(xgb_estimator.get_booster().feature_names)
v2_envelope = _json.loads(v2_weights_bytes.decode("utf-8"))
v2_features = v2_envelope.get("feature_names")
cache["v2_features"] = list(v2_features) if v2_features else cache["xgb_features"]
```

with:

```python
v2_features, _v2_dim = _parse_v2_envelope_features(v2_weights_bytes)
cache["v2_features"] = v2_features
```

The XGBoost model is still loaded (it's used elsewhere in the UDF for tabular feature extraction); only the `xgb_features` extraction-as-fallback-source is gone.

- [ ] **Step 5: Run the new test — expect PASS**

```bash
uv run pytest src/tests/test_xg_model_v2.py::test_v2_inference_raises_on_envelope_missing_feature_names -v
```

Expected: PASS.

- [ ] **Step 6: Update or delete any existing test that asserted the fallback path**

For each test identified in Step 1:
- If the test ASSERTED that legacy envelopes fall back to v1's xgb_features → DELETE the test (it asserted the now-removed behavior).
- If the test just used a legacy envelope as a generic fixture without asserting fallback specifically → REGENERATE the fixture to include `feature_names` matching `tabular_dim`.

- [ ] **Step 7: Run the entire xg_model_v2 test file**

```bash
uv run pytest src/tests/test_xg_model_v2.py -v
```

Expected: all PASS. If failures, debug case-by-case before proceeding.

---

### Task 8: Write new invariant unit test (spec §5.2)

**Files:**
- Create: `src/tests/test_sk3_coord_correctness.py`

This is the unit-test equivalent of Gate B. Catches any future regression that re-introduces a stray mirror anywhere in our call chain.

- [ ] **Step 1: Look at the existing silly-kicks boundary test for fixture conventions**

```bash
head -100 src/tests/test_silly_kicks_boundary.py
```

Note the fixture-construction style — particularly how synthetic StatsBomb-shape events are built. We'll mirror that style.

- [ ] **Step 2: Write the new test file**

Create `src/tests/test_sk3_coord_correctness.py`:

```python
"""SK3-MIG invariant — post-conversion, per-team avg start_x must split.

Regression test that catches any future re-introduction of a stray
direction-of-play mirror anywhere in the conversion call chain. The
post-silly-kicks-3.0.0 invariant: in canonical SPADL LTR (which all 4
of our converters now produce), team A's actions cluster at high-x
and team B's actions cluster at low-x (or vice versa) — NOT both
clustered at the same end.

Pre-3.0.0 broken state was: StatsBomb / Wyscout converters mirrored
away-team to wrong end, so both teams' shots clustered at the same
end (high-x for both). This test would have caught that bug.
"""

from __future__ import annotations

import pandas as pd
import pytest


def _make_synthetic_statsbomb_events() -> pd.DataFrame:
    """Build a 10-event synthetic StatsBomb DataFrame: 5 shots from team A
    (a home team attacking right, all near opponent goal at x=110/120),
    5 shots from team B (an away team attacking left in possession-perspective
    — also near opponent goal at x=110/120 in StatsBomb's raw frame).

    Post-conversion to canonical SPADL LTR (105m), team A actions should
    map to high-x (~95-100) and team B actions should map to low-x (~5-10).
    """
    rows = []
    for i in range(5):
        rows.append({
            "game_id": 1,
            "event_id": f"home-shot-{i}",
            "period_id": 1,
            "timestamp": f"00:0{i}:00.000",
            "team_id": 100,  # home
            "player_id": 1000 + i,
            "type_name": "Shot",
            "location": [110.0 + i * 0.5, 40.0],
            "extra": {"shot": {"end_location": [120.0, 40.0], "outcome": {"name": "Goal"}}},
        })
    for i in range(5):
        rows.append({
            "game_id": 1,
            "event_id": f"away-shot-{i}",
            "period_id": 1,
            "timestamp": f"00:1{i}:00.000",
            "team_id": 200,  # away — possession-perspective also at x=110/120
            "player_id": 2000 + i,
            "type_name": "Shot",
            "location": [110.0 + i * 0.5, 40.0],
            "extra": {"shot": {"end_location": [120.0, 40.0], "outcome": {"name": "Goal"}}},
        })
    df = pd.DataFrame(rows)
    df["home_team_id"] = 100
    df["match_id"] = 1
    df["competition_id"] = 1
    df["season_id"] = 1
    return df


def test_statsbomb_post_conversion_per_team_x_splits():
    """Team A and team B must have post-conversion start_x means at OPPOSITE ends."""
    import silly_kicks.spadl.statsbomb as sb
    from silly_kicks.spadl.orientation import POSSESSION_PERSPECTIVE  # noqa: F401  -- referenced for documentation; converter self-routes

    events = _make_synthetic_statsbomb_events()
    actions, _report = sb.convert_to_actions(events, home_team_id=100)

    home_avg_x = actions.loc[actions["team_id"] == 100, "start_x"].mean()
    away_avg_x = actions.loc[actions["team_id"] == 200, "start_x"].mean()

    # Canonical SPADL LTR: pitch length 105. One team at high-x, other at low-x.
    pitch_len = 105.0
    pitch_mid = pitch_len / 2.0
    home_high = home_avg_x > pitch_mid
    away_high = away_avg_x > pitch_mid

    assert home_high != away_high, (
        f"Per-team start_x failed to split: home_avg_x={home_avg_x:.2f}, "
        f"away_avg_x={away_avg_x:.2f}. Both teams cluster on the same end of "
        f"the pitch. This is the SK3-MIG bug recurring — direction-of-play "
        f"mirror is broken somewhere in the conversion chain."
    )
```

- [ ] **Step 3: Run the new test — expect PASS (because silly-kicks 3.0.0 is correct)**

```bash
uv run pytest src/tests/test_sk3_coord_correctness.py -v
```

Expected: PASS. If FAIL, this would mean either (a) silly-kicks 3.0.0 has a bug we missed, or (b) our synthetic fixture is malformed — investigate before proceeding.

---

### Task 9: Update existing unit tests for `SILLY_KICKS_ASSERT_INVARIANTS=1`

**Files:**
- Modify (if any test fails): `src/tests/test_spadl_vaep.py`, `src/tests/test_spadl_vaep_writer_parity.py`, `src/tests/test_silly_kicks_boundary.py`, `src/tests/test_spadl_conversion.py`, `src/tests/test_spadl_enrichments.py`

- [ ] **Step 1: Run the full SPADL/VAEP test suite WITHOUT the strict env var to establish baseline**

```bash
uv run pytest src/tests/test_spadl_vaep.py src/tests/test_spadl_vaep_writer_parity.py src/tests/test_silly_kicks_boundary.py src/tests/test_spadl_conversion.py src/tests/test_spadl_enrichments.py -v
```

Expected: all PASS (they should pass on silly-kicks 3.0.0's standard path).

If any test fails, the most likely cause is a fixture generated under the old converter behavior that produces different output post-3.0.0 (especially IDSSE/Metrica fixtures, since the absolute-frame providers received the wrong-mirror in pre-3.0.0). For each failure:

1. Diff the failing assertion's expected vs actual values.
2. If the expected values look like the old (broken) behavior (e.g., away-team shots at low-x for IDSSE), regenerate the fixture by re-running the fixture-build path.
3. If the expected values look like the new (correct) behavior but the actual is wrong, you've found a real regression — STOP and investigate.

- [ ] **Step 2: Run the same test suite WITH `SILLY_KICKS_ASSERT_INVARIANTS=1` to confirm strict mode passes**

```bash
SILLY_KICKS_ASSERT_INVARIANTS=1 uv run pytest src/tests/test_spadl_vaep.py src/tests/test_spadl_vaep_writer_parity.py src/tests/test_silly_kicks_boundary.py src/tests/test_spadl_conversion.py src/tests/test_spadl_enrichments.py -v
```

(On Windows PowerShell, use: `$env:SILLY_KICKS_ASSERT_INVARIANTS = "1"; uv run pytest ...; Remove-Item Env:SILLY_KICKS_ASSERT_INVARIANTS`)

Expected: all PASS. If any test fails ONLY in strict mode, the underlying call site is feeding silly-kicks input that violates the canonical convention. Either fix the call site OR fix the fixture.

---

### Task 10: Local CI green (pytest + ruff + pyright)

**Files:**
- No file changes (just verification)

- [ ] **Step 1: Run ruff lint**

```bash
uv run ruff check src/ scripts/
```

Expected: 0 violations.

- [ ] **Step 2: Run ruff format check**

```bash
uv run ruff format --check src/ scripts/
```

Expected: 0 violations. If reformatting needed, run `uv run ruff format src/ scripts/` and re-check.

- [ ] **Step 3: Run pyright (basic mode)**

```bash
uv run pyright src/
```

Expected: 0 errors.

- [ ] **Step 4: Run the full unit test suite WITH strict mode**

```bash
SILLY_KICKS_ASSERT_INVARIANTS=1 uv run pytest src/tests/ -v
```

Expected: all PASS. This is the local pre-flight before pushing.

---

## Phase 2 — Verification + orchestration scripts (local TDD)

### Task 11: Write `scripts/sk3_mig_verify.py`

**Files:**
- Create: `scripts/sk3_mig_verify.py`

Implements all 3 verification gates + the §6 xG v1/v2 dimension pre-flight + the xT sanity probe. Exit code 0 = clean. Re-runnable post-merge for regression checking.

- [ ] **Step 1: Look at the existing SDK-over-SQL-connector pattern**

```bash
grep -rnE 'WorkspaceClient|statement_execution' scripts/ | head -20
```

Per memory `reference_sdk_over_sql_connector.md`, prefer `WorkspaceClient.statement_execution` over `databricks-sql-connector` for any CI-side SQL execution.

- [ ] **Step 2: Sketch the script structure**

The script has 5 callable check functions + a `main()` that runs them in order. Each check function returns `(passed: bool, diagnostic: str)`.

```python
"""SK3-MIG verification — Group A merge gates + §6 xG pre-flight.

Run modes:
    --pre-flight          Run only Step-7 xG v1/v2 dim check (called by orchestrator before inference triggers)
    --gate-a              Run Gate A (provider coverage)
    --gate-b              Run Gate B (coord-correctness)
    --xt-sanity           Run xT sanity probe (informational)
    --full                Run all gates + sanity probes; exit 1 on any gate failure (default)
    --report-md           Emit Markdown table for PR body insertion (use with --full)

Examples::

    # Pre-rebuild snapshot capture (orchestrator step 1)
    uv run python scripts/sk3_mig_verify.py --gate-a --output sk3_mig_pre.json

    # Post-rebuild full verification (orchestrator step 11 + PR-body capture)
    uv run python scripts/sk3_mig_verify.py --full --report-md
"""
```

- [ ] **Step 3: Implement Gate A (provider coverage)**

```python
def gate_a_provider_coverage(client, *, table: str, expected_sources: list[str], pre_counts: dict[str, int] | None = None, tolerance_pct: float = 0.5) -> tuple[bool, str]:
    """Assert all expected data_source values present in `table` with row counts within ±tolerance_pct of pre_counts."""
    sql = f"SELECT data_source, COUNT(*) AS rows, COUNT(DISTINCT match_id) AS matches FROM {table} GROUP BY data_source ORDER BY data_source"
    rows = _execute_query(client, sql)
    actual = {r["data_source"]: r["rows"] for r in rows}

    diagnostic_lines = [f"Gate A — {table}", f"  expected sources: {expected_sources}", f"  observed: {sorted(actual.keys())}"]
    missing = sorted(set(expected_sources) - set(actual.keys()))
    if missing:
        diagnostic_lines.append(f"  MISSING SOURCES: {missing}")
        return False, "\n".join(diagnostic_lines)

    if pre_counts is not None:
        max_drift = 0.0
        worst_source = ""
        for src in expected_sources:
            pre = pre_counts.get(src, 0)
            post = actual.get(src, 0)
            if pre == 0:
                continue
            drift = abs(post - pre) / pre * 100
            if drift > max_drift:
                max_drift = drift
                worst_source = src
            diagnostic_lines.append(f"  {src}: pre={pre:,} post={post:,} drift={drift:.2f}%")
        if max_drift > tolerance_pct:
            diagnostic_lines.append(f"  FAIL: {worst_source} drift {max_drift:.2f}% exceeds tolerance {tolerance_pct}%")
            return False, "\n".join(diagnostic_lines)
    else:
        for src, post in actual.items():
            diagnostic_lines.append(f"  {src}: rows={post:,}")

    return True, "\n".join(diagnostic_lines)
```

- [ ] **Step 4: Implement Gate B (coord-correctness)**

```python
def gate_b_coord_correctness(client, *, table: str = "dev_gold.fct_action_values", max_imbalance: float = 0.10) -> tuple[bool, str]:
    sql = f"""
    SELECT data_source,
           COUNT(*) AS pairs,
           SUM(CASE WHEN avg_x > 52.5 THEN 1 ELSE 0 END) AS high_teams,
           SUM(CASE WHEN avg_x <= 52.5 THEN 1 ELSE 0 END) AS low_teams
    FROM (
        SELECT match_id, team_id, data_source, AVG(start_x) AS avg_x, COUNT(*) AS n
        FROM {table}
        WHERE action_type IN ('shot', 'shot_penalty', 'shot_freekick')
        GROUP BY match_id, team_id, data_source
        HAVING COUNT(*) >= 3
    ) AS per_team
    GROUP BY data_source
    """
    rows = _execute_query(client, sql)
    diagnostic_lines = [f"Gate B — {table} per-source per-team shot-x split"]
    overall_pass = True
    for r in rows:
        src = r["data_source"]
        pairs = r["pairs"]
        high = r["high_teams"]
        low = r["low_teams"]
        if pairs == 0:
            diagnostic_lines.append(f"  {src}: no pairs (skipped)")
            continue
        imbalance = abs(high - low) / pairs
        verdict = "PASS" if imbalance <= max_imbalance else "FAIL"
        if verdict == "FAIL":
            overall_pass = False
        diagnostic_lines.append(f"  {src}: pairs={pairs} high={high} low={low} imbalance={imbalance:.3f} -> {verdict}")
    return overall_pass, "\n".join(diagnostic_lines)
```

- [ ] **Step 5: Implement the xT sanity probe (informational, never fails)**

```python
def xt_sanity_probe(client, *, table: str = "dev_gold.expected_threat_grids") -> tuple[bool, str]:
    """Informational: report global xT grid (max, sum, monotonic-in-x check)."""
    sql = f"SELECT * FROM {table} WHERE competition_id IS NULL"  # global row
    rows = _execute_query(client, sql)
    if not rows:
        return True, f"xT sanity: {table} has no global row (need_global=True will trigger on next compute_expected_threat run)"
    # Inspect the grid column (likely an array<array<double>>); compute max, sum, monotonicity
    # ...implementation depends on the actual schema of expected_threat_grids; consult dbt model first
    raise NotImplementedError("Implement based on dev_gold.expected_threat_grids schema — see dbt_project/models/marts/")
```

(Leave this as `NotImplementedError` initially; flesh out in Step 9 below by inspecting the actual schema.)

- [ ] **Step 6: Implement the §6 xG v1/v2 pre-flight (3 checks)**

```python
def xg_dimension_preflight(client) -> tuple[bool, str]:
    """§6 of spec — 3 checks before triggering compute_xg_predictions / _v2."""
    diagnostic_lines = ["xG v1/v2 dimension pre-flight"]

    # Check 1: artifact resolution + feature-list extraction
    try:
        v1_features, v1_run_id = _resolve_v1_xgboost_features(client)
        v2_features, v2_tabular_dim, v2_run_id = _resolve_v2_envelope_features(client)
    except Exception as exc:  # noqa: BLE001 -- top-level pre-flight; re-raise after diagnostic capture
        return False, f"xG pre-flight artifact resolution FAILED: {exc!r}"

    diagnostic_lines.append(f"  Check 1: v1_features={len(v1_features)} v2_features={len(v2_features)} v2_tabular_dim={v2_tabular_dim}")
    diagnostic_lines.append(f"    v1 run_id: {v1_run_id}")
    diagnostic_lines.append(f"    v2 run_id: {v2_run_id}")

    # Check 2: envelope consistency (post-§1.5 — collapsed to single assertion)
    if len(v2_features) != v2_tabular_dim:
        return False, "\n".join(diagnostic_lines + [
            f"  Check 2: FAIL — v2 envelope inconsistent: feature_names={len(v2_features)} != tabular_dim={v2_tabular_dim}"
        ])
    diagnostic_lines.append(f"  Check 2: PASS — feature_names matches tabular_dim")

    # Soft warning: v1 vs v2 feature divergence
    if set(v1_features) != set(v2_features):
        only_v1 = sorted(set(v1_features) - set(v2_features))
        only_v2 = sorted(set(v2_features) - set(v1_features))
        diagnostic_lines.append(f"  WARN: v1 vs v2 feature divergence — only_v1={only_v1[:5]}... only_v2={only_v2[:5]}...")

    # Check 3: end-to-end smoke inference on synthetic input
    try:
        _smoke_inference_v1(v1_features)
        _smoke_inference_v2(v2_features, v2_tabular_dim)
    except Exception as exc:  # noqa: BLE001 -- pre-flight smoke
        return False, "\n".join(diagnostic_lines + [
            f"  Check 3: FAIL — smoke inference raised: {exc!r}"
        ])
    diagnostic_lines.append(f"  Check 3: PASS — smoke inference succeeded for both v1 and v2")

    return True, "\n".join(diagnostic_lines)
```

The helpers `_resolve_v1_xgboost_features`, `_resolve_v2_envelope_features`, `_smoke_inference_v1`, `_smoke_inference_v2` are concrete and modest:

- `_resolve_v1_xgboost_features`: `mlflow.xgboost.load_model("models:/<v1_fqn>@Champion")` then `.get_booster().feature_names`
- `_resolve_v2_envelope_features`: download `models:/<v2_fqn>@Champion` artifact bytes, parse JSON envelope, return `feature_names + tabular_dim + run_id`
- `_smoke_inference_v1`: build a 1-row pandas DataFrame with all v1_features as columns set to neutral defaults (zeros + sensible sentinels for categorical) → `predict_proba` → assert returns shape (1, 2)
- `_smoke_inference_v2`: build a 1-row pandas DataFrame matching v2's tabular features + a synthetic 1-player `shot_freeze_frame` JSON → invoke the v2 UDF on a single-element pandas group → assert returns 1 row with `xg_set_encoder` non-NaN

If the smoke implementations are nontrivial, factor each into its own ~30-line helper.

- [ ] **Step 7: Implement `main()` with arg parsing**

```python
def main() -> int:
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--pre-flight", action="store_true")
    parser.add_argument("--gate-a", action="store_true")
    parser.add_argument("--gate-b", action="store_true")
    parser.add_argument("--xt-sanity", action="store_true")
    parser.add_argument("--full", action="store_true")
    parser.add_argument("--output", type=str, default=None, help="JSON output path (Gate A snapshot capture)")
    parser.add_argument("--report-md", action="store_true", help="Emit Markdown summary for PR body")
    args = parser.parse_args()

    from databricks.sdk import WorkspaceClient
    client = WorkspaceClient()

    results = []  # list of (name, passed, diagnostic)
    if args.full or args.gate_a:
        # ...wire each gate; write JSON sidecar if --output set
        ...
    # ...similar for other modes

    overall_pass = all(r[1] for r in results)
    for name, passed, diagnostic in results:
        verdict = "PASS" if passed else "FAIL"
        print(f"=== {name} [{verdict}] ===")
        print(diagnostic)
        print()
    if args.report_md:
        print(_render_markdown_report(results))
    return 0 if overall_pass else 1


if __name__ == "__main__":
    import sys
    sys.exit(main())
```

- [ ] **Step 8: Implement `_execute_query` helper using SDK**

```python
def _execute_query(client, sql: str) -> list[dict]:
    """Run SQL via SDK, return list of dict rows."""
    from databricks.sdk.service.sql import StatementState

    warehouse_id = _get_warehouse_id(client)  # standard helper — copy from existing scripts
    response = client.statement_execution.execute_statement(
        warehouse_id=warehouse_id,
        statement=sql,
        wait_timeout="30s",
    )
    # ...poll on QUEUED/RUNNING; raise on FAILED; return parsed rows on FINISHED
```

Use the existing pattern from `scripts/maintain_synced_tables.py` or similar — copy verbatim to avoid SDK-pattern drift.

- [ ] **Step 9: Implement `xt_sanity_probe` properly (un-stub it)**

```bash
cat dbt_project/models/marts/expected_threat_grids.sql | head -40
```

Inspect the schema, then compute max + sum + monotonic-in-x check. Implementation depends on whether the grid is stored as an `array<array<double>>` column or as one row per (zone_id, value).

- [ ] **Step 10: Run the verify script in pre-flight mode against current dev_gold (does NOT modify state)**

```bash
uv run python scripts/sk3_mig_verify.py --pre-flight
```

Expected: pre-flight PASSES (we already have v2 envelope with feature_names per memory). If FAIL, the script has a bug — fix before proceeding to orchestrator.

- [ ] **Step 11: Run Gate A in snapshot mode against current dev_gold to capture pre-rebuild counts**

```bash
uv run python scripts/sk3_mig_verify.py --gate-a --output sk3_mig_pre.json
```

Expected: writes `sk3_mig_pre.json` with per-source row counts for `bronze.spadl_actions`, `bronze.vaep_action_values`, `dev_gold.fct_action_values`. This file is consumed by the orchestrator in Phase 5.

---

### Task 12: Write `scripts/sk3_mig_rebuild.py` orchestrator

**Files:**
- Create: `scripts/sk3_mig_rebuild.py`

Implements the 11-step sequencing from spec §2. Idempotent + resumable via `--start-at <step>`. Captures Delta versions in JSON sidecar before any DELETE.

- [ ] **Step 1: Inspect existing Databricks-job-trigger patterns**

```bash
grep -rnE 'jobs\.run_now|jobs.*trigger|run_job' scripts/
```

Find a recent script that triggers Databricks jobs and waits for completion (e.g., `scripts/dbt_build_and_refresh.py` or similar). Use it as the structural template — match the SDK call patterns + the polling style.

- [ ] **Step 2: Sketch the script structure**

```python
"""SK3-MIG rebuild orchestrator — 11-step force-rebuild of bronze.spadl_actions
and downstream marts under silly-kicks 3.0.0.

Run modes:
    --start-at <step>     Resume from step N (idempotent, each step re-runnable)
    --dry-run             Print steps without executing
    --confirm-deletes     Required for steps that DELETE production data

Steps:
    0. Pre-flight: silly-kicks 3.0.0 active
    1. Capture pre-rebuild Delta versions to sk3_mig_rollback.json
    2. DELETE bronze.spadl_actions + bronze.vaep_action_values (full)
    3. Trigger compute_spadl_vaep
    4. Gate A — provider-coverage verification
    5. 3-stage dbt build (input → intermediate → output)
    6. DELETE dev_gold.expected_threat_grids → trigger compute_expected_threat
    7. xG v1/v2 dimension pre-flight (verify gate before any inference triggers)
    8. Trigger coord-dependent inference workflows in dependency order
    9. Final 3-stage dbt build (refresh marts dependent on Step 8 bronze writes)
    10. Refresh Lakebase synced tables + restore custom indexes
    11. Final coord-correctness gate B + xT sanity probe
"""
```

- [ ] **Step 3: Implement Step 0 (pre-flight) and Step 1 (sidecar capture)**

```python
def step_0_preflight() -> None:
    import silly_kicks
    if not silly_kicks.__version__.startswith("3."):
        raise RuntimeError(f"silly-kicks 3.x required, got {silly_kicks.__version__}")
    print(f"silly-kicks {silly_kicks.__version__}: OK")


def step_1_capture_delta_versions(client) -> None:
    import json
    from pathlib import Path

    versions = {}
    for table in ["bronze.spadl_actions", "bronze.vaep_action_values", "dev_gold.expected_threat_grids"]:
        rows = _execute_query(client, f"DESCRIBE HISTORY {table} LIMIT 1")
        versions[table] = rows[0]["version"] if rows else None
    Path("sk3_mig_rollback.json").write_text(json.dumps({"delta_versions": versions, "captured_at": _now_iso()}, indent=2))
    print("Wrote sk3_mig_rollback.json:", versions)
```

- [ ] **Step 4: Implement Step 2 (DELETE) — gated by `--confirm-deletes`**

```python
def step_2_delete_bronze_spadl(client, args) -> None:
    if not args.confirm_deletes:
        raise RuntimeError("Step 2 requires --confirm-deletes flag for safety")
    for table in ["bronze.spadl_actions", "bronze.vaep_action_values"]:
        print(f"DELETE FROM {table}")
        _execute_query(client, f"DELETE FROM {table}")
        rows = _execute_query(client, f"SELECT COUNT(*) AS n FROM {table}")
        if rows[0]["n"] != 0:
            raise RuntimeError(f"DELETE failed: {table} still has {rows[0]['n']} rows")
        print(f"  {table}: 0 rows confirmed")
```

- [ ] **Step 5: Implement Step 3 (trigger compute_spadl_vaep + wait)**

```python
def step_3_trigger_spadl_vaep(client) -> None:
    job_id = _resolve_job_id_by_name(client, "compute_spadl_vaep")
    print(f"Triggering compute_spadl_vaep (job_id={job_id})")
    run = client.jobs.run_now(job_id=job_id)
    _wait_for_run(client, run.run_id, name="compute_spadl_vaep")  # polls every 30s, prints progress
```

The `_wait_for_run` helper polls `client.jobs.get_run(run_id)` every 30 seconds, prints `state.life_cycle_state`, and raises if `result_state != SUCCESS`. Per CLAUDE.md "Never disappear into long-running commands": the orchestrator MUST be invoked with `run_in_background=true` and Bash output polled every 15-30s by the operator.

- [ ] **Step 6: Implement Step 4 (Gate A) — call into `sk3_mig_verify`**

```python
def step_4_gate_a(client) -> None:
    import json
    pre = json.loads(Path("sk3_mig_pre.json").read_text()) if Path("sk3_mig_pre.json").exists() else None
    from sk3_mig_verify import gate_a_provider_coverage
    expected = ["statsbomb", "wyscout", "idsse", "metrica"]
    for table in ["bronze.spadl_actions", "bronze.vaep_action_values", "dev_gold.fct_action_values"]:
        pre_counts = (pre or {}).get(table)
        passed, diag = gate_a_provider_coverage(client, table=table, expected_sources=expected, pre_counts=pre_counts, tolerance_pct=0.5)
        print(diag)
        if not passed:
            raise RuntimeError(f"Gate A FAILED for {table}")
```

- [ ] **Step 7: Implement Steps 5-11 by following the same pattern**

For each step, write a `step_N_<name>(client, args)` function. Steps 5 and 9 (3-stage dbt build) trigger the existing dbt build job. Step 6 deletes + triggers `compute_expected_threat`. Step 7 calls `xg_dimension_preflight` from `sk3_mig_verify.py`. Step 8 triggers each coord-dependent inference job sequentially via `_resolve_job_id_by_name + jobs.run_now + _wait_for_run`. Step 10 invokes `scripts/maintain_synced_tables.py` + `scripts/run_lakebase_grants.py` (subprocess). Step 11 calls `gate_b_coord_correctness` + `xt_sanity_probe`.

The exact list of jobs in Step 8 (per spec §2):

```python
STEP_8_JOBS_IN_ORDER = [
    "compute_xg_predictions",
    "compute_xg_predictions_v2",
    "compute_defcon_lite",
    "compute_pausa",
    "import_obso_results",
    # Note: compute_obso may be a no-op — verify whether such a job exists; skip if not
    "compute_player_embeddings_v1",
    "compute_player_embeddings_v2",
    "compute_player_embeddings_360",
    # F2V batch inference workflows — list TBD by inspecting workflow-cards/wf-football2vec*.yaml
]
```

Confirm exact job names against `terraform/modules/workflows/main.tf` resources.

- [ ] **Step 8: Implement `main()` with `--start-at` arg parsing**

```python
def main() -> int:
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--start-at", type=int, default=0)
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--confirm-deletes", action="store_true")
    args = parser.parse_args()

    steps = [
        (0, step_0_preflight),
        (1, step_1_capture_delta_versions),
        (2, step_2_delete_bronze_spadl),
        # ... 3-11
    ]

    from databricks.sdk import WorkspaceClient
    client = WorkspaceClient()

    for n, fn in steps:
        if n < args.start_at:
            print(f"--- Step {n}: SKIPPED (--start-at {args.start_at}) ---")
            continue
        print(f"=== Step {n}: {fn.__name__} ===")
        if args.dry_run:
            print("  (dry-run)")
            continue
        try:
            sig = inspect.signature(fn)
            kwargs = {}
            if "client" in sig.parameters:
                kwargs["client"] = client
            if "args" in sig.parameters:
                kwargs["args"] = args
            fn(**kwargs)
        except Exception as exc:
            print(f"!!! Step {n} FAILED: {exc!r}")
            print(f"!!! Resume with: --start-at {n}")
            return 1

    print("=== ALL STEPS COMPLETE ===")
    return 0


if __name__ == "__main__":
    import sys
    sys.exit(main())
```

- [ ] **Step 9: Dry-run the orchestrator to verify wiring**

```bash
uv run python scripts/sk3_mig_rebuild.py --dry-run
```

Expected: prints all 11 steps without executing. If any step's function fails to import or the dry-run errors out, fix before proceeding to live execution in Phase 5.

---

## Phase 3 — Documentation

### Task 13: Write ADR-022

**Files:**
- Create: `docs/superpowers/adrs/ADR-022-direction-of-play-migration.md`

- [ ] **Step 1: Read the ADR template**

```bash
cat docs/superpowers/adrs/ADR-TEMPLATE.md
```

- [ ] **Step 2: Write ADR-022 following the template**

Concrete sections to populate:

- **Date:** 2026-05-02
- **Status:** Accepted
- **Deciders:** Karsten S. Nielsen (human), Claude Opus 4.7 (AI)
- **Context (3 paragraphs):** (1) The OPT-1 e2e probe's discovery of the U-shaped global xT grid + the dual-mirror diagnosis. (2) silly-kicks 3.0.0's per-converter `to_spadl_ltr(input_convention=...)` design — the upstream fix. (3) The lakehouse-side migration challenge — every coord-dependent mart must be rebuilt; model retrains are downstream of the rebuild.
- **Decision (one sentence):** Adopt silly-kicks 3.0.0; force-rebuild `bronze.spadl_actions` + downstream coord-dependent marts in a single PR (Group A), defer model retraining and HF dataset republishing to a follow-up PR (Group B).
- **Alternatives considered:** Table comparing (A) defer migration + suppress xT, (B) one mega-PR with retrains, (C — chosen) two-phase with Group A correctness now + Group B retrains as follow-up.
- **Consequences:**
  - Positive: coord-correct SPADL + xT for all 4 sources; ADR-012 §2 grace-period closed; SK3-coordinate invariant codified as unit test + production gate via `SILLY_KICKS_ASSERT_INVARIANTS=1`.
  - Negative: Group A → Group B window has biased model predictions (current weights against new coords); drift detection fires harmlessly; `fct_model_validation_baselines` rebase deferred.
  - Neutral: tracking-adapter LTR-by-default migration deferred (consumers pinned to `output_convention="absolute_frame"`).
- **Related:** silly-kicks ADR-006, ADR-012, ADR-014, ADR-018; spec `docs/superpowers/specs/2026-05-02-sk3-mig-direction-of-play-migration-design.md`; OPT-1 PR #248.

---

### Task 14: Update ADR-012 §2 to record the grace-period closure

**Files:**
- Modify: `docs/superpowers/adrs/ADR-012-training-to-production-delivery-hardening.md`

- [ ] **Step 1: Find the §2 paragraph that mentions the grace-period rule**

```bash
grep -nE "for one release window|fallback is removed" docs/superpowers/adrs/ADR-012-training-to-production-delivery-hardening.md
```

Expected: line 39 (per the read in earlier context).

- [ ] **Step 2: Edit the relevant sentence to record the closure**

Change:
```markdown
Legacy envelopes without the field fall back to the companion v1 XGBoost feature list for one release window, then the fallback is removed.
```

to:
```markdown
Legacy envelopes without the field fall back to the companion v1 XGBoost feature list for one release window, then the fallback is removed. **Grace-period closure (2026-05-02, SK3-MIG cycle):** the fallback was removed in [SK3-MIG PR](#) — v2 envelopes lacking `feature_names` now raise `RuntimeError` at inference time. See ADR-022.
```

(The `#` placeholder for the PR link gets filled in at Task 18.)

---

### Task 15: TODO.md updates — XG1-RETIRE row + SK3-MIG row pointer

**Files:**
- Modify: `TODO.md`

- [ ] **Step 1: Find the SK3-MIG row in TODO.md**

```bash
grep -nE 'SK3-MIG' TODO.md | head -3
```

- [ ] **Step 2: Update the SK3-MIG row's "References" section to point at this PR's spec + plan**

In the SK3-MIG row, add to the **References** section:
```
spec: docs/superpowers/specs/2026-05-02-sk3-mig-direction-of-play-migration-design.md;
plan: docs/superpowers/plans/2026-05-02-sk3-mig-direction-of-play-migration.md;
ADR-022.
```

The SK3-MIG row stays in On Deck until the PR ships; on merge, the row gets ARCHIVED + replaced by a Group B row (handled at Task 22).

- [ ] **Step 3: Add the new XG1-RETIRE row to the On Deck table, immediately below SK3-MIG**

Insert before the OPT-2 row (or wherever the table cleanly extends):

```
| XG1-RETIRE | Retire compute_xg_predictions (v1) workflow + fct_xg_predictions mart | Wicked | SK3-MIG (2026-05-02) — fallback removal made v1 dead-code from inference path | **Triggered when:** SK3-MIG ships (the v2 fallback removal eliminates v1's inference-path role; only Shot Map's display columns remain). **Scope:** (1) Delete src/ingestion/xg_model.py + the v1 entry point in pyproject.toml + the v1 workflow card wf-xg-v1.yaml + Terraform job declaration; (2) Delete dbt_project/models/marts/fct_xg_predictions.sql + dbt_project/models/staging/xg/stg_xg__predictions.sql + their YAML contract entries + _xg__sources.yml v1 references; (3) Drop the Lakebase fct_xg_predictions_synced synced table + indexes via scripts/delete_synced_table.py; (4) Wipe the v1 MLflow registered model + UC Volume v1 weights folder; (5) **UI migration in hf_taipy_app/src/state/shot_map.py**: either DROP the v1 custom xG display columns (xg_logistic, xg_gradient_boosted), OR MIGRATE to display v2's xg_set_encoder + xg_ci_lower + xg_ci_upper instead (UX decision — needs design call); (6) Delete fetch_xg_predictions() in hf_taipy_app/src/queries/shots.py; (7) Update HF model card xg-model-statsbomb-wyscout.md to reflect retirement (or delete + update org-card.md per ADR-014). **References:** ADR-012 §2 grace-period closure; SK3-MIG PR; hf_taipy_app/src/state/shot_map.py:82-235. |
```

- [ ] **Step 4: Update the TODO.md "Last updated" line at top**

```bash
grep -nE 'Last updated' TODO.md | head -1
```

Update to: `**Last updated**: 2026-05-02 (SK3-MIG implementation in progress — silly-kicks 3.0.0 direction-of-play migration; XG1-RETIRE queued in On Deck behind SK3-MIG.)`

---

## Phase 4 — Single commit + PR

### Task 16: Final local verification

**Files:**
- No file changes (just verification)

- [ ] **Step 1: Re-run the full local CI gate**

```bash
uv run ruff check src/ scripts/
uv run ruff format --check src/ scripts/
uv run pyright src/
SILLY_KICKS_ASSERT_INVARIANTS=1 uv run pytest src/tests/ -v
```

All must PASS with 0 violations / 0 errors. If anything fails, fix before proceeding.

- [ ] **Step 2: Re-run `bump_wheel.py --check` to confirm no stale wheel references**

```bash
uv run python scripts/bump_wheel.py --check
```

Expected: exit 0.

- [ ] **Step 3: Re-run terraform validate**

```bash
cd terraform/environments/dev && terraform validate && cd ../../..
```

Expected: success.

- [ ] **Step 4: Inspect `git status` to verify the change set is what you expect**

```bash
git status
git diff --stat
```

Expected files changed (approximate):
- `pyproject.toml`
- `terraform/modules/workflows/main.tf`
- Several PEP 723 scripts in `scripts/` (wheel version)
- `deploy.sh` (wheel version)
- `src/ingestion/xg_model_v2.py`
- 7 tracking-aware ingestion modules (output_convention pinning)
- `.github/workflows/python-ci.yml`, `.github/workflows/dbt-live-ci.yml`
- `src/tests/test_xg_model_v2.py` (or wherever the fallback test lives)
- `src/tests/test_sk3_coord_correctness.py` (NEW)
- `scripts/sk3_mig_rebuild.py` (NEW)
- `scripts/sk3_mig_verify.py` (NEW)
- `docs/superpowers/adrs/ADR-022-direction-of-play-migration.md` (NEW)
- `docs/superpowers/adrs/ADR-012-training-to-production-delivery-hardening.md`
- `docs/superpowers/specs/2026-05-02-sk3-mig-direction-of-play-migration-design.md` (created during brainstorming)
- `docs/superpowers/plans/2026-05-02-sk3-mig-direction-of-play-migration.md` (this file)
- `TODO.md`

If the change set has unexpected additions or omissions, investigate before committing.

---

### Task 17: USER APPROVAL GATE — single commit + push

**Files:**
- No file changes (commit operation)

⚠️ **EXPLICIT USER APPROVAL REQUIRED** per `feedback_no_commits_without_explicit_approval.md`. Wait for the sentinel `~/.claude-git-approval` per `reference_git_commit_sentinel.md`.

- [ ] **Step 1: Stage all changes explicitly (no `git add .` per CLAUDE.md security)**

```bash
git add pyproject.toml terraform/modules/workflows/main.tf src/ingestion/xg_model_v2.py src/ingestion/pitch_control_batch.py src/ingestion/line_breaking.py src/ingestion/off_ball_xt.py src/ingestion/formations_efpi.py src/ingestion/formations_shape_graph.py src/ingestion/defcon_lite.py src/ingestion/elastic_sync.py src/tests/test_xg_model_v2.py src/tests/test_sk3_coord_correctness.py scripts/sk3_mig_rebuild.py scripts/sk3_mig_verify.py docs/superpowers/adrs/ADR-022-direction-of-play-migration.md docs/superpowers/adrs/ADR-012-training-to-production-delivery-hardening.md docs/superpowers/specs/2026-05-02-sk3-mig-direction-of-play-migration-design.md docs/superpowers/plans/2026-05-02-sk3-mig-direction-of-play-migration.md TODO.md .github/workflows/python-ci.yml .github/workflows/dbt-live-ci.yml
git add scripts/*.py deploy.sh  # auto-bumped wheel consumers (verify with git status before committing)
```

- [ ] **Step 2: Verify staging is correct**

```bash
git status
```

Expected: ALL changed files staged; NO unstaged changes; NO untracked files (except output sidecars `sk3_mig_pre.json` etc. which we explicitly don't commit).

- [ ] **Step 3: Wait for the user's explicit "approved, commit" + sentinel**

Per memory: the user must run `!touch ~/.claude-git-approval` before the commit can fire. Do NOT commit without this.

- [ ] **Step 4: Create the single squash-style commit**

```bash
git commit -m "$(cat <<'EOF'
feat(sk3-mig): silly-kicks 3.0.0 direction-of-play migration (Group A)

Bumps silly-kicks 2.5.0 → 3.0.0 — corrects the dual-mirror direction-of-play
inversion present since v0.1.0. Per silly-kicks ADR-006 the converter layer now
self-routes via to_spadl_ltr(input_convention=...) and the VAEP framework no
longer applies the second mirror.

Group A (this PR) — data correctness:
- Pin bump + wheel 0.3.30 + TF env-spec parity
- SILLY_KICKS_ASSERT_INVARIANTS=1 in CI + production
- Tracking adapters pinned to output_convention="absolute_frame"
  (deprecation flagged for follow-up cycle)
- ADR-012 §2 grace-period closure: v2→v1 XGBoost feature-list fallback
  removed; v2 envelopes lacking feature_names now raise at inference time
- New scripts/sk3_mig_rebuild.py orchestrator + scripts/sk3_mig_verify.py
  verification (Gate A provider coverage, Gate B coord-correctness, §6 xG
  v1/v2 dimension pre-flight, xT sanity probe)
- New invariant unit test src/tests/test_sk3_coord_correctness.py
- ADR-022 (lakehouse-side migration) + TODO.md SK3-MIG → spec/plan link
- TODO.md adds XG1-RETIRE row (v1 retirement queued behind SK3-MIG)

Group B (next PR) — model retraining + HF republishing — captured as a
single follow-up TODO row at PR-merge time. The Group A → Group B window
will see drift detection fire harmlessly (model predictions are biased —
current weights against new SPADL coords); fct_model_validation_baselines
rebases as part of Group B.

Spec: docs/superpowers/specs/2026-05-02-sk3-mig-direction-of-play-migration-design.md
Plan: docs/superpowers/plans/2026-05-02-sk3-mig-direction-of-play-migration.md
ADR:  docs/superpowers/adrs/ADR-022-direction-of-play-migration.md

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>
EOF
)"
```

- [ ] **Step 5: Push to origin**

```bash
git push -u origin feat/sk3-mig-direction-of-play
```

- [ ] **Step 6: Verify push succeeded**

```bash
git status
```

Expected: `Your branch is up to date with 'origin/feat/sk3-mig-direction-of-play'.`

---

### Task 18: USER APPROVAL GATE — open PR

**Files:**
- No file changes (PR creation)

⚠️ **EXPLICIT USER APPROVAL REQUIRED** per `feedback_no_commits_without_explicit_approval.md`.

- [ ] **Step 1: Wait for the user's explicit "approved, open PR"**

- [ ] **Step 2: Open the PR via `gh pr create`**

```bash
gh pr create --title "feat(sk3-mig): silly-kicks 3.0.0 direction-of-play migration (Group A)" --body "$(cat <<'EOF'
## Summary

Group A of SK3-MIG: silly-kicks 3.0.0 direction-of-play correctness migration. Closes the dual-mirror inversion that produced 9× xT-magnitude divergence in OPT-1's e2e probe (2026-05-02). Group B (model retraining + HF republishing) is deferred to a single follow-up PR captured as a TODO row.

## What this PR does

- **silly-kicks 2.5.0 → 3.0.0** (pin bump + wheel 0.3.29 → 0.3.30 + TF env-spec parity)
- **`SILLY_KICKS_ASSERT_INVARIANTS=1`** in CI + production (raise on input-convention mismatch — fail-loud on any future regression)
- **Tracking adapters pinned** to `output_convention="absolute_frame"` to preserve current behaviour
- **ADR-012 §2 grace-period closure** — v2→v1 XGBoost feature-list fallback removed (`xg_model_v2.py:240-261`); v2 envelopes lacking `feature_names` now raise at inference time
- **New `scripts/sk3_mig_rebuild.py`** — 11-step orchestrator: DELETE + recompute `bronze.spadl_actions` for all 4 sources → `dbt build` → wipe + recompute `expected_threat_grids` → trigger coord-dependent inference workflows → refresh Lakebase synced tables → final verification gates
- **New `scripts/sk3_mig_verify.py`** — Gate A (provider coverage, ±0.5%), Gate B (per-source per-team shot-x split, ≤10% imbalance), xT sanity probe, §6 xG v1/v2 dimension pre-flight (3 checks: artifact resolution, envelope consistency, smoke inference)
- **ADR-022** documents the migration; **XG1-RETIRE TODO row** queued (v1 inference workflow becomes dead-code post-fallback-removal except for Shot Map display columns)

## Verification (filled in after Phase 5 orchestrator run)

### Gate A — Provider coverage
*(per-source row counts pre vs post, ±0.5% tolerance)*

### Gate B — Coord-correctness
*(per-source per-team shot-x split, expected ~50/50)*

### xT sanity probe
*(global grid max/sum/monotonicity)*

### xG v1/v2 dimension pre-flight
*(v1 features, v2 features, tabular_dim, run IDs)*

## Test plan

- [x] `uv run ruff check src/ scripts/` — clean
- [x] `uv run pyright src/` — clean
- [x] `SILLY_KICKS_ASSERT_INVARIANTS=1 uv run pytest src/tests/` — clean
- [x] dbt-live-CI passes against `dev_gold`
- [x] Orchestrator dry-run prints all 11 steps
- [ ] Orchestrator full run against `dev_gold` succeeds; gates A + B pass
- [ ] PR body updated with verification probe outputs

## Group B — what's deferred

Captured as a single follow-up TODO row (added at merge time per `feedback_no_scope_decisions.md`). Items: re-train VAEP / xG v1+v2 / xT v1 production grid / ExT v2 P0+P1 baselines / DEFCON-lite / OBSO / PAUSA / Football2Vec v1+v2+360 / ScoutGPT; re-publish all HF datasets riding on `fct_action_values`; re-baseline `fct_model_validation_baselines`; refresh `docs/performance-baselines.md`.

## References

- silly-kicks v3.0.0 + ADR-006 (in `D:\Development\karstenskyt__silly-kicks`)
- Lakehouse spec: `docs/superpowers/specs/2026-05-02-sk3-mig-direction-of-play-migration-design.md`
- Lakehouse plan: `docs/superpowers/plans/2026-05-02-sk3-mig-direction-of-play-migration.md`
- Lakehouse ADR-022 (this PR)
- Lakehouse ADR-012 §2 (grace-period closure recorded)
- OPT-1 PR #248 — the e2e probe that surfaced the bug

🤖 Generated with [Claude Code](https://claude.com/claude-code)
EOF
)"
```

- [ ] **Step 3: Verify PR opened — capture the PR number for ADR-012 + ADR-022 cross-references**

```bash
gh pr view --json number,url
```

Note the PR number (e.g., `#249`) for use in Task 14's ADR-012 placeholder + Task 13's ADR-022 References block.

- [ ] **Step 4: Wait for CI to fire (PR-trigger pulls`feat/sk3-mig-direction-of-play` against `main`)**

```bash
gh pr checks
```

Expected: GitHub CI starts running. dbt-live-CI is the longest gate (~6-10 min). If CI doesn't fire within 1 minute, suspect a stale-base issue per `feedback_pull_origin_main_before_branching.md`.

---

## Phase 5 — Live execution against dev_gold

### Task 19: Run the orchestrator end-to-end

**Files:**
- Live data writes — `bronze.spadl_actions`, `bronze.vaep_action_values`, `dev_gold.expected_threat_grids`, every coord-dependent mart, every Lakebase synced table

⚠️ Per CLAUDE.md "Never disappear into long-running commands": orchestrator is invoked with `run_in_background=true` and polled every 15-30s.

- [ ] **Step 1: Verify the wheel has propagated to UC Volume + Databricks workflows can pick up 0.3.30**

The wheel is built/uploaded by the existing CI/deploy pattern. After PR push (Task 17 Step 5), GitHub Actions builds the wheel + uploads to UC Volume per `bump_wheel.py` consumer wiring. Verify before triggering jobs:

```bash
uv run python -c "from databricks.sdk import WorkspaceClient; w = WorkspaceClient(); print([f.path for f in w.files.list_directory_contents('/Volumes/soccer_analytics/dev_gold/wheels/')])"
```

Expected: `luxury_lakehouse-0.3.30-py3-none-any.whl` is present. If not, the wheel build/upload pipeline hasn't fired yet — wait for it.

- [ ] **Step 2: Capture pre-rebuild Gate A snapshot (orchestrator step would do this too, but capture once explicitly so we have the baseline)**

```bash
uv run python scripts/sk3_mig_verify.py --gate-a --output sk3_mig_pre.json
```

Expected: writes `sk3_mig_pre.json` with current per-source row counts.

- [ ] **Step 3: Dry-run the orchestrator one more time as a safety check**

```bash
uv run python scripts/sk3_mig_rebuild.py --dry-run
```

Expected: prints all 11 steps with their actions + the deletes flagged. No errors.

- [ ] **Step 4: Run the orchestrator in background mode**

```bash
uv run python scripts/sk3_mig_rebuild.py --confirm-deletes
```

⚠️ MUST be invoked with `run_in_background=true` in the Bash tool. Output written to log file; poll every 30 seconds and report progress.

- [ ] **Step 5: Poll progress every 30s; report each step's start + completion**

For each step:
- Step 0-1: completes in seconds
- Step 2: DELETE — completes in seconds
- Step 3: `compute_spadl_vaep` — wall-clock budget 60-90 min for 4 sources at full re-derive
- Step 4: Gate A — completes in seconds
- Step 5: 3-stage dbt build — wall-clock budget 30-90 min
- Step 6: DELETE + `compute_expected_threat` — wall-clock budget 5-15 min (post-OPT-1 streaming refactor)
- Step 7: xG pre-flight — completes in <30 seconds
- Step 8: each inference job — most complete in 10-30 min; F2V jobs longer
- Step 9: 3-stage dbt build — wall-clock budget 30-60 min
- Step 10: Lakebase sync — wall-clock budget 30-90 min depending on synced-table count
- Step 11: Gate B + xT sanity — completes in seconds

Total expected wall-clock: well under daily-job budget. Each step's verification gate prevents downstream steps from running on broken state. If any step fails, the orchestrator halts and prints the resume command.

- [ ] **Step 6: On any step failure, investigate root cause before re-firing**

Per CLAUDE.md "Three-strikes rule" + "Investigate before retrying": once a step fails, do NOT retry blindly. Read the failure diagnostic, check Databricks job logs via `gh` or the Databricks UI, identify the root cause, fix in code if needed (which means another commit + push + wait for CI), then resume with `--start-at <step>`.

- [ ] **Step 7: On orchestrator success, capture the post-rebuild verification report**

```bash
uv run python scripts/sk3_mig_verify.py --full --report-md > sk3_mig_post_report.md
```

Expected: exit code 0; `sk3_mig_post_report.md` contains Markdown-formatted Gate A + Gate B + xT + xG pre-flight outputs ready for PR-body insertion.

---

### Task 20: Update PR body with verification outputs

**Files:**
- No file changes; PR description update via `gh`

- [ ] **Step 1: Read the current PR body**

```bash
gh pr view --json body --jq .body > pr_body_current.md
```

- [ ] **Step 2: Splice the verification outputs into the "Verification" section**

The PR body has a placeholder section `## Verification (filled in after Phase 5 orchestrator run)` with empty subsections. Replace those placeholders with the contents of `sk3_mig_post_report.md`.

- [ ] **Step 3: Edit the PR body**

```bash
gh pr edit --body "$(cat pr_body_updated.md)"
```

- [ ] **Step 4: Verify the update via `gh pr view`**

```bash
gh pr view
```

Expected: the verification subsections now contain the probe outputs.

---

### Task 21: USER APPROVAL GATE — merge

**Files:**
- No file changes (merge operation)

⚠️ **EXPLICIT USER APPROVAL REQUIRED** per `feedback_no_commits_without_explicit_approval.md`. Merge is destructive (changes `main`).

- [ ] **Step 1: Verify all CI checks have passed**

```bash
gh pr checks
```

Expected: all required checks GREEN.

- [ ] **Step 2: Wait for the user's explicit "approved, merge"**

- [ ] **Step 3: Merge the PR (squash mode per project convention)**

```bash
gh pr merge --squash --delete-branch
```

- [ ] **Step 4: Pull merged main locally**

```bash
git checkout main
git pull --ff-only origin main
git log --oneline -1
```

Expected: new squash commit at HEAD.

---

## Phase 6 — Post-merge memory updates

### Task 22: Save complete memory entry + update MEMORY.md index + update SK3-MIG row

**Files:**
- Create: `C:\Users\Karsten\.claude\projects\D--Development-karstenskyt--luxury-lakehouse\memory\project_sk3_mig_complete.md`
- Modify: `C:\Users\Karsten\.claude\projects\D--Development-karstenskyt--luxury-lakehouse\memory\MEMORY.md`
- Modify: `C:\Users\Karsten\.claude\projects\D--Development-karstenskyt--luxury-lakehouse\memory\project_silly_kicks_direction_of_play_bug.md` (mark historical)
- Modify: `D:\Development\karstenskyt__luxury-lakehouse\TODO.md` (archive SK3-MIG row, add Group B row)

These memory edits do NOT require git commits — memory lives outside the repo.

- [ ] **Step 1: Write `project_sk3_mig_complete.md`**

```markdown
---
name: SK3-MIG Group A SHIPPED — silly-kicks 3.0.0 direction-of-play migration
description: silly-kicks 2.5.0 → 3.0.0 + bronze.spadl_actions full re-derive for all 4 sources + downstream coord-dependent mart rebuild + ADR-012 §2 grace-period closure (v2→v1 fallback removed). Group B (model retrains + HF republish + validation_baseline rebase) queued as single follow-up TODO. Shipped <DATE>, PR #<N>, squash <SHA>.
type: project
---

# SK3-MIG Group A — SHIPPED

## What landed
[Bullet summary mirroring the PR description]

## What's queued (Group B)
[The single follow-up TODO row content]

## Verification evidence
[Gate A + Gate B + xT + xG pre-flight outputs from sk3_mig_post_report.md]

## Files changed
[git diff --stat from the squash commit]

## Wheel state
- Pre: 0.3.29 (silly-kicks >=2.5.0,<3.0)
- Post: 0.3.30 (silly-kicks >=3.0.0,<4)

## Next session work
[The Group B TODO row content + any follow-ups discovered during the live rebuild]
```

- [ ] **Step 2: Add an index entry to `MEMORY.md`**

In the User Preferences section (mirrors the OPT-1 / PR-Cycle-C entries), add:

```markdown
- [project_sk3_mig_complete.md](project_sk3_mig_complete.md) — **SK3-MIG Group A SHIPPED <DATE>**. main HEAD `<SHA>` (PR #<N> squash). Wheel 0.3.30. silly-kicks 3.0.0 direction-of-play migration; ADR-012 §2 grace-period closed. Group B (model retrains + HF republish + validation_baseline rebase) queued in TODO On Deck.
```

- [ ] **Step 3: Mark `project_silly_kicks_direction_of_play_bug.md` as historical**

Edit the file's frontmatter `description` to prepend `**HISTORICAL** — superseded by project_sk3_mig_complete.md (Group A shipped <DATE>). Original content preserved below for context.`

- [ ] **Step 4: Archive the SK3-MIG row from TODO.md On Deck section + add Group B row**

In `TODO.md`, the SK3-MIG row gets DELETED from On Deck and a new Group B row takes its place with this content:

```
| SK3-MIG-B | silly-kicks 3.0.0 Group B — model retrains + HF dataset republishing + fct_model_validation_baselines rebase + perf-baselines refresh | Wicked | SK3-MIG Group A shipped <DATE> PR #<N> | **Trigger:** Group A shipped + drift detection has been firing harmlessly during the window. **Scope** (per spec §7): (1) re-train VAEP / xG v1+v2 / xT v1 production grid / ExT v2 P0+P1 baselines / DEFCON-lite / OBSO / PAUSA / Football2Vec v1+v2+360 / ScoutGPT (re-tokenize); (2) re-publish all HF datasets riding on fct_action_values (spadl-vaep, xg-shots, freeze-frame, shots-on-target, embedding training data); (3) re-baseline fct_model_validation_baselines (every drift threshold will fire simultaneously without a fresh baseline once retrains shift prediction distributions); (4) refresh docs/performance-baselines.md with re-build cycle timings. **References:** project_sk3_mig_complete.md; spec docs/superpowers/specs/2026-05-02-sk3-mig-direction-of-play-migration-design.md §7. |
```

The TODO.md "Last updated" line at top updates to: `**Last updated**: <DATE> (SK3-MIG Group A SHIPPED PR #<N>; Group B queued at SK3-MIG-B in On Deck.)`

- [ ] **Step 5: Verify memory + TODO consistency**

```bash
ls -la "C:\Users\Karsten\.claude\projects\D--Development-karstenskyt--luxury-lakehouse\memory\" | head -20
grep -nE 'SK3-MIG' TODO.md | head -5
```

Expected: `project_sk3_mig_complete.md` exists; TODO.md no longer has the original SK3-MIG row but has SK3-MIG-B in its place.

---

## Self-review checklist

After writing this plan, run:

1. **Spec coverage:** Map each spec section to a task. ✓ §1.1 → Task 2; §1.2 → Task 4; §1.3 → Task 5; §1.4 → Task 6; §1.5 → Task 7; §1.6 → Task 13; §1.7 → Task 15; §2 → Tasks 12 + 19; §3 → Task 11; §4 → Task 12 (orchestrator captures rollback sidecar) + general re-fire flow; §5 → Tasks 8, 9, 10, 16; §6 → Task 11 Step 6; §7 → Task 22 Group B row content; §8 → Task 15.
2. **Placeholder scan:** Searched for "TBD", "TODO", "implement later", "fill in details". Two intentional placeholders remain: PR # in Task 14 + Task 22 (filled at merge time); the F2V job names list in Task 12 ("F2V batch inference workflows — list TBD by inspecting workflow-cards/wf-football2vec*.yaml") — this is a discovery step the engineer must run, not a future-me-fill-it-in.
3. **Type consistency:** Function names defined in Task 11 (`gate_a_provider_coverage`, `gate_b_coord_correctness`, `xt_sanity_probe`, `xg_dimension_preflight`) reused in Task 12 step functions consistently. The `_parse_v2_envelope_features` helper defined in Task 7 Step 4 is not referenced elsewhere in the plan but is a module-level helper that lives in `xg_model_v2.py` (referenced by the test in Task 7 Step 2). The Step 8 jobs list in Task 12 names jobs the engineer must verify exist via `terraform/modules/workflows/main.tf` inspection.

If issues are found during execution, fix inline.
