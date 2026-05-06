# silly-kicks Dependency Restructure + 3.7.0 Bump + Phase 9 Orchestrator Fixes

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Unblock Phase 9 operator runtime by making silly-kicks available on HF Jobs via a new `spadl` pyproject extra, bumping the pin to 3.7.0, and landing 6 uncommitted orchestrator fixes.

**Architecture:** New `[spadl]` optional extra in pyproject.toml carries `silly-kicks>=3.7.0,<4`. PEP 723 trainer scripts switch from `luxury-lakehouse @ ...wheel` to `luxury-lakehouse[spadl] @ ...wheel`. The `analytics` extra inherits via self-referential `luxury-lakehouse[spadl]`. Orchestrator fixes are already applied in the working tree — they get committed as-is.

**Tech Stack:** pyproject.toml (PEP 621), uv (lock + sync), Terraform HCL, pytest sentinels

**Spec:** `docs/superpowers/specs/2026-05-06-silly-kicks-dep-restructure-and-bump-design.md`

---

## File Map

| File | Action | Responsibility |
|------|--------|----------------|
| `pyproject.toml` | Modify | Add `spadl` extra, update `analytics` to inherit, bump silly-kicks pin |
| `uv.lock` | Auto-updated | `uv sync` regenerates |
| `scripts/train_vaep_model_hf.py` | Modify | `[spadl]` on wheel dep, `_REQUIRED_SK_MIN` → `(3, 7, 0)` |
| `scripts/train_xg_v2_hf.py` | Modify | Same |
| `scripts/train_football2vec.py` | Modify | Same |
| `scripts/train_football2vec_v2.py` | Modify | Same |
| `scripts/train_football2vec_360.py` | Modify | Same |
| `scripts/train_scoutgpt_hf.py` | Modify | Same |
| `terraform/modules/workflows/main.tf` | Modify | Bump silly-kicks pin `>=3.7.0,<4` |
| `src/tests/test_sk3_mig_b_orchestrator_invariants.py` | Modify | Update docstring + expected constant |
| `scripts/sk3_mig_b_retrain.py` | Modify (already changed) | 6 orchestrator fixes (staged from working tree) |
| `scripts/train_vaep_model_hf.py` | Modify (already changed) | `VALIDATED_HF_FLAVOR` already `cpu-xl` |
| `scripts/train_football2vec.py` | Modify (already changed) | `VALIDATED_HF_FLAVOR` already `cpu-xl` |
| 25+ files via `bump_wheel.py` | Auto-modified | Wheel version 0.3.33 → 0.3.34 |

---

### Task 1: Create `spadl` extra and update `analytics` inheritance

**Files:**
- Modify: `pyproject.toml:22-44`

- [ ] **Step 0: Verify silly-kicks 3.7.0 is published on PyPI**

Run: `pip index versions silly-kicks 2>/dev/null | head -3`
Expected: Shows `3.7.0` in available versions. If not present, stop — the silly-kicks release must be published first.

- [ ] **Step 1: Add `spadl` extra and refactor `analytics`**

In `pyproject.toml`, insert the new `spadl` extra BEFORE the existing `analytics` extra, and replace the direct `silly-kicks` line in `analytics` with a self-referential `luxury-lakehouse[spadl]`:

```toml
[project.optional-dependencies]
spadl = [
    "silly-kicks>=3.7.0,<4",
]
analytics = [
    "luxury-lakehouse[spadl]",
    "pandas>=2.1.0",
    "numpy>=1.26.0",
    "mplsoccer>=1.1.3",
    "matplotlib>=3.8.0",
    "scipy>=1.11.0",
    "scikit-learn>=1.3.0",
    "xgboost==3.2.0",
    "rapidfuzz>=3.6.0",
    "unidecode>=1.3.0",
    "sparse-dot-topn>=1.1.0",
    "optuna>=4.0",
    "databricks-sql-connector>=4.0.0",
]
```

The edit replaces lines 22-44. The `"silly-kicks>=3.0.1,<4",` line is REMOVED from `analytics` — it's now inherited via `[spadl]`.

- [ ] **Step 2: Verify `uv sync --extra spadl` resolves silly-kicks 3.7.0**

Run: `uv sync --extra spadl 2>&1 | grep -i silly`
Expected: `silly-kicks` resolved at `3.7.0`

- [ ] **Step 3: Verify `uv sync --extra analytics` also resolves silly-kicks 3.7.0**

Run: `uv sync --extra analytics 2>&1 | grep -i silly`
Expected: `silly-kicks` resolved at `3.7.0` (validates self-referential inheritance)

- [ ] **Step 4: Verify `uv.lock` updated**

Run: `git diff uv.lock | head -40`
Expected: diff shows `spadl` extra and silly-kicks version change

---

### Task 2: Update PEP 723 wheel dep line in all 6 trainer scripts

**Files:**
- Modify: `scripts/train_vaep_model_hf.py:4`
- Modify: `scripts/train_xg_v2_hf.py:4`
- Modify: `scripts/train_football2vec.py:4`
- Modify: `scripts/train_football2vec_v2.py:4`
- Modify: `scripts/train_football2vec_360.py:4`
- Modify: `scripts/train_scoutgpt_hf.py:4`

Each script's PEP 723 block line 4 currently reads:
```python
#     "luxury-lakehouse @ https://huggingface.co/luxury-lakehouse/build-artifacts/resolve/main/luxury_lakehouse-0.3.33-py3-none-any.whl",
```

- [ ] **Step 1: Add `[spadl]` to wheel dep in all 6 scripts**

In each script, change:
```python
#     "luxury-lakehouse @ https://huggingface.co/luxury-lakehouse/build-artifacts/resolve/main/luxury_lakehouse-0.3.33-py3-none-any.whl",
```
to:
```python
#     "luxury-lakehouse[spadl] @ https://huggingface.co/luxury-lakehouse/build-artifacts/resolve/main/luxury_lakehouse-0.3.33-py3-none-any.whl",
```

The only change is `luxury-lakehouse` → `luxury-lakehouse[spadl]`. The version string (`0.3.33`) will be updated later by `bump_wheel.py` in Task 6.

**NOTE:** `scripts/train_psxg_hf.py` is intentionally excluded — it has no silly-kicks imports.

- [ ] **Step 2: Verify no trainer has a standalone silly-kicks PEP 723 dep**

Run: `grep -n "silly-kicks" scripts/train_*.py`
Expected: Only hits are `_REQUIRED_SK_MIN` comments and `import silly_kicks` lines — NO `"silly-kicks..."` in PEP 723 blocks. The sentinel test `test_no_trainer_pins_silly_kicks_explicitly` will also verify this in Task 5.

---

### Task 3: Bump `_REQUIRED_SK_MIN` in all 6 trainer scripts

**Files:**
- Modify: `scripts/train_vaep_model_hf.py:72`
- Modify: `scripts/train_xg_v2_hf.py:96`
- Modify: `scripts/train_football2vec.py:81`
- Modify: `scripts/train_football2vec_v2.py:77`
- Modify: `scripts/train_football2vec_360.py:75`
- Modify: `scripts/train_scoutgpt_hf.py:81`

- [ ] **Step 1: Change `_REQUIRED_SK_MIN` from `(3, 0, 1)` to `(3, 7, 0)` in all 6 scripts**

In each script, change:
```python
_REQUIRED_SK_MIN: tuple[int, int, int] = (3, 0, 1)
```
to:
```python
_REQUIRED_SK_MIN: tuple[int, int, int] = (3, 7, 0)
```

- [ ] **Step 2: Verify all 6 scripts updated**

Run: `grep "_REQUIRED_SK_MIN" scripts/train_*.py | grep -v "^.*:#"`
Expected: All 6 show `(3, 7, 0)`. Zero show `(3, 0, 1)`.

---

### Task 4: Bump silly-kicks pins in Terraform and orchestrator

**Files:**
- Modify: `terraform/modules/workflows/main.tf:1029`
- Modify: `scripts/sk3_mig_b_retrain.py:7,376`

- [ ] **Step 1: Update silly-kicks pin in TF analytics environment**

Change line 1029:
```hcl
        "silly-kicks>=3.0.1,<4",
```
to:
```hcl
        "silly-kicks>=3.7.0,<4",
```

- [ ] **Step 2: Update orchestrator PEP 723 silly-kicks pin**

In `scripts/sk3_mig_b_retrain.py` line 7, change:
```python
#     "silly-kicks>=3.0.1",
```
to:
```python
#     "silly-kicks>=3.7.0,<4",
```

**NOTE:** The orchestrator pins silly-kicks directly (not via the wheel's `[spadl]` extra) because it doesn't install the `luxury-lakehouse` wheel at all — its PEP 723 deps are `databricks-sdk`, `huggingface_hub`, `mlflow`, and `silly-kicks` only. The direct pin is intentional: the orchestrator needs silly-kicks for preflight version checking before any wheel code runs.

- [ ] **Step 3: Fix orchestrator runtime version check (string → tuple comparison)**

The committed orchestrator (line 375) uses lexicographic string comparison:
```python
    if sk_version < "3.0.1":
```
This is fragile — `"3.10.0" < "3.7.0"` evaluates `True` because `"1" < "7"` character-by-character. Since we're touching this line anyway, fix the comparison to use a proper version tuple:

```python
    sk_tuple = tuple(int(x) for x in sk_version.split(".")[:3])
    if sk_tuple < (3, 7, 0):
        raise RuntimeError(f"silly-kicks {sk_version} < 3.7.0")
```

This replaces both line 375 (the condition) and line 376 (the error message). Consistent with how `_assert_silly_kicks_min()` works in the trainer scripts.

---

### Task 4.5: Review uncommitted orchestrator fixes

**Files:**
- Review: `scripts/sk3_mig_b_retrain.py` (working tree diff)

The 6 Phase 9 orchestrator fixes (spec §5) are already applied in the working tree but uncommitted. Before proceeding, verify they match the spec.

- [ ] **Step 1: Review the full orchestrator diff**

Run: `git diff scripts/sk3_mig_b_retrain.py | head -200`

Verify against spec §5 table:
- Fix 2: Preflight env var gate — `DATABRICKS_HTTP_PATH` accepted as alternative
- Fix 3: Secrets dispatch — `state.warehouse_id` instead of `os.environ["DATABRICKS_WAREHOUSE_ID"]`
- Fix 4: Group 0 merge — `_step_0a` + `_step_0b` merged into single `_step_0a_group_0_inputs`; `steps_in_order` and `_step_already_at_or_past` updated
- Fix 5: SDK enum comparison — `getattr(..., "value", None) == "TERMINATED"` pattern
- Fix 6: `cpu-large` → `cpu-xl` in `_FLAVOR_MAP` (vaep + f2v_v1)

(Fix 1 — wheel version — will be handled in Task 6 after `bump_wheel.py`.)

---

### Task 5: Update sentinel tests

**Files:**
- Modify: `src/tests/test_sk3_mig_b_orchestrator_invariants.py:194-242`

- [ ] **Step 1: Update docstring of `test_no_trainer_pins_silly_kicks_explicitly`**

Replace the docstring (lines 195-201):
```python
    """No trainer may pin `silly-kicks` in its PEP 723 deps.

    The wheel's transitive pin (silly-kicks>=3.0.1,<4) is the single source of
    truth. uv silently picks a conflicting top-level pin over the wheel's
    transitive pin (verified empirically 2026-05-04 — silly-kicks 1.0.2 loaded
    under `silly-kicks>=1.0.0,<2.0` + wheel-pulled `>=3.0.1`). An explicit pin
    in a PEP 723 deps block is therefore an active footgun, not a safety net.
    """
```
with:
```python
    """No trainer may pin `silly-kicks` in its PEP 723 deps.

    The wheel's ``[spadl]`` extra (silly-kicks>=3.7.0,<4) is the single source
    of truth. Trainers install ``luxury-lakehouse[spadl] @ ...wheel`` which
    resolves silly-kicks transitively. uv silently picks a conflicting
    top-level pin over the wheel's transitive pin (verified empirically
    2026-05-04). An explicit ``"silly-kicks..."`` pin in PEP 723 deps is
    therefore an active footgun, not a safety net.

    NOTE: the regex ``r'"silly-kicks'`` intentionally does not match the
    ``luxury-lakehouse[spadl]`` wheel line — the token ``"silly-kicks``
    never appears there.
    """
```

- [ ] **Step 2: Update expected constant in `test_all_trainers_assert_silly_kicks_runtime_min`**

Replace lines 223-242. Change the docstring and expected value:

```python
def test_all_trainers_assert_silly_kicks_runtime_min() -> None:
    """Each trainer must declare module-level `_REQUIRED_SK_MIN = (3, 7, 0)`.

    Per spec §2.10.5: the runtime check inside `main()` is not directly
    introspectable post-hoc, so we assert the constant. Code review covers
    that the constant is actually consulted in `main()`. (Honest about what's
    mechanically testable — Q18 commitment.)
    """
    missing: list[str] = []
    wrong_value: dict[str, object] = {}
    for item, path in _TRAINER_PATHS.items():
        trainer = _load_script_module(path, f"_sk3_trainer_{item}")
        if not hasattr(trainer, "_REQUIRED_SK_MIN"):
            missing.append(item)
            continue
        expected = (3, 7, 0)
        actual = trainer._REQUIRED_SK_MIN
        if actual != expected:
            wrong_value[item] = actual
    assert not missing, f"Trainers missing module-level `_REQUIRED_SK_MIN: tuple[int, int, int] = (3, 7, 0)`: {missing}"
    assert not wrong_value, f"Trainers with `_REQUIRED_SK_MIN` not equal to (3, 7, 0): {wrong_value}"
```

Key changes: docstring `(3, 0, 1)` → `(3, 7, 0)`, `expected = (3, 0, 1)` → `expected = (3, 7, 0)`, both assert messages updated.

- [ ] **Step 3: Run sentinel tests**

Run: `uv run pytest src/tests/test_sk3_mig_b_orchestrator_invariants.py -v 2>&1 | tail -30`
Expected: All tests PASS (including the two updated ones).

---

### Task 6: Wheel version bump

**Files:**
- 25+ files via `scripts/bump_wheel.py`

- [ ] **Step 1: Run bump_wheel.py**

Run: `uv run python scripts/bump_wheel.py`

This bumps `0.3.33` → `0.3.34` across pyproject.toml, `src/shared/wheel.py`, 17 PEP 723 scripts (including the 6 trainer scripts from Task 2 — the `[spadl]` annotation survives because `bump_wheel.py` only replaces the version number in the wheel URL, not the full dep line), `deploy.sh`, and 2 TF files.

**PREREQUISITE — working-tree state:** The `old_string` values below (all `"0.3.33"`) reflect the working-tree state AFTER the 6 uncommitted orchestrator fixes from spec §5 have been applied. The committed state on `main` (HEAD `d2e7572`) has `"0.3.32"` at these locations. If working on a clean checkout without the uncommitted fixes, apply fixes 1-6 first (see `git diff scripts/sk3_mig_b_retrain.py` in the implementation session's working tree), otherwise every Edit targeting `"0.3.33"` will fail to match.

**IMPORTANT:** After `bump_wheel.py`, the orchestrator's hardcoded wheel version must be updated to `"0.3.34"`. There are 5 locations in `scripts/sk3_mig_b_retrain.py` (spec §5 Fix 1 lists 3 — the status message and error message are additional):

1. Preflight check: `if "0.3.33" not in version_lines[0]` → `if "0.3.34" not in version_lines[0]`
2. Preflight status msg: `msg="wheel 0.3.33 OK"` → `msg="wheel 0.3.34 OK"`
3. Preflight error msg: `Expected 0.3.33 (PR-2).` → `Expected 0.3.34.`
4. Telemetry SQL: `"0.3.33" if cycle_item != "pre_state"` → `"0.3.34" if cycle_item != "pre_state"`
5. CycleState constructor: `wheel_at_start="0.3.33"` → `wheel_at_start="0.3.34"`

- [ ] **Step 2: Update orchestrator wheel version references**

In `scripts/sk3_mig_b_retrain.py`, replace all `0.3.33` with `0.3.34` (5 locations listed above).

- [ ] **Step 3: Verify version consistency**

Run: `grep -rn "0\.3\.33" scripts/ src/shared/wheel.py pyproject.toml deploy.sh terraform/ 2>/dev/null`
Expected: Zero hits — all bumped to `0.3.34`.

Run: `grep -rn "0\.3\.34" src/shared/wheel.py pyproject.toml`
Expected: Both files show `0.3.34`.

---

### Task 7: Run full verification suite

**Files:** None (verification only)

- [ ] **Step 1: Ruff lint**

Run: `uv run ruff check src/ scripts/ 2>&1 | tail -5`
Expected: `All checks passed!` (or similar zero-violation output)

- [ ] **Step 2: Pyright type check**

Run: `uv run pyright src/ 2>&1 | tail -5`
Expected: `0 errors, 0 warnings` (or only pre-existing warnings)

- [ ] **Step 3: Run sentinel tests**

Run: `uv run pytest src/tests/test_sk3_mig_b_orchestrator_invariants.py -v 2>&1 | tail -20`
Expected: All 5 tests PASS

- [ ] **Step 4: Run TF env dep parity test**

Run: `uv run pytest src/tests/test_terraform_env_dep_parity.py -v 2>&1 | tail -10`
Expected: PASS

- [ ] **Step 5: Run silly-kicks boundary test**

Run: `uv run pytest src/tests/test_silly_kicks_boundary.py -v 2>&1 | tail -10`
Expected: All PASS (SPADL/VAEP surface unchanged in 3.7.0)

- [ ] **Step 6: Verify `[spadl]` annotation survived bump_wheel.py**

Run: `grep "\[spadl\]" scripts/train_vaep_model_hf.py scripts/train_xg_v2_hf.py scripts/train_football2vec.py scripts/train_football2vec_v2.py scripts/train_football2vec_360.py scripts/train_scoutgpt_hf.py`
Expected: All 6 show `luxury-lakehouse[spadl] @`

- [ ] **Step 7: Verify `uv.lock` has `spadl` extra**

Run: `grep -A2 "spadl" uv.lock | head -10`
Expected: Shows the `spadl` extra with silly-kicks reference

---

### Task 8: Commit

**Files:** All modified files from Tasks 1-6

- [ ] **Step 1: Stage all changes**

Run:
```bash
git status
git diff --stat
```

Review the full change set. Expected modified files:
- `pyproject.toml` — spadl extra + analytics refactor + version bump
- `uv.lock` — regenerated
- `scripts/train_vaep_model_hf.py` — `[spadl]`, `_REQUIRED_SK_MIN`, version bump
- `scripts/train_xg_v2_hf.py` — same
- `scripts/train_football2vec.py` — same + `VALIDATED_HF_FLAVOR` already `cpu-xl`
- `scripts/train_football2vec_v2.py` — `[spadl]`, `_REQUIRED_SK_MIN`, version bump
- `scripts/train_football2vec_360.py` — same
- `scripts/train_scoutgpt_hf.py` — same
- `scripts/sk3_mig_b_retrain.py` — 6 orchestrator fixes + version bump
- `terraform/modules/workflows/main.tf` — silly-kicks pin + version bump
- `src/tests/test_sk3_mig_b_orchestrator_invariants.py` — docstring + expected constant
- `src/shared/wheel.py` — version bump
- `deploy.sh` — version bump
- Other PEP 723 scripts — version bump only
- `docs/superpowers/specs/2026-05-06-silly-kicks-dep-restructure-and-bump-design.md` — spec

- [ ] **Step 2: Await user approval, then commit**

**Post-merge:** After PR merges and post-merge CI is GREEN, resume Phase 9 per spec §8.2 (`--start-at group_1`). The vaep HF Job is the first Group 1 dispatch — its success confirms `luxury-lakehouse[spadl]` resolves silly-kicks 3.7.0 on HF Jobs runtime.

Propose commit message:
```
fix(deps): add spadl extra for HF Jobs silly-kicks resolution + bump 3.7.0

Root cause: PEP 723 trainer scripts installed the wheel without extras —
silly-kicks (in [analytics] optional extra) never resolved on HF Jobs.

- New [spadl] pyproject extra: silly-kicks>=3.7.0,<4
- [analytics] inherits via self-referential luxury-lakehouse[spadl]
- 6 trainer scripts: luxury-lakehouse[spadl] @ wheel, _REQUIRED_SK_MIN→(3,7,0)
- TF analytics env: silly-kicks>=3.7.0,<4
- 6 Phase 9 orchestrator fixes (wheel ver, env var gate, secrets dispatch,
  Group 0 merge, SDK enum comparison, cpu-large→cpu-xl)
- Wheel 0.3.33 → 0.3.34
```
