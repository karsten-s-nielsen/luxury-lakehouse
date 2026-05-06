# silly-kicks Dependency Restructure + 3.7.0 Bump + Phase 9 Orchestrator Fixes

| Field | Value |
|---|---|
| **Date** | 2026-05-06 |
| **Status** | Design — awaits implementation approval |
| **Cycle** | Post-SK3-MIG-B PR-2 (#259). Phase 9 operator runtime unblock. |
| **Triggering failure** | HF Job `69fab24bf2f4addb7839c155` (vaep trainer) crashed with `ModuleNotFoundError: No module named 'silly_kicks'` — wheel installed without `[analytics]` extra, silly-kicks never resolved. |
| **Predecessors** | PR #259 (SK3-MIG-B PR-2, wheel 0.3.33) |

## §0 — Why this PR exists

### §0.1 Root cause

`silly-kicks` is declared under `[project.optional-dependencies] analytics` in
`pyproject.toml`. Every PEP 723 trainer script installs the wheel **without
extras**:

```
luxury-lakehouse @ https://...wheel
```

This resolves only the 6 base deps (requests, requests-cache, pydantic, PyYAML,
huggingface-hub, gitpython). silly-kicks is never installed on HF Jobs.

The VAEP trainer crashes at module load (top-level `import silly_kicks.spadl`).
The other 5 trainers crash later when `main()` calls `_assert_silly_kicks_min()`
(lazy `import silly_kicks`).

### §0.2 Why the sentinel test was wrong

`test_no_trainer_pins_silly_kicks_explicitly` (§2.10.4 in
`test_sk3_mig_b_orchestrator_invariants.py`) **forbids** adding `silly-kicks`
to PEP 723 deps, citing the uv silent-downgrade footgun. Its rationale says
"the wheel's transitive pin is the single source of truth." This is false —
the wheel only transitively pins silly-kicks when installed with `[analytics]`
extra. Without the extra, silly-kicks is invisible to the resolver.

The test was written during SK3-MIG-B PR-1 (2026-05-04) based on a correct
observation about uv downgrade behavior, but with an incorrect assumption that
the wheel's base deps include silly-kicks.

### §0.3 Version bump rationale

silly-kicks 3.0.1 → 3.7.0 (7 releases). All additions are tracking-layer
features (TF-2 through TF-7: pitch control, off-ball runs, sync_score,
smoothing, interpolation, GK angles, defensive-line geometry, infer_ball_carrier).
The SPADL/VAEP surface consumed by the lakehouse is unchanged. The lakehouse
will adopt the new tracking features in a future PR; this PR bumps the pin
floor so the retrain cycle validates against the version we intend to use.

### §0.4 Phase 9 orchestrator fixes

Six runtime fixes discovered during Phase 9 dry-run and first execution
attempts are uncommitted in `scripts/sk3_mig_b_retrain.py`. These are bundled
into this PR because they gate the same Phase 9 retrain cycle this dep
restructure unblocks.

## §1 — New `spadl` extra

### §1.0 Why `spadl`, not `sillykicks` or `tracking`

silly-kicks provides SPADL conversion, VAEP features/labels, and (since 3.1+)
tracking-layer utilities. The extra is named `spadl` because that is the
**consumption surface** used by every trainer script today — SPADL actions and
VAEP feature/label computation. The tracking features are consumed elsewhere
(Databricks compute tasks, not HF Jobs trainers). If a future trainer needs
only tracking utilities without SPADL, a separate `tracking` extra can be added
at that point. `spadl` is accurate for the current use case.

### §1.1 pyproject.toml changes

Create a new minimal extra that carries only silly-kicks:

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
    # silly-kicks line REMOVED — inherited via [spadl]
    "xgboost==3.2.0",
    "rapidfuzz>=3.6.0",
    "unidecode>=1.3.0",
    "sparse-dot-topn>=1.1.0",
    "optuna>=4.0",
    "databricks-sql-connector>=4.0.0",
]
```

Self-referential extras are valid PEP 621, supported by pip and uv.

### §1.2 Why not promote to base

silly-kicks transitively requires numpy, pandas, scikit-learn. The base deps
intentionally exclude these because Databricks serverless pre-installs them —
pinning them in the wheel causes Python kernel version conflicts. This is
documented in `pyproject.toml` lines 10-11.

### §1.3 Why not use `[analytics]`

The `analytics` extra pulls matplotlib, mplsoccer, optuna,
databricks-sql-connector, sparse-dot-topn — none needed by HF Jobs trainers.
Unnecessary install time and potential version conflicts.

## §2 — PEP 723 trainer script updates

### §2.1 Wheel dep line

All 6 HF Jobs trainer scripts change the PEP 723 dep from:

```python
#     "luxury-lakehouse @ https://...wheel",
```

to:

```python
#     "luxury-lakehouse[spadl] @ https://...wheel",
```

**Scripts (6 of 7 HF Jobs trainers — `train_psxg_hf.py` excluded, has no silly-kicks imports):**
1. `scripts/train_vaep_model_hf.py`
2. `scripts/train_xg_v2_hf.py`
3. `scripts/train_football2vec.py`
4. `scripts/train_football2vec_v2.py`
5. `scripts/train_football2vec_360.py`
6. `scripts/train_scoutgpt_hf.py`

### §2.2 `_REQUIRED_SK_MIN` bump

All 6 scripts: `(3, 0, 1)` → `(3, 7, 0)`.

The floor tracks what we've validated against. Running a retrain on a stale
silly-kicks (e.g. HF Jobs cache resolving 3.2.0) would produce artifacts from
an untested code path. The runtime assertion catches this.

## §3 — Terraform env spec

Only the `analytics` environment in `terraform/modules/workflows/main.tf`
(line 1029) contains `silly-kicks`. No other TF environments (`hf`,
`embeddings`, etc.) reference it.

```hcl
"silly-kicks>=3.0.1,<4",  →  "silly-kicks>=3.7.0,<4",
```

The `test_terraform_env_dep_parity.py` conformance test catches drift between
pyproject.toml and TF env specs. This update keeps them aligned.

## §4 — Sentinel test updates

### §4.1 `test_no_trainer_pins_silly_kicks_explicitly` — DOCSTRING-ONLY CHANGE

This test remains correct after the restructure. Trainers should NOT pin
`silly-kicks` explicitly in PEP 723 deps — it comes transitively via the
`[spadl]` extra on the wheel dep. The test's regex (`r'"silly-kicks'`) scans
PEP 723 dep lines for a standalone `"silly-kicks...` entry; the
`luxury-lakehouse[spadl]` wheel line does not match because the token
`"silly-kicks` never appears there.

**Update the docstring** to reflect the corrected rationale: the wheel's
`[spadl]` extra (not base deps) is the single source of truth. Add an inline
comment confirming the regex intentionally does not match the `[spadl]`
annotation on the wheel line.

### §4.2 `test_all_trainers_assert_silly_kicks_runtime_min`

Update expected constant from `(3, 0, 1)` to `(3, 7, 0)`.

## §5 — Phase 9 orchestrator fixes

All in `scripts/sk3_mig_b_retrain.py`. Currently uncommitted in working tree.

| # | Fix | Detail |
|---|-----|--------|
| 1 | Wheel version | 5 locations (preflight check, preflight status msg, preflight error msg, telemetry SQL, CycleState constructor) updated to new wheel version |
| 2 | Preflight env var gate | Accept `DATABRICKS_HTTP_PATH` as alternative to `DATABRICKS_WAREHOUSE_ID` (derivation happens after preflight) |
| 3 | Secrets dispatch | Use `state.warehouse_id` instead of `os.environ["DATABRICKS_WAREHOUSE_ID"]` (KeyError when var not set) |
| 4 | Group 0 dispatch merge | Steps 0a + 0b merged into single `_step_0a_group_0_inputs`. **Why**: Group 0 publishers (`publish_spadl_vaep_hf`, `publish_xg_shots_hf`, `publish_freeze_frame_hf`) use `spark.sql()` — cannot run locally via `uv run`. The merged step triggers the mega-job's `hf_sync` task (which runs all 10 sub-ops including the 3 publishers on Databricks runtime). Old `_step_0b_hf_sync_prereq` removed. `steps_in_order` updated from `["preflight","0a","0b","group_1",...]` to `["preflight","0a","group_1",...]`. `_step_already_at_or_past` updated accordingly. |
| 5 | SDK enum comparison | `task_run.state.life_cycle_state == "TERMINATED"` → `getattr(task_run.state.life_cycle_state, "value", None) == "TERMINATED"`. SDK returns `RunLifeCycleState.TERMINATED` (Python enum), not string. Same fix for `result_state`. |
| 6 | HF Jobs flavor | `cpu-large` → `cpu-xl` in `_FLAVOR_MAP` (vaep + f2v_v1). `cpu-large` removed from HF Jobs API. |

Fix 1 overlaps with the wheel version bump (§6). Fix 6 overlaps with the
trainer-side `VALIDATED_HF_FLAVOR` constants (already `cpu-xl` in trainers
from a prior fix; this aligns the orchestrator's `_FLAVOR_MAP`).

## §6 — Wheel version bump

Via `uv run python scripts/bump_wheel.py`. Touches 25+ files (pyproject.toml,
`src/shared/wheel.py`, 17 PEP 723 scripts, `deploy.sh`, 2 TF files).

The new version number will be determined at implementation time (next minor
after 0.3.33 → 0.3.34).

## §7 — Out of scope

- Boundary test updates (`test_silly_kicks_boundary.py`) — SPADL/VAEP surface
  unchanged in 3.7.0
- Adopting new silly-kicks 3.1–3.7 tracking features — future PR
- Running Phase 9 retrain cycle — separate operator action after this PR merges

## §8 — Verification

### §8.1 Local verification

1. `uv sync --extra spadl` resolves silly-kicks 3.7.0 (validates self-referential extra)
2. `uv sync --extra analytics` also resolves silly-kicks 3.7.0 (validates inheritance)
3. **`uv.lock` updated**: verify `uv.lock` (tracked in git, ~4400 lines) reflects
   the new `spadl` extra and silly-kicks 3.7.0 pin. `uv sync` updates it
   automatically; `git diff uv.lock` confirms the change.
4. `uv run ruff check src/ scripts/` — zero violations
5. `uv run pyright src/` — zero errors
6. `uv run pytest src/tests/test_sk3_mig_b_orchestrator_invariants.py -v` — all pass
7. `uv run pytest src/tests/test_terraform_env_dep_parity.py -v` — pass
8. `uv run pytest src/tests/test_silly_kicks_boundary.py -v` — pass (unchanged surface)

### §8.2 Remote verification (post-merge, Phase 9 resume)

9. After PR merges and post-merge CI is GREEN, resume Phase 9 with
   `--start-at group_1`. The vaep HF Job is the first Group 1 dispatch — its
   success confirms that `luxury-lakehouse[spadl]` resolves silly-kicks 3.7.0
   on HF Jobs runtime (the exact environment that triggered this PR).
10. If the vaep job still fails, inspect `hf jobs logs` for the dependency
    resolution output — verify `silly-kicks` appears in the installed packages
    list. Fallback: add `pip list | grep silly` as a diagnostic line at the top
    of the trainer's `main()`.
