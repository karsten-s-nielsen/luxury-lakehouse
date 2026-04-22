# ScoutGPT `cross_attention` Promotion + Fourier Retention Cycle

**Date:** 2026-04-21
**Branch:** `evolve/scoutgpt-cross-attn-promote`
**Status:** Design approved; implementation plan pending (writing-plans skill next).
**Cycle-following:** ScoutGPT Fourier / Swiglu Promotion A/B (PR #166, session 52). That cycle promoted `fourier_cross_attention` as a first-class enum value but left a surprise finding: Arm 5 (`cross_attention` alone, NO Fourier spatial features) achieved rho = +0.2995 vs Arm 2 (`fourier_cross_attention`) rho = +0.2812 — the cross-attention conditioning mechanism, not the Fourier spatial encoding, appears to be the primary rho driver. This cycle validates that hypothesis at production fidelity and settles Fourier's fate.

## Problem

The PR #166 A/B at production fidelity (30 epochs, 256d/6L/8h) produced the following Arm 5 vs Arm 2 evidence:

| Arm | conditioning_type | `counterfactual_rho` | `test_top1` | `val_loss` |
|---|---|---:|---:|---:|
| Arm 1 (control) | `additive` | 0.1372 | 0.8153 | 0.4654 |
| Arm 2 | `fourier_cross_attention` | 0.2812 | 0.8368 | 0.4048 |
| Arm 5 | `cross_attention` alone | 0.2995 | 0.8410 | 0.3956 |

Decomposed mechanism contributions (PR #166 SUMMARY):
- **Cross-attention conditioning contribution** (Arm 5 − Arm 1): +0.162 rho, +0.026 top1
- **Fourier spatial contribution** (Arm 2 − Arm 5): −0.018 rho, −0.004 top1 — both within rho_std ~0.30 noise envelope

Two open questions this cycle addresses:

1. **Should `ScoutGPTConfig.conditioning_type` default flip from `"additive"` to `"cross_attention"`?** The current default is still `"additive"` (line 39 of `src/analytics/scoutgpt_decoder.py`). PR #166 added enum values but did not flip defaults. Arm 5's PR #166 data would trigger PROMOTE under the pre-registered rule (rho delta +0.1623 ≥ +0.10, top1 delta +0.0257 ≥ −0.005), but Arm 5 ran under GPU contention that session — two concurrent training runs shared the 5070 Ti for ~6 hrs. A clean re-run is warranted before committing to a default flip.
2. **What is Fourier's ultimate fate?** PR #166's Arm 5 vs Arm 2 evidence suggests `cross_attention` alone ≥ `fourier_cross_attention`, but the +0.018 rho gap was within noise and can't drive a deprecation decision on its own.

**Prior art:**
- Fourier / Swiglu promotion cycle (PR #166, 2026-04-21) — same A/B template, same decision-rule discipline. Spec: `docs/superpowers/specs/2026-04-20-scoutgpt-fourier-cross-attention-promote-design.md`. SUMMARY: `docs/evolve/fourier-scoutgpt/SUMMARY.md`.
- RoPE-for-ScoutGPT A/B (PR #159, 2026-04-19) — rho delta +0.016 rejected as within noise. Establishes the threshold floor.

## Non-goals

- **Canonical checkpoint retraining** on HF Hub (`luxury-lakehouse/scoutgpt`). The current HF Hub checkpoint has `conditioning_type="additive"` serialized in its config JSON, so it continues to load correctly after a default flip (explicit value overrides new default). Retraining a new canonical checkpoint under the new default is a separate follow-up cycle.
- **Hard removal of `fourier_cross_attention`** from the enum. Soft deprecation (DeprecationWarning) only — preserves backward compat for saved checkpoints and explicit users.
- **Multi-seed variance estimation.** Single seed (42) matches PR #166 and RoPE cycles. A second seed (43) would triple compute for a noise-estimate we can infer indirectly from PR #166's concurrent-run evidence.
- **Fourier hyperparameter re-tuning** (`n_freqs`, spatial encoding alternatives). Not scope.
- **Spatial-encoding × conditioning-type axis decomposition.** Future refactor; backward-compat loader shim deferred.
- **HF Jobs compute.** Explicitly local-only this cycle (AI-PC primary 5070 Ti + Media-PC secondary 5070 Ti). HF Hub is used only for read-only dataset streaming (revision-pinned).
- **Football2Vec cross-attention port.** Separate question; ScoutGPT-only this cycle.

## Scope — 3-arm A/B at production fidelity, local execution (2 machines)

Chosen after the user (a) rejected the cheaper "solo Arm 5 re-run reusing existing Arm 1" scope in favour of a fresh-A/B for env cleanliness, and (b) added Fourier's fate to the cycle rather than deferring it to a separate A/B. Final framing: one paired decision set under a single sweep, two independent pre-registered rules.

## A. Architecture changes to `ScoutGPTDecoder`

No new enum values. No new decoder branches. Surface area is intentionally tiny.

### A.1 Default value flip — `src/analytics/scoutgpt_decoder.py:39` (conditional on Arm 5 PROMOTE)

```python
@dataclass(frozen=True)
class ScoutGPTConfig:
    ...
    conditioning_type: str = "cross_attention"  # was "additive"
```

One-character semantic line change. Applied only if the pre-registered promotion rule fires (Section C.1).

### A.2 `ScoutGPTConfig.__post_init__` DeprecationWarning — `src/analytics/scoutgpt_decoder.py` (conditional on Fourier DEPRECATE)

```python
def __post_init__(self) -> None:
    if self.conditioning_type == "fourier_cross_attention":
        import warnings
        warnings.warn(
            "conditioning_type='fourier_cross_attention' is deprecated as of wheel 0.3.10; "
            "cross_attention is preferred (see docs/evolve/cross-attention-promote/SUMMARY.md).",
            DeprecationWarning,
            stacklevel=2,
        )
```

Frozen dataclasses support `__post_init__`. The warning is soft (`warnings.warn`, not exception) — enum value retained, saved checkpoints continue to load, explicit users see the warning and can migrate at their own pace.

### A.3 New pre-registered retention rule — `src/analytics/promotion_rules.py` (always added)

```python
RETENTION_DEPRECATE_RHO_THRESHOLD: float = 0.05


def apply_retention_rule(
    rho_incumbent: float,
    rho_challenger: float,
    top1_incumbent: float,
    top1_challenger: float,
) -> Literal["KEEP", "DEPRECATE"]:
    """Pre-registered retention rule for existing mechanisms facing a better alternative.

    Returns DEPRECATE iff the challenger clearly beats the incumbent:
      - rho_challenger - rho_incumbent >= +0.05 AND
      - top1_challenger - top1_incumbent >= -0.005 (safety floor, same as promotion rule)
    Otherwise KEEP.

    Asymmetric with apply_decision_rule by design: removing an existing mechanism
    requires positive evidence of a better alternative, not absence of evidence.
    The +0.05 rho threshold is the natural "parity threshold" at which two arms
    are considered within noise of each other; +0.10 (the promotion threshold)
    would effectively retain any incumbent under almost any challenger.
    """
    rho_delta = rho_challenger - rho_incumbent
    top1_delta = top1_challenger - top1_incumbent
    rho_ok = rho_delta >= RETENTION_DEPRECATE_RHO_THRESHOLD
    top1_ok = top1_delta >= TOP1_REGRESSION_FLOOR
    if rho_ok and top1_ok:
        return "DEPRECATE"
    return "KEEP"
```

Lives next to the existing `apply_decision_rule`; reuses the existing `TOP1_REGRESSION_FLOOR` constant. Added to the codebase unconditionally — it is a reusable pre-registered tool for any future ScoutGPT retention question.

### A.4 Conditional code-change matrix

| Arm 5 (flip) | Fourier (retention) | A.1 default flip | A.2 deprecation warning |
|---|---|:---:|:---:|
| PROMOTE | DEPRECATE | ✓ | ✓ |
| PROMOTE | KEEP | ✓ | — |
| ARCHIVE | DEPRECATE | — | ✓ |
| ARCHIVE | KEEP | — | — |

In all four outcomes, A.3 (retention rule function) is added. Wheel bump (Section E.1) is required iff any of A.1 or A.2 applies.

### A.5 Invariants

- `_encode`, `forward`, `predict`, `train_loop`, and all conditioning_type branches in `_embed` are unchanged. This cycle only touches defaults and warnings.
- `position_embedding="learnable"` held constant across all arms (same as PR #166).
- Dataset, split sizes, batch size, epochs all unchanged from PR #166.

## B. A/B run plan

### B.1 Arm roster

| Arm | Name | conditioning_type | hidden_dim | num_layers | num_heads | Role | Machine |
|---|---|---|---:|---:|---:|---|---|
| 1 | CONTROL | `additive` | 256 | 6 | 8 | Current production default | Media-PC (post-smoke) |
| 5 | CROSS-ATTN | `cross_attention` | 256 | 6 | 8 | Default flip candidate | AI-PC (first serial) |
| 2 | FOURIER | `fourier_cross_attention` | 256 | 6 | 8 | Retention candidate | AI-PC (second serial) |

Arm numbering intentionally matches PR #166 for cross-reference.

### B.2 Shared config across all arms

| Setting | Value |
|---|---|
| `position_embedding` | `learnable` |
| `dropout` | 0.10 |
| `learning_rate` | 1e-4 |
| `batch_size` | 256 |
| `vaep_loss_weight` | 0.10 |
| `epochs` | 30 |
| `patience` | 5 |
| `seed` | 42 |
| Dataset | `luxury-lakehouse/scoutgpt-training-data`, revision-pinned at dispatch start |
| Train / val / test split | 715,520 / 89,440 / 89,441 |
| Counterfactual eval | 1000 episodes × 100 players |

### B.3 Branch + Media-PC workdir

- Working branch: `evolve/scoutgpt-cross-attn-promote` (off `main @ db4f2a9`)
- Media-PC workdir: `~/luxury-lakehouse-cross-attn/` (fresh path, decoupled from PR #166's `~/luxury-lakehouse-fourier-promote/`)

### B.4 Machine assignment rationale

- **AI-PC** (primary workstation, RTX 5070 Ti, Windows 11 + Git Bash): tested platform — PR #166 Arms 2/3/5 ran there successfully. No env validation needed. Runs Arm 5 first (primary treatment — the default flip hinges on this), then Arm 2 serially (single GPU, no parallel same-machine dispatch per the PR #166 contention lesson).
- **Media-PC** (`super@192.168.68.70`, RTX 5070 Ti, Ubuntu/WSL per user — brand new SSH/Ubuntu setup): new to ScoutGPT training. Runs Arm 1 (control) after passing the smoke test (Section B.5). Assigning the control arm to the unvalidated machine minimizes primary-decision risk: if Media-PC drifts numerically, it shifts the control baseline, which the pre-registered rule handles as-is (Arm 5 either clears the threshold against the shifted baseline or doesn't).
- Long-term intent: Media-PC becomes a trusted second GPU for future cycles. This cycle doubles as Media-PC's inaugural validation.

### B.5 Smoke test — Media-PC only (BLOCKING gate)

AI-PC is tested; skipping its smoke is intentional (redundant).

**Pre-flight checks** (~30 seconds via SSH):
1. SSH reachable: `ssh super@192.168.68.70 "echo ok"`.
2. Torch + CUDA + GPU: `ssh super@192.168.68.70 "source ~/venv-fourier/bin/activate && python -c 'import torch; print(torch.__version__, torch.cuda.is_available(), torch.cuda.get_device_name(0))'"` — assert `torch.cuda.is_available() == True` and `device_name` contains `5070 Ti`.
3. HF_TOKEN set (orchestrator passes explicitly via env if needed).

**Training smoke** (~5–10 min wall): 2 epochs × ~2000 episodes of Arm 1 config (additive) on Media-PC.

Pass criteria (ALL must hold):
- Training subprocess exits with code 0.
- Train loss decreases monotonically epoch-over-epoch (loss[end] < loss[start]).
- `metrics.json` written to local output dir with required keys: `counterfactual_rho`, `test_top1`, `val_loss`, `wall_min`.
- No CUDA OOM, no dtype mismatch, no dataset-access errors in stderr.

**On smoke fail**: orchestrator HALTS. Emits diagnostic dump (last 100 stderr lines, `python -c "import torch; ..."` output, `nvidia-smi` state). User decides: (a) fix Media-PC env and retry smoke; (b) fall back to serial A/B on AI-PC only (all 3 arms sequential on AI-PC, ~9 hrs wall clock instead of ~6).

### B.6 Dataset revision pinning

Same pattern as PR #166:
- Orchestrator calls `huggingface_hub.HfApi().repo_info("luxury-lakehouse/scoutgpt-training-data", repo_type="dataset").sha` at dispatch start.
- Writes `artifacts/cross-attn-promote/dispatch-manifest.json` with fields: `dispatch_timestamp_utc`, `dataset_sha`, `arm_roster`, `seed`, `branch_commit_sha`.
- Every `--mode run-arm` invocation receives `--dataset-revision <sha>` and asserts the resolved revision matches at training time.
- All 3 arms see byte-identical training data even if someone pushes to the dataset mid-cycle.

### B.7 Orchestrator — `scripts/run_cross_attention_ab.py` (new, local Python, not PEP 723)

Imports `train_loop` from the source tree directly (`from analytics.scoutgpt_training import train_loop`), bypassing the PEP 723 wheel fetch — same pattern as PR #166's orchestrator. Avoids the chicken-and-egg of needing a new wheel before the cycle that justifies the wheel bump.

Two modes:

- **`--mode drive` (default)**: top-level driver. Resolves dataset SHA → pre-flight checks on Media-PC → smoke test on Media-PC → dispatches arms per B.1 → polls per-machine log files every 30s → generates `docs/evolve/cross-attention-promote/SUMMARY.md` → applies `apply_decision_rule` + `apply_retention_rule` → conditionally stages A.1/A.2 code changes based on rule outcomes.

- **`--mode run-arm --arm <name> --local-output-dir <path> --dataset-revision <sha>`**: single-arm execution. Invoked from `--mode drive`:
  - **AI-PC arms (5, 2)**: subprocess call to the same script with `run-arm` mode, writes metrics to `artifacts/cross-attn-promote/<arm>/`.
  - **Media-PC Arm 1**: `ssh super@192.168.68.70 "cd ~/luxury-lakehouse-cross-attn && source ~/venv-fourier/bin/activate && nohup python scripts/run_cross_attention_ab.py --mode run-arm ... &>> run-arm.log &"` with `PYTHONPATH=./src` so uncommitted source-tree code is authoritative.

Orchestrator is re-attachable: SSH session drops don't kill training (nohup + file redirect). Training state persists via per-epoch checkpoint + `metrics.json` writes.

### B.8 Media-PC deployment flow

All code deploys from AI-PC. No manual edits on Media-PC.

1. On AI-PC: `git checkout -b evolve/scoutgpt-cross-attn-promote` off `main @ db4f2a9`.
2. Stage all pre-A/B code changes (A.3 retention rule + orchestrator script + retention rule tests + this spec doc) in the working tree. **No commit** — per the single-commit-at-end rule in CLAUDE.md.
3. Tar-pipe sync to Media-PC (Git Bash lacks rsync per `feedback_orchestration_lessons.md`):

   ```bash
   tar czf - --exclude=.git --exclude=artifacts --exclude=__pycache__ --exclude='*.pyc' . \
     | ssh super@192.168.68.70 "mkdir -p ~/luxury-lakehouse-cross-attn && cd ~/luxury-lakehouse-cross-attn && tar xzf -"
   ```

4. Set `PYTHONPATH=./src` on every Media-PC invocation (per `feedback_orchestration_lessons.md`: avoids installed-wheel conflicts while testing uncommitted code).
5. Pre-flight + smoke + Arm 1 dispatch (per B.5–B.7).

### B.9 GPU contention prevention (reprise-of-PR-#166-bug guard)

Mitigations in the orchestrator preamble:
- `mkdir -p "$(dirname $LOG_FILE)"` before every `tee` redirect — the original PR #166 Arm 5 concurrent-dispatch bug was caused by a missing log directory causing tee to fail while the training subprocess kept running.
- `pgrep -f "run-arm.*--arm" | grep -v $$` check before dispatching a new arm on AI-PC — halt if another ScoutGPT training is already running. User must kill it manually before continuing.
- On orchestrator crash recovery: user manually checks `pgrep -af scoutgpt` before restarting the driver, because Windows Popen children survive parent death without a job object (per `feedback_orchestration_lessons.md`).

### B.10 Cost

- Compute: $0 (electricity only, two local 5070 Tis).
- Wall time: target ~6 hrs (see B.11 timeline). If smoke fails and we fall back to serial AI-PC, ~9 hrs.

### B.11 Execution timeline (happy path — smoke passes, no re-dispatches)

| Time | Event |
|---|---|
| T+0min | Orchestrator start; dataset SHA resolved; manifest written |
| T+1min | Tar-pipe sync to Media-PC complete |
| T+2min | Pre-flight checks pass on Media-PC |
| T+10min | Smoke test complete; gate passed |
| T+11min | Arm 1 (Media-PC) + Arm 5 (AI-PC) dispatched in parallel |
| T+3hr | Arm 1 done (Media-PC idle henceforth); Arm 5 done on AI-PC; Arm 2 dispatched on AI-PC |
| T+6hr | Arm 2 done; all metrics.json in place |
| T+6:05 | SUMMARY.md generated; both rules applied; conditional code changes staged |
| T+6:15 | Full pytest + ruff + pyright + ruff format check; wheel bump run if needed |
| T+6:20 | Ready for user review → commit → PR |

Cycle 2 (XG2 production fix) is a natural parallel task during the T+11min → T+6hr polling window.

## C. Decision rules (both pre-registered)

Both rules live in `src/analytics/promotion_rules.py` as pure functions with no state. SUMMARY.md generation calls them — no motivated-reasoning possible at disposition time.

### C.1 Default flip rule (reused from PR #166, unchanged)

For Arm 5 (`cross_attention`) vs Arm 1 (`additive`):

```python
apply_decision_rule(
    rho_ctrl=arm1.counterfactual_rho,
    rho_trt=arm5.counterfactual_rho,
    top1_ctrl=arm1.test_top1,
    top1_trt=arm5.test_top1,
)  # -> Literal["PROMOTE", "ARCHIVE"]
```

**PROMOTE** iff `rho_trt - rho_ctrl >= +0.10` AND `top1_trt - top1_ctrl >= -0.005`. Otherwise **ARCHIVE**.

Thresholds calibrated in PR #166:
- +0.10 rho threshold is ≥0.33σ at `rho_std ≈ 0.30`, and 6× the RoPE-rejected margin of +0.016.
- −0.005 top1 safety floor prevents catastrophic next-action regression.

PR #166's contention-affected Arm 5 data would PROMOTE under this rule (rho delta +0.1623, top1 delta +0.0257). A clean re-run validates whether that direction holds.

### C.2 Fourier retention rule (new, pre-registered)

For Fourier (`fourier_cross_attention`) facing its challenger `cross_attention`:

```python
apply_retention_rule(
    rho_incumbent=arm2.counterfactual_rho,
    rho_challenger=arm5.counterfactual_rho,
    top1_incumbent=arm2.test_top1,
    top1_challenger=arm5.test_top1,
)  # -> Literal["KEEP", "DEPRECATE"]
```

**DEPRECATE** iff `rho_challenger - rho_incumbent >= +0.05` AND `top1_challenger - top1_incumbent >= -0.005`. Otherwise **KEEP**.

Threshold calibration:
- +0.05 rho threshold: half the promotion threshold. Asymmetric by design — removing an existing mechanism requires positive evidence of a better alternative, not absence of evidence. +0.05 is the natural "parity threshold" at which two arms are within noise of each other.
- −0.005 top1 safety floor: same as promotion rule (same semantics — don't remove something catastrophically better on top1).

PR #166's Arm 5 vs Arm 2 data (rho delta +0.018, top1 delta +0.004) would KEEP Fourier — within-noise evidence alone doesn't drive deprecation. This is the intended behavior of the rule. Clean re-runs at matched conditions may or may not produce a larger separation.

### C.3 Null-cycle outcome

If both rules return the "no action" verdict (Arm 5 ARCHIVE + Fourier KEEP):
- No code changes. No wheel bump.
- SUMMARY.md is still generated — documents the clean re-run data and why the PR #166 directional signals didn't reach the promotion threshold under clean conditions.
- Cycle archives with a commit containing only: `docs/superpowers/specs/...` (this file), `docs/superpowers/plans/...`, `docs/evolve/cross-attention-promote/SUMMARY.md`, `src/analytics/promotion_rules.py` (retention rule always lands as a reusable tool), `src/tests/test_retention_rule.py`, and `scripts/run_cross_attention_ab.py`.

## D. Validation & testing

### D.1 `src/tests/test_retention_rule.py` (new, always added)

Pure-function parametrized tests covering:
- Exactly +0.05 rho with no top1 regression → DEPRECATE (boundary).
- +0.049 rho → KEEP (just below threshold).
- +0.05 rho BUT top1 regression −0.01 → KEEP (safety floor triggers).
- +0.05 rho with top1 regression exactly −0.005 → DEPRECATE (boundary).
- Clear deprecate case (+0.10 rho, no top1 loss) → DEPRECATE.
- RoPE historical case (rho delta +0.016) → KEEP.
- PR #166 Arm 5 vs Arm 2 historical case (rho delta +0.018, top1 delta +0.004) → KEEP (confirms the rule doesn't retroactively second-guess PR #166's evidence).

### D.2 Extensions to `src/tests/test_scoutgpt_decoder.py` (conditional on rule outcomes)

- `test_default_conditioning_type` — asserts `ScoutGPTConfig().conditioning_type` matches the cycle's intended default (`"additive"` on ARCHIVE, `"cross_attention"` on PROMOTE). Locks in intent; prevents silent drift in future PRs.
- `test_fourier_deprecation_warning` (added iff Fourier DEPRECATE) — `with pytest.warns(DeprecationWarning, match="fourier_cross_attention")`, constructs `ScoutGPTConfig(conditioning_type="fourier_cross_attention")` and asserts the warning fires. Asserts the enum value is still accepted (soft deprecation, not removal).

### D.3 No parity tests needed

`cross_attention` and `fourier_cross_attention` branches already exist and were validated in PR #166's parity tests (`test_scoutgpt_fourier_parity.py` still passes on this branch). This cycle changes no decoder-branch internals.

### D.4 Pre-existing usage audit (BLOCKING before A.2 lands)

Before adding the `fourier_cross_attention` DeprecationWarning, grep the entire repo for any code that constructs `ScoutGPTConfig(conditioning_type="fourier_cross_attention")`:

```bash
grep -rn "fourier_cross_attention" --include="*.py" src/ scripts/ hf_taipy_app/
```

Expected hits:
- `src/analytics/scoutgpt_decoder.py` — the branch itself + `__post_init__` warning.
- `src/evolve/targets/scoutgpt/seed_programs/fourier_cross_attention.py` — the seed file. Does NOT construct a `ScoutGPTConfig` — safe.
- Tests under `src/tests/` — must not break under the new warning.

If any production call site exists that triggers the warning in a hot loop (unlikely, but verify), use `warnings.filterwarnings("once", category=DeprecationWarning, module="...")` at module boundary to prevent log flooding.

### D.5 Pre-commit gates (zero violations)

- `uv run ruff check src/ scripts/`
- `uv run ruff format --check src/ scripts/`
- `uv run pyright src/`
- `uv run pytest src/tests/ -v`

### D.6 Test gate ordering

1. Write `test_retention_rule.py` (TDD — fails until `apply_retention_rule` is added).
2. Land `apply_retention_rule` in `src/analytics/promotion_rules.py`. Tests go green.
3. Full test suite passes locally.
4. Smoke test on Media-PC (Section B.5).
5. Full 3-arm A/B.
6. Generate SUMMARY.md; apply both decision rules.
7. Conditionally apply A.1 / A.2 code changes based on rule outcomes.
8. Add conditional tests (`test_default_conditioning_type` updated value; `test_fourier_deprecation_warning`).
9. Wheel bump if A.1 or A.2 landed.
10. Full test suite + ruff + pyright one more time before commit.

## E. Artefacts & governance

### E.1 Wheel bump `0.3.9 → 0.3.10` (conditional on A.1 or A.2 landing)

Required if the default value changes (A.1) or the `__post_init__` warning is added (A.2). Both are semantic API changes — consumers pinned to 0.3.9 and below would continue to see the old behavior.

- Update `[project] version` in `pyproject.toml`.
- Update `WHEEL_VERSION` + `WHEEL_FILENAME` in `src/shared/wheel.py`.
- Run `uv run python scripts/bump_wheel.py` to propagate across PEP 723 scripts + Terraform + `deploy.sh`.
- `bump_wheel.py --check` enforced by CI on main.
- No `--pin-hash` — CI uploads the new wheel on main merge; hash pin is a follow-up if needed.

Null-cycle outcome (both rules no-action): no wheel bump; the retention-rule-only code addition is a pure internal tool and does not warrant a version bump.

### E.2 `ARCHITECTURE.md` Appendix D — no change

No new academic reference this cycle. `cross_attention` conditioning uses the existing Hong et al. (2025) citation already present in Appendix D and in `workflow-cards/wf-scoutgpt.yaml`.

### E.3 `AI_GOVERNANCE.md` — no change

ScoutGPT is system #13 in §5 with status Development. This cycle is a pure architectural internal change (flip a default, add a deprecation warning, add a reusable decision-rule function). It does not alter:
- Intended use (counterfactual evaluation of per-player decisions) — unchanged.
- Non-use clauses (no employment decisions, no club customer) — unchanged.
- Data processed (SPADL episodes) — unchanged.
- Deployment context (research only) — unchanged.
- §13 re-classification triggers — none fire.

### E.4 `workflow-cards/wf-scoutgpt.yaml` — no change

No new `references:` entries; Hong 2025 already cited.

### E.5 `docs/evolve/cross-attention-promote/SUMMARY.md` (new, written post-A/B)

Mirrors PR #166's `docs/evolve/fourier-scoutgpt/SUMMARY.md` structure:
- Run metadata (timestamps, branch, machines, dataset SHA).
- Headline table (3 arms: rho, rho_std, top1, val_loss, fitness, param_count, wall_clock).
- Cross-reference to PR #166 (Arm 5 contention caveat + clean re-run result) and RoPE A/B (baseline rho context).
- Pre-registered decision rules (both) quoted verbatim.
- Dispositions via `apply_decision_rule(...)` and `apply_retention_rule(...)` output.
- Mechanism narrative: did clean Arm 5 match PR #166's Arm 5 rho (contention was not the driver) or not?
- Follow-ups: canonical checkpoint retraining (deferred), Football2Vec cross-attention port (separate cycle).

## F. Risks & mitigations

| # | Risk | Mitigation |
|---|---|---|
| 1 | Media-PC smoke test fails | Halt, diagnostic dump. Fall back to serial A/B on AI-PC only (3 arms sequential, ~9 hrs wall) |
| 2 | GPU contention recurs on AI-PC (reprise of PR #166 Arm 5 bug) | Orchestrator preamble: `mkdir -p` before every `tee`; `pgrep -f scoutgpt_training` pre-dispatch check; crash-recovery manual check of surviving Popen children before restart |
| 3 | Clean Arm 5 rho drops below +0.237 (i.e., PR #166 contention inflated it) | Valid ARCHIVE disposition. Cycle still useful — quantifies the contention artifact. Fourier retention decision still applies independently on the clean Arm 2 vs clean Arm 5 data |
| 4 | Arm 2 clean re-run diverges substantially from PR #166's 0.2812 | New clean run is authoritative for retention decision; PR #166 data becomes cross-check, documented in SUMMARY |
| 5 | Wheel bump breaks PEP 723 / Terraform / `deploy.sh` consumers | `bump_wheel.py --check` in CI catches drift; propagation across `_HASH_CONSUMER_GLOBS` + `_VERSION_ONLY_CONSUMER_GLOBS` is handled by the script |
| 6 | DeprecationWarning breaks existing tests that construct `ScoutGPTConfig(conditioning_type="fourier_cross_attention")` | Pre-audit with D.4 grep; `warnings.warn` (not exception) keeps the code path functional; full test suite run after A.2 lands |
| 7 | Null-cycle outcome (neither rule fires) | Valid — no A.1, no A.2, no wheel bump. Retention rule function still lands as a reusable tool. SUMMARY archives the data for future reference |
| 8 | Media-PC torch version drifts from AI-PC (`venv-fourier` has torch nightly cu128; AI-PC has project `uv` torch) | Smoke test verifies Media-PC's env produces reasonable-looking training metrics. Cross-machine env-drift absorbed into the baseline (Arm 1 on Media-PC) — promotion rule handles it as-is. If drift is meaningful, shows up as Arm 1 rho differing from PR #166's Arm 1 rho (0.1372) — documented in SUMMARY if so |
| 9 | SSH drop during Media-PC Arm 1 training | `nohup` + file-based log redirect; training state persists via checkpoint + metrics writes. Orchestrator polls log file, can re-attach |

## G. Deliverables (single commit at end of cycle, pending user approval)

### G.1 New files

- `src/tests/test_retention_rule.py` — pure-function tests for `apply_retention_rule`
- `scripts/run_cross_attention_ab.py` — local orchestrator (not PEP 723)
- `docs/superpowers/specs/2026-04-21-scoutgpt-cross-attention-promote-design.md` (this file)
- `docs/superpowers/plans/2026-04-21-scoutgpt-cross-attention-promote.md` (writing-plans skill output, next phase)
- `docs/evolve/cross-attention-promote/SUMMARY.md` (written post-A/B)

### G.2 Modified files

- `src/analytics/promotion_rules.py` — add `apply_retention_rule` + `RETENTION_DEPRECATE_RHO_THRESHOLD` (always)
- `src/analytics/scoutgpt_decoder.py` — A.1 default flip (conditional) + A.2 `__post_init__` warning (conditional)
- `src/tests/test_scoutgpt_decoder.py` — `test_default_conditioning_type` + `test_fourier_deprecation_warning` (conditional)
- `pyproject.toml` — version 0.3.9 → 0.3.10 (conditional on A.1 or A.2)
- `src/shared/wheel.py` — WHEEL_VERSION + WHEEL_FILENAME (conditional)
- Files touched by `bump_wheel.py` — PEP 723 consumer headers + Terraform wheel paths + `deploy.sh` (conditional)

### G.3 Gitignored

- `artifacts/cross-attn-promote/**` — per-arm checkpoints, metrics.json, dispatch manifest, smoke test output

## H. Approval log

- 2026-04-21 — Cycle selection (cross_attention promotion, Cycle 1 of queue) approved.
- 2026-04-21 — Q1 (scope): Option B (fresh A/B) approved over Option A (solo Arm 5 re-run) — user rejected reusing existing Arm 1 data for methodological cleanliness.
- 2026-04-21 — Q2 (execution): Option 3 (smoke Media-PC first, parallel if pass) approved. User correction: AI-PC is tested platform; Media-PC is new — smoke test validates Media-PC as long-term trusted second GPU; code deploys from AI-PC.
- 2026-04-21 — Q3 (scope boundary): Fourier's ultimate fate IN scope this cycle; canonical checkpoint retraining deferred to separate cycle.
- 2026-04-21 — Q4 (3-arm roster + retention rule): approved. Roster: Arm 1 (additive/Media-PC), Arm 5 (cross_attention/AI-PC), Arm 2 (fourier_cross_attention/AI-PC serial). Decision rules: `apply_decision_rule` (reused) + `apply_retention_rule` (new, +0.05 rho threshold, soft deprecation).
- 2026-04-21 — Section A (architecture) approved.
- 2026-04-21 — Section B (A/B run plan) approved.
- 2026-04-21 — Section C (validation, artefacts, risks, deliverables) approved.
- 2026-04-21 — Spec doc written and committed (this file).

## I. Follow-ups (deferred)

- **Canonical checkpoint retraining** on HF Hub (`luxury-lakehouse/scoutgpt`). If Arm 5 PROMOTE lands, retrain the production checkpoint under the new `cross_attention` default. Separate cycle.
- **Spatial-encoding × conditioning-type axis decomposition**. Today `fourier_cross_attention` bundles two architectural decisions; future refactor splits them into independent config axes with a loader shim for backward compat. Deferred.
- **Football2Vec cross-attention port**. Does cross-attention conditioning help Football2Vec's MLM task too? Separate cycle.
- **Hard removal of deprecated `fourier_cross_attention`**. If the soft deprecation lands and no one pushes back for 3+ months, consider hard removal in a subsequent cycle. Separate cycle.
