# ScoutGPT L2 Seed Harvest Implementation Plan

> **Retroactive plan** — fire happened overnight (2026-04-20T02:48Z → 06:02Z) before this doc was written. Preserved here for the paper trail alongside the spec.

**Goal:** Evaluate the 4 unshipped L2 seeds in `src/evolve/targets/scoutgpt/seed_programs/` against the `additive` baseline at 15-epoch fidelity using the evolve evaluator, apply the pruning rule from the spec, document dispositions in `docs/evolve/scoutgpt-l2-harvest/SUMMARY.md`, and ship everything in a single atomic commit.

**Architecture:** Thin PEP 723 orchestration script invokes `evolve.targets.scoutgpt.evaluator.train_and_evaluate()` for each of 5 variants (4 L2 seeds + `additive` baseline) sequentially on one HF Jobs L40S instance; results written incrementally to `luxury-lakehouse/scoutgpt-l2-harvest` HF Hub repo.

**Tech Stack:** Python 3.10 / uv / PyTorch / HF Jobs L40S / huggingface_hub / existing evolve engine.

**Commit policy:** Per user rule — single atomic commit, explicit approvals for commit + push + PR.

**Reference spec:** `docs/superpowers/specs/2026-04-20-scoutgpt-l2-harvest-design.md`

---

## File map

| File | Action | Role |
|---|---|---|
| `scripts/evaluate_scoutgpt_l2_seeds.py` | Create | PEP 723 orchestration script |
| `docs/superpowers/specs/2026-04-20-scoutgpt-l2-harvest-design.md` | Create | Design doc |
| `docs/superpowers/plans/2026-04-20-scoutgpt-l2-harvest.md` | Create | This plan |
| `docs/evolve/scoutgpt-l2-harvest/SUMMARY.md` | Create | Evaluation results + dispositions |
| `docs/evolve/scoutgpt-l2-harvest/results.json` | Create | Mirror of HF Hub results |
| `src/evolve/targets/scoutgpt/seed_programs/*.py` | Delete | 0 files (ARCHIVE ratified, no deletions) |

No production code changes (no touches under `src/analytics/` or `src/ingestion/`).

---

## Phase A — Orchestration script (pre-fire)

### Task 1: Write the orchestration script

**Files:** Create `scripts/evaluate_scoutgpt_l2_seeds.py`

- [x] PEP 723 header pinning wheel `0.3.4` SHA `e2c1526…` plus dependencies: `numpy`, `pandas`, `pyarrow`, `torch`, `safetensors`, `huggingface-hub`, `scikit-learn`, `scipy`, **`openevolve>=0.2.0`** (required for the import chain `evolve.targets.scoutgpt.evaluator` → `evolve/__init__.py` → `EvolveEvaluator` → `openevolve.evaluation_result`)
- [x] `_VARIANTS` tuple hard-codes the 5 variants (`additive` as baseline with `program_path=None`, then the 4 L2 seeds each with their relative filename under `seed_programs/`)
- [x] `_SHARED_CONFIG` dict captures the L2 seeds' declared hyperparameters for consistent A/B
- [x] `_run_variant()` overrides `conditioning_type="additive"` for L2 variants (so unused cross-attention layers aren't allocated alongside the `_apply_program`-registered custom layers)
- [x] Per-variant metrics uploaded immediately to `luxury-lakehouse/scoutgpt-l2-harvest` so partial crashes still leave evidence
- [x] Combined sorted `results.json` uploaded after all variants complete
- [x] Non-zero exit if any variant errored, so the HF Jobs status stage lands ERROR and gets caught by the wake-up poll

### Task 2: Lint + static-check the script

- [x] `uv run ruff check scripts/evaluate_scoutgpt_l2_seeds.py` — all checks passed
- [x] `uv run ruff format --check scripts/evaluate_scoutgpt_l2_seeds.py` — already formatted
- [x] `uv run pyright scripts/evaluate_scoutgpt_l2_seeds.py` — exit 0

---

## Phase B — Fire (approval-gated)

### Task 3: [APPROVAL #1] Fire HF Jobs L40S

- [x] User approved (scope A ratified; cost estimate $7, actual $4)
- [x] `hf jobs uv run --flavor l40sx1 --timeout 360m --secrets HF_TOKEN=$HF_TOKEN --detach scripts/evaluate_scoutgpt_l2_seeds.py`
- [x] First attempt (`69e598afcd8c002f31dffc98`) ERRORed in 3 min — missing `openevolve` dep. Fix: add to PEP 723 header, refire.
- [x] Second attempt (`69e59c76cd8c002f31dffcc1`) — RUNNING confirmed at 3 min, progressed through all 5 variants.

### Task 4: Monitor + wait

- [x] 3-min wake: confirmed RUNNING after refire
- [x] 1h wake: 2/5 variants done, `fourier_cross_attention` showed `rho=+0.380` (early outlier)
- [x] 2h wake: 3/5 done
- [x] Final wake at 06:33 UTC: all 5 complete, job status COMPLETED

---

## Phase C — Results + SUMMARY (inline)

### Task 5: Download results from HF Hub

- [x] `hf_hub_download(repo_id="luxury-lakehouse/scoutgpt-l2-harvest", filename="results.json", ...)`
- [x] Mirror to `docs/evolve/scoutgpt-l2-harvest/results.json` for inclusion in the commit

### Task 6: Apply pruning rule per variant

- [x] Apply spec rule: `IF fitness < baseline AND fitness < 0.63 → PRUNE; ELSE IF fitness < baseline → ARCHIVE; ELSE → FLAG FOR PROMOTION`
- [x] Resolve 0.63 ambiguity: user ratified ARCHIVE (spirit interpretation — 0.63 is top1 floor; orthogonal + hybrid have top1=0.842, above floor)
- [x] No seed files deleted in this cycle

### Task 7: Write SUMMARY.md

- [x] `docs/evolve/scoutgpt-l2-harvest/SUMMARY.md` — full metrics table, cross-reference to session-50 findings, disposition table, follow-ups

### Task 8: Write retroactive spec + plan

- [x] `docs/superpowers/specs/2026-04-20-scoutgpt-l2-harvest-design.md`
- [x] `docs/superpowers/plans/2026-04-20-scoutgpt-l2-harvest.md` (this file)

---

## Phase D — Ship (approval-gated)

### Task 9: Final full-suite sweep

- [ ] `uv run ruff check src/ scripts/`
- [ ] `uv run ruff format --check src/ scripts/`
- [ ] `uv run pyright src/ scripts/` (expecting 0 errors — no src changes in this cycle)
- [ ] `uv run pytest src/tests/ --benchmark-disable -q` (expecting no regressions — no production code touched)
- [ ] `git status -s` — expected set: 2 new files under `docs/evolve/scoutgpt-l2-harvest/`, 2 new files under `docs/superpowers/`, 1 new script. Nothing else.

### Task 10: [APPROVAL #2] Single commit

- [ ] Stage exact file list (no `git add -A`)
- [ ] Commit with heredoc message referencing the fourier finding + dispositions

### Task 11: [APPROVAL #3] Push + open PR

- [ ] `git push -u origin evolve/scoutgpt-l2-harvest`
- [ ] `gh pr create --title "chore(evolve): harvest ScoutGPT L2 seeds (evaluate + archive)" --body …`
- [ ] Wait for CI to go green (lint-and-test + semgrep + plan) — should be ~6 min end-to-end given the new `concurrency` blocks from PR #160

### Task 12: Post-merge memory update (separate cycle)

- [ ] Follow the established pattern: update MEMORY.md "IN-FLIGHT" banner → move cycle to "Cycle Completion" one-liner. Remove `project_scoutgpt_l2_harvest_inflight.md` (superseded by the committed cycle-complete record; or rename to `_closed` suffix).

---

## Self-review

- **Spec coverage:** every cycle task maps back to the spec's Evaluation Protocol, Pruning Rule, or Deliverables sections.
- **No placeholders:** every step has concrete commands or explicit "completed" markers for pre-commit work already done.
- **Type consistency:** `fitness`, `rho`, `top1` names match across the orchestration script, SUMMARY.md, and spec.
- **Scope discipline:** no production code changes, no wheel bump (wheel 0.3.4 already shipped for RoPE cycle), no new dependencies beyond the PEP 723 addition of `openevolve>=0.2.0` (already in the `evolve` extra in `pyproject.toml`).
