# EV2 — Football2Vec v2 L2 Adversarial Architecture Search

**Date:** 2026-04-23
**Branch:** `evolve/football2vec-l2-adversarial`
**Status:** Design approved, pre-registered; implementation pending
**TODO entry:** EV2 (user-selected as next cycle in session 54)
**Cycle-following:** XG2 production unblock + ADR-012 (PR #177, session 54). Prior evolve cycles: EV1 (PR #158), RoPE-for-ScoutGPT (PR #159/#160), ScoutGPT L2 harvest (PR #163), Fourier+Swiglu promotion (PR #166), cross-attention promotion (PR #176).

## Problem

`Football2VecEncoder` stage-2 training (`scripts/train_football2vec_v2.py::_train_stage2_loop`) applies a Ganin-2016 gradient-reversal adversary to debias competition identity out of the learned embeddings. The current architecture is:

- `TeamClassifierHead` = `GradientReversalLayer → Linear(hidden_dim, num_competitions)` — single layer, no hidden, no activation
- GRL receives the **pooled CLS** sequence embedding `(B, hidden_dim)` — not per-token output
- Lambda schedule: **linear ramp** `lam = ADVERSARIAL_LAMBDA_MAX · min(epoch / ADVERSARIAL_WARMUP_EPOCHS, 1.0)` with `ADVERSARIAL_LAMBDA_MAX=0.2`, `ADVERSARIAL_WARMUP_EPOCHS=5`

These values were chosen at the time the stage-2 debiasing was introduced and have never been searched. The ScoutGPT L2 harvest (PR #163) demonstrated that reduced-fidelity evolve-scale evaluation reliably surfaces architectural wins on related embedding models (Fourier cross-attention rho=+0.38). EV2 applies the same harvest methodology to Football2Vec v2's adversary.

## Non-goals

- **Changing stage-1 (MLM) training.** The stage-1 encoder architecture was searched and promoted via EV1 (PR #158). EV2 freezes stage-1 — all variants load the same pinned stage-1 checkpoint.
- **Fourier-for-F2V (stage-1 spatial encoder).** The kickoff raised "does Fourier help Football2Vec v2's MLM task too?" as an open follow-up from session 51. This is a **stage-1 spatial encoder** question, not a stage-2 adversary question. Mixing stage-1 spatial mutations into EV2 would confound the adversary signal. Queueing as a separate single-variant EV1-extension cycle.
- **Evaluating at evolve-scale reduced fidelity.** Prior cycles (ScoutGPT L2 harvest at 15 epochs, EV1 at 5 epochs) used reduced fidelity for budget reasons. EV2 runs at **full 30-epoch production fidelity** for both phases — local hardware is free, time is not pressing, and production-fidelity evaluation eliminates reduced-vs-production calibration concerns.
- **Wheel bump.** Phase 1 and Phase 2 run locally — PEP 723 scripts do not import the published wheel. Wheel bump deferred until/unless a promotion introduces API changes consumed by HF Jobs scripts.
- **Workflow card schema refactor.** New card mirrors existing `wf-evolve-scoutgpt.yaml` / `wf-evolve-football2vec.yaml` structure.
- **xG v1 retrain.** Separate TODO entry from session 54.

## Scope

Two phases on local compute pool, gated by a user checkpoint between them.

- **Phase 1 — Harvest** (~60 min wall clock, $0): 6 hand-written seeds (each defining a `custom_layers` function returning `{"adversary": <nn.Module>}`) + `linear` baseline evaluated at 30-epoch production fidelity against pinned stage-1 encoder and dataset. Results uploaded to `luxury-lakehouse/football2vec-l2-harvest` HF Hub repo. `SUMMARY.md` + `results.json` mirrored to `docs/evolve/ev2-football2vec-l2-adversarial/`.
- **Checkpoint** (user decision): Inspect SUMMARY, apply pre-registered disposition rules, decide Phase 2 GO / NO-GO.
- **Phase 2 — Sweep, conditional** (~3–4 h overnight, $0): If GO, 20–30 iter OpenEvolve sweep seeded by top-k Phase 1 WINNERs; 30 epochs per candidate; `BackendPool`-dispatched across local pool; disposition rules fire automatically; winner gets a clean solo re-run on a single machine for machine-noise-free validation.
- **Production promotion, conditional**: If sweep winner passes promotion rule at clean solo re-run, `AdversaryConfig.architecture` default flips; retained alternatives become additional enum values.

Budget: **$0** (all local). No HF Jobs, no Databricks compute for this cycle.

## Architecture

### Module changes

**New — `src/analytics/football2vec_adversary.py`:**

```python
@dataclass(frozen=True)
class AdversaryConfig:
    """Stage-2 adversary configuration. L1 search axes for Phase 2 evolve sweep."""
    architecture: Literal["linear"] = "linear"   # registry below; grows with promoted variants
    lambda_schedule_shape: Literal["linear", "sigmoid", "cosine"] = "linear"
    lambda_max: float = 0.2
    lambda_warmup_epochs: int = 5

def lambda_schedule(cfg: AdversaryConfig, epoch: int, total_epochs: int) -> float:
    """Compute lambda at a given epoch. Shape-dependent: linear|sigmoid|cosine."""

def build_adversary(cfg: AdversaryConfig, hidden_dim: int, num_comp: int) -> nn.Module:
    """Build the canonical adversary module for cfg.architecture from the registry."""

_ADVERSARY_REGISTRY: dict[str, Callable[[int, int], nn.Module]] = {
    "linear": _build_linear_head,
    # WINNER + RETAINED variants appended post-promotion, each carrying its module class.
}
```

Phase-1 baseline (`architecture="linear"`) builds byte-equivalent to the current `TeamClassifierHead` — enforced by `test_football2vec_adversary_config_defaults`. Lambda schedule for `"linear"` shape at production values reproduces current hardcoded ramp — enforced by `test_football2vec_adversary_schedule_shapes`.

**New — `src/evolve/targets/football2vec/validation.py`:**

```python
FOOTBALL2VEC_ADVERSARY_PROFILE = ValidationProfile(
    patch_method="adversary",                            # semantic: the returned dict's "adversary" key is what the evaluator extracts
    patch_signature=["hidden_dim", "num_competitions"],  # custom_layers signature for stage-2 adversary
    return_shape="dict with key 'adversary' -> nn.Module taking (encoder_output, attention_mask) -> (B, num_competitions)",
    known_model_attrs=frozenset(),                       # seeds don't access self.* — they return standalone modules in a dict
    allowed_namespaces=frozenset({
        "torch", "math",
        "GradientReversal",         # from building_blocks — required in every seed
        "MoERouter", "HyperLinear", "KANLayer", "AdaLNZero",
        "CrossLayer", "CompetitiveGate", "AdaptiveBandwidth", "RatioGate",
    }),
    layers_args=["hidden_dim", "num_competitions"],      # custom_layers function args for stage-2
    rejected_builtins=frozenset({
        "eval", "exec", "compile", "__import__", "open", "print", "input",
        "getattr", "hasattr", "setattr", "delattr", "globals", "locals",
        "vars", "dir", "type", "super", "breakpoint", "memoryview",
        "classmethod", "staticmethod", "property",
    }),
)
```

**New — `src/evolve/targets/football2vec/seed_programs_stage2/`** (kept separate from existing `seed_programs/` which are stage-1 L1 configs):

- `linear.py` (baseline reference — `program_path=None` at Phase 1 dispatch; config-only `architecture="linear"`)
- `deep_mlp_2layer.py`
- `deep_mlp_3layer.py`
- `cross_attention_adversary.py`
- `attention_pool_head.py`
- `residual_mlp.py`
- `dual_head_ensemble.py`

Each L2 seed file defines:

```python
"""<One-paragraph hypothesis — what design axis is being tested, expected outcome>."""

def custom_layers(hidden_dim, num_competitions):
    """Return {'adversary': <nn.Module>}. EV2 reuses the validator's existing custom_layers
    function-name (hardcoded in code_validator.py) with an adversary-specific return-key
    convention — avoids extending the validator."""
    class H(torch.nn.Module):
        def __init__(self):
            super().__init__()
            self.grl = GradientReversal(lambda_=1.0)   # lambda_ injected per-epoch by evaluator
            # ... architecture-specific init ...
        def forward(self, encoder_output, attention_mask):
            # ... architecture-specific forward, returning (B, num_competitions) ...
            return logits
    return {"adversary": H()}
```

`GradientReversal.lambda_` is patched per-epoch by the evaluator via `adversary.grl.lambda_ = lambda_schedule(cfg, epoch, total_epochs)`. Each seed references `self.grl` for this hook. The evaluator's `_apply_program_adversary` helper execs the seed under restricted globals, calls `custom_layers(hidden_dim, num_competitions)`, extracts `result["adversary"]`, and moves it to the target device — analogous to ScoutGPT's `_apply_program` but extracting a single module instead of registering a dict of layers as model children.

**New — `src/evolve/targets/football2vec/evaluator.py` extension:**

```python
def train_and_evaluate_stage2(
    candidate_config: dict[str, Any],
    device: str,
    epochs: int,
    seed: int,
    program_path: str | None = None,
) -> dict[str, Any]:
    """Stage-2 adversarial fine-tuning evaluator.

    1. Load pinned stage-1 encoder from HF Hub (cached per-process like dataset).
    2. Build adversary: _apply_program(seed) if program_path else build_adversary(cfg).
    3. Run refactored _train_stage2_loop with injected adversary + lambda schedule.
    4. Evaluate: val_mlm_loss, val_adv_accuracy, num_competitions, param_count, etc.
    5. Return dict of scalar fitness metrics.
    """
```

Existing `train_and_evaluate` (stage-1 MLM, EV1) is unchanged. New entry point disambiguates stage.

**Module-level stage-1 encoder cache** (`_stage1_cache`): same pattern as `_dataset_cache` but keyed by `(model_repo, commit_sha)`. Loads once per process, reused across all candidate evaluations. Stage-1 weights ~500 MB — skipping the re-download per candidate is necessary.

**Refactor — `scripts/train_football2vec_v2.py::_train_stage2_loop`:**

- Accept new parameters: `adversary_module: nn.Module | None = None`, `lambda_schedule_fn: Callable[[int, int], float] | None = None`
- If both `None`: reproduce current production behavior byte-equivalent (hardcoded `TeamClassifierHead` + linear ramp)
- If provided: use the injected adversary + schedule
- Move lambda-ramp math out of the loop body into `football2vec_adversary.lambda_schedule` (production default = linear shape)
- **Backward-compat guarantee**: `test_stage2_loop_injection_backcompat` asserts byte-equivalent training trajectory at 1-epoch fidelity on a 10-sample fixture between old and new loop when defaults are used

**New orchestration script — `scripts/evaluate_football2vec_l2_adversary_seeds.py`** (PEP 723):

```python
# /// script
# requires-python = ">=3.10,<3.11"
# dependencies = [
#     "luxury-lakehouse @ https://huggingface.co/luxury-lakehouse/build-artifacts/resolve/main/luxury_lakehouse-0.3.12-py3-none-any.whl",
#     "numpy>=1.24", "pandas>=2.0", "pyarrow>=14.0", "datasets>=3.0",
#     "torch>=2.0", "safetensors>=0.4.0", "huggingface-hub>=1.5.0",
#     "openevolve>=0.2.0",   # required transitively via evolve/__init__.py
# ]
# ///
```

Instantiates `BackendPool([LocalCudaBackend, RemoteSshBackend(media-pc), RemoteSshBackend(dgx-spark)])`, dispatches 7 variants through `ThreadPoolExecutor(max_workers=3)`, per-variant uploads `metrics.json` to `luxury-lakehouse/football2vec-l2-harvest`, writes combined `results.json` at end, mirrors to `docs/evolve/ev2-football2vec-l2-adversarial/results.json`.

**New evolve config — `src/evolve/targets/football2vec/config_stage2.yaml`:** Phase 2 sweep config. Mirrors `scoutgpt/config.yaml` structure with stage-2-specific `CandidateConfig` (extends Phase 1 shared config with the three new L1 enums as mutation axes).

**New workflow card — `workflow-cards/wf-evolve-football2vec-l2-stage2.yaml`:** mirrors `wf-evolve-football2vec.yaml`. `runtime: local-gpu`, `trigger: manual`, `typical_cost_usd: 0.00`, `typical_duration_minutes: 240`.

### Dispatch

`BackendPool` (already in `src/evolve/backends/pool.py`) provides thread-safe priority-queue dispatch: `train()` blocks on `queue.PriorityQueue.get()`, delegates to the claimed backend, releases it back to the queue on completion. `ThreadPoolExecutor(max_workers=3)` over the 7 variants yields automatic load balancing — fastest backend naturally serves more candidates and is preferred when multiple are idle.

Priority order (lowest number = highest priority):

1. `LocalCudaBackend(device="cuda:0")` — AI-PC 5070 Ti (priority 0: no SSH hop, fastest round-trip)
2. `RemoteSshBackend(host="super@192.168.68.70", ...)` — Media-PC 5070 Ti (priority 1: identical hardware, requires user to wake)
3. `RemoteSshBackend(host="karsten@192.168.68.73", ...)` — DGX Spark GB10 (priority 2: ARM+Blackwell, different torch build)

Orchestrator accepts `--hosts ai,media,spark` override (subset for fallback if a machine is unavailable) and `--force-sequential` for single-machine debugging.

**Machine-noise caveat:** same variant on different machines may differ by a few ulps of FP32/TF32 rounding. Phase 1 disposition thresholds (`δ_promote = 0.02` fitness ≈ 1.3 pp adversary-accuracy swing) are well above expected rounding noise. Phase 2 winner gets a clean solo re-run on a single machine to eliminate machine-noise from the promotion decision — established pattern from PR #176 Arm 5.

## Phase 1 harvest protocol

### Shared config (pinned across all 7 variants)

```python
# Stage-1 encoder architecture (post-EV1 + post-cross-attention-promote production)
hidden_dim           = 192
num_layers           = 4
num_heads            = 6
dropout              = 0.1
mask_prob            = 0.22
spatial_mlp_dim      = 64
pooling_type         = "cls"
spatial_injection    = "additive"
position_embedding   = "learnable"

# Training hyperparameters (production defaults)
learning_rate        = 3e-4
batch_size           = 256
weight_decay         = 0.01

# Stage-2 fidelity (full production-fidelity harvest)
stage2_epochs        = 30
patience             = 7
seed                 = 42

# Phase 1 L1 stage-2 enums — pinned to current production, isolating adversary head architecture
adversary_lambda_max       = 0.2
adversary_warmup_epochs    = 5
lambda_schedule_shape      = "linear"

# Reproducibility anchors — captured by orchestrator at Phase 1 kickoff, written to SUMMARY.md
stage1_encoder_sha   = <HEAD of luxury-lakehouse/football2vec-v2 at harvest start>
training_dataset_sha = <HEAD of luxury-lakehouse/football2vec-training-data at harvest start>
wheel_version        = "0.3.12"   # current as of 2026-04-23; SHA256 verified via HF sha256sums.json sidecar
```

### Seed slate (6 + baseline)

| # | Seed | Adversary | Hypothesis |
|---|---|---|---|
| 0 | `linear` (no L2 patch, `architecture="linear"`) | `GRL → Linear(hd, num_comp)` | Baseline — current production. Reference to beat. |
| 1 | `deep_mlp_2layer` | `CLS → GRL → Linear(hd, hd) → GELU → LN → Linear(hd, num_comp)` | **Depth**: stronger adversary → more competition signal recovered → more debias pressure on encoder |
| 2 | `deep_mlp_3layer` | `CLS → GRL → Linear(hd, 2·hd) → GELU → LN → Linear(2·hd, hd) → GELU → LN → Linear(hd, num_comp)` | **Depth + width**: even higher-capacity adversary, stronger version of #1 |
| 3 | `cross_attention_adversary` | `GRL(per-token) → per-class learnable-query cross-attention → scalar out-proj` | **Per-class structured aggregation**: each competition extracts its own per-token evidence directly. Mechanistic match with ScoutGPT's cross-attention Fourier finding (PR #163→#166→#176). |
| 4 | `attention_pool_head` | `GRL(per-token) → single-query attention pool → Linear(hd, num_comp)` | **Per-token access**: if competition signal is localized at specific positions (kickoff locations, unique patterns), attention can focus on them |
| 5 | `residual_mlp` | `CLS → GRL → (Linear(hd,hd) → GELU → LN → Linear(hd,hd) + residual) → Linear(hd, num_comp)` | **Residual depth**: same capacity as #1 but with residual — better gradient flow for adversary |
| 6 | `dual_head_ensemble` | Two parallel heads (linear + 2-layer MLP); losses averaged; GRL in both | **Ensemble adversary**: different-capacity adversaries in parallel — pushes encoder against both (multi-scale discriminator analog) |

### Fitness function (pre-registered)

```python
L_0         = val_mlm_loss of seed 0 (linear baseline)   # captured during Phase 1, written to SUMMARY
chance      = 1.0 / num_competitions                     # = 1/22 ≈ 0.0455 (uniform — strict floor)

mlm_score     = min(1.0, L_0 / val_mlm_loss)             # ∈ [0, 1]; 1.0 if variant matches/beats baseline MLM
leakage       = max(0.0, (val_adv_acc - chance) / (1.0 - chance))   # ∈ [0, 1]; 0 at chance, 1 if adversary perfect
debias_score  = 1.0 - leakage                            # ∈ [0, 1]; 1.0 at chance, 0.0 at perfect leak

fitness       = 0.4 * mlm_score + 0.6 * debias_score
```

**Weights rationale:** stage-1 already achieves good MLM; stage-2 goal is debiasing without destroying MLM. Debias is the delta being optimized (weight 0.6); MLM is the floor being protected (weight 0.4). Symmetric to ScoutGPT's `0.7·rho + 0.3·top1` where the larger weight is on the stretch goal.

**Uniform-chance rationale:** adversary is trained unweighted (`nn.CrossEntropyLoss` with no class weights). Uniform chance is the strict "adversary cannot tell competitions apart at all" test. Max-prior chance is weaker — accepting always-predict-majority behavior. Uniform keeps pressure on the encoder.

**`L_0 / val_mlm_loss` normalization rationale:** in expected range (~1.2–1.5), `exp(-loss)` lives in 0.22–0.30, poor dynamic range. `L_0 / val_loss` is 1.0 at baseline, drops linearly with MLM degradation — interpretable as "fraction of baseline embedding quality preserved".

### Per-seed disposition rules (fire automatically after Phase 1 evaluation)

| Disposition | Rule | Effect |
|---|---|---|
| **WINNER** | `fitness(v) > fitness(linear) + 0.02` AND `mlm_score(v) ≥ 0.70` | Used as Phase 2 initial-population seed. Committed to `seed_programs_stage2/`. |
| **ARCHIVE** | Doesn't meet WINNER, doesn't meet PRUNE | Committed, documented in SUMMARY. No Phase 2 use. |
| **PRUNE** | `mlm_score(v) < 0.70` | Variant destroyed embedding quality. Committed in `seed_programs_stage2/` but flagged in SUMMARY as "broke MLM; not exploration material". No deletion — preserve the research record. |

### Phase 1 → Phase 2 checkpoint (user decision)

**GO** criterion: ≥1 variant meets WINNER disposition.
**NO-GO** criterion: no WINNER; top variant is ARCHIVE-tier only.

User inspects `docs/evolve/ev2-football2vec-l2-adversarial/SUMMARY.md` — table of variants by fitness, disposition column, hypothesis vs. outcome notes — and decides. The thresholds frame the evidence; the user makes the call (e.g., may still decide NO-GO despite a marginal WINNER if the signal is close to noise).

### Phase 1 artefacts

- `metrics.json` per variant uploaded to `luxury-lakehouse/football2vec-l2-harvest/{variant}/metrics.json` as each completes (partial-crash survival)
- `results.json` combined after all complete, uploaded to `luxury-lakehouse/football2vec-l2-harvest/results.json`
- `docs/evolve/ev2-football2vec-l2-adversarial/results.json` local mirror for commit
- `docs/evolve/ev2-football2vec-l2-adversarial/SUMMARY.md` human-written writeup per ScoutGPT L2 harvest template

## Phase 2 sweep protocol (conditional — only if checkpoint is GO)

### Evolve configuration

`src/evolve/targets/football2vec/config_stage2.yaml`:

```yaml
target: football2vec_stage2
description: "Evolve Football2Vec v2 stage-2 adversary head + schedule to minimize val_mlm_loss while debiasing val_adv_accuracy toward chance"

fitness:
  primary: fitness
  combined_weights:
    mlm_score: 0.4
    debias_score: 0.6
  minimize: false

evaluation:
  epochs: 30                       # matches Phase 1 fidelity — no calibration gap
  dataset: "luxury-lakehouse/football2vec-training-data"
  timeout_seconds: 2400            # 40 min — generous vs ~25 min typical on 5070 Ti
  seed: 42

backend:
  type: "local_cuda,remote_ssh"
  # Identical pool to Phase 1 orchestrator
  device: "cuda:0"
  ssh_hosts:
    - "super@192.168.68.70"
    - "karsten@192.168.68.73"

llm:
  models:
    - name: "anthropic/claude-sonnet-4"
      weight: 0.8
    - name: "anthropic/claude-haiku-4.5"
      weight: 0.2
  temperature: 0.7

evolution:
  iterations: 30                   # upper bound — budget allows overnight at 3-way parallel
  population_size: 60              # scaled-down from ScoutGPT's 200 (smaller design space)
  num_islands: 2
  parallel_evaluations: 3
  early_stopping_patience: 15
  checkpoint_interval: 5
```

**Initial population:** all WINNER-tier Phase 1 seeds. The `linear` baseline itself is NOT added to the initial population — it is the reference fitness being beaten, not an exploration starting point. If only 1 WINNER emerges from Phase 1, include ARCHIVE-tier seeds (but NOT PRUNE-tier) in the initial population as well, to give the LLM diverse starting points; the pre-registered disposition rules still apply post-sweep so ARCHIVE-derived mutations must still clear the PROMOTE bar to be relevant. LLM mutates both the `custom_layers` adversary code (L2) and the three L1 enums (`lambda_schedule_shape`, `lambda_max`, `lambda_warmup_epochs`).

### LLM mutation prompt

`src/evolve/targets/football2vec/prompts/stage2_system_message.txt`: spells out the signature, allowed namespaces, `GradientReversal` usage requirement, per-epoch lambda injection convention, and the three L1 enums the LLM may mutate. Follows the ScoutGPT L2 system message template.

### Phase 2 disposition rules

After the sweep completes, the top candidate by fitness is extracted. **Clean solo re-run** is performed ONLY on the Phase-2 winner — at 30 epochs with identical pinned SHAs, on any single local machine with no GPU contention (AI-PC preferred, Media-PC or DGX Spark acceptable fallbacks). This eliminates machine-noise from the single promotion decision without the O(N) expense of clean-re-running every candidate — same pattern as PR #176 Arm 5. RETAIN evaluation uses the original Phase-2 sweep fitness (no clean re-run) because retention is a secondary enum registration, not a production-default flip.

| Rule | Criterion | Effect |
|---|---|---|
| **PROMOTE** | Winner's clean-solo-re-run `fitness ≥ fitness(linear) + 0.02` AND `mlm_score ≥ 0.70` | Flip `AdversaryConfig.architecture` default to winner; register winner's adversary class in `_ADVERSARY_REGISTRY`; old `"linear"` stays as enum option for reproducibility/rollback |
| **RETAIN** | Any non-winner Phase-2 candidate with sweep `fitness delta ≥ +0.05` vs `linear` baseline | Register as additional enum option alongside promoted winner (same pattern as Fourier retention in PR #176, via `apply_retention_rule` in `src/analytics/promotion_rules.py`). No cap on count — all candidates clearing the bar are retained. |
| **REJECT** | Clean-re-run fails PROMOTE criterion | No production change. Sweep results documented in SUMMARY. Seeds still committed. |

## Tests

| Test | What it asserts |
|---|---|
| `test_football2vec_adversary_config_defaults` | `build_adversary(AdversaryConfig(), 192, 22)` produces byte-equivalent state_dict keys to current `TeamClassifierHead(192, 22)` |
| `test_football2vec_adversary_schedule_shapes` | `lambda_schedule(cfg(linear), epoch, 30)` matches current production ramp at each epoch 0..29. Sigmoid and cosine shapes produce monotonic-to-max trajectories with the same endpoint `lambda_max`. |
| `test_football2vec_adversary_build_registry` | `build_adversary(cfg, ...)` returns expected module type for each `architecture` value; unknown value raises `ValueError` |
| `test_stage2_loop_injection_backcompat` | Refactored `_train_stage2_loop` with `adversary_module=None, lambda_schedule_fn=None` produces byte-equivalent 1-epoch training trajectory (same loss values to 1e-6) as current loop on a 10-sample fixture |
| `test_football2vec_l2_validation_profile_accepts_seeds` | `validate_program(source, FOOTBALL2VEC_ADVERSARY_PROFILE)` returns `(True, ...)` for each of the 6 seed files |
| `test_football2vec_l2_validation_profile_rejects_disallowed` | Fabricated seed using `os.system(...)` is rejected; seed using `torch.nn.Linear` is accepted; seed using a bare `subprocess.run(...)` is rejected |
| `test_football2vec_l2_seed_programs_load` | Each of the 6 seed files parses, exec's under restricted globals, and returns an `nn.Module` with correct forward signature `(encoder_output, attention_mask) → (B, num_competitions)` when invoked with `hidden_dim=192, num_competitions=22` |
| `test_card_wf_evolve_football2vec_l2_stage2_yaml_parses` | New workflow card parses via `WorkflowCard.from_yaml_file(...)` |
| `test_ast_regression_f2v_adversary_imports` | `from analytics.football2vec_adversary import ...` AST-parseable without import cycle; `build_adversary` + `lambda_schedule` + `AdversaryConfig` are the public surface |

Existing pytest-benchmark tests on `Football2VecEncoder` (in `test_benchmarks.py`) are unchanged — stage-1 encoder architecture is frozen.

Baseline test count: 1807 (from PR #176). New test count: +9 = **1816 tests expected all green**.

## Acceptance criteria (definition of done per phase)

### Phase 1 complete

1. `uv run ruff check src/ scripts/` clean; `uv run ruff format --check src/ scripts/` clean; `uv run pyright src/` clean (basic mode)
2. `uv run pytest src/tests/ -v` — 1815 green
3. Orchestrator completes on the local pool without crash; all 7 variants produce `metrics.json`; `results.json` uploaded to HF Hub; mirror committed to `docs/evolve/ev2-football2vec-l2-adversarial/results.json`
4. `docs/evolve/ev2-football2vec-l2-adversarial/SUMMARY.md` written — contains disposition table, hypothesis-vs-outcome notes, Phase 2 recommendation, pinned SHAs, wall-clock + machine assignments
5. User checkpoint completed — GO/NO-GO recorded in SUMMARY

### Phase 2 complete (conditional)

6. Sweep completes (early-stop or hits iter 30) without crash
7. `best_program.py` exists at `results/evolve/football2vec_stage2/<timestamp>/best_program.py` and is a valid L2 program
8. Clean solo re-run of winner completes at 30 epochs on a single machine; metrics recorded
9. Disposition rule fires — PROMOTE / RETAIN / REJECT — documented in SUMMARY
10. (IF PROMOTE) `AdversaryConfig.architecture` default flipped; `_ADVERSARY_REGISTRY` extended; old `"linear"` retained as enum option; new tests cover the registered variant

### Documentation

11. `TODO.md` — EV2 removed from On-Deck
12. `MEMORY.md` — new `project_ev2_football2vec_adversary_cycle.md` summarizing outcome
13. (IF PROMOTE) `CLAUDE.md` — no new rules expected unless cycle exposes a new operational learning
14. (IF PROMOTE) `ARCHITECTURE.md` — stage-2 adversary section updated to reflect new default + enum options

## Commit cadence

All work on `evolve/football2vec-l2-adversarial` feature branch. Single commit per phase completion; each commit + push + PR separately approved at moment of decision per `feedback_no_commits_without_approval.md` and `feedback_one_commit_at_a_time.md`.

1. **Commit #1 — Infrastructure + Phase 1 ready** (after implementation, before Phase 1 dispatch). Contents: new modules (`football2vec_adversary.py`, `football2vec/validation.py`, `football2vec/evaluator.py` extension), refactored `_train_stage2_loop`, 6 seed files, orchestrator script, workflow card, tests, this design doc.
2. **Commit #2 — Phase 1 harvest results** (after Phase 1 completes, before checkpoint). Contents: `SUMMARY.md`, `results.json`, disposition table, pinned SHAs.
3. **Commit #3 — Phase 2 sweep results** (conditional, after sweep + clean re-run). Contents: evolve config + prompts, sweep `best_program.py`, updated SUMMARY with clean-re-run, disposition outcome.
4. **Commit #4 — Production promotion** (conditional, only if PROMOTE fires). Contents: `AdversaryConfig.architecture` default flip, `_ADVERSARY_REGISTRY` extension, updated tests, `ARCHITECTURE.md` stage-2 section update.

No PR opened without explicit user approval. No merge without explicit user approval.

## Risks

| Risk | Mitigation |
|---|---|
| Stage-1 encoder upstream changes mid-cycle | Pin `stage1_encoder_sha` in shared_config at Phase 1 kickoff; orchestrator fetches that exact revision via `huggingface_hub.hf_hub_download(revision=sha)` |
| Training dataset upstream changes mid-cycle | Same as above for `training_dataset_sha` |
| DGX Spark ARM+Blackwell incompatibility | Cycle-start smoke test (`torch.cuda.is_available()`, import stage-1 encoder) on all 3 machines before dispatch; if DGX Spark fails, `--hosts ai,media` fallback; queue dispatch absorbs machine dropout gracefully |
| Media-PC asleep at Phase 1 kickoff | Explicit user protocol (established in session 53): user wakes Media-PC before kickoff; orchestrator smoke-tests before accepting machine into pool |
| Adversary seed bug crashes mid-harvest | Per-variant upload as each completes (partial-crash survival, same pattern as ScoutGPT L2 harvest); only failed variant needs re-run, not the whole batch |
| `_train_stage2_loop` refactor silently changes production training | `test_stage2_loop_injection_backcompat` asserts byte-equivalent behavior before any new seed runs; guard against accidental production behavior drift |
| OpenEvolve dep drift at Phase 2 | Pin `openevolve>=0.2.0` explicitly in PEP 723 header of orchestrator (lesson from PR #163 which crashed on missing `openevolve` import) |
| Wheel version mid-cycle collision with another session | This cycle does NOT bump the wheel; PEP 723 scripts pin the wheel SHA at kickoff; local imports work from `src/` for development |
| Machine-noise skews disposition at decision boundary | `δ_promote = 0.02` fitness threshold is well above expected FP rounding noise (~0.001); Phase 2 winner gets clean solo re-run on single machine to eliminate machine-noise from promotion decision |
| `PruneHead` variant in fitness ranking but MLM regression missed | Two-gate disposition: WINNER requires both `fitness > baseline + δ_promote` AND `mlm_score ≥ 0.70`. A variant with high debias_score but destroyed MLM fails the second gate |

## Out of scope (deferred)

- **Fourier-for-F2V stage-1 spatial encoder** — follow-up single-variant EV1-extension cycle. Separate spec, separate cycle.
- **xG v1 retrain** — separate cycle per TODO.md.
- **Wheel bump** — not needed for this cycle (all local, PEP 723 scripts pin wheel SHA).
- **Workflow card schema refactor** — new card mirrors existing structure.
- **AI Governance update** — `wf-evolve-scoutgpt` and `wf-evolve-football2vec` are not in `PER_PLAYER_EVALUATIVE_CARDS` (per `src/tests/test_ai_governance_md.py`); same posture for `wf-evolve-football2vec-l2-stage2`. Confirm in Phase 2 commit if card lands.
- **ARCHITECTURE.md Appendix D update** — Ganin 2016 is already cited in NOTICE + existing model card for Football2Vec v2; no new academic citation introduced.
- **HF Space / Taipy UI** — embeddings from the new adversary reach Taipy only through the Lakebase synced `behavioral_vector` column (unchanged pipeline); no UI work needed in EV2.

## Follow-ups (after EV2 lands)

- If a promoted adversary variant significantly improves debiasing: validate downstream player-similarity metrics (cosine distance on `behavioral_vector` across competitions) to confirm competition-identity has been suppressed from embeddings.
- Fourier-for-F2V stage-1 cycle (queued).
- EV3 (evolve-engine SSH backend) — scope reduced after PR #176 validated Media-PC as SSH-reachable; EV2 will exercise this backend further, surfacing any remaining rough edges.
- If `lambda_schedule_shape` mutations surface a non-linear winner: document in a new feedback memory (`feedback_adversarial_schedule_shapes.md`).

## References

### Academic

- Ganin, Y., Ustinova, E., Ajakan, H., et al. (2016). "Domain-Adversarial Training of Neural Networks." *JMLR* 17(1), pp. 1–35. Original gradient-reversal mechanism. Currently cited in NOTICE and `wf-football2vec-v2` card.
- Carion, N., Massa, F., Synnaeve, G., et al. (2020). "End-to-End Object Detection with Transformers." *ECCV*. Learnable object queries for set aggregation — mechanistic analog for `cross_attention_adversary` seed.
- Lee, J., Lee, Y., Kim, J., et al. (2019). "Set Transformer: A Framework for Attention-based Permutation-Invariant Neural Networks." *ICML*. Per-class learnable queries for structured set aggregation.
- Romera-Paredes, B., Barekatain, M., Novikov, A., et al. (2025). "AlphaEvolve: A coding agent for scientific and algorithmic discovery." arXiv:2506.13131. LLM-guided evolutionary coding — the Phase 2 pattern this cycle reuses.

### Prior cycles / ADRs

- ADR-001 — Evolve Level 2 code execution (AST allowlist + restricted globals + subprocess isolation). Policy EV2 inherits.
- ADR-012 — Training-to-production delivery hardening. Not directly applicable to this cycle (no HF Jobs, no Databricks inference), but the posture of "explicit contract boundaries for every hand-off" is the design philosophy behind `AdversaryConfig` + `_ADVERSARY_REGISTRY`.
- `2026-04-04-evolve-engine-design.md` — original evolve engine design.
- `2026-04-05-multi-backend-dispatcher-design.md` — `BackendPool` priority-queue dispatch (EV2 reuses unchanged).
- `2026-04-07-evolve-level2-code-evolution-design.md` — L2 `custom_embed` + `custom_layers` patch-point pattern (EV2 reuses `custom_layers` with an adversary-specific `{"adversary": <module>}` return-key convention to avoid extending the validator).
- `2026-04-18-ev1-football2vec-l1-sweep-design.md` — EV1 L1 precedent; stage-1 architecture enum patterns EV2 builds on.
- `2026-04-20-scoutgpt-l2-harvest-design.md` — ScoutGPT L2 harvest protocol; EV2 Phase 1 methodology is a direct reuse.
- `2026-04-21-scoutgpt-cross-attention-promote-design.md` — promotion + retention rule structure EV2 Phase 2 reuses.

## Approval gates

1. **[APPROVAL #1 — design approval]** ✅ — user approved consolidated design 2026-04-23 (this session)
2. **[APPROVAL #2 — spec review]** pending — user reviews this written spec before implementation plan
3. **[APPROVAL #3 — Commit #1 (infrastructure + Phase 1 ready)]** pending — after implementation + tests green
4. **[APPROVAL #4 — Phase 1 dispatch]** pending — after Commit #1, explicit "fire the harvest"
5. **[APPROVAL #5 — Commit #2 (Phase 1 results)]** pending — after harvest + SUMMARY
6. **[APPROVAL #6 — Phase 2 GO/NO-GO]** pending — the user checkpoint
7. **[APPROVAL #7 — Phase 2 dispatch]** conditional on #6 = GO
8. **[APPROVAL #8 — Commit #3 (Phase 2 results)]** conditional, after sweep + clean re-run
9. **[APPROVAL #9 — Commit #4 (production promotion)]** conditional, only if PROMOTE fires
10. **[APPROVAL #10 — push + PR]** pending — separate from commit approvals

Each approval is a distinct user decision at the moment of need per `feedback_only_approve_agreed_gates.md`. Per `feedback_one_commit_at_a_time.md`, "commit + push + PR" is three separate approvals even when chained.
