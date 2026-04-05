# Evolve Engine — LLM-Guided Architecture Search

**Date:** 2026-04-04
**Branch:** `feature/evolve-engine`
**Status:** Design approved

## Problem

ScoutGPT (D32) achieves 81.5% top-1 action prediction accuracy but only 0.094 Spearman rho on counterfactual player ranking. The weak rho indicates the player conditioning mechanism (additive embedding at position 0) is insufficient — the model predicts actions well from sequence context alone, without meaningfully differentiating between players.

Hand-designing architectural improvements is limited to 2-3 ideas per iteration. With the perf fixes merged (PR #86, 4-10x training speedup), we can afford to explore the design space much more broadly.

## Solution

An **AlphaEvolve-style evolutionary architecture search** integrated into the Luxury Lakehouse as a first-class capability. Uses OpenEvolve (Apache 2.0) as the evolution engine with Claude as the mutation LLM. Trains candidates on local GPU (RTX 5070 Ti, 16 GB VRAM) at zero compute cost. Designed for extensibility: pluggable compute backends (local CUDA, Docker, HF Jobs, SSH remote) and pluggable targets (ScoutGPT now, any trainable model later).

**References:**
- DeepMind AlphaEvolve (arXiv:2506.13131) — evolutionary coding agent, MAP-Elites + LLM mutations
- OpenEvolve (GitHub: algorithmicsuperintelligence/openevolve) — open-source implementation, Apache 2.0
- Perez et al. 2018, FiLM — Feature-wise Linear Modulation for conditional computation

## Architecture

### Module Structure

```
src/evolve/
├── __init__.py
├── config.py              # Pydantic: EvolveConfig, BackendConfig, TargetConfig
├── runner.py              # CLI entry point — wires OpenEvolve to our evaluator
├── evaluator.py           # Bridges OpenEvolve interface to ComputeBackend protocol
├── backends/
│   ├── __init__.py
│   ├── base.py            # Protocol: ComputeBackend
│   ├── local_cuda.py      # Direct PyTorch training on local GPU
│   ├── docker.py          # Train inside container (stub — future)
│   ├── hf_jobs.py         # Submit to HF Jobs (stub — future)
│   └── remote_ssh.py      # SSH to network GPU (stub — future)
└── targets/
    └── scoutgpt/
        ├── __init__.py
        ├── evaluator.py   # Build model, train N epochs, return rho + metrics
        ├── config.yaml    # Default evolution config
        └── seed_programs/
            ├── additive.py
            ├── cross_attention.py
            ├── film.py
            └── gated.py
```

### Pluggable Dimensions

**Compute backends** (how candidates are trained):

| Backend | `train()` implementation | `available()` check | Status |
|---------|------------------------|-------------------|--------|
| `local_cuda` | Direct PyTorch in-process | `torch.cuda.is_available()` | Build now |
| `docker` | `docker run` with mounted config, parse stdout JSON | `docker info` succeeds | Stub |
| `hf_jobs` | `huggingface_hub.create_job()`, poll for completion | HF token + org GPU quota | Stub |
| `remote_ssh` | SSH + rsync config, run remotely, rsync results | SSH key auth succeeds | Stub |

**LLM providers** (who generates mutations): handled natively by OpenEvolve via any OpenAI-compatible endpoint — Claude API, OpenRouter, Ollama, etc. Configured in YAML; no code needed per provider.

**Targets** (what models can be evolved): each target provides seed programs, a model-specific evaluator, and a default config. Adding a target requires zero changes to the engine or backends.

### Compute Backend Protocol

```python
class ComputeBackend(Protocol):
    def train(
        self,
        candidate_config: dict[str, Any],
        target: str,
        epochs: int,
        seed: int,
    ) -> dict[str, float]:
        """Train a candidate, return evaluation metrics.

        Returns dict with at minimum the fitness metric keys.
        Raises TimeoutError if training exceeds budget.
        """
        ...

    def available(self) -> bool:
        """Check if this backend is usable."""
        ...
```

### OpenEvolve Integration

Three touch points:

1. **Runner** (`runner.py`): CLI entry point. Loads config, instantiates backend, selects seed program, calls `openevolve.run_evolution()`.

2. **Evaluator bridge** (`evaluator.py`): Bridges OpenEvolve's `evaluate(program_path) -> Dict[str, float]` to our `ComputeBackend.train()`. Extracts config dict from the evolved program file, validates search space bounds, dispatches to backend, computes `combined_score` from fitness weights.

3. **Config translation**: Our `EvolveConfig` generates OpenEvolve-native YAML at runtime. The user writes one config; we translate.

### Evolution Levels

**Level 1 (build now):** The evolved "program" is a Python file containing a config dict. The LLM mutates hyperparameters and selects from pre-implemented conditioning types. The evaluator extracts the dict and builds the model from it.

**Level 2 (future extension):** The evolved program includes code blocks (e.g., a `custom_embed()` function). The evaluator checks for these, monkey-patches them onto the decoder class before training. The LLM can invent novel conditioning mechanisms.

Transition path: the evaluator already imports the program file dynamically. Level 2 adds a check for callable attributes — no changes to the backend, runner, or OpenEvolve config.

```python
# Level 1 program (config only)
config = {"hidden_dim": 256, "conditioning_type": "film", ...}

# Level 2 program (config + code overrides)
config = {"hidden_dim": 256, ...}

def custom_embed(self, action_ids, start_x, start_y, end_x, end_y,
                 result, time_delta, player_ids, attention_mask):
    """Replaces ScoutGPTDecoder._embed() for this candidate."""
    ...
```

## ScoutGPT Target

### Conditioning Mechanisms

Four pre-implemented types added to `ScoutGPTDecoder`:

| Type | Mechanism | Hypothesis |
|------|-----------|-----------|
| `additive` | Sum player embedding with other embeddings (current) | Baseline |
| `cross_attention` | Player embedding as K/V, action sequence as Q in dedicated cross-attention layer before each transformer block | Separating player signal from action signal prevents dilution |
| `film` | Player embedding predicts per-channel scale + shift applied to action embedding (Feature-wise Linear Modulation) | Multiplicative interaction gives player stronger control over representation |
| `gated` | Learned gate: `sigma(W * player_emb) . action_emb` — player selectively amplifies/suppresses action features | Player can suppress irrelevant action features |

### Seed Programs

Four seeds, one per conditioning type. Each uses different hyperparameter starting points to maximize diversity in the initial population. Seed 4 (gated) includes an auxiliary `player_prediction_weight` loss to seed that idea into the evolution.

### Search Space Bounds

Validated in the evaluator — candidates exceeding bounds are rejected (score 0.0):

| Parameter | Min | Max | Notes |
|-----------|-----|-----|-------|
| `hidden_dim` | 64 | 512 | 512 at batch 256 uses ~4 GB VRAM |
| `num_layers` | 2 | 12 | |
| `num_heads` | 2 | 16 | Must divide hidden_dim; reject with score 0.0 if not |
| `dropout` | 0.0 | 0.5 | |
| `learning_rate` | 1e-5 | 1e-2 | |
| `vaep_loss_weight` | 0.0 | 1.0 | 0 = disable VAEP head |
| `player_prediction_weight` | 0.0 | 1.0 | 0 = no auxiliary player loss |
| `batch_size` | 64 | 512 | |
| `param_count` | — | 20M | Hard cap for 16 GB VRAM budget |

### Evaluation Budget

Per-candidate (fast): 5 epochs, 200 episodes x 50 players for counterfactual eval.
Final winner (full): 30 epochs with early stopping, 1000 episodes x 100 players.

### Fitness Function

```yaml
combined_score = 0.7 * spearman_rho + 0.3 * top1_accuracy
```

Primary criterion is rho (the problem to solve). Secondary criterion prevents accuracy regression and reduces noise (top-1 has lower variance than rho).

## Configuration

Single YAML config per evolution run:

```yaml
target: scoutgpt
description: "Evolve ScoutGPT conditioning to maximize counterfactual Spearman rho"

fitness:
  primary: spearman_rho
  secondary: top1_accuracy
  combined_weights:
    spearman_rho: 0.7
    top1_accuracy: 0.3
  minimize: false

evaluation:
  epochs: 5
  dataset: "luxury-lakehouse/scoutgpt-training-data"
  timeout_seconds: 900
  seed: 42

backend:
  type: local_cuda
  device: "cuda:0"

llm:
  models:
    - name: "claude-sonnet-4-20250514"
      weight: 0.8
      api_base: "https://api.anthropic.com/v1/"
      api_key_env: "ANTHROPIC_API_KEY"
    - name: "claude-haiku-4-5-20251001"
      weight: 0.2
      api_base: "https://api.anthropic.com/v1/"
      api_key_env: "ANTHROPIC_API_KEY"
  temperature: 0.7
  max_tokens: 4096

evolution:
  iterations: 150
  population_size: 200
  num_islands: 3
  migration_interval: 30
  parallel_evaluations: 1
  diff_based: true
  early_stopping_patience: 40
```

Secrets via `api_key_env` (environment variable name), never inline values.

## Entry Point

```toml
[project.scripts]
evolve = "evolve.runner:main"

[project.optional-dependencies]
evolve = ["openevolve>=0.3.0"]
```

Usage:
```bash
uv run evolve --target scoutgpt
uv run evolve --target scoutgpt --backend local_cuda --device cuda:0 --iterations 150
uv run evolve --target scoutgpt --resume results/evolve/scoutgpt/2026-04-04T20-00-00/
```

Install: `uv sync --extra evolve`

## Results Persistence

Each run saves to `results/evolve/{target}/{timestamp}/` (gitignored):

```
results/evolve/scoutgpt/2026-04-04T20-00-00/
├── best_program.py          # Winning candidate
├── metrics.json             # Full evaluation of winner
├── evolution_log.jsonl      # Per-iteration metrics
├── config.yaml              # Config snapshot
├── population/              # OpenEvolve checkpoint (resume)
└── seed_results/            # Baseline metrics for each seed
```

Winning architectures are promoted into the codebase manually after human review.

## Workflow Card

`workflow-cards/wf-evolve-scoutgpt.yaml`:
- **id:** wf-evolve-scoutgpt
- **runtime:** local (not hf-jobs)
- **depends_on:** wf-scoutgpt-export
- **sla_hours:** null (exploratory, on-demand)
- **cost_estimate:** $8-15 LLM API per run, $0 GPU (local)
- **metrics:** best_spearman_rho (threshold_min: 0.15, baseline: 0.094), best_top1_accuracy (threshold_min: 0.75, baseline: 0.815)

## Hardware Sizing

ScoutGPT at maximum search space bounds (512 hidden, 12 layers, batch 512):

| Component | Memory |
|-----------|--------|
| Model params (~44M x 4B) | ~176 MB |
| Adam optimizer states | ~352 MB |
| Gradients | ~176 MB |
| Activations | ~2.5 GB |
| PyTorch CUDA context | ~1 GB |
| **Total** | **~4.2 GB** |

RTX 5070 Ti (16 GB): comfortable for `parallel_evaluations: 1`, ~12 GB headroom.
DGX Spark (128 GB unified): can run `parallel_evaluations: 4-8`, cutting wall time proportionally.

Typical overnight run: ~150 iterations x ~5 min per candidate = ~12.5 hours at `parallel_evaluations: 1`.

## Dependencies

- **OpenEvolve** (>=0.3.0, Apache 2.0): evolution engine. Optional dependency group `[evolve]`.
- **openai** (transitive via OpenEvolve): talks to any OpenAI-compatible endpoint.
- **PyTorch** (already an optional dep): model training.
- No new core dependencies.

## CI

- `src/evolve/` gets ruff lint/format + pyright like all src/ code.
- Unit tests: config validation, evaluator bridge with mock backend, search space bounds.
- The `evolve` optional dependency group is NOT installed in CI.
- No benchmark tests — evolution runs are too slow for CI.

## Future Targets

The `targets/` directory is designed for extensibility:

```
targets/
├── scoutgpt/          # D32 — counterfactual player prediction (now)
├── football2vec/      # D28 — player/action embeddings (future)
├── football2vec_360/  # D29 — 360-enriched embeddings (future)
├── xg_v2/             # D25 — expected goals model (future)
└── psxg/              # D38 — post-shot expected goals (future)
```

Each target provides: seed programs, a model-specific evaluator, and a default config. Shared infrastructure (evolution engine, compute backends) is reused across all targets.

## What Gets Built Now vs. Future

| Component | Now | Future |
|-----------|-----|--------|
| `config.py` (Pydantic models) | Full | — |
| `runner.py` (CLI + OpenEvolve wiring) | Full | — |
| `evaluator.py` (bridge) | Level 1 (config dict) | Level 2 (code evolution) |
| `backends/local_cuda.py` | Full | — |
| `backends/docker.py` | Stub | Implement when needed |
| `backends/hf_jobs.py` | Stub | Implement when needed |
| `backends/remote_ssh.py` | Stub | Implement when needed |
| `targets/scoutgpt/` | Full (4 seeds + evaluator) | — |
| `targets/football2vec/` | — | When needed |
| 4 conditioning mechanisms in decoder | All 4 | LLM invents more via Level 2 |

## Files Changed

**New files (~12):**
- `src/evolve/__init__.py`
- `src/evolve/config.py`
- `src/evolve/runner.py`
- `src/evolve/evaluator.py`
- `src/evolve/backends/__init__.py`
- `src/evolve/backends/base.py`
- `src/evolve/backends/local_cuda.py`
- `src/evolve/backends/docker.py` (stub)
- `src/evolve/backends/hf_jobs.py` (stub)
- `src/evolve/backends/remote_ssh.py` (stub)
- `src/evolve/targets/scoutgpt/__init__.py`
- `src/evolve/targets/scoutgpt/evaluator.py`
- `src/evolve/targets/scoutgpt/config.yaml`
- `src/evolve/targets/scoutgpt/seed_programs/additive.py`
- `src/evolve/targets/scoutgpt/seed_programs/cross_attention.py`
- `src/evolve/targets/scoutgpt/seed_programs/film.py`
- `src/evolve/targets/scoutgpt/seed_programs/gated.py`
- `workflow-cards/wf-evolve-scoutgpt.yaml`

**Modified files (~3):**
- `src/analytics/scoutgpt_decoder.py` — add cross_attention, film, gated conditioning types
- `pyproject.toml` — optional dep group + entry point
- `.gitignore` — results/evolve/
