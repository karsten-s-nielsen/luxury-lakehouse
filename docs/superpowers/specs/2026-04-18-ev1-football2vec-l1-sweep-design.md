# EV1 — Football2Vec v2 Level 1 Config Sweep

**Date:** 2026-04-18
**Branch:** `evolve/football2vec-l1-sweep`
**Status:** Design approved
**TODO entry:** EV1 (Wicked)

## Problem

Football2Vec v2 stage-1 (MLM pre-training) ships with untested defaults: `hidden_dim=128`, `num_layers=4`, `num_heads=4`, `dropout=0.1`, `mask_prob=0.15`, `lr=1e-4`, `batch_size=256`, `spatial_mlp_dim=64`. The full retrain (PR #124, 2026-04-16) achieved 56.9% MLM accuracy at 15 epochs / $0.32 / 12.6 min on L40S, but no hyperparameter search has ever been run — these values are educated guesses inherited from the original Doc2Vec replacement design.

The Evolve Engine (PRs #88, #90, #91, #94, #99, #107) is operational for ScoutGPT and ready to host additional targets. Adding a Football2Vec target unlocks systematic search using existing infrastructure (OpenEvolve loop, multi-backend dispatcher, MAP-Elites, LLM-guided mutation) at zero compute cost on the local pool (RTX 5070 Ti + DGX Spark GB10).

## Solution

A new `evolve` target named `football2vec` that runs an LLM-guided sweep over the Football2Vec v2 stage-1 hyperparameters and architectural enums, using the existing OpenEvolve loop and the existing local backend pool, emitting a `best_program.py` config that can be fed to `train_football2vec_v2.py` for a full retrain.

**References:**
- DeepMind AlphaEvolve (arXiv:2506.13131) — evolutionary coding agent, MAP-Elites + LLM mutations (already cited in `wf-evolve-scoutgpt`)
- Danesi, P. (2025). Football2Vec — transformer-based player embeddings (already cited in `wf-football2vec-v2`)
- Perez et al. 2018, FiLM — Feature-wise Linear Modulation (already cited in `wf-evolve-scoutgpt`; reused for spatial_injection enum below)

No new academic citations introduced — Appendix D of `ARCHITECTURE.md` does not need updating.

## Two orthogonal axes — what EV1 covers and what it does not

The `evolve` codebase has two independent dimensions:

| Axis | L1 | L2 |
|------|----|----|
| What the LLM mutates | JSON `config = {...}` dict | PyTorch source code (`custom_embed()`, `custom_layers()`) — gated by `--code-evolution` flag, AST allowlist, restricted globals (ADR-001) |

| Axis | Stage 1 | Stage 2 |
|------|---------|---------|
| Football2Vec training phase | MLM pre-training | Adversarial competition debiasing |

| | Stage 1 (MLM) | Stage 2 (adversarial) |
|---|---|---|
| **L1 (config)** | **EV1** ← this spec | (small extension of EV1, not scoped) |
| **L2 (code)** | (no patch surface — see below) | **EV2** (separate TODO) |

EV1 is **L1 + stage-1 only**. EV2 is **L2 + stage-2** — separate TODO entry, separate spec, separate cycle.

**Why no L2 in EV1:** Football2Vec stage-1 has no conditioning surface comparable to ScoutGPT's `_embed(self, action_ids, ..., player_ids)` patch point. The encoder is `tokens + spatial_x + spatial_y + position → transformer → mean-pool` — no side signal to gate on. To get gate-space exploration on stage-1, this design adds three architectural enums into the config schema (option B in brainstorming) and lets L1 explore them — no `--code-evolution` needed, no validation profile needed, no building blocks module needed.

**Why no stage-2 in EV1:** Stage-2 has a real architectural lever (the gradient-reversal layer + team classifier head + lambda schedule). That is exactly EV2's scope and is best served by L2, not L1.

## Architecture

### Module structure (added)

```
src/evolve/targets/football2vec/
├── __init__.py
├── search_space.py                      # CandidateConfig + validate_candidate()
├── evaluator.py                         # train_and_evaluate() + dataset cache + self-contained MLM loop
├── config.yaml                          # EvolveConfig: backend=local_cuda,remote_ssh; iterations=150
├── prompts/
│   └── system_message.txt               # search-space rules + bounds for the LLM
└── seed_programs/
    ├── __init__.py
    ├── baseline.py                      # current Football2VecConfig defaults
    ├── wider.py                         # hidden_dim=192, num_heads=6
    ├── deeper.py                        # num_layers=6, hidden_dim=128
    ├── heavier_mask.py                  # mask_prob=0.20, lr=2e-4
    ├── attention_pool.py                # pooling_type="attention"
    ├── film_spatial.py                  # spatial_injection="film"
    └── sinusoidal_pos.py                # position_embedding="sinusoidal"
```

### Module structure (modified)

```
src/evolve/evaluator.py                  # validate_search_space() becomes a target-aware dispatcher
src/evolve/targets/scoutgpt/search_space.py   # extracted from evaluator.py — no behavior change
src/analytics/football2vec_transformer.py     # add 3 architectural enums (pooling_type, spatial_injection, position_embedding)
```

### New workflow card

```
workflow-cards/wf-evolve-football2vec.yaml    # mirrors wf-evolve-scoutgpt.yaml
```

### Per-target search-space dispatch (D1)

Today's `src/evolve/evaluator.py:CandidateConfig` is hard-coded to ScoutGPT (`conditioning_type`, `vaep_loss_weight`, `player_prediction_weight`). To support a per-target schema without polluting one shared bounds dict, refactor as:

1. Move ScoutGPT's `CandidateConfig` + `_BOUNDS` into `src/evolve/targets/scoutgpt/search_space.py`, exporting `validate_candidate(config: dict) -> tuple[bool, str]`.
2. Create `src/evolve/targets/football2vec/search_space.py` with its own `CandidateConfig` + `validate_candidate()`.
3. `src/evolve/evaluator.py:validate_search_space(config, target)` becomes a thin dispatcher that imports `evolve.targets.<target>.search_space:validate_candidate` and delegates.
4. The free-function `validate_search_space(config)` (no `target` arg) is preserved for backward-compat by routing to `scoutgpt`.

This is a **mechanical refactor with no behavior change for ScoutGPT** — its existing tests must remain green.

### Self-contained training loop in the evaluator (D2)

The evaluator at `src/evolve/targets/football2vec/evaluator.py` does **not** import from `scripts/train_football2vec_v2.py`. Instead, it duplicates ~40 lines of MLM train + eval logic, importing only from the wheel:

- `analytics.football2vec_transformer.{Football2VecConfig, Football2VecEncoder}`
- `ingestion.football2vec_v2_training.{Football2VecDataset, load_training_data, parse_actions, stratified_split, get_cosine_schedule_with_warmup, VOCAB_SIZE, MASK_TOKEN_ID, PAD_TOKEN_ID, WEIGHT_DECAY, WARMUP_FRACTION}`

This decision keeps `src/evolve/targets/football2vec/` importable from any backend (current local pool, future HF Jobs) and respects import-isolation rules (`scripts/` is not on the wheel). It also strips out training-script concerns the evolve loop must not touch: HF Hub publishing, checkpoint writing, MLflow logging, embedding generation. The training-script version stays unchanged.

### Module-level dataset cache (D3)

Same pattern as `src/evolve/targets/scoutgpt/evaluator.py:_load_or_cache`. A frozen `_CachedData` dataclass holds parsed action sequences + train/val/test splits + competition_id list, keyed by HF dataset repo id. First evaluation downloads + parses + tensorizes once; all subsequent evaluations reuse the cached `Football2VecDataset` instances.

Pulling 114K rows from HF Hub takes ~30s; doing that 150× is wasted wall-time and wasted HF egress. Thread-safe (the cache is also accessed from OpenEvolve's worker processes — each process has its own cache instance, but within a process the lock serialises first-load).

### Search space — 8 scalars + 3 architectural enums

| Key | Type | Range / values | Constraint | Default |
|-----|------|----------------|------------|---------|
| `hidden_dim` | int | 64–256 | divisible by `num_heads` | 128 |
| `num_layers` | int | 2–8 | — | 4 |
| `num_heads` | int | 2–8 | divides `hidden_dim` | 4 |
| `dropout` | float | 0.0–0.4 | — | 0.1 |
| `mask_prob` | float | 0.10–0.30 | — | 0.15 |
| `spatial_mlp_dim` | int | 16–128 | — | 64 |
| `learning_rate` | float | 1e-5–1e-3 (log-scale recommended in prompt) | — | 1e-4 |
| `batch_size` | int | 64–512 | power of 2 recommended | 256 |
| `pooling_type` | enum | `"mean"` \| `"attention"` \| `"cls"` | — | `"mean"` |
| `spatial_injection` | enum | `"additive"` \| `"concat"` \| `"film"` | concat requires `spatial_mlp_dim ≤ hidden_dim/2` | `"additive"` |
| `position_embedding` | enum | `"learnable"` \| `"sinusoidal"` \| `"rope"` | — | `"learnable"` |

**Excluded from sweep, with rationale:**

- `vocab_size` — SPADL-fixed at 23.
- `max_seq_len` — memory-coupled to dataset construction; varying it requires re-tensorizing the cache (defeats D3) and risks OOM on small VRAM. Hardcoded to current default 512.
- `weight_decay`, `warmup_fraction` — currently module-level constants in `ingestion/football2vec_v2_training.py`. Exposing them requires modifying the training-data module's contract; deferred to a follow-up if EV1 results suggest they matter.

### Architectural enum implementation (`src/analytics/football2vec_transformer.py`)

The 3 enums require small additions to `Football2VecEncoder`:

- `pooling_type`:
  - `"mean"` — current behaviour (sum × mask / lengths).
  - `"attention"` — learned attention pool: `nn.Linear(hidden_dim, 1)` over valid tokens, softmax-weighted sum.
  - `"cls"` — prepend a learnable CLS token at position 0; output is the CLS position embedding after encoder.
- `spatial_injection`:
  - `"additive"` — current behaviour (`tok + spatial_x + spatial_y + pos`).
  - `"concat"` — concatenate `[tok, spatial_x, spatial_y]` along feature dim, project back via `nn.Linear(3 × hidden_dim, hidden_dim)`. Raises if `spatial_mlp_dim > hidden_dim/2` (memory guard).
  - `"film"` — spatial coords produce per-channel scale + shift, applied to token embedding (Perez et al. 2018).
- `position_embedding`:
  - `"learnable"` — current behaviour (`nn.Embedding(max_seq_len, hidden_dim)`).
  - `"sinusoidal"` — fixed sinusoidal table, no parameters.
  - `"rope"` — rotary position embedding applied inside attention; uses `torch.nn.functional` only, no third-party dep.

The `Football2VecConfig` dataclass adds the three enum fields with defaults matching current behaviour (full backward compatibility — `train_football2vec_v2.py` and the inference path see no behavioural change unless they explicitly opt in).

### Sweep budget

| Setting | Value | Rationale |
|---------|-------|-----------|
| `iterations` | 150 | Upper bound; not a target. Matches ScoutGPT defaults. |
| `population_size` | 200 | Larger gene pool helps the LLM mutate; matches ScoutGPT. |
| `num_islands` | 3 | Diversity sub-populations; useful when enum knobs introduce categorical structure. |
| `migration_interval` | 30 | Periodic cross-island migration. |
| `parallel_evaluations` | 2 | Matches local backend pool (RTX 5070 Ti priority 0, DGX Spark priority 1). |
| `early_stopping_patience` | 40 | Self-correcting upper bound — if best score does not improve for 40 consecutive iterations, stop. Bounds wasted compute. |
| `checkpoint_interval` | 5 | Crash-resume insurance — every 5 iter writes a checkpoint. `--resume` rehydrates seed cache + can continue from last checkpoint. |
| `epochs` per candidate | 5 with `patience=3` | Same pattern as ScoutGPT L1. Full retrain is 15 epochs; 5+early-stop gives a strong fitness signal at ~⅓ the time. |
| `timeout_seconds` | 1800 (30 min) | Football2Vec stage-1 is ~13 min @ 15 epochs on L40S; on RTX 5070 Ti at 5 epochs we expect ~3-5 min per candidate. 30 min is a generous guard against pathological configs. |

### Wall-clock + cost

- Per-candidate: ~3-5 min on RTX 5070 Ti @ 5 training epochs (~10 min on DGX Spark GB10).
- Full 150 candidates × ~4 min ÷ 2 parallel = **~5 hours expected, overnight-friendly**.
- Realistic upper bound with early-stopping (best case): ~3 hours.
- **Cost: $0** (all local backends).

### Fitness metric (D4)

- **Primary:** `val_accuracy` (max). The single metric that exists to beat the 56.9% baseline.
- **Secondary (emitted, not weighted):** `val_loss`, `param_count`, `training_time_seconds`, `epochs_trained`.
- **Combined score:** equals `val_accuracy` (no weighted blend). FitnessConfig:
  ```yaml
  fitness:
    primary: val_accuracy
    combined_weights:
      val_accuracy: 1.0
    minimize: false
  ```

Combining with parsimony / efficiency adds tuning hyperparameters without empirical justification at this point. Trivial to extend post-hoc by inspecting the per-candidate metrics if a Pareto frontier is interesting.

### Backend (D6)

```yaml
backend:
  type: "local_cuda,remote_ssh"
  device: "cuda:0"
  ssh_host: "192.168.68.73"
  ssh_user: "karsten"
  ssh_remote_dir: "/home/karsten/Development"
  ssh_python_path: "/home/karsten/Development/evolve-env/bin/python"
```

Identical to `src/evolve/targets/scoutgpt/config.yaml`. `BackendPool` dispatches via priority queue: RTX 5070 Ti gets priority 0 (faster on small models); DGX Spark gets priority 1.

**Out of scope for the YAML default**, but supported by the engine for ad-hoc runs:
- `hf_jobs` (e.g. `--backend hf_jobs` for HF Jobs L40S at $0.35/candidate, ~$5 for full sweep).

### LLM mutation prompt

`src/evolve/targets/football2vec/prompts/system_message.txt` follows the ScoutGPT L1 prompt template:

- Lists every valid config key + range + constraint.
- Spells out divisibility (`hidden_dim % num_heads == 0`).
- Spells out enum values verbatim (LLM cannot invent novel ones — instant rejection at validation).
- Notes the `learning_rate` log-scale convention (encourages mutations like `1e-4 → 3e-4` rather than `1e-4 → 1.1e-4`).
- Encourages 1-3 changes per mutation (same as ScoutGPT).

Default LLM ensemble (from `EvolveConfig`): Sonnet 4 (80%) + Haiku 4.5 (20%) via OpenRouter, at temperature 0.7. Cost is trivial (~$2/day during continuous runs per `project_evolve_llm_costs.md`).

### Workflow card (`workflow-cards/wf-evolve-football2vec.yaml`)

Mirrors `wf-evolve-scoutgpt.yaml`:

- `id: wf-evolve-football2vec`
- `type: training`
- `domain: player-embeddings`
- `references`: AlphaEvolve (Romera-Paredes 2025), Danesi (2025), Perez (2018) — all already in the project's reference set.
- `inputs.datasets`: `luxury-lakehouse/football2vec-training-data`.
- `outputs.models`: `football2vec-evolved` (uc-volume, format=python, alias=best_program).
- `execution.training`: `trigger: manual`, `runtime: hf-jobs` (the runtime label is "where this *could* run" — actual default is local; the field accepts a free-form `flavor` string), `script: "uv run evolve --target football2vec"`, `timeout: "24h"`.
- `depends_on: [wf-football2vec-v2]` (the v2 training data is the input).
- `idempotency.strategy: full-overwrite`, `key: timestamp` (each run creates a new timestamped results dir).
- `cost.training`: `runtime: hf-jobs`, `flavor: "local-gpu"`, `rate_usd_per_hour: 0.00`, `typical_duration_minutes: 300`, `typical_cost_usd: 0.00`.
- `monitoring.metrics`: baseline `val_accuracy = 0.569`, `warn_below: 0.50`.
- `links.source_code`: `src/evolve/targets/football2vec/`, `src/analytics/football2vec_transformer.py`, this spec, plus `2026-04-04-evolve-engine-design.md` and `2026-04-05-multi-backend-dispatcher-design.md`.

### Acceptance flow (POC then full)

**Step 1 — POC smoke test (~20-25 min, $0):**

```bash
nohup uv run evolve --target football2vec --iterations 3 \
  > results/evolve/football2vec/smoke.log 2>&1 &
```

Wall-clock breakdown: 7 seed evaluations × ~4 min ÷ 2 parallel ≈ 14 min, then 3 LLM-mutation iterations × ~4 min ÷ 2 parallel ≈ 6 min, total ~20 min (plus 1× pin-memory penalty on the first seed eval per `project_evolve_openevolve_overhead.md`).

Pass criteria:
- `seed_results/*.json` exist for all 7 seed programs with non-zero `val_accuracy`.
- `best_program.py` exists at `results/evolve/football2vec/<timestamp>/best_program.py` and is a valid `config = {...}` dict.
- No exceptions in `smoke.log` stderr; no GPU OOM events.
- `metrics.json` reports a `val_accuracy` figure within ±10 pp of the 56.9% baseline (sanity bound — the POC budget is too small to expect actual improvement).

If the POC passes: commit & push to feature branch (per user instruction — see "Commit cadence" below), then proceed to Step 2.

If the POC fails: fix and re-run. Do not commit until POC is clean.

**Step 2 — Full overnight run (~5 hr, $0):**

```bash
nohup uv run evolve --target football2vec \
  > results/evolve/football2vec/full_run.log 2>&1 &
# uses iterations=150 from config.yaml
```

Pass criteria:
- Run completes (early-stop or hits iter 150) without crash.
- `best_program.py` and `metrics.json` exist.
- `metrics.json` reports `val_accuracy ≥ 0.569` (baseline floor — anything below means the search did not find an improvement, but does not block merge; documented in PR notes).
- A README-style summary of the best config + improvement-over-baseline is written to `results/evolve/football2vec/<timestamp>/SUMMARY.md` (manual artifact — generated by hand from the metrics.json).

After Step 2: commit & push if there are meaningful updates (typically just `results/evolve/football2vec/<timestamp>/SUMMARY.md` and a `TODO.md` update marking EV1 complete with the achieved val_accuracy).

### Commit cadence (per user)

- **Before Step 1:** all implementation files staged on local branch but **not committed**. POC is the verification gate.
- **After Step 1 passes (any kinks worked out):** commit & push to `evolve/football2vec-l1-sweep` — explicit user approval required for this commit.
- **After Step 2 (assuming meaningful updates):** commit & push the SUMMARY artifact + TODO.md update — explicit user approval required.
- **No PR opened** without explicit user approval. **No merge** to main without explicit user approval.

This honours the project's `feedback_no_commits_without_approval.md` and `feedback_one_commit_at_a_time.md` rules: each commit is approved separately at the moment of the decision.

## Tests

Tests live in three files (one new, two existing):

**New — `src/tests/test_evolve_football2vec.py`:**

| Test | What it asserts |
|------|-----------------|
| `test_football2vec_search_space_valid_config_passes` | Baseline config validates. |
| `test_football2vec_search_space_rejects_*` | Each bound (hidden_dim, dropout, mask_prob, lr, batch_size, num_layers, num_heads, spatial_mlp_dim) rejects out-of-range values. |
| `test_football2vec_search_space_divisibility` | Rejects `hidden_dim=130, num_heads=8`. |
| `test_football2vec_search_space_rejects_invalid_enums` | Rejects `pooling_type="max"`, `spatial_injection="cross_attention"`, `position_embedding="alibi"`. |
| `test_football2vec_search_space_concat_guard` | Rejects `spatial_injection="concat"` with `spatial_mlp_dim > hidden_dim/2`. |
| `test_football2vec_evaluator_dispatches_to_backend` | With mocked backend, evaluator passes the right kwargs and computes `combined_score = val_accuracy`. |
| `test_football2vec_seed_programs_load` | All 7 seed programs parse via `_load_program` and validate against the schema. |
| `test_card_evolve_football2vec_yaml_parses` | `WorkflowCard.from_yaml_file(...)` succeeds and the card has `links.source_code` pointing at `src/evolve/targets/football2vec/`. |

**Extended — existing `src/tests/test_football2vec_transformer.py`:**

| Test | What it asserts |
|------|-----------------|
| `test_football2vec_encoder_pooling_variants` | `Football2VecEncoder` constructs and forward-passes for each `pooling_type` value with the same input shapes. |
| `test_football2vec_encoder_spatial_variants` | `Football2VecEncoder` constructs and forward-passes for each `spatial_injection` value. |
| `test_football2vec_encoder_position_variants` | `Football2VecEncoder` constructs and forward-passes for each `position_embedding` value. |
| `test_football2vec_encoder_backward_compat` | Default `Football2VecConfig()` produces the same module structure (parameter count + named modules) as before this change. |

**Extended — existing `src/tests/test_evolve_evaluator.py`:**

| Test | What it asserts |
|------|-----------------|
| `test_per_target_search_space_dispatch` | `validate_search_space(cfg, target="scoutgpt")` and `target="football2vec"` route to the correct schema (regression guard for D1). |
| `test_evolve_evaluator_*` (existing) | All pre-existing ScoutGPT evaluator tests stay green (regression guard for the dispatcher refactor). |

**Existing pytest-benchmark tests on `Football2VecEncoder` (in `test_benchmarks.py`):** unchanged — they exercise `Football2VecConfig()` defaults, which the spec preserves byte-for-byte (every enum's default value reproduces the current architecture).

`pytest src/tests/test_evolve_* src/tests/test_football2vec_* src/tests/test_benchmarks.py` must stay green.

## Acceptance criteria (definition of done)

1. All tests above are green: `uv run pytest src/tests/test_evolve_* src/tests/test_football2vec_* -v`.
2. `uv run ruff check src/ scripts/` passes; `uv run ruff format --check src/ scripts/` passes; `uv run pyright src/` passes basic mode.
3. POC smoke test (`--iterations 3`) passes the criteria above.
4. Full overnight run (`--iterations 150`) completes without crash.
5. `best_program.py` exists and is a valid config; `metrics.json` reports a `val_accuracy` figure.
6. `SUMMARY.md` is written by hand to the results directory describing the best config and improvement-over-baseline.
7. `TODO.md` updated to remove EV1 from the On-Deck table; `MEMORY.md` updated with a new project memory file describing the EV1 cycle outcome.
8. PR description (drafted but not opened without user approval) documents what changed, what passed, and the achieved `val_accuracy`.

## Out of scope (deferred)

- HF Jobs backend on this card (config supports it via `--backend hf_jobs`, but YAML default stays local).
- Stage-2 adversarial sweep — covered by EV2.
- Auto-promote winning config to a full retrain — manual follow-up; no `train_football2vec_v2.py --config <path>` flag added in this cycle.
- Adding `weight_decay` / `warmup_fraction` to the search space — requires modifying `ingestion/football2vec_v2_training.py` constants; deferred until EV1 results suggest they matter.
- AI Governance update — `wf-evolve-scoutgpt` is not in `PER_PLAYER_EVALUATIVE_CARDS` (verified in `src/tests/test_ai_governance_md.py:26-39`); same posture for `wf-evolve-football2vec`.
- ARCHITECTURE.md Appendix D update — no new academic citation introduced.

## Follow-ups (after EV1 lands)

- If `weight_decay` / `warmup_fraction` mattering is suggested by the run: a small follow-up PR to expose them as kwargs in `_train_stage1_loop` and add them to the search space.
- If a discovered architecture (enum combo) significantly improves accuracy: promote to the default in `Football2VecConfig` and rerun the full v2 retrain via `train_football2vec_v2.py`. That is a separate cycle.
- EV2 (Wicked) remains scoped as L2 + stage-2; this spec does not constrain it.
- EV3 (Dunkin') — second RTX 5070 Ti SSH backend — independent of EV1; if it lands first, add a third entry to `BackendPool` priority queue.

## Risks

- **Risk:** Architectural enum implementations (`attention` pool, `concat` injection, `rope` position) introduce regressions for `train_football2vec_v2.py` callers expecting the original module structure.
  **Mitigation:** `test_football2vec_encoder_backward_compat` asserts default config produces equivalent module wiring; default `Football2VecConfig()` keeps every enum at its current behaviour value.

- **Risk:** OpenEvolve's initial-seed eval is ~2.5x slower in main thread on Windows (per `project_evolve_openevolve_overhead.md`). With 7 seeds, this adds up.
  **Mitigation:** Use `--resume` on retries — seed results are cached after the first successful run.

- **Risk:** Long overnight run is killed by terminal close.
  **Mitigation:** `nohup ... > log 2>&1 &` is mandatory (per `feedback_nohup_evolve.md`). PID + log path recorded in conversation before launch.

- **Risk:** DGX Spark backend appears unavailable mid-run.
  **Mitigation:** `BackendPool` already handles per-backend failures by routing to surviving backends; the failed candidate returns `fail_metrics()` and evolution continues. Run is not blocked.

- **Risk:** Search finds no improvement over the 56.9% baseline.
  **Mitigation:** Acceptable outcome — EV1's purpose is to *test* the defaults, not to *guarantee* improvement. The negative result is itself a documented data point. Spec does not block merge if `val_accuracy < 0.569`.
