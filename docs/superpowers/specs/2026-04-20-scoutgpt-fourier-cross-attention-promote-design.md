# ScoutGPT `fourier_cross_attention` Promotion Cycle

**Date:** 2026-04-20
**Branch:** `evolve/scoutgpt-fourier-promote`
**Status:** Design approved; implementation plan pending (writing-plans skill next).
**Cycle-following:** ScoutGPT L2 Seed Harvest (PR #163, session 51). The harvest flagged `fourier_cross_attention` FOR PROMOTION with `rho = +0.3799` — ~20× the baseline's magnitude (`−0.0179`) and the single strongest signal across all 5 L2 seeds. This cycle validates that signal at production fidelity.

## Problem

The L2 harvest (2026-04-20) evaluated 4 unshipped L2 seeds against an `additive` baseline at evolve-scale fidelity (15 epochs, 192d/3L/6h). Two variants were flagged for promotion:

| Variant | `spearman_rho` | Δ fitness vs baseline | Disposition |
|---|---:|---:|---|
| `fourier_cross_attention` | **+0.3799** | **+0.2826** | **FLAG FOR PROMOTION** — strongest signal |
| `swiglu_conditioning` | +0.0409 | +0.0413 | FLAG FOR PROMOTION (weak; re-eval at production fidelity) |

Both variants currently exist only as OpenEvolve seed files (`src/evolve/targets/scoutgpt/seed_programs/*.py`) that are applied via runtime monkey-patching (`_apply_program` in the evolve evaluator). This cycle:

1. Ports both mechanisms into `ScoutGPTDecoder` as first-class `conditioning_type` enum values (`"fourier_cross_attention"`, `"swiglu"`).
2. Runs a 5-arm A/B at production fidelity (30 epochs, 256d/6L/8h, full 894K-episode dataset, 1000 × 100 counterfactual eval) on local hardware (1× RTX 5070 Ti + 1× DGX Spark via SSH).
3. Applies a pre-registered decision rule to promote (flip default) or archive each mechanism independently.

**Prior art:**
- RoPE-for-ScoutGPT A/B (PR #159, 2026-04-19) — same A/B template, same decision-rule discipline. RoPE was **archived** after a +0.016 rho delta fell below the noise floor.
- L2 seed harvest (PR #163, 2026-04-20) — reduced-fidelity eval that produced the promotion candidate. Shared config + fitness framework documented in `docs/evolve/scoutgpt-l2-harvest/SUMMARY.md`.

## Non-goals

- **Decomposing `fourier_cross_attention` into independent `spatial_encoding` × `conditioning_type` axes.** Future refactor; backward-compat preserved via a loader shim that maps old `conditioning_type="fourier_cross_attention"` → new `(spatial_encoding="fourier", conditioning_type="cross_attention")`. Flagged in an in-code comment on the new branch.
- **Fourier hyperparameter search.** `n_freqs=32` is hardcoded (matches the harvested seed). Surfacing it as a config knob would add an untested hyperparameter.
- **Swiglu capacity ablation.** The harvest signal was weak-positive (+0.04 fitness); a 192d seed-size arm for swiglu doesn't buy enough information to justify another 30-epoch run.
- **RoPE × Fourier/Swiglu combinations.** RoPE was rejected independently; `position_embedding="learnable"` is held constant across all arms.
- **HF Jobs compute.** Explicitly local-only this cycle (1× RTX 5070 Ti + 1× DGX Spark). HF Hub is used only for read-only dataset streaming (140 MB, revision-pinned).
- **Retraining the canonical `luxury-lakehouse/scoutgpt` checkpoint.** Promotion flips the *default value* in `ScoutGPTConfig`; producing a new canonical production checkpoint under the new default is a separate follow-up cycle if Fourier or Swiglu wins.
- **Mechanism-probe investigation.** "Why is Fourier's rho the outlier?" (per-player cluster quality on embeddings) — follow-up filed in the L2 harvest SUMMARY, not addressed here.
- **Football2Vec Fourier port.** Separate open question flagged in MEMORY; this cycle is ScoutGPT-only.

## Scope — 5-arm A/B at production fidelity, local execution

Chosen after the 3-arm-A/B-C and 4-arm-+-swiglu scopes were sequentially expanded by user approval. Final framing acknowledges that `fourier_cross_attention` is a 2-factor change (RFF spatial encoding + cross-attention conditioning), which without a mechanism-isolation arm would leave the "why did it win?" question open. Arm 5 (`cross_attention` alone) disambiguates.

## A. Architecture changes to `ScoutGPTDecoder`

Additive change following the RoPE-cycle template. No touches to existing branches; minimal diff.

### A.1 `ScoutGPTConfig.conditioning_type` enum extended

Current literal: `{additive, cross_attention, film, gated}` → new literal: `{additive, cross_attention, film, gated, fourier_cross_attention, swiglu}`.

### A.2 `__init__` module registration (`src/analytics/scoutgpt_decoder.py:71-83`)

Append two branches that register the mechanism-specific modules:

**`fourier_cross_attention`** — 4 modules, `n_freqs=32` hardcoded (matches harvest seed; `_N_FREQS * 4 = 128` is the projection output dim, the internal convention from `src/evolve/targets/scoutgpt/seed_programs/fourier_cross_attention.py`):
- `fourier_B: nn.Linear(4, 128, bias=False)` — learnable random projection from the 4-dim spatial vector `(start_x, start_y, end_x, end_y)` into a 128-dim frequency space. Learnable per Tancik's "learnable Fourier features" variant.
- `fourier_proj: nn.Linear(256, hd)` — projects 256-dim Fourier features (`[sin(B·x); cos(B·x)]` concat → 2 × 128 = 256) to `hidden_dim`.
- `fourier_cross_attn: nn.MultiheadAttention(hd, num_heads, dropout=dropout, batch_first=True)`
- `fourier_cross_norm: nn.LayerNorm(hd)`

**`swiglu`** — 4 modules:
- `swiglu_w1: nn.Linear(2 * hd, hd, bias=False)` — data path
- `swiglu_w2: nn.Linear(2 * hd, hd, bias=False)` — gating path (SiLU-activated)
- `swiglu_proj: nn.Linear(hd, hd, bias=False)` — back-projection
- `swiglu_norm: nn.LayerNorm(hd)`

**Deliberate conflation comment** — in-code:
```python
# NOTE: fourier_cross_attention bundles two architectural changes:
# (1) RFF spatial encoding (replaces the 4 SpatialMLPs) and
# (2) cross-attention conditioning (replaces additive conditioning).
# Future work: decompose into spatial_encoding x conditioning_type axes.
# See docs/superpowers/specs/2026-04-20-scoutgpt-fourier-cross-attention-promote-design.md.
```

### A.3 `_embed` computation branches (`scoutgpt_decoder.py:193-208`)

**`fourier_cross_attention`** — *rebuilds* `action_emb` from scratch, substituting RFF for the four `SpatialMLP` outputs. Preserves `token_embedding + result_embedding + time_delta_mlp + (position_embedding if learnable)`. Then cross-attention + residual + norm. (The initial `action_emb` computation at lines 181-189 is computed but discarded in this branch — a small wasted computation that keeps the diff minimal.)

```python
elif self._conditioning_type == "fourier_cross_attention":
    spatial = torch.stack([start_x, start_y, end_x, end_y], dim=-1)  # (B, S, 4)
    projected = self.fourier_B(spatial)                               # (B, S, 128)
    fourier_feats = torch.cat([torch.sin(projected), torch.cos(projected)], dim=-1)  # (B, S, 256)
    spatial_emb = self.fourier_proj(fourier_feats)                    # (B, S, hd)
    action_emb_f = (
        self.token_embedding(action_ids)
        + spatial_emb
        + self.result_embedding(result)
        + self.time_delta_mlp(time_delta)
    )
    if self.config.position_embedding == "learnable":
        action_emb_f = action_emb_f + self.position_embedding(self._pos_ids[:, :seq_len])
    attn_out, _ = self.fourier_cross_attn(query=action_emb_f, key=player_emb, value=player_emb)
    emb = self.fourier_cross_norm(action_emb_f + attn_out)
```

**`swiglu`** — reuses the shared `action_emb` built at lines 181-189. Concats with `player_emb`, splits into data path and Swish-gated path (Shazeer 2020), Hadamard product, project back with residual + norm.

```python
elif self._conditioning_type == "swiglu":
    combined = torch.cat([action_emb, player_emb], dim=-1)             # (B, S, 2*hd)
    data_path = self.swiglu_w1(combined)                                # (B, S, hd)
    gate_path = torch.nn.functional.silu(self.swiglu_w2(combined))      # (B, S, hd)
    fused = data_path * gate_path                                       # (B, S, hd) — Hadamard
    emb = self.swiglu_norm(action_emb + self.swiglu_proj(fused))
```

### A.4 Invariants

- `_encode`, `forward`, `predict`, and `train_loop` are unchanged — `conditioning_type` is transparent to them.
- `position_embedding="learnable"` is held constant for all arms this cycle. `rope` interaction with the new branches is untested and out of scope.
- `num_players`, `vocab_size`, `max_seq_len`, `spatial_mlp_dim` are unchanged from production defaults.

## B. A/B run plan

### B.1 Arm roster (5 arms)

| Arm | Name | conditioning_type | hidden_dim | num_layers | num_heads | Role |
|---|---|---|---:|---:|---:|---|
| 1 | CONTROL | `additive` | 256 | 6 | 8 | Current production default + harvest baseline |
| 2 | FOURIER@PROD | `fourier_cross_attention` | 256 | 6 | 8 | Promotion candidate — production scale |
| 3 | FOURIER@SEED | `fourier_cross_attention` | 192 | 3 | 6 | Capacity ablation (matches harvest architecture) |
| 4 | SWIGLU | `swiglu` | 256 | 6 | 8 | Promotion candidate — secondary mechanism |
| 5 | CROSS-ATTN | `cross_attention` | 256 | 6 | 8 | Mechanism isolation — Fourier spatial contribution |

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
| Train / val / test split | 715,520 / 89,440 / 89,441 (per RoPE A/B precedent) |
| Counterfactual eval | 1000 episodes × 100 players |

The unused `player_prediction_weight=0.18` from the harvest seed is **dropped** — it declared no implementation, was silently ignored by the evolve evaluator, and is not part of the mechanism being promoted.

### B.3 Local execution on 1× RTX 5070 Ti + 1× DGX Spark

- **DGX Spark** (128 GB unified, `karsten@192.168.68.73`, `~/Development/evolve-env`): larger arms (Arms 1, 2, 4, 5 at 256d) preferentially dispatched here — unified memory is most forgiving for capacity experiments.
- **RTX 5070 Ti** (primary workstation, 16 GB VRAM): smaller arm (Arm 3 at 192d) dispatched here — trivial fit. Additional larger arms as machine becomes free.
- **Pair-parallel dispatch**: one arm per machine at a time. Orchestrator polls each via `run_in_background + 30s polling`; when a machine's arm completes, dispatches the next pending arm. Over 5 arms: approximately three pair-cycles (or two pairs + one solo, depending on finish order).
- **No HF Jobs compute**. No sibling HF repos created. All artefacts stay local.

### B.4 Smoke test first (BLOCKING gate)

Before the full A/B, a smoke-test run validates env on each machine:
- 2 epochs × tiny subset (~1000 episodes) for Arm 2 (FOURIER@PROD) + Arm 4 (SWIGLU) — cheapest arms exercising the new mechanisms.
- On 5070 Ti first (fastest venue). If pass, repeat on Spark.
- Asserts: loss decreases monotonically, metrics.json written, no CUDA/dtype errors, parity-test assertions continue to pass after training.
- If smoke fails on either machine: halt, diagnose env, do not dispatch full runs.

### B.5 Dataset revision pinning

The orchestrator resolves the current HF dataset SHA once at dispatch start via `huggingface_hub.HfApi.repo_info(..., repo_type="dataset")`, writes it to a local manifest (`artifacts/fourier-scoutgpt/dispatch-manifest.json`), and passes `--dataset-revision=<sha>` to every arm's training invocation. If anyone pushes to `luxury-lakehouse/scoutgpt-training-data` mid-cycle, our arms are unaffected. If the cycle needs to re-run later for any reason, we pin to the same SHA for comparability.

### B.6 Script changes

**Execution model — critical detail**: `scripts/train_scoutgpt_hf.py` is a PEP 723 script pinned to wheel `0.3.4`. Running it via `uv run` would fetch the OLD wheel from HF Hub, which doesn't contain the new `conditioning_type` values — creating a chicken-and-egg problem (we'd need to publish wheel 0.3.5 before running the A/B that justifies the wheel bump). Resolution: the local orchestrator imports training code directly from the source tree, bypassing the PEP 723 wheel fetch.

**`scripts/train_scoutgpt_hf.py`** — add CLI args for *future* HF Jobs callers (once wheel 0.3.5 is published post-merge):
- `--conditioning-type` (choices: all 6 enum values incl. new ones)
- `--hidden-dim` (int, default None → uses config default)
- `--num-layers` (int, default None)
- `--num-heads` (int, default None)
- `--local-output-dir PATH` — when set, `_save_checkpoint` writes `{dir}/stage1/model.safetensors` + `{dir}/metrics.json` to local disk instead of uploading to HF Hub. Default behavior (HF Hub upload) preserved.

These args are not exercised by this cycle's A/B (the orchestrator doesn't invoke this script) but are tested for forward-compatibility in the unit tests.

**`scripts/run_fourier_scoutgpt_ab.py`** (new) — pure-local orchestrator, regular Python (no PEP 723 header), imports `src/analytics/scoutgpt_training.train_loop` directly from the source tree. Two modes:

- **`--mode drive` (default)**: the top-level driver. Resolves dataset SHA once at dispatch start. Maintains the 5-arm roster. Before dispatching Spark arms, rsyncs the current branch to `karsten@192.168.68.73:~/Development/luxury-lakehouse-fourier-promote/`. Dispatches each arm to its chosen machine and polls until complete. Collects `metrics.json`, applies `apply_decision_rule(...)`, writes `docs/evolve/fourier-scoutgpt/SUMMARY.md`.
- **`--mode run-arm --arm <name> --local-output-dir <path>`**: single-arm execution. Called by `drive` mode as:
  - Local (5070 Ti): subprocess invocation of the same script in `run-arm` mode.
  - Spark: SSH command `ssh karsten@192.168.68.73 "cd ~/Development/luxury-lakehouse-fourier-promote && source ~/Development/evolve-env/bin/activate && python scripts/run_fourier_scoutgpt_ab.py --mode run-arm ..."`, wrapped in `nohup` + file-based log redirect (`&> run-arm.log &`). The exact env-activation command is chosen during implementation (evolve-env already has PyTorch 2.11 + CUDA 13 per `user_local_hardware.md`).
  - In both cases, `run-arm` mode imports `train_loop` from `src/analytics/scoutgpt_training.py` and invokes it in-process with the arm's `ScoutGPTConfig`. No wheel fetch; source tree is authoritative.

Orchestrator polls each machine's log file every 30s. Re-attachable if SSH drops (training state persists via per-epoch checkpoint + metrics writes).

### B.7 Cost

- **Compute**: $0 (electricity).
- **Wall time**: unknown until smoke-test. ScoutGPT at 256d/6L/8h (~3M params) is small enough that training is likely I/O-bound, not compute-bound; 5070 Ti vs L40S gap is probably much smaller than raw TFLOPS ratio suggests. Target: all 5 arms complete within ~1-2 days wall clock.

## C. Decision rule (pre-registered — Section 3 of the brainstorm, approved)

Pre-registered in code via `src/tests/test_fourier_promotion_decision.py::apply_decision_rule`. Invoked by the SUMMARY.md generator — no post-hoc reinterpretation possible.

### Rule

For each of Fourier (Arm 2 vs Arm 1) and Swiglu (Arm 4 vs Arm 1), **PROMOTE** if both conditions hold:

- `rho_treatment − rho_control ≥ +0.10` (≥0.33σ at `rho_std ≈ 0.30`; 6× the RoPE-rejected margin of +0.016)
- `top1_treatment ≥ top1_control − 0.005` (no catastrophic next-action regression; current top1 ≈ 0.815)

Otherwise **ARCHIVE**.

### Rationale

- **Rho-primary**: the harvest signal (+0.38) was rho-specific; top1 barely moved. Using rho as the primary metric preserves what the harvest found.
- **Top1 safety floor**: prevents shipping a config that improves rho at the cost of next-action accuracy.
- **Threshold calibration**: +0.10 is 26% of the harvest's +0.38 effect size — a reasonable "signal survived the capacity jump" bar. It is 6× the RoPE-rejection margin, which was judged indistinguishable from noise.

### Arm 3 and Arm 5 are informational

- **Arm 3 (FOURIER@SEED)**: tells us whether Fourier needs production capacity. If Arm 2 promotes and Arm 3 also beats Arm 1, the mechanism is scale-invariant. If Arm 2 promotes but Arm 3 doesn't, capacity matters — this informs future priors about where to set evolve-scale fidelity.
- **Arm 5 (CROSS-ATTN)**: disambiguates the Fourier mechanism. If Arm 2 wins and Arm 5 also beats Arm 1 by a similar margin, cross-attention conditioning alone is doing the work. If Arm 2 wins and Arm 5 is flat vs Arm 1, the Fourier spatial encoding is doing the work.

Neither Arm 3 nor Arm 5 drives a promote/archive decision. They drive the "why did it win?" narrative in the SUMMARY.

## D. Validation & testing

### D.1 Parity tests (CRITICAL, blocker-on-fail)

**`src/tests/test_scoutgpt_fourier_parity.py`** and **`src/tests/test_scoutgpt_swiglu_parity.py`**:

1. Instantiate vanilla `ScoutGPTDecoder(conditioning_type="cross_attention")` and monkey-patch it with the seed's `custom_layers` + `custom_embed` (exactly as `src/evolve/targets/scoutgpt/evaluator.py::_apply_program` does).
2. Instantiate a second decoder with the new `conditioning_type="fourier_cross_attention"` (or `"swiglu"`).
3. Match random seeds at init, then copy state_dict across between the corresponding new modules so random init differences don't contaminate.
4. Forward pass on a fixed input tensor; assert `torch.allclose(monkey_patched_output, first_class_output, atol=1e-6)`.

If parity fails, the first-class branch is semantically different from what harvest scored. **Cycle halts — diagnose before running any A/B arm.**

### D.2 Config & registration tests

Extend `src/tests/test_scoutgpt_decoder.py`:
- `conditioning_type="fourier_cross_attention"` instantiates without error; `fourier_B`, `fourier_proj`, `fourier_cross_attn`, `fourier_cross_norm` registered on the model with correct shapes.
- `conditioning_type="swiglu"` instantiates; `swiglu_w1`, `swiglu_w2`, `swiglu_proj`, `swiglu_norm` registered with correct shapes.
- Invalid `conditioning_type` still raises `ValueError` with the existing message format.
- Checkpoint round-trip: save state_dict + config JSON, reload, forward-pass output matches byte-for-byte on a fixed input.

### D.3 Forward / backward tests

Extend `src/tests/test_scoutgpt_decoder.py`:
- Output shape `(B, S, hidden_dim)` for both new branches across batch sizes 1, 2, 8.
- No NaN / Inf in forward pass with a range of input tensors.
- `loss.backward()` runs without error; gradients flow to all new parameters of both new branches.

### D.4 Decision rule test

**`src/tests/test_fourier_promotion_decision.py`**:
- Pure function `apply_decision_rule(rho_ctrl, rho_trt, top1_ctrl, top1_trt) -> Literal["PROMOTE", "ARCHIVE"]`.
- Parametrized cases: margin at exactly +0.10 / −0.005 (boundary — both pass), margin just below +0.10 (archive), clear promote, top1-regression-only archive, rho-gain-only archive, RoPE historical case (delta +0.016 → ARCHIVE).
- SUMMARY.md generator imports this function — eliminates motivated-reasoning risk at disposition time.

### D.5 Smoke test (E2E, local)

`scripts/smoke_test_fourier_local.py` (not committed — a local verification script, deleted or gitignored before commit):
- 2 epochs × ~1000 episodes for Arm 2 + Arm 4.
- Runs on 5070 Ti first, then Spark. ~5 min each.
- Gate: loss decreases monotonically, metrics.json written, no CUDA/dtype errors, parity-test assertions continue to pass after training.

### D.6 Standard pre-commit gates

Must pass with zero violations before commit:
- `uv run ruff check src/ scripts/`
- `uv run ruff format --check src/ scripts/`
- `uv run pyright src/`
- `uv run pytest src/tests/ -v`

### D.7 Test gate ordering

1. Write parity tests + decision-rule test (TDD: these fail until implementation lands).
2. Land decoder + CLI changes. Parity + decision-rule tests go green.
3. Full unit + integration test suite passes locally.
4. Smoke test on 5070 Ti: Arms 2 + 4 each × 2 epochs on tiny subset.
5. Smoke test on Spark: same.
6. Full 30-epoch A/B across all 5 arms.
7. Apply `apply_decision_rule` in SUMMARY generation.
8. Full test suite + ruff + pyright one more time before commit.

## E. Artefacts & governance

### E.1 Wheel bump `0.3.4 → 0.3.5`

Required because `conditioning_type` gains 2 new enum values that must be consumable by any future PEP 723 caller (even though this cycle's training runs from source tree, not via wheel).

- Update `[project] version` in `pyproject.toml`.
- Update `WHEEL_VERSION` + `WHEEL_FILENAME` in `src/shared/wheel.py`.
- Run `uv run python scripts/bump_wheel.py` to propagate across PEP 723 scripts + Terraform + `deploy.sh`.
- `bump_wheel.py --check` must pass (enforced by CI).
- No `--pin-hash`: CI uploads the new wheel on main merge; hash pin is a follow-up if needed.

### E.2 Workflow card — `workflow-cards/wf-scoutgpt.yaml`

Append to `references:`:

```yaml
- citation: "Tancik et al. (2020). Fourier Features Let Networks Learn High Frequency Functions in Low Dimensional Domains. arXiv:2006.10739."
  role: methodology  # Fourier Random Features for spatial encoding
- citation: "Shazeer, N. (2020). GLU Variants Improve Transformer. arXiv:2002.05202."
  role: methodology  # SwiGLU conditioning
```

### E.3 `ARCHITECTURE.md` Appendix D

Add 2 rows to the "D. Academic References" table:
- `Tancik, M. et al. (2020)` → "Fourier Features Let Networks Learn High Frequency Functions in Low Dimensional Domains." arXiv:2006.10739 → `src/analytics/scoutgpt_decoder.py` (fourier_cross_attention branch), `wf-scoutgpt`
- `Shazeer, N. (2020)` → "GLU Variants Improve Transformer." arXiv:2002.05202 → `src/analytics/scoutgpt_decoder.py` (swiglu branch), `wf-scoutgpt`

### E.4 `src/tests/test_architecture_md_appendix.py`

Extend the `expected_authors` list with the two new entries so the test stays green. This is the enforcement mechanism for the CLAUDE.md rule: "`ARCHITECTURE.md` Appendix D is the living record of academic references."

### E.5 `AI_GOVERNANCE.md` — no change

ScoutGPT is system #13 in §5 with status Development. This cycle is a pure architectural change (how player conditioning is computed inside the decoder). It does not alter:
- Intended use (counterfactual evaluation of per-player decisions)
- Non-use clauses (no employment decisions, no club customer)
- Data processed (SPADL episodes, unchanged)
- Deployment context (research only, unchanged)
- §13 re-classification triggers (none fire for internal architecture changes)

### E.6 `docs/evolve/fourier-scoutgpt/SUMMARY.md`

Written after the A/B completes, mirroring the L2 harvest SUMMARY structure:
- Run metadata (timestamps, branch, machines)
- Headline table (5 arms: rho, rho_std, top1, val_loss, fitness, param_count, wall_clock)
- Cross-reference to L2 harvest (+0.38 signal) and RoPE A/B (baseline rho context)
- Pre-registered decision rule quoted verbatim
- Dispositions via `apply_decision_rule(...)` output
- Mechanism analysis narrative informed by Arms 3 and 5
- Follow-ups filed (mechanism probe, canonical checkpoint retraining if promoted, Football2Vec Fourier port)

## F. Scope & risks

### F.1 Out of scope (deferred to future cycles)

See "Non-goals" above for the full list. Summary:
1. `spatial_encoding` × `conditioning_type` axis decomposition
2. Fourier hyperparameter search (`n_freqs=32` hardcoded)
3. Swiglu capacity ablation
4. RoPE × Fourier/Swiglu combinations
5. HF Jobs compute
6. Canonical checkpoint retraining
7. Mechanism-probe (per-player cluster quality)
8. Football2Vec Fourier port

### F.2 Risks & mitigations

| # | Risk | Mitigation |
|---|---|---|
| 1 | Parity test fails (first-class branch ≠ monkey-patched harvest path) | **BLOCKER** — halt, diagnose before running any A/B arm. We must be testing the exact mechanism that harvest scored. |
| 2 | Env drift: 5070 Ti / Spark (CUDA 13, PyTorch 2.11) produces numerically different results vs L40S harvest env | Smoke-test first (2 epochs, tiny subset) on both machines. If smoke results are within noise of expected loss trajectory, proceed. If meaningfully different, investigate before burning full-run compute. Disposition notes env if numbers diverge meaningfully from harvest direction. |
| 3 | SSH session drop during Spark training | Orchestrator uses `nohup` + file-based log redirect; polls log file rather than tailing stdout. SSH can drop and reconnect without killing training. Training state persists (checkpoint + metrics written to disk). |
| 4 | Training divergence at 30 epochs despite 15-epoch success (possible for new branches at extended training) | Partial results from non-divergent arms still drive the promote/archive decision. Divergence documented in SUMMARY.md. |
| 5 | Null result (Fourier doesn't replicate at production fidelity) | Valid cycle outcome. Disposition = ARCHIVE, seed file preserved, mechanism hypothesis documented as "failed to replicate at 256d × 30ep." Cycle is still useful — it bounds future evolve priors. |
| 6 | Dispatch failure (one arm fails to start) | Orchestrator halts on first failure, logs root cause. User decides whether to retry just that arm or abort the whole cycle. No silent-skip. |
| 7 | Dataset revision drift mid-cycle | Orchestrator resolves HF dataset SHA once at dispatch start, writes to manifest, passes `--dataset-revision=<sha>` to every arm. All 5 arms see byte-identical data. |

## G. Deliverables (single commit)

### G.1 New files

- `src/tests/test_scoutgpt_fourier_parity.py`
- `src/tests/test_scoutgpt_swiglu_parity.py`
- `src/tests/test_fourier_promotion_decision.py`
- `scripts/run_fourier_scoutgpt_ab.py` (local orchestrator, not PEP 723)
- `docs/superpowers/specs/2026-04-20-scoutgpt-fourier-cross-attention-promote-design.md` (this file)
- `docs/superpowers/plans/2026-04-20-scoutgpt-fourier-cross-attention-promote.md` (next skill output)
- `docs/evolve/fourier-scoutgpt/SUMMARY.md` (written post-A/B)

### G.2 Modified files

- `src/analytics/scoutgpt_decoder.py` (add 2 conditioning_type branches + in-code comment)
- `scripts/train_scoutgpt_hf.py` (add `--conditioning-type`, `--hidden-dim`, `--num-layers`, `--num-heads`, `--local-output-dir` args)
- `pyproject.toml` (version 0.3.4 → 0.3.5)
- `src/shared/wheel.py` (WHEEL_VERSION + WHEEL_FILENAME)
- `workflow-cards/wf-scoutgpt.yaml` (references: Tancik 2020, Shazeer 2020)
- `ARCHITECTURE.md` (Appendix D: 2 new rows)
- `src/tests/test_architecture_md_appendix.py` (expected_authors: add 2 entries)
- `src/tests/test_scoutgpt_decoder.py` (extend parametrized tests for new conditioning_types)
- Plus whatever `bump_wheel.py` touches in Terraform and PEP 723 consumer scripts

### G.3 Gitignored

- `artifacts/fourier-scoutgpt/**` — per-arm checkpoints, metrics.json, dispatch manifest

## H. Approval log

- 2026-04-20 — Cycle selection (Fourier over EV2) approved.
- 2026-04-20 — Branch name `evolve/scoutgpt-fourier-promote` approved; branch created off clean `main @ a39905a`.
- 2026-04-20 — Design Section A (architecture) approved.
- 2026-04-20 — Design Section B first-cut (4 arms, HF Jobs) approved, then superseded by:
- 2026-04-20 — Design Section B revision 1 (4 arms, local execution) approved.
- 2026-04-20 — Design Section B revision 2 (5 arms — Arm 1 corrected to `additive`, added Arm 5 for mechanism isolation) approved.
- 2026-04-20 — Design Section C (validation & testing) approved.
- 2026-04-20 — HF Hub for dataset (revision-pinned, read-only streaming) confirmed as acceptable under "no HF unless required by a job that needs to run there".
- 2026-04-20 — Design Section D (artefacts & governance) approved.
- 2026-04-20 — Design Section E (scope & risks) approved.
- 2026-04-20 — Design doc committed (this file).

## I. Follow-ups (deferred)

- **Canonical checkpoint retraining**: if Fourier (or Swiglu) promotes, retrain the canonical `luxury-lakehouse/scoutgpt` checkpoint under the new default in a follow-up cycle.
- **Mechanism probe**: "Why is Fourier's rho the outlier?" — per-player cluster quality analysis on learned embeddings. Flagged in L2 harvest SUMMARY.
- **Football2Vec Fourier port**: does the Fourier spatial encoding help F2V's MLM task? `spatial_injection="fourier"` as a new value alongside `additive|concat|film`. Out of this cycle's scope.
- **Spatial encoding × conditioning type axis decomposition**: split `fourier_cross_attention` into two independent config axes, add loader shim for backward compat. Future refactor, driven by need (not mandatory).
- **Swiglu re-evaluation at production fidelity with a different seed**: if the first production run is close to the promotion threshold, a second run with `seed=43` disambiguates noise from signal.
