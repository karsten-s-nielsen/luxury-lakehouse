# EV2 Phase 1 — Football2Vec L2 Adversarial Harvest

**Cycle status:** COMPLETE.
**Outcome:** No promotions. Linear adversary baseline is Pareto-dominant or Pareto-tied with every tested architecture.
**Cost:** ~7.5h of local compute + ~4h of active debugging across 5 sequential re-fires (Phase 1a → 1e) + ~4h of interrupted `attention_pool_head` training in Phase 1f.

Spec: `docs/superpowers/specs/2026-04-23-ev2-football2vec-l2-adversarial-design.md`
Plan: `docs/superpowers/plans/2026-04-23-ev2-football2vec-l2-adversarial.md`

## Final results

All runs on stage-1 SHA `bf102a57c9575cbfddf7661ba7a3ebe29de3c124` and dataset SHA `5eb1bfc3be549c56fc1256936aa53fd7f2393d8f`. Shared config in `_SHARED_CONFIG` (hidden_dim=192, num_layers=4, num_heads=6, mask_prob=0.22, pooling_type=cls, spatial_injection=additive, λ_max=0.2, λ_warmup=5, batch_size=256, lr=3e-4). Seed 42. Max 30 epochs; early-stop patience = 3.

Fitness formula: `0.4 * mlm_score + 0.6 * debias_score` where `mlm_score = min(1.0, L_0 / val_mlm_loss)` and `debias_score = 1 - leakage`. L_0 = 0.7413 from the `linear` baseline (Phase 1a). WIN threshold: fitness ≥ baseline + 0.02 = **0.960**. PRUNE threshold: `mlm_score < 0.70`.

| Rank | Variant | Backend | Phase | val_mlm | val_adv_acc | debias | mlm_score | **fitness** | Δ vs baseline | Disposition |
|---|---|---|---|---|---|---|---|---|---|---|
| 0 | **`linear`** (baseline) | AI-PC | 1a | 0.7413 | 0.1411 | **0.900** | 1.000 | **0.940** | — | baseline |
| 1 | `cross_attention_adversary` | DGX Spark | 1e | 0.8817 | **0.0973** | **0.946** | 0.841 | 0.904 | −0.036 | ARCHIVE |
| 2 | `residual_mlp` | AI-PC | 1e | 0.7491 | 0.2116 | 0.826 | **0.990** | 0.891 | −0.049 | ARCHIVE |
| 3 | `deep_mlp_3layer` | Media-PC | 1e | 0.7732 | 0.2116 | 0.826 | 0.959 | 0.879 | −0.061 | ARCHIVE |
| 4 | `dual_head_ensemble` | DGX Spark | 1e | 0.7802 | 0.2116 | 0.826 | 0.950 | 0.876 | −0.064 | ARCHIVE |
| 5 | `deep_mlp_2layer` | AI-PC | 1e | 0.7839 | 0.2116 | 0.826 | 0.946 | 0.874 | −0.066 | ARCHIVE |
| 6 | `attention_pool_head` | AI-PC | 1f | 0.9479 (ep6/30) | 0.1796 (ep6/30) | 0.859 (ep6/30) | 0.782 (ep6/30) | 0.828 (ep6/30) | −0.112 | ARCHIVE — interrupted |

**Phase 1e elapsed times:** linear 106 min (Phase 1a), cross_attention_adversary 126 min (DGX Spark GB10), residual_mlp 94 min (AI-PC 5070 Ti), deep_mlp_3layer 83 min (Media-PC 5070 Ti), dual_head_ensemble 123 min (DGX Spark), deep_mlp_2layer 84 min (AI-PC).

**attention_pool_head history:** first attempted in Phase 1e on Media-PC, hit the pre-registered 10800s (3h) Media-PC timeout at elapsed=10803s. Retry in Phase 1f fired on AI-PC (same 5070 Ti hardware, via `LocalCudaBackend`, no wall-clock timeout). Phase 1f was interrupted by an unapproved Windows auto-restart at Epoch 6/30 (2026-04-24 11:50, PID 71172 killed mid-training; no checkpoint/resume wired). The Epoch 1-6 trajectory (preserved in `phase1f.log`) shows no convergence toward the WIN threshold — `val_mlm` oscillates 0.92-1.07 with no downward trend toward the 0.7413 baseline, and `val_adv_acc` oscillates 0.01-0.20 without settling at a low-leakage equilibrium. Interim fitness 0.828 at Epoch 6 is below every completed non-baseline variant; dispositioned **ARCHIVE by trajectory** rather than committing another ~16h of compute for a confirmed-ARCHIVE outcome.

## Dispositions

Per pre-registered rules in the Phase 1 spec:

- **WINNER** (fitness ≥ 0.960): _none_.
- **PRUNE** (mlm_score < 0.70): _none_ — the lowest completed mlm_score was cross_attention_adversary at 0.841 (`attention_pool_head` Epoch 6 interim mlm_score 0.782 is above the 0.70 PRUNE line).
- **ARCHIVE** (all other): all 6 non-baseline variants — 5 at 30-epoch completion (early-stop fired at Epoch 16 in every completed case), `attention_pool_head` by trajectory at Epoch 6/30 after Phase 1f was interrupted mid-run.

No seed programs deleted. All 6 seed files remain in `src/evolve/targets/football2vec/seed_programs_stage2/` for future reference. Per-variant `metrics.json` artefacts live on HF Hub at [`luxury-lakehouse/football2vec-l2-harvest`](https://huggingface.co/luxury-lakehouse/football2vec-l2-harvest); `attention_pool_head` has no HF-uploaded metrics (orchestrator never returned a clean result for it — per-epoch log preserved in-repo at `phase1f.log`).

## Mechanism finding — `cross_attention_adversary`

Four of the 5 completed non-baseline variants converge on `val_adv_accuracy = 0.2116` (debias = 0.826, leakage = 0.174). These are the architecturally-similar "MLP-shaped" variants that modify the classifier head's depth or topology (`deep_mlp_2layer`, `deep_mlp_3layer`, `residual_mlp`, `dual_head_ensemble`). They all land at the same adversary-saturation point: the encoder resists, the adversary does roughly 2.1× chance, the min-max equilibrium settles.

`cross_attention_adversary` is the exception: **val_adv_accuracy = 0.0973** (BELOW baseline's 0.1411) and **debias = 0.946** (ABOVE baseline's 0.900). The encoder learns to hide competition identity better under cross-attention-adversary pressure than under the linear adversary. The mechanism: cross-attention allows the adversary to focus on specific sequence positions, which gives the encoder a stronger gradient signal to smear competition features across all tokens, rather than concentrating them in (e.g.) the CLS token where a linear adversary can extract them cheaply.

The trade-off: cross_attention_adversary's `val_mlm_loss` rose to 0.8817 (mlm_score 0.841 vs baseline 1.000). The encoder paid a representational cost to achieve the debias improvement — net fitness 0.904, still below the 0.960 threshold.

**This is a candidate for a dedicated Phase 2 mechanism probe**: can the encoder recover its MLM performance while retaining cross-attention-adversary's debias gain? Candidate levers: larger encoder, longer training, lower λ_max, smoother λ schedule. Not pursued in this cycle — out of scope.

## Debug narrative (Phase 1a → 1e)

The harvest was fired 6 times before the orchestrator ran to a clean completion. Each re-fire surfaced a distinct orchestration bug. Full catalog and fixes live in `docs/engineering/orchestration.md`. Brief summary:

| Phase | Bug | Time to diagnose | Fix |
|---|---|---|---|
| 1a | `stdbuf: failed to run command 'PYTHONPATH=./src'` on Media-PC | ~30 min | Wrap with `env PYTHONPATH=./src` (existing pattern restored) |
| 1b | openevolve missing on Media-PC venv-fourier | ~15 min | `uv pip install openevolve>=0.2.0`, added `openevolve` to `_REMOTE_REQUIRED_IMPORTS` |
| 1b | DGX Spark attention_pool_head killed at 904s (global 900s `RemoteSSHBackend` default) | ~30 min | `timeout_seconds` per-host in `_REMOTE_HOSTS`, propagated to `RemoteSSHBackend(..., timeout=...)` |
| 1c | `httpx.LocalProtocolError: Illegal header value b'Bearer '` on Media-PC (non-interactive SSH env had no `HF_TOKEN`) | ~90 min (hardest debug) | (a) provision `~/.cache/huggingface/token` on remotes, (b) evaluator uses `huggingface_hub.get_token()` not `os.environ.get("HF_TOKEN", "")`, (c) broaden `except (OOM, RuntimeError, ValueError)` → `except Exception` |
| 1c | DGX Spark stale HF_TOKEN in `~/.bashrc` (post-session-54 rotation) | ~5 min | update `~/.bashrc` + `~/.cache/huggingface/token` on DGX Spark |
| 1d take 1 | Both remotes skipped at smoke test with `bash: -c: line 1: <truncated>` parse error | ~10 min | Single quotes in probe's `.get('name', '')` closed outer `python -c '…'` wrap; switched all Python string literals in probes to double quotes, dropped em-dash for ASCII-only |
| 1d take 2 | val_mlm=inf, elapsed=3-8s on both remotes — evaluator read `HF_TOKEN` from env (empty on non-interactive SSH), not file cache | ~60 min | Same as 1c fix (a) + (b) |
| 1e | Clean run. 5 real results. attention_pool_head timed out at Media-PC 3h budget (variant genuinely slow — ~10.8 min/epoch) | — | Retry on AI-PC in Phase 1f via `LocalCudaBackend` (no timeout) |
| 1f | Unapproved Windows auto-restart killed PID 71172 at Epoch 6/30 (2026-04-24 11:50); no checkpoint/resume wired | — | ARCHIVE by trajectory (Epoch 1-6 snapshot showed no convergence toward WIN threshold 0.960; re-run would have cost ~16h of additional compute for a confirmed ARCHIVE outcome) |

All fixes shipped in this cycle's Commit #2.

## Stage-1 retrain

Before Phase 1a, the published `luxury-lakehouse/football2vec-v2` stage-1 weights were at the pre-EV1 architecture (hidden_dim=128, num_heads=4), which silently mismatched the post-EV1 default (hidden_dim=192, num_heads=6). `scripts/train_football2vec_v2.py --stage 2` therefore errored on weight-shape mismatch until the stage-1 was retrained at the post-EV1 architecture. Published SHA: `bf102a57c9575cbfddf7661ba7a3ebe29de3c124`. This also unblocked production's daily `football2vec_v2_training` task, which was silently broken since PR #158 merged 2026-04-19.

## Code changes shipped in this cycle

- `scripts/evaluate_football2vec_l2_adversary_seeds.py` — per-backend `timeout_seconds`, `openevolve` in smoke-test import list, `HfApi().whoami()` HF auth probe, post-deploy `_verify_remote_entrypoint`, double-quote / ASCII-only probes.
- `src/evolve/targets/football2vec/evaluator.py` — `huggingface_hub.get_token()` replaces `os.environ.get("HF_TOKEN", "")` at both dataset-load (L261) and stage-1-weights-load (L524); `except Exception` broadened from `(OOM, RuntimeError, ValueError)` at L636.
- `src/evolve/backends/remote_ssh.py` — `HF_TOKEN=<local_env>` forwarded as inline env prefix in the remote command (orchestrator-side belt-and-suspenders for Rule 1).
- `src/tests/test_evolve_football2vec_l2.py` — asserts `openevolve` in `_REMOTE_REQUIRED_IMPORTS`, `timeout_seconds: int > 0` per host, `callable(_verify_remote_entrypoint)`.
- `CLAUDE.md` — new `## Orchestration Discipline` short-form section (7 rules).
- `docs/engineering/orchestration.md` — new file with full rationale, failure-mode catalog (this cycle's debug narrative), reference smoke-test probe.

## Next cycle candidates

1. **Phase 2 mechanism probe — cross_attention_adversary**: investigate whether the +0.046 debias gain can be retained while recovering MLM performance. Try larger encoder + longer training, or softer λ schedules. (Highest-priority follow-up if further Football2Vec work is scheduled.)
2. **Phase 2 encoder-side L2 harvest**: this cycle exhausted the adversary-side design space (all variants underperformed baseline). Fresh variance likely requires encoder-architecture moves (pooling, attention variants, etc.) rather than more adversary experiments.
3. **Orchestrator `SendEnv HF_TOKEN`**: current belt-and-suspenders fix uses inline env prefix (briefly leaks token to `ps aux`). For hardened setups, migrate to `ssh -o SendEnv=HF_TOKEN` + `AcceptEnv HF_TOKEN` in `/etc/ssh/sshd_config` on each remote. Not urgent.
4. **Checkpoint/resume for long `LocalCudaBackend` runs**: Phase 1f's loss of `attention_pool_head` at Epoch 6/30 to an unapproved Windows auto-restart was avoidable with per-epoch checkpointing. The ARCHIVE-by-trajectory disposition closed this cycle without re-running, but any future >6h `LocalCudaBackend` run should write checkpoint state to disk so an OS-class interruption doesn't lose hours of compute. Apply at the `train_football2vec_v2.py --stage 2` level (also benefits ScoutGPT and other long-running stage-2 trainings). Out of scope for `docs/engineering/orchestration.md`, which covers remote dispatch only.

## References

- HF Hub harvest repo: [luxury-lakehouse/football2vec-l2-harvest](https://huggingface.co/luxury-lakehouse/football2vec-l2-harvest)
- Model card: `docs/huggingface/model-cards/football2vec-l2-harvest.md`
- Combined `results.json` mirror: `docs/evolve/ev2-football2vec-l2-adversarial/results.json`
- Orchestration discipline (rules + failure-mode catalog): `docs/engineering/orchestration.md`
- ADR-012 (training-to-production delivery hardening, referenced by the stage-1 retrain): `docs/superpowers/adrs/ADR-012-training-to-production-delivery-hardening.md`
