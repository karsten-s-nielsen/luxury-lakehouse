---
language:
  - en
license: cc-by-nc-4.0
library_name: pytorch
tags:
  - sports-analytics
  - soccer
  - football
  - transformer
  - decoder
  - counterfactual
  - player-substitution
  - development-status
datasets:
  - luxury-lakehouse/scoutgpt-training-data
pipeline_tag: feature-extraction
---

# ScoutGPT — Player-Conditioned Causal Decoder (DEVELOPMENT)

> **⚠️ Development status.** This model is not yet released. The architecture and training recipe are implemented and smoke-tested on HF Jobs GPU, but the model has not yet reached the evaluation thresholds that would qualify it for production consumption. This card exists for governance traceability and to document the intended design; a production-grade card will replace it when the model ships.

Player-conditioned GPT-style causal decoder trained on SPADL possession episodes. Each action token is conditioned on a focal-player embedding, enabling counterfactual substitution: swap the focal player ID to predict "what would Player X do here?" Follows Hong et al. (2025).

Part of the (Right! Luxury!) Lakehouse soccer analytics platform.

## Status

- **Status**: Development (as of 2026-04-14)
- **Production release target**: Not scheduled; gated by evaluation thresholds (see §Evaluation below)
- **HuggingFace Hub repo**: Not yet created. Deferred to post-release.

## Method Description

### Architecture

| Component | Detail |
|---|---|
| **Token embedding** | 23 SPADL action types + PAD + BOS → 256d lookup |
| **Player embedding** | 11,918 players → 256d (per-action attribution + position-0 conditioning) |
| **Spatial encoding** | 4× SpatialMLP (start_x/y, end_x/y) + 1× SpatialMLP (time_delta), each → 256d |
| **Result embedding** | Binary success/fail → 256d |
| **Decoder** | 6-layer causal transformer, 8 attention heads, GELU activation |
| **Pooling** | Mean pooling over valid tokens → 256d player-episode representation |
| **Primary head** | Next action type (23-class cross-entropy) |
| **Auxiliary head** | VAEP regression (MSE, loss weight 0.1) |

Approximate parameter count: ~11M.

### References

- Hong, S. et al. (2025). **ScoutGPT: Player-conditioned Football Language Model for Counterfactual Evaluation.** arXiv:2512.17266. <https://arxiv.org/abs/2512.17266>
- Decroos, T., Bransen, L., Van Haaren, J., & Davis, J. (2019). **Actions Speak Louder than Goals: Valuing Player Actions in Soccer.** KDD.

## Training Data

SPADL possession episodes with per-action player attribution, published as [`luxury-lakehouse/scoutgpt-training-data`](https://huggingface.co/datasets/luxury-lakehouse/scoutgpt-training-data) on HF Hub (source dataset, not yet populated pending production release).

Underlying open-data sources:

| Source | Licence |
|---|---|
| [StatsBomb Open Data](https://github.com/statsbomb/open-data) | CC-BY 4.0 |
| [Wyscout Public Dataset](https://figshare.com/collections/Soccer_match_event_dataset/4415000) | CC-BY-NC 4.0 |

## Training

Runtime: HF Jobs GPU (`l40sx1`), script `scripts/train_scoutgpt_hf.py`, typical duration ~120 minutes per run.

## Evaluation

Acceptance thresholds for production release (not yet met):

| Metric | Baseline | Threshold for production |
|---|---|---|
| `next_action_top1_accuracy` | 0.04 (random-ish) | ≥ 0.20 |
| `counterfactual_spearman_rho` | 0.00 | ≥ 0.15 |

Additional evaluation protocol (once thresholds are met):

- Next-action accuracy (top-1, top-5) stratified by episode length
- Counterfactual ranking correlation (Spearman ρ over 1K episodes, 100 player swaps)
- Cross-source validation gap (StatsBomb vs. Wyscout)

## Intended Use (when released)

- **Counterfactual action prediction**: "What would Player X do in this situation?" — substitute the focal-player ID and predict the next action
- **Player similarity via decoding behaviour**: Compare how two players would complete the same counterfactual episode
- **Research**: Reproducible player-conditioned decoder on open event data

## EU AI Act — Intended Use and Non-Use

This model is in development and has not been released for any use. When and if it reaches production, it will be published for **research and reproducibility** purposes on public, open-licensed match data. It will **not** be intended for, validated for, or supplied to any use that would fall within Annex III §4 (Employment, workers management and access to self-employment) of Regulation (EU) 2024/1689 — including recruitment or selection of natural persons, decisions affecting work-related contractual relationships, promotion, termination, task allocation based on individual traits, or the monitoring and evaluation of performance and behaviour of workers for employment decisions.

**Special care note for generative models.** Counterfactual player substitution is a design that is *more* likely to be mistaken for a decision system by a downstream deployer than a regression model is. When this model reaches production, the HF Hub card and the source-repo card will both carry an explicit banner stating that the model does not evaluate actual players' performance — it predicts hypothetical action probabilities conditioned on a focal-player embedding — and that these predictions are not fit-for-purpose inputs to any employment-related decision without a full Article 14 human-oversight process administered by the deploying organisation.

Any deployer who wishes to use this model for such a purpose (now or after release) is responsible for performing their own conformity assessment under Article 43, for drawing up the technical documentation required by Article 11 and Annex IV, for implementing the human oversight measures required by Article 14, for declaring accuracy metrics under Article 15, and for ensuring the data governance obligations of Article 10 are met.

See the [`AI_GOVERNANCE.md`](https://github.com/karsten-s-nielsen/luxury-lakehouse/blob/main/AI_GOVERNANCE.md) gap analysis in the source repository for the project's full risk classification, re-classification triggers, and governance posture. Note that the release of this model to a wider audience is itself an explicit re-classification trigger under §13 of that document.

## Limitations

- **Development status**: Performance thresholds not yet met. Production use is actively gated.
- **Player-ID leakage risk**: Because the architecture conditions on a focal-player embedding, the decoder can memorise per-player action preferences in a way that may not generalise. Counterfactual substitution of a rare player may produce unreliable predictions.
- **Event-based only**: No tracking-data context.
- **Open data only**: Trained on StatsBomb and Wyscout; cross-source differences in event classification propagate.
- **No uncertainty quantification**: Output probabilities are point estimates; no MC dropout or Bayesian approximation at inference.
- **No temporal generalisation guarantees**: The model may overfit to the strategic patterns of its training corpus era and degrade for contemporary play.

## Files

No files published yet. On release:

- `scoutgpt.safetensors` — causal decoder weights
- `config.json` — architecture and training configuration
- `player_index.json` — mapping of player IDs to embedding rows
- `metrics.json` — evaluation metrics and training configuration

## Citation

```bibtex
@article{hong2025scoutgpt,
  title={ScoutGPT: Player-conditioned Football Language Model for Counterfactual Evaluation},
  author={Hong, Seungwoo and others},
  journal={arXiv preprint arXiv:2512.17266},
  year={2025}
}
```

```bibtex
@inproceedings{decroos2019actions,
  title={Actions Speak Louder than Goals: Valuing Player Actions in Soccer},
  author={Decroos, Tom and Bransen, Lotte and Van Haaren, Jan and Davis, Jesse},
  booktitle={KDD},
  year={2019}
}
```

```bibtex
@software{nielsen2026scoutgpt,
  title={ScoutGPT: Player-Conditioned Causal Decoder on Open Event Data (Development)},
  author={Nielsen, Karsten Skyt},
  year={2026},
  url={https://github.com/karsten-s-nielsen/luxury-lakehouse}
}
```

## More Information

- **License**: [CC-BY-NC 4.0](https://creativecommons.org/licenses/by-nc/4.0/) — inherited from Wyscout training data
- **Source repository**: <https://github.com/karsten-s-nielsen/luxury-lakehouse>
- **Workflow card**: [`workflow-cards/wf-scoutgpt.yaml`](https://github.com/karsten-s-nielsen/luxury-lakehouse/blob/main/workflow-cards/wf-scoutgpt.yaml) — `status: development`
