---
language:
  - en
license: cc-by-nc-4.0
library_name: gensim
tags:
  - sports-analytics
  - soccer
  - football
  - player-embeddings
  - doc2vec
  - deprecated
datasets:
  - luxury-lakehouse/football2vec-training-data
pipeline_tag: feature-extraction
---

# Football2Vec v1 — Doc2Vec Player Embeddings (DEPRECATED)

> **⚠️ DEPRECATED.** This model is superseded by [`luxury-lakehouse/football2vec-v2`](https://huggingface.co/luxury-lakehouse/football2vec-v2), a transformer-based player embedding model with adversarial competition debiasing (Ganin et al. 2016). New downstream consumers MUST use v2. This card is retained for traceability and for interpretation of historical outputs.

32-dimensional player embedding vectors produced by a Doc2Vec (PV-DBOW) model trained on SPADL action sequences from open soccer event data. The v1 model was the project's first behavioural-embedding artefact and was used by the Player Similarity page until it was superseded by the v2 transformer encoder.

Part of the (Right! Luxury!) Lakehouse soccer analytics platform.

## Status

- **Status**: Deprecated (superseded 2026-03-31 by Football2Vec v2)
- **Replacement**: [`luxury-lakehouse/football2vec-v2`](https://huggingface.co/luxury-lakehouse/football2vec-v2) (192-dim transformer + adversarial competition debiasing)
- **Retained for**: Reproducibility of pre-2026-03-31 downstream analyses; documentation traceability for the governance baseline established in [`AI_GOVERNANCE.md`](https://github.com/karsten-s-nielsen/luxury-lakehouse/blob/main/AI_GOVERNANCE.md)

## Method Description

Football2Vec v1 represents each player as a "document" whose "words" are the 23-type SPADL action tokens emitted in their match-level action sequences. A Doc2Vec PV-DBOW model learns distributed representations that capture playing style at the player level.

### Algorithm

1. **Tokenisation.** For each player-match, extract the sequence of SPADL action types produced by the player during that match.
2. **Doc2Vec training.** Gensim's `Doc2Vec(dm=0, ...)` (PV-DBOW variant) is trained with player-season IDs as document tags.
3. **Inference.** The 32-dim document vector for each player-season is the output embedding.

### Reference

- Le, Q. & Mikolov, T. (2014). **Distributed Representations of Sentences and Documents.** ICML. <https://arxiv.org/abs/1405.4053>

This project's implementation was inspired by the community implementation at <https://github.com/ofirmg/football2vec> (MIT License) but is an independent re-implementation via gensim Doc2Vec.

## Training Data

| Source | Matches | Licence |
|---|---|---|
| [StatsBomb Open Data](https://github.com/statsbomb/open-data) | ~3,000 | CC-BY 4.0 |
| [Wyscout Public Dataset](https://figshare.com/collections/Soccer_match_event_dataset/4415000) | ~1,900 | CC-BY-NC 4.0 |

Training data is published as [`luxury-lakehouse/football2vec-training-data`](https://huggingface.co/datasets/luxury-lakehouse/football2vec-training-data) on HuggingFace Hub.

## Intended Use (Historical)

This model was used to power player-similarity search on the Player Similarity page prior to the v2 migration. All current downstream consumers have moved to v2; the v1 vectors are retained in `bronze.player_embeddings_raw` for historical continuity but are no longer refreshed.

## EU AI Act — Intended Use and Non-Use

This model is deprecated and is not in active use. It is published on HuggingFace Hub for **research and reproducibility** purposes on public, open-licensed match data. It is **not intended for, not validated for, and not supplied to** any use that would fall within Annex III §4 (Employment, workers management and access to self-employment) of Regulation (EU) 2024/1689 — including recruitment or selection of natural persons, decisions affecting work-related contractual relationships, promotion, termination, task allocation based on individual traits, or the monitoring and evaluation of performance and behaviour of workers for employment decisions.

Any deployer who wishes to use this model for such a purpose is responsible for performing their own conformity assessment under Article 43, for drawing up the technical documentation required by Article 11 and Annex IV, for implementing the human oversight measures required by Article 14, for declaring accuracy metrics under Article 15, and for ensuring the data governance obligations of Article 10 are met.

**Additional non-use note specific to v1**: Because this model is deprecated and has been superseded by v2 (which carries explicit competition-level adversarial debiasing from Ganin et al. 2016), any deployer considering v1 for any purpose should use v2 instead. The v1 model does not incorporate debiasing and is therefore more susceptible to league-style confounds in similarity search.

See the [`AI_GOVERNANCE.md`](https://github.com/karsten-s-nielsen/luxury-lakehouse/blob/main/AI_GOVERNANCE.md) gap analysis in the source repository for the project's full risk classification, re-classification triggers, and governance posture.

## Limitations

- **Deprecated.** See "Replacement" above.
- **Event-based only.** Captures on-ball actions. Off-ball movement, positioning, and pressing intensity are not represented.
- **No debiasing.** The v1 model has no mechanism to remove league-level or team-level style confounds. Similarity-search results correlate strongly with league rather than individual style. This was the primary reason for the v2 migration.
- **Low dimensionality.** 32-dim vectors provide coarser style discrimination than the v2 192-dim embeddings.
- **Deterministic inference only.** No uncertainty quantification.

## Files

- `football2vec_v1.model` — gensim Doc2Vec model file (PV-DBOW, 32-dim)

## Citation

```bibtex
@inproceedings{le2014distributed,
  title={Distributed Representations of Sentences and Documents},
  author={Le, Quoc and Mikolov, Tomas},
  booktitle={International Conference on Machine Learning},
  year={2014}
}
```

```bibtex
@software{nielsen2026football2vec_v1,
  title={Football2Vec v1: Doc2Vec Player Embeddings on StatsBomb and Wyscout Open Data (DEPRECATED)},
  author={Nielsen, Karsten Skyt},
  year={2026},
  url={https://github.com/karsten-s-nielsen/luxury-lakehouse}
}
```

## Companion Resources

| Resource | Description |
|---|---|
| [Football2Vec v2](https://huggingface.co/luxury-lakehouse/football2vec-v2) | **Active replacement** (192-dim transformer + adversarial debiasing) |
| [Football2Vec 360](https://huggingface.co/luxury-lakehouse/football2vec-360) | Alternative active model with 360 freeze-frame context |
| [Training Data](https://huggingface.co/datasets/luxury-lakehouse/football2vec-training-data) | SPADL action sequences used for training |

## More Information

- **License**: [CC-BY-NC 4.0](https://creativecommons.org/licenses/by-nc/4.0/) — inherited from Wyscout training data
- **Source repository**: <https://github.com/karsten-s-nielsen/luxury-lakehouse>
- **Workflow card**: [`workflow-cards/wf-football2vec.yaml`](https://github.com/karsten-s-nielsen/luxury-lakehouse/blob/main/workflow-cards/wf-football2vec.yaml) — `status: deprecated`
