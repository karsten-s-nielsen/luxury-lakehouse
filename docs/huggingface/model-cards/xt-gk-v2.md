---
language:
  - en
license: cc-by-nc-4.0
library_name: numpy
tags:
  - sports-analytics
  - soccer
  - football
  - goalkeeper-valuation
  - xt-gk
  - markov-possession-value
  - expected-threat
pipeline_tag: other
---

# xT-GK v2 — Markov Possession Value + Empirical Turnover

Values a goalkeeper's distribution (passes and throws) as the change in expected possession
value it produces, netted against the empirical cost of a turnover. **v2 replaces** the
retired in-repo v1 `add_xt_gk` metric (16 columns + 5 philosophy presets). The
possession-value surface and turnover cost are **fitted**; the retention model ships
bundled in the wheel.

Part of the (Right! Luxury!) Lakehouse soccer analytics platform. Implemented via the
silly-kicks `xtgk` family (4.87.0).

## Method Description

xT-GK v2 is a fitted three-part model:

1. **MarkovPossessionValue** — a possession-value surface fit on the gold action corpus
   (AC-enriched actions joined to per-shot xG, carrying a `pressure` column, `game_id`,
   `possession_id`, and start coordinates).
2. **EmpiricalTurnoverValue** — an empirically-fit turnover cost.
3. **GkRetentionModel** — a bundled retention model (per-provider variant), shipped in the
   wheel via `GkRetentionModel.from_variant`.

The surfaces are fitted by `scripts/train_xt_gk_v2_hf.py` (ADR-012, HF Jobs) on a
**v2-free** corpus (never the post-join `fct_action_context` mart, which would be a data
cycle) and delivered to a UC Volume as a single JSON envelope. The ADR-013 writer
`src/ingestion/xt_gk_v2_writer.py` loads the fitted bundle and scores `xt_gk_v2` per
GK-distribution action. `pressure_levels` round-trips in the envelope so the metric's
terciles match the corpus the surface was fit on (never refit at score time).

### References

- Eyestone, J. **"xT-GK: Expected Threat for Goalkeepers"** (course materials).
- Singh, K. (2018). **Introducing Expected Threat (xT).** <https://karun.in/blog/expected-threat.html>

## Inputs

Fit corpus (training): AC-enriched actions ⋈ `fct_shot_xg`, carrying non-null `game_id`,
`possession_id`, `start_x` / `start_y`, a `pressure` column (AC-layer), and the xG column.

Scoring inputs (inference): the v2-free `{catalog}.bronze.spadl_action_context` corpus,
pre-filtered to `is_gk_distribution` rows, with resolved keeper geometry
(`xt_gk_origin_x/_y`, `xt_gk_dest_x/_y`).

## Execution

- **Training**: `scripts/train_xt_gk_v2_hf.py` (ADR-012, HF Jobs, manual). Fits the
  possession-value + turnover surfaces and uploads the envelope to a UC Volume.
- **Inference**: the ADR-013 writer `src/ingestion/xt_gk_v2_writer.py` scores per
  GK-distribution action into `{catalog}.bronze.xt_gk_v2_predictions`; dbt joins the six v2
  columns into `{catalog}.dev_gold.fct_action_context` (a mart-join column set, not an AC
  drain column, per the ADR-013 two-tier split). The writer's operational scheduling is a
  pending operator decision — see
  [`workflow-cards/wf-xt-gk-v2.yaml`](https://github.com/karsten-s-nielsen/luxury-lakehouse/blob/main/workflow-cards/wf-xt-gk-v2.yaml).

## Intended Use

- **Goalkeeper distribution analysis**: value a keeper's passing/throwing under a
  possession-value model
- **Tactical analysis**: compare keepers' distribution value with turnover risk netted out
- **Research**: reproducible fitted xT-GK model on open event + tracking data

## EU AI Act — Intended Use and Non-Use

This model is published for **research and reproducibility** purposes on public,
open-licensed match data. It is **not intended for, not validated for, and not supplied
to** any use that would fall within Annex III §4 (Employment, workers management and access
to self-employment) of Regulation (EU) 2024/1689 — including recruitment or selection of
natural persons, decisions affecting work-related contractual relationships, promotion,
termination, task allocation based on individual traits, or the monitoring and evaluation
of performance and behaviour of workers for employment decisions.

Any deployer who wishes to use this model for such a purpose is responsible for performing
their own conformity assessment under Article 43, for drawing up the technical
documentation required by Article 11 and Annex IV, for implementing the human oversight
measures required by Article 14, for declaring accuracy metrics under Article 15, and for
ensuring the data governance obligations of Article 10 are met. Note specifically that the
match data contains no protected attributes and therefore cannot support the group-fairness
audits required by Article 10(2)(g) without ingesting additional personal data.

This posture is the project's remediation record for the internal audit finding
`SEC-AUDIT-v1.12.0 REG-01`. See the
[`AI_GOVERNANCE.md`](https://github.com/karsten-s-nielsen/luxury-lakehouse/blob/main/AI_GOVERNANCE.md)
gap analysis in the source repository for the project's full risk classification,
re-classification triggers, and governance posture.

## Limitations

- **Capability regression from v1.** The five v1 philosophy presets
  (`xt_gk_{possession,counter,direct,high_press,low_block}`) have **no v2 successor**; any
  view built on them loses that decomposition. This is a documented, accepted consequence
  of the v2 replacement.
- **Not independently construct-validated.** The v2 possession-value surface is adopted
  without a separate construct-validation study.
- **GK-distribution domain only.** The metric is defined for goalkeeper distribution
  actions; off-domain rows get NULL v2 in the mart LEFT-JOIN.
- **Fitted-surface staleness.** The surfaces are fit periodically; they do not adapt online
  and inherit the representativity of the fit corpus.

## Files

The fitted artifact is delivered to the UC Volume
`/Volumes/{catalog}/dev_gold/model_weights/xt_gk_v2/` as a single JSON envelope
(MarkovPossessionValue surfaces + EmpiricalTurnoverValue cost + metadata, with
`pressure_levels`). The retention model is bundled in the wheel. Source:

- `scripts/train_xt_gk_v2_hf.py`
- `src/ingestion/xt_gk_v2_writer.py`

Output is the six-column `xt_gk_v2` family joined into
`{catalog}.dev_gold.fct_action_context`.

## Citation

```bibtex
@misc{eyestone_xtgk,
  title={xT-GK: Expected Threat for Goalkeepers},
  author={Eyestone, John},
  howpublished={course materials}
}
```

```bibtex
@misc{singh2018expectedthreat,
  title={Introducing Expected Threat (xT)},
  author={Singh, Karun},
  year={2018},
  howpublished={\url{https://karun.in/blog/expected-threat.html}}
}
```

```bibtex
@software{nielsen2026xtgkv2,
  title={xT-GK v2: Markov Possession Value + Empirical Turnover on Open Match Data},
  author={Nielsen, Karsten Skyt},
  year={2026},
  url={https://github.com/karsten-s-nielsen/luxury-lakehouse}
}
```

## More Information

- **License**: [CC-BY-NC 4.0](https://creativecommons.org/licenses/by-nc/4.0/) — inherited from the xG lineage (Wyscout non-commercial data in the fit corpus)
- **Source repository**: <https://github.com/karsten-s-nielsen/luxury-lakehouse>
- **Workflow card**: [`workflow-cards/wf-xt-gk-v2.yaml`](https://github.com/karsten-s-nielsen/luxury-lakehouse/blob/main/workflow-cards/wf-xt-gk-v2.yaml)
