---
language:
  - en
license: cc-by-nc-4.0
library_name: numpy
tags:
  - sports-analytics
  - soccer
  - football
  - defensive-valuation
  - credit-assignment
  - action-context
pipeline_tag: other
---

# Defensive Credit — Per-Action and Long-Form Attribution

Attributes xG-weighted defensive value to individual defenders for the actions that
suppress a scoring opportunity — blocks, interceptions, and pressure on shot- or
cross-resulting-in-shot events. Produces two grains: a per-action defending-team aggregate
(`fct_action_defensive`) and a long-form per-(action, credited player, rule) attribution
table (`fct_defensive_credit_attributions`). **Deterministic** rules engine — no trained
weights.

Part of the (Right! Luxury!) Lakehouse soccer analytics platform. Implemented via the
silly-kicks defensive-credit family (4.87.0, TF-51).

## Method Description

`add_defensive_credit` scores the defending team's credit per action from the resolved
frame geometry, the shot outcome (`shot_blocked` / `shot_on_target_derived`), and the
per-shot expected goals (xG) of the shot the action prevented or contested. The per-action
outputs are `defensive_credit_net` / `_plus` / `_minus` (DOUBLE, `0.0` not NaN when no
credit) and `n_defensive_credits` (BIGINT). `compute_defensive_credits` emits the same
signal in long form, one row per (action, credited player, rule), with an `anchor_type` /
`sizing` / `resolution` vocabulary. The rules are deterministic.

This is a **distinct rules engine** from DEFCON — the DEFCON counterfactual estimator and
its Kim et al. (2025) methodology are documented separately in
[`defcon.md`](defcon.md) / `wf-defcon`.

### Reference

The rules engine itself — the credit taxonomy, proximity gating, and the RB Salzburg /
Tigres Femenil coaching vocabulary — is **silly-kicks-native** (TF-51), anchored to a
practitioner source (Sumpter, *Soccermatics Pro*, module 16.3), not a re-implementation of a
single published paper. Its one sub-mechanism with a published, empirically-validated precedent
is the **xT(origin) turnover sizing** that converts a giveaway into a signed value:

- Bischofberger, J., Bauer, P., & Baca, A. (2026). **"Blame is easier than praise."** *arXiv:2606.19931* (code: github.com/jonas-bischofberger/defensive-network). — derives xDT = -dxT -> +xT(origin) for a failed pass and validates the resulting fault/contribution metrics against player market value and FIFA defensive-awareness ratings.

This is a **distinct** methodology from DEFCON (Kim et al. 2025, `defcon.md` / `wf-defcon`), a
separate GNN counterfactual estimator — not the xT(origin) sizing precedent above.

## Inputs

**No training data** — this is a deterministic rules engine, not a learned model. It reads:

| Field | Source | Description |
|---|---|---|
| Resolved frame geometry (defenders) | `{catalog}.bronze.spadl_action_context` | Defender positions at event time |
| SPADL actions + `shot_blocked` | `{catalog}.bronze.spadl_actions` | Action stream + block enrichment |
| Per-shot xG | `{catalog}.bronze.xg_shot_predictions` | Pre-shot xG (from `wf-shot-xg-scorer`), LEFT-joined on the native shot identity |

The xG merge is what pins the mart **downstream of `fct_shot_xg`** (not `fct_action_values`,
which xG already `ref()`s — otherwise a dbt cycle).

## Execution

Materialised by the ADR-013 writer `src/ingestion/defensive_credit_writer.py`
(bronze → dbt staging → the two contract-enforced gold marts). Its operational scheduling
is a pending operator decision — see
[`workflow-cards/wf-defensive-credit.yaml`](https://github.com/karsten-s-nielsen/luxury-lakehouse/blob/main/workflow-cards/wf-defensive-credit.yaml).

## Intended Use

- **Defensive-value reporting**: per-defender credit for suppressing scoring chances
- **Tactical analysis**: which defenders and actions prevent the most xG
- **Research**: reproducible defensive-credit attribution on open event + tracking data

## EU AI Act — Intended Use and Non-Use

This method is published for **research and reproducibility** purposes on public,
open-licensed match data. It is **not intended for, not validated for, and not supplied
to** any use that would fall within Annex III §4 (Employment, workers management and access
to self-employment) of Regulation (EU) 2024/1689 — including recruitment or selection of
natural persons, decisions affecting work-related contractual relationships, promotion,
termination, task allocation based on individual traits, or the monitoring and evaluation
of performance and behaviour of workers for employment decisions.

Any deployer who wishes to use this method for such a purpose is responsible for performing
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

- **xG-conditioned.** Credit fires only on shot / cross-resulting-in-shot rows and is
  weighted by the pre-shot xG of the contested shot; it inherits any bias in the xG model.
- **Rule-based.** The taxonomy of credit rules is deterministic and does not adapt to
  idiosyncratic coaching vocabulary or league conventions.
- **Frame dependence.** The per-action geometry needs a resolved tracking / freeze-frame
  snapshot; coverage gaps reduce the credit signal.

## Files

No model weights. The method is implemented in source:

- `src/ingestion/defensive_credit_writer.py`

Outputs are the gold marts `{catalog}.dev_gold.fct_action_defensive` and
`{catalog}.dev_gold.fct_defensive_credit_attributions` (dbt-built, contract-enforced).

## Citation

```bibtex
@article{bischofberger2026blame,
  title={Blame is easier than praise},
  author={Bischofberger, Jonas and Bauer, Pascal and Baca, Arnold},
  journal={arXiv preprint arXiv:2606.19931},
  year={2026}
}
```

```bibtex
@software{nielsen2026defensivecredit,
  title={Defensive Credit: Per-Action and Long-Form Attribution on Open Match Data},
  author={Nielsen, Karsten Skyt},
  year={2026},
  url={https://github.com/karsten-s-nielsen/luxury-lakehouse}
}
```

## More Information

- **License**: [CC-BY-NC 4.0](https://creativecommons.org/licenses/by-nc/4.0/) — inherited from the xG lineage (Wyscout non-commercial data via the upstream training corpus)
- **Source repository**: <https://github.com/karsten-s-nielsen/luxury-lakehouse>
- **Workflow card**: [`workflow-cards/wf-defensive-credit.yaml`](https://github.com/karsten-s-nielsen/luxury-lakehouse/blob/main/workflow-cards/wf-defensive-credit.yaml)
