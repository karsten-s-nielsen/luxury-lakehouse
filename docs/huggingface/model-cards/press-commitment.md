---
language:
  - en
license: cc-by-4.0
library_name: numpy
tags:
  - sports-analytics
  - soccer
  - football
  - defensive-valuation
  - press-commitment
  - action-context
  - tracking-data
pipeline_tag: other
---

# Press Commitment — Defender Closing-Speed Valuation

Measures how hard the nearest defending player commits to pressing the ball carrier, as a
**signed** projection of the defender's velocity onto the axis toward the carrier.
**Deterministic** frame-geometry computation — no trained weights.

Part of the (Right! Luxury!) Lakehouse soccer analytics platform. Implemented via the
silly-kicks action-context pipeline (4.87.0).

## Method Description

For each on-ball action with tracking, the nearest defending player to the ball carrier is
identified and their velocity vector is projected onto the unit axis pointing from the
defender to the carrier: `v_close = vx·axis[0] + vy·axis[1]`. A defender closing down
scores positive; a retreating defender is legitimately negative, so the physical invariant
is a bounded magnitude (`|press_commitment_closing_speed| ≤ 15 m/s`), **not**
non-negativity. `press_commitment_source` records the provenance of the closing-speed
computation.

### Reference

This is a deterministic, silly-kicks-native geometric metric; it implements no single
published academic methodology. The provenance is the silly-kicks tracking-features port
(TF-51 family), not a paper.

## Inputs

**No training data** — this is a deterministic metric, not a learned model.

| Input | Source |
|---|---|
| Resolved tracking frame (defender positions + velocities) | `{catalog}.bronze.spadl_action_context` |

## Execution

Computed as a drain-native column of the action-context pipeline (module
`src/analytics/action_context/enrich.py`), materialised per-action in
`{catalog}.bronze.spadl_action_context` and surfaced in `dev_gold.fct_action_context`.
The operational contract is owned by the action-context drain — see
[`workflow-cards/wf-press-commitment.yaml`](https://github.com/karsten-s-nielsen/luxury-lakehouse/blob/main/workflow-cards/wf-press-commitment.yaml)
and `wf-action-context.yaml`.

## Intended Use

- **Pressing analysis**: quantify defender commitment to pressing on the dashboard
- **Tactical profiling**: identify players and teams with high pressing intensity
- **Research**: reproducible closing-speed computation on open tracking data

## EU AI Act — Intended Use and Non-Use

This method is published for **research and reproducibility** purposes on public,
open-licensed tracking data. It is **not intended for, not validated for, and not supplied
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
tracking corpus contains no protected attributes and therefore cannot support the
group-fairness audits required by Article 10(2)(g) without ingesting additional personal
data.

This posture is the project's remediation record for the internal audit finding
`SEC-AUDIT-v1.12.0 REG-01`. See the
[`AI_GOVERNANCE.md`](https://github.com/karsten-s-nielsen/luxury-lakehouse/blob/main/AI_GOVERNANCE.md)
gap analysis in the source repository for the project's full risk classification,
re-classification triggers, and governance posture.

## Limitations

- **Instantaneous kinematics.** Closing speed is a single-frame velocity projection; it
  does not model the defender's intent, anticipation, or whether the press succeeded.
- **Tracking-data dependence.** Requires resolved player velocities; event-only matches do
  not receive press commitment.
- **Nearest-defender only.** Only the single nearest defender is scored; coordinated
  pressing by multiple defenders is not captured by this column.

## Files

No model weights. The method is implemented in source:

- `src/analytics/action_context/enrich.py`

Output is the per-action column `press_commitment_closing_speed` (+ `press_commitment_source`)
in `{catalog}.bronze.spadl_action_context`, surfaced in `{catalog}.dev_gold.fct_action_context`.

## Citation

```bibtex
@software{nielsen2026presscommitment,
  title={Press Commitment: Defender Closing-Speed Valuation on Open Tracking Data},
  author={Nielsen, Karsten Skyt},
  year={2026},
  url={https://github.com/karsten-s-nielsen/luxury-lakehouse}
}
```

## More Information

- **License**: [CC-BY 4.0](https://creativecommons.org/licenses/by/4.0/) — inherited from open tracking sources
- **Source repository**: <https://github.com/karsten-s-nielsen/luxury-lakehouse>
- **Workflow card**: [`workflow-cards/wf-press-commitment.yaml`](https://github.com/karsten-s-nielsen/luxury-lakehouse/blob/main/workflow-cards/wf-press-commitment.yaml)
