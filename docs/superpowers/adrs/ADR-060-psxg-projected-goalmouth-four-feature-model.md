# ADR-060: PSxG goalmouth feature — projected goal-line crossing + 4-feature model

| Field | Value |
|---|---|
| **Date** | 2026-06-21 |
| **Status** | Accepted |
| **Deciders** | Karsten Nielsen |

## Context

PSxG (post-shot expected goals / xGOT) underpins the goalkeeper `goals_prevented` metric. After correcting the training population (D-0: true on-target `Goal/Saved/Post/Saved to Post`, 32,698 shots @ 29.9%) and adding GroupKFold-by-match out-of-sample CV (ADR-012-hardened trainer), the existing **2-feature** model (raw `end_location_y`, `end_location_z`) scored a near-random **OOS AUC 0.525, Brier 0.209 ≈ base-rate variance**. The prior model's apparent skill was an artifact of off-target contamination (off-target shots have extreme `end_location` → trivially separable → inflated AUC); no one had measured true on-target discrimination because the old trainer used a random train/test split on contaminated data.

Root cause (evidence in `docs/superpowers/specs/2026-06-21-psxg-end-location-feature-inadequacy-finding.md`): StatsBomb `end_location` is the goal-line crossing **only for goals** (`end_location_x`=120); for saved shots it is the **save point** (`end_location_x`≈118) or the deflected end position (`end_location_y`∈[21,59] vs the goal frame [36,44]). So for ~67% of the population the "placement" feature was the wrong coordinate, and goal-vs-save placement looked statistically identical (avg z 0.939 vs 0.932). A model that scores both StatsBomb and the tracking cohort (one Champion, both modalities) needs a goalmouth-target feature that is well-defined for **all** outcomes.

## Decision

Replace raw `end_location_y/z` with a **projected goal-line crossing** and model PSxG on a **4-feature** vector — `(goalmouth_dist_from_centre = min(|y_norm−0.5|, 0.5), goalmouth_z, distance_to_goal_m, shot_angle)` — built through one shared port (`analytics.goalkeeper.assemble_psxg_features`) by two modality adapters: StatsBomb projects the shot trajectory `location → end_location` onto the goal plane (x=120); the tracking cohort uses its measured ball crossing (TF-48 `shot_crossing_y/z`) directly. Distance/angle are computed identically across modalities by replicating the dbt `distance_to_goal`/`shot_angle` macros in SPADL coordinates (goal (105, 34), width 7.32 m). This lifts OOS AUC **0.525 → 0.818** (Brier 0.209 → 0.153, GroupKFold by match, n=32,698).

## Alternatives considered

| Option | Pros | Cons | Why rejected |
|---|---|---|---|
| A. Keep raw `end_location_y/z` (2-feature) | no change | near-random on the corrected population (AUC 0.525); save-point ≠ crossing | Not viable — would ship a misleading evaluative metric |
| B. Projected crossing only (dist-from-centre + z) | minimal change; placement-honest | leaves ~0.15 AUC on the table | AUC 0.673 — superseded by C at ~nil extra cost |
| D. Add GK position via StatsBomb freeze-frames | likely highest ceiling | freeze-frame parsing (medium/high effort); asymmetric (free for tracking, costly for StatsBomb); uncertain gain beyond 0.82 | Deferred (spec §8/E) — not worth the cost now |
| **C. Projected crossing + `distance_to_goal` + `shot_angle` (chosen)** | AUC 0.818; distance/angle are existing `fct_shots` columns (near-free); derivable for tracking from `start_x/y` | makes PSxG "xG conditioned on on-target" rather than placement-only | — |

`distance_to_goal`/`shot_angle` are pre-shot geometry, so this is "xG conditioned on the shot being on target" rather than pure placement xGOT — the standard for GK `goals_prevented` (it credits a keeper for stopping inherently-harder shots).

## Consequences

### Positive

- The corrected-population PSxG model is genuinely discriminative (OOS AUC 0.818), making `goals_prevented` a defensible shot-stopping metric.
- **One model scores both modalities** — StatsBomb (projection) and tracking (measured crossing) funnel through the same 4-feature port; geometry definitions match by construction (the angle is scale-invariant; distance is metres for both via a yard→metre harmonisation).
- Feature derivation lives in `analytics.goalkeeper` (`project_sb_shot_to_goal_line`, `spadl_shot_geometry`, `assemble_psxg_features`, `build_psxg_features_{statsbomb,tracking}`) — testable, single-source, no Spark.

### Negative

- Cross-modality consistency is a standing maintenance contract: the SPADL geometry in `spadl_shot_geometry` must stay in lockstep with the dbt `distance_to_goal`/`shot_angle` macros. A change to one without the other silently skews the model on one modality.
- The StatsBomb projection is a linear extrapolation to the goal line; for the small wide-deflection tail (|y_proj−40|>4, ~2% of shots) it is approximate (clipped at the post).
- The tracking read now requires `start_x/y` from `fct_action_context`.

### Neutral

- Richer covariates (GK position via freeze-frame for StatsBomb / free from frames for tracking, `shot_speed`) remain deferred (spec §8/E) — a future lever beyond 0.82.

## Related

- **Specs:** `docs/superpowers/specs/2026-06-21-psxg-end-location-feature-inadequacy-finding.md` (root-cause + confirmation), `docs/superpowers/specs/2026-06-20-psxg-tracking-extension-design.md` (§8/E deferred richer model)
- **ADRs:** complements ADR-059 (PSxG tracking-extension shot-grain fact — the mart architecture this model feeds), ADR-012 (training→production delivery)
- **Code:** `src/analytics/goalkeeper.py`, `src/analytics/psxg_tracking.py`, `src/ingestion/export_shots_on_target.py`, `src/ingestion/compute_psxg_tracking.py`

## Notes

Feature-stack sweep (GroupKFold by match, n=32,698; `tmp/psxg_proj_test.py`):

| feature set | OOS AUC | Brier |
|---|---|---|
| raw `end_location_y/z` | 0.525 | 0.209 |
| proj dist-from-centre + z | 0.673 | 0.195 |
| + z projected to goal line | 0.699 | 0.190 |
| **+ `distance_to_goal` + `shot_angle` (chosen)** | **0.818** | **0.153** |

Goal rate by projected zone (|y_proj−40|): central(0–1) 15.7%, mid(1–3) 28.7%, near-post(3–4) 51.4%, wide(>4) 11.6% — the symmetric near-post arch that distance-from-centre captures.
