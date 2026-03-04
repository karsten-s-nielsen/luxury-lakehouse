# (Right! Luxury!) Lakehouse — Roadmap

Research directions, long-horizon features, and exploratory ideas beyond the [phased plan](PLAN.md). Items here are **unscheduled** — they represent valuable directions that may graduate into numbered phases as prerequisites are met and priorities clarify.

**Last updated**: 2026-03-04

---

## Line-Breaking Pass Detection

> **Complete** — implemented in Phase 13. See [PLAN.md §8.5](PLAN.md#85--phase-13-line-breaking-pass-detection).

**Status:** Complete (Phase 13, PR #18)
**License:** Apache 2.0 ([parmacalcio1913/line-breaking-passes](https://github.com/parmacalcio1913/line-breaking-passes))

Ward hierarchical clustering + cross-product straddle test for defensive line penetration. Two data paths: StatsBomb 360 (323 matches) and Metrica tracking (3 matches). Gold arrows on Pass Map, LB Passes/90 on Player Radar.

---

## Visual Exploratory Behavior (Pose-Enhanced Tracking)

**Status:** Blocked by data procurement — active paths being pursued
**License:** BSD 3-Clause ([USSoccerFederation/ssac26_visual_exploratory_behavior](https://github.com/USSoccerFederation/ssac26_visual_exploratory_behavior))
**Paper:** Bekkers (2026), "Wide Open Gazes: Quantifying Visual Exploratory Behavior in Soccer with Pose Enhanced Positional Data" (SSAC26)

Probabilistic 2D vision model: for each player at each frame, computes a pitch-surface probability grid of what they can see, accounting for head rotation (120-degree FoV), speed-dependent perception decay, and occlusion by other players' torsos.

### Why it matters

The paper proves that aggregated vision features improve prediction of pitch value gained (AUC 0.744 to 0.788 with vision, +0.0 without), while traditional VEA counting (head movements > 125 deg/s) adds zero predictive power. This is the frontier of off-ball analysis.

### Hard blocker: pose data

The model requires **`head_angle`** and **`shoulders_angle`** per player per frame — data from pose estimation applied to broadcast video. None of luxury-lakehouse's current tracking sources provide these angles.

| Data Source | Has pose angles? | Viable? |
|-------------|-----------------|---------|
| Metrica / IDSSE / SkillCorner tracking | No | No |
| StatsBomb 360 freeze frames | No | No |
| **Respo.Vision** (commercial) | Yes | Yes — active inquiry |
| **Parma Calcio contact** (sample data) | Possibly | Yes — active inquiry |

### What's ready now

The `Vision` class is a clean NumPy/scipy implementation. Once pose data arrives, integration is straightforward:
- Narrow format matches `fct_tracking_frames` (no format adaptation)
- Pitch dimensions are configurable
- Speed is already computed in our tracking pipeline
- Combines naturally with Phase 11 pitch control via element-wise matrix multiplication

### Potential artifacts (once data available)

| Artifact | Layer | Description |
|----------|-------|-------------|
| `src/ingestion/pose_tracking.py` | Ingestion | Ingest pose-enhanced tracking (Respo.Vision format) |
| `src/analytics/vision.py` | Analytics | Adapted `Vision` class for 120x80 coordinate system |
| `int_vision_maps.sql` | dbt intermediate | Per-player per-frame vision metrics |
| `fct_player_stats.sql` (update) | dbt marts | Vision-derived per-90 stats |
| Heat Map page (update) | Streamlit | Vision map overlay on tracking viz |

### Dependencies

- Pose-enhanced tracking data procurement (blocker)
- Phase 11 (pitch control) — for vision x pitch control x pitch value framework
- Phase 10 (tracking) — **complete** (velocity computation ready)

---

## Staging Environment (Lakebase Branching)

**Status:** Design phase
**Budget impact:** Moderate — second Lakebase project with scale-to-zero minimizes idle cost

Currently the platform has a single `dev` environment. Adding a `staging` environment leverages Lakebase's unique serverless PostgreSQL capabilities — particularly **copy-on-write database branching** — for pre-production validation without duplicating the full data pipeline.

### Why it matters

- **Lakebase branching**: Create lightweight branches of the production database for testing schema changes, index strategies, and synced table migrations — without affecting dev
- **dbt environment isolation**: Run `dbt build --target staging` against a separate Gold schema, validating transformations before promoting to dev
- **Synced table dry-run**: Test synced table schema changes (the current delete-drop-recreate workflow) in staging before applying to dev
- **Learning objective**: Hands-on experience with Lakebase's PostgreSQL branching, which is a differentiating capability vs. traditional RDS

### Implementation sketch

| Component | Dev (current) | Staging (new) |
|-----------|--------------|---------------|
| Unity Catalog schema | `dev_bronze`, `dev_silver`, `dev_gold` | `staging_bronze`, `staging_silver`, `staging_gold` |
| Lakebase project | `soccer-analytics-dev` | `soccer-analytics-staging` |
| Lakebase branch | `production` | `staging` (branched from dev production) |
| dbt target | `dev` | `staging` |
| Synced tables | 11 tables | Subset (fact tables only for validation) |
| Terraform | `terraform/environments/dev/` | `terraform/environments/staging/` |
| Budget | Under $100/month | Minimal incremental (scale-to-zero) |

### Key decisions to make

1. **Branch source**: Branch staging from dev's production, or maintain independently?
2. **Data scope**: Full data replication or subset (e.g., 1 competition per source)?
3. **CI integration**: Should GitHub Actions run `dbt build --target staging` on PRs?
4. **Synced table subset**: Which tables justify staging replication?

### Dependencies

- No blocking dependencies — can be implemented at any time
- Terraform module refactoring to support multi-environment

---

## Graph-Based Tactical Pattern Recognition

**Status:** Research direction
**Paper:** Raabe, Nabben & Memmert (2022), "Graph representations for the analysis of multi-agent spatiotemporal sports data" (*Applied Intelligence*, CC BY open access)

Proposes **Tactical Graphs** — representing players as graph nodes and spatial interactions as edges — processed by lightweight Tactical Graph Networks (TGNets) for classifying defensive outcomes from tracking data. Key finding: graph representations match or outperform CNN/LSTM approaches at a fraction of computational complexity.

### Relevance

- Directly applicable to luxury-lakehouse's 20 tracking matches (38M frames)
- Player-to-player distance edges naturally model defensive structure
- Graph representation is permutation-invariant (player ordering doesn't matter) and rotation-invariant
- Could power tactical pattern classification: pressing triggers, defensive shape transitions, counter-attack detection
- Lightweight architecture means feasible without GPU infrastructure

### Relationship to existing phases

- **Phase 11** (pitch control): TGNets could classify game states by pitch control regime
- **Phase 12** (movement analysis): Graph features complement physical metrics
- **Phase 17** (DEFCON): The DEFCON paper also uses Graph Attention Networks — shared infrastructure
- Would require a new `src/analytics/` module for graph construction and model training

### Not immediately actionable

Requires labeled training data (defensive outcomes per tracking sequence). The paper used proprietary German football data with expert labels. Luxury-lakehouse would need to derive labels from events (e.g., possession outcome after defensive sequence) or use manual annotation.

---

## Decision Optimization (Beyond VAEP)

**Status:** Research direction
**Paper:** Rahimian, Van Haaren & Toka, "Beyond action valuation: A deep reinforcement learning framework for optimizing player decisions in soccer"

Extends VAEP (Phase 9) from *valuing what happened* to *optimizing what should happen*. Uses RL to learn team-specific optimal pass selection and success probability surfaces, then compares actual decisions against optimal ones.

### Relevance

- Natural evolution of the Phase 9 VAEP pipeline
- Answers "where *should* the player have passed?" not just "how valuable was the pass?"
- Requires synchronized 25fps tracking + event data (Stats Perform level — commercial)
- Implementation would need CNN policy networks trained on 11-channel game state representations

### Not immediately actionable

Requires commercial-grade tracking data (Belgian Pro League / Stats Perform) — significantly beyond current public datasets. Filed as a long-horizon research direction.

---

## Space Creation Quantification (Fernandez & Bornn 2018)

**Status:** Research direction — deferred from Phase 12
**Paper:** Fernandez & Bornn (2018), "Wide Open Spaces: A statistical technique for measuring space creation in professional soccer"

Full OBSO (Off-Ball Scoring Opportunity) requires computing N+1 pitch control surfaces per frame (one counterfactual surface with each player removed) to measure each player's space creation contribution. At 25fps with 22 players, this is ~2,700 pitch control evaluations per second of play — prohibitively expensive for the current compute budget.

### What was implemented instead

Phase 12 implemented a simpler Off-Ball xT metric: `pitch_control(player_location) x xT(player_zone)`, computed at 1fps sampling. This captures positional value without the counterfactual computation.

### What would be needed

- GPU-accelerated pitch control (vectorized TTI computation across grid)
- ~25x compute budget increase (from 1fps to 25fps full OBSO)
- Differential pitch control: `PC_with_player - PC_without_player` per player per frame

### Dependencies

- Phase 11 (pitch control) — complete
- Phase 12 (off-ball xT) — complete (provides foundation)
- GPU compute infrastructure (not currently available)

---

## Other Ideas (Unscheduled)

- [ ] Voronoi area persistence — pre-compute in dbt (lower priority if Phase 11 replaces Voronoi)
- [ ] Pitch Control animation — frame-by-frame playback in Streamlit
- [ ] Event overlay on Pitch Control — render events on pitch control view
- [ ] Respo.Vision 3D pose tracking — skeletal keypoints from broadcast video (user pursuing via network)
- [ ] Wyscout match metadata — formations, coaches, venue (not in public Figshare dataset)
- [ ] Parallelized Databricks ingestion — fan-out patterns for concurrent provider ingestion

---

*Items graduate from this roadmap into numbered phases in [PLAN.md](PLAN.md) when prerequisites are met and the scope is well-defined.*
