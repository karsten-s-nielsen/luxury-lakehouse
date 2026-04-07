# Adversarial Training & Team-Agnostic Player Embeddings — Investigation Notes

> **Source session**: 2026-03-22, investigation-only (no code changes)
> **Primary source**: Davide Danesi, "The Imposter on the Pitch: Learning Team-Agnostic Signatures for Counterfactual Evaluation" (HPI 2025)
> **Review article**: Alex Marin Felices, The xG Football Club — https://thexgfootballclub.substack.com/p/can-football-models-separate-player
> **Paper PDF**: https://static.hudl.com/craft/performance-insights-research-stage/2025/The-Imposter-on-the-Pitch_-Learning-Team-Agnostic-Signatures-for-Counterfactual-Evaluation-Davide-Danesi.pdf

---

## 1. The Core Problem

A player's observable metrics (pass completion, xG contribution, VAEP) are **not** a pure function of individual ability. They are confounded by:

- Tactical scheme of the team (system)
- Teammate quality (enablers)
- Opposition strength
- Game state (scoreline, time, venue)

**Consequence for scouting**: A player who plays similarly to Rodri at Man City may do so *because* they are in a Pep system, not because they would replicate that output at Wolves. Current embedding systems (including ours) cannot distinguish these cases.

**Consequence for Luxury Lakehouse**: Our current player similarity search answers "who plays similarly to X" — but doesn't control for whether that similarity is player-driven or system-driven. This is the most fundamental unsolved problem in player evaluation.

---

## 2. Danesi's Paper — Full Technical Summary

### Author Background

- **Davide Danesi** — Data Scientist at Bending Spoons (Italian consumer app company, Milan/Brescia)
- Education: Bachelor's + Master's in Mathematics/Data Science, Universita degli Studi di Padova
- Prior: Match and Data Analyst roles, research practicum at UNSW
- LinkedIn: https://www.linkedin.com/in/davide-danesi-900754162/
- Hudl blog entry: https://www.hudl.com/blog/hpi-25-davide-danesi
- **Venue**: Hudl Performance Insights 2025, Research Stage — presented November 14, 2025, Fulham Pier, London
- This appears to be his first public research contribution

### Two Training Objectives

#### Objective 1: Contextual Swap Detection

Inspired by Word2Vec negative sampling. The analogy:

| NLP Concept | Football Analogue |
|---|---|
| Word | Player |
| Context window | Football situation (pass event) |
| Negative samples | Imposter players |

For each training example: the model sees a pass situation + 1 authentic player + **64 negative (imposter) samples**. Hard negatives use **same-role, same-team imposters** to force discovery of individual tendencies beyond positional habits. The task is to rank the authentic player highest.

**Result**: Swap accuracy ~26% vs. ~1.5% random baseline (1/65). 17x above chance — model learned something real, but far from perfect ceiling.

#### Objective 2: Adversarial Purification

Inspired by DANN (Ganin et al., JMLR 2016 — the gradient reversal paper, arXiv:1505.07818):

```
L_total = L_swap - lambda * L_adv_ce
```

Where:
- `L_swap` = contrastive ranking loss from swap detection
- `L_adv_ce` = cross-entropy loss of the team adversary classifier
- `lambda = 0.2` (governs aggressiveness: too low = no purification; too high = erases stylistic signal with the noise)

The main model is penalized when its player embeddings allow the adversary to identify team. The adversary itself is trained normally to maximize team prediction accuracy. Net effect: **gradient reversal on the shared feature space**.

**Result**: Team adversary accuracy ~15% (vs. 3.6% random for 28 clubs). Team information "not fully erased but strongly reduced" — partial but meaningful purification.

### Architecture Details

- **Data**: 1,377 player embeddings, validated on 1.6 million "difficult" pass situations (below 0.7 baseline success probability)
- **League**: Likely Serie A (28 clubs referenced, Italian players dominate examples: Bastoni, Criscito, Rrahmani, Locatelli)

#### Feature Vector: 61 Dimensions

- **37 continuous features**: pass geometry (distance, angle, direction), nearest-opponent distances, event metadata, StatsBomb-derived positional variables
- **24 from categorical embeddings**: pass outcome and pass height each get separate embedding tables, then concatenated with the continuous block

This is manually engineered — does NOT use raw 360 freeze frame coordinates (deferred as future work).

#### Neural Architecture

- **Situation encoder**: 3-layer MLP from 61 inputs -> **256-dimensional** situation representation
- **Player embedding table**: 1,377 players x **128-dimensional** learned embeddings
- **Swap detector head**: concatenates [256 situation + 128 player] -> 384-d -> single logit (binary: authentic vs. imposter)
- **Team adversary head**: takes the 128-d player embedding alone -> predicts one of 28 clubs

### Results

**Embedding quality (UMAP)**:
- Role clustering: Strong (goalkeepers show 0.224 intra-role similarity, highly isolated group)
- Team clustering: Weak (intra-team and cross-team similarity essentially identical at ~0.010)
- UMAP shows visually mixed team labels — adversarial purification working directionally

**Nearest-neighbor analysis**:
- Bastoni <-> Criscito (plausible: both left-sided progressive defenders)
- Rrahmani <-> goalkeeper Federico Ravaglia (implausible)
- Locatelli <-> Sebastian De Maio (implausible)

**Counterfactual rankings (who would complete the most passes in 1.6M difficult situations)**:
- Top players: Daichi Kamada, Tijjani Reijnders, Paredes — technically strong passers
- **Four goalkeepers in the top 20** (David de Gea, Samir Handanovic) — domain leakage
- Score spread: **< 2.7 percentage points** separates top and bottom players
- Context dominates; individual signature contributes marginally

**Author's own assessment**: Outputs "did not fully align with established, real-world player assessments."

### Why the Implementation Fell Short

The author attributes limitations to **architectural simplification**, not conceptual flaws:

1. **Temporal loss**: Static snapshots eliminate movement, timing, and sequential decision-making that define player style
2. **Feature engineering bottleneck**: 61-dimensional manually engineered vector likely misses nonlinear interactions that GNNs would capture automatically
3. **Single-event bias**: Passing behavior requires possessional context, not isolated action analysis

### Proposed (But Not Implemented) Architecture

- Spatiotemporal **graph model** processing full possessions
- **Players as nodes** in a dynamic graph with coordinates, velocities, contextual tags
- **GNN** (e.g., Graph Attention Network) for spatial relationships
- **LSTM** for temporal sequences within possessions
- **StatsBomb 360 freeze frames** as ideal input — every player's position captured at every event
- Modular signatures (passing, shooting, defensive) combinable into multidimensional player identity

---

## 3. Related Work — Full Landscape

### Self-Supervised / Contrastive Player Embeddings

#### RisingBALLER (Adjileye, StatsBomb Conference 2024)

- arXiv: https://arxiv.org/abs/2410.00943
- "A player is a token, a match is a sentence"
- Transformer architecture with Masked Player Prediction (MPP) pre-training
- Dataset: 1,792 matches, 2,600 players, Top 5 European leagues 2015-16 via StatsBomb
- 4 embedding components: player ID, spatial position, temporal stats, team affiliation
- **Key finding**: With team embeddings, players cluster by club; without them, players cluster by profile. This is exactly the same tension Danesi tries to resolve via adversarial purification.
- No adversarial debiasing — team context is either included or ablated, not purged

#### ScoutGPT / EventGPT (Hong, Lee, Jo, So, Bauer, Ko — arXiv Dec 2025)

- arXiv: https://arxiv.org/abs/2512.17266
- Presented at HPI 2025, same event as Danesi
- Player-conditioned GPT transformer over SPADL action sequences
- Enables counterfactual player substitution by replacing player ID token
- 5 Premier League seasons (2020/21-2024/25), ~174,000 episodes, 1,221 players
- Does NOT use adversarial purification — team context is modeled, not removed
- **Closest practical implementation of the downstream use case Danesi targets**
- **Most relevant for Luxury Lakehouse**: We have ~9.5M SPADL actions, VAEP values, and 23-action vocabulary

#### A Foundation Model for Soccer (Baron, Hocevar, Salehe — University of Toronto, 2024)

- arXiv: https://arxiv.org/abs/2407.14558
- GPT-style decoder, 50-d action embeddings, FA WSL 2018-2021
- Team encoded explicitly in action tokens: `<team>, <action_type>, <x_bin>, <y_bin>`
- No team-context removal; player embeddings not the primary output

#### football2vec (Ofir Magdaci, 2021-2022)

- GitHub: https://github.com/ofirmg/football2vec (Apache-2.0)
- Not a peer-reviewed paper — practitioner blog series (Towards Data Science)
- Word2Vec + Doc2Vec via gensim on StatsBomb event data
- Action2Vec (32-d), PlayerMatch2Vec (32-d Doc2Vec), Player2Vec (career average)
- **No adversarial debiasing** — team context retained in embedding
- **This is what Luxury Lakehouse currently implements** (Theiner et al. 2022 variant)

**Danesi vs. football2vec comparison:**

| Dimension | football2vec | Danesi |
|---|---|---|
| Embedding paradigm | Doc2Vec (language analogy) | Contrastive learning (Word2Vec analogy but task-specific) |
| Negative sampling | Standard Doc2Vec | Hard negatives: same-role, same-team imposters |
| Team context | Retained (embedded implicitly) | Adversarially purged via gradient reversal |
| Feature input | Action-level tokenized events | 61-d engineered pass context vector |
| Embedding size | 32-d | 128-d |
| Goal | Stylistic similarity / player comparison | Counterfactual evaluation across team contexts |
| Open source | Yes (GitHub, Apache-2.0) | No code released |

### GNN-Based Spatiotemporal Football Models

#### GoalNet (Jiang, Cai, Kyrillidis — submitted ICML)

- arXiv: https://arxiv.org/abs/2503.09737
- GNN variants (GCN, GAT, Graph Transformer) for xT credit assignment
- Event-centric graphs: 22 player nodes, StatsBomb PL 2015-16 (380 matches)
- Assigns credit proportional to node embedding magnitude
- No team-context removal

#### Player-Team Heterogeneous Interaction Graph Transformer (Wang et al., KDD Aug 2025)

- arXiv: https://arxiv.org/pdf/2507.10626
- Heterogeneous graph with player and team nodes, transformer attention

#### Learning Spatial Formations via GNNs (Swiatek, Pilka, Gorecki — Research Square Dec 2025)

- https://www.researchsquare.com/article/rs-7743155/v1
- Graph autoencoder with EdgeConv layers, contrastive objective
- Polish Ekstraklasa tracking data; clusters tactical formations

### StatsBomb 360 Freeze Frame-Based ML

#### Clustering Football Game Situations (StatsBomb Conference 2023)

- PDF: https://blogarchive.statsbomb.com/uploads/2023/10/Clustering-Football-Game-Situations-via-Deep-Representation-Learning.pdf
- Multi-task auto-encoder on 360 freeze frames: soft Voronoi, pass success prediction, next-action prediction
- Spatial encoding: 64x96 grid with 4 feature maps (actor, offense, defense, goalkeeper)

#### xPass 360 (Hudl StatsBomb)

- https://statsbomb.com/what-we-do/soccer-data/360-2/
- GNNs and CNNs to encode 360 freeze frames for pass probability estimation

### Counterfactual / Context-Adjusted Player Evaluation

#### Fine-tuned Large Event Models (Mendes-Neves et al., arXiv Feb 2024)

- https://arxiv.org/abs/2402.06815
- Fine-tunes Large Event Models on WyScout PL 2017-18
- Enables hypothetical transfer simulation by modeling players across different team contexts

#### EPI over Time (Tureen & Chen, HPI 2025)

- https://static.hudl.com/craft/performance-insights-research-stage/2025/EPI-over-Time-in-English-Football-Tureen-Chen.pdf
- GLMMs (Generalized Linear Mixed Models) to isolate player-level effects from team context
- Frequentist statistical approach to the same core problem

#### Goals Above Expectation and Beyond (Bajons & Kook, arXiv Sept 2025)

- Formal statistical framework for context-adjusted player evaluation

### Foundational ML Techniques Referenced

#### DANN — Domain-Adversarial Training (Ganin et al., JMLR 2016)

- arXiv: https://arxiv.org/abs/1505.07818
- Introduced the gradient reversal layer (GRL)
- GRL multiplies gradients by -lambda during backpropagation
- Feature extractor receives conflicting signals: improve main task, confound domain classifier
- Result: domain-invariant features useful for the main task
- **Danesi's adaptation**: team = domain, player signature = domain-invariant feature, pass outcome = main task
- Reference implementation: https://github.com/pumpikano/tf-dann

#### Deep Sets (Zaheer et al. 2017)

- Already in production in Luxury Lakehouse (`set_encoder.py` for xG v2)
- Set function architecture for variable-size inputs (freeze frame player sets)

---

## 4. Luxury Lakehouse — Current State vs. Danesi's Framework

### What We Have

| Capability | Luxury Lakehouse | Danesi |
|---|---|---|
| Player embeddings | Doc2Vec 32-d behavioral + 13-d z-score stat | 128-d contrastive MLP |
| Team-context removal | **None** | Adversarial gradient reversal (lambda=0.2) |
| Negative sampling | Standard Doc2Vec context window | Hard negatives (same-role, same-team) |
| Spatial context | 12x8 grid tokenization (on-ball only) | 61-d engineered features (single event) |
| 360 freeze frames | 15.58M rows, 323 matches — xG set encoder + DEFCON, **not** embeddings | Proposed but not implemented (GNN future) |
| Tracking data | 38.1M frames (20 matches: Metrica/IDSSE/SkillCorner) | Not used |
| Counterfactual evaluation | PAUSA/OBSO space creation (7 IDSSE matches) | 1.6M difficult-pass scenarios |
| Action vocabulary | 12-13 types (source-specific tokenization) | Not applicable (feature vector) |
| Cross-source resolution | 11,918 unified players, 2,388 xrefs | Single-source only |
| Similarity search | pgvector HNSW (4 indexes), cosine distance | UMAP visualization only |
| SPADL actions | ~9.5M actions with VAEP values | Not used |
| Predictive models | xG (logistic + XGBoost + Deep Sets), xT, VAEP, pitch control, OBSO | Pass success only |

### The Critical Gap

We have **zero mechanism to disentangle player skill from team/system context**. Our embeddings encode "what this player does" including "what this player's manager asks them to do" and "what this player's teammates enable them to do."

### Our Advantages Over Danesi

1. **Multi-source data with entity resolution** — players appearing in both StatsBomb and Wyscout provide natural cross-team validation (different league contexts, same player)
2. **Tracking data with pitch control** — richer spatial counterfactuals than event-only data
3. **SPADL + VAEP** — action-level reward signal for contrastive training (23-type vocabulary vs. our current 12-type tokenizer)
4. **360 freeze frames + Deep Sets** — proven spatial encoding architecture already in production
5. **HF Jobs GPU infrastructure** — no compute constraint for training

### Current Embedding Architecture Details

**Behavioral vector (32-d Doc2Vec)**:
- Algorithm: Theiner et al. (2022) Football2Vec via gensim Doc2Vec (Le & Mikolov 2014, DM mode)
- Training config: `vector_size=32`, `window=5`, `min_count=2`, `epochs=20`, `dm=1`, `workers=1` (determinism)
- Tokenizer: 12x8 spatial grid (96 cells) over 120x80 pitch. Each event -> `"{action_type}_{grid_x}_{grid_y}"`
- Action types (StatsBomb): pass, cross, corner, throw_in, carry, shot, duel, interception, foul, clearance, take_on, goalkeeper, other
- Action types (Wyscout): pass, cross, shot, duel, foul, goalkeeper, interception, take_on, throw_in, clearance, free_kick, other
- Inference: `infer_vector` on per-player-match token sequence
- Model artifacts: UC Volume `/Volumes/soccer_analytics/dev_gold/model_weights/football2vec/`
- HF Hub: `luxury-lakehouse/football2vec-statsbomb-wyscout`

**Stat vector (13-d z-score)**:
- Features: `goals_per_90`, `xg_per_90`, `passes_per_90`, `pass_completion_pct`, `progressive_passes_per_90`, `line_breaking_per_90`, `vaep_per_90`, `offensive_vaep_per_90`, `defensive_vaep_per_90`, `defcon_per_90`, `intercept_per_90`, `deter_per_90`, `xg_overperformance`
- Normalization: population z-score (`ddof=0`), **global** across all players (not position-group scoped)
- Grain mismatch: stat vector is per competition-season, joined to per-match behavioral keys. All matches in same season share identical stat vector.

**Aggregation (dbt)**:
- `fct_player_embeddings`: per player-match (raw)
- `fct_player_embeddings_season`: per player-competition-season (element-wise mean)
- `fct_player_embeddings_career`: per player (element-wise mean across career)

**Similarity search**:
- 4 pgvector HNSW indexes (behavioral + stat, season + career)
- Cosine distance (`<=>` operator)
- Distance thresholds: <0.20 Very Similar, <0.35 Similar, <0.50 Moderately Similar, >=0.50 Different
- Scale: ~87,035 per-match embeddings, ~8,950 players

### Data Assets Available for Future Work

**StatsBomb 360 freeze frames** (15.58M rows, 323 matches):
- Schema: `freeze_frame_id`, `event_uuid`, `match_id`, `is_teammate`, `is_actor`, `is_keeper`, `location_x`, `location_y`, `visible_area_vertices`
- **Critical limitation**: No `player_id` — only role flags (teammate/actor/keeper). Anonymous positional snapshots.
- Already used by: xG set encoder (Deep Sets), DEFCON-lite (attacker perspective)

**Tracking data** (38.1M frames, 20 matches):
- Metrica: 3 matches, 25fps, [0,1] normalized coords
- IDSSE (Bundesliga/DFL): 7 matches, 25fps, center-origin meters, ELASTIC-synced to events
- SkillCorner (A-League): 10 matches, 10fps (broadcast video), tracking only (no events)
- Gold table: `fct_tracking_frames` with speed, velocity, acceleration, distance_to_ball
- **Player identity available** in tracking (DFL person IDs) but NOT yet linked to `dim_players.canonical_player_id`

**SPADL actions** (~9.5M rows):
- 23 SPADL action types with start/end coords (105x68m), body part, result
- VAEP values per action (offensive, defensive, net)
- Sources: StatsBomb + Wyscout

**Player aggregations**:
- `fct_player_stats`: 19,154 rows (player x competition x season), 13 stat features
- `fct_physical_stats`: 616 rows (player x match, tracking-only), distance/speed/sprint/accel
- `fct_pass_timing`: ~600 rows (PAUSA-enabled matches), temporal/spatial judgment scores
- `dim_players`: 11,918 unified players with cross-source IDs

**Predictive model outputs**:
- xG: 87,999 shots with logistic, XGBoost, and Deep Sets predictions + confidence intervals
- xT: data-driven grid in Delta `expected_threat_grids`
- VAEP: baked into `fct_action_values`
- OBSO: pitch control x transition x EPV surface (7 IDSSE matches)
- PAUSA: temporal_judgment x spatial_selection decomposition (7 IDSSE matches)

---

## 5. Complementary Metrics for Future Roadmap

| Metric | What It Captures | Data Requirements | Current Status |
|---|---|---|---|
| **Context-adjusted VAEP** | Player's VAEP controlling for teammate/opponent quality | VAEP + team strength proxy | Partially (VAEP yes, context adjustment no) |
| **Passing style signature** | Direction/distance/risk distribution, independent of team system | Pass events + adversarial debiasing | No (raw pass data yes, debiasing no) |
| **Positional versatility** | How much a player's spatial footprint varies across matches | Per-match grid heatmaps | Yes (behavioral vectors encode this implicitly) |
| **Space creation value** | OBSO delta when player is removed from pitch control | Tracking + pitch control + OBSO | Yes, 7 matches (PAUSA) |
| **Decision quality under pressure** | Does the player choose optimal actions given spatial context? | 360 frames or tracking + action outcomes | Partially (PAUSA temporal_judgment) |
| **Role-normalized output** | Player's stats z-scored within position group, not global population | `fct_player_stats` + `position_group` | No (current z-score is global) |
| **Counterfactual pitch control substitution** | What changes about pitch control surface when swapping Player A for Player B | Player-identified tracking data | Infrastructure exists (pitch control), data limited (20 matches, no player-canonical ID bridge for tracking) |
| **Sequential action style** | Possession-level behavioral patterns (not single-event) | SPADL action sequences | Data exists (~9.5M actions), model does not |

---

## 6. Concrete Roadmap Items

### Near-Term (Low Effort, High Insight)

#### Position-Group Z-Scoring

- **What**: Normalize stat vectors within `position_group` (Goalkeeper, Defender, Midfielder, Forward) instead of globally
- **Why**: Eliminates the goalkeeper-as-best-passer problem Danesi encountered. A goalkeeper's passing z-score should be relative to other goalkeepers, not all players.
- **Where**: `src/ingestion/player_embeddings.py`, function `_compute_stat_vectors()`
- **Effort**: Trivial — add a groupby on `position_group` before z-score normalization
- **Impact**: Immediately improves similarity search quality for cross-position comparisons

#### SPADL Vocabulary Upgrade

- **What**: Replace the 12-13 type action tokenizer in `football2vec.py` with the 23-type SPADL taxonomy already in `fct_action_values`
- **Why**: Richer behavioral tokens at zero additional data cost. Distinguishes `tackle` from `interception`, `corner_short` from `corner_crossed`, `freekick_short` from `freekick_crossed`
- **Where**: `src/analytics/football2vec.py`, tokenizer section
- **Effort**: Low — SPADL data already exists, just need to rewire tokenization source
- **Impact**: More expressive behavioral embeddings, especially for defensive players

### Medium-Term (Moderate Effort)

#### Adversarial Team Debiasing

- **What**: Migrate from gensim Doc2Vec to a trainable PyTorch embedding model with gradient reversal layer
- **Why**: Produce embeddings that answer "who plays like X regardless of system" instead of "who plays in a similar system"
- **Architecture**:
  - Keep existing tokenization and data pipeline
  - Replace Doc2Vec with a learnable embedding table + MLP encoder
  - Add team adversary head with gradient reversal (lambda=0.2, tunable)
  - Loss: `L_total = L_contrastive - lambda * L_team_classification`
  - Hard negative mining: same `position_group`, same `team_id`
- **Training**: HF Jobs GPU (A10G)
- **Validation**: Cross-source entity resolution provides natural test — players appearing in both StatsBomb and Wyscout should have close embeddings despite different league contexts
- **Publish**: Debiased embeddings to HF Hub alongside current ones (dual-track, not replacement)
- **Effort**: Moderate — requires PyTorch model, training pipeline, evaluation framework
- **Impact**: High — fundamentally changes what similarity search means

#### 360-Enriched Situational Context

- **What**: Extend `set_encoder.py`'s Deep Sets architecture to produce a situational context vector that augments behavioral embeddings
- **Why**: For the 323 matches with 360 data, encode the spatial relationship structure around each event (who's nearby, pressing intensity, passing lanes) as additional embedding input
- **Architecture**: Deep Sets encoder (already proven for xG) -> 16-d or 32-d context vector -> concatenated with action token embedding before Doc2Vec/MLP encoding
- **Constraint**: 360 frames are anonymous (no player_id). Can encode spatial relationships but not player-specific graph nodes.
- **Effort**: Moderate — architecture exists, need to wire it into embedding pipeline
- **Impact**: Medium — only applies to 323 StatsBomb 360 matches, but tests the concept

### Long-Term (Significant Effort, Own-Footage Dependent)

#### Counterfactual Pitch Control Substitution

- **What**: With player-identified tracking from Veo3 footage, run "swap Player A for Player B" simulations across full matches using existing pitch control model
- **Why**: Answers "what would happen to our team's spatial control if we signed Player X?" — the ultimate scouting question
- **Prerequisites**:
  - Own-footage pipeline complete (Veo3 -> Metrica GameCloud -> EPTS)
  - Tracking player IDs bridged to `dim_players.canonical_player_id`
  - Sufficient match volume (target: 50+ matches for statistical reliability)
- **Architecture**: Existing `pitch_control.py` (Spearman 2017) already supports arbitrary player positions/velocities. Replace one player's trajectory with another's (velocity-adjusted for position) and recompute surfaces.
- **Effort**: High — dependent on own-footage pipeline, player ID resolution, trajectory normalization
- **Impact**: Very high — unique capability, not available in any known open-source or commercial platform

#### Sequence Model (ScoutGPT-Inspired)

- **What**: Player-conditioned transformer over SPADL action sequences for counterfactual evaluation
- **Why**: Danesi identified single-event features as the primary limitation. Sequential context (full possessions) captures timing, decision-making, and multi-step buildup that define style.
- **Architecture** (based on Hong et al. 2025):
  - SPADL action sequences as input tokens (we have ~9.5M actions)
  - Player ID as conditioning token (enables counterfactual substitution by swapping ID)
  - VAEP as reward signal for action quality assessment
  - Autoregressive prediction of next action given player + context
- **Training**: HF Jobs GPU, likely multi-GPU (A10G-large or better)
- **Prerequisites**: SPADL vocabulary upgrade (near-term item above), entity resolution for unified player IDs across sources
- **Effort**: Very high — novel model architecture, significant training infrastructure
- **Impact**: Very high — production-grade counterfactual player evaluation

#### GNN Over Tracking + 360 Data

- **What**: Spatiotemporal graph neural network where players are nodes, with coordinates/velocities as node features and spatial relationships as edges
- **Why**: This is the architecture Danesi proposed but could not implement. It is the theoretically optimal approach for capturing relational player behavior.
- **Architecture**:
  - Graph Attention Network (GAT) layers for spatial relationships
  - LSTM/Transformer for temporal sequences within possessions
  - Adversarial team purification on graph embeddings
  - 360 freeze frames (323 matches, anonymous) + tracking data (20 matches, player-identified) as heterogeneous input
- **Prerequisites**: Substantially more tracking data (target: 500+ matches per DEFCON Tier 4 requirements), player ID bridge for all tracking sources
- **Effort**: Very high — novel architecture, large data requirement
- **Impact**: Potentially transformational — represents the frontier of the field

---

## 7. License Audit — All Referenced Work

Every technique and dataset referenced in this investigation has been verified for licensing compatibility with Luxury Lakehouse.

| Work | Type | License | Verdict |
|---|---|---|---|
| Danesi "Imposter on the Pitch" (HPI 2025) | Published conference paper, no code | Ideas freely implementable | GREEN |
| DANN / Gradient Reversal (Ganin et al. 2016) | JMLR open-access paper | Foundational ML, freely usable | GREEN |
| football2vec (Magdaci) | GitHub repo | Apache-2.0 | GREEN |
| ScoutGPT (Hong et al. 2025) | arXiv preprint, no code | Ideas freely implementable | GREEN |
| RisingBALLER (Adjileye 2024) | arXiv preprint, no code | Ideas freely implementable | GREEN |
| GoalNet (Jiang et al.) | arXiv preprint | Ideas freely implementable | GREEN |
| Deep Sets (Zaheer et al. 2017) | Published paper | Already in production (`set_encoder.py`) | GREEN |
| Doc2Vec / gensim | Library | LGPL-2.1 (training-time only, not shipped) | GREEN |
| silly-kicks / VAEP | GitHub repo | MIT | GREEN |
| Word2Vec negative sampling | Foundational NLP | Google patent US 9,740,680 **expired 2023** | GREEN |
| Spearman 2017 pitch control | Published paper | Already in production | GREEN |
| PAUSA / OBSO (Lee et al. SSAC26) | GitHub repo | Apache-2.0 (secured March 2026) | GREEN |
| ELASTIC (Kim et al. 2025) | GitHub repo | Apache-2.0 (secured March 2026) | GREEN |
| StatsBomb open data | Dataset | CC-BY 4.0 — derived work permitted with attribution | GREEN |
| Wyscout open data (Pappalardo et al.) | Dataset on figshare | CC-BY 4.0 | GREEN |

**No licensing blockers exist for any roadmap item.** The only obligation is attribution, which aligns with existing `NOTICE.md` academic citation standards.

---

## 8. Key Academic References (for NOTICE.md when implemented)

- Ganin, Y., Ustinova, E., Ajakan, H., Germain, P., Larochelle, H., Laviolette, F., Marchand, M. & Lempitsky, V. (2016). Domain-Adversarial Training of Neural Networks. *JMLR*, 17(59), 1-35.
- Danesi, D. (2025). The Imposter on the Pitch: Learning Team-Agnostic Signatures for Counterfactual Evaluation. *Hudl Performance Insights 2025*.
- Hong, J., Lee, S., Jo, J., So, D., Bauer, S. & Ko, S. (2025). ScoutGPT: Player-conditioned Football Language Model for Counterfactual Evaluation. *arXiv:2512.17266*.
- Adjileye, A.A. (2024). RisingBALLER: A player is a token, a match is a sentence, football is a language. *arXiv:2410.00943*.
- Jiang, X., Cai, D. & Kyrillidis, A. (2025). GoalNet: Towards More Nuanced xT Credit Assignment. *arXiv:2503.09737*.
- Mendes-Neves, T., Meireles, L. & Mendes-Moreira, J. (2024). Estimating Player Performance in Different Contexts Using Fine-tuned Large Event Models. *arXiv:2402.06815*.
- Theiner, J., Gritz, W., Falk, C. & Memmert, D. (2022). football2vec. *ECML PKDD Workshop on Machine Learning and Data Mining for Sports Analytics*.
- Le, Q. & Mikolov, T. (2014). Distributed Representations of Sentences and Documents. *ICML*.
- Zaheer, M., Kottur, S., Ravanbakhsh, S., Poczos, B., Salakhutdinov, R. & Smola, A. (2017). Deep Sets. *NeurIPS*.
- Spearman, W. (2017). Beyond Expected Goals. *MIT Sloan Sports Analytics Conference*.
- Van Haaren, J. (2025). Soccer Analytics Review 2025. https://janvanhaaren.be/posts/soccer-analytics-review-2025/index.html

---

## 9. Summary: Why This Matters for Luxury Lakehouse

The most valuable takeaway from Danesi's work isn't any single technique — it's the **framing**. Player embeddings that don't account for team context are fundamentally answering the wrong question for scouting.

Our current system tells you "who plays similarly to X." A team-agnostic system would tell you "who would play similarly to X *if placed in any system*." That's the difference between descriptive analytics and predictive scouting.

We are better positioned than Danesi to solve this:

1. **Multi-source data with entity resolution** provides natural cross-team validation
2. **Tracking data with pitch control** enables richer spatial counterfactuals
3. **SPADL + VAEP** gives action-level reward signals for contrastive training
4. **360 freeze frames + Deep Sets** is a proven spatial encoding already in production
5. **HF Jobs GPU infrastructure** removes the compute constraint that limited Danesi
6. **Own-footage pipeline** (in progress) will provide player-identified tracking at scale

The near-term items (position-group z-scoring, SPADL vocabulary upgrade) are low-hanging fruit that improve embedding quality immediately. The medium-term adversarial debiasing is the conceptual unlock. The long-term sequence model and GNN work represent the frontier — and our data pipeline is being built to support exactly that future.
