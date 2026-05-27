# External Research Tracking

> **Purpose:** durable record of external research sources (specific pre-publication papers, academic labs, conferences, and actively-developed libraries) that the lakehouse is monitoring for potentially actionable upgrades or integrations. Reviewed quarterly; promoted to `TODO.md` / `ROADMAP.md` / an ADR when trigger conditions fire.
>
> **Source session:** 2026-04-24. Added when the [LISS Football Analytics Symposium](https://www.kuleuven.be/liss/events/liss-football-analytics-symposium) surfaced pre-publication ExT poster work (Salimi / Salmankhah 2026) and highlighted the DTAI Sports Analytics Lab / MLSA conference as a systematic research stream worth following. The lakehouse already had a LinkedIn-post → TODO pattern (D60-D63 from Abu Kwaider, D64 from the ml-intern release) but no durable mechanism for tracking *academic* research streams over time.

---

## How to use this file

- **Add a tracker** when: (a) a paper or project is pre-publication and you want to catch the preprint/code; (b) an academic group publishes at a cadence worth following; (c) a conference proceedings stream is worth scanning annually; (d) an actively-developed library's releases could change an integration decision.
- **Don't add** when: the work is already public and either incorporated or rejected (historical references already in `ARCHITECTURE.md` Appendix D); a one-shot interesting post can go directly to TODO/ROADMAP without ongoing tracking.
- **Quarterly review cadence**: first week of January / April / July / October. For each active tracker, check the mechanism, update "Last reviewed," promote anything new, archive anything that went stale.
- **Promotion path**: tracker → TODO item (if scoped and scheduled) OR ROADMAP item (if research direction) OR ADR (if an architectural decision follows). Once promoted, move the entry to the Promotion log below.
- **Keep mechanisms low-toolchain**: GitHub watches, Google Scholar alerts, arXiv alerts, and calendar reminders are sufficient. Resist automating polls of a dozen sources — signal volume is low and the quarterly review is the right cost/signal curve.

---

## Active trackers

### T1 — ExT: Expected Threat with higher resolution + contextual features

- **Authors:** M. Sadegh Salimi (lead, MS thesis, Sharif University of Technology), Amirparsa Salmankhah (MSc CS@PoliMi), Ali Nodin, with supervision from Prof. Mohammad Hossein Rohban (Sharif) and Shiva Kamkar.
- **Surfaced:** LinkedIn post by Salmankhah, 2026-04-21 (poster at [LISS Football Analytics Symposium](https://www.kuleuven.be/liss/events/liss-football-analytics-symposium) 2026-04-23).
- **What it is:** "ExT: Improving the Computational Efficiency and Spatial Granularity of the Expected Threat Model" — per-source-cell conditional xT surface at ~24×16 grid resolution (vs Singh-2018's 12×8 or 16×12), with contextual features beyond spatial-only inputs. Evidence: the poster figure shows a dark-theme heatmap labeled `Sup X / Sup Y` source markers with a colorbar on the standard xT scale (0-0.23), clearly finer grid than any published xT implementation. Post text claims previous contextual xT formulations were "not feasible" at this resolution until their efficiency trick.
- **Why the lakehouse cares:** xT is load-bearing in `wf-xt-grids`, `fct_action_values`, every action-value surface, and the D60-D63 pass-decision suite. If ExT delivers a working conditional + fine-grid formulation with open code, it's a qualitative xT v2 upgrade (not a drop-in), with downstream effects on D60 EPV formula, U4 uncertainty bounds, and U6 three-axis VAEP/xT framing.
- **Methodology (per author response on LinkedIn 2026-04-25, cited as channel):**
  - **Sparsity:** Addressed via Kernel Density Estimation smoothing; authors state explicitly that KDE + KNN does not fully eliminate the curse of dimensionality.
  - **Storage:** Transition matrix replaced by K-nearest-neighbor lookup in (source, context) feature space — converts the cost of adding a contextual feature from multiplicative on tensor storage to linear on KNN dimensionality. This is the efficiency claim implicit in the poster.
  - **Features:** Not limited to source position; named examples include position of the last defender and count of opposition between ball and opponents' goal — both derivable from existing lakehouse tracking sources. Feature set still under experimentation.
  - **Release:** No preprint or code-release date stated.
- **Mechanism:**
  - GitHub watch (Releases + new repos) on [SMSadegh19](https://github.com/SMSadegh19) and [Amirparsa-Sal](https://github.com/Amirparsa-Sal).
  - Google Scholar alert on [Salmankhah's profile](https://scholar.google.com/citations?user=kYj5p-oAAAAJ&hl=en).
- **Last reviewed:** 2026-04-25.
- **Next review:** 2026-07-24 (or earlier if either watch fires).
- **Status:** Active, paper pre-publication.

### T2 — DTAI Sports Analytics Lab (KU Leuven)

- **Group:** Prof. Jesse Davis's lab at KU Leuven DTAI, co-organizers of the LISS symposium. Co-authors of the original VAEP paper that anchors the lakehouse's action-value stack. Publishing consistently on soccer analytics (off-ball defensive impact, pose-tracked dribbling, game-context style, VAEP quality methodology, xG calibration).
- **Why the lakehouse cares:** Highest-signal academic source for this problem domain. Recent outputs already map onto several items in the TODO/ROADMAP: defensive impact via cover shadows → Defensive Impact page; 3D pose tracking dribbling → Visual Exploratory Behavior ROADMAP item; game context on playing style → PA1 Game State Segmentation; VAEP metric-quality → U4/U6 UX items; xG Monte Carlo → xG calibration posture. Tracking them in aggregate is higher signal-per-effort than tracking individual lines of research.
- **Mechanism:**
  - Google Scholar alert on Jesse Davis, KU Leuven.
  - Manual review of [dtai.cs.kuleuven.be/sports](https://dtai.cs.kuleuven.be/sports/) publications + blog list at quarterly cadence.
  - Blog posts (*How Do We Know a Metric Is Good?*, *Expected Goals and the Monte Carlo Trap*) are often more immediately actionable than the papers themselves for UX decisions on VAEP/xG presentation.
- **Last reviewed:** 2026-04-24 (initial; most recent outputs captured as of quarterly-review baseline: Cover Shadows 2025-11, Dribble 3D-pose 2025-09, Playing Style 2025-09, VAEP Methodology 2025-09, xG Monte Carlo 2025-09).
- **Next review:** 2026-07-24.
- **Status:** Active.

### T3 — MLSA Conference (Machine Learning and Sports Analytics)

- **Venue:** Annual ECML/PKDD workshop, typically September. Proceedings posted at `dtai.cs.kuleuven.be/events/MLSA2X/papers/` (DTAI hosts). ~20-40 papers per year across all sports.
- **Why the lakehouse cares:** Soccer-relevant MLSA papers in 2025 included the DTAI dribble/style pieces above; historically MLSA has been where the next year's VAEP-style methodology gets trialed. Single annual hour of skim-titles-and-abstracts is enough to catch anything relevant.
- **Mechanism:**
  - Calendar reminder "Check MLSA26 papers" for 2026-10-01.
  - Skim titles/abstracts at the MLSA26 proceedings page when it lands.
- **Last reviewed:** 2026-04-24 (initial; MLSA25 papers captured via T2 DTAI review).
- **Next review:** 2026-10-01 (single annual review; quarterly review can confirm calendar item still set).
- **Status:** Active.

### T4 — UnravelSports ecosystem: CDF standard, fast-forward, unravelsports library (Joris Bekkers)

- **What it is:** Three related projects from Joris Bekkers / UnravelSports:
  1. **Common Data Format (CDF)** — a JSON/JSONL schema specification for standardizing football match data delivery across providers. Covers 6 data types: match sheet, meta, event, tracking, skeletal (body pose), and video. Coordinate system: origin at pitch center, meters, x ∈ [-pitch_length/2, +pitch_length/2], y ∈ [-pitch_width/2, +pitch_width/2]. Paper: Anzer, Arnsmeyer, Bauer, Bekkers, Brefeld, Davis, Evans, Kempe, Robertson, Smith, Van Haaren (2025) — [arXiv:2505.15820](https://arxiv.org/abs/2505.15820). Pursuing **IEEE Standard 3715**. Validator on PyPI (`common-data-format-validator`, v0.2.3 alpha). Author consortium: RB Leipzig, DFB, FIFA, KU Leuven/DTAI (Jesse Davis, Jan Van Haaren — original VAEP co-authors, see T2), U.S. Soccer, UnravelSports/PySport.
  2. **fast-forward** — Rust-powered tracking data loader with Python bindings (Polars DataFrames). 11 providers: CDF, GradientSports, HawkEye, OptaVision, RespoVision, SecondSpectrum, Signality, SkillCorner, Sportec, StatsPerform, Tracab. 3 layouts (long/long_ball/wide), 6 orientation modes, 14 named coordinate systems (6 unique, CDF as pivot). Python 3.11+ only, beta, closed-source Rust backend. PyPI: `fast-forward-football`.
  3. **unravelsports** — Python library for soccer analytics (EFPI, visualization). Python 3.11+, which does NOT fit the lakehouse's Python 3.10 lock (Databricks serverless constraint). The lakehouse reimplemented EFPI in-house to avoid this dependency.
- **Overlap assessment (2026-05-26):**
  - **CDF vs lakehouse**: CDF is a data *delivery* specification; the lakehouse is data *analysis* infrastructure. They operate at different layers. CDF events are raw provider events with standardized field names — not SPADL, not action-converted. The CDF paper explicitly acknowledges SPADL (§4) as complementary. CDF's coordinate system (center-origin, meters) differs from silly-kicks' post-SPADL convention (bottom-left origin, meters) but is trivially convertible. CDF's `play_direction` metadata is a cleaner approach than the lakehouse's empirical direction-of-play inference (ADR-022), but depends on providers delivering it.
  - **fast-forward vs lakehouse ingestion**: Same problem (per-provider tracking parsing), different environment. fast-forward outputs Polars DataFrames locally; the lakehouse ingests to Spark/Delta on Databricks serverless. Neither can replace the other. 3 shared providers (SkillCorner, Sportec, GradientSports); fast-forward covers 8 additional providers the lakehouse doesn't touch.
  - **No duplication with silly-kicks**: CDF/fast-forward have zero SPADL, VAEP, xT, or action-value computation. silly-kicks owns that entire layer unchallenged.
  - **Natural integration point (future)**: silly-kicks could accept CDF events as an input format alongside StatsBomb/Wyscout/etc. This is a silly-kicks change, not a lakehouse change. Low priority since no provider currently delivers in CDF format natively.
- **Why the lakehouse cares:**
  - If CDF achieves IEEE 3715 ratification AND providers adopt it, the lakehouse would benefit from adding a CDF ingestion path (a new data source, not a retrofit of existing ones). silly-kicks adding a CDF converter would be the most natural first step.
  - fast-forward's Python 3.11+ requirement and closed-source Rust backend make it unsuitable as a lakehouse dependency. Watching only.
  - unravelsports Python 3.10 compatibility: if `python_requires` drops to `>=3.10`, trigger a "retire in-house EFPI reimplementation" TODO item.
- **Mechanism:**
  - GitHub watch (Releases) on [UnravelSports/fast-forward](https://github.com/UnravelSports/fast-forward) and [UnravelSports/common-data-format-validator](https://github.com/UnravelSports/common-data-format-validator).
  - GitHub watch (Releases) on the unravelsports repo for Python version changes.
  - Google Scholar alert on "Common Data Format football soccer" for IEEE 3715 ratification / provider adoption announcements.
  - Check CDF validator changelog at quarterly review for schema stability signals (breaking changes = still immature).
- **Last reviewed:** 2026-05-26 (expanded from EFPI-only to full ecosystem).
- **Next review:** 2026-07-24.
- **Status:** Active, passive watch. CDF schema still alpha (v0.2.3, 4 breaking revisions in 14 months). IEEE 3715 not ratified. No provider delivering in CDF format natively. kloppy CDF support mentioned on cdf.football but not landed in kloppy's repo. No near-term action for lakehouse or silly-kicks.

### T5 — Vidal-Codina et al. (2022): Automatic event detection from tracking data

- **Paper:** Vidal-Codina F, Evans N, El Fakir B (2022) "Automatic event detection in football using tracking data." Sports Engineering 25:18. DOI: [10.1007/s12283-022-00381-6](https://doi.org/10.1007/s12283-022-00381-6).
- **Surfaced:** Mills et al. (2026) "Automatic event detection in association football using broadcast-derived tracking data" (Sports Engineering, DOI: 10.1007/s12283-026-00549-4) — FIFA-funded study evaluating broadcast auto-eventing against TRACAB Gen5 + FIFA DCU ground truth. Vidal-Codina 2022 is the algorithm used.
- **What it is:** A deterministic, rules-based algorithm that takes player x,y + ball x,y,z + ball status (live/dead) at 25 Hz and detects match events (set pieces, passes, shots, goals, saves) without human intervention. Uses the FIFA Football Language event definitions. Key mechanics: tunable possession-zone radius `pz`, set-piece detection via ball/player locations relative to pitch geometry, pass detection via possession changes + trajectory.
- **Why the lakehouse cares:** Relevant to the own-footage pipeline (`project_own_footage_tracking.md`). If the lakehouse ever needs to generate events from raw tracking data without a commercial event data provider (e.g., own video → tracking → auto-events → SPADL), this algorithm is the starting point. Currently our architecture relies on provider event data (StatsBomb, DFL/IDSSE, Wyscout), so this is a "watch for when we need it" tracker, not an immediate action. Also: the Mills 2026 evaluation quantifies broadcast ball tracking accuracy at RMSE 3.5–16.2m — relevant context for SkillCorner data quality caveats.
- **Mechanism:**
  - Google Scholar alert on Vidal-Codina F, "automatic event detection football."
  - Check for open-source implementations or follow-up papers at quarterly review.
- **Last reviewed:** 2026-05-13 (initial).
- **Next review:** 2026-07-24.
- **Status:** Active, passive watch (no expected near-term action; own-footage pipeline trigger).

### T6 — Kognia Sports Intelligence: diffusion-based trajectory completion with uncertainty (Capellera et al.)

- **Group:** Guillem Capellera, Antonio Rubio, Luis Ferraz, Antonio Agudo — Institut de Robòtica i Informàtica Industrial (CSIC-UPC) + Kognia Sports Intelligence, Barcelona. Publishing consistently at top CV/ML venues on sports trajectory modeling.
- **Surfaced:** LinkedIn post by Capellera, 2026-05-18 (TPAMI acceptance announcement).
- **What it is:** A research stream producing diffusion-based models for multi-agent trajectory tasks in sports:
  - **TranSPORTmer** (ACCV 2024) — holistic trajectory understanding in multi-agent sports.
  - **U2Diff** (CVPR 2025) — uncertainty-aware diffusion for trajectory completion (~4× faster variant).
  - **U2Diffine** (IEEE TPAMI 2026, [arXiv:2605.10717](https://arxiv.org/abs/2605.10717)) — full model with heteroscedastic uncertainty via Taylor-propagated covariance through the reverse diffusion process. Evaluated on Soccer-U (SoccerTrack, 22 players + ball, broadcast tracking): minSADE₂₀ = 50.65 px (49% improvement over UniTraj), AccRate >95% at 95% confidence. RankNN post-processor ranks generated modes by error probability (Spearman ρ = 0.78 with true SADE on Soccer-U). Two variants: U2Diffine (calibrated uncertainty, ~31-59ms/mode) and U2Diff (comparable displacement error, ~8-14ms/mode).
  - **JointDiff** (ICLR 2026, [project page](https://guillem-cf.github.io/JointDiff/)) — joint generation of continuous trajectories + discrete possession events; introduces CrossGuid conditioning for multi-agent domains.
- **Why the lakehouse cares:** Trajectory completion (imputing missing player/ball positions in tracking data) is an unsolved gap in the lakehouse. SkillCorner broadcast tracking has inherent position gaps; the planned own-footage pipeline will face the same class of missing data. U2Diffine's calibrated uncertainty ellipses could propagate through pitch control → OBSO → PAUSA, giving calibrated confidence on the entire downstream chain. JointDiff's joint trajectory + event generation could enable counterfactual analysis. No pre-trained soccer model released yet — would require training on our data.
- **Mechanism:**
  - Google Scholar alert on Guillem Capellera.
  - GitHub watch on [guillem-cf](https://github.com/guillem-cf) (code releases for U2Diffine, JointDiff, FootBots).
- **Last reviewed:** 2026-05-23 (initial).
- **Next review:** 2026-07-24.
- **Status:** Active, passive watch (no near-term action; own-footage pipeline trigger).

---

## Promotion log

Trackers that produced an artifact converted to TODO / ROADMAP / ADR. Format: `YYYY-MM-DD — tracker → artifact — one-line rationale`.

- _(none yet — file created 2026-04-24)_

**Pre-tracker precedent (retained for context, not retrospectively logged above):** The lakehouse already practices LinkedIn-post → TODO conversion — D60-D63 (pass-decision suite from Abu Kwaider's 2026-04-21 LinkedIn post), D64 (ml-intern from HF's 2026-04-22 release). This tracking file formalizes the discipline for *ongoing* sources where a single-shot conversion to TODO would lose the stream.

---

## Archived trackers

Trackers retired because the source went dormant, the work got published and incorporated (or rejected), or the lakehouse no longer needs to watch.

- _(none yet)_

---

## Review checklist (quarterly)

1. For each active tracker: check the mechanism, update `Last reviewed`, set next review.
2. Promote anything ready: write the TODO / ROADMAP / ADR, move the entry to the Promotion log with a one-line rationale.
3. Archive anything stale: move the entry to Archived trackers with a brief reason.
4. Add new trackers if new sources surfaced this quarter.
5. Sanity-check the Scholar alerts and GitHub watches are still firing (dead alert → silently miss a year of signal).
6. Update the file's implicit "Last reviewed" date at the top of this section: **2026-04-24** (initial creation).
