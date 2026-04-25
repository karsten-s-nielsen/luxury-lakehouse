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

### T4 — UnravelSports library (Joris Bekkers)

- **What it is:** Python library for soccer analytics (tracking data utilities, EFPI, visualization). Currently Python 3.11+, which does NOT fit the lakehouse's Python 3.10 lock (Databricks serverless constraint, see ADR referenced in `docs/engineering/conventions.md`). The lakehouse reimplemented EFPI in-house specifically to avoid this dependency.
- **Why the lakehouse cares:** Bekkers presented at LISS 2026-04-23, confirming the library is actively maintained. A Python 3.10-compatible release (or even a compat fork) would change the EFPI reimplementation decision — it could be retired in favor of the upstream. Not urgent, but worth watching to avoid indefinitely maintaining an in-house reimplementation if the upstream moves to broader Python support.
- **Mechanism:**
  - GitHub watch (Releases) on the unravelsports repo.
  - Check Python version support in quarterly review — if `python_requires` drops to `>=3.10`, trigger a "retire in-house EFPI reimplementation" TODO item.
- **Last reviewed:** 2026-04-24 (initial).
- **Next review:** 2026-07-24.
- **Status:** Active, passive watch (no expected near-term action).

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
