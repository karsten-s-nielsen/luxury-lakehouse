# AI Governance — EU AI Act Gap Analysis

| Field | Value |
|---|---|
| **Date** | 2026-04-14 |
| **Status** | Baseline — not high-risk under the current operating posture |
| **Framework** | Regulation (EU) 2024/1689 (the EU AI Act) |
| **Source finding** | `SEC-AUDIT-v1.12.0 REG-01` (internal audit tag; this document is the remediation artifact closing the finding) |
| **Author** | Karsten S. Nielsen |
| **Review cadence** | Annual, or on any re-classification trigger (see §13) |
| **Next review** | 2027-04-14 |

> **Reader orientation.** This document is the remediation artifact for the `REG-01` finding. It establishes the project's baseline posture against the EU AI Act, identifies the conditions under which any of the platform's models would become high-risk, and maps the repo's existing artifacts (workflow cards, HuggingFace model cards, `ARCHITECTURE.md`, `SECURITY.md`, `NOTICE`) to the Act's documentation, oversight, and data-governance obligations so the project is prepared to respond quickly if the operating context changes.

---

## Table of Contents

1. [Executive Summary](#1-executive-summary)
2. [Operating Context](#2-operating-context)
3. [Regulatory Scope Boundary](#3-regulatory-scope-boundary)
4. [Legal Framework Primer](#4-legal-framework-primer)
5. [Scope — Systems in Scope](#5-scope--systems-in-scope)
6. [Risk Classification Per System](#6-risk-classification-per-system)
7. [Conformity Assessment Obligations](#7-conformity-assessment-obligations)
8. [Technical Documentation Mapping](#8-technical-documentation-mapping)
9. [Human Oversight Mechanisms](#9-human-oversight-mechanisms)
10. [Fairness Analysis](#10-fairness-analysis)
11. [Gap Summary](#11-gap-summary)
12. [Recommended Governance Actions](#12-recommended-governance-actions)
13. [Re-Classification Triggers](#13-re-classification-triggers)
14. [Maintenance](#14-maintenance)
15. [References](#15-references)

---

## 1. Executive Summary

The (Right! Luxury!) Lakehouse hosts thirteen machine-learning systems that evaluate player performance on the basis of publicly licensed soccer match data. Under Annex III §4 of Regulation (EU) 2024/1689 (the EU AI Act), AI systems "intended to be used to make decisions affecting terms of work-related relationships, the promotion or termination of work-related contractual relationships, to allocate tasks… or to monitor and evaluate the performance and behaviour of persons in such relationships" are classified as **high-risk**. On their face, most of the project's per-player evaluative outputs could fall within that language **if a club deployed them for employment decisions**.

Under the **current operating posture**, they do not. The project is a single-maintainer research artifact, trained exclusively on public match data, published under a Creative Commons non-commercial licence, not sold or licensed to any club, and not used to make any employment decision about any natural person. On that basis, **none of the thirteen systems is classified as high-risk under the EU AI Act as currently operated**, and the project is not a "provider" of a high-risk AI system within the meaning of Article 3(3) and Chapter III of the Act.

This document establishes that baseline, names the conditions under which re-classification would be required (§13), and maps every Article 11, 14, 15 and Annex IV obligation to the existing artifacts in the repository so that the project can demonstrate compliance readiness without having to build documentation from scratch under time pressure if the operating context changes. The Annex III §4 obligations referenced herein become applicable from **2 August 2026** per Article 113.

---

## 2. Operating Context

The following facts together constitute the "mitigating context" cited in `REG-01` and are the foundation for every classification decision in §6.

| Fact | Evidence |
|---|---|
| Single-maintainer research project, no employees, no customers | `SECURITY.md` line 7 ("solo-maintained project") |
| No personal data in any data store; all subjects are professional athletes whose match performance is already in the public domain | `SECURITY.md` line 46 (Finding I-1, Informational) |
| All training and evaluation data is publicly licensed (CC-BY 4.0, CC-BY-NC 4.0, MIT) | [`NOTICE`](NOTICE) lines 9–32 |
| No club, league, federation, or employer is a customer, deployer, or partner of the project | `README.md`, `docs/getting-started.md` — absence of any customer list |
| Model artifacts are published on HuggingFace Hub under CC-BY-NC 4.0 for **research and reproducibility** only | `docs/huggingface/model-cards/xg-v2-model-card.md` (NC license stanza); `docs/huggingface/model-cards/vaep-model.md` line 258; etc. |
| The only user-facing surface is a Taipy dashboard hosted as a HuggingFace Space, labelled an "Interactive Demo · Published Datasets" in its site footer | `hf_taipy_app/src/page_template.py` — shared `_FOOTER_CONTENT` constant |
| Models consume public event data (StatsBomb, Wyscout) and public tracking data (Metrica, IDSSE, SkillCorner); no biometric, health, IoT, or wearable-device data is processed | [`NOTICE`](NOTICE) third-party data attribution section |
| No employment decision, recruitment ranking, contract negotiation, or task allocation is made with the outputs of any of these models by anyone, including the maintainer | Project has no employees and no user base that makes such decisions |

Because Article 6(2) and Annex III classify AI systems **by intended purpose of use**, the classification is a function of deployment context, not of the mathematics of the model. The operating context above is therefore load-bearing for every "not high-risk as currently operated" conclusion in this document. Any change to one of these facts is a re-classification trigger (§13).

---

## 3. Regulatory Scope Boundary

This document is deliberately narrow. It addresses **one regulation** and one category within it. The following adjacent frameworks are enumerated explicitly so that the scope is unambiguous and so that a future auditor does not have to infer the reasoning.

| Framework | Applicable? | Reasoning |
|---|---|---|
| EU AI Act, Annex III §4 (Employment) | **Conditionally (scope of this document)** | See §§4–6. |
| EU AI Act, Annex III §§1–3, 5–8 | N/A | No biometric identification, critical-infrastructure control, education/vocational-training admissions, essential-services access, law-enforcement, migration/asylum/border, or administration-of-justice use. |
| EU AI Act, Article 6(1) + Annex I (product-safety route) | N/A | No model is a safety component of a product covered by Union harmonisation legislation listed in Annex I. |
| EU AI Act, Article 5 (prohibited practices) | N/A | No social scoring, manipulative techniques, exploitation of vulnerabilities, real-time remote biometric ID in public spaces, or workplace emotion recognition. |
| EU AI Act, Chapter V (general-purpose AI models) | N/A | None of the models are general-purpose AI models within the meaning of Article 3(63). ScoutGPT is a domain-specific player-conditioned decoder (≈11M parameters) trained exclusively on SPADL action sequences, not a general-purpose language or vision model. |
| GDPR (Regulation (EU) 2016/679) | N/A for storage and processing | `SECURITY.md` I-1: no personal data is in any data store. The subjects are professional athletes whose match performance data is already in the public domain. If the project were ever to ingest personal data (scouting notes, biometric measurements, health data), this determination would need to be revisited. |
| BIPA / CCPA / similar biometric-privacy statutes | N/A | Tracking coordinates are spatial-temporal ball and player positions derived from broadcast cameras or stadium-mounted arrays. They are not fingerprints, facial-recognition templates, or iris scans, and they are not used to identify natural persons; identity is provided by the source data vendors as match-level metadata. |
| EU Data Act (Regulation (EU) 2023/2854) | N/A | The project does not process data from IoT or connected products, does not operate a data space, and is not a data holder or data user within the meaning of the Data Act. |
| National AI laws (EU Member State transpositions, UK, US state laws) | N/A currently | No customers or deployments in any specific jurisdiction. Re-assess on first commercial deployment. |

Anything not listed above is also out of scope. A future expansion of this document to additional regulations must be an explicit decision recorded in §14.

---

## 4. Legal Framework Primer

This section establishes the minimum legal vocabulary needed to read §§5–10. Every provision below is identified by article number so the text itself can be retrieved from the authoritative source (§15). Summaries are faithful paraphrases; direct quotations are marked with quotation marks.

### 4.1 Who the Act regulates (Article 3 — Definitions)

- **AI system** (Art. 3(1)): "machine-based system that is designed to operate with varying levels of autonomy and that may exhibit adaptiveness after deployment."
- **Provider** (Art. 3(3)): a person that "develops an AI system… and places it on the market or puts the AI system into service under its own name or trademark."
- **Deployer** (Art. 3(4)): a person "using an AI system under its authority except where the AI system is used in the course of a personal non-professional activity."
- **High-risk AI system**: not defined in Article 3; classified under Article 6 and enumerated in Annex III.

Consequence for the project: as currently operated, the project is neither a provider nor a deployer of a high-risk AI system within the meaning of the Act, because the "intended purpose" test in Article 6(2) is not met (see §4.2). Research publication of model artifacts on HuggingFace Hub under CC-BY-NC 4.0 is not "placing on the market or putting into service" for an Annex III purpose.

### 4.2 The two high-risk paths (Article 6)

Article 6 defines two disjoint routes by which an AI system is classified as high-risk:

- **Article 6(1) — product-safety route.** The AI system is a safety component of, or is itself, a product covered by Union harmonisation legislation listed in Annex I, and that product is required to undergo a third-party conformity assessment. **Not applicable to this project.**
- **Article 6(2) — Annex III route.** "In addition to the high-risk AI systems referred to in paragraph 1, AI systems referred in Annex III shall be considered to be high-risk." This is the route that `REG-01` invokes.

### 4.3 Annex III §4 — the clause that makes performance models conditionally high-risk

> "4. Employment, workers management and access to self-employment:
> (a) AI systems intended to be used for the recruitment or selection of natural persons, in particular to place targeted job advertisements, to analyse and filter job applications, and to evaluate candidates;
> (b) AI systems intended to be used to make decisions affecting terms of work-related relationships, the promotion or termination of work-related contractual relationships, to allocate tasks based on individual behaviour or personal traits or characteristics or to monitor and evaluate the performance and behaviour of persons in such relationships."

The language of §4(b) — "to monitor and evaluate the performance and behaviour of persons" — is the hook that makes per-player performance evaluation models relevant to the AI Act at all. The test is the **intended purpose of use**, not the mathematical form of the model. A model that computes xG for tactical analysis on a dashboard is not an Annex III §4 system. The same model, deployed by a club to inform squad-selection decisions or to determine whether to renew a player's contract, would be.

### 4.4 Provider obligations for high-risk AI systems (Chapter III, Section 2)

Where Article 6(2) applies, the provider must satisfy Articles 8–17. The five obligations below are the ones `REG-01` explicitly asks this document to address (Deliverables 2–5):

- **Article 10 — Data and data governance.** Training, validation, and testing data sets must be "relevant, sufficiently representative, and, to the best extent possible, free of errors and complete in view of the intended purpose." Datasets must reflect "characteristics particular to the specific geographical, contextual, behavioural or functional setting." Bias detection and mitigation is required; sensitive personal data may be processed exceptionally for bias correction under strict conditions.
- **Article 11 — Technical documentation.** "The technical documentation of a high-risk AI system shall be drawn up before that system is placed on the market or put into service and shall be kept up-to date." The minimum content is enumerated in Annex IV.
- **Article 14 — Human oversight.** High-risk AI systems "shall be designed and developed in such a way… that they can be effectively overseen by natural persons during the period in which they are in use." Overseers must be able to "properly understand the relevant capacities and limitations" of the system, "remain aware of the possible tendency of automatically relying or over-relying on the output", "correctly interpret" it, "decide… not to use" or to "disregard, override or reverse" it, and "intervene… or interrupt the system through a 'stop' button."
- **Article 15 — Accuracy, robustness and cybersecurity.** High-risk systems "shall be designed and developed in such a way that they achieve an appropriate level of accuracy, robustness, and cybersecurity, and that they perform consistently in those respects throughout their lifecycle." Accuracy metrics must be "declared in the accompanying instructions of use." Robustness requirements include resilience against data and model poisoning, adversarial examples, and confidentiality attacks.
- **Article 43 — Conformity assessment.** Article 43 establishes two procedures: **Annex VI (internal control)**, a self-assessment by the provider, and **Annex VII (notified body)**, assessment by an independent body. Systems referred to in points 2–8 of Annex III (which includes §4 Employment) "shall follow" the Annex VI internal-control procedure in the default case; the notified-body route only applies when harmonised standards are not used or only partially applied, or where the Commission has required it.

### 4.5 Annex IV — Technical documentation checklist

Annex IV enumerates the minimum headings the Article 11 technical documentation file must contain. The nine principal headings are:

1. General description of the AI system (intended purpose, provider, version, hardware/software interactions, user interface).
2. Detailed description of elements of the system and development (design specs, system architecture, data requirements, human oversight measures, pre-determined changes).
3. Detailed information about monitoring, functioning and control (capabilities, limitations, accuracy, unintended outcomes, risk sources).
4. Description of the appropriateness of the performance metrics.
5. Detailed description of the risk management system as required by Article 9.
6. Description of relevant changes made to the system through its lifecycle.
7. List of harmonised standards applied (or, where not applied, detailed description of alternative solutions).
8. Copy of the EU declaration of conformity referred to in Article 47.
9. Detailed description of the post-market monitoring system referred to in Article 72.

### 4.6 Entry into application (Article 113)

Regulation (EU) 2024/1689 entered into force on 1 August 2024. Its provisions apply in phases. **For high-risk AI systems classified under Article 6(2) and Annex III — the only category relevant to this document — the obligations apply from 2 August 2026.** Article 6(1) (the Annex I product-safety route, not applicable here) is deferred to 2 August 2027. Chapter I (general provisions) and Chapter II (prohibited practices) have already applied since 2 February 2025. Chapter V (general-purpose AI), Chapter VII (governance), and penalties applied from 2 August 2025.

The 2 August 2026 date is the **compliance deadline** cited in `REG-01`. It is the date by which the project must either (a) remain demonstrably outside the Annex III §4 scope as described in §2, or (b) have completed the obligations in §§4.4–4.5 for every in-scope system before operating in a regulated capacity.

---

## 5. Scope — Systems in Scope

The `REG-01` finding names seven systems by label. This document analyses the full set of thirteen machine-learning systems in the repository whose outputs evaluate individual-player performance or are load-bearing for systems that do, on the principle that a gap analysis limited to the minimum literal list would create dead space between the scoped systems and the rest of the same pipeline. Inclusion here does **not** imply high-risk classification; see §6 for the per-system determination.

| # | System | Workflow card | Source code (primary) | HuggingFace model card | Per-player evaluative? | Status |
|---|---|---|---|---|---|---|
| 1 | Expected Goals v2 (Deep Sets) | [`wf-xg-v2.yaml`](workflow-cards/wf-xg-v2.yaml) | `src/ingestion/xg_model_v2.py`, `scripts/train_xg_v2_hf.py` | [`docs/huggingface/model-cards/xg-v2-model-card.md`](docs/huggingface/model-cards/xg-v2-model-card.md) | Indirect — shot-level with uncertainty; aggregates to player xG | Production |
| 3 | VAEP Action Valuation | [`wf-vaep.yaml`](workflow-cards/wf-vaep.yaml) | `src/ingestion/spadl_vaep.py`, `scripts/train_vaep_model_hf.py` | [`docs/huggingface/model-cards/vaep-model.md`](docs/huggingface/model-cards/vaep-model.md) | **Direct** — per-action attribution aggregates to per-player total value | Production |
| 4 | PSxG (Post-Shot Expected Goals) | [`wf-goalkeeper.yaml`](workflow-cards/wf-goalkeeper.yaml) | `src/analytics/goalkeeper.py`, `scripts/train_psxg_hf.py` | [`docs/huggingface/model-cards/psxg-model.md`](docs/huggingface/model-cards/psxg-model.md) | **Direct** — goalkeeper shot-stopping evaluation | Production |
| 5 | Pitch Control | [`wf-pitch-control.yaml`](workflow-cards/wf-pitch-control.yaml) | `src/analytics/pitch_control.py`, `src/ingestion/pitch_control_batch.py` | [`docs/huggingface/model-cards/pitch-control.md`](docs/huggingface/model-cards/pitch-control.md) | Indirect — surface-level; no per-player output by itself, but feeds 6, 7, 9 | Production (heuristic, no training) |
| 6 | DEFCON Defensive Credit | [`wf-defcon.yaml`](workflow-cards/wf-defcon.yaml) | `src/ingestion/defcon_lite.py` | [`docs/huggingface/model-cards/defcon.md`](docs/huggingface/model-cards/defcon.md) | **Direct** — per-defender credit attribution | Production |
| 7 | Off-Ball xT | [`wf-off-ball-xt.yaml`](workflow-cards/wf-off-ball-xt.yaml) | `src/ingestion/off_ball_xt.py`, `src/analytics/off_ball_xt.py` | [`docs/huggingface/model-cards/off-ball-xt.md`](docs/huggingface/model-cards/off-ball-xt.md) | **Direct** — per-player off-ball threat attribution | Production |
| 8 | OBSO + PAUSA pass timing | [`wf-obso-pausa.yaml`](workflow-cards/wf-obso-pausa.yaml) | `src/ingestion/pausa.py`, `scripts/compute_obso_hf.py` | [`docs/huggingface/model-cards/obso-pausa.md`](docs/huggingface/model-cards/obso-pausa.md) | **Direct** — per-passer pass-timing evaluation | Production |
| 9 | Space Creation | [`wf-space-creation.yaml`](workflow-cards/wf-space-creation.yaml) | `scripts/compute_space_creation_hf.py` | [`docs/huggingface/model-cards/space-creation.md`](docs/huggingface/model-cards/space-creation.md) | **Direct** — per-player counterfactual space-creation value | Production |
| 10 | Football2Vec v1 | [`wf-football2vec.yaml`](workflow-cards/wf-football2vec.yaml) | `src/ingestion/player_embeddings_v1.py` | [`docs/huggingface/model-cards/football2vec-statsbomb-wyscout.md`](docs/huggingface/model-cards/football2vec-statsbomb-wyscout.md) | **Direct** — per-player Doc2Vec style vectors | **Deprecated** — superseded by v2 |
| 11 | Football2Vec v2 (transformer) | [`wf-football2vec-v2.yaml`](workflow-cards/wf-football2vec-v2.yaml) | `src/analytics/football2vec_transformer.py`, `src/ingestion/player_embeddings_v2.py` | [`docs/huggingface/model-cards/football2vec-v2-model-card.md`](docs/huggingface/model-cards/football2vec-v2-model-card.md) | **Direct** — per-player 192-d behavioural embedding with adversarial competition debiasing; powers the Player Similarity page | Production |
| 12 | Football2Vec 360 (transformer + Deep Sets) | [`wf-football2vec-360.yaml`](workflow-cards/wf-football2vec-360.yaml) | `src/analytics/football2vec_360.py` | [`docs/huggingface/model-cards/football2vec-360-model-card.md`](docs/huggingface/model-cards/football2vec-360-model-card.md) | **Direct** — per-player 208-d embedding (192-d event + 16-d freeze-frame context) with adversarial team debiasing | Production |
| 13 | ScoutGPT (player-conditioned decoder) | [`wf-scoutgpt.yaml`](workflow-cards/wf-scoutgpt.yaml) | `src/analytics/scoutgpt_decoder.py`, `scripts/train_scoutgpt_hf.py` | [`docs/huggingface/model-cards/scoutgpt.md`](docs/huggingface/model-cards/scoutgpt.md) (development-status card) | **Direct** — counterfactual player-substitution predictor | **Development** |

Systems 10 and 13 are explicitly included despite their non-production status. Deprecated code is load-bearing for interpretation of historical outputs, and development-status code has a non-zero chance of being the subject of an `REG-01`-related question before it reaches production; including it keeps the document honest and complete.

---

## 6. Risk Classification Per System

The test applied to each system is Article 6(2) read through Annex III §4(a) and §4(b): *if this system were deployed by a club for recruitment, selection, contract-affecting decisions, promotion/termination decisions, task allocation, or the monitoring and evaluation of worker performance, would it fall within Annex III §4?*

The question is hypothetical for every row in the table below because, under the operating context in §2, no such deployment exists. The answer is therefore conditional and forward-looking, not descriptive of any current use.

| # | System | §4(a) recruitment/selection? | §4(b) work-relationship decisions / performance monitoring? | Conditional classification | Current classification (this project) |
|---|---|---|---|---|---|
| 1 | xG v2 (Deep Sets) | Yes — aggregated to player xG can inform scouting shortlists | Yes — can feed squad-selection or contract decisions | **Would be high-risk** under §4 in such deployment | **Not high-risk** (no such deployment) |
| 3 | VAEP | Yes — player-level VAEP is a scouting-ready per-player value | Yes — "goals beyond goals" is canonically used for player valuation | **Would be high-risk** under §4 | **Not high-risk** |
| 4 | PSxG | Yes — goals-prevented is the canonical shot-stopping metric for goalkeeper recruitment | Yes — could inform goalkeeper contract decisions | **Would be high-risk** under §4 | **Not high-risk** |
| 5 | Pitch Control | No on its own | No on its own; but indirect, through 6, 7, 9 | **Not directly high-risk**; inherits any classification of its downstream consumers | **Not high-risk** |
| 6 | DEFCON | Yes — per-defender credit | Yes — per-defender value is interpretable as performance evaluation | **Would be high-risk** under §4 | **Not high-risk** |
| 7 | Off-Ball xT | Yes — attributes attacking threat to off-ball movement | Yes — used to evaluate off-ball contribution | **Would be high-risk** under §4 | **Not high-risk** |
| 8 | OBSO + PAUSA | Yes — per-passer pass-timing evaluation | Yes — identifies players who retain possession under pressure | **Would be high-risk** under §4 | **Not high-risk** |
| 9 | Space Creation | Yes — per-player counterfactual space-creation value | Yes — used for valuing off-ball movement | **Would be high-risk** under §4 | **Not high-risk** |
| 10 | Football2Vec v1 (deprecated) | Possibly — similarity search is canonical scouting tooling | Yes — could underpin like-for-like replacement decisions | **Would be high-risk** under §4 if reactivated | **Not high-risk** (deprecated, not served) |
| 11 | Football2Vec v2 | Yes — behavioural similarity is a classical scouting use case, and the Player Similarity page is the nearest surface | Yes — like-for-like replacement, transfer analysis | **Would be high-risk** under §4 | **Not high-risk** (dashboard is a research demo) |
| 12 | Football2Vec 360 | Yes — same as v2 | Yes — same as v2 | **Would be high-risk** under §4 | **Not high-risk** |
| 13 | ScoutGPT (development) | Yes by construction — counterfactual player substitution is literally "who else could fill this role" | Yes — counterfactual performance evaluation | **Would be high-risk** under §4 if released for deployment | **Not high-risk** (in development; not released) |

The column **Current classification (this project)** is a statement about deployment context, not about the models. Every row in that column is anchored to §2 — the facts that together rule out `high-risk` classification for the project as currently operated. If any fact in §2 changes, the relevant row re-classifies to the "Conditional" column and the project must take up the obligations in §§7–10 before continuing.

---

## 7. Conformity Assessment Obligations

This section addresses **Deliverable 2** of `REG-01`.

Article 43 establishes two conformity-assessment procedures for high-risk AI systems. Systems referred to in **points 2 to 8 of Annex III** — which includes the Annex III §4 Employment category that is the sole scope of this document — are subject by default to the **Annex VI internal-control procedure**. The notified-body route (Annex VII) applies only where harmonised standards are not applied or are only partially applied, or where the Commission has required the Annex VII route by delegated act.

The practical consequence for this project, in any hypothetical future where one of the systems in §5 is deployed under Annex III §4, is that the conformity assessment would be self-assessment by the provider (this project, or a successor organisation). The maintainer would be responsible for:

1. Maintaining the Article 11 technical documentation (see §8).
2. Maintaining and operating a quality-management system compliant with Article 17.
3. Drawing up a written EU declaration of conformity referred to in Article 47 and keeping it for ten years.
4. Affixing the CE marking under Article 48 (where applicable to the system's deployment form).
5. Registering the system in the EU database under Article 49 before placing it on the market or putting it into service.
6. Implementing the post-market monitoring system required under Article 72.

**Current obligation status:** none of the above is an active obligation today, because none of the systems in §5 is being operated or placed on the market for an Annex III §4 purpose. Item 1 (technical documentation) is partially satisfied already by the existing repo artifacts as mapped in §8; items 2–6 would require fresh work on activation of a re-classification trigger.

---

## 8. Technical Documentation Mapping

This section addresses **Deliverable 3** of `REG-01`. It maps the nine Annex IV headings to the repo artifacts that already satisfy them, notes the gaps, and names what would need to be added in the event of re-classification.

| Annex IV heading | Existing repo artifacts | Coverage | Gap, if any |
|---|---|---|---|
| (1) General description of the AI system | `ARCHITECTURE.md` §§1–2 (executive summary, target architecture, § 3 C4 model); per-system workflow cards under [`workflow-cards/`](workflow-cards/) (`name`, `id`, `version`, `status`, `domain`, `owners`, `references`, `governance`); per-system HF model cards under [`docs/huggingface/model-cards/`](docs/huggingface/model-cards/) (all thirteen systems) | **Strong for all thirteen systems** | None. Every system in §5 now carries a dedicated model card with intended-use, limitations, and governance sections. Heuristic systems (pitch control, off-ball xT, space creation) carry "method cards" with the same structure, minus a trained-weights section. |
| (2) Detailed description of elements and development process | Workflow cards (`inputs`, `outputs`, `execution`, `depends_on`, `idempotency`, `performance`); source code paths listed in the workflow cards under `links.source_code`; `ARCHITECTURE.md` § 5 (Repository Structure); `ARCHITECTURE.md` § 3 (C4 model, Container + Component diagrams) | **Strong** | None for the current operating posture. Under re-classification, a formal design-document-level write-up linking each workflow card's `references:` citations to the precise algorithmic choices would need to be produced, but the information exists in scattered form in the codebase and NOTICE today. |
| (3) Detailed information about monitoring, functioning and control (capabilities, limitations, accuracy, unintended outcomes, risk sources) | HF model cards' "Limitations" sections (all thirteen systems); workflow cards' `monitoring.metrics` block (Brier score, ROC-AUC, MLM accuracy, etc.) with baselines and warn/alert thresholds; `wf-model-validation` central validator driving PSI/Wasserstein/CUSUM drift detection; `ARCHITECTURE.md` § 6 (Cross-Cutting Concerns) | **Strong** | Under re-classification, the Limitations narrative on each card would need to be extended with failure-mode enumeration at Article 9 risk-management depth. |
| (4) Description of the appropriateness of performance metrics | HF model cards' "Performance" or "Results" sections quantify Brier / ROC-AUC / log-loss / calibration error / MLM accuracy and state why each is appropriate for the task; workflow cards repeat the baselines; method cards for heuristic systems document validation baselines from the original publications (Spearman 2017, Fernández & Bornn 2018, Singh 2018, Kim et al. 2025) | **Strong** | For the heuristic systems (pitch control, off-ball xT, space creation) the performance metrics are task-appropriate by construction but are not numerical accuracy declarations in the Article 15 sense. Under re-classification, an Article 15 accuracy declaration would need to translate these into deployment-relevant uncertainty statements per system. |
| (5) Detailed description of the risk management system (Article 9) | Not present as an Article 9 formal risk management system. Adjacent: `SECURITY.md` executive summary and informational findings; `ARCHITECTURE.md` § 7 (Risk Register, 10 risks R1–R10); `CLAUDE.md` "Failure Investigation Protocol" (three-strikes rule, report-findings-before-fixes discipline) | **Partial** | An Article 9 risk management system is a formal, documented, iterative process spanning the entire AI lifecycle. The project has operational risk discipline but does not have an Article 9 document per se. On re-classification, the existing risk register plus a new lifecycle risk-management plan would need to be combined. |
| (6) Description of relevant changes made to the system through its lifecycle | Git history (public at the GitHub remote); `TODO.md` "Last updated" summary line at the top; cycle-level design docs under `docs/superpowers/plans/` and `docs/superpowers/specs/` (dated `YYYY-MM-DD`); `docs/superpowers/adrs/ADR-001-evolve-code-execution.md` for architecture-level decisions | **Strong** | None. Git history is authoritative and the cycle design docs give a narrative layer on top of it. |
| (7) List of harmonised standards applied | None. No harmonised standards have been adopted by the Commission for Annex III §4 systems at the time of writing. | **N/A at present** | If harmonised standards become available before re-classification, they must be evaluated and adopted. Track this via §14 maintenance. |
| (8) Copy of the EU declaration of conformity referred to in Article 47 | None (not required under the current operating posture). | **N/A at present** | Would be drafted only on re-classification. A template would be needed. |
| (9) Detailed description of the post-market monitoring system (Article 72) | `wf-model-validation` runs daily drift detection via PSI / Wasserstein / CUSUM against baselines stored in `model_baseline_scalars`. Failing thresholds surface on the Taipy "Workflows" page. `fct_workflow_costs` retains runtime/cost history for capacity and incident analysis. | **Medium** | Article 72 post-market monitoring is narrower than the platform's drift monitoring — it specifically requires collecting data on system behaviour "in real-world use" and acting on it. Under re-classification this existing drift-detection infrastructure is a head start but would need an Article 72 plan explicitly referencing it. |

**Summary.** Under re-classification, the technical documentation file can be substantially assembled from existing repo artifacts within days rather than weeks. Every system in §5 now carries a dedicated HF model card with intended-use, limitations, and governance sections, closing the Annex IV(1) gap that originally motivated Deliverable 12.1. The remaining weakest area is Annex IV(5) — the absence of a formal Article 9 risk-management system — which would need to be drafted on re-classification.

---

## 9. Human Oversight Mechanisms

This section addresses **Deliverable 4** of `REG-01`.

Article 14(4) requires that the persons assigned to oversee a high-risk AI system must be enabled to (a) understand the system's capacities and limitations, (b) remain aware of automation bias, (c) correctly interpret the output, (d) decide not to use it or to override or disregard the output, and (e) intervene or interrupt the system via a stop function. The table below reads each (a)–(e) against the current Taipy dashboard and the existing repo artifacts, which is the only surface on which any natural person currently encounters the outputs of these systems.

| Art. 14(4) requirement | Current state | Evidence | Gap |
|---|---|---|---|
| (a) Overseer understands capacities and limitations | Every Taipy page that implements a published methodology carries a `Citation(text, url)` in its `PageConfig`; every page metric requires a `help_text` tooltip per `CLAUDE.md` "Template Rules"; every sidebar filter requires `help=` text; the glossary in `hf_taipy_app/src/template.py` provides domain definitions per-page | `CLAUDE.md` lines 167–180 (Template Rules); `hf_taipy_app/src/pages/player_similarity.py` lines 14–26 (explicit citations); the `PageConfig` dataclass constructor enforces `help_text` | **Weak for non-in-dashboard surfaces.** HF Hub users who download a model directly without interacting with the dashboard do not receive this scaffolding. See Deliverable 12.1. |
| (b) Awareness of automation bias | `UX Standards` rule in `CLAUDE.md` line 202 requires that "Computed metrics must show scale and direction" — any displayed score on a 0–1 or non-obvious scale must include range and direction. xG v2 confidence intervals are surfaced on the shot map page. | `CLAUDE.md` lines 202–203 | No explicit automation-bias warning on the dashboard or on the HF model cards. Deliverable 12.1. |
| (c) Correct interpretation of output | All metrics carry scale/direction via `help_text`. Football2Vec similarity page explicitly labels behavioural (128d) vs statistical (13d) space; the adversarial debiasing is documented in the page description. | `hf_taipy_app/src/pages/player_similarity.py` lines 12–18 | Adequate for a research demo. Under re-classification, per-page "Known failure modes" sections would be needed. |
| (d) Decide not to use, override, disregard | Implicit — the dashboard is a research viewer and any downstream action is entirely under the user's control. No automated decision is emitted. | Absence of any "decision engine" in the codebase | Adequate for the current operating posture. Under re-classification, an explicit "do not use for employment decisions" banner on every page would be the minimum intervention. See Deliverable 12.2. |
| (e) Intervene or interrupt via stop function | N/A — there is no automated closed-loop decision system. The pipeline can be paused by stopping the Databricks job or the HuggingFace Space. | Databricks Jobs UI; HuggingFace Spaces runtime controls | Adequate. A formal Article 14 "stop button" is not meaningful for a non-real-time, non-closed-loop dashboard. |

**Summary.** The existing UX discipline encoded in `CLAUDE.md` (Template Rules and UX Standards) is already doing most of the work that Article 14(4) would require. The main gap today is that the discipline applies only to the dashboard surface; HuggingFace Hub consumers who download models without touching the dashboard do not see the same scaffolding. The mitigation in Deliverable 12.1 closes that gap.

---

## 10. Fairness Analysis

This section addresses **Deliverable 5** of `REG-01`.

### 10.1 What Article 10 requires

Article 10 requires that training, validation and testing data be "relevant, sufficiently representative, and, to the best extent possible, free of errors and complete in view of the intended purpose" and that they "have the appropriate statistical properties… including, where applicable, as regards the persons or groups of persons in relation to whom the high-risk AI system is intended to be used." Bias detection and mitigation is required (Art. 10(2)(f)–(g)). Article 10(5) permits exceptional processing of special-category personal data for bias detection and correction under strict conditions.

### 10.2 Training data composition

All training data used by the thirteen systems in §5 is publicly licensed open-data. A complete inventory and licence attribution is maintained in [`NOTICE`](NOTICE) lines 6–32.

| Source | Rough coverage | Licence | Training data for |
|---|---|---|---|
| StatsBomb Open Data | ≈ 3,000 matches (events); 323 matches (360 freeze frames) | CC-BY 4.0 | xG v2, VAEP, PSxG, Football2Vec v1/v2/360, ScoutGPT |
| Wyscout Public Dataset | ≈ 1,900 matches (events) | CC-BY-NC 4.0 | xG v2, VAEP, Football2Vec v1/v2, ScoutGPT |
| Metrica Sports sample-data | Small sample | Permissive | Pitch Control, Off-Ball xT, OBSO/PAUSA, Space Creation |
| IDSSE (Bassek et al. 2025, DFL) | 7 Bundesliga matches | CC-BY 4.0 | Pitch Control, Off-Ball xT, OBSO/PAUSA, Space Creation |
| SkillCorner Open Data | 10 A-League matches | MIT | Pitch Control, Off-Ball xT, OBSO/PAUSA, Space Creation |

### 10.3 Representativity

Representativity of the training data is a function of the competitions present in the source datasets. StatsBomb Open Data and Wyscout together cover men's top-flight European leagues (Premier League, La Liga, Serie A, Bundesliga, Ligue 1), UEFA Champions League, selected domestic cups, the FIFA World Cup (men's and women's tournaments), the UEFA European Championship, and some South American matches; coverage of women's football is present but skews toward national-team rather than club football. Tracking data (Metrica / IDSSE / SkillCorner) is limited to a small number of matches from European and Asia-Pacific competitions. Lower-division and academy-level football is almost entirely absent.

**Consequence.** A model trained on this corpus that is then deployed to evaluate a player in, for example, the Norwegian second division, the Liga MX women's league, or a U-17 academy league would be operating outside the representative distribution of its training data. This is an **Article 10(2)(f)** representativity gap. It does not invalidate the models for the research purposes they are currently used for, but it is load-bearing for any future classification discussion.

### 10.4 Protected-attribute analysis

Article 10(2)(g) requires "examination in view of possible biases that are likely to affect the health and safety of persons, have a negative impact on fundamental rights, or lead to discrimination prohibited by Union law." Operationalising this would normally mean computing outcome statistics stratified by protected attributes (nationality, race, gender, age, disability, socioeconomic background).

**The open-data sources used by this project do not contain protected attributes.** StatsBomb, Wyscout and the tracking datasets provide player, team, match and action metadata only. There is no race or ethnicity field, no disability status, no socioeconomic marker. Gender can be inferred from the competition (men's vs women's) but only at the match level. Nationality can be retrieved from external sources but is not in the datasets themselves.

Two consequences follow:

1. **Statistical parity, equal opportunity, predictive parity, calibration within groups, or any of the other standard group-fairness metrics cannot be computed on-corpus by construction.** This is itself a finding under Article 10.
2. **Processing protected attributes under the Article 10(5) derogation is not an option the project can exercise today**, because doing so would require ingesting personal data from external sources (which would invalidate `SECURITY.md` I-1 and trigger GDPR obligations). Any future attempt to compute group-fairness metrics must be preceded by a lawful-basis analysis under GDPR Article 6 and, for special-category data, Article 9.

### 10.5 Existing fairness-relevant interventions

Two of the thirteen systems already ship with an explicit mechanism designed to remove a specific source of unfair advantage:

- **Football2Vec v2** (system 11) applies **adversarial competition debiasing** via a gradient-reversal layer (Ganin et al. 2016, *Domain-Adversarial Training of Neural Networks*). A competition classifier is attached to the encoder through a gradient-reversal layer during Stage 2 training; the encoder learns to produce embeddings from which the competition cannot be predicted. The intent is to remove league-level style confounds so that behavioural similarity search generalises across leagues. Source: `docs/huggingface/model-cards/football2vec-v2-model-card.md` §§ "Two-Stage Training" and "Adversarial head"; `docs/research/adversarial-training.md`; `hf_taipy_app/src/pages/player_similarity.py` lines 14–20.
- **Football2Vec 360** (system 12) applies the same technique to **team-identity debiasing** instead of competition debiasing. The adversarial head predicts team identity; the encoder learns to produce embeddings from which the team cannot be recovered. Source: `docs/huggingface/model-cards/football2vec-360-model-card.md` §§ "Two-Stage Training" and "Adversarial head".

These interventions are not general-purpose group-fairness corrections in the Article 10 sense — neither league nor team is a protected attribute under EU law. They are context-confounder controls that happen to reduce a form of bias that would be observable as unfair advantage in scouting use. They are worth documenting as existing fairness-adjacent work under Article 10(2)(g) because, on re-classification, they are **evidence that the project has considered confounding effects during training**, not just at evaluation time.

### 10.6 Known imbalances that would need explicit treatment under re-classification

| Imbalance | Nature | Likely impact | Mitigation path |
|---|---|---|---|
| Men's vs women's football coverage | Strong skew toward men's competitions | Models fit tighter to men's football; accuracy on women's football may be lower | Retrain with competition-balanced sampling; report per-competition metrics |
| Top five European leagues vs everything else | Coverage skew | Models generalise less well to lower divisions, continental cups, regional leagues | Ablation studies; competition-stratified Brier / ROC-AUC reporting; explicit statement of representativity in model cards |
| 360 data only available for ≈ 323 matches | Freeze-frame features only for a subset of StatsBomb's event data | xG v2 and Football2Vec 360 fall back to zero-context on non-360 matches (see xg-v2 card "Limitations" §3) | Already documented; Article 14 overseers must know about the degradation |
| Wyscout non-commercial licence | Legal constraint | Downstream commercial re-use is restricted | Already handled via CC-BY-NC 4.0 inheritance in every HF model card |

**Summary.** The fairness analysis is honest about two facts: (a) existing models include specific, published confounder-removal techniques and those are genuine fairness-adjacent interventions worth citing, and (b) formal group-fairness auditing against protected attributes is **structurally infeasible** on this corpus without first ingesting personal data from external sources, which would break the `SECURITY.md` I-1 posture. This is itself an Article 10 finding that any future high-risk deployer would have to take on explicitly before operating the system under Annex III §4.

---

## 11. Gap Summary

| Area | Present | Partial / Adjacent | Missing | Applicable under current posture? |
|---|---|---|---|---|
| Risk classification (Deliverable 1) | §6 | — | — | Yes — this document |
| Conformity assessment plan (Deliverable 2) | §7 — identifies Annex VI route | Art. 17 quality-management system, Art. 47 declaration template, Art. 48 CE marking, Art. 49 EU database registration | — | **No — not applicable under current posture.** Would be required on re-classification. |
| Article 11 / Annex IV technical documentation (Deliverable 3) | Workflow cards, HF model cards (6 of 13 systems), `ARCHITECTURE.md`, `NOTICE`, git history, `docs/superpowers/plans/` + `specs/` | Article 9 risk-management system document; Article 72 post-market monitoring plan | HF model cards for systems 5, 6, 7, 8, 9, 13; Article 47 EU declaration of conformity template | **Partial.** Bulk of the content exists; gaps are explicit per system in §8. |
| Article 14 human oversight (Deliverable 4) | Dashboard citations, glossary, tooltips, help text, `CLAUDE.md` Template Rules and UX Standards, xG v2 confidence intervals | Dashboard covers the research surface; HF Hub consumers do not receive the same scaffolding | Explicit "not for employment decisions" statement on every surface; automation-bias warnings | **Partial.** Adequate for the research posture; gaps are addressed in Deliverable 12.1. |
| Article 10 / Art. 10(2) fairness analysis (Deliverable 5) | §10 — training-data inventory, representativity findings, existing debiasing interventions, explicit infeasibility of group-fairness auditing | Competition-balanced training ablations | Protected-attribute statistical audits (infeasible without ingesting personal data) | **Partial and structurally bounded.** Full group-fairness analysis cannot be performed on this corpus under the current security posture. |

---

## 12. Recommended Governance Actions

These are minimal, reversible, and can be completed in a single working session. They address the gaps in §11 without changing the operating posture.

### 12.1 Add an "EU AI Act — Intended Use and Non-Use" stanza to every HF model card

Every HuggingFace Hub model card listed in §5 must carry a short, clearly-labelled stanza explicitly stating that the model is not intended for, not validated for, and not supplied to any Annex III §4 use, and that any deployer who wishes to use it for such a purpose must perform their own conformity assessment. The stanza appears between "Intended Use" and "Limitations" so HF Hub consumers see it without scrolling past the usage instructions.

**Applies to all twelve cards** in [`docs/huggingface/model-cards/`](docs/huggingface/model-cards/): `xg-v2-model-card.md`, `vaep-model.md`, `psxg-model.md`, `pitch-control.md`, `defcon.md`, `off-ball-xt.md`, `obso-pausa.md`, `space-creation.md`, `football2vec-statsbomb-wyscout.md`, `football2vec-v2-model-card.md`, `football2vec-360-model-card.md`, `scoutgpt.md`. The xG v1 card was retired with the v1 model on 2026-05-03 (SK3-MIG-B XG1-RETIRE).

### 12.2 Add a short governance section to `README.md` pointing at this document

A single paragraph under the project description, explicitly stating that the project is a research artifact, not deployed for employment decisions, and linking to `AI_GOVERNANCE.md`. This ensures that any cold reader — including a regulator or auditor who lands on the repo from a search engine — sees the governance posture on the front page without having to hunt for it.

### 12.3 Add an "AI Governance Framework" section to `NOTICE`

A short paragraph at the end of the `NOTICE` file (after Mathematical References) stating that the project is assessed against Regulation (EU) 2024/1689 and pointing at `AI_GOVERNANCE.md`. This keeps the single authoritative attribution file consistent with the governance posture.

### 12.4 Update `TODO.md` SEC1 entry to reflect resolution

Mark `SEC1` as resolved by this document in the same way `D56`, `SEC2`, and other closed cycles are resolved — by removal from the "On Deck" table (per the `feedback_completed_items.md` memory convention: TODO and ROADMAP are forward-looking; completed work is deleted, and the "Last updated" line at the top of `TODO.md` is amended).

### 12.5 Wire this document into the final-review mechanism

See §14 for the maintenance mechanism. In summary: a CI test at [`src/tests/test_ai_governance_md.py`](src/tests/test_ai_governance_md.py), mirroring [`src/tests/test_architecture_md_appendix.py`](src/tests/test_architecture_md_appendix.py), validates that (a) the document exists, (b) it contains all required top-level sections, (c) every workflow card matching the per-player evaluative pattern is listed in §5, (d) each per-player evaluative workflow card has a matching model-card document on disk under [`docs/huggingface/model-cards/`](docs/huggingface/model-cards/), (e) each per-player evaluative workflow card carries a `governance:` block pointing at `AI_GOVERNANCE.md`, (f) the **Next review** date has not gone more than 30 days stale, and (g) the `REG-01` provenance tag is present. A short bullet under a new "AI Governance" subsection of `CLAUDE.md` instructs that modifications to per-player evaluative models must be accompanied by updates to this document and re-running the test.

### 12.6 Deferred, explicitly out of scope for `REG-01` closure

The items below are catalogued here so they are not forgotten, but they are **not required** for `REG-01` closure under the current operating posture. They would become required only on a re-classification trigger (§13).

- Author an Article 9 risk-management system document (lifecycle-spanning, formal).
- Author an Article 72 post-market monitoring plan (the `wf-model-validation` drift infrastructure is a head start).
- Author an Article 47 EU declaration of conformity template.
- Evaluate harmonised standards as they become available under Article 40.

---

## 13. Re-Classification Triggers

Any one of the following events requires this document to be re-read and the classifications in §6 re-examined **before** the action in question is taken. This list is exhaustive for the current operating posture; new triggers can be added via the maintenance procedure in §14.

1. **First paying customer, licensee, or commercial deployer.** Any arrangement that has a natural or legal person other than the maintainer operating or consuming one of the systems in §5 for a professional activity.
2. **First partnership with a club, league, federation, agency, scouting organisation, player union, or sports-data vendor** that involves any of the systems in §5.
3. **Integration of any system in §5 into a workflow that informs an employment decision about any natural person**, regardless of who operates that workflow. "Informs" is interpreted broadly: any use of a model output in a decision pipeline that terminates in a contract, transfer, promotion, task allocation, or performance review.
4. **Ingestion of personal data** — any field that identifies a natural person beyond what is already in the public match data (e.g., personal biometric measurements, health data, training-load data, scouting notes, private messages, salary information).
5. **Ingestion of protected-attribute data** — any field that would constitute special-category personal data under GDPR Article 9 (racial or ethnic origin, political opinions, religious or philosophical beliefs, trade-union membership, genetic data, biometric data for the purpose of uniquely identifying a natural person, health data, sex-life or sexual-orientation data).
6. **Release of ScoutGPT or any future generative model** to a wider audience than the current development context. Generative counterfactual models are more likely to be mistaken for decision systems by a deployer than regression models are.
7. **Reclassification of any source data** from public to non-public — e.g., if StatsBomb or Wyscout tightens their licence terms or introduces a personal-data field the project decides to ingest.
8. **Publication of a harmonised standard under Article 40** covering any Annex III §4 category. Adoption of the standard is not automatic, but its existence triggers a review decision.
9. **Change of maintainer or introduction of additional maintainers** — the "single-maintainer research project" fact in §2 would no longer hold.
10. **Change of legal jurisdiction** — if the project's operating entity moves, is acquired, or begins to have EU-based users who would trigger Article 2 territorial scope in a way the current posture does not.

On any of the above, the review must be completed **before** the action is taken, not after. The result of the review is either (a) a re-confirmation that the trigger does not change the classification (recorded in §14 with a short note) or (b) an updated classification for one or more systems in §6 and the activation of the relevant obligations in §§7–10.

---

## 14. Maintenance

**Cadence.** This document is reviewed at least **annually**, and **immediately** on any trigger in §13.

**Mechanism.** The document is enforced via a CI test at `src/tests/test_ai_governance_md.py` (added by §12.5) which validates:

- The file exists at the project root.
- All required top-level sections (§§1–15) are present by heading.
- Every workflow card matching the per-player evaluative pattern is named in §5.
- The `REG-01` provenance tag (`SEC-AUDIT-v1.12.0 REG-01`) is present and `TODO.md` is referenced.
- The **Next review** date in the frontmatter is parseable and is not more than 30 days stale at test run time.

The "30 days stale" grace period is deliberate: it lets the annual review slide by a month if other work intervenes, but not by a year. When the grace period expires the CI test fails; the maintainer's next branch cannot ship until the review is performed, the date is updated, and any new findings are incorporated.

**`CLAUDE.md` reminder.** A short rule under a new "AI Governance" section in the project `CLAUDE.md` instructs that any addition, modification or removal of a per-player evaluative ML system must be accompanied by an update to this document and to `src/tests/test_ai_governance_md.py`, and that the `ARCHITECTURE.md` Appendix D academic-references appendix must be updated in the same commit whenever a new methodology citation is introduced (enforced by `src/tests/test_architecture_md_appendix.py`). These two tests together form the "root-level governance documents are automatically reviewed" mechanism.

**Review log.** Significant updates are recorded below. Minor typographical corrections do not require a log entry.

| Date | Reviewer | Trigger | Outcome |
|---|---|---|---|
| 2026-04-14 | Karsten S. Nielsen | Initial creation; closes `REG-01`. | Baseline established; no high-risk classification under the current operating posture. |

---

## 15. References

### 15.1 Regulation (EU) 2024/1689

- **Canonical text.** Regulation (EU) 2024/1689 of the European Parliament and of the Council of 13 June 2024 laying down harmonised rules on artificial intelligence (Artificial Intelligence Act), *Official Journal of the European Union*, L series, 12 July 2024. EUR-Lex: <https://eur-lex.europa.eu/legal-content/EN/TXT/?uri=CELEX:32024R1689>.
- **Article and annex deep-links** (community-maintained reference implementation): <https://artificialintelligenceact.eu/>.
- **Articles cited in this document:** 3 (Definitions), 6 (Classification rules for high-risk AI systems), 9 (Risk management system), 10 (Data and data governance), 11 (Technical documentation), 14 (Human oversight), 15 (Accuracy, robustness and cybersecurity), 17 (Quality management system), 40 (Harmonised standards), 43 (Conformity assessment), 47 (EU declaration of conformity), 48 (CE marking), 49 (Registration), 72 (Post-market monitoring), 113 (Entry into force and application).
- **Annexes cited in this document:** III point 4 (Employment, workers management and access to self-employment) and IV (Technical documentation referred to in Article 11(1)).

### 15.2 Repository artifacts cited

- [`SECURITY.md`](SECURITY.md) — security audit, informational finding I-1 (no PII).
- [`NOTICE`](NOTICE) — third-party data, library, and mathematical-reference attributions.
- [`ARCHITECTURE.md`](ARCHITECTURE.md) — platform architecture, C4 model, risk register, Appendix D academic references.
- [`CLAUDE.md`](CLAUDE.md) — project instructions, Template Rules, UX Standards, Failure Investigation Protocol.
- [`workflow-cards/`](workflow-cards/) — 13 workflow cards cited individually in §5.
- [`docs/huggingface/`](docs/huggingface/) — HuggingFace Hub model and dataset cards.
- [`docs/research/adversarial-training.md`](docs/research/adversarial-training.md) — supporting research for systems 11 and 12.
- [`hf_taipy_app/src/pages/player_similarity.py`](hf_taipy_app/src/pages/player_similarity.py) — the dashboard page whose description documents the adversarial-debiasing methodology used by systems 11 and 12.
- [`src/tests/test_architecture_md_appendix.py`](src/tests/test_architecture_md_appendix.py) — the parallel enforcement mechanism for `ARCHITECTURE.md` Appendix D.
- [`src/tests/test_ai_governance_md.py`](src/tests/test_ai_governance_md.py) — the enforcement mechanism for this document (to be added per §12.5).

### 15.3 Academic references supporting the fairness analysis (§10)

- Ganin, Y., Ustinova, E., Ajakan, H., Germain, P., Larochelle, H., Laviolette, F., Marchand, M., & Lempitsky, V. (2016). **Domain-Adversarial Training of Neural Networks.** *Journal of Machine Learning Research*, 17, 1–35. <https://jmlr.org/papers/v17/15-239.html> — the gradient-reversal technique used by systems 11 and 12 for competition and team debiasing.

(Additional academic references for each methodology are in `ARCHITECTURE.md` Appendix D and in the `references:` block of each workflow card. This document intentionally does not duplicate that appendix.)
