# ADR-014: HuggingFace card inventory parity via a shared `hf_publish` helper

| Field | Value |
|---|---|
| **Date** | 2026-04-24 |
| **Status** | Accepted |
| **Deciders** | Karsten Skyt |

## Context

At the start of PR 4c the `luxury-lakehouse` HuggingFace org hosted 19 datasets, 17 models, and 4 Spaces, plus a `build-artifacts` wheel-hosting repo. The READMEs ("cards") attached to each of those repos were the primary consumer-facing documentation, but their production pipeline was chaotic:

- Some cards were pushed via PEP 723 scripts (`scripts/publish_*_hf.py`) that embedded the README content as Python string literals inside the publishing code — drift-prone the moment anyone edited either side.
- Some cards were pushed by Databricks-notebook cells (`notebooks/publish_datasets.py`, `notebooks/publish_obso_data.py`) that had their own inline `publish_dataset` helper, a second implementation of the "copy card + upload folder" pattern.
- Some cards lived in `docs/huggingface/dataset-cards/` and `docs/huggingface/model-cards/` but had never been pushed to HF; the on-Hub READMEs were older drafts pushed manually via the HF web UI.
- The org Space's README (`docs/huggingface/org-card.md`) had no automation at all — the operational note said "paste via web UI".
- Five "method" model cards (`pitch-control`, `defcon`, `off-ball-xt`, `obso-pausa-method`, `space-creation-method`) had no training script to ride on, so they had no push path at all.
- Critically: there was no test that checked parity between the on-Hub inventory and the in-repo card directory. Cards could silently drift, disappear, or point at HF repos that had been renamed or deleted.

Separately, the Kimball `match_id` → `match_key` migration (ADR-011) opened 90-day dual-column windows on three datasets (`spadl-vaep-action-values`, `statsbomb-shots-on-target`, `xg-shot-data`). Each window's `2026-07-22` sunset date needs to be visible on HF Hub the moment the migration commits land. The pre-PR-4c push pipeline could not guarantee that — a producer might run a data publish without touching the README, and consumers would read stale schema documentation for weeks.

The forcing function: PR 4c was scoped to close the PR 3 deferral on per-dataset README drift. During Phase 0 pre-flight we discovered the starting state was much worse than the plan assumed (16 existing in-repo cards the plan thought did not exist; two filename-vs-HF-repo-basename mismatches; the `publish_hf_org_card.py` the plan described as a refactor target did not exist). The user chose the expanded best-practice path.

## Decision

Every HF Hub artifact that carries a README under `luxury-lakehouse/` **must be documented by an in-repo markdown file whose basename equals the HF repo basename**, and every publisher **must push that file via a single shared helper** (`ingestion.hf_publish.upload_hf_readme`) as the final step of its publication sequence. The invariant is enforced in CI by a parity test that queries HF Hub and diffs against the on-disk card directory.

## Alternatives considered

| Option | Pros | Cons | Why rejected |
|---|---|---|---|
| A. Keep inline README strings in each publisher | Zero new code; each script self-contained | Drift between published content and in-repo markdown is the exact problem we are solving; no single place to audit what's on HF | Rejected — this is the state we are leaving |
| B. Markdown source only in `docs/`; per-script `shutil.copy2` at push time | Minimal abstraction; matches `notebooks/publish_datasets.py`'s existing pattern | Duplicates the staging-folder boilerplate across every publisher; no LF normalization; no sha256 return for auditing; error handling is ad-hoc per caller | Rejected — "drop-in helper" path is strictly better than ad-hoc copy-paste |
| C. **Chosen: shared helper + parity test + filename-basename invariant** | Single implementation of LF normalization, SHA-256 return, HfApi failure propagation, repo-type dispatch; one test enforces the whole inventory; wheel-aware path resolver works identically in dev and in the Databricks workflow runtime | ~40 files touched to wire every publisher; an ADR needed to establish the convention so future maintainers know not to add a new inline-string publisher | — |
| D. Monorepo of per-artifact publish_<name>.py PEP 723 scripts, one per HF repo | Maximum isolation; each repo owned by its own script | ~40 scripts for 36 artifacts; each would need to re-implement the same 20 lines of HF push glue; no enforcement against forgetting to add a new one | Rejected — multiplies the maintenance surface without solving the parity problem |
| E. Content in `docs/` + on-Hub cards published only on manual operator request | Zero automation needed | The entire class of "new HF repo published without a card" bugs remains; sunset dates (2026-07-22 in particular) land on HF at the operator's discretion, not the code's | Rejected — violates the "no silent drift" invariant |

## Consequences

### Positive

- **Zero-drift invariant**: `src/tests/test_hf_publish_parity.py` fails CI the moment a HF repo exists without an in-repo card, or an in-repo card has no corresponding HF repo. Both halves of the parity are enforced.
- **Single push convention**: every publisher (3 PEP 723 dataset scripts, 4 workflow-task data producers, 4 PEP 723 compute scripts, 7 PEP 723 training scripts, 2 Databricks notebooks, 1 org-card CLI) calls the same helper. Reviewers see one pattern in diffs.
- **Dual-column sunsets visible immediately**: the 2026-07-22 sunset blocks on `spadl-vaep-action-values`, `statsbomb-shots-on-target`, and `xg-shot-data` reach HF Hub on the next publish run — no separate manual step.
- **Wheel-aware path resolution** (`get_hf_card_path(name, kind=...)`) works both in source-tree dev and in Databricks workflow runtime (the wheel force-includes `docs/huggingface/` as a sibling of the `ingestion` package, path-preserving).
- **AD002 compliance**: the helper propagates `HfHubHTTPError` (no silent swallow); publishers that don't want to fail the whole workflow on a README push failure must explicitly opt into skip behaviour (only used by the UC-Volume workflow publishers when HF_TOKEN is absent, matching `upload_volume_to_hf_hub`'s existing no-token-skip behaviour).
- **Orphan push path**: `scripts/publish_hf_cards.py` handles the artifacts that have no payload publisher (org Space, five method cards, `football2vec-l2-harvest`). `--org` / `--orphans` / `--name --kind` cover every case.

### Negative

- **One-time wheel bump and consumer sync**: `pyproject.toml`'s force-include now lists three `docs/huggingface/*` entries alongside the existing `dbt_project/*` entries; wheel 0.3.13 → 0.3.14 propagated to 19 consumers by `bump_wheel.py`. Future `docs/huggingface/` additions do not require a wheel bump.
- **Filename-basename invariant creates upstream-rename coupling**: if the HF repo `luxury-lakehouse/xg-v2-model-set-encoder` is ever renamed, the in-repo card must be renamed in the same commit, and so must the repo-to-card alias map in `test_hf_publish_parity.py`. A historical-aliases dict in the parity test absorbs the cases where the HF repo name and the card filename already differ (e.g., `xg-model-statsbomb-wyscout` ↔ `xg-model-card.md`), but new divergences should be avoided.
- **Parity test is network-dependent**: it queries `HfApi.list_datasets` / `list_models` on every run. It skips gracefully on network errors, but adds ~1 s to CI when online. Acceptable for the safety the test provides.
- **Two notebooks (`publish_datasets.py`, `publish_obso_data.py`) still follow the older `shutil.copy2` pattern** rather than calling the Python helper — they're Databricks notebooks that can't trivially import from the `luxury-lakehouse` wheel without extra setup. The Phase 2 filename updates removed the most acute drift source; a future follow-up can migrate those cells to PEP 723 publishers.

### Neutral

- The helper lives in `src/ingestion/hf_publish.py` as a documentation-delivery peer to `src/ingestion/artifact_deploy.py` (ADR-012's weight-delivery helper). The two are structurally parallel and unify the HF producer-side delivery surface.
- `docs/huggingface/org-card.md` stays at the top level of `docs/huggingface/` (not moved into a `spaces/` subdir), matching the existing convention and the `scripts/publish_hf_cards.py --org` resolver's expectation.

## CLAUDE.md Amendment

CLAUDE.md Project Conventions gains one bullet naming this helper as the only HF-README push path, with the filename-basename invariant and the three orphan/override CLI modes (`--org`, `--orphans`, `--name`) called out. The rule is non-negotiable: no new inline-string README pushes, no ad-hoc `HfApi.upload_file(path_in_repo="README.md", ...)` calls outside the helper.

## Related

- **Commits:** PR 4c (single squash-merge; branch `kimball-pr4-hf-readme`).
- **Specs:** `docs/superpowers/specs/2026-04-23-kimball-pr4-action-values-plus-deferrals-design.md`
- **Plans:** `docs/superpowers/plans/2026-04-23-kimball-pr4-hf-readme.md` (amended 2026-04-24 with expanded-scope section at the top).
- **ADRs:** sibling of ADR-012 (training-to-production *weight* delivery); depends on ADR-011 (Kimball `match_key` — the 3 sunset-block datasets whose 2026-07-22 windows this ADR's helper makes visible).
- **Enforcement tests:** `src/tests/test_hf_publish.py` (helper logic + content invariants), `src/tests/test_hf_publish_parity.py` (HF-Hub ↔ in-repo inventory parity).
- **External references:** HuggingFace `HfApi.upload_file` documentation — `https://huggingface.co/docs/huggingface_hub/en/package_reference/hf_api`.
