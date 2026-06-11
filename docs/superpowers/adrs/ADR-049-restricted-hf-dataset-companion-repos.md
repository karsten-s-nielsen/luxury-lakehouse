# ADR-049: Restricted-data HF publishing — permanent private companion repos

| Field | Value |
|---|---|
| **Date** | 2026-06-10 |
| **Status** | Accepted |
| **Deciders** | Karsten Nielsen, Claude |

## Context

Some provider data is computed in the lakehouse but may not be publicly redistributed (today:
GradientSports, pending license). The old mechanism was a SQL-side filter in the dataset
publisher (`WHERE data_source != 'gradientsports'`) — which the VAEP trainer then silently
inherited because it loads the published HF dataset: **Champions v10-and-earlier trained without
GS and nobody noticed**. Policy (user, 2026-06-10): restricted data stays OFF public HF datasets
but is 100% in training corpora; the restriction mechanics must be permanent infrastructure that
works even when NO data is currently restricted; and the design must extend to row-level splits
(a provider with both public and restricted feeds, e.g. a future restricted SkillCorner dataset).

## Decision

Every HF dataset that carries provider data gets a **permanent private companion repo** named
`<public-repo>-restricted` (org-members-only), with the identical partition layout. The split is
owned by `ingestion.hf_publish` — `RESTRICTED_HF_PROVIDERS` (single source of truth),
`split_restricted(df)` (the criterion lives ONLY here), `restricted_repo_id(repo)` (naming).
Publishers ensure BOTH repos on every run and publish each side — including a **sweep-only
publish** of the restricted repo when the set is empty (`delete_patterns=["**"]` removes
departed partitions; the same run's public publish carries them — migration is one
constant edit). Trainers import the same constant: set non-empty → restricted partitions
REQUIRED (fail-loud), set empty → skip gracefully; both repos' commit hashes are recorded in
MLflow for full corpus lineage.

## Alternatives considered

| Option | Pros | Cons | Why rejected |
|---|---|---|---|
| A. SQL-side filter in the publisher (status quo) | zero infra | trainers silently inherit the filter (the v10 corpus bug); restricted slice has NO versioned home | the bug this ADR fixes |
| B. Trainer pulls restricted rows from Databricks directly | no new repo | restricted corpus slice has no commit hash → unreproducible model lineage; second auth path (warehouse) in trainers; per-trainer reimplementation | provenance hole |
| C. HF *gated* repos (public card, per-user approval) | external collaborators possible | approval surface + repo existence is public; overkill for "org-only for now" | private is the conservative default; gating is a settings upgrade later, not a redesign |
| D. Private companion repos + shared split helpers (chosen) | one loading mechanism, full lineage, one-edit migration, machinery survives empty state | one extra repo per dataset | — |

## Consequences

### Positive
- The publish split and the training-corpus expectation **cannot drift** (one imported constant).
- Model lineage covers the restricted slice (`hf_restricted_dataset_commit` MLflow tag).
- Granting a provider permission is ONE edit (`RESTRICTED_HF_PROVIDERS`); the next publish
  migrates the partition publicly and sweeps the private repo, which remains standing.
- The `delete_patterns` fix also retired a latent publisher bug: hf_hub matches delete
  patterns against paths RELATIVE to `path_in_repo` (`"data/"`), so the historical
  `"data/*"` pattern matched nothing and silently no-opped — legacy raw Spark part-files
  survived inside statsbomb/wyscout partition dirs for months (any `*.parquet` glob
  double-counted them). The correct sweep pattern is `"**"`; `upload_folder` itself keeps
  files re-uploaded by the same run. (Amendment 2026-06-10: the first cut of this ADR
  shipped `"data/**"`, which no-ops for the same anchoring reason — caught on the first
  live publish and corrected to `"**"`.)

### Negative
- One more repo + card per dataset; private repos are invisible to public consumers (by design).
- Trainers MUST have an org-scoped HF token (already true for HF Jobs).

### Neutral / Future — row-level access tiers (designed-in, not implemented)
When one provider has both public and restricted data: ingestion modules stamp an `access_tier`
column on bronze at retrieval (default per source module; the restricted feed's module stamps
`'restricted'`), carried through SPADL → marts via the standard passthrough machinery (ADR-016 /
LL1; `result_source` is the template). `split_restricted`'s mask then becomes
`access_tier == 'restricted'` with the provider set as NULL-fallback — **call sites unchanged**.
Both repos may then hold partitions for the same provider; trainers already concat + dedup by
`action_value_id`.

## CLAUDE.md Amendment

A Project Conventions bullet mandates: new HF dataset publishers carrying provider data use
`split_restricted` + the companion-repo pattern; trainers consuming such datasets derive their
restricted expectation from `RESTRICTED_HF_PROVIDERS` and fail loud when expected partitions are
absent. Migrated in this change: `publish_spadl_vaep_hf.py` and `publish_action_context_hf.py`
(the latter pre-first-publish, so its very first publish is born split).
`publish_tracking_context_hf.py` remains legacy-SQL-gated pending its deprecation; the two-mode
guard in `src/tests/test_gradientsports_hf_exclusion.py` pins every GS-carrying publisher to
exactly one gating mode.

## Related
- **ADRs:** ADR-014 (HF card parity — the restricted repo's card rides the same mechanism),
  ADR-048 (the 4.22.0 campaign where the corpus bug surfaced)
- **Code:** `ingestion/hf_publish.py` (constants + helpers), `scripts/publish_spadl_vaep_hf.py`,
  `scripts/publish_action_context_hf.py`, `scripts/train_vaep_model_hf.py`
- **Cards:** `docs/huggingface/dataset-cards/spadl-vaep-action-values-restricted.md`,
  `docs/huggingface/dataset-cards/spadl-action-context-restricted.md`
- **Memory:** project_gs_hf_publishing_restriction
