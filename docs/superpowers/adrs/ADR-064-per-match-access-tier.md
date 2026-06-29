# ADR-064: Per-match HF redistribution restriction (`access_tier`)

| Field | Value |
|---|---|
| **Date** | 2026-06-29 |
| **Status** | Accepted |
| **Deciders** | Karsten (operator/data owner), lakehouse session, pining-for-the-data session |

## Context

HF redistribution restriction was **provider-level, all-or-nothing**: `RESTRICTED_HF_PROVIDERS = frozenset({"gradientsports"})` (`ingestion.hf_publish`). Every row of a "restricted" provider went to the private companion repo (ADR-049); every other provider's rows were public.

This broke when **SkillCorner gained mixed licensing**. SkillCorner is ingested from the pining-for-the-data API, and each match carries a per-match `visibility` field (`"public" | "private"`, pining canonical `pattern=r"^(public|private)$"`). Some SkillCorner matches are publicly redistributable (the ~10 public A-League matches); 98 Real Madrid matches are restricted. A provider can no longer be classified wholly public or wholly restricted — **the boundary is now per-match**. The owner pining token (already used for restricted GradientSports) returns the private matches, so the only thing holding restricted SkillCorner out of public HF was that it had not been ingested yet. A restricted match reaching a public HF repo — raw OR derived (embeddings) — is an **irreversible licensing breach** (HF caches/indexes). Licence rationale (operator): the data may be **stored/backed up** (local, cloud, a private HF repo) but may **not be shared** (a public repo). ADR-049 §Neutral/Future explicitly designed in the `access_tier` seam for exactly this; `split_restricted(df, column=…)` already carried the parameter.

Full design + three rounds of producer-side review: `docs/superpowers/specs/2026-06-29-per-match-hf-redistribution-restriction.md` (Rev 4) and `docs/superpowers/plans/2026-06-29-per-match-access-tier.md` (Rev 2.1).

## Decision

Introduce a first-class **`access_tier`** domain concept (`PUBLIC | RESTRICTED`), classified by a pure stdlib core `shared.access_tier.classify_access_tier(provider, visibility)` (`"private"`/unknown → RESTRICTED fail-safe; `"public"` → PUBLIC; `None` → provider default via `RESTRICTED_HF_PROVIDERS`). Ingestion stamps raw `visibility` + derived `access_tier` on bronze; it rides **per-row** through SPADL/AC/tracking/psxg → gold marts (direct stamp, never a publish-time join). `split_restricted` splits on `access_tier` (fail-safe: NULL/unknown → restricted). **Every** public-HF publisher (raw and derived) is wrapped by an enumerate-all, fail-closed leak guard (`ingestion.hf_leak_guard.assert_no_private_leak`) that refuses to publish any non-`public` row and fails the test suite for any publisher missing from its registry. Row-level datasets keep the ADR-049 split-to-both-repos (private rows → private `-restricted` companion); derived aggregates (football2vec) are rebuilt **public-only upstream** with input/output (vocabulary) assertions.

## Alternatives considered

| Option | Pros | Cons | Why rejected |
|---|---|---|---|
| A. Keep provider-level `RESTRICTED_HF_PROVIDERS` | Zero change | Cannot express a provider with both public + private matches | Mixed-license SkillCorner makes it incorrect — would leak or over-restrict |
| B. Drop-private for SkillCorner (no HF repo at all) | Simplest "never touch HF" | Loses the private backup; per-provider special-case | Operator: a **private** HF repo is permitted storage/backup; only a public repo is "sharing" → split-to-both is licence-clean |
| C. Resolve `access_tier` by joining `dim_matches` at publish | No per-row carry | Unmatched key → NULL → fail-safe-restricted → silently drops PUBLIC data (availability bug) | Per-row carry (ADR-016 passthrough) avoids the NULL-on-unmatched failure mode |
| D. **Per-match `access_tier`, carried per-row, fail-safe split + fail-closed leak guard** (chosen) | Correct per match; never leaks; every publisher guarded | Backfill + re-materialize needed; a new lockstep invariant | — |

## Consequences

### Positive

- Correct per-match boundary; a provider can publish public + private partitions in one run.
- **Fail-safe everywhere**: NULL/unknown tier → restricted at the split; the leak guard enumerates every publisher and fails closed (a new publisher with no registry entry fails CI); the public-repo guard IS the "do-not-share" licence enforcement.
- Derived-artifact leak closed (football2vec public-only upstream + vocabulary assertion — a private-only player id is an existence leak).
- The VAEP trainer gate (`policy-can-produce-restricted`) doubles as a token-misconfig canary.
- The `MatchInfo.visibility` required-no-default invariant + the cross-repo vocabulary contract test pin the producer↔consumer seam.

### Negative

- A one-time bronze backfill of historical rows + a scoped AC/SPADL/tracking re-materialize are required before the first publish (else the fail-safe blocks every publish / empties the public datasets).
- More surface: a per-row `access_tier` column threaded through ~15 ingestion/schema files, ~12 marts, and every publisher; the policy is a new lockstep invariant.
- Putting the pining **owner** token in Databricks extends the owner-tier trust boundary to the lakehouse (already true for GradientSports; recorded here).

### Neutral

- `RESTRICTED_HF_PROVIDERS` does not disappear — it relocates to the pure core as the NULL-fallback provider default (keeps GradientSports correct with no per-match data).
- ADR-049 (companion-repo pattern) is unchanged in mechanism; this ADR only changes the split **key** (provider → per-match `access_tier`) and adds the fail-closed guard.
- Per-match licensing requires pining to keep `visibility` aligned with the actual licence; a future non-pining SkillCorner ingestion path (e.g. Dropbox import) would re-open the classifier (default unknown-source → RESTRICTED).
