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

## Amendment — 2026-06-30 (SkillCorner mixed-license + privacy-default hardening)

When the restricted SkillCorner Real Madrid games approached ingestion, two refinements landed (spec
`docs/superpowers/specs/2026-06-30-skillcorner-keeper-origin-rebuild-and-access-tier-completion.md`, reviews H1/P1):

1. **The no-signal default is now an ALLOWLIST, not a denylist (review P1).** The original
   `classify_access_tier(provider, None)` defaulted public unless the provider was in `RESTRICTED_HF_PROVIDERS` — so
   `skillcorner+None → PUBLIC` (a mixed-license leak shape once private SkillCorner exists) and, worse, *any unknown
   new provider → PUBLIC*. The core now defaults public **only** for `PUBLIC_BY_LICENSE_PROVIDERS = {statsbomb,
   wyscout, idsse, metrica}`; everything else — skillcorner/GS with no signal, AND any unknown provider — **fails safe
   to restricted**. A wrong-restrict is one line to fix; a wrong-public of private data is unrecoverable. Existing
   SkillCorner (the public A-League) is encoded **explicit-public** (`visibility='public'`, premise-asserted on
   competition 61) rather than relying on the now-restricted default.

2. **H1.3 publish guard — why per-row `visibility` is NOT threaded everywhere (name the fence not built).** A literal
   reading of "no SkillCorner public row without explicit `visibility='public'`" suggests threading `visibility`
   per-row through SPADL/AC bronze → marts → every publish frame (a ~15-file second column-migration mirroring
   `access_tier`). It is deliberately **not** built, because after change (1) **`access_tier` already encodes the
   per-row visibility decision**: a non-allowlisted provider can only reach `access_tier='public'` via an explicit
   `visibility='public'` (or the verified confirmed-public override), so the all-public leak-guard check already
   enforces "non-allowlisted public ⟹ confirmed public." The residual risk — a *stamp divergence*
   (`access_tier='public'` on a row whose true `visibility` is not public) — is enforced by **one policy at three
   points**: the classifier (`PUBLIC_BY_LICENSE_PROVIDERS`), the per-publish leak guard
   (`hf_leak_guard._assert_no_access_tier_visibility_divergence`, fires when a frame carries `visibility`), and the
   build-gating `dim_matches` dbt consistency test (`assert_access_tier_visibility_consistency.sql`, the source of
   truth where both columns coexist). The shared constant prevents drift
   (`test_access_tier_visibility_consistency_allowlist.py`). Out of scope by design: a provider **mis-tag** (a private
   row wrongly stamped with an allowlisted `data_source`) — that is upstream ingestion-integrity, not the publish
   guard.

## Amendment — 2026-07-02 (pre-H1 backfill + blocked-public-data guard)

The 2026-06-30 fail-safe-to-restricted default (`split_restricted`: `NULL/unknown → restricted`) is correct for
*new* data, but it exposed a **latent inverse defect on historical data**: every bronze tracking table
(`skillcorner_tracking`, `idsse_tracking`, `metrica_tracking`, `gradientsports_tracking`) was populated **before** H1
added tier stamping to the tracking writers, so every historical row carried `access_tier = NULL`. Fail-safe then
mislabels that NULL as `restricted` — **withholding public open-data tracking (A-League 9.6M rows, IDSSE 21.9M,
Metrica 430K) from the public HF datasets**. Fail-safe protects private data by restricting the unknown; it cannot
know that a *pre-existing* NULL is public. (GS's 270M NULL rows are the one case where fail-safe→restricted is the
correct outcome, so they are intentionally left NULL.)

Two changes close it:

1. **Data remediation (operator-applied):** `scripts/migrations/2026-07-02-backfill-tracking-access-tier.sql` stamps
   the historical tracking rows to mirror `classify_access_tier` — skillcorner from its per-match `visibility`,
   idsse/metrica → `public` (open-data), GS left NULL. Idempotent (`WHERE access_tier IS NULL`); must also be applied
   to prod bronze, which carries the same pre-H1 NULLs.

2. **Recurrence guard (daily dbt):** `dbt_project/tests/assert_tracking_access_tier_not_blocking_public.sql` — the
   **blocked-public mirror** of `assert_access_tier_visibility_consistency.sql` (which guards the leak direction).
   Asserts public tracking rows carry `access_tier='public'` (never NULL/restricted) for the open-data providers +
   skillcorner public-visibility matches, so a re-ingested/wiped public table left NULL — or a new public-by-license
   tracking provider ingested without stamping — fails the build. Shares the `public_by_license_providers` var (with a
   compile-time drift guard). Together the two dbt tests now bound `access_tier` from **both** sides: no public tier
   without public visibility (leak), and no public data left un-public (block).
