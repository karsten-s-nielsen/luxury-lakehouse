# Spec: Per-Match HF Redistribution Restriction (`access_tier`)

**Status:** Revision 4 — incorporates the pining-for-the-data **Round-2 evidence-backed review** (live
warehouse query of every mart these publishers read). Two must-fixes added, both proven against live
data: (1) `publish_pitch_control_tracking_hf.py` reads `fct_tracking_frames` which **carries SkillCorner
(9.6M rows / 10 matches)** with no split — raw restricted tracking → public HF; (2) `football2vec`
publishes a single public repo from **pre-aggregated, stochastically-trained** embeddings sourced from
`fct_action_values` (which carries SkillCorner 11,777 / 10 matches) — publish-time filtering can't fix
pre-mixed aggregates. The §6.7 publisher audit is now the live mart-membership table. (Rev 3 history:
the `private` value, owner-token posture / no credential gate, classification-before-data-flow ordering,
automated leak guard, split-publish-continues-keyed-on-access_tier. Round-1: all resolved.)

**Severity frame (operator):** GradientSports is effectively public (licence hygiene, not breach).
**Restricted SkillCorner (the 98 RM matches) is zero-tolerance — must not leak in any form, raw or
derived.** The spec is calibrated to that.
**Date:** 2026-06-29
**Author:** lakehouse session · **Reviewed by:** pining-for-the-data session (producer of the `visibility` signal)
**Implements:** [ADR-049](../adrs/ADR-049-restricted-hf-dataset-companion-repos.md) §"Neutral/Future — row-level access tiers" (the designed-in seam)
**Related:** ADR-016 (SPADL passthrough), ADR-018 (cross-table format contracts), ADR-054 (per-provider HF configs)

> **This spec warrants a new ADR on acceptance** (it introduces a cross-cutting domain concept +
> security boundary). Draft `ADR-0xx-per-match-access-tier` from this once the implementation lands.

---

## 1. Problem

HF redistribution restriction is currently **provider-level, all-or-nothing**:
`RESTRICTED_HF_PROVIDERS = frozenset({"gradientsports"})` (`src/ingestion/hf_publish.py:86`). Every row
of a "restricted" provider goes to the private companion repo; every other provider's rows are public.

This breaks now that **SkillCorner has mixed licensing**. SkillCorner is ingested from the
**pining-for-the-data** API (`src/ingestion/skillcorner_common.py:3,20`), and each match carries a
per-match **`visibility`** field (`MatchInfo.visibility: str`, `skillcorner_common.py:33`). Some
SkillCorner matches are publicly redistributable (the A-League open data); some are restricted (e.g.
98 Real Madrid matches). A provider can no longer be classified wholly public or wholly restricted —
**the redistribution boundary is now per-match**. The signal already arrives at ingestion but is
**discarded** (`skillcorner.py` fetches `MatchInfo` then calls `parse_match_json` without threading
`visibility`). GradientSports has the same shape (`gradientsports_common.py` `MatchInfo.visibility`).

## 2. Goal / Non-goals

**Goal:** Move the HF redistribution boundary from provider-level to **per-match**, driven by the
ingestion-time `visibility` property, implemented as a first-class domain concept (`access_tier`)
stamped at ingestion and carried per-row to the publishers — the seam ADR-049 designed in.

**Core requirement (operator-stated):** **No published-to-public artifact — raw OR derived — may
contain private data, unless that same data also appears in public data.** The invariant is about the
**public** artifact; private data is **not discarded** — wherever a restricted companion makes sense it
is published there.

- **Row-level dataset publishers (action-context, vaep, psxg): the ADR-049 split-publish continues
  exactly as it does today** — `split_restricted` → publish BOTH repos every run — only now keyed on
  `access_tier` instead of provider. Public rows → public repo; **private rows → the `-restricted`
  companion** (not dropped). This is the "when it makes sense" case and stays the default.
- **Derived/aggregate publishers (e.g. football2vec embeddings):** the **public** artifact is derived
  from public-tier rows only — a player in both public and private matches contributes their *public*
  rows (the "overlap with public" case) and nothing from their private rows. A `-restricted` companion
  is built **only when it makes sense** for that artifact (§6.7); where a split is not meaningful, the
  public artifact simply excludes the private contribution.

So the boundary generalizes to **all** public-HF publishers, but the *mechanism* per publisher is
"split to both repos where it makes sense (the datasets), else public-tier-only (some derived)".

**Non-goals:**
- Changing which repos exist (the public + `-restricted` companion pattern is unchanged).
- Changing trainer dual-repo concat/dedup mechanics (already row-level by `action_value_id`).
- Re-architecting dim_matches / Kimball keying.
- Any Databricks/Lakebase-side access control (internal analytics already sees all providers; this
  spec is **only** about what leaves the building to public HF).
- Publishing raw **player reference data** to HF (pining `/players` carries its own `visibility`) — the
  lakehouse does not do this and won't here. (Derived player artifacts ARE in scope — see §6.7.)

## 3. Current state (what the seam already gives us)

ADR-049 §Neutral/Future (lines 62–69) anticipated this exactly: stamp `access_tier` at ingestion,
carry through SPADL→marts via the ADR-016 passthrough, `split_restricted`'s mask becomes
`access_tier == 'restricted'` with the provider set as NULL-fallback, **call sites unchanged**.

Two load-bearing facts that keep this low-risk:
1. **`split_restricted(df, column="data_source")`** (`hf_publish.py:94`) already takes a `column` param
   and is tested with a custom column. The split *mechanism* (disjoint + complete partition) is
   unchanged — only which column it reads.
2. **Publishers call the helper, not the constant** — the 3 ADR-049 publishers never branch on
   provider names, so the publisher diff is one argument.

## 4. Domain model (hexagonal core)

The restriction decision is a **domain policy**. Model it as a pure, dependency-free core that the
adapters (ingestion stamp, publisher split, trainer gate) depend on.

```
# src/shared/access_tier.py  (stdlib only — like src/shared/identifiers.py)

class AccessTier(str, Enum):
    PUBLIC = "public"
    RESTRICTED = "restricted"

# The provider-default set lives HERE (NOT hf_publish.py) — see §6.1 / D5. hf_publish.py imports it.
RESTRICTED_HF_PROVIDERS: frozenset[str] = frozenset({"gradientsports"})

def classify_access_tier(*, provider: str, visibility: str | None) -> AccessTier:
    """SINGLE source of truth for the public/restricted policy. Pure: no Spark/HF/I-O.

    Authoritative mapping (pining-for-the-data canonical model — visibility ∈ {"public","private"}):
        "private"            -> RESTRICTED      # the positive trigger; test against the LITERAL value
        "public"             -> PUBLIC
        None (no feed)       -> RESTRICTED if provider in RESTRICTED_HF_PROVIDERS else PUBLIC
        anything else        -> RESTRICTED      # fail-safe (D1) + caller increments a loud counter
    """
```

**Critical correctness note (review A1):** the pining API value is **`"private"`, never `"restricted"`**
(`pining canonical models.py:60`, `pattern=r"^(public|private)$"`). Encode the positive mapping
`"private" → RESTRICTED` explicitly and **unit-test it against the literal API values**. Do NOT code
the trigger as `visibility == "restricted"` — the real value `"private"` would fall into the unknown
branch (safe only while D1 fail-safe holds, a latent leak the moment anyone "tidies" the unknown
branch to PUBLIC).

Why a pure enum + function (vs. a bare boolean or a provider-set check): testable without infra (TDD),
one place to evolve the policy, and `access_tier` rows are self-documenting. `RESTRICTED_HF_PROVIDERS`
does not disappear — it becomes the **provider-default input** (the NULL-fallback), keeping
GradientSports correct with zero per-match data.

## 5. Data flow (ingestion → publish)

```
pining-for-the-data API ──(MatchInfo.visibility: "public"|"private")──┐
                                                                      ▼
                                       classify_access_tier(provider, visibility)
                                                                      ▼
   bronze.<provider>_matches.{visibility, access_tier}   (persist BOTH — raw input + derived, C1)
                                                                      ▼  (ADR-016 / LL1 passthrough)
                       bronze.spadl_actions.access_tier / spadl_action_context.access_tier
                                                                      ▼  (dbt staging passthrough)
              dev_gold.fct_action_values / fct_action_context / fct_shot_psxg  (access_tier per-row)
                                                                      ▼
                       split_restricted(df, column="access_tier")  →  (public_df, restricted_df)
                                                                      ▼  + every-run leak assertion (C3)
                        <repo>  +  <repo>-restricted   (unchanged ADR-049 dual publish)
```

**Carry per-row (D3, confirmed):** carry `access_tier` per-row through SPADL→marts via the ADR-016
passthrough (the publishers already `SELECT *`; the column rides along — no publish-time join, no
NULL-on-unmatched-key failure mode). `dim_matches` also gets `access_tier` **and** raw `visibility`
(C1) as the dimensional reference, but the per-row carry is the publish path.

## 6. Detailed changes by layer

### 6.1 Domain core (new)
- `src/shared/access_tier.py`: `AccessTier` enum + `classify_access_tier` + `RESTRICTED_HF_PROVIDERS`.
  Stdlib only.
- **D5 RESOLVED (review C4 — required, not a choice):** `RESTRICTED_HF_PROVIDERS` **must relocate**
  into the pure core. If it stayed in `hf_publish.py` (which imports pandas/HF), the stdlib-only core
  would have to import `hf_publish.py` → violates the `src/shared/` zero-dep rule and inverts the
  dependency into a cycle (publisher → core → publisher). `hf_publish.py` imports the set *from* the
  core. (Keep a re-export alias in `hf_publish.py` if needed to avoid churn at existing import sites.)

### 6.2 Ingestion stamp (the seam)
- **SkillCorner** (`skillcorner.py` + `skillcorner_matches.py`): thread `MatchInfo.visibility`
  through `fetch_match_list → parse_match_json → bronze.skillcorner_matches`; persist **raw
  `visibility`** AND stamp `access_tier = classify_access_tier("skillcorner", visibility)` (C1).
- **GradientSports** (`gradientsports.py` + `gradientsports_metadata.py`): same — stamp from its own
  `visibility` (D7 RESOLVED: uniform path, provider-default as fallback).
- **SPADL/AC/psxg bronze writers**: read the per-match tier and stamp each action/frame-derived row
  → new `access_tier` column on `bronze.spadl_actions`, `bronze.spadl_action_context`, and the psxg
  shots bronze.
- **No-`visibility` providers** (StatsBomb, Wyscout, IDSSE, Metrica): `visibility=None` → classifier
  returns the provider default (PUBLIC). Stamp the default; no other change.
- **A3 (immutability):** pining forbids in-place re-tiering (`_check_no_tier_mixing` raises on a flip),
  so a match's `visibility` is immutable. On re-ingest, **assert** the stored `visibility` for a
  `match_id` did not change (cheap regression guard; surfaces any producer-side violation loudly).

### 6.3 SPADL / AC schema passthrough (ADR-016)
- Add `access_tier` to `analytics.action_context.schema.RESULT_COLUMNS` + `ACTION_CONTEXT_DDL`, the
  SPADL `_SPADL_SCHEMA` / `_VAEP_SCHEMA`, and the applyInPandas StructTypes — the parity-tested
  passthrough discipline (`test_spadl_vaep_writer_parity.py`; closes the LL1 silent-drop class). It is
  a provider-native passthrough → canonical name `access_tier` (no `<provider>_` prefix).
- Bronze migrations: `ALTER TABLE … ADD COLUMNS (visibility STRING, access_tier STRING)` per affected
  bronze table (operator-applied per the convention).

### 6.4 dim_matches + dbt marts
- `dim_matches.sql`: add `access_tier` + raw `visibility` as per-match attributes (aggregate from the
  bronze match-info rows). Add to `_marts__models.yml` contract.
- Gold marts that feed a restricted-aware publisher carry `access_tier` per-row via the staging
  passthrough; add to each mart SQL + contract: `fct_action_values`, `fct_action_context`,
  `fct_shot_psxg`, **`fct_tracking_frames`** (Rev 4 — pitch-control source), and the per-match
  **`fct_player_embeddings`** (the career/season aggregates need an upstream public-only build, §6.8).

### 6.5 Row-level dataset publishers (split-publish, keyed on `access_tier`)
- `publish_spadl_vaep_hf.py`, `publish_action_context_hf.py`, `publish_psxg_shots_hf.py`:
  `split_restricted(df)` → `split_restricted(df, column="access_tier")`. Both repos still publish
  every run; fail-loud gate keys per D4.
- **`publish_pitch_control_tracking_hf.py` — ADDED in Rev 4 (Round-2 must-fix, HIGHEST severity).**
  It reads `FROM dev_gold.fct_tracking_frames` (`:58`) with **no split, no filter**, and that mart
  **carries SkillCorner today** (live: 9,606,256 rows / 10 matches). The moment the 98 restricted RM
  matches are ingested, **raw restricted positional tracking frames go straight to a public HF repo.**
  It is row-level → it gets the **identical** `split_restricted(df, column="access_tier")` + dual-repo
  treatment as the three datasets (and needs `access_tier` carried onto `fct_tracking_frames` — §6.4).
  This was missed in Rev 3; it is the single highest-severity path in the publisher surface.
- **C5 — the dual publisher (must fix):** `src/ingestion/publish_spadl_vaep_hf.py` partitions by
  `data_source` with **no** `split_restricted` — a latent public-leak path the one-arg diff does NOT
  touch. **Delete it** (confirm it is unwired/dead) **or migrate it** to the split, in this change. Add
  a guard test: **no publisher partitions on `data_source` for the restriction decision** (the split
  must key on `access_tier`).
- `publish_tracking_context_hf.py` (legacy `WHERE data_source != 'gradientsports'`): **D6 RESOLVED —
  migrate now.** Leaving a second restriction mechanism alive while the rest goes per-match is the
  split-brain that leaks a restricted SkillCorner match through the un-migrated path. If it is truly
  being deprecated, deprecate it in this change rather than deferring.

### 6.6 Trainer gate (D4 RESOLVED — "policy-can-produce-restricted")
- `train_vaep_model_hf.py`: gate "restricted repo required" on **whether the policy can produce
  RESTRICTED** (`RESTRICTED_HF_PROVIDERS` non-empty or any feed can emit `visibility=private`),
  NOT on observed-rows-exist. **C6 canary:** this makes the trainer a token-misconfig detector — if the
  owner token regresses to public, restricted rows vanish, the restricted repo goes empty, and the
  training run fails loud. Record this canary property so nobody softens the gate to observed-rows.
  Dual-repo concat + dedup by `action_value_id` unchanged.

### 6.7 The full publisher surface (live audit — Round-2 evidence)
Provider membership of every mart these publishers read, queried live (`dev_gold`, 2026-06-29). This
**is** the completed §6.7 audit; it replaces Rev 3's "audit every publisher" prose:

| Mart | SkillCorner present? | Publisher | Today | Rev 4 treatment |
|---|---|---|---|---|
| `fct_action_context` | (split-protected) | `action_context` | split | `column="access_tier"` |
| `fct_shot_psxg` | (split-protected) | `psxg` | split | `column="access_tier"` |
| `fct_action_values` | **yes** (11,777 / 10 mt) | `spadl_vaep` (scripts) | split | `column="access_tier"` |
| `fct_action_values` | **yes** | `src/ingestion/spadl_vaep` (C5 twin) | **no split** | resolve to one canonical publisher (§6.5) |
| `fct_action_values` | **yes** | `football2vec` (derived) | **single public repo, no split** | §6.8 — upstream public-only + assertions |
| `fct_tracking_frames` | **yes** (9.6M / 10 mt) | `pitch_control_tracking` | **no split** | **add split (§6.5) — highest severity** |
| `fct_passes` | no (SB/WS/IDSSE/Metrica) | `line_breaking_passes` | no split | safe-by-absence → make tier-aware + fail-closed |
| `fct_shots` | no (SB/WS) | `xg_shots` | no split | safe-by-absence → make tier-aware + fail-closed |
| StatsBomb/IDSSE-only | no | `freeze_frame`, `obso_pausa`, `shots_on_target` | n/a | confirm; fail-closed guard still applies |

Key facts: SkillCorner is exactly **10 public A-League matches** in both `fct_action_values` and
`fct_tracking_frames` today; the 98 restricted RM matches are **not ingested yet** — so nothing leaks
*now*, but for `pitch_control_tracking` + `football2vec` the protection is *absence*, not a gate.

**"Safe-by-absence" is not safe enough.** `line_breaking_passes` / `xg_shots` carry no SkillCorner
today, but that can silently become a leak if a future mart change pulls it in. Make them **tier-aware
and fail-closed**: each publisher asserts its source contains no `access_tier != 'public'` row (or
splits if one ever appears). The point is the **leak guard enumerates EVERY public publisher and fails
closed** (§9.7) — it must not be a list of "the ones we remembered to split."

### 6.8 football2vec embeddings — pre-aggregated + stochastic (Round-2 must-fix)
`publish_football2vec_embeddings_hf.py` publishes a **single public repo**
(`football2vec-player-embeddings`) — **no companion, no split, no filter** — from
`fct_player_embeddings{,_career,_season}` (`:52,60,66`), with training reading `fct_action_values`
unfiltered. Two structural problems mean a **publish-time row filter cannot fix it**:

1. **Pre-aggregation contamination.** The career/season vectors are aggregated over *all* of a player's
   matches at mart-build time — by publish time a private contribution is baked into the stored average
   and cannot be un-mixed. Fix must be **upstream**: build the public career/season aggregate from
   `access_tier='public'` rows only (publisher re-aggregates from the per-match grain, or the dbt mart
   produces a public-only aggregate). The *per-match* `fct_player_embeddings` IS row-level → splits
   normally (§6.5-style); only career/season are the problem.
2. **Co-occurrence + stochastic.** A public player's vector is shaped by the other entities in the
   training corpus, so the **corpus** must be public-only before training (filter `fct_action_values`
   to `access_tier='public'`), not just the output. And training is **stochastic + unseeded**
   (`DataLoader(shuffle=True)`, `torch.randperm`, no `manual_seed`) → not byte-reproducible → the
   differential-recompute test (§9.8) is unavailable. Substitute two assertions: **(a) input** — the
   materialized training/aggregation input had **zero** `access_tier != 'public'` rows; **(b) output**
   — the published player vocabulary ⊆ players with ≥1 public row, so a **private-only player is
   entirely absent** (their ID as a key is itself an existence leak).
   **Default fail-closed:** do NOT publish public career/season embeddings until provably
   public-recomputed; ship the per-match (split) embeddings meanwhile. **Recommended regardless:** seed
   the model — restores reproducibility and lets §9.8's differential test apply.

The open question is only whether to build a **`-restricted` embedding companion** (trained on
restricted data for an owner-side consumer). **Default: don't (YAGNI)** until a specific consumer needs
it; the *public* embedding is fully resolved by public-only sourcing.

## 7. Security boundary + credentials (review A2 — RESOLVED: token already owner-tier)

The pining API is **two-tier**: a **public** bearer token gets restricted matches filtered out + a
uniform **404** on their artifacts; only the **owner** token returns `visibility=private` entries.

**RESOLVED (operator):** the Databricks `pining/token` secret is the **same owner token already used to
load GradientSports**. It is already owner-tier (the restricted-GS pull proves it); **no credential
change is needed** for restricted SkillCorner to ingest. (The term "restricted" was ours; the API value
was `visibility=private` all along.)

Consequences:
- **The trust boundary is already established + accepted.** The lakehouse already holds the owner token
  for restricted GS; one credential unlocks all restricted providers (GS WC2022 + SkillCorner RM).
  Nothing new to grant — just record the existing posture in the ADR.
- **There is NO credential safety gate** (this changes the rollout vs. Rev 2). Because the owner token
  already returns `visibility=private` SkillCorner matches, the only thing keeping restricted SkillCorner
  out of the lakehouse *today* is that those matches have not yet been ingested. The protection is
  entirely data-flow side (§8) and MUST be in place **before** the next SkillCorner ingestion pulls them.
- **Immediate operational hold:** until the split + leak guard are live (§8 steps 1–6), do **not** run
  SkillCorner ingestion of the new private matches, and do **not** run the SkillCorner-carrying HF
  publishes against any bronze that might already hold a private match. (The dev daily job is
  schedule-paused, so this is a hold-on-manual-trigger, not a race against cron — but it is the active
  guardrail until the code lands.)

## 8. Rollout ordering — CLASSIFICATION BEFORE DATA FLOW (review B — CRITICAL)

**The leak hazard:** today `RESTRICTED_HF_PROVIDERS = {"gradientsports"}`, so every SkillCorner row
routes to the **public** repo. The owner token is **already configured** (§7), so it already returns
the 98 private Real Madrid matches — the *only* thing holding them out is that the lakehouse has not
ingested them yet. The moment SkillCorner ingestion next runs, those matches flow into `skillcorner`
bronze → marts → and the next publish ships them to the **public** HF repo. That is the irreversible
breach (§12). The split + leak guard must be live and verified **before** the private matches are
allowed into the data flow — there is no credential to gate on (§7).

**Mandatory order:**
1. Domain core + truth-table tests (`access_tier.py`) — no behavior change.
2. Ingestion stamp (SkillCorner + GS) + bronze `visibility`/`access_tier` columns + migrations.
3. SPADL/AC/psxg **+ `fct_tracking_frames`** schema passthrough + parity tests + migrations.
4. dim_matches + mart columns + contracts (incl. the public-only career/season embedding aggregate, §6.8).
5. Publisher work: the row-level split swap to `access_tier` for all four — vaep, action_context, psxg,
   **and `pitch_control_tracking` (D9, highest severity)** — plus C5 dual-publisher resolution, D6
   tracking_context migration, **football2vec public-only upstream + assertions (D10)**, the
   `line_breaking`/`xg_shots` fail-closed (D11), the trainer gate, and the **enumerate-every-publisher
   fail-closed leak guard** (§9.7) + test updates.
6. **Verify** the public repo cannot contain a private contribution (C3 guard green on current
   all-public-SkillCorner data; a synthetic `access_tier='private→restricted'` row must NOT appear in any
   public artifact — row-level or derived).
7. **ONLY THEN** allow the private data to flow: run SkillCorner ingestion (the owner token already
   returns the private matches) + scoped AC/SPADL re-materialize → the private RM matches now carry
   `access_tier='restricted'` end-to-end.
8. Republish HF; confirm a known private match lands **only** in `-restricted` and a known public match
   **only** in public; the derived publishers (embeddings) exclude the private contribution.
9. (Optional) retire `RESTRICTED_HF_PROVIDERS` as the *primary* gate once all rows carry `access_tier`
   (keep as provider-default fallback).

**Until step 7, hold SkillCorner private-match ingestion and the SkillCorner-carrying publishes** (§7
operational hold). The dev daily job is schedule-paused, so this is a hold-on-manual-trigger.

## 9. TDD / test plan

Pure-first, hexagonal:
1. **Domain (no infra):** `test_access_tier.py` — `classify_access_tier` truth table against the
   **literal** pining values (`"public"`, `"private"`), `None` → provider-default, unknown →
   fail-safe-RESTRICTED. The policy's executable spec.
2. **Cross-repo contract (review C2 — the real e2e gap):** a boundary test that the pining
   `/skillcorner/matches` vocabulary is only ever `{"public","private"}` — live or a recorded VCR/
   fixture of a real response — and that the classifier has an explicit mapping for each; an unrecognized
   value **fails loud**. Mirrors pining's producer-side schema test on the consumer side. This is the
   seam that catches the producer changing the value set (the failure mode every other test misses).
   **Cadence (review minor):** the live pining check needs the owner token + network → env-gated
   (`skipif`) scheduled/on-demand, NOT every PR. The hermetic classifier unit test (item 1) + a recorded
   fixture run every PR; the live contract runs on schedule.
3. **Split:** extend `test_hf_publish.py::TestRestrictedPublishing` — split on `access_tier` disjoint +
   complete; **one provider in BOTH partitions** (the new capability: public + restricted SkillCorner in
   one frame); NULL/unknown tier routes to restricted (fail-safe, D1).
4. **Schema passthrough parity:** extend `test_spadl_vaep_writer_parity.py` + AC schema-parity tests —
   `access_tier` present in RESULT_COLUMNS/DDL/StructTypes.
5. **Boundary:** SkillCorner/GS ingestion test that `visibility` survives `fetch → parse → bronze`,
   is persisted raw, and is classified; the A3 immutability assertion on re-ingest.
6. **Publisher contract:** `test_gradientsports_hf_exclusion.py` / `test_hf_publish_parity.py` — ADR-049
   publishers split on `access_tier`, no SQL provider filter, `delete_patterns=["**"]`, both cards; plus
   the C5 guard (**no publisher partitions on `data_source` for restriction**).
7. **Automated leak assertion (review C3 — the most important guarantee):** a **post-publish, every-run**
   assertion that the public artifact contains **no** private contribution, **enumerated over EVERY public
   publisher and fail-closed** (Round-2): a registry of all `scripts/publish_*_hf.py` + their source mart,
   each asserted to publish no `access_tier != 'public'` row (row-level) or to have built from a
   public-only source (derived). A new publisher with no entry **fails the test** (so the guard can never
   silently omit one — the §6.7 "safe-by-absence" publishers are covered too). The symmetric twin of
   pining's `verify_skillcorner_realmadrid_load.py` — the producer verifies it can't *serve* a leak; the
   consumer must verify it can't *publish* one. **Not** a manual step.
8. **Differential-recompute invariant (deterministic derived artifacts):** the rigorous, testable form of
   the operator requirement — **delete all restricted rows from the source, recompute, assert the public
   artifact is byte-identical.** One e2e test that subsumes "filter applied" + "overlap with public." Use
   it wherever the artifact is deterministic. **Stochastic fallback (football2vec, §6.8):** since the
   embedding training is unseeded/non-reproducible, substitute the two assertions — (a) the materialized
   training/aggregation input had **zero** `access_tier != 'public'` rows; (b) the published vocabulary ⊆
   players with ≥1 public row (a private-only player's ID is itself an existence leak). (Seeding the model
   would restore reproducibility and let this differential test apply directly.)

## 10. Observability (review C7)

Every publish run logs **per-tier row counts published to each repo** (public N, restricted M per
provider) at INFO; **alert (ERROR) when restricted-count == 0 while the policy expects restricted**
(backstops the C3/C6 leak + token-misconfig detectors). Per the CLAUDE.md telemetry rule, the alert is
ERROR-level, not warning.

## 11. Decisions (resolved — producer-side review + operator answers)

| # | Decision | Resolution |
|---|----------|-----------|
| D1 | Fail-safe default | **Fail-safe-to-RESTRICTED** at the split + a loud publish-time NULL/unknown counter that refuses to publish. Non-negotiable. |
| D2 | `visibility`→tier mapping | Values are `{"public","private"}`. `private→RESTRICTED`, `public→PUBLIC`, `None→provider-default`, unknown→fail-safe-RESTRICTED. Test against literal values. |
| D3 | Carry per-row vs join | **Carry per-row** via ADR-016 passthrough; keep `dim_matches.{access_tier,visibility}` as dimensional reference. |
| D4 | Trainer gate | **Policy-can-produce-restricted** (preserves the C6 token-misconfig canary). |
| D5 | Policy home | **Relocate `RESTRICTED_HF_PROVIDERS` into `access_tier.py`** (required — avoids the zero-dep violation + import cycle). |
| D6 | `publish_tracking_context_hf.py` | **Migrate now** (don't leave a second restriction mechanism alive). |
| D7 | GradientSports | **Stamp GS from its own `visibility`** (uniform path), provider-default as fallback. |
| D8 | Per-publisher mechanism (operator) | **Public artifact never contains private data, but private data is not dropped where a companion makes sense.** Row-level datasets keep the ADR-049 split-publish (both repos, keyed on `access_tier`) — private rows → `-restricted` companion. Derived artifacts (embeddings) source public-tier only, `-restricted` companion only where meaningful. §6.7. |
| Token | `pining/token` tier (operator) | **Already the owner token** (same one loading restricted GS). No credential change; trust boundary already established. The gate is data-flow, not credentials (§7/§8). |
| D9 | `pitch_control_tracking` (Round-2) | **Add to the row-level split** (§6.5) — it reads `fct_tracking_frames` which carries SkillCorner; highest severity. Needs `access_tier` on `fct_tracking_frames`. |
| D10 | football2vec (Round-2) | **Public-only UPSTREAM** sourcing (career/season are pre-mixed; publish-time filter can't fix) + input/output assertions (stochastic) + fail-closed until public-recomputed (§6.8). Public embedding is resolved by public-only sourcing; a `-restricted` companion is **YAGNI** until an owner-side consumer needs it. |
| D11 | Leak guard scope (Round-2) | The guard **enumerates EVERY public publisher and fails closed** (§9.7); `line_breaking`/`xg_shots` made tier-aware so safe-by-absence can't silently regress. |
| C5 | dual `publish_spadl_vaep_hf.py` | Confirm the `src/ingestion/` twin is **not job-wired before deleting** (it reads a SkillCorner-carrying mart with no split — a real path if it ever runs); resolve to one canonical publisher. |

## 12. Risks

- **Leak (Critical):** a private SkillCorner match (or its derived contribution) reaching a public repo
  is an irreversible licensing breach (HF caches/indexes). The owner token is already live, so the only
  gate is data-flow. Mitigated by: D1 fail-safe-to-restricted, the every-run leak guard that **enumerates
  every publisher and fails closed** (§9.7), classification-**before-data-flow** ordering (§8), and the
  §8.6/8.8 verify before/after allowing private data to ingest. **Round-2 closed two real paths**:
  `pitch_control_tracking` (raw restricted frames — now split, §6.5) and `football2vec` (pre-mixed
  embeddings — now public-only upstream + input/output assertions, §6.8); `line_breaking`/`xg_shots` made
  fail-closed so their current safe-by-absence can't silently regress.
- **Silent-drop** of the new column through the SPADL/AC writers (LL1 class) — mitigated by parity tests (§9.4).
- **Token misconfig** (public token where owner expected) → feature silently inert — mitigated by the
  C6 trainer canary + C7 restricted-count-zero alert.
- **Producer vocabulary drift** (pining adds a value) — mitigated by the C2 cross-repo contract test.
- **Derived-data leak (flagged, unresolved):** football2vec embeddings trained on now-restricted players
  (§2 non-goal follow-on) — a licensing question for the data-owning sessions before any embedding republish.
```
