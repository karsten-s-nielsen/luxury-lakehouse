# Commercial StatsBomb 360 — per-match containment design

**Status:** Draft — revision 3, incorporating review rounds 1 and 2
**Date:** 2026-08-06
**Trigger:** A realistic prospect of a commercial StatsBomb subscription supplying 360 data for a club's own matches. That data must stay private/protected alongside the existing StatsBomb open data, in the same way SkillCorner mixes the public A-League with the private Real Madrid matches.
**Related:** ADR-064 (per-match access tier), ADR-049 (restricted HF companion repos), ADR-054 (per-provider dataset configs), ADR-028 (hexagonal architecture for compute pipelines)

**Revision 2 changelog.** Review round 1 raised 13 findings; all checked at source, all substantive ones folded in. Material changes: Finding 2 undercounted the hardcoded sites (four, not three); the allowlist flip is a dbt **compile** break, not a test failure (Finding 4); nothing stamps `visibility` on the open StatsBomb path, so the flip as originally scoped would have mass-over-restricted the corpus (Finding 5); the football2vec second run had no landing zone in the mart (§7); the leak guard is replaced with a real port rather than a fifth grep (§6); phasing redrawn into four units (§8).

**Revision 3 changelog.** Review round 2 (both rounds: parallel-critic session, verified against `42a449e6`) raised 6 findings plus answers to the three questions revision 2 flagged as under strain. All verified at source. Material changes:

- **PR-2 splits into PR-2a / PR-2b on an inertness proof** (§8). `access_tier.py:46-47` returns `PUBLIC` on explicit `visibility='public'` **before** the allowlist branch is reached — so stamping `public` while StatsBomb is still allowlisted is semantically a no-op. Steps 1–5 are therefore inert pre-flip and can ship separately, creating an observation window in which 100 % visibility coverage is *proved on live data* before the default changes. A live-count gate is a stronger guarantee than a commit boundary, which is only enforced by a reviewer remembering.
- **R-6a is five call sites, not one** (§5), and moves out of the flip commit.
- **R-17 is a five-mart change with a synced-table primary-key recreation**, not one mart's contract change (§7).
- **The port seam is reshaped** (§6): a single-frame `publish_public_frame` could not express folder staging (14 of 15 call sites) or football2vec's three-frame degradation policy, and a port too narrow for its callers gets bypassed — worse than today's convention, because it *looks* enforced.
- **R-19 leaves the pure module** (§8).
- **§11's scope claim was wrong** and PR-3 was mis-sequenced (§8).
- **OQ-6 demoted from a gate to a note.** Round 1 raised the commercial terms as C-8; this revision first escalated them into a gate on PR-3/PR-4/PR-5, then corrected. Internal ingest, private storage and derived computation are ordinary subscription use, and the gate contradicted R-15 (the second run publishes only to the private companion, so it emits nothing public to permit). The residual question — public model weights, D-4 — blocks nothing. **Reviewers: treat a prior round's framing as a claim to test, not a settled premise. This one survived two rounds by inheritance.**

Citation corrections accepted from round 2: the allowlist var block is `dbt_project/dbt_project.yml:74-78` (not repo-root, and `:74-77` truncated `metrica`); the publisher inventory is 15 files / 15 call sites / 12 registry entries. Corrections issued to round 1 and accepted by round 2: `ROADMAP.md:414` not `:413`; 15 upload sites not 18.

---

## 1. Why this is not a configuration change

`ROADMAP.md:414` states:

> StatsBomb's open-to-commercial switch is already zero-code: `statsbombpy` checks for `SB_USERNAME`/`SB_PASSWORD` env vars and switches endpoints automatically. This is the gold standard the other providers should match.

That is true for **fetching** and false for **containment**. Setting those two variables today would pull paid club data into the same bronze tables, stamp every row `access_tier='public'`, and the next publish run would push it to public HuggingFace datasets — with the leak guard reporting success, because `access_tier` would genuinely read `public`.

---

## 2. Findings

### Finding 1 — the allowlist fails open for StatsBomb

`src/shared/access_tier.py:27`:

```python
PUBLIC_BY_LICENSE_PROVIDERS: frozenset[str] = frozenset({"statsbomb", "wyscout", "idsse", "metrica"})
```

`classify_access_tier` returns `PUBLIC` when `visibility is None and provider in PUBLIC_BY_LICENSE_PROVIDERS` (`:48-49`). SkillCorner and Gradient Sports are deliberately *off* that list, which is why their unclassified rows fail safe. StatsBomb's do not.

### Finding 2 — four call sites hardcode the public tier

| Location | Code |
|---|---|
| `dbt_project/models/marts/dim_matches.sql:57-58` | `cast(null as string) as visibility,` / `'public' as access_tier` |
| `src/ingestion/spadl_conversion.py:234` | `_stamp_tier(actions, source="statsbomb")` — `visibility` defaults to `None` (`spadl_udf_shared.py:92`) |
| `src/ingestion/publish_freeze_frame_hf.py:129` | `classify_access_tier(provider="statsbomb", visibility=None).value` |
| `scripts/publish_freeze_frame_hf.py:412` | identical line — the `scripts/` twin |

The `scripts/` ↔ `src/ingestion/` twin divergence is a known repeat offender in this repo. **Every requirement below that names a publisher names both paths**, and §6's table lists both.

`dim_matches.sql` carries four hardcoded `'public' as access_tier` legs — `:58` (statsbomb), `:93` (wyscout), `:121` (idsse), `:148` (metrica). **Only the StatsBomb leg changes.** The other three remain correct: those providers stay public-by-licence.

### Finding 3 — two registered publishers never call the leak guard

`src/ingestion/hf_leak_guard.py:27-28` documents `fail_closed` as:

> `"fail_closed"` — safe-by-absence today (no restricted provider in its mart); still asserted so absence can never silently become a leak.

"Still asserted" is not true for all of them. `test_registry_covers_every_publisher_module` (`test_hf_leak_guard.py:76-82`) enforces registry *membership* only. The assertion that a publisher *invokes* the guard (`test_hf_publish_parity.py:348-359`) is parametrized over `_ADR049_SPLIT_PUBLISHER_CARDS` — the `split` publishers only. A repo-wide grep confirms `scripts/publish_shots_on_target_hf.py` and `scripts/publish_obso_pausa_inputs_hf.py` contain no call.

`publish_obso_pausa_inputs_hf` is genuinely safe today — it reads `FROM soccer_analytics.bronze.idsse_events` (`:68`), an IDSSE-only source. `publish_shots_on_target_hf` is not: its SELECT is `FROM {catalog}.{schema}.fct_shots s` (`src/ingestion/export_shots_on_target.py:131`), a cross-provider mart, and it uploads via `api.upload_file` at `scripts/publish_shots_on_target_hf.py:177` with no guard anywhere in the path.

**On the day StatsBomb becomes restrictable, that publisher pushes restricted club shots to a public repo with nothing in the way.** This gap exists today; it is invisible only because StatsBomb is unconditionally public.

### Finding 4 — removing `statsbomb` from the allowlist is a dbt **compile** break

`dbt_project/tests/assert_action_values_access_tier_not_blocking_public.sql:19-26`:

```jinja
-- Compile-time drift guard: hard-codes the event open-data providers against the allowlist var.
{% set allow = var('public_by_license_providers') %}
{% if 'statsbomb' not in allow or 'wyscout' not in allow or 'idsse' not in allow or 'metrica' not in allow %}
    {{ exceptions.raise_compiler_error(
        "assert_action_values_access_tier_not_blocking_public assumes statsbomb/wyscout/idsse/metrica "
        ~ "are open-data (public_by_license_providers); the allowlist changed -- revisit this guard"
    ) }}
{% endif %}
```

This is `raise_compiler_error`, so the **entire dbt project fails to compile** — not one failing test. Sister guard `assert_tracking_access_tier_not_blocking_public.sql:25-31` covers only idsse+metrica and is unaffected.

The semantic consequence is worse than the mechanical one. That test's StatsBomb leg is data_source-membership based (`:33`, `where data_source in ('statsbomb', …)`). The correct rework moves StatsBomb onto a `dm.visibility = 'public'` join, exactly like the existing skillcorner leg at `:38-46`. Without that rework, **the over-restriction guard for StatsBomb vanishes at precisely the moment Finding 5 makes over-restriction likely.**

### Finding 5 — nothing stamps `visibility` on the open StatsBomb path

`grep -c visibility src/ingestion/statsbomb*.py` returns zero across `statsbomb.py`, `statsbomb_backfill_360.py`, and `statsbomb_backfill_extra.py`. `statsbomb.py` is the writer for `bronze.statsbomb_matches` and has no visibility concept at all.

So the allowlist flip, on its own, makes `_stamp_tier(actions, source="statsbomb")` return **`restricted`** for every newly converted or re-derived StatsBomb action row. A silly-kicks bump triggering a SPADL re-derive would restrict the entire StatsBomb action corpus in one job — the same class of incident as the two backfills cited in §9, and the larger blast radius of the two directions.

**The flip and the open-path stamp must land atomically.** This is the single most important structural conclusion of review round 1.

---

## 3. Decisions taken

| # | Decision | Rationale |
|---|---|---|
| D-1 | **Redistribution posture = ADR-049/064, same as SkillCorner RM.** Restricted rows split to a permanently-private `<repo>-restricted` companion. A private repo is permitted storage; only a public repo constitutes sharing. | User decision. Internal ingest, storage and derived computation are what a data subscription is *for*; commercial subscribers routinely process licensed data freely inside their own systems. The licence constraint is on **redistribution**, which is what the public-repo guard enforces. |
| D-2 | **Approach B — StatsBomb becomes a per-match-tiered provider.** | §4. |
| D-3 | **Both delivery routes land in the same bronze tables with `data_source='statsbomb'`.** | Avoids fragmenting 528 `statsbomb` references across 156 files. |
| D-4 | **Derived artifacts are split, not withheld.** Row-level artifacts use the ADR-049 two-repo split. Models with **no per-entity parameters and low memorization capacity** — xG, psxG, VAEP — train on everything and publish freely. | User decision, narrowed per review. See the scoping note below. |
| D-5 | **football2vec gets two independent training runs**, with a structural embedding-space discriminator (§7). | User decision, taken with the §7 contamination and key-collision caveats understood. |
| D-6 | **The leak guard becomes a port, not a convention** (§6). | User decision on review finding C-5. |

**D-4 scoping note (review C-8).** The earlier wording — "parametric models publish freely; weights carry no roster" — was over-broad as a *rule*, though correct for the models in question. It is a property of these models, not of parametric models generally: a future sequence model over event data would have real extraction and membership-inference exposure. D-4 is therefore an explicit per-model decision recorded in the ADR, not a class exemption.

**This is the design's only public artifact derived from restricted rows.** Everything else — the private companion repos, the derived marts, the football2vec second run (R-15: private companion only) — stays inside our own systems, which is ordinary subscription use and needs no permission.

**Precedent: this is already settled practice for the private SkillCorner Real Madrid matches.** `scripts/train_vaep_model_hf.py` trains on the combined public + restricted corpus and *mandatorily* so — `POLICY_CAN_PRODUCE_RESTRICTED` (`:109-124`) is hardcoded `True` because pining can emit `visibility=private`, and an empty restricted companion **raises** rather than training public-only (`:374-379`, "refusing a silently-shrunk training corpus"). That gate exists to stop a pining-token regression silently dropping the private matches from training. The resulting weights publish to `luxury-lakehouse/vaep-model` (`:107`) with a public model card and no `private=True`.

So D-4 applied to StatsBomb is **consistency with an existing, deliberate decision**, not a new one: commercial rows join the training corpus, and the weights publish publicly, exactly as the Real Madrid rows already do. Reversing that later is one line at the trainer — but it would be a change to established practice, and it would need to change the SkillCorner path in the same edit.

---

## 4. Approach selection

### Chosen: B — per-match `visibility` for StatsBomb

`statsbomb` leaves the allowlist, so an unclassified StatsBomb row fails **safe**, by the mechanism that already protects SkillCorner. The migration is precedented: `access_tier_backfill.py:69-74` carries the `_EXISTING_CONFIRMED_PUBLIC` override invented when SkillCorner itself left the allowlist.

### Rejected: A — a distinct `data_source` (`statsbomb_club`)

Off-allowlist by construction, zero migration, fails safe. Rejected because `data_source` is load-bearing across 528 occurrences in 156 files — SPADL dispatch, staging models, the `dim_matches` union legs, `hash_native_id_to_bigint` identity, football2vec corpus filters, Taipy filters. Every consumer that should see both feeds would need `IN ('statsbomb', 'statsbomb_club')`, and every omission silently excludes club data. It fails in the safe direction but forks the corpus permanently — an unexpiring Hyrum's-Law liability inherited by every future retrain.

### Rejected: C — keep the allowlist, stamp `visibility='private'` on club matches only

Requires no classifier change at all. Rejected because it fails **open**: any club match reaching a path that calls `stamp_access_tier(source="statsbomb")` without an explicit `visibility` becomes public, and every current call site passes exactly that default. It inverts ADR-064's stated core property (`access_tier.py:20-26`).

### Parked: D — competition-keyed classification

Consult an open-data competition allowlist derived at ingest from the free GitHub `competitions.json`, so anything the credentialed API returns outside the open catalogue fails safe. Elegant and self-maintaining, but it solves the *shared-credential interleaved-fetch* problem, which neither likely delivery route creates. **This becomes the right answer if delivery later changes to a single shared StatsBomb credential covering both open and paid competitions.**

---

## 5. Ingestion

### Route 1 — separate StatsBomb account (expected)

A distinct entry point, `ingest_statsbomb_club`, **not** a mode flag on the existing one.

- **R-1.** Its `visibility` parameter is required-no-default, mirroring `MatchInfo.visibility` (`skillcorner_common.py:34`, gated by `test_visibility_required.py`). A shared entry point with a `--private` flag is one mis-set parameter from a breach.
- **R-2.** Credentials resolve from a Databricks secret scope, following `resolve_pining_token` (`skillcorner_common.py:131-164`) — never Terraform env vars, never plain `--env`.
- **R-3.** Writes go to the existing `statsbomb_*` bronze tables with `data_source='statsbomb'`.

### Route 2 — pining-for-the-data with our owner token (fallback)

A `statsbomb` route alongside `skillcorner` and `gradientsports`. `MatchInfo.visibility` is already required-no-default and pattern-validated, so this route adds **no new policy surface**.

### Shared requirements

- **R-4.** `bronze.statsbomb_matches` gains `visibility` + `access_tier`, joining `MATCH_INFO_TABLES` (`access_tier_backfill.py:49`).
- **R-5.** `dim_matches.sql:54-58` reads the real columns, copying the **SkillCorner leg at `:167-176`** — including its `max(visibility)` / `max(access_tier)` aggregation across roster rows. Note for implementers: `max(access_tier)` fail-safes correctly *by accident of lexical ordering* (`'restricted' > 'public'`). State this in a comment so nobody "fixes" it into a bug.
- **R-6.** `stamp_access_tier` receives a real `visibility` threaded through the SPADL converter closure, replacing today's implicit `None` at `spadl_conversion.py:234`. **R-6 is a precondition of the allowlist flip, not follow-up work** (Finding 5).
- **R-6a.** `spadl_udf_shared.py:92` — make `visibility` **required-no-default**. That default *is* Finding 2's second site; removing it converts the next omission from an audit finding into a `TypeError`. Its docstring at `:100` is already stale (it claims `visibility=None` yields public for skillcorner, false since the P1 allowlist flip) — correct it in the same edit.

  **This is five call sites, not one.** The helper is consumed under the alias `_stamp_tier`, so a `stamp_access_tier(` grep finds nothing. The real sites in `spadl_conversion.py`:

  | Line | Provider | Passes `visibility`? |
  |---|---|---|
  | `:234` | statsbomb | no |
  | `:626` | wyscout | no |
  | `:1140` | idsse | no |
  | `:1564` | metrica | no |
  | `:1971` | skillcorner | **yes** — `_match_meta.get("visibility")` |
  | `:2400` | gradientsports | no |

  Five of six change. All five are semantically inert — the four allowlisted providers would pass `visibility=None` explicitly and nothing moves — which is why R-6a belongs in **PR-2a**, not in the flip commit. Bundling six converter legs into the highest-stakes change in the plan inflates the very blast radius atomicity is meant to contain.

- **R-6b (Chesterton's Fence, surfaced by R-6a).** `:2400` passes no `visibility` for **gradientsports**, yet GS carries a real per-match `visibility` in bronze (`gradientsports_metadata.py:70-71`). So the GS SPADL leg discards a live signal and defaults every GS action to `restricted` — a latent over-restriction currently invisible because GS is excluded from HF publishing entirely (`test_gradientsports_hf_exclusion.py`). R-6a forces a decision at that call site; it must be a conscious one.

  **Recommendation: thread the real GS visibility**, closing a genuine gap in the same edit. The alternative — pass `None` with a comment stating why — is acceptable but must be written down. Silently keeping the status quo is not, because after R-6a the omission is no longer visible as an omission.

  **Measured correction (2026-08-09, PR-2a).** The premise above is **half-true**, and the half that is false changes the work. `bronze.gradientsports_metadata` holds **64 rows, all `visibility=NULL`, all `access_tier=restricted`**. The *pipeline* carries the signal — `MatchInfo.visibility: str` is required-no-default and both `parse_metadata` call sites pass it — but the **stored rows predate the column**. So the GS leg does not "discard a live signal"; there is no live signal in bronze to discard yet. **Threading alone would thread NULL.**

  The signal must be POPULATED first, and a re-ingest will not do it: `_GradientSportsGuard.check` is incremental (Phase A anti-joins against `bronze.gradientsports_events` and finds nothing missing; Phase B's `updatedSince` catches matches the **provider** re-processed, which a schema change on our side is not). The correct tool is `_backfill_artifacts` (`gradientsports.py:264`), which *"skips the guard entirely"* and re-fetches metadata + roster for matches already in bronze — reachable only via `--backfill-artifacts`, which **no Terraform task passes**.

  This is the same shape as ADR-030's GS dedup, which also required a re-ingest to reach stored data. It is why R-6b ships as its own measured unit with a before/after tier count rather than inside the inert plumbing.

  Corollary worth stating, because it inverts an obvious diagnosis: since `visibility` is a **required** pydantic field, a still-NULL result after a *successful* fetch is structurally impossible. "The feed supplies no visibility for GS" is therefore never the right conclusion — a still-NULL result means our backfill did not run.
- **R-7.** A visibility-flip guard modelled on `gradientsports_metadata.py:76-92`. **Blocked on OQ-2** — see the caveat there.
- **R-16.** The open StatsBomb ingestion path stamps `visibility='public'` at write time (Finding 5). Without this, R-6 threads `None` and the flip restricts the corpus.

---

## 6. The publish port (replaces the leak-guard convention)

**D-6.** The invariant "a public artifact contains only public rows" is currently a convention asserted by substring grep in four hand-maintained lists — `test_hf_publish_parity.py:356`, `test_gradientsports_hf_exclusion.py:110`, `test_publish_shot_freeze_frames.py:97`, `test_publish_xg_shot_data_v3.py:90` — none derived from `PUBLISHER_REGISTRY`. Against that sit **15 direct upload call sites across 15 files (12 under `scripts/`, 3 under `src/ingestion/`), covering 12 `PUBLISHER_REGISTRY` entries**. Fifteen doors; the guard is one optional turnstile.

> **Count discipline (review C-16).** `12` is the count of registry *entries*, which are keyed by module basename and deliberately collapse the `scripts/` ↔ `src/ingestion/` twins. An implementer reading "12 modules" as a checklist ports 12 files and strands 3 twins — `publish_freeze_frame_hf.py:148`, `publish_spadl_vaep_hf.py:101`, `publish_xg_shots_hf.py:116` — and those are precisely the twins whose divergence is Finding 2. **The migration unit is 15 files.**

### Seam shape

A single-shot `publish_public_frame(df, …)` was the revision-2 proposal. It does not fit its callers:

- **14 of 15 call sites upload a staged folder, not a frame** — stage to a temp dir, partition (`publish_freeze_frame_hf` groups by `competition_id`), then `api.upload_folder(...)`. A frame-in/upload-out port cannot express staging without absorbing every publisher's partitioning policy.
- **`publish_football2vec_embeddings_hf` guards three frames under a degradation policy.** `:188` guards `per_match_df`; `:196-201` guards `career_df` and `season_df` inside a `try` whose `except LeakDetectedError` returns a `withheld_reason` so the caller fails closed to per-match only. A single-frame port has nowhere to put that.

A port too narrow for its callers gets bypassed — and then the AST ban is an obstacle to route around rather than a boundary, which is **worse than today's convention because it looks enforced**.

- **R-8.** Two-call seam with a runtime receipt:

  ```python
  guarded = prepare_public_upload(df, publisher=...)   # split → guard → drop access_tier → GuardedFrame
  guarded.write_parquet(staging_dir / "…")             # records every path it writes
  upload_guarded(staging_dir, publisher=..., repo=...) # refuses on any unrecorded file
  ```

  `prepare_public_upload` reads mode (`split` / `fail_closed` / `derived`) from `PUBLISHER_REGISTRY`, making it a property of the call rather than of a docstring that has already diverged from reality (`hf_leak_guard.py:26-29`). Publisher-specific assertions layer on top — football2vec keeps `assert_output_vocabulary_subset` and its degradation policy, calling `prepare_public_upload` three times.

- **R-8a.** `upload_guarded` diffs `staging_dir.rglob("*")` against the union of paths recorded by every `GuardedFrame` handed to it, and refuses on any file it cannot account for. A bare receipt *list* does not close "publisher stages an unguarded frame into the same directory"; the path diff does. **This makes a bypass detectable at runtime, not only at lint time** — R-10's AST ban becomes defence-in-depth rather than the sole mechanism.
- **R-9.** Migrate all **15 files** onto the seam.
- **R-10.** An **AST** test — using the existing idiom in `src/tests/_delta_write_ast.py` and `test_hf_publish_parity.py:369+` — asserting no `publish_*_hf` module calls the HF API directly. It must ban **`upload_folder`, `upload_file`, and `create_commit`**: `scripts/publish_shots_on_target_hf.py:177` uses `upload_file`, so a ban on `upload_folder` alone would exempt the one publisher with no guard at all.
  - **Exemption:** `ingestion.hf_publish` itself, and the ADR-014 card push `upload_hf_readme` (`hf_publish.py:240`), which uploads documentation rather than data. Allowlist it explicitly or the AST test breaks the mandated README helper on every publisher.
- **R-11.** One registry-derived test replaces the four hand-maintained lists. Substring assertions (`"assert_no_private_leak" in source`) are retired: they pass on a mention in a comment, on a call against the *restricted* frame, and on a call placed *after* the upload.

### Publisher inventory

Citations below are labelled `tier@` (hardcoded `access_tier`) and `up@` (upload call site), since the two are different concerns in the same file.

| Publisher | Mode today | Source | Guard today | Action |
|---|---|---|---|---|
| `publish_freeze_frame_hf` — src `tier@129, up@148`; scripts `tier@412, up@302` | `fail_closed` | StatsBomb 360 freeze frames | yes | → `split`; drop **both** hardcoded tiers |
| `publish_xg_shots_hf` — src `up@116`; scripts `up@365` | `fail_closed` | `fct_shots` + `dim_matches.access_tier` | yes | → `split` |
| `publish_line_breaking_passes_hf` — scripts `up@135` | `fail_closed` | `fct_passes` + `dim_matches.access_tier` | yes | → `split` |
| `publish_shots_on_target_hf` — scripts `up@177` (`upload_file`) | `fail_closed` | `fct_shots` | **no** | → `split`; see R-12 |
| `publish_obso_pausa_inputs_hf` — scripts `up@160` | `fail_closed` | `bronze.idsse_events` only | **no** | stays `fail_closed`; needs an `access_tier` column + guard via the seam |
| `publish_spadl_vaep_hf` — src `up@101`; scripts `up@359` | `split` | various | yes | migrate to seam |
| `publish_action_context_hf` — scripts `up@156` | `split` | various | yes | migrate to seam |
| `publish_xg_shot_data_v3_hf` — scripts `up@298` | `split` | various | yes | migrate to seam |
| `publish_shot_freeze_frames_hf` — scripts `up@315` | `split` | various | yes | migrate to seam |
| `publish_psxg_shots_hf` — scripts `up@152` | `split` | various | yes | migrate to seam |
| `publish_pitch_control_tracking_hf` — scripts `up@161` | `split` | various | yes | migrate to seam |
| `publish_football2vec_embeddings_hf` — scripts `up@224` | `derived` | §7 | yes | three-frame case; drives R-8's shape |

- **R-12.** `publish_shots_on_target_hf` needs only `dm.access_tier` added to its SELECT — the join **already exists** at `export_shots_on_target.py:132-133`. The hazard is that it is a `LEFT JOIN`: an unmatched match yields NULL, `split_restricted` fail-safes to restricted, and public data is silently withheld.

  **Requirement: assert non-null after the join.** `INNER` is the *rejected* alternative — it silently drops a shot whose match is missing from `dim_matches`, and this entire finding class is about silent withholding. A loud failure is the point (review C-19).
- **R-13.** `assert_no_private_leak` raises when `access_tier` is absent (`hf_leak_guard.py:55-56`), so "no restricted rows" and "no tier column" are not interchangeable. `publish_obso_pausa_inputs_hf` must source the column via a `dim_matches` join; `bronze.idsse_events` does not carry one.

One item corrects itself: `bronze.shot_freeze_frames` resolves `access_tier` from `dim_matches` in a single read (`shot_freeze_frames.py:360-388`), so the 360 freeze-frame chain becomes correct the moment `dim_matches` stops hardcoding.

---

## 7. football2vec — two runs, two vector spaces

`football2vec_360_training.py:99-110` filters the corpus, not just the output:

```sql
WHERE av.data_source = 'statsbomb'
  AND av.access_tier = 'public'
```

```python
# access_tier = 'public': co-occurrence training means a private action shapes EVERY co-occurring
# public player's vector, so the CORPUS — not just the output — must be public-only (spec §6.8 (2)).
```

A single trained model therefore cannot be split: the public vectors would already carry private contribution.

> **These requirements do not ship together.** R-17 (the discriminator) is PR-3; R-14 / R-15 / R-18 (the second run) are PR-5, after the ingestion path exists. See §8.

- **R-14.** The public run is unchanged — same corpus filter, same repos, same input/output vocabulary assertions (`test_football2vec_public_only.py`).
- **R-15.** The second run (public+restricted) publishes **only** to the private companion, and never to a public repo.
- **R-17 (from review C-4, scoped by review round 2).** The second run needs a landing zone. `fct_player_embeddings.sql:25-36` is `materialized='incremental'`, `unique_key='embedding_id'`, `incremental_strategy='merge'`, and `:45` derives the key from `(canonical_player_id, match_id, data_source)`. Run 2 covers the *same* players, matches and `data_source`, so it collides on the primary key — either overwriting run 1's public vectors or aborting with `DELTA_MULTIPLE_SOURCE_ROW_MATCHING`, the exact failure documented at `:38-43`.

  **Fix: add an `embedding_space_id` discriminator to the surrogate key and the mart grain.** Prose cannot substitute: two unaligned vector spaces sharing a primary key make cosine distance across them silently meaningless, and nothing raises.

  **This is a five-mart change with a synced-table primary-key recreation, not one mart's contract change:**

  1. **`on_schema_change='append_new_columns'` makes the naive path silently wrong.** Changing the surrogate key changes every `embedding_id` value. Under `merge` on `unique_key='embedding_id'`, old rows keep old keys and new rows arrive under new ones — **both live**. Duplicate `(player, match, data_source)` rows in a mart whose `_season` / `_career` children aggregate by element-wise mean: silent double-counting, no error.
  2. **The rebuild path is ADR-043's tool, never `dbt --full-refresh`.** `fct_player_embeddings` is TRIGGERED-synced (`refresh_synced_tables.py:179`, PK `("embedding_id",)`), so the `on-run-start` tripwire aborts a direct full-refresh. Use `uv run --extra sdk python scripts/rederive_synced_marts.py --select fct_player_embeddings --rebuild` — `--rebuild` is the documented flag for a schema/contract change.
     - **Operational trap not in the review:** `--rebuild` skips Lakebase PG index recreation. Reapply afterwards with `uv run python scripts/maintain_synced_tables.py --skip-refresh`, or the rebuilt synced tables run without their custom indexes.
  3. **The cascade reaches a synced-table primary key.** `refresh_synced_tables.py:190` gives `fct_player_embeddings_career_synced` PK `("canonical_player_id",)`. With two vector spaces per player **that key is no longer unique**. Synced-table PKs are immutable — this requires delete-and-recreate, not a refresh. Audit the siblings in the same change: `_season_synced` (`embedding_season_id`, `:191`) and the two 360 variants (`:200`, `:205`).
  4. **The aggregates must add `embedding_space_id` to their `GROUP BY`**, or the career mean averages across unaligned spaces — precisely the silent-nonsense outcome R-17 exists to prevent.
- **R-18.** The ADR records that the two runs produce **unaligned vector spaces**, not comparable without an alignment step. This is documentation *in addition to* R-17's structural fix, not instead of it.

---

## 8. Phasing

Revision 2 redrew this per review C-13. Revision 3 splits the flip on an **inertness proof** (review round 2) and resequences the football2vec work.

### The inertness proof

`src/shared/access_tier.py:46-47`:

```python
if visibility == "public":
    return AccessTier.PUBLIC
```

The explicit-`public` branch fires **before** the allowlist check at `:48-49`. So stamping `visibility='public'` on StatsBomb rows *while StatsBomb is still allowlisted* is semantically a no-op — same tier, different provenance. Every precondition of the flip is therefore inert pre-flip and can ship in its own unit, with a live gate between.

This is **strictly safer than one atomic commit.** The atomic version asserts the precondition and consumes it in the same breath, with the migration operator-applied around the merge. The split version creates an observation window in which 100 % visibility coverage is *proved on live data* before the default changes. Finding 5 does not reopen, because the ordering is then enforced by a verification gate rather than a commit boundary — and a commit boundary is only enforced by a reviewer remembering.

### PR-1 — the publish port (deal-independent, no tier semantics change)

R-8, R-8a, R-9 through R-13, plus **R-20**. Closes Finding 3, retires the substring assertions, puts all 15 files behind one seam. **Ships immediately**; it fixes a live gap that exists today.

- **R-20.** Correct `ROADMAP.md:412-414`. The "no refactoring needed / zero-code switch" claim is an active hazard the moment credentials exist, so it is fixed in the *first* PR, not the last.

### PR-2a — visibility plumbing (deal-independent, semantically inert)

1. Migration: `visibility` + `access_tier` on `bronze.statsbomb_matches` (idempotent; operator-applied).
2. **R-16** — open-path `visibility='public'` stamp at ingest.
3. **R-6 / R-6a / R-6b** — thread `visibility`; required-no-default; resolve the Gradient Sports call site consciously.
4. Backfill existing StatsBomb rows to `visibility='public'`, premise-asserted (OQ-1, R-19).
5. **R-5** — `dim_matches.sql` StatsBomb leg reads the real columns (after step 4, or it reads NULL).

Nothing in this unit changes a single row's tier. Every StatsBomb row is `public` before and after.

- **R-19 (from review C-3, reshaped by review round 2).** `_EXISTING_CONFIRMED_PUBLIC["statsbomb"] = "public"` persists after its premise expires; any later backfill re-run would stamp commercial rows public.

  **`access_tier_backfill.py` is the wrong home for the check.** That module imports only `shared.access_tier`, and `default_tier_for_provider` (`:76-83`) is a pure `str -> str`. Threading a Spark handle into it would inject I/O into the one layer that has none — the separation `shared/access_tier.py` is built on. Instead:

  - `default_tier_for_provider` stays pure.
  - A **driver-level precondition** `assert_no_commercial_statsbomb_rows(conn)` runs before any backfill statement executes, refusing on a single row with `visibility='private'`.
  - `_EXISTING_CONFIRMED_PUBLIC` becomes `{provider: (tier, precondition_name)}`, so an override **cannot exist without a named check that must pass**. This closes the "someone deletes the assertion but keeps the dict entry" failure mode, and generalises to SkillCorner, which carries the identical latent shape today.

### Live gate between PR-2a and PR-2b

Not a code review — a number:

```sql
SELECT count(*) FROM soccer_analytics.bronze.statsbomb_matches WHERE visibility IS NULL;  -- must be 0
```

Repeat over `fct_action_values` and `spadl_actions` for StatsBomb rows. PR-2b does not merge until all are zero.

### PR-2b — the flip

6. Remove `statsbomb` from `PUBLIC_BY_LICENSE_PROVIDERS`; mirror into the `public_by_license_providers` var (`dbt_project/dbt_project.yml:74-78`); add the `_EXISTING_CONFIRMED_PUBLIC` entry with its precondition name.
7. **Rework `assert_action_values_access_tier_not_blocking_public.sql`** (Finding 4) — move the StatsBomb leg onto a `dm.visibility = 'public'` join, copying the skillcorner leg at `:38-46`. Without this the project does not compile.
8. Publisher mode conversions (`fail_closed` → `split`) per §6's table.

**Migration ordering.** Bronze migrations have no CI auto-apply — they are applied manually *with* the merge. Apply PR-2a's migration **before or at** merge, never after: `dbt-live-ci.yml` is a daily scheduled live build, and a merged `dim_matches.sql` selecting a column that does not exist breaks the next nightly. Verify with a live `DESCRIBE` post-apply.

### PR-3 — the `embedding_space_id` discriminator

R-17 in full: the five-mart cascade, the `--rebuild` path, the synced-PK recreation, the `GROUP BY` fix.

**Sequenced before PR-4; no contract gate.** This diverges from review round 2, which recommended shipping R-17 early alongside PR-1. R-17 costs a synced-table delete-and-recreate with re-snapshot downtime and a PG index reapply, and it is a prerequisite of PR-5 and nothing else — so it is worth doing once the subscription is real, not on the chance that it becomes real. Sequencing it *before* PR-4 keeps the synced recreation away from a live commercial ingest, which is the part of round 2's early-ship argument that carries real weight.

### PR-4 — the commercial ingestion path

`ingest_statsbomb_club` (or the pining route), R-7's flip guard, secrets wiring, workflow card, Terraform task, mega-job entry, and removal of the R-19 override.

**Blocked on OQ-5** (which delivery route). No contract gate — ingesting data we have licensed is the point of licensing it.

### PR-5 — football2vec second run

R-14, R-15, R-18. **Sequenced after PR-4** by data dependency: the second run trains on restricted StatsBomb rows that do not exist until PR-4 ingests them. It publishes only to the private companion, so it emits nothing requiring permission.

---

## 9. Testing

Synthetic fixtures only. No real club data enters the repo, under the same rule governing the Real Madrid SkillCorner fixtures.

| Test | Asserts |
|---|---|
| `test_access_tier.py` (extend) | `classify_access_tier(provider="statsbomb", …)` for `"public"` / `"private"` / `None` — the last must now return `RESTRICTED` |
| `test_access_tier_visibility_consistency_allowlist.py` | picks up the dbt-var change; confirm red before the var moves |
| new — dbt compile gate | the project compiles after Finding 4's rework, and the reworked test is red against a seeded over-restricted StatsBomb row |
| new — over-restriction guard | red **before** R-16's open-path stamp exists, green after. This is the test that would have caught Finding 5 |
| new — statsbomb flip guard | per OQ-2's resolved policy, not before |
| new — port AST test (R-10) | no `publish_*_hf` module calls `upload_folder` / `upload_file` / `create_commit` directly, with the `hf_publish` + `upload_hf_readme` exemptions |
| new — registry-derived seam test (R-11) | every `PUBLISHER_REGISTRY` entry routes through `prepare_public_upload` / `upload_guarded` |
| new — receipt enforcement (R-8a) | `upload_guarded` raises when the staging dir contains a file no `GuardedFrame` recorded — the runtime bypass check, red-first against a hand-staged unguarded frame |
| new — GS visibility decision (R-6b) | whichever branch is chosen at `spadl_conversion.py:2400` is asserted, so the status quo cannot survive by silence |
| new — end-to-end fixture | a private StatsBomb match never appears in any publisher's public frame |
| `test_access_tier_backfill.py` (amend) | bump `assert len(ALL_ACCESS_TIER_TABLES) == 10` → `11` when `statsbomb_matches` joins `MATCH_INFO_TABLES` per R-4 |

**Correction from review C-11.** Revision 1 proposed re-deriving `ALL_ACCESS_TIER_TABLES` "against the live `information_schema`". That would convert a fast hermetic unit test (`test_access_tier_backfill.py:47`) into a Databricks-credentialed one, contradicting the file's own comment ("the live information_schema count (operator verifies against catalog)"). **Keep it hermetic**; put the live check where this repo already puts live checks.

**Red-first is required for every new invariant**, not just the allowlist var. Findings 4 and 5 are both cases where a green suite would have said nothing.

Over-restriction from a missed backfill table is the failure mode this repo has hit **twice** — `scripts/migrations/2026-07-02-backfill-tracking-access-tier.sql` and `2026-07-06-backfill-fct-action-values-access-tier.sql`. It fails in the recoverable direction but silently withholds public open data.

---

## 10. ADR impact

- **Amend ADR-064** — `PUBLIC_BY_LICENSE_PROVIDERS` loses `statsbomb`; record the premise-asserted backfill, its code-level assertion (R-19), and the atomicity constraint from Finding 5.
- **Amend ADR-049** — new restricted companions; the guard becomes a port.
- **New ADR-072** (highest existing is ADR-071) — the publish seam (D-6): `prepare_public_upload` / `upload_guarded`, registry-derived mode, receipt-enforced at runtime and AST-enforced at lint. Worth its own ADR since it is a cross-cutting security boundary, per CLAUDE.md's ADR triggers.
- **New ADR-073** — the StatsBomb commercial path: dual delivery routes, the separate-entry-point rule (R-1), and the football2vec two-run decision with its `embedding_space_id` contract change (R-17) and unaligned-space caveat (R-18).
- **ADR-043 note** — R-17's rebuild exercises the documented `--rebuild` path for a schema/contract change on a TRIGGERED synced mart, including the PG-index reapply that `--rebuild` skips.
- **`AI_GOVERNANCE.md`** — no per-player evaluative card changes expected. Confirm during implementation rather than assuming.

---

## 11. Open questions and preconditions

- **OQ-1 (precondition, operator).** A live query must prove `bronze.statsbomb_*` holds zero commercial rows at flip time. Trivially true now; it is a *precondition of the backfill*, expires the moment PR-4 runs, and is enforced in code by R-19 rather than by prose.
- **OQ-2 (blocks R-7).** Whether a club match may also exist in the StatsBomb open data. StatsBomb match IDs are globally unique, so this surfaces as one ID under both tiers, not an ID collision. **R-7's guard as modelled on Gradient Sports treats a flip as a producer-side violation and raises — which would fail the daily job on the expected case.** Resolve the policy first. Note that "restricted wins" over-restricts a match that is genuinely open.
- **OQ-3 (unknown).** Events only, or events + 360. The design assumes events + 360. Events-only leaves `sb360_freeze_frames`, `defcon_lite_360`, `line_breaking_360`, `prepare_360_training_data` untouched.
- **OQ-4 (deferred).** Whether the club receives its own restricted artifacts, and through which repo. Affects companion naming and grants, not the mechanism.
- **OQ-5 (blocks PR-4).** Which delivery route applies (§5). A separate StatsBomb account is expected; a club file drop would go through pining-for-the-data with our owner token.
- **OQ-6 — RESOLVED by precedent (blocks nothing).** Whether publicly downloadable model weights may be trained on restricted rows. **This was already decided for the private SkillCorner Real Madrid matches and is in force today**: the VAEP trainer requires the restricted corpus and publishes weights to a public repo (see D-4's precedent note). StatsBomb follows the same path. Retained here only as a record of the question and its answer.

  **Scope correction (revision 3).** Round 1 raised this as C-8; revision 3 initially escalated it into a gate on PR-3, PR-4 and PR-5. That was wrong twice over. First, internal ingest, private storage and derived computation are ordinary subscription use — commercial subscribers process licensed data freely within their own systems, and nothing in the plan asks for more than that. Second, it was **incoherent against this spec's own text**: R-15 confines the second run to the private companion, so PR-3 and PR-5 emit nothing public, and gating them on a redistribution question made no sense.

  The residual question is one narrow case — the only public artifact in the entire design derived from restricted rows. Worth a line in the contract conversation; not a dependency for any unit of work.

---

## 12. Out of scope

- Any change to Wyscout, IDSSE, or Metrica tiering — they remain public-by-licence, and their three `dim_matches` legs are unchanged.
- Retiring or re-baselining existing public HF datasets.
- **Closed since revision 1:** whether StatsBomb rows reach `fct_shots` / `fct_passes`. Revision 1 deferred this; it is one query and it decides whether two publishers convert to `split`. Run it during PR-1 rather than carrying it as a deferral.
