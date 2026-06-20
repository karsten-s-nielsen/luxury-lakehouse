# TF-23 + TF-23b / silly-kicks 4.34.0 SkillCorner+Metrica frame-builder adoption + in-repo net deletion — Phase A (code PR)

> **For agentic workers:** REQUIRED SUB-SKILL: superpowers:executing-plans. Steps use `- [ ]`.
> **Status:** DRAFT for cross-session review (not yet approved to build).
> **Scope:** Phase A = the code PR only (pin bump + delete-and-depend + tests). **Phase B** (operational re-materialization of SC/metrica `fct_action_context` — the retrain trigger) is OUT OF SCOPE here and gated on a separate user go-ahead.

**Goal:** Replace the lakehouse's three duplicated SkillCorner/Metrica coordinate/clock/orientation transforms with silly-kicks 4.33.0's `tracking.{skillcorner,metrica}.convert_to_frames`, recover SkillCorner `ball_z`, and (for SC/metrica) retire the in-repo orientation net — closing ADR-031 Gate C on the live `fct_action_context` path.

**Architecture:** Two lakehouse copies of `_bronze_{skillcorner,metrica}_to_frames` exist — `src/analytics/action_context/convert.py` (the LIVE `fct_action_context` path, primary) and `src/ingestion/tracking_context.py` (legacy `fct_tracking_context`). Both are replaced by thin lakehouse **adapters** that shape the post-join bronze to the silly-kicks `EXPECTED_INPUT_COLUMNS` contract and call the upstream builders. silly-kicks owns rescale + clock + `ball_z` + GK + orientation; the lakehouse keeps ingestion (raw→bronze, the `skillcorner_matches` join, Spark↔pandas), velocity derivation, and identity resolution.

**Tech Stack:** Python 3.10, pandas, PySpark, silly-kicks 4.34.0, pytest. No new runtime dependency.

**Reference:** silly-kicks ADR-034 / spec `2026-06-18-tf23-skillcorner-metrica-bronze-frame-builders-design.md` (Rev 3); lakehouse ADR-053 (`correct_frames_to_home_ltr`, promoted upstream); memory `project-tf23-sk433-frame-builder-adoption`, `reference-shot-goalmouth-z-gap-metrica-skillcorner`, `reference-silly-kicks-bump-version-sentinels`.

**Branch:** `feat/tf23-sk433-frame-builder-adoption` off `main`. **No commit without explicit user approval** (CLAUDE.md). Single PR.

**Test command:** `uv run pytest src/tests/ -q` (full suite — the sentinel bump REQUIRES the full suite, a curated slice misses them, per reference-silly-kicks-bump-version-sentinels).

### Revision 2 — cross-session review integrated (2026-06-18)

A reviewer found a **HIGH** bug in Rev 1: D1 treated the SC and metrica clocks symmetrically ("builder owns the rebase"), but they are **not** symmetric, and the metrica path as drafted would silently corrupt the clock. Verified against source + live data:
1. **Metrica clock delegation is ACTIVE-broken, not latent.** The silly-kicks metrica builder rebases by the per-`(period)`-min **of the frames it is handed** (`metrica.py:188`). The lakehouse dispatch feeds it **250-frame sub-period batches** (`DEFAULT_FRAME_BATCH_SIZE=250`, no metrica override). Live metrica periods are **67,941–74,100 frames = 272–297 batches each** — so only ~1 batch in ~285 contains the period's first frame; the builder would re-zero the clock to the **batch** start on the other ~99.6% → batch-relative time → action↔frame linkage collapse. (SkillCorner's nominal-constant rebase IS batch-invariant — delegation is fine there.) silly-kicks' own validation ran on full game-1 frames so never hit this; a single-batch fixture would mask it.
2. **Worse, the methods disagree:** the lakehouse derives the metrica clock from continuous **frame numbers** (`pipeline.py:575–583`) specifically to be immune to Sample_Game_3's hand-curated P2 timestamp reset; the silly-kicks builder uses the raw **timestamp**. Delegating loses that robustness even on full frames.
3. **Fix (Rev 2):** D1 is now explicitly **asymmetric** — SC delegates the clock to the builder; **metrica does NOT** (the adapter keeps the lakehouse frame-number clock and **overwrites** the builder's `time_seconds`). Builder used for metrica coords/`ball_z`/GK/orientation only. Gated by a **mid-period-batch regression test** (Task 4/5).
Also integrated: metrica `home_team_id=="Home"` tail-net assertion (MEDIUM — D2); metrica roster keys pinned to `"Home"/"Away"` + a no-synthetic-id linkage test (MEDIUM — D5, the `"Home_11"` vs old `"Player11"` fallback Hyrum break); migration gate must run a **multi-batch** slice (else it masks #1); TF-23b is now in flight upstream (GS+sportec native-adapter geometric backstop, closing ADR-031 Gate D) → the GS-ET reason for our tail net is being removed, so full net deletion (not a permanent provider-gate) stays the end state.

### Revision 3 — second review integrated (2026-06-18)

The reviewer confirmed the Rev-2 metrica-clock fix but flagged that **D1's A/B was never actually decided** — the prose leaned B while Tasks 5/7/8 + the file-structure table were written for A (delete the constant + the dispatch rebase), which would **`ImportError`** as-written (B keeps `pipeline.py:592–596`, whose import targets the to-be-deleted constant). Also: plain-B perpetuates the duplicated SC clock constant (the "duplicated-truth #3" this PR exists to kill). **Rev 3 DECIDES `B′`** (uniform clock-overwrite, ownership untouched, BUT delete the lakehouse SC constant and import the offset from `silly_kicks.spadl.skillcorner` → single-sourced) and propagates it through every task (D1, Task 4/5/7/8, file structure). Plus: (a) the metrica clock overwrite is an explicit **`(frame_id, period_id)` map-join**, not positional (builder drops rows); (b) a new **metrica cross-batch GK/orientation consistency guard** replaces the unevidenced "batch-invariant" claim (a 10 s window can mis-derive the positional GK → mirror-flip that batch); (c) a Task-5 assert that metrica `actions["team_id"]` maps to exactly `{"Home","Away"}` (else the builder's roster merge silently misses → all synthetic ids). **Accepting the reviewer's upstream offer to expose `_PERIOD_START_SECONDS` publicly** in silly-kicks (target the public accessor; interim = private import + a value-guard test).

### Revision 5 — D4 legacy-path: retirement BLOCKED by a live dependency (2026-06-20)

At commit-prep, owner directed "nothing skipped/deferred" and chose to **retire** the legacy
`fct_tracking_context` path (vs the Rev-4 descope), then asked to **confirm nothing relies on it**.
Chesterton's-Fence verification (grep across dbt/taipy/terraform) found the retirement is **NOT
safe** — the belief that AC fully replaced tracking_context holds only for the dead mart:

- **`fct_tracking_context` MART = DEAD** — zero `ref()` consumers in dbt, zero in the Taipy app.
  Safely droppable on its own.
- **The `compute_tracking_context` JOB is LOAD-BEARING.** It is the SOLE producer of
  `bronze.spadl_tracking_context` → `stg_spadl__tracking_context` → consumed by
  **`int_tracking_goalkeepers`** (GK identities) + **`int_minutes_played_per_match`** (IDSSE
  roster/minutes) → feed **`fct_goalkeeper_stats`** (taipy: 14 refs), **`fct_tracking_frames`**
  (taipy: 11 refs), **`int_minutes_played`**. action_context does NOT emit GK-identity roster /
  minutes / the tracking_frames feed, so those would break.

**Decision:** legacy retirement is **descoped from this PR** (now evidence-backed, not a judgment
call). Retiring it first requires **re-homing** GK identities (`int_tracking_goalkeepers`) and the
IDSSE minutes branch (`int_minutes_played_per_match`) onto another source (events / silly-kicks GK
output / action_context), then dropping the job + the dead `fct_tracking_context` mart. That is a
separate, properly-scoped follow-up. **This PR remains the AC-path-only TF-23/TF-23b migration**
(complete + green); the legacy `_bronze_*` copies in `tracking_context.py` stay untouched.

### Revision 4 — fold onto silly-kicks 4.34.0 / TF-23b; delete the in-repo net entirely (2026-06-19)

Owner decision (2026-06-19): do **not** adopt on 4.33.0 then re-rev for TF-23b. Wait for **silly-kicks 4.34.0** (TF-23b — the geometric backstop on the GS+sportec native adapters, ADR-035) and run TF-23 + the net retirement as ONE campaign. Rationale: one wheel bump + one sentinel/terraform/uv.lock lockstep + one SC/metrica AC recompute, and `correct_frames_to_home_ltr` becomes retirable for **all four** providers at once (SC/metrica via the TF-23 builders; idsse/GS via the TF-23b native-adapter backstop) instead of the provider-conditional half-state ADR-034 warned against ossifying.

Changes from Rev 3:
1. **Target 4.34.0, not 4.33.0** — Task 0 bump, all sentinels `(4,32,0)→(4,34,0)` (skip 4.33.0), terraform `==4.34.0`. The Task-0 precondition also verifies the TF-23b surface (`direction.finalize_orientation`; `orient_frames_to_ltr_by_geometry(on_missing_home=, copy=)`).
2. **D2 upgraded: DELETE `correct_frames_to_home_ltr` ENTIRELY in this PR** (was: leave as no-op + fast-follow). On 4.34.0 every provider comes out home-LTR upstream, so the lakehouse tail net is fully redundant. **Acceptance oracle = `test_frame_orientation_golden.py` staying green WITHOUT the net** — its `gradientsports/10517_p3` case is EXTRA TIME, the exact GS-ET flip the deleted net used to fix (the lakehouse AC path already runs the silly-kicks GS native adapter, which on 4.33.0 lacks the backstop → the lakehouse net is what fixes 10517_p3 today; on 4.34.0 the backstop fixes it at the adapter). A green 10517_p3 with the net removed proves the backstop carries it end-to-end. Deleting the net also removes the Rev-2 metrica `home_team_id="Home"` tail-net special-case (no net left to feed).
3. **Period-5/PSO — preflight RAN 2026-06-20, changed set = EMPTY (concern closed).** Read-only Databricks census of `soccer_analytics.bronze.{idsse,gradientsports,skillcorner,metrica}_tracking` period histogram: **every provider carries only periods 1–4** — GS 1,2 (all 64) + ET 3,4 (3 matches); idsse/metrica/skillcorner 1,2 only. **ZERO period-5/PSO rows anywhere.** (The "GS knockouts have shootouts → expect GS period-5" expectation was wrong: the GS feed has no shootout tracking.) So the net-deletion's "never orient PSO" change is a **no-op on all production data** — period 5 does NOT contribute to the Phase-B changed set. The real changed set is driven solely by orientation-now-upstream + SC `ball_z` recovery.
4. **ADR-053 amendment now references ADR-035** (the net is deleted, not "pending TF-23b") — Task 9.
5. **ball_z stays in this campaign (not pulled forward).** The SC `ball_z` recovery is realized *through* the builder swap (the lakehouse builders that hardcode `z=NaN` are deleted; the silly-kicks builder + the SC projection carry it). The only version-independent sliver — adding `ball_z`/`is_visible` to the Spark SELECT — yields no value alone (nothing consumes it until the swap), and consuming z early would be a throwaway edit to soon-deleted builders. So it rides with TF-23 here, not as standalone pre-work.

---

## ⚠️ Open decisions for the reviewing session (resolve before/at build)

These are the load-bearing integration questions. The mapped facts make the *mechanical* edits trivial; these three are where the risk lives.

- **D1 — Clock ownership (HIGHEST RISK; Rev 3 — DECIDED: option B′, propagated through all tasks).** The metrica builder rebases by per-`(period)`-min of the **input batch** (`metrica.py:188`) → batch-relative on any non-first batch (live: 272–297 batches/period → ~99.6% corrupted; ACTIVE, not latent). SkillCorner's nominal-constant rebase (`skillcorner.py:156`) is batch-invariant but the constant is duplicated. **The chosen design (B′) — uniform overwrite + single-source the SC constant, ownership untouched:**
  - **Keep the dispatch rebase for BOTH providers** (`pipeline.py:575–583` metrica frame-number method; `:592–596` SC) so `_owned_action_ids` keeps consuming period-relative input frame time — **no ownership-path surgery** (this is why B′ over A; see below).
  - **Re-point the SC dispatch rebase's offset to silly-kicks** and **delete the lakehouse copy.** Today `pipeline.py:593` does `from analytics.action_context.convert import _SKILLCORNER_PERIOD_START_SECONDS`; change it to import `_PERIOD_START_SECONDS` from `silly_kicks.spadl.skillcorner` (the builder's own constant), and delete `convert.py::_SKILLCORNER_PERIOD_START_SECONDS`. This **kills duplicated-truth #3** (one source of truth, upstream) — the actual TF-23 thesis — *without* touching ownership. (Metrica's dispatch rebase is the frame-number method and uses no SC constant, so it's already single-sourced — nothing to dedup there; it's robust to SG3's P2 timestamp reset, gated by `test_metrica_period_relative_time`.)
  - **Both adapters call the builder for coords / `ball_z` / GK / orientation only, then OVERWRITE `time_seconds`** with the lakehouse period-relative clock via a **key map-join on `(frame_id, period_id)`** — NOT positional (the builder drops NaN-ball + malformed-JSON rows, so row positions won't align). The builder's own clock output is discarded.
  - **silly-kicks private-symbol caveat:** `_PERIOD_START_SECONDS` is underscore-private. The reviewer offered to expose it publicly upstream (folded into TF-23b or a tiny PR) — **accept that** and target the public accessor. Interim: import the private symbol + a **guard test** asserting it equals the expected nominal `{1:0.0, 2:2700.0, 3:5400.0, 4:6300.0, 5:7200.0}` (so an upstream change fails loud).
  - **Why B′ and not A (delete-rebase + single-source-to-builder):** A is the cleaner hexagonal end-state (clock truth lives entirely upstream, lakehouse just consumes builder output) but requires **re-anchoring `_owned_action_ids`** — which runs on the **pre-build input** frames (`enrich_batch:476` → `:374,395`) at period-relative time. Deleting the SC dispatch rebase desyncs it; re-anchoring touches the load-bearing M13 ownership path. B′ achieves the same single-sourcing (no duplicated constant) for Phase A without that surgery. **A is recorded as the eventual cleanup** (ADR), gated on the ownership reorder.
  - **Why not "convert-full-match-then-batch":** it would give every global op full-period input, but the 250-frame batching exists for the 1 GB serverless UDF cap (a full metrica period ≈ 70k frames × ~23 entities ≈ 1.6M rows → OOM). Deferred; the clock-overwrite + the GK/orientation guard (below) cover Phase A.
  - **Clock gate:** a regression test feeds the metrica adapter a batch that does NOT start at the period's first frame and asserts output `time_seconds` is period-relative-from-period-start (fails on the naive delegate design, passes only with the map-join overwrite).
  - **Rev-3 MEDIUM — metrica per-batch GK/orientation are a MONITORED residual, not "batch-invariant".** For SkillCorner the native roster GK is batch-invariant (safe). For **metrica**, GK is positionally derived (`derive_goalkeepers`, Tier-2 validated over *full* matches); on a ~10 s (250-frame) window during a GK rush / corner / sweeper-keeper episode the deepest player can transiently be a defender → wrong GK pick for that batch → geometric orientation anchors on the wrong player → that batch's 250 frames get mirror-flipped. Full-match validation can't see this. **Mitigation (cheap, required): a cross-batch consistency guard** — for each `(match, team)`, assert the derived GK `player_id` and the per-period flip decision are identical across all batches; surface any disagreeing batch. If it ever fires on real data, that's the signal to move metrica to convert-full-match-then-batch. Drop the "batch-invariant" claim for derived-GK providers; call it monitored.
- **D2 — Orientation net (Rev 4: DELETE ENTIRELY this PR; was "leave as no-op + fast-follow").** `correct_frames_to_home_ltr` is applied to ALL providers at the dispatch tail (`pipeline.py:317–324`, no gating). On **4.34.0** every provider comes out home-LTR upstream (SC/metrica via the TF-23 builders; idsse/GS via the TF-23b native-adapter backstop, ADR-035), so the tail net is fully redundant → **delete the function (`pipeline.py:80`) + its call (`:317–324`)**. **Acceptance oracle: `test_frame_orientation_golden.py` stays green WITHOUT the net** — its `gradientsports/10517_p3` case is EXTRA TIME, the exact GS-ET flip the net fixes today, so a green 10517_p3 with the net removed proves the TF-23b backstop carries it end-to-end. Deleting the net also removes the Rev-2 metrica `home_team_id="Home"` tail-net special-case below (no net left to feed).
  - **Rev 2 MEDIUM — metrica `home_team_id` consistency at the tail net.** The metrica builder hard-codes team labels `"Home"/"Away"` and is called with `home_team_id="Home"` (Task 4). But the tail net is called with `home_team_id=meta.home_team_id` (`pipeline.py:322`). If `meta.home_team_id` for metrica is not literally `"Home"`, the net matches zero home players. The in-repo `correct_frames_to_home_ltr` **returns-unoriented (warns, does NOT raise)** on a zero-home match (per `test_frame_ltr_correction.py::test_zero_home_match_returns_unoriented`) — so this is not a crash, and since the builder already oriented the frames the tail no-op is harmless for *correctness* — but it's a silent warn that masks a real mismatch. **Decision:** for the metrica branch, pass `home_team_id="Home"` to the tail net explicitly (matching the builder's labels) and assert it; do not rely on `meta.home_team_id` happening to equal `"Home"`.
- **D3 — Velocity derivation.** The current convert.py builders emit `vx`/`vy` (asserted by `test_tracking_context_converters.py::test_skillcorner_to_frames_basic`); the silly-kicks builder emits `speed` but **not** `vx`/`vy`. **Decision:** locate the lakehouse velocity-derivation step (`_derive_velocities_savgol` per the handoff) and apply it to the builder output, OR pass `preprocess=` to the builder. *Recommendation: apply the existing lakehouse savgol step post-builder (keeps smoothing lakehouse-owned, DFL-port precedent); confirm its exact location + signature in Task 3 Step 0.*
- **D4 — Legacy path scope.** Retire `tracking_context.py`'s builders by rewiring its dispatch to the same adapters (full delete-and-depend), OR leave the legacy `fct_tracking_context` path frozen and only migrate the AC path now? *Recommendation: rewire both to the shared adapter (the handoff asks for both); the legacy path is low-traffic but the duplicated truth is the whole point.* If the reviewer prefers to descope, Task 8 becomes "delete the legacy builders + retire the legacy path" instead.
- **D5 — Metrica GK + roster shape.** silly-kicks metrica builder takes `jersey_to_player_id={"Home":{...},"Away":{...}}` and **positionally derives GK** (seeds none). The lakehouse currently builds a **flat** `jersey→pid` dict from `actions["player_id_native"]` (`pipeline.py:257–269`) and flags GK from team-agnostic `gk_jersey_numbers`. *Recommendation: build the per-team roster dict from `player_id_native` split by `actions["team_id"]` (we own identity, O4), with outer keys **exactly `"Home"/"Away"`** (the builder hard-codes those labels at `metrica.py:77,133,146`), and stop passing GK jerseys (let the builder positionally derive — sidesteps the team-agnostic mis-flag).*
  - **Rev 2 MEDIUM — fallback-id Hyrum break.** Unmapped jerseys: the old `fallback_fmt` produced `"Player11"`; the builder produces `"Home_11"` (`metrica.py:153`). A frame `player_id` of `"Home_11"` will NOT match an action `player_id_native` of `"Player11"` → action↔frame linkage breaks for any unmapped acting player. The roster is built FROM the actions, so every *acting* player is mapped (fallback only fires for tracking-only players, who have no actions to link) — but pin it: **add a test asserting every metrica frame `player_id` in the LINKED set resolves to a roster pid (no `"Home_"/"Away_"` synthetic id among linked actions)**, and surface a count if any fallback fires.

---

## File structure

| File | Action | Responsibility |
|---|---|---|
| `src/analytics/action_context/sk_frame_adapters.py` | **Create** | Thin lakehouse adapters: post-join bronze → silly-kicks builder call → AC result-frame schema. The single integration seam. |
| `src/analytics/action_context/convert.py` | Modify | Delete `_bronze_{metrica,skillcorner}_to_frames` + `_SKILLCORNER_PERIOD_START_SECONDS` (B′: dispatch imports the offset from silly-kicks instead) + `_SKILLCORNER_CONSUMED_COLS`. |
| `src/analytics/action_context/pipeline.py` | Modify | `_convert_tracking_batch`: call the adapters for SC/metrica; **keep** both dispatch rebases (B′), re-point the SC offset import to `silly_kicks.spadl.skillcorner`; **DELETE `correct_frames_to_home_ltr` (`:80`) + its dispatch call (`:317–324`)** (D2, Rev 4 — all 4 providers oriented upstream on 4.34.0). |
| `src/ingestion/tracking_context.py` | Modify | Add `ball_z`,`is_visible` to `_SKILLCORNER_TRACKING_SELECT_COLS`; delete `_bronze_*_to_frames` + `_SKILLCORNER_PERIOD_START_SECONDS`; rewire `_make_tracking_context_udf` dispatch to the adapters (D4). |
| `src/ingestion/action_context.py` | Modify | SkillCorner join (`:1443–1458`): rename `team`→`team_id`, add `ball_z`/`is_visible` passthrough; keep the SC dispatch rebase (`:1615–1618`) but re-point its offset import to silly-kicks (B′). |
| `pyproject.toml`, `uv.lock`, `terraform/modules/workflows/main.tf` | Modify | silly-kicks `>=4.33.0,<5` / `==4.33.0` + `uv lock`. |
| `src/ingestion/exec_visibility.py` + 6 `scripts/train_*.py` + `src/tests/test_sk3_mig_b_orchestrator_invariants.py` | Modify | `_REQUIRED_SK_MIN` / expected `(4,32,0)`→`(4,33,0)` (the sentinel dance). |
| `src/tests/action_context/test_sk_frame_adapters.py` | **Create** | Adapter unit tests (rescale via builder, ball_z recovered, schema, guards). |
| `src/tests/action_context/test_sk_builder_migration_gate.py` | **Create** | Old-vs-new diff-explainer on the committed SC fixture (z populated + oriented; else equal). |
| `src/tests/action_context/test_convert_drift.py` | Modify/retire | The AST drift guards die when the legacy copies are deleted — replace with a "both dispatch to silly-kicks" structural guard. |
| `test_tracking_context_converters.py`, `test_metrica_tracking_player_id.py`, `test_skillcorner_frame_time_base.py`, `test_skillcorner_dispatch_time_base.py`, `test_metrica_builder_y.py`, `test_frame_orientation_golden.py`, `test_frame_y_identity_golden.py`, `test_tracking_context_skillcorner_local.py` | Modify | Re-point/retire builder-specific assertions; **goldens must stay green** (they encode the coordinate truth — they are the acceptance oracle). |
| `docs/superpowers/adrs/ADR-053-*.md` | Modify | Amend: net promoted upstream (ADR-034) + GS/sportec backstop (ADR-035); in-repo `correct_frames_to_home_ltr` DELETED (Rev 4); Phase-B re-materialization + period-5/PSO preflight noted. |
| `src/tests/action_context/test_frame_ltr_correction.py` | Retire | Unit test of the deleted in-repo net; orientation coverage moves to the cross-provider `test_frame_orientation_golden.py` (Rev 4). |

---

## Task 0: Branch, env, and the silly-kicks 4.33.0 API precondition

- [ ] **Step 1: Branch.** `git checkout main && git pull && git checkout -b feat/tf23-sk433-frame-builder-adoption`
- [ ] **Step 2: Bump + lock first (so the API exists locally).** Edit `pyproject.toml:44` `>=4.32.0,<5`→`>=4.34.0,<5` (Rev 4 — target **4.34.0**, not 4.33.0), then `uv lock --upgrade-package silly-kicks && uv sync --extra spadl`.
- [ ] **Step 3: Precondition — verify the 4.33.0 surface the adapters depend on.** Run:
```bash
uv run python -c "
import inspect
from silly_kicks.tracking import skillcorner, metrica, orient_frames_to_ltr_by_geometry
from silly_kicks.tracking.schema import (SKILLCORNER_TRACKING_FRAMES_COLUMNS, METRICA_TRACKING_FRAMES_COLUMNS,
                                          KLOPPY_TRACKING_FRAMES_COLUMNS, TrackingConversionReport)
import silly_kicks; assert silly_kicks.__version__.startswith('4.34'), silly_kicks.__version__
sc = inspect.signature(skillcorner.convert_to_frames); assert {'bronze','home_team_id','output_convention'} <= set(sc.parameters)
mt = inspect.signature(metrica.convert_to_frames); assert {'bronze','jersey_to_player_id','home_team_id'} <= set(mt.parameters)
print('EXPECTED_INPUT (sc):', skillcorner.EXPECTED_INPUT_COLUMNS)
print('EXPECTED_INPUT (mt):', metrica.EXPECTED_INPUT_COLUMNS)
print('frame cols == kloppy:', SKILLCORNER_TRACKING_FRAMES_COLUMNS == KLOPPY_TRACKING_FRAMES_COLUMNS)
# Rev 4 — TF-23b (4.34.0) surface the net deletion depends on:
from silly_kicks.tracking import direction
assert hasattr(direction, 'finalize_orientation'), 'TF-23b finalize_orientation missing'
_op = inspect.signature(direction.orient_frames_to_ltr_by_geometry).parameters
assert {'on_missing_home', 'copy'} <= set(_op), 'TF-23b on_missing_home/copy params missing'
print('OK')
"
```
Expected: `OK`, and `EXPECTED_INPUT_COLUMNS` matches the contract (sc includes `ball_z`,`is_visible`,`team_id`; mt includes `home_players`,`away_players`,`gk_jersey_numbers`). **If the surface differs, STOP and reconcile this plan before writing adapter code** (the adapters are written against this contract).

---

## Task 1: Version-bump sentinels (the lockstep dance)

**Files + exact lines** (from the sentinel audit). All `(4, 32, 0)`→`(4, 34, 0)` (Rev 4 — 4.34.0, skipping 4.33.0):
- `src/ingestion/exec_visibility.py:450`
- `scripts/train_vaep_model_hf.py:73`, `train_xg_v2_hf.py:96`, `train_football2vec.py:81`, `train_football2vec_v2.py:78`, `train_football2vec_360.py:76`, `train_scoutgpt_hf.py:82`
- `src/tests/test_sk3_mig_b_orchestrator_invariants.py:340` (the `expected = (4, 32, 0)` line) + the comment on `:257`
- `terraform/modules/workflows/main.tf:1421` `==4.32.0`→`==4.34.0`
- `pyproject.toml:44` + `uv.lock` (done in Task 0 Step 2)

- [ ] **Step 1:** Apply all the above edits.
- [ ] **Step 2: Verify lockstep.** `uv run pytest src/tests/test_executor_env_guard.py src/tests/test_sk3_mig_b_orchestrator_invariants.py src/tests/test_terraform_env_dep_parity.py -q` — Expected: PASS (the floor-matches-`_REQUIRED_SK_MIN` bridge + `==`-pin + lock parity all green).

---

## Task 2: SkillCorner bronze projection — recover `ball_z` + `is_visible`

**Files:** `src/ingestion/tracking_context.py` (`_SKILLCORNER_TRACKING_SELECT_COLS:87–102`), `src/ingestion/action_context.py` (the SC join `:1443–1458`), `src/analytics/action_context/convert.py` (`_SKILLCORNER_CONSUMED_COLS:32`).

- [ ] **Step 1: Write a failing test** in `test_sk_frame_adapters.py` (created in Task 3) asserting the adapter receives `ball_z`/`is_visible` and emits non-NaN `z` for the ball — see Task 3.
- [ ] **Step 2: Add `ball_z`, `is_visible` to `_SKILLCORNER_TRACKING_SELECT_COLS`** (the Spark SELECT imported by both AC + legacy drivers). They already exist in `bronze.skillcorner_tracking` (dtype-overridden in `skillcorner_tracking.py:32–45`).
- [ ] **Step 3: SkillCorner join (`action_context.py:1443–1458`)** — the silly-kicks builder's contract wants `team_id` (not `team`) and `is_goalkeeper`. Change the matches-meta `.alias("team")`→`.alias("team_id")` and carry `ball_z`/`is_visible` from the tracking side through the join (they're on `skillcorner_tracking`, not `skillcorner_matches`, so they survive the left join automatically once SELECTed). Mirror in the legacy join (`tracking_context.py:1567–1583`) per D4.
- [ ] **Step 4:** `_SKILLCORNER_CONSUMED_COLS` (convert.py) is deleted with the old builder (Task 5), so no edit needed there — the adapter pins its own contract.

---

## Task 3: SkillCorner adapter (`sk_frame_adapters.py`)

**Files:** Create `src/analytics/action_context/sk_frame_adapters.py`; Test `src/tests/action_context/test_sk_frame_adapters.py`.

- [ ] **Step 0: Confirm the velocity step (D3).** Locate `_derive_velocities_savgol` (or the current vx/vy derivation applied to AC frames). Record its import path + signature here before writing the adapter. *(Do not invent it — grep `velocit` under `src/analytics/action_context/` and `src/ingestion/tracking_context.py`.)*
- [ ] **Step 1: Failing tests** (`test_sk_frame_adapters.py`): build a synthetic post-join SC bronze DF (centre-origin x/y, `ball_z`, `is_visible`, `team_id`, `is_goalkeeper`), call `convert_skillcorner_bronze_to_frames(...)`, assert: ball `z` ≈ bronze `ball_z` (NOT NaN); `visibility` mapped; coords in SPADL 105×68; ids object-strings; output columns == the AC result-frame schema; `vx`/`vy` present (velocity step applied); home GK low-x after LTR.
- [ ] **Step 2: Implement** `convert_skillcorner_bronze_to_frames(bronze, *, home_team_id, period_relative_time, derive_velocities=True)`:
  - Call `silly_kicks.tracking.skillcorner.convert_to_frames(bronze, home_team_id=str(home_team_id), output_convention="ltr")`. Under B′ the dispatch already rebased the input to period-relative; the builder re-subtracts the nominal offset internally but its clock output is discarded (next bullet). Coords/`ball_z`/GK/orientation are time-independent (reviewer-verified analytically; the Task-0 guard confirms byte-identical coords vs raw input).
  - **OVERWRITE `time_seconds` (B′):** map-join the builder output onto `period_relative_time` (the dispatcher's period-relative clock) on `(frame_id, period_id)` — NOT positional (the builder drops NaN-ball + malformed rows, so positions won't align).
  - Apply the lakehouse velocity step (Step 0) if `derive_velocities` (D3); the builder's `speed` is recomputed/discarded.
  - Reindex/rename the silly-kicks frame columns to the AC result-frame schema. **Pin this mapping as a module constant + assert it in the test** so a silly-kicks schema change fails loudly.
  - Return `(frames, report)`; the AC pipeline consumes just `frames` from `_convert_tracking_batch` — keep that contract (drop/log the report).
- [ ] **Step 3:** Run `uv run pytest src/tests/action_context/test_sk_frame_adapters.py -q` → PASS.

---

## Task 4: Metrica adapter

**Files:** add `convert_metrica_bronze_to_frames` to `sk_frame_adapters.py`; tests in `test_sk_frame_adapters.py`.

- [ ] **Step 1: Failing tests:** synthetic metrica bronze (JSON `home_players`/`away_players` 0–1 coords, `gk_jersey_numbers`), a per-team roster dict, call the adapter, assert: rescale `*105/*68` (no y-flip); ball `z` NaN; ids = roster pids; home GK low-x post-LTR; schema == AC result-frame schema. **Plus the two Rev-2 gates:**
  - **Mid-period-batch clock (the D1 gate):** feed a batch whose frames do NOT start at the period's first frame (i.e. `time_seconds`/`frame` begin mid-period) and assert the adapter output `time_seconds` is period-relative-**from-period-start** (equal to the lakehouse clock passed in), NOT re-zeroed to the batch start. This test FAILS on a naive "delegate to builder" impl and passes only with the clock-overwrite.
  - **No-synthetic-id (the D5 gate):** with a roster missing one acting jersey, assert the adapter surfaces it (count / raises) and that no `"Home_"/"Away_"` synthetic id appears among linked-action player_ids.
- [ ] **Step 2: Implement** `convert_metrica_bronze_to_frames(bronze, *, home_team_id="Home", jersey_to_player_id, period_relative_time, derive_velocities=True)`:
  - Call `silly_kicks.tracking.metrica.convert_to_frames(bronze, home_team_id="Home", jersey_to_player_id=..., output_convention="ltr")` for coords/`ball_z`/GK/orientation.
  - **OVERWRITE `time_seconds` (D1/B′):** the builder's per-(period)-min rebase is batch-broken under 250-frame batching; replace the builder output `time_seconds` with the lakehouse period-relative clock (`period_relative_time`, computed by the dispatcher via the frame-number method and passed in) via a **`(frame_id, period_id)` map-join** — NOT positional (the builder drops NaN-ball + malformed-JSON rows, so positions won't align). Do NOT use the builder's clock.
  - Apply velocities (D3); map to the AC result-frame schema (pinned constant + asserted).
  - **Build the per-team roster dict in the dispatcher (Task 5)** with outer keys exactly `"Home"/"Away"` (we own identity, D5).
- [ ] **Step 3:** PASS (incl. both Rev-2 gates).

---

## Task 5: Rewire the AC dispatch (`pipeline.py::_convert_tracking_batch`)

**Files:** `src/analytics/action_context/pipeline.py` (`:228–324`), `src/ingestion/action_context.py` (dispatcher rebase `:1615–1618`).

- [ ] **Step 1:** Replace the `metrica` branch (`:257–269`) — build the per-team roster `{"Home":{jersey:pid},"Away":{...}}` from `actions` (split by `team_id` + `player_id_native`, D5). **Assert `set(actions["team_id"]) == {"Home","Away"}` for metrica** (the builder hard-codes those labels at `metrica.py:77,133,146`; a numeric/club-name `team_id` would make the builder's roster merge silently miss → every player → synthetic fallback). Compute the lakehouse period-relative clock (frame-number method) and pass it as `period_relative_time`; call `convert_metrica_bronze_to_frames(...)`.
- [ ] **Step 2:** Replace the `skillcorner` branch (`:271–273`) — call `convert_skillcorner_bronze_to_frames(pdf, home_team_id=meta.home_team_id, period_relative_time=...)`.
- [ ] **Step 3: Clock — B′ (D1, highest risk; UNIFORM overwrite + single-source the SC constant):**
  - **KEEP both dispatch rebases** (`pipeline.py:575–583` metrica frame-number; `:592–596` SC) — ownership (`_owned_action_ids`) keeps consuming period-relative input; **do NOT delete the dispatch rebase** (that was the A path and would break ownership).
  - **Re-point the SC rebase offset to silly-kicks:** change `pipeline.py:593` (and `action_context.py:1615–1618`) from `from analytics.action_context.convert import _SKILLCORNER_PERIOD_START_SECONDS` → import `_PERIOD_START_SECONDS` from `silly_kicks.spadl.skillcorner` (public accessor once exposed; private + guard test interim). The lakehouse copy is deleted in Task 8.
  - **Both adapters overwrite the builder's `time_seconds`** with the dispatch's period-relative clock via a `(frame_id, period_id)` map-join (Task 3/4).
  - Regression tests (the D1 gates): mid-period metrica batch → period-relative-from-period-start (Task 4 Step 1); SC 2-period synthetic asserting P2 min ≈ 0 (not −2700, not 2700), mirroring `test_skillcorner_dispatch_time_base.py`; **+ the cross-batch GK/orientation consistency guard** (per `(match,team)`: derived GK `player_id` + per-period flip identical across all batches — Rev-3 MEDIUM).
- [ ] **Step 4: Orientation (D2, Rev 4 — DELETE the net):** remove the `correct_frames_to_home_ltr` call at `:317–324` (the function itself is deleted in Task 8). On 4.34.0 all four providers are oriented upstream. **Gate: `test_frame_orientation_golden.py` (idsse / skillcorner / gradientsports-ET 10517_p3) stays green without the net** — the ET case is the GS-ET backstop acceptance. The Rev-2 metrica `home_team_id="Home"` tail-net special-case is dropped (no net to feed).
- [ ] **Step 5:** `uv run pytest src/tests/action_context/ -q` → triage failures into Task 7.

---

## Task 6: Migration gate (diff-explainer, NOT a correctness gate)

**Files:** Create `test_sk_builder_migration_gate.py`.

- [ ] **Step 1:** On the committed SC fixture (`src/tests/fixtures/action_context/skillcorner/1886347_p2/frames.parquet` is the *built* frames; use the raw bronze slice or reconstruct the post-join bronze), run the OLD `_bronze_skillcorner_to_frames` (kept temporarily on a git stash / a copied snapshot) vs the new adapter, and assert: `z` newly populated (old NaN, new non-NaN); `team_attacking_direction` labeled; **all other coordinate columns byte-equal within float tol**. Document that the old builder is the *suspect*, so this only explains the diff — the **goldens (Task 7) are the correctness gate**.
  - **Rev 2 — the slice MUST be multi-batch** (span >1 × 250 frames in a period) for both metrica and SC, otherwise the gate passes while masking the D1 metrica clock bug (a single-batch slice never re-zeroes). Assert the slice covers ≥2 `frame_batch_id`s per period.
- [ ] **Step 2:** PASS. (If non-z/non-orientation columns differ, STOP — that's an unexplained behavioural change to investigate before deletion.)

---

## Task 7: Update / retire builder-coupled tests; keep goldens green

The goldens are the acceptance oracle — they must stay GREEN, proving the silly-kicks builders preserve the coordinate truth:
- [ ] **`test_frame_orientation_golden.py`** (idsse/skillcorner/GS fixtures, home-GK-low) — must stay green unchanged.
- [ ] **`test_frame_y_identity_golden.py`** (skillcorner off-centre y-identity, `_Y_IDENTITY_MAX=1.5m`) — must stay green; this is the SC event-anchored y proof.
- [ ] **`test_metrica_builder_y.py`** — re-point from `convert._bronze_metrica_to_frames` to the metrica adapter; assert `y01*68` no-flip still holds (`_EXPECTED_Y=17.0`).
- [ ] **`test_convert_drift.py`** — the AST drift guards (`convert.py` vs `tracking_context.py`) are obsolete once both copies are deleted. Replace with a structural guard: both dispatchers (`pipeline._convert_tracking_batch`, `tracking_context._make_tracking_context_udf`) reference the shared adapters and neither defines `_bronze_*_to_frames`.
- [ ] **`test_skillcorner_frame_time_base.py` / `test_skillcorner_dispatch_time_base.py`** — under B′ the dispatch still rebases (ownership unchanged), so the period-relative output assertions (P2 min ≈ 0) STAY. Re-point the `_SKILLCORNER_PERIOD_START_SECONDS`-equality assertions from the deleted `convert.py`/`tracking_context.py` copies to the **silly-kicks source** — i.e. the new value-guard test asserts the imported `silly_kicks.spadl.skillcorner._PERIOD_START_SECONDS == {1:0.0, 2:2700.0, 3:5400.0, 4:6300.0, 5:7200.0}` (catches upstream drift). The converter-pass-through test (`test_skillcorner_frame_time_base`) is replaced by the adapter map-join overwrite test (Task 3).
- [ ] **`test_tracking_context_converters.py`, `test_metrica_tracking_player_id.py`** — re-point imports to the adapter or retire the builder-internal assertions (player-id format, GK detection) now owned upstream; keep any assertion still meaningful against the adapter.
- [ ] **`test_tracking_context_skillcorner_local.py`** — fixture-gated integration; re-point to the adapter, keep the DAS/PC/linkage-rate assertions (they validate the end-to-end enrichment, provider-agnostic).
- [ ] Run `uv run pytest src/tests/action_context/ -q` → all green.

---

## Task 8: Delete the legacy + AC builder copies (D4)

- [ ] **Step 1:** Delete `convert.py::_bronze_{metrica,skillcorner}_to_frames`, `_SKILLCORNER_CONSUMED_COLS`, `_JERSEY_RE` if now unused, AND `convert.py::_SKILLCORNER_PERIOD_START_SECONDS` (B′ — the dispatch now imports the offset from `silly_kicks.spadl.skillcorner`, re-pointed in Task 5 Step 3, so nothing imports the lakehouse copy). **Verify no remaining `from analytics.action_context.convert import _SKILLCORNER_PERIOD_START_SECONDS`** (grep) before deleting — this is the ImportError the Rev-2 plan would have hit.
- [ ] **Step 2:** Delete `tracking_context.py::_bronze_{metrica,skillcorner}_to_frames` + its `_SKILLCORNER_PERIOD_START_SECONDS` copy; rewire `_make_tracking_context_udf` (`:441–473`) to the adapters (build the metrica roster dict there too); re-point the legacy path's SC dispatch rebase to the silly-kicks offset (same as Task 5 Step 3, B′).
- [ ] **Step 3 (Rev 4): Delete the in-repo net.** Remove `pipeline.py::correct_frames_to_home_ltr` (`:80`) and its dispatch call (`:317–324`); grep for every other caller (the legacy `tracking_context.py` tail, `test_frame_ltr_correction.py`, any `from ... import correct_frames_to_home_ltr`) and re-point/retire. `test_frame_orientation_golden.py` run without the net is the acceptance gate (D2). The unit `test_frame_ltr_correction.py` (which tested the now-deleted function) is retired; its orientation coverage moves to the cross-provider golden.
- [ ] **Step 4:** `uv run ruff check src/ && uv run pyright src/` — no dead imports / undefined refs.

---

## Task 9: ADR + docs

- [ ] Amend `ADR-053` Status/Consequences (Rev 4): the geometric net is promoted to silly-kicks `orient_frames_to_ltr_by_geometry` (ADR-034) and the GS/sportec native-adapter backstop (ADR-035); the in-repo `correct_frames_to_home_ltr` is **DELETED** (all four providers now oriented upstream — SC/metrica via the builders, idsse/GS via the backstop). Note Phase B (re-materialization) **and the period-5/PSO preflight** as consequences. Reference ADR-034 + ADR-035.

---

## Task 10: Full verification + final-review + commit (approval-gated)

- [ ] **Step 1:** `uv run ruff check src/ scripts/ && uv run ruff format --check src/ scripts/ && uv run pyright src/ && uv run pytest src/tests/ -q` — all green (FULL suite, sentinel-critical).
- [ ] **Step 2:** Run `/final-review` (incl. C4 if structure changed).
- [ ] **Step 3:** **STOP. Present for user approval before any commit.** Phase B (re-materialize SC/metrica `fct_action_context` → mart/synced → downstream) is a SEPARATE, compute-gated step requiring its own go-ahead.

---

## Self-review (plan author)

**Spec coverage:** pin+sentinels→T1; ball_z recovery→T2/T3; SC adapter→T3; metrica adapter→T4; dispatch rewire + clock + orientation→T5 (D1/D2); migration gate→T6; goldens preserved + tests updated→T7; delete both copies→T8; ADR→T9.
**Decisions surfaced (not hidden):** D1 clock ownership (HIGHEST risk; Rev 2 made it asymmetric + offered uniform-overwrite option B), D2 net **deletion** (Rev 4 — full delete on 4.34.0, gated by the cross-provider golden; supersedes the Rev-2/3 "keep as no-op + metrica home_team_id tail-net"), D3 velocity, D4 legacy scope, D5 metrica roster/GK + fallback-id Hyrum — all flagged with recommendations.
**Rev 2 fixed the HIGH metrica-clock bug; Rev 3 DECIDED D1 = B′ and propagated it** (uniform `(frame_id,period_id)` map-join overwrite for both providers; keep both dispatch rebases so M13 ownership is untouched; delete the lakehouse SC constant and import the offset from silly-kicks → single-sourced). Added: metrica cross-batch GK/orientation consistency guard (the "batch-invariant" claim was unevidenced); metrica `team_id == {"Home","Away"}` assert.
**Remaining items for the reviewer:** (1) silly-kicks `_PERIOD_START_SECONDS` is private — accepting the upstream offer to expose it publicly; interim = private import + value-guard test. (2) Task 6 needs the *raw post-join bronze* fixture spanning ≥2 batches per period (Databricks extract like the DFL slice). (3) Velocity function location (D3) is a Task-3-Step-0 lookup, deliberately not invented. (4) A (full clock-upstream + ownership re-anchor) is recorded as the eventual post-Phase-A cleanup.
**Type consistency:** adapters return `(frames, report)` but the AC dispatch consumes only `frames` — the adapter or the call site must drop the report; pinned in T3/T4. The silly-kicks→AC frame-column mapping is a pinned constant + asserted, so an upstream schema change fails loud.
