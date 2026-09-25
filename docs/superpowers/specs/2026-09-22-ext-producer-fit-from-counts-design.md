# ExT producer — distributed `fit_from_counts` rewrite — Design

**Status:** Spec **APPROVED** (fresh-session review 2026-09-22). Implementation gated on owner approval of the
(revised) plan. The sk dependency is **RELEASED** — `ExpectedThreat.fit_from_counts` (SK-XT-COUNTS, ADR-102)
shipped in silly-kicks **4.123.0** (merged `df46c85`, tagged `v4.123.0`, PyPI-live; impl reviewed + APPROVED).
**Cycle:** sk4118 P2. **Supersedes** the *fit mechanism* in `docs/superpowers/plans/2026-09-17-sk4118-p1-metrics.md`
Task G1 (the `.fit(actions)` producer) — everything else in G1 (the `to_dict` JSON schema, the zone projection,
the loader/consumer re-points) stands.

## Context

`compute_expected_threat` (`src/ingestion/expected_threat.py::run_pipeline`) fits a per-competition + a
`global` sk `ExpectedThreat` (16×12) over the gold action corpus. Because sk exposes only `.fit(actions)`,
the producer pulls each competition's actions to the Spark **driver** via `.toPandas()` (~30 sequential
reads), keeps every slice, and `pd.concat`s them (~1 GB) for the global fit. It is driver-bound and
single-threaded; in P2 (2026-09-22) it hit the 600 s task timeout with **0/28 grids written**. It is also a
scale cliff — cost grows with the corpus.

The in-repo v1 producer used single-pass distributed ZoneCounter accumulation (one Spark aggregation → per-zone
counters → fit from counters, no full-corpus driver pull). ADR-085 **reverted** that when moving to sk
`ExpectedThreat`, because sk had no additive counts-based fit. SK-XT-COUNTS restores that capability:
`ExpectedThreat.fit_from_counts(**counts)` is byte-identical to `.fit(actions)` on the same aggregates, and sk
now documents the zone-binning contract. This design rewrites `run_pipeline` onto it.

**Live measurement (2026-09-22, OAUTH):** valid-start/NaN-end `pass|dribble|cross` moves in
`fct_action_values` = **0** across all 30 competitions / **7.93 M moves** (`valid_start == valid_start_end`
everywhere). So the current `.fit()` grids do **not** diverge from a counts-fit today; the counts contract is
byte-identical here. The distinction between the two move populations (below, §Design) is still a hard contract
because a future re-ingest / new provider / the pending metrica-y / IDSSE-cross fixes could introduce NaN-end
moves — the rewrite must honour it unconditionally, not lean on the current 0.

## Goal

Produce **byte-identical grids** to today's `.fit()` path via a **distributed count aggregation** — one Spark
pass computing the five count arrays per `competition_id`, `global` as their element-wise sum, then
`fit_from_counts` per grid. Eliminate the ~30 `.toPandas()` reads and the ~1 GB `pd.concat`; the only data
crossing to the driver is the tiny count tables (tens of thousands of rows, not 7.9 M actions).

## Non-goals

- **No mart / schema change.** Still writes bronze `expected_threat_grids` (`xt_model_json` JSON) +
  `expected_threat_grid_zones` (long-form projection). Same `_RESULTS_SCHEMA` / `_ZONES_SCHEMA`.
- **No change to the guard / scope decision** (`_ExpectedThreatGuard.check`, `_decide_rebuild`), the zone
  projection (`_project_physical_zones`), the materiality / directionality / structural guards, or the
  watermark. They operate on `model.xT`, which `fit_from_counts` sets identically.
- **No downstream retrain.** Grids are byte-identical; the AC re-materialize / VAEP / off-ball-xt in P2 proceed
  on the same corpus but are not *triggered* by any grid change from this rewrite.
- **No KDE.** singh only — matches the current default (`_fit_sk_grid` never passes `method="kde_smoothed"`).
- **No env change.** The task already moved to `analytics` (PR #583); it stays there.

## Design

### 1. One binning primitive: replicate `_get_flat_indexes` in Spark SQL

The counts are correct only if the lakehouse bins `(x, y) → flat zone` with the **exact** boundaries sk uses.
sk (`silly_kicks/xthreat/_grid.py:16-26`) does, for `l=16, w=12`, `field_length=105.0`, `field_width=68.0`:

```
xi = clip(int(x / 105.0 * 16), 0, 15)      # .astype("int64") truncates toward zero, then .clip
yj = clip(int(y / 68.0  * 12), 0, 11)
flat = (12 - 1 - yj) * 16 + xi              # y-inverted, C-order  (yj.rsub(w-1).mul(l).add(xi))
```

Spark SQL, on the canonical-LTR `start_x/start_y` (and `end_x/end_y` for transitions):

```sql
LEAST(GREATEST(CAST(start_x / 105.0 * 16 AS INT), 0), 15)                                   AS xi,
LEAST(GREATEST(CAST(start_y / 68.0  * 12 AS INT), 0), 11)                                   AS yj,
(12 - 1 - LEAST(GREATEST(CAST(start_y/68.0*12 AS INT),0),11)) * 16
  + LEAST(GREATEST(CAST(start_x/105.0*16 AS INT),0),15)                                      AS flat_start
```

`CAST(double AS INT)` in Spark truncates toward zero, identical to pandas `.astype("int64")`; for the
non-negative canonical coordinates this equals floor, so it matches sk exactly (edges: `x=105 → 16 → clip 15`;
`y=68 → 12 → clip 11`). Dims `105/68/16/12` are module constants already present (`_PITCH_LENGTH`,
`_PITCH_WIDTH`, `_XT_L`, `_XT_W`); a parity test (D-TEST-1) pins the SQL output against sk `_get_flat_indexes`.

### 2. Five counts, all flat-indexed, one pass per `competition_id`

All five derive from `flat` (not `(yj, xi)` group keys) so the driver-side arrays reshape to the **same
y-inverted `(w, l)` layout sk's `_count` produces** (`_grid.py:52-56`), eliminating the layout-inversion class
of bug:

| count | rows | grouping | driver array |
|---|---|---|---|
| `shot_counts` | `action_type='shot'`, valid start | `flat(start)` | `vector[192] → reshape(12,16)` |
| `goal_counts` | `action_type='shot' AND action_result='success'`, valid start | `flat(start)` | `reshape(12,16)` |
| `move_counts` | `action_type IN ('pass','dribble','cross')`, **valid start** | `flat(start)` | `reshape(12,16)` |
| `transition_start_counts` | moves, **valid start AND end** | `flat(start)` | `reshape(12,16)` (fit_from_counts `.ravel()`s it) |
| `transition_counts` | moves, `action_result='success'`, **valid start AND end** | `(flat(start), flat(end))` | dense `(192, 192)` |

**The D1-SPEC-01 lakehouse mirror (hard contract, 0 divergence today but distinct by definition):**
`move_counts` filters **valid start only** (feeds sk `_action_prob`, which masks NaN start only); it is NOT the
same population as `transition_start_counts` (**valid start AND end** — the Singh denominator, sk
`_transitions.py:32` `dropna(subset=[start_x,start_y,end_x,end_y])`). Reusing one array for both is the exact
error SK-XT-COUNTS r1 flagged BLOCKING.

**Only `{pass, dribble, cross, shot}` rows are counted.** sk's `_scoring_prob` (`_grid.py:80`) filters
`type=='shot'` only (not `shot_penalty`/`shot_freekick`); `_get_move_actions` (`:105-109`) is `pass|dribble|cross`
only. The other `_RELEVANT_TYPES` (`throw_in`, `freekick_*`, `corner_*`, `take_on`, `goalkick`, `clearance`,
`shot_penalty`, `shot_freekick`) are handed to `.fit()` today but never counted by `_scoring_prob`/`_action_prob`/
singh — so the count SQL restricts to the four, and the grids stay byte-identical.

### 3. Global = Σ per-comp counts (additivity, one pass)

Counts of disjoint action sets sum element-wise (SK-XT-COUNTS §5). `global`'s five arrays are the element-wise
sum of the per-competition arrays — computed in the driver from the already-collected tiny count tables, no
second Spark pass, no corpus pull. This is the OPT-1 single-pass restoration ADR-085 had to undo.

### 4. Fit + persist (unchanged surface)

Per comp + global:
`ExpectedThreat(l=16, w=12).fit_from_counts(shot_counts=…, goal_counts=…, move_counts=…, transition_counts=…,
transition_start_counts=…)` → `to_dict()` → `expected_threat_grids`; `_project_physical_zones(model)` →
`expected_threat_grid_zones`. `_write_grid_if_material` (materiality), `assert_directional` (per-comp ≥ 5000 +
global HARD), `validate_structural` (global HARD, max 0.50), and `record_watermarks` (only after success) are
**unchanged** — all read `model.xT`.

### 5. Deletions (Chesterton's-fence audit required)

The driver-pull path — `_load_actions_for_competition`, `_to_sk_actions`, `_fit_sk_grid` — is removed. Grep for
other importers first; delete only what nothing else uses. Remove the `_topandas_exemptions.yml` entry that
covered the global fit and re-run `test_topandas_boundedness`. `scripts/compute_xt_grid_hf.py` (the manual HF
producer, not the timed daily task) either mirrors the rewrite or stays on `.fit` — decided in the plan
(it is not on the timeout critical path).

## Decisions

- **D1 — SQL binning primitive, not `zones_of`.** sk can't be called per-row inside a Spark aggregation, so the
  binning is replicated in SQL from the documented `_get_flat_indexes` formula. A probe-grid parity test pins it
  against sk (fails if `spadlconfig.field_length/width` or `_XT_L/_XT_W` ever drift).
- **D2 — flat-indexed counts + driver reshape.** Every count groups by the flat index and reshapes to `(w, l)`
  exactly as `_count` does, guaranteeing the y-inverted layout matches byte-for-byte. No `(yj, xi)` grouping.
- **D3 — count only `{pass, dribble, cross, shot}`.** The sk grid ignores the other action types even though
  `.fit` is handed them (verified against sk source).
- **D4 — global via Σ per-comp counts** (additive) — restores the single distributed pass.
- **D5 — sk pin bump.** `_REQUIRED_SK_MIN` (all 7 sites, grep `src/` + `scripts/`), the TF env pins
  (`sync_tf_env_pins.py`, not hand-edited), `uv.lock`, `submit_ac1_oneshot.py`, and the bump sentinel test move
  to the `fit_from_counts` release; `main()` keeps a runtime `_REQUIRED_SK_MIN` assertion. (See the sk-bump
  sentinels reference.)
- **D6 — byte-identity is the acceptance gate.** A test fits BOTH `.fit(actions)` and
  `fit_from_counts(<agg of actions>)` on a fixture and asserts `np.array_equal` on all four matrices +
  `np.allclose(xT)`; the fixture **synthesizes** a valid-start/NaN-end move (real data has 0 — §Context) so the
  `move_counts` vs `transition_start_counts` split is actually exercised. A post-merge live spot-check on one
  competition confirms the deployed grid is unchanged.

## Tests

- `src/tests/test_expected_threat_producer.py` — extend:
  1. **SQL-binning parity** — the SQL flat-index expression == sk `_get_flat_indexes` on a probe grid incl.
     edges `x∈{0,105,120}`, `y∈{0,68,-5}` and the `15`/`11` clamp. (D-TEST-1 / D1.)
  2. **count-agg correctness** — on a small SPADL fixture the five arrays equal sk's own `_count` / singh
     intermediates (`_scoring_prob` shot/goal, `_action_prob` move, singh start/transition).
  3. **byte-identity** (D6) — `fit_from_counts(<agg>)` vs `.fit(fixture)`: `array_equal` on all 4 matrices +
     `allclose(xT)`, **fixture includes a synthesized valid-start/NaN-end move** (non-vacuity for the
     move/transition-start split) + a 0-goal/0-action zone + an off-pitch end.
  4. **dims parity** — `_PITCH_LENGTH/_WIDTH/_XT_L/_XT_W` match `spadlconfig.field_length/width` + sk defaults.
  5. **global = Σ per-comp** — on a 2-competition fixture the global arrays equal the element-wise sum, and the
     global grid equals `.fit(concat)`.
  6. **excluded-type non-vacuity** (EXT-PLAN-01) — the fixture carries a `shot_penalty`/`shot_freekick` (NOT a
     `shot`) and a `take_on`/`throw_in` (NOT a `move`) each in an otherwise-empty zone; assert those zones'
     `shot_counts`/`move_counts` == 0, AND (control) that the same zones ARE counted when the rows are re-typed
     to `shot`/`pass`. Byte-identity (test 3) alone is vacuous for the type filter — a fixture with no excluded
     types cannot catch a SQL bug that counts a penalty as a shot / a take-on as a move (the real corpus HAS
     both). The producer's SQL type lists derive from `ExpectedThreat.SHOT_TYPE_NAME`/`MOVE_TYPE_NAMES` (single
     source vs sk), so the filter cannot silently drift from `.fit`.
- Guard tests (`_decide_rebuild`, directionality, structural, materiality) unchanged — assert still green.
- Full seven-gate + `validate_workflow_cards` (the 8th) + `test_topandas_boundedness` after the exemption
  removal + the sk-bump sentinel.

## Blast radius

`src/ingestion/expected_threat.py` (`run_pipeline` + the three deleted helpers + a new count-agg helper),
`src/tests/test_expected_threat_producer.py`, the sk pin sites (D5), `_topandas_exemptions.yml`, possibly
`scripts/compute_xt_grid_hf.py`. **No** dbt model, mart schema, workflow card (env already `analytics`), C4,
or downstream-consumer change. Card `wf-xt-grids.yaml` unchanged.

## Ship

Spec (this) → lakehouse review → plan → lakehouse review → **[sk releases 4.123.0]** → implement → unbiased
impl review → **STOP for explicit owner approval** → single commit on the P2 branch → owner-gated push / PR /
CI / admin-merge → P2 recompute (`run_now(only=["compute_expected_threat"])` first, then the AC drain etc.).
No `git commit` / push / run_now without explicit owner approval for that specific action.
