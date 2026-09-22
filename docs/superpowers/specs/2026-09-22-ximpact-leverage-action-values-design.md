# xImpact + win-prob leverage on `fct_action_values` — Design

**Status:** DRAFT (awaiting review + owner approval). **Cycle:** sk4118 P2. **Sibling of** the ExT-producer
`fit_from_counts` rewrite (`2026-09-22-ext-producer-fit-from-counts-design.md`): both land on the **same P2
branch**, as **two coherent commits in one PR**, and both must merge **before the P2 operator recompute** so a
single recompute session materializes both. sk dependency **4.123.0** carries TF-63 xImpact (ADR-101); this
adopts it. Owner-approved forks (2026-09-22): bundled WP model; fold under `wf-vaep`; 2-commit/1-PR; include
the player rollup; exclude the in-game `p_win/p_draw/p_loss` trio.

## Context

sk 4.122.0 (TF-63, riding along in the 4.123.0 bump) adds `VAEP.rate_ximpact` + `silly_kicks.win_probability`
(`goal_leverage`, bundled `WinProbabilityModel`). **xImpact = `VAEP_adjusted × ΔP(win | goal at the pre-action
state)`** — a late go-ahead goal scores high, a garbage-time goal ≈ 0 (Paul, Klemp & Memmert 2025). It is a
per-ACTION value, the same grain as the lakehouse's existing `vaep_value` / `vaep_adjusted_value` (P1 Phase D)
on `fct_action_values`.

Adopting it **now** (bundled with the P2 recompute) avoids a second full `compute_spadl_vaep` corpus pass: the
corpus is 4.90.1-materialized (no re-ingest), and `compute_spadl_vaep` is **not otherwise re-run in P2** (the
ExT/AC work does not change VAEP columns). Folding xImpact in **adds one `compute_spadl_vaep` full re-run** to
the P2 operator session (to populate the 2 new bronze columns per the AC-recompute-for-new-column pattern) —
one more task in the same window, not a separate cycle.

## Goal

Two new per-action columns on `fct_action_values`, computed in `ingestion.spadl_vaep` via the bundled
`WinProbabilityModel`:
- **`win_prob_leverage`** — `goal_leverage` = `ΔP(win | goal)` = `P(win | score+1) − P(win | score)` at the
  action's pre-action game state (sk `win_prob_leverage`, unit=probability, higher=better).
- **`ximpact`** — `vaep_adjusted_value × win_prob_leverage` (sk `ximpact_values`).

Plus a per-`(competition_id, team_id, player_id, action_type)` **`total_ximpact` = `sum(ximpact)`** rollup on
`fct_vaep_breakdown_agg` (dbt-only, mirrors `total_vaep`).

## Non-goals

- **No in-game `p_win`/`p_draw`/`p_loss`** — those per-action base names collide with `fct_match_outcome`'s
  pre-match trio (team-match grain, P1 Phase A). `goal_leverage` needs the WP chain internally but emits only
  the leverage scalar; the trio is out of scope (a later cycle would need disambiguated names, e.g.
  `ingame_p_win`).
- **No lakehouse WP-model fit** — the sk `WinProbabilityModel.bundled()` (public StatsBomb corpus, 3961 matches,
  OOF ECE 0.017, SHA256-verified) is used as-is, exactly like `XSuccessModel.bundled()` / `PassCompletionModel`.
- **No new mart / grain / workflow card** — per-action → `fct_action_values`; player rollup → the existing
  `fct_vaep_breakdown_agg`; governance folds under the existing `wf-vaep` (already in
  `PER_PLAYER_EVALUATIVE_CARDS`). No re-materialize of anything the ExT rewrite doesn't already touch beyond the
  `compute_spadl_vaep` re-run.

## Design

### 1. Compute (`ingestion.spadl_vaep`)

The VAEP writer already computes `rate_adjusted` per game (`spadl_vaep.py:594`), caches
`XSuccessModel.bundled()`, and carries `home_team_id_native`. Extend the per-game compute:

```
adjusted = vaep_model.rate_adjusted(game, game_actions, xsuccess_model)   # existing → vaep_adjusted_value (DataFrame)
leverage = goal_leverage(game_actions, model=wp_model, games=<(game_id, home_team_id)>)   # → win_prob_leverage
ximpact  = ximpact_values(adjusted, leverage)                             # sk core → ximpact (pass the DataFrame)
```
**XIMP-01 — pass the `rate_adjusted` DataFrame, NOT `adjusted["vaep_value"]`.** `ximpact_values(adjusted:
pd.DataFrame, leverage)` reads `v = adjusted["vaep_value"]` internally (`vaep/ximpact.py:36`); a Series has no
`["vaep_value"]` → per-game `KeyError`. **Index alignment:** `ximpact_values` does `leverage.reindex(v.index)`,
so `adjusted` and `leverage` must share an index. The writer currently does
`rate_adjusted(...).reset_index(drop=True)` (`:594`); compute `ximpact`/`leverage` on a CONSISTENT index —
either call `ximpact_values` before that reset (both on the `game_actions` index, as `VAEP.rate_ximpact` does),
or reset `game_actions`/`leverage` to match — else the reindex silently NaNs every row.
- `wp_model = WinProbabilityModel.bundled()` — added to the module cache alongside `XSuccessModel.bundled()`.
- **`games` frame home_team_id is the hashed BIGINT** `shared.identifiers.hash_native_id_to_bigint(home_team_id_native)` (ADR-016), so `goal_leverage`'s `same_id(team_id, home_id)` matches the SPADL `team_id` space (which is already the hashed BIGINT). Do NOT pass the native string.
- `goal_leverage` derives per-action state (score_diff, minutes_remaining, man_advantage, home) from the action stream (`win_probability._state.derive_match_states`) — inputs `game_id, action_id, period_id, team_id, time_seconds, type_id, result_id, player_id` are all present. Period 5 (PSO) is excluded upstream by the state offset (matches the AC/xT convention).
- **Honest-NaN (sk contract):** an unresolved state (no home_id, or a non-two-team group) → NaN leverage → NaN ximpact. Never fabricated. Matches `rate_ximpact`'s NaN-propagation.
- **`ximpact` is byte-consistent with `VAEP.rate_ximpact`** — computing `goal_leverage` explicitly + `ximpact_values(adjusted, leverage)` is exactly what `rate_ximpact` does internally; we split it only to emit both columns.

### 2. Schema (ADR-016 parity)

Add `ximpact DOUBLE, win_prob_leverage DOUBLE` to `_VAEP_SCHEMA` (`spadl_vaep.py:131`) AND the applyInPandas
`StructType` (`:1085`). Parity gated by `test_spadl_vaep_writer_parity` (closes the LL1 silent-drop class).

### 3. Bronze migration

`scripts/migrations/2026-09-22-vaep-action-values-add-ximpact.sql` — `ALTER TABLE … ADD COLUMNS (ximpact
DOUBLE, win_prob_leverage DOUBLE)` on `vaep_action_values` (single-leading-column idempotent; operator-applied
WITH the merge). The `compute_spadl_vaep` re-run then backfills them corpus-wide.

### 4. dbt

- `stg_spadl__action_values.sql` — pass through `ximpact`, `win_prob_leverage`.
- `fct_action_values.sql` — +2 columns; `_marts__models.yml` contract entry (contract enforced;
  `test_marts_models_yml_completeness`).
- `fct_vaep_breakdown_agg.sql` — `sum(ximpact) as total_ximpact` (cast double), mirroring `total_vaep`.

### 5. Governance (wf-vaep — existing evaluative member)

xImpact is a new **output** of the existing `wf-vaep` per-player-evaluative system (not a new system), so **no
new card**:
- `AI_GOVERNANCE.md` §5 — note the xImpact/leverage output under wf-vaep.
- `docs/huggingface/model-cards/vaep-model.md` — add the xImpact intended-use / non-use + the bundled WP-model
  dependency + the `EU AI Act — Intended Use and Non-Use` stanza coverage.
- `ARCHITECTURE.md` Appendix D — add **Dixon & Robinson (1998)** + **Robberechts, Van Haaren & Davis (2019)**
  (the WP Markov-chain lineage); Paul, Klemp & Memmert (2025) already landed in Phase D. Extend
  `expected_authors` in `test_architecture_md_appendix.py`.
- Re-run `test_ai_governance_md.py` + `test_architecture_md_appendix.py`.

### 6. HF

The `spadl_vaep` HF dataset gains 2 columns → publish via the ADR-072 guarded frame (`prepare_public_upload`,
per-row `access_tier` rides through unchanged); update the HF card + `docs/huggingface/org-card.md` + `README.md`
(HF artifact-link completeness).

## Decisions

- **X1 — bundled `WinProbabilityModel`** (offline, SHA256-verified, public-corpus). Not lakehouse-fit.
- **X2 — emit only `ximpact` + `win_prob_leverage`** (no in-game `p_win/p_draw/p_loss` → no `match_outcome`
  name collision).
- **X3 — `games.home_team_id` = hashed BIGINT** (ADR-016) so `goal_leverage`'s `same_id` matches `team_id`.
- **X4 — honest-NaN** on unresolved state; propagates to NaN `ximpact`. Never fabricated.
- **X5 — player rollup = `sum(ximpact) → total_ximpact`** on `fct_vaep_breakdown_agg` (dbt-only, no recompute
  cost).
- **X6 — rides the P2 recompute** — adds one `compute_spadl_vaep` full re-run to the P2 operator session; no
  separate cycle.

## Tests

- `test_spadl_vaep_writer_parity` — `_VAEP_SCHEMA` ↔ StructType carries the 2 new cols.
- xImpact correctness (`test_spadl_vaep.py` or a new `test_ximpact.py`): on an in-code fixture, `ximpact ==
  vaep_adjusted_value × win_prob_leverage`; a **late go-ahead** state has high leverage, a **decided/garbage-time**
  state ≈ 0 (non-vacuity, mirrors sk's fixture); an **unresolved** state → NaN leverage AND NaN ximpact
  (honest-NaN control). `win_prob_leverage` within `[−1, 1]`.
- **End-to-end byte-consistency (XIMP-02):** the writer's `ximpact` == `VAEP.rate_ximpact(game, game_actions,
  XSuccessModel.bundled(), win_prob_model=WinProbabilityModel.bundled())` on the fixture — pins the full args
  wiring + index alignment, not just the `adjusted × leverage` multiply (catches the XIMP-01 DataFrame/Series
  and any `games`/index misalignment).
- `fct_action_values` contract (+2 cols) + `_marts__models.yml` completeness.
- `fct_vaep_breakdown_agg` carries `total_ximpact`.
- `test_ai_governance_md` + `test_architecture_md_appendix` green; `test_silly_kicks_boundary` (4.123.0).
- Full seven-gate + `validate_workflow_cards` + HF publish-parity/leak-guard.

## Blast radius

`src/ingestion/spadl_vaep.py`, `scripts/migrations/2026-09-22-vaep-action-values-add-ximpact.sql`,
`dbt_project/models/staging/spadl/stg_spadl__action_values.sql`, `fct_action_values.sql`,
`fct_vaep_breakdown_agg.sql`, `_marts__models.yml`, `AI_GOVERNANCE.md`, `vaep-model.md`, `ARCHITECTURE.md`,
`test_architecture_md_appendix.py`, the spadl_vaep HF publisher + card + org-card + README, and the xImpact
test. **No** new mart, workflow card, C4 entity, or synced table. Wheel bump rides the PR.

## Ship

2nd commit on the P2 branch (after the ExT-rewrite commit) → one PR carrying both → owner-gated push / PR / CI
(incl. live DDL-parity for the migration + dbt build) → admin-merge → the operator applies the bronze migration
+ runs the P2 recompute (which now includes the `compute_spadl_vaep` re-run). **STOP for explicit owner approval
before the commit**; no commit / push / run_now without it.
