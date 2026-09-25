# ADR-084: Retire the standalone ELASTIC producer; source elastic frame linkage from the AC drain

| Field | Value |
|---|---|
| **Date** | 2026-09-20 |
| **Status** | Accepted |
| **Deciders** | Karsten |

## Context

The silly-kicks 4.90.1 → 4.120.0 adoption cycle (sk4118) brought the TF-57 ELASTIC v2 aligner. Its public entry point changed shape: `align_events_to_frames(actions, frames, *, params)` is now **action-keyed** — it consumes SPADL actions (`action_id, game_id, period_id, time_seconds, player_id, team_id, type_id`) plus sk-canonical home-LTR frames, and emits one row per matched action with seven columns (`elastic_frame_id/_confidence/_error_seconds` + `elastic_receive_frame_id/_confidence/_error_seconds`). SB360 freeze-frames are unsupported (velocity-less).

The lakehouse carried a **standalone** ELASTIC chain predating this: `ingestion/elastic_sync.py` (the `wf-elastic-sync` mega-job task) ran a **local greedy** engine (`analytics/elastic_sync.py`) over raw `bronze.idsse_events` + `bronze.idsse_tracking`, writing `(match_id, event_id)`-keyed `bronze.elastic_sync_results`. Consumers: the OBSO/PAUSA HF publisher (`publish_obso_pausa_inputs_hf.py`, INNER JOIN on `event_id`), a dbt idsse staging view (`stg_idsse__elastic_sync`, gated `pausa_enabled=false`, zero downstream `ref()`s), and a committed legacy oracle fixture. The legacy greedy engine also had a known IDSSE frame-origin bug (documented in `oracle_map.py`).

Two facts forced a decision. First, the **AC drain already produces the exact seven action-keyed elastic columns** for IDSSE (via `add_elastic_sync` in `analytics/action_context/enrich.py`), landing them on `fct_action_context` / `stg_action_context__values` — same engine, same grain, same corpus. Second, a re-keyed standalone producer would have to rebuild the AC drain's per-match `MatchMeta` (home_team_id + DFL direction-of-play flags) and sk-canonical frame conversion, then run the aligner a **second time** on IDSSE — pure duplication of work the drain already does.

## Decision

Retire the standalone `wf-elastic-sync` producer and the local greedy engine entirely; source action-grain ELASTIC frame linkage from the AC drain's output (`fct_action_context` / `stg_action_context__values`), and drop `bronze.elastic_sync_results`. One engine, one grain, **one producer** — the AC drain — with values that are byte-for-byte the drain's because they *are* the drain's.

## Alternatives considered

| Option | Pros | Cons | Why rejected |
|---|---|---|---|
| A. Preserve event grain — bridge `action_id → original_event_id` on `spadl_actions`, keep `elastic_sync_results` `(match_id, event_id)`-keyed | Hyrum-safe: OBSO/PAUSA publisher + dbt source + oracle unchanged | Events with no SPADL action lose coverage; still rebuilds MatchMeta+conversion; keeps a dead-engine table alive | Owner wanted "done properly" (action grain), not a compatibility shim |
| B-std. Standalone producer re-keyed to action grain (sk aligner in `ingestion/elastic_sync.py`) | Keeps the producer boundary; action grain | Rebuilds AC-drain MatchMeta + frame conversion; runs the aligner a **second time** on IDSSE; two code paths to keep in sync | Pure duplication of the AC drain's already-computed output |
| B2. Adopt v2 in the AC drain only; leave the standalone chain on the local greedy engine | Smallest change | Two different elastic aligners repo-wide (v2 for AC, greedy for OBSO/PAUSA) — a standing inconsistency | Owner rejected: not "one engine" |
| **B-ac (chosen).** Retire the standalone producer; source elastic from the AC drain | One engine, one grain, **one producer**; no second aligner run; no MatchMeta duplication; deletes the buggy greedy engine + a dead-gated dbt view | Re-points 4 consumers; changes the published OBSO/PAUSA HF dataset grain (event→action); a bronze table is dropped | — |

## Consequences

### Positive

- One ELASTIC engine (silly-kicks TF-57 v2) and one grain (action) repo-wide; the buggy local greedy engine (IDSSE frame-origin bug) is deleted.
- No duplicated compute: the aligner runs once, in the AC drain, not a second time in a standalone producer.
- Smaller surface: `ingestion/elastic_sync.py`, `analytics/elastic_sync.py`, the `wf-elastic-sync` mega-job task + card + seed + guard registration, and the `stg_idsse__elastic_sync` dbt view + source are all removed.

### Negative

- The **published** `luxury-lakehouse/obso-pausa-inputs` HF dataset changes grain (event → action). Its card + org-card entry are updated; a downstream consumer of that dataset (the external OBSO/PAUSA GPU batch that writes `pausa_raw_scores`) must adapt — tracked as a P2/operator follow-up, out of the code scope.
- Coverage change: only actions ELASTIC anchored to a frame ship (events with no SPADL action drop out), accepted as the action-grain contract.
- `bronze.elastic_sync_results` is dropped via an idempotent, operator-applied migration; the elastic values re-materialize with the AC drain in P2 (no separate producer run).

### Neutral

- The AC drain's `add_elastic_sync(actions, frames, *, params=None)` call site was already positionally compatible with the v2 signature (the removed greedy-weight kwargs were never passed), so the drain path needed no call-site change — only the three new `elastic_receive_*` output columns threaded through the schema/staging/mart/contract.
- The committed `oracle_elastic_sync_results.parquet` fixture is retained as historical evidence only (already flagged NOT-a-usable-oracle in `oracle_map.py`).

## Related

- **Specs:** `docs/superpowers/specs/2026-09-17-silly-kicks-4118-adoption-design.md` §5.4 (Option B-ac).
- **Migration:** `scripts/migrations/2026-09-20-drop-elastic-sync-results.sql`.
- **Tests:** `src/tests/test_elastic_retirement.py` (structural retirement gate).
- **External references:** silly-kicks TF-57 ELASTIC v2 (sk:ADR-093); Kim et al. (2025/2026) ELASTIC-NW, arXiv:2508.09238.
