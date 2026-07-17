# ADR-070: Retire the standalone space_creation internal chain onto AC-1 (keep HF publish-only); orphan-mart sweep + tracking-context-publisher retirement

| Field | Value |
|---|---|
| **Date** | 2026-07-16 |
| **Status** | Accepted |
| **Deciders** | Karsten Nielsen |

## Context

This is the second half of the mart-consolidation program ([ADR-069](ADR-069-tc1-retirement-and-gk-identity-rehome.md)
retired the TC-1 tracking-context pipeline in PR-1). It bundles three retirement cuts; only the third crosses the
ADR threshold, and it drives this record.

**Cut 3 — the standalone `space_creation` pipeline is redundant with AC-1, and was already dormant.** The vertical
is a round-trip: `compute_space_creation_hf.py` (manual HF-Jobs GPU) publishes the `space-creation-values` HF
dataset → `import_space_creation.py` (an `hf_sync` sub-operation) lands it in `bronze.space_creation_values` →
`stg_space_creation__values` → `fct_space_creation` (gold mart). Three facts make the *internal* legs dead weight:

- **The mart never produced a row.** `fct_space_creation.sql` is gated `{% if var('space_creation_enabled',
  false) %}` with an `else` typed-null `where 1=0`; the var is never overridden, so in every environment the mart
  is a permanent 0-row stub.
- **AC-1 already emits the same metric.** `fct_action_context` carries `space_created_m2` +
  `space_denied_m2_opponent` at **action grain**, computed by silly-kicks' `add_space_creation` on live **oriented**
  tracking frames — an independent code path that does *not* import the standalone `analytics/space_creation.py`.
- **The in-repo `analytics/space_creation.py` is dead code.** Its only importer repo-wide is its own unit test;
  the kept HF compute job reimplements the Fernandez & Bornn (2018) differential-OBSO math inline in JAX.

The one historical defect in AC-1's opponent column (`≡0`/NaN under silly-kicks ≤4.23.0) was root-caused and
**closed by [ADR-050](ADR-050-silly-kicks-4-25-0-space-creation-lean-and-gs-nan-identifiers.md)** (wheel 0.5.36,
regenerated golden: 97 non-null / 71 distinct in [0, 3.65]). So AC-1 is a *verified* successor, not an aspirational
one. The pausa two-pipeline disagreement (MAD 0.425) noted in project memory is a **different metric**
(`add_pausa`) and does not apply here.

**Cut 1 — four orphan output marts** (`fct_space_creation`, `fct_off_ball_xt`, `fct_line_breaking_results`,
`fct_gk_actions_detail`) have **zero dbt refs, zero Taipy queries, zero downstream marts** — verified consumer-free.
`fct_off_ball_xt` / `fct_line_breaking_results` keep their staging views (`stg_off_ball_xt__results` feeds
`fct_physical_stats`; `stg_line_breaking__results` feeds `fct_passes`). `fct_gk_actions_detail` read
`fct_action_values` directly (no dedicated staging).

**Cut 2 — the tracking-context HF publisher** (`scripts/publish_tracking_context_hf.py` + its 2 in-repo cards)
executes the operator post-merge step ADR-069 deferred: the `spadl-tracking-context` HF dataset was already deleted
from the Hub, so the publisher is dead.

## Decision

**Retire the space_creation *internal consumption + ingestion* chain, and KEEP the external HF compute job +
dataset publish-only** (Option B below). Concretely deleted: `stg_space_creation__values` (+ its source),
`import_space_creation.py` (+ its `pyproject` console-script, its `hf_sync._SUB_OPERATIONS` entry [10 → 9], its
`guards._GUARD_MODULES` entry, and `wf-import-space-creation.yaml`), and the test-only `analytics/space_creation.py`
(+ `test_space_creation.py`). **Kept:** `compute_space_creation_hf.py` + `wf-space-creation` + the
`space-creation-values` HF dataset — a live, externally-citable research artifact (Fernandez & Bornn 2018, ~875K
player-frames). AC-1's `space_created_m2` is now the sole internal consumer-facing space-creation signal.

Bundled with it: cut 1 (delete the 4 orphan marts + their synced-table configs, `triggered_synced_marts` entries,
`rederive_planner` sets, PG-index blocks, workflow-card outputs, and live data-quality FK tuples) and cut 2 (delete
the tracking-context publisher + cards + 5 HF-parity/leak-guard registries). Wheel bumped 0.5.80 → 0.5.81.

## Alternatives considered

| Option | Pros | Cons | Why rejected |
|---|---|---|---|
| A. Keep the whole vertical | No change | A dormant 0-row mart + a daily import writing a bronze table nothing reads; dead analytics module | Pure dead weight; AC-1 already serves the metric |
| B. Retire the internal chain, keep the HF dataset (chosen) | Removes all dead internal weight; preserves the external research artifact; AC-1 is the single internal source | `bronze.space_creation_values` DROP is an operator step; the HF dataset becomes publish-only | — |
| C. Full vertical incl. the HF compute job + dataset | Nothing left | **Destroys a live external research dataset** + its regeneration pipeline; larger blast radius (AI-governance rows, HF-Hub-repo teardown, org-card) | A product/value call the executor must not make; escalate only on explicit "the dataset has no further value" |

The publish-only asymmetry is deliberate and verified safe: `compute_space_creation_hf.py` imports only
`analytics.obso` + `analytics.pitch_control` (both shared, untouched) and its own inline helper — it does **not**
import the deleted `analytics.space_creation`, so the kept leg stands alone.

## Consequences

### Positive

- One whole ingestion module, one whole (dead) analytics module, an `hf_sync` sub-operation, a staging view, and
  four orphan marts removed. AC-1's `space_created_m2` (oriented frames, 0-dup bronze) is the single internal
  space-creation source.
- The `space-creation-values` HF dataset survives as a standalone published artifact (its cards were rewritten,
  not deleted, to state values now come via the dataset / GPU job rather than the deleted bronze→staging layer).

### Negative

- **`bronze.space_creation_values` is now written by no one** (its writer, `import_space_creation`, is deleted). Its
  `DROP TABLE` is an operator post-merge step (destructive, never auto-applied), alongside the four orphan marts'
  gold-Delta + Lakebase synced-table teardown.
- The HF compute job is now **publish-only** — it produces a dataset with no internal lakehouse consumer. This is
  the intentional asymmetry (external value retained), not an oversight.

### Neutral

- **Reverses [ADR-050](ADR-050-silly-kicks-4-25-0-space-creation-lean-and-gs-nan-identifiers.md) §Neutral**, which
  recorded that "the standalone `fct_space_creation` T-mart uses a lakehouse-local numpy `compute_space_created` …
  unaffected by the upstream lean." That standalone compute is now retired; AC-1's silly-kicks path is the successor.
- This ADR **resolves ADR-069's deferred `spadl-tracking-context` HF decision as RETIRE** (cut 2).
- Cuts 1 and 2 alone would not warrant an ADR (routine consumer-free deletion + an ADR-069-deferred operator step);
  they are recorded here only because they ship in the same PR.

## CLAUDE.md Amendment

None required — no rule references the deleted `space_creation` internal-chain modules, and the ADR-064/ADR-054 HF
bullets' `tracking-context` mentions were scrubbed in-place with the publisher deletion (cut 2).

## Related

- **Specs:** `docs/superpowers/specs/2026-07-14-mart-consolidation-tc1-retirement-design.md` §Phase 2 (+ REVIEW, REVIEW-2)
- **Plans:** `docs/superpowers/plans/2026-07-14-pr2-orphan-mart-deletions.md`
- **Issues / PRs:** PR TBD (branch `feat/orphan-marts-and-hf-cleanup`)
- **ADRs:** sibling of [ADR-069](ADR-069-tc1-retirement-and-gk-identity-rehome.md) (the TC-1 half of this program);
  reverses [ADR-050](ADR-050-silly-kicks-4-25-0-space-creation-lean-and-gs-nan-identifiers.md) §Neutral;
  builds on [ADR-054](ADR-054-hf-dataset-per-provider-configs.md) (per-provider HF configs) and
  [ADR-064](ADR-064-per-match-access-tier.md) (per-match access_tier — the kept dataset is IDSSE-only, public-by-licence).

## Notes

**Discovery evidence (read-only, before any deletion).** A six-agent discovery pass established: exactly one reader
of `bronze.space_creation_values` (the deleted staging view); zero `ref('stg_space_creation__values')` anywhere
after the mart deletion; the compute helper's independence from `analytics.space_creation`; and the AC-1 column
provenance + ADR-050 closure. Scope (B vs C) was surfaced to the decider as an explicit fork because deleting a
live external dataset is a product decision, not cleanup.

**Execution lesson (recorded so future mart-block teardowns do not repeat it).** The `sed`-range deletion of the
`fct_space_creation` block from `_marts__models.yml` was scoped by a grep that matched only *named* mart blocks;
it did not reveal that `fct_player_embeddings_career_360` + `_season_360` sat inside the same line range, so those
two **still-live, HF-published** marts' contract + data-test blocks were collaterally deleted — silently, because
no test parses `_marts__models.yml` for `- name:` ↔ `marts/*.sql` completeness. It was caught only by the
adversarial final-review (yml `- name:` count 41 vs 43 surviving `.sql`), and the blocks were restored verbatim
from HEAD. Lesson: delete YAML mart blocks by explicit start/end anchors of *that* block, and assert
`count(- name:) == count(marts/*.sql)` after any bulk `_marts__models.yml` edit.
