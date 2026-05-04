# PR-1.5 — CI Gating Plumb-In + Kimball Drift Audit

| Field | Value |
|---|---|
| **Date** | 2026-05-04 |
| **Status** | Phase 1 design — awaits implementation approval |
| **Cycle** | SK3-MIG-B sequencing — between PR-1 (orchestrator hardening) and PR-2 (γ + wf-hf-sync + evolve) |
| **Triggering memory** | Discovery during PR-1 final sweep that `test_marts_kimball_contracts.py` has been red on main for ≥days, hidden because `python-ci.yml`'s test step has no Databricks credentials → all DB-touching tests skip silently in CI. SkillCorner ID-format drift (39% NULL on team/player/match keys across 4 tracking marts) is the surfaced symptom. |
| **Predecessors** | PR-1 (sk3-mig-b-pr1-orchestrator-hardening — pending merge) |
| **Successor** | PR-2 (γ trainer rewrites — gated on this audit clearing) |

## §0 — Why this PR exists

PR-1's final sweep surfaced 10 pre-existing test failures, 9 of which (`test_marts_kimball_contracts.py::test_kimball_key_populated_per_provider[fct_*-skillcorner-1.0]`) point at a real data-correctness bug: 2,896 of 7,429 SkillCorner rows in `fct_player_positions` (and 3 sibling tracking marts) have NULL Kimball keys because the upstream `match_id` / `player_id` columns use a different ID format than `dim_matches` / `dim_players` carry for the skillcorner provider. Specifically:

- `fct_player_positions.match_id` for skillcorner: `J03WMX` (raw SkillCorner public-API format)
- `fct_player_positions.player_id` for skillcorner: `DFL-OBJ-XXXXXX` (DFL ObjectId — the IDSSE/Bundesliga format, indicating prior entity-resolution that kept the wrong target ID)
- `dim_matches.native_match_id` for skillcorner: `skillcorner_2017461` (canonical `skillcorner_<numeric>`)

The test exists. It catches the bug. It has been silently red. The two-week Kimball / hardening cycle (PR 4b, PR 6, PR 7, ADR-011, ADR-018) bought us contract-level enforcement at dbt build time but did NOT close the ingestion → mart JOIN-resolution drift, AND did not wire the runtime contract-test signals into CI.

**The trust-deficit question is the more important one:** if SkillCorner has been silently broken, what else has? PR-2's γ rewrites trainers to read directly from gold marts via SQL — they expand the surface area where ingestion drift could silently produce stale or wrong embeddings. PR-2 should not ship onto a lakehouse that can't prove its Kimball contracts are tight.

This PR does two things, in order:

1. **β — CI plumb-in.** Wire `DATABRICKS_HOST` + `DATABRICKS_TOKEN` + `DATABRICKS_HTTP_PATH` (+ optionally `DATABRICKS_WAREHOUSE_ID`, `DATABRICKS_CATALOG`) into `.github/workflows/python-ci.yml`'s `Run tests` step so DB-gated contract tests actually run. This is the structural fix that prevents this class of bug from re-hiding.
2. **α — Audit + targeted fixes.** With β in place, run the full Kimball + live-schema test surface against current main, enumerate every drift, fix or ticket each one. This is the data-correctness baseline PR-2 expects.

## §1 — β: CI plumb-in (~30-60 min, ~10 LOC)

### §1.1 What the test step needs

`src/tests/sk3_mig_b/conftest.py` and several other gates skip on:
- `DATABRICKS_HOST` (URL)
- `DATABRICKS_TOKEN` (PAT)

`src/tests/test_marts_live_schema.py::requires_databricks` skips on the trio:
- `DATABRICKS_HOST`
- `DATABRICKS_HTTP_PATH`
- `DATABRICKS_TOKEN`

`src/tests/test_marts_kimball_contracts.py` (per the existing connection fixture in that file or its conftest) similarly requires the trio.

### §1.2 Workflow YAML diff (sketch)

```yaml
# .github/workflows/python-ci.yml — Run tests step
- name: Run tests
  env:
    DATABRICKS_HOST: ${{ vars.DATABRICKS_HOST }}
    DATABRICKS_HTTP_PATH: ${{ vars.DATABRICKS_HTTP_PATH }}
    DATABRICKS_TOKEN: ${{ secrets.DATABRICKS_TOKEN }}
  run: uv run pytest
```

`vars.DATABRICKS_HOST` and `vars.DATABRICKS_HTTP_PATH` already exist as repo vars per the existing `Deploy wheel to Databricks` step pattern (see line 141 of python-ci.yml — uses `${{ vars.DATABRICKS_HOST }}`). `secrets.DATABRICKS_TOKEN` already exists per line 142.

**Q1 — Pull request runs vs forks:** GitHub Actions does NOT pass secrets to PRs from forks. Forks will continue to skip DB-gated tests. Internal-team branches (the only ones we accept anyway) will run them.

**Q2 — Cost concern:** every PR's CI now spins up the SQL warehouse + runs the suite. Per `bronze.workflow_costs` history, a typical pytest run touches ~5-10 mart-schema queries (DESCRIBE / count / param-test loop on Kimball contracts). Estimated marginal cost per PR: ~$0.20-0.50 (warehouse warm-up + queries). Bounded.

**Q3 — Data drift between PR open + merge:** lakehouse data state changes daily. A PR that passed contract tests on Monday could hypothetically fail re-run on Wednesday if data degraded. The test is asserting CONTRACTS, not data freshness — drift would mean the contract is broken, which is actionable. Acceptable.

**Q4 — Concurrency:** `cancel-in-progress: ${{ github.event_name == 'pull_request' }}` already in workflow → superseded PR runs are killed before they query the warehouse. Idle warehouses don't stay warm long. Cost-bounded.

### §1.3 Sanity-check probe before the audit

After β lands on main, the very next push should re-run `python-ci.yml` and now see the `test_marts_kimball_contracts.py` failures + the `test_marts_live_schema.py` failure surface in CI. That confirms β plumbed correctly.

If the CI surface is silent (still all green), β isn't routing the env. Investigate before declaring α complete.

## §2 — α: Kimball audit (Wicked-sized, partially data-driven)

### §2.1 Approach

Run two test files end-to-end with credentials:

```bash
DATABRICKS_HOST=$DATABRICKS_HOST \
DATABRICKS_TOKEN=$DATABRICKS_TOKEN \
DATABRICKS_HTTP_PATH=$DATABRICKS_HTTP_PATH \
DATABRICKS_WAREHOUSE_ID=$DATABRICKS_WAREHOUSE_ID \
uv run pytest \
    src/tests/test_marts_kimball_contracts.py \
    src/tests/test_marts_live_schema.py \
    --tb=line --no-header -q 2>&1 | tee audit-2026-05-04.log
```

Expected outputs:
- `test_marts_kimball_contracts.py` parametrizes across {mart, key_column, provider, threshold} — total parametrization count ~50-100 cases. Today, 9 fail (skillcorner only). Audit will surface any others.
- `test_marts_live_schema.py` has multiple `test_*_live_schema_matches_contract` tests across `fct_action_values`, `fct_defensive_values`, `fct_defcon_actions`, `fct_defcon_pressure`, `fct_goalkeeper_stats`, `fct_gk_actions_detail`. Audit will surface any column drift.

### §2.2 Triage of expected drifts

For each FAILED case, classify into one of 4 buckets:

| Bucket | Symptom | Disposition |
|---|---|---|
| **A** — ID-format drift between provider ingestion and dim recipe | NULL Kimball keys at LEFT JOIN to `dim_matches` / `dim_players` because the source's `*_id_native` column uses a different format than the dim's `native_*_id`. | **Fix in this PR — canonical-id-migration approach** (operator-locked 2026-05-04). The dim's format is the canonical format. The ingestion path is fixed at the source so all downstream writes use the canonical ID. Existing broken rows are corrected via re-ingestion or a deterministic backfill. NO fix-in-place tolerance branches in dim recipes — those compound the debt. Re-run `dbt build --select <changed>+`. |
| **B** — Column drift between live mart and YAML/Python-dict contract | Test fails because the live mart has columns the contract doesn't enumerate, or vice versa. | **Fix in this PR.** Update `_marts__models.yml` + the corresponding hardcoded dict in `test_marts_live_schema.py`. Pattern matches PR-1's A1 fix. |
| **C** — Provider-not-loaded false positive | Test fails because total=0 (provider has zero rows in mart) and `pytest.skip` was supposed to fire but didn't. | **Fix in this PR.** Tighten the skip path in the kimball contract test (or accept that some marts legitimately don't have data for some providers — adjust expectation). |
| **D** — Genuinely unfixable in PR-1.5 scope | E.g., requires upstream provider re-ingestion that exceeds reasonable runtime, schema migration with cross-team coordination, or external-vendor cooperation. | **Document + ticket.** Surface as a known limitation, write the ticket, exit α with the ticket as the disposition. Bucket D is the escape hatch but should be used sparingly — most ID-format drift IS fixable in this PR via canonical-id-migration. |

### §2.3 Known starting point — SkillCorner ID drift (Bucket A)

Per PR-1 diagnostic queries (2026-05-04):
- 7 of 17 distinct skillcorner match_ids in fact aren't in dim_matches (39% of rows orphan)
- 150 of 356 distinct skillcorner player_ids aren't in dim_players

**Investigation hooks already in place:**
- `dim_matches` recipe — search for `provider = 'skillcorner'` join logic (likely in `dbt_project/models/marts/dim_matches.sql` or a staging model upstream)
- SkillCorner ingestion path — `src/ingestion/` likely contains `skillcorner_*.py`; identify whether it canonicalizes match_id to `skillcorner_<numeric>` or passes through `J03WMX`-style IDs
- The DFL-OBJ-XXXXXX player_ids in fct_player_positions for skillcorner suggest entity-resolution against IDSSE players keeps the WRONG side's ID; investigate `resolve_players` workflow output

**Fix shape (sketch — confirm during implementation):**
- Either update SkillCorner ingestion to write `skillcorner_<numeric>` consistently in bronze
- Or add a join-format-tolerance branch in `dim_matches` recipe that resolves both formats
- Re-run `dbt build --select +fct_player_positions+ +fct_position_maps+ +fct_formation_labels+ +fct_off_ball_xt+`
- Verify the 9 failing kimball tests turn green

### §2.4 Other marts to audit (extend list as findings come in)

Per `_marts__models.yml`, marts under contract enforcement include (but are not limited to):

- `fct_action_values` (PR 4b)
- `fct_passes` (LL2)
- `fct_match_summary`
- `fct_xg_predictions_v2`
- `fct_pausa_values`
- `fct_defcon_actions` / `fct_defcon_pressure` / `fct_defensive_values` (PR 6)
- `fct_goalkeeper_stats` / `fct_gk_actions_detail` (PR 6)
- `fct_player_embeddings` (career + season + 360 variants — known mixed-dim issue per `project_career_mart_v1_v2_dim_mismatch.md`, may surface again)
- `fct_player_positions` / `fct_position_maps` / `fct_formation_labels` / `fct_off_ball_xt` (skillcorner Bucket A — already known)
- `fct_off_ball_xt` (re-listed — same bucket)
- `dim_*` (smaller; less likely to have drift but include for completeness)

The audit is data-driven: the test parametrization count + failure log determines the actual scope.

## §3 — Out of scope

- **Provider re-ingestion at scale.** If SkillCorner ingestion needs to be re-run for historical data, that's an operator-runtime task triggered after this PR's ingestion-path fix lands. Not in PR-1.5 itself.
- **dim_players cleanup beyond skillcorner.** PR-1.5 fixes Bucket A drifts that are mechanical canonicalization. Strategic dim cleanup (deprecating dual ID columns, etc.) is its own cycle.
- **Adding NEW Kimball contract tests.** Existing tests are sufficient for the audit. New gates can be PR-2-or-later concerns.
- **Pyright optional-extra import warnings.** Pre-existing CI noise, separate concern.

## §4 — Test plan / TDD ordering

| Step | Action | Verification |
|---|---|---|
| 1 | β: edit `python-ci.yml`'s `Run tests` step to inject `DATABRICKS_HOST` + `DATABRICKS_HTTP_PATH` + `DATABRICKS_TOKEN` env vars | Push a no-op commit to a sacrificial branch; observe CI run; the kimball + live-schema tests should now FAIL in CI (where today they pass-by-skipping) |
| 2 | α: run audit pytest locally with credentials; capture FAIL list to `audit-2026-05-04.log` | Failure count + per-test breakdown captured |
| 3 | α: triage each failure into A/B/C/D buckets; write the inventory into the PR description | Inventory complete |
| 4 | α: fix each Bucket A failure (canonicalization fixes in ingestion + staging models) | Re-run target test; assert PASS |
| 5 | α: fix each Bucket B failure (contract-test parity fixes) | Re-run target test; assert PASS |
| 6 | α: address each Bucket C false-positive (skip-path tightening or expectation update) | Re-run target test; assert PASS |
| 7 | α: file tickets for any Bucket D drifts; update `project_known_pretest_failures_on_main_2026_05_04.md` to reflect post-PR-1.5 known-broken set | Memory updated |
| 8 | Final sweep: full suite green (or down to documented Bucket D set) under CI conditions | CI green on the PR |
| 9 | Wheel republish if any wheel-bundled file changed (likely if ingestion paths get touched) | `bump_wheel.py --check` clean |

## §5 — Cost / risk / size

### §5.1 Cost
- β plumb-in: ~$0 (workflow YAML edit + a sacrificial CI run)
- Audit run: ~$1-3 (warehouse queries across the test surface; several DESCRIBE + count(*) + parametrize loops)
- Per-fix dbt build cost: ~$2-5 per affected mart × N marts. SkillCorner alone touches 4 tracking marts; if other Bucket A drifts surface, total grows linearly.
- Estimated total: $20-100 depending on what α finds.

### §5.2 Sizing
- β: trivial (~10 LOC, ~30-60 min including sacrificial-CI verification)
- α: depends on audit findings. Lower bound: skillcorner-only fix (Wicked-sized: investigation + ingestion or dim recipe change + 4 mart rebuilds + verification). Upper bound: 5-10 Bucket A drifts surface (Wicked-to-Monstah: similar fix shape × N).

### §5.3 Risk
- **β risk: secrets leakage.** `DATABRICKS_TOKEN` already lives in CI as a secret (used by `Deploy wheel to Databricks` step on main pushes). Extending its env scope to the test step doesn't widen the secret — it's still scoped to the same job. PR-from-fork posture stays the same (forks can't see secrets). Low.
- **β risk: warehouse burn from runaway test loops.** New test pattern that loops without `LIMIT` could blow CI budget. Mitigation: spot-check the audit run's cost via `bronze.workflow_costs`; tighten any test that costs >$1 per CI run.
- **α risk: data-correctness fix introduces regression.** Mitigation: each Bucket A fix is bisect-friendly (ingestion change + dbt build + targeted re-test). Halt + revert if any non-target test goes red.
- **α risk: Bucket D ticketing.** If audit finds drift that genuinely can't be fixed in this PR, it's tempting to silently accept. Discipline: every Bucket D entry needs an explicit ticket + memory update so the signal stays loud.

## §6 — Approval gate — DECISIONS LOCKED 2026-05-04

**Sequencing (locked):**
- PR-1 ships first (orchestrator hardening — independent of this work).
- PR-1.5 (β + α) ships next, **as a single PR / single commit** on a clean post-PR-1 main.
- PR-2 (γ + wf-hf-sync + evolve) ships LAST, on top of the audit-clean main.

**Operator decisions (locked 2026-05-04):**

| Question | Decision |
|---|---|
| β + α split or bundled? | **Bundled — single PR / single commit.** β plumb-in + α audit fixes ship together. CI surfacing of the audit failures is internal to the PR's own iteration loop, not a separate landing event. |
| Bucket A approach | **Canonical-id-migration as best practice / long-term posture.** The dim's format wins. Ingestion paths get fixed at the source so all bronze writes use canonical IDs. Existing broken rows are re-ingested or deterministically backfilled. NO fix-in-place tolerance branches in dim recipes — those compound debt. |
| Cost ceiling | **No hard cap.** Track cumulative cost as the audit runs. Surface for explicit operator approval before: (a) initiating any single dbt build that's likely >$10, (b) any cumulative-cost increment of ~$25 since the last approval, (c) any operation that requires re-ingestion of >1 day of historical data. Rationale: audit scope is data-driven and unknown a priori — a hard cap would force premature Bucket-D ticketing instead of fixing real drift. |

**Operational checkpoint policy (derived from cost-ceiling decision):**

Before initiating any of these operations, surface a one-line cost estimate + brief rationale + ask for explicit operator go/no-go:
1. `dbt build --select <mart>+` for any mart that's not in the trivial-rebuild category (i.e., not a thin staging model)
2. Re-running upstream ingestion via `WorkspaceClient.jobs.run_now(...)` for any provider
3. Any backfill query touching >1 day of historical data
4. Each ~$25 increment of cumulative audit cost

Implementation begins now per operator approval. **β plumb-in is the first step; sacrificial-CI verification of β before α dispatch is non-negotiable** (catches misconfiguration before paying for the audit run).
