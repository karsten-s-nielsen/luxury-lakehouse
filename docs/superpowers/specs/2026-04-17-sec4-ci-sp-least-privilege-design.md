# SEC4 — Terraform CI SP Least-Privilege — Design

| Field | Value |
|---|---|
| **Date** | 2026-04-17 |
| **Status** | Draft — pending user approval |
| **Branch** | `feat/sec4-ci-sp-least-privilege` |
| **TODO items** | SEC4 (TODO.md On-Deck line 34) + ADR-005 §Neutral orphan PG role |
| **Out of scope** | TD #25 Lakebase CU right-sizing (provider-blocked); TD #28 Databricks budget automation (different risk profile); other humans in the `admins` group (SEC4 targets the CI SP only); TF state surgery |

## Why this cycle

SEC-AUDIT-v1.12.0 INF-01 (CWE-250 — Execution with Unnecessary Privileges) identified that `luxury-lakehouse-terraform-ci-dev` holds workspace admin via `admins`-group membership plus `account_admin`. Prior cycles (PR #126, #128, #131) eliminated transitive-admin dependency for everything except Lakebase workspace-scoped objects. The surface is now narrow enough for a surgical cleanup:

- The **workspace-admin floor is only 2 resources** (Lakebase project + endpoint) per the starting-hypothesis inventory in §Inventory. Removing admin-group membership is gated on adding explicit ACLs for those two.
- The **account-admin floor is 8 resources** but the 2026-04-13 D59 investigation never pressure-tested the provider for narrower roles. This cycle applies a stricter bar: ADR-006 accepting the floor is only valid if backed by a citable provider issue number OR source-code-level limitation.
- A separate pre-existing finding — orphan Postgres role `be66af99-5296-4fd9-887a-c081bce38bfa` documented in ADR-005 §Neutral (line 69) — is included as item H. Per the "kill pre-existing findings" preference, we don't defer IAM cleanup to a future cycle when it's cheap and thematically adjacent.

## Goals

- `databricks_group_member.terraform_ci_admin` and `data.databricks_group.admins` are absent from `terraform/modules/service_principals/main.tf`. The CI SP has zero workspace-admins-group membership.
- Either `databricks_service_principal_role.terraform_ci_account_admin` is reduced to a narrower role, OR `docs/superpowers/adrs/ADR-006-<topic>.md` exists with a citable provider issue URL or source-code reference documenting why the account-admin floor is accepted.
- Every workspace-scoped and account-scoped TF resource the CI SP manages has either an explicit `databricks_permissions` / `databricks_grant` covering the CI SP at minimum-necessary privilege, or a documented exemption in the inventory markdown.
- Orphan Postgres role `be66af99-5296-4fd9-887a-c081bce38bfa` is removed from Lakebase; `pg_roles` no longer contains it; a pytest guards against its reappearance.
- A living inventory markdown `docs/superpowers/specs/2026-04-17-sec4-workspace-resource-inventory.md` exists, populated against live state at commit time, so a future TF-adding PR can be audited against it.
- Regression tests prevent silent re-introduction of admin-group membership and ACL-shape drift on the Lakebase resources.

## Non-goals

- Changing UC grant model (catalog ownership via `dbt-owners-dev` group is already in place — untouched).
- Adding `databricks_budget` automation (TD #28 — separate cycle).
- Lakebase CU right-sizing (TD #25 — provider-blocked).
- Reviewing non-CI-SP entries in the workspace `admins` group (other human members — out of SEC4's scope).
- Creating a new Databricks account or workspace for staged testing — live dev is the test surface.
- TF state surgery (no `terraform state mv` / `rm` operations).

---

## Cycle rules (user-stated)

- **No commits, pushes, or PRs without explicit user approval.** Applies per-commit; approval for one action is not blanket authorization for any next action.
- **Minimal commits.** Target a single commit for the whole cycle if it passes E2E cleanly; split only if forced by apply failures.
- **E2E testing first when possible.** Live `terraform apply` against dev workspace before declaring anything done. Prefer provable evidence over plan-only confidence.
- **Evidence-based claims.** Every factual claim in any check-in cites file:line, command output, or URL so the user can verify independently.

## Sequencing (β — two-phase apply, single commit)

| Phase | Contents | Gate |
|---|---|---|
| **Phase 1 — additive** | Item G (audit) → Item A (Lakebase ACLs) → Item B (account_admin spike: reduction OR ADR-006) → Item H (orphan PG role drop) → Item D (static ACL-shape tests) | `terraform apply` Phase 1; all tests green; `terraform plan -detailed-exitcode` returns 0 (idempotent re-plan) |
| **Phase 2 — reduction** | Core (admin-group member removal + comment-block rewrite) → Item C (regression tests for absent resources) → Item E (doc sweep) | `terraform apply` Phase 2; all tests green; `terraform plan -detailed-exitcode` returns 0; `terraform plan -refresh-only` succeeds |
| **Commit** | Single commit once both phase gates have passed. Request explicit approval before `git commit`. | User approval |

Phase 1 is purely additive and individually revertible via `git checkout`. Phase 2 is the only privilege-reducing step; by the time it runs, Phase 1 has already proven the CI SP has sufficient explicit grants for every operation the cycle exercises.

---

## Inventory format & starting hypothesis (item G)

Item G's deliverable is `docs/superpowers/specs/2026-04-17-sec4-workspace-resource-inventory.md`. The format:

| Field | What it captures |
|---|---|
| Resource address | `module.foo.databricks_bar.baz` (terraform-canonical) |
| Resource type | `databricks_job` / `databricks_permissions` / `databricks_grant` / `databricks_postgres_endpoint` / etc. |
| Scope | workspace-scoped / account-scoped / UC-scoped / AWS |
| What CI SP does to it | create / update / read-only (plan-only) |
| Current auth path | admins-group / account_admin / explicit ACL / grant / N/A |
| Target auth path (post-SEC4) | explicit ACL / grant / documented exemption |
| Action needed | none / add ACL / add grant / rewrite / already done |
| Verification command | `databricks permissions get ...` / `databricks grants get ...` output with timestamp |

**Starting hypothesis (to be verified against live state in item G):**

| Resource | Scope | Current auth | Target auth |
|---|---|---|---|
| `module.workflows.databricks_job.data_ingestion` | workspace | explicit `IS_OWNER` (PR #128) | already done |
| `module.sql_warehouse.*` (warehouse ACL) | workspace | explicit `CAN_MANAGE` (PR #126) | already done |
| `module.workspace.databricks_catalog.soccer_analytics` | UC | `ALL_PRIVILEGES` + group ownership | already done (MANAGE via group) |
| `module.catalog.databricks_schema.{bronze,silver,gold,observability}` | UC | transitive via catalog grant | likely already done — verify |
| `module.catalog.databricks_volume.{libs,training_data}` | UC | transitive via catalog grant | likely already done — verify |
| `module.catalog.databricks_grant.*` (21 resources) | UC | MANAGE via ownership | already done |
| `module.lakebase.databricks_postgres_project.soccer_analytics` | workspace | **admins-group transit (untested)** | **item A** |
| `module.lakebase.databricks_postgres_endpoint.primary` | workspace | **admins-group transit (untested)** | **item A** |
| `module.synced_tables.databricks_database_synced_database_table.*` (34×) | workspace | `lifecycle { ignore_changes = all }` — TF doesn't manage | N/A (documented in TD #1) |
| `module.service_principals.databricks_service_principal.*` (3×) | account | `account_admin` | **item B** (spike) |
| `module.service_principals.databricks_service_principal_role.terraform_ci_account_admin` | account | self-managed via `account_admin` | **item B** |
| `module.service_principals.databricks_service_principal_federation_policy.github_actions` | account | `account_admin` | **item B** |
| `module.service_principals.databricks_access_control_rule_set.ingestion_sp_user_role` | account | `account_admin` | **item B** |
| `module.service_principals.databricks_group.dbt_owners` | account | `account_admin` | **item B** |
| `module.service_principals.databricks_group_member.{dbt_owners_*}` (3×) | account | `account_admin` | **item B** |
| `module.service_principals.databricks_mws_permission_assignment.dbt_owners_workspace` | account | `account_admin` | **item B** |
| `module.state_kms.*` + `module.github_oidc.*` + `aws_budgets_budget.monthly` | AWS | IAM role (not Databricks) | N/A — AWS OIDC role is unchanged |

**Observations this hypothesis already exposes:**

1. The workspace-admin floor is only 2 resources (Lakebase). Removing admin membership is low-risk **if** the provider supports `databricks_permissions` on those two resource types. If it doesn't, we have a hard blocker — see Risk R1.
2. The account-admin floor is 8 resources. The spike's real question is whether any can be restructured so the CI SP doesn't need to manage them at all (e.g., one-time bootstrap analogous to UC catalog ownership).
3. "Likely already done — verify" items (UC schemas, volumes) need empirical confirmation. `ALL_PRIVILEGES` is known to NOT confer MANAGE (that's why group ownership was introduced). I'll test by attempting a schema-level operation as CI SP during the audit.

---

## Item-by-item implementation approach

### Phase 1

**Item G — Pre-flight audit.** Walk every `*.tf` under `terraform/`, populate the inventory table against live state. Verification technique: for workspace-scoped resources, run `databricks permissions get <object_type> <object_id>` as the CI SP OAuth identity (not the admin-identity used for development). For UC-scoped resources, run `databricks grants get <object>`. Output includes cited commands and timestamps per row. Auth setup: `databricks auth login --client-id $CI_SP_CLIENT_ID --profile ci-sp-audit` one-time; profile discarded after cycle.

**Item A — Lakebase `databricks_permissions` (contingent on provider support).** First sub-step: probe whether provider v1.113 supports `databricks_permissions` on `databricks_postgres_project` and `databricks_postgres_endpoint`. Check the docs at `registry.terraform.io/providers/databricks/databricks/1.113.0/docs/resources/permissions` for supported `object_type` values.

- **If supported**: add two resource blocks in `terraform/environments/dev/main.tf`, granting CI SP the minimum permission level needed. Alphabetical sort of `access_control` blocks by principal.
- **If not supported**: fall back to `databricks_access_control_rule_set` at account-API level (same pattern as existing `ingestion_sp_user_role`).
- **If neither works**: HARD BLOCKER — see Risk R1. Cycle pauses at end of Phase 1 for scope decision with user.

**Item B — `account_admin` reduction spike.** Evidence-driven workplan, check-in at the end of each step:

1. Read provider v1.113 CHANGELOG entries since v1.107 (last D59 check) for IAM / workspace-assignment / SCIM-role changes.
2. For each of the 8 account-scoped resources, grep the provider source (`github.com/databricks/terraform-provider-databricks`) for the API call it makes. Classify: `/api/2.0/accounts/*` (account_admin-gated) vs `/api/2.0/preview/accounts/scim/*` (has finer-grained roles).
3. Run `terraform plan` once with `terraform_ci_account_admin` removed (no apply). Capture exact error messages per failing resource.
4. For each failing resource, try to find a narrower role (`group_admin`, `workspace_admin`, account-scoped SCIM roles) that would permit it.

Exit rule: **ADR-006 is only acceptable if I can cite a specific provider issue number OR a source-code-level limitation** (e.g., "the provider calls `accountsClient.FederationPolicy.Create` at `pkg/databricks/account_federation_policy.go:<line>` which is `account_admin`-gated with no alternative SCIM role per upstream SDK <version>"). Otherwise ship the reduction. If the provider is truly a black box on some resource, fallback is to open an upstream issue at `github.com/databricks/terraform-provider-databricks` as part of this cycle and cite that issue in ADR-006 — confirm with user before filing publicly.

**Item H — Orphan PG role cleanup.** Connect to Lakebase as `databricks_superuser` via existing `scripts/run_lakebase_grants.py` auth path or a one-off script. Steps:
1. Query `pg_roles` and `pg_auth_members` for role `"be66af99-5296-4fd9-887a-c081bce38bfa"` — enumerate grants and memberships. Record pre-state in the inventory markdown.
2. `REASSIGN OWNED BY "be66af99-5296-4fd9-887a-c081bce38bfa" TO databricks_superuser` (defensive — clears any owned objects).
3. `DROP OWNED BY "be66af99-5296-4fd9-887a-c081bce38bfa"` (revokes all grants).
4. `DROP ROLE "be66af99-5296-4fd9-887a-c081bce38bfa"`.

Test: `src/tests/test_orphan_pg_role_absent.py` — connects to Lakebase via the shared auth path, asserts `SELECT COUNT(*) FROM pg_roles WHERE rolname = 'be66af99-5296-4fd9-887a-c081bce38bfa'` returns 0. Skipped when `DATABRICKS_HOST` is unavailable (CI without Lakebase creds).

**Item D — Static ACL-shape tests.** Extend `src/tests/test_sec4_ci_sp_job_owner.py` with test functions asserting the new Lakebase `databricks_permissions` resources (or `databricks_access_control_rule_set` fallback) exist with CI SP at the expected permission level, alphabetically sorted. Pattern mirrors the existing `_assert_acl_resource_correctly_shaped` helper. Estimated ~40 lines.

**Phase 1 apply gate.** `terraform apply` Phase 1 changes; all static + live tests green locally; `terraform plan -detailed-exitcode` returns 0 on a re-run (idempotence confirmed). Check-in with evidence.

### Phase 2

**Core — remove admin-group membership.** Delete `terraform/modules/service_principals/main.tf:84-87` (the `databricks_group_member.terraform_ci_admin` resource). Delete lines `:80-82` (the `data "databricks_group" "admins"`, now unused). Rewrite the comment block at `:54-78` to reflect the new floor: admins-group section removed, dbt-owners group ownership and Lakebase ACLs added as the authoritative inventory. `terraform plan` should show exactly two removals (resource + data source) and nothing else.

**Item C — Regression tests.** New tests in `src/tests/test_sec4_ci_sp_job_owner.py`:
- `test_terraform_ci_admin_group_member_absent` — parse `service_principals/main.tf`, assert no `resource "databricks_group_member" "terraform_ci_admin"` block exists AND no `data "databricks_group" "admins"` block exists.
- `test_admins_group_not_referenced_anywhere` — grep all `*.tf` files for `data.databricks_group.admins`, assert zero matches. Prevents re-introduction anywhere in the tree.

**Item E — Doc sweep.**
- `SECURITY.md` — add INF-01 close-out line to audit log (date + commit reference).
- `ARCHITECTURE.md` §6.1 Security table row for `IAM` — refresh to note "Terraform CI SP operates without workspace admins group; account-admin floor [reduced to X / accepted per ADR-006]."
- `CLAUDE.md` — no edits expected; verify by grep for admin-group references.
- `AI_GOVERNANCE.md` — verify `SEC-AUDIT-v1.12.0 REG-01` provenance tag; likely no-op.
- `TODO.md` — remove SEC4 On-Deck row (per `feedback_todo_cleanup_in_commit.md`, belongs in the SEC4 commit, not a follow-up).
- `terraform/modules/service_principals/main.tf:54-78` comment block rewritten as part of Core.
- If B produces ADR-006: commit to `docs/superpowers/adrs/ADR-006-<topic>.md` using template; reference from `SECURITY.md` and the inventory markdown.

**Phase 2 apply gate.** `terraform apply` admin-removal; all tests green; `terraform plan -detailed-exitcode` returns 0; `terraform plan -refresh-only` succeeds (confirms CI SP can still read state). Check-in with evidence.

### Single commit

Once both phase gates have passed, stage everything, draft the commit message (HEREDOC citing evidence for each non-trivial claim), request commit approval.

---

## Rollback plan

| Phase | Failure mode | Rollback |
|---|---|---|
| Phase 1, item A apply | Provider rejects `databricks_permissions` on postgres resources | Revert TF edits; cycle blocks on item A; consult user (Risk R1) |
| Phase 1, item B spike | `account_admin` removal breaks `terraform plan` | No apply yet (plan-only); revert TF edit; investigate per workplan step 4 |
| Phase 1, item H apply | `DROP ROLE` fails with dependency error | `REASSIGN OWNED` + `DROP OWNED` sequence already covers typical cases. If something exotic blocks it, document and consult user |
| Phase 2, core apply | `terraform apply` succeeds but breaks subsequent `plan`/`apply` | Re-add `databricks_group_member.terraform_ci_admin` + `data.databricks_group.admins` (4 lines); `terraform apply`; ~20 s + 30-60 s SCIM propagation |
| Phase 2, core apply | Apply breaks mid-way | Partial state. Re-add the two TF blocks, apply, move on |
| Post-commit / post-merge | CI-triggered `terraform apply` on main breaks | Revert merge commit; `terraform apply` the revert branch; push follow-up fix after investigation |

---

## Testing surfaces

1. **Static TF-parser tests** — existing `test_sec4_ci_sp_job_owner.py` + new tests from items C, D. Fast, no infrastructure dependency. Run on every CI invocation.
2. **Live PG regression test** — `src/tests/test_orphan_pg_role_absent.py` (item H). Gated by `DATABRICKS_HOST` env var; skipped when creds are unavailable.
3. **`terraform plan -detailed-exitcode`** — returns 0 after each apply. Manual gate during the cycle; not automated in CI.
4. **Full test suite at end of Phase 2** — `uv run pytest src/ -v && uv run ruff check src/ scripts/ && uv run ruff format --check src/ scripts/ && uv run pyright src/`. Standard gate from `CLAUDE.md`.

---

## Check-in cadence

Per user Q2-B (evidence-driven, not wall-clock):

| # | Trigger | What I report |
|---|---|---|
| 1 | End of item G | Inventory markdown + surprises vs starting-hypothesis table |
| 2 | Item B spike — end of each workplan step (1, 2, 3, 4) | Provider CHANGELOG findings / source-code observations / plan errors — user can redirect |
| 3 | End of item B — spike decision reached (reduction vs ADR-006) | Reduction TF diff (not yet applied) OR ADR-006 draft with citations |
| 4 | End of item A implementation (before Phase 1 apply) | Provider-support confirmation for Lakebase `databricks_permissions`, exact resource blocks to be added, plan output (not yet applied) |
| 5 | End of Phase 1 apply | Live state changes (item A + item B reduction if chosen + item D tests) + idempotent re-plan evidence |
| 6 | End of item H | Pre-state grant list + post-state confirmation (orphan gone) |
| 7 | End of Phase 2 apply | Live state changes (core + item C + item E) + idempotent re-plan + full test suite output |
| 8 | Before any `git commit` | Final `git status` + diff stat + consolidated evidence; request commit approval |

Beyond these 8 milestones, no status-only updates unless blocked. If an investigation step is taking longer than expected without producing direction, I surface that as an extra check-in.

---

## Risks & contingencies

**R1 — Provider doesn't support `databricks_permissions` on Lakebase resources.** Likelihood: moderate. Provider v1.110 added the Lakebase resources; the permissions surface for Lakebase objects is historically thin. Contingency: fall back to `databricks_access_control_rule_set` at account-API level. If that also fails, cycle pauses at end of Phase 1 with an evidence package. **Abort behavior: scope decision with user (not auto-proceed)** — options are (a) keep admins membership, rescope SEC4 to "reduced as much as the provider allows" + ADR-007 documenting the gap, or (b) move Lakebase management out of Terraform (large scope expansion; separate cycle).

**R2 — `account_admin` is irreducible AND no citable provider issue/source exists.** Likelihood: moderate-to-high (2026-04-13 D59 investigation hit this wall). Contingency: spike workplan step 2 (grep provider source) produces the source-code citation needed for ADR-006 even in the worst case. If provider is a black box on some resource (unlikely — it's open source), fallback is to open an upstream issue as part of this cycle and cite its number in ADR-006. Filing a public issue requires user confirmation first.

**R3 — Admin removal breaks something `terraform plan` didn't predict.** Likelihood: low-moderate. Plan's coverage for permission-side effects is imperfect — a resource may plan successfully but fail at apply because the provider only detects the auth gap when calling the API. Contingency: fast rollback (4-line TF re-add + ~20 s apply + ~60 s SCIM propagation). Two-phase structure is designed so Phase 1 has already proven the CI SP's explicit ACLs are sufficient for additive operations; Phase 2 just removes the safety net. If Phase 2 apply breaks, rollback restores the state Phase 1 ended in.

**Smaller risks** noted without dedicated contingencies:
- SCIM propagation lag after admin removal could cause a 30-60 second inconsistency window. Wait before re-testing.
- Orphan PG role drop could surface a forgotten Lakebase grant (unlikely; audit enumerates first).
- The account-admin spike may take >2 hours. Per Q2-B, that's acceptable with check-ins.

---

## Definition of done (pre-commit checklist)

Before requesting commit approval at check-in #8, every item below must be literally true with cited evidence.

### Infrastructure state

- [ ] `databricks_group_member.terraform_ci_admin` is absent from `terraform/modules/service_principals/main.tf`.
- [ ] `data "databricks_group" "admins"` is absent from `terraform/modules/service_principals/main.tf`.
- [ ] `grep -r "databricks_group_member.terraform_ci_admin\|data.databricks_group.admins" terraform/` returns zero matches.
- [ ] Either `databricks_service_principal_role.terraform_ci_account_admin` is absent (reduction shipped), OR `docs/superpowers/adrs/ADR-006-<topic>.md` exists with a citable provider issue URL or source-code reference.
- [ ] If Lakebase ACLs shipped via `databricks_permissions`: one new resource block in `terraform/environments/dev/main.tf` targeting the `database-projects` object type (via the `database_project_name` attribute on `databricks_permissions`); endpoints inherit auth from the parent project — no separate endpoint resource. `access_control` blocks alphabetically sorted by principal. (Original design hypothesised two resources — project + endpoint — but the audit found Lakebase Autoscaling exposes no endpoint-level ACL surface; see the inventory's §Surprises item 2.)
- [ ] If Lakebase ACLs shipped via `databricks_access_control_rule_set`: rule-set resource block in `terraform/modules/service_principals/main.tf`.
- [ ] Orphan PG role `be66af99-5296-4fd9-887a-c081bce38bfa` absent from Lakebase (`SELECT 1 FROM pg_roles WHERE rolname = 'be66af99-5296-4fd9-887a-c081bce38bfa'` returns 0 rows).

### Test state

- [ ] `src/tests/test_sec4_ci_sp_job_owner.py` contains: (i) existing 4 tests still green, (ii) `test_terraform_ci_admin_group_member_absent`, (iii) `test_admins_group_not_referenced_anywhere`, (iv) ACL-shape tests for Lakebase resources.
- [ ] `src/tests/test_orphan_pg_role_absent.py` exists and passes (or is skipped when creds unavailable).
- [ ] `uv run pytest src/ -v` — all green.
- [ ] `uv run ruff check src/ scripts/` — zero violations.
- [ ] `uv run ruff format --check src/ scripts/` — clean.
- [ ] `uv run pyright src/` — zero errors.

### Infrastructure verification

- [ ] `terraform plan -detailed-exitcode` returns 0 after Phase 1 apply — output saved.
- [ ] `terraform plan -detailed-exitcode` returns 0 after Phase 2 apply — output saved.
- [ ] `terraform plan -refresh-only` succeeds after Phase 2 apply — CI SP can still read state.
- [ ] Final post-Phase-2 working-tree `terraform plan` shows zero changes (idempotence final check).

### Documentation

- [ ] `docs/superpowers/specs/2026-04-17-sec4-workspace-resource-inventory.md` — populated, with "verified against live state" timestamp per row and exact commands used.
- [ ] `docs/superpowers/specs/2026-04-17-sec4-ci-sp-least-privilege-design.md` — this design doc, committed.
- [ ] `docs/superpowers/plans/2026-04-17-sec4-ci-sp-least-privilege.md` — implementation plan from `writing-plans` skill.
- [ ] `SECURITY.md` — INF-01 close-out line added.
- [ ] `ARCHITECTURE.md` §6.1 Security `IAM` row — refreshed.
- [ ] `AI_GOVERNANCE.md` — verified (no changes expected).
- [ ] `CLAUDE.md` — verified (no changes expected).
- [ ] `TODO.md` — SEC4 On-Deck row removed.
- [ ] `terraform/modules/service_principals/main.tf:54-78` comment block rewritten.
- [ ] If ADR-006 written: committed; listed in inventory markdown; referenced from `SECURITY.md`.

### Commit hygiene

- [ ] `git diff --stat main..HEAD` — all changes accounted for.
- [ ] `git status --short` — clean.
- [ ] Commit message drafted (HEREDOC) with evidence citations.

---

## Design self-review checklist

- [x] Every item in §Per-item has either a failing-test-first path or an E2E-verification path.
- [x] Every `terraform apply` happens only after a gate; no silent apply.
- [x] Rollback is documented for every apply step.
- [x] Risks have contingencies that include an explicit human-decision branch where auto-proceed would be wrong (R1).
- [x] All factual claims in §Inventory are marked "to verify" if not yet confirmed live.
- [x] User's "no commits/PRs without explicit approval" rule is honored at 8 separate check-in points.
- [x] No hidden dependencies between items: G precedes A, A precedes core, but B + H are independent and can run in parallel with A.
- [x] Non-goals section exists and is explicit.
