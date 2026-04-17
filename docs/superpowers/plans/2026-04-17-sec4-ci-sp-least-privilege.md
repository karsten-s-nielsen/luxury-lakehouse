# SEC4 CI SP Least-Privilege — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Remove `databricks_group_member.terraform_ci_admin` and reduce or accept `databricks_service_principal_role.terraform_ci_account_admin`; add explicit Lakebase ACLs; drop orphan PG role `be66af99-...`; ship regression tests + living inventory + docs. Single commit, two `terraform apply` gates, 8 human check-ins.

**Architecture:** Two-phase apply sequenced as β per spec: Phase 1 is additive (audit, spike decision, Lakebase ACLs, orphan PG drop, static tests); Phase 2 removes admin-group membership. Hybrid regression coverage: narrow pytest guards in `test_sec4_ci_sp_job_owner.py` + a living inventory markdown. ADR-006 is only acceptable if backed by a citable provider issue URL or source-code-level limitation.

**Tech Stack:** Terraform `>= 1.9` + Databricks provider `>= 1.110` (currently 1.113), Python 3.10 + pytest, `databricks-sdk` Python, `psycopg2-binary`, `uv`.

**Cycle rules (non-negotiable):**
- **No commits, pushes, PRs without explicit user approval** — per-commit, not blanket.
- **Minimal commits** — target a single commit for the entire cycle. Do NOT add per-task `git commit` steps.
- **E2E testing first when possible** — `terraform apply` against live dev workspace before declaring anything done.
- **Evidence-based claims** — every factual assertion cites file:line, command output, or URL.

**Spec reference:** `docs/superpowers/specs/2026-04-17-sec4-ci-sp-least-privilege-design.md`.

---

## File Structure

Files created or modified, by task:

| Task | Action | Path | Responsibility |
|------|--------|------|----------------|
| 1 | Create | `docs/superpowers/specs/2026-04-17-sec4-workspace-resource-inventory.md` | Living inventory of CI SP's TF resources + their auth paths |
| 4 (if reduction) | Modify | `terraform/modules/service_principals/main.tf` (remove lines 91-95) | Drop `databricks_service_principal_role.terraform_ci_account_admin` |
| 4 (if ADR) | Create | `docs/superpowers/adrs/ADR-006-account-admin-floor.md` | Document why `account_admin` is accepted as the floor |
| 5-6 | Modify | `terraform/environments/dev/main.tf` OR `terraform/modules/service_principals/main.tf` | Add Lakebase ACL resource(s) — primary path `databricks_permissions`; fallback `databricks_access_control_rule_set` |
| 6 | Modify | `src/tests/test_sec4_ci_sp_job_owner.py` | Add ACL-shape tests for Lakebase resources (item D) |
| 8 | Create | `src/tests/test_orphan_pg_role_absent.py` | Assert PG role `be66af99-...` absent from `pg_roles` |
| 9 | Modify | `terraform/modules/service_principals/main.tf` (remove lines 80-82, 84-87; rewrite comment block 54-78) | Drop `data.databricks_group.admins` + `databricks_group_member.terraform_ci_admin`; rewrite CI SP roles comment |
| 9 | Modify | `src/tests/test_sec4_ci_sp_job_owner.py` | Add `test_terraform_ci_admin_group_member_absent` + `test_admins_group_not_referenced_anywhere` |
| 11 | Modify | `SECURITY.md` | INF-01 close-out line in audit log |
| 11 | Modify | `ARCHITECTURE.md` | §6.1 Security `IAM` row refresh |
| 11 | Modify | `TODO.md` | Remove SEC4 On-Deck row |
| 11 | Verify only (no changes expected) | `CLAUDE.md`, `AI_GOVERNANCE.md` | Confirm via grep |

Tasks are linear; parallelism is possible (item B spike and item H are independent of item A) but the plan presents a single ordered path for simplicity.

---

## Task 1: Item G — Pre-flight Audit + Inventory Markdown

**Goal:** Produce `docs/superpowers/specs/2026-04-17-sec4-workspace-resource-inventory.md` — the living inventory of every TF resource the CI SP manages, cited against live state.

**Files:**
- Create: `docs/superpowers/specs/2026-04-17-sec4-workspace-resource-inventory.md`

**Prerequisites:**
- `databricks` CLI installed (`uv run databricks --version` or from `PATH`).
- CI SP OAuth client secret available (from `terraform output -raw terraform_ci_sp_application_id` + the secret in Databricks account console).

- [ ] **Step 1.1: Configure a one-time `ci-sp-audit` auth profile**

```bash
export CI_SP_CLIENT_ID=$(AWS_PROFILE=devops-agent terraform -chdir=terraform/environments/dev output -raw terraform_ci_sp_application_id)
echo "CI SP client_id: $CI_SP_CLIENT_ID"
# Retrieve the client secret from Databricks account console UI or a previously-saved location.
# Do NOT commit the secret to the repo or shell history.
databricks auth login \
    --host "$DATABRICKS_HOST" \
    --client-id "$CI_SP_CLIENT_ID" \
    --profile ci-sp-audit
# Interactive; paste client_secret when prompted.
```

Verify with: `databricks current-user me --profile ci-sp-audit` — should return the CI SP's application_id, NOT an email.

- [ ] **Step 1.2: Enumerate TF resources by walking the tree**

Run:
```bash
grep -r '^resource "' terraform/ --include='*.tf' -h | sort -u
```

Expected: ~50-60 resource declarations. Classify each into the buckets from the spec's starting-hypothesis table.

- [ ] **Step 1.3: For each workspace-scoped resource, query live permissions as the CI SP**

For each workspace-scoped resource (start with the 2 Lakebase resources + SQL warehouse + job for comparison), run:

```bash
# SQL warehouse
databricks permissions get warehouses <WAREHOUSE_ID> --profile ci-sp-audit > /tmp/perm_warehouse.json

# Data ingestion job
databricks permissions get jobs <JOB_ID> --profile ci-sp-audit > /tmp/perm_ingestion_job.json

# Lakebase project
databricks permissions get ??? --profile ci-sp-audit   # Probe: this may fail if not supported
```

For Lakebase specifically, probe whether `databricks permissions get` accepts the project + endpoint as `object_type`. If it rejects, note the exact error message. Save outputs to `/tmp/sec4_audit/*.json` for reference. These outputs are evidence for the inventory but are NOT committed to the repo.

- [ ] **Step 1.4: For each UC-scoped resource, query grants**

```bash
databricks grants get catalog soccer_analytics --profile ci-sp-audit > /tmp/grants_catalog.json
databricks grants get schema soccer_analytics.dev_gold --profile ci-sp-audit > /tmp/grants_gold.json
databricks grants get schema soccer_analytics.dev_silver --profile ci-sp-audit > /tmp/grants_silver.json
databricks grants get schema soccer_analytics.bronze --profile ci-sp-audit > /tmp/grants_bronze.json
databricks grants get schema soccer_analytics.observability --profile ci-sp-audit > /tmp/grants_observability.json
```

- [ ] **Step 1.5: Verify the "likely already done" UC items empirically**

Attempt a no-op schema-level modify as CI SP to confirm MANAGE via group ownership works without admin:

```bash
databricks sql-workspace create-query \
    --profile ci-sp-audit \
    --query-text "COMMENT ON SCHEMA soccer_analytics.dev_gold IS 'sec4-audit-probe-$(date +%s)'" \
    --warehouse-id $WAREHOUSE_ID
```

If this succeeds, document in the inventory as "verified MANAGE via group ownership"; if it fails, capture the error and add the schema to the "needs explicit grant" list.

- [ ] **Step 1.6: Write the inventory markdown**

Create `docs/superpowers/specs/2026-04-17-sec4-workspace-resource-inventory.md` using this structure:

```markdown
# SEC4 Workspace Resource Inventory — CI SP Auth Paths

| Field | Value |
|---|---|
| **Date** | 2026-04-17 |
| **Status** | Living document — update on every TF-adding PR |
| **Verification identity** | `luxury-lakehouse-terraform-ci-dev` SP via `databricks-cli --profile ci-sp-audit` |
| **Spec** | `docs/superpowers/specs/2026-04-17-sec4-ci-sp-least-privilege-design.md` |

## Purpose

Enumerate every Terraform-managed resource the CI SP touches, classify its
current auth path (admins-group transitive vs explicit ACL/grant vs
account-admin), and document the target post-SEC4 state. A future TF-adding PR
must add a row here and confirm the CI SP has explicit auth before merge.

## Inventory

| Resource | Scope | CI SP action | Current auth | Target auth | Verification | Timestamp |
|---|---|---|---|---|---|---|
| `module.workflows.databricks_job.data_ingestion` | workspace | create/update | explicit `IS_OWNER` | same | `databricks permissions get jobs <id> --profile ci-sp-audit` → IS_OWNER | 2026-04-17 <HH:MM> UTC |
| ... | ... | ... | ... | ... | ... | ... |
```

Populate one row per resource. For "Verification" column, paste the exact command used (truncate output to the relevant permission level). For "Timestamp", use UTC HH:MM from the moment the command was run.

- [ ] **Step 1.7: Highlight surprises vs starting-hypothesis**

At the bottom of the inventory markdown, add a "## Surprises & deviations from starting-hypothesis" section. List any row where the current auth is different from what the spec predicted, or where "likely already done — verify" turned out to need additional work.

- [ ] **Step 1.8: CHECK-IN #1 with user**

Present the inventory markdown path, summarize the surprises section, and confirm Phase 1 can proceed. Do NOT proceed to Task 2 without explicit user acknowledgment.

---

## Task 2: Item B Spike — Discovery (Provider CHANGELOG + Source)

**Goal:** Gather evidence for whether `account_admin` on the CI SP can be reduced. No TF changes yet. Per-step check-ins with the user.

**Files:**
- No file changes (research task).

- [ ] **Step 2.1: Read provider CHANGELOG since v1.107**

The last `account_admin` investigation was 2026-04-13 during D59; provider was ~v1.107 at that time. Current pin is `>= 1.110.0` at `terraform/environments/dev/main.tf:25` and live state is on 1.113.

```bash
# Clone a shallow copy of the provider if not already
git clone --depth 100 https://github.com/databricks/terraform-provider-databricks.git /tmp/tf-db-provider
cd /tmp/tf-db-provider
cat CHANGELOG.md | head -400   # Covers v1.107 → v1.113
```

Scan for entries matching `(?i)role|admin|workspace.assignment|scim|federation|service.?principal`. Record each relevant entry with its version and line from CHANGELOG.

- [ ] **Step 2.2: Grep provider source for each account-scoped resource's API path**

For each of the 8 account-scoped resources from the inventory, identify the Go source file implementing it and the API path it calls:

```bash
cd /tmp/tf-db-provider

# databricks_service_principal
grep -rn 'resource_service_principal' --include='*.go'

# databricks_service_principal_role
grep -rn 'resource_service_principal_role' --include='*.go'

# databricks_service_principal_federation_policy
grep -rn 'federation_policy' --include='*.go'

# databricks_access_control_rule_set
grep -rn 'rule_set' --include='*.go'

# databricks_group, databricks_group_member
grep -rn 'resource_group' --include='*.go'

# databricks_mws_permission_assignment
grep -rn 'permission_assignment' --include='*.go'
```

For each resource, record:
- The source file + line range of the Create/Update/Delete functions.
- The API endpoint it calls (look for `Config.Client()` calls or SDK method calls like `accountsClient.ServicePrincipals.Create(...)`).
- Whether the SDK call is under `/api/2.0/accounts/*` (account_admin-gated) or `/api/2.0/preview/accounts/scim/v2/*` (potentially finer roles).

- [ ] **Step 2.3: CHECK-IN #2a with user (post-CHANGELOG)**

Report the CHANGELOG findings + the API-path classification. User can redirect if the spike is heading somewhere unproductive.

- [ ] **Step 2.4: Experimental `terraform plan` with `account_admin` removed**

Comment out (do NOT delete) `terraform/modules/service_principals/main.tf:91-95`:

```hcl
# resource "databricks_service_principal_role" "terraform_ci_account_admin" {
#   provider             = databricks.account
#   service_principal_id = databricks_service_principal.terraform_ci.id
#   role                 = "account_admin"
# }
```

Run plan (NO apply):

```bash
cd terraform/environments/dev
AWS_PROFILE=devops-agent terraform plan -no-color 2>&1 | tee /tmp/sec4_spike_plan.txt
```

Expected: plan reports the role removal. The plan operation itself may or may not succeed — if the CI SP lacks read access to account-level resources without `account_admin`, some `data` blocks will fail. Capture the exact error per failing resource.

- [ ] **Step 2.5: For each failing resource, search for narrower roles**

For each resource that failed in Step 2.4, consult the provider source (Step 2.2) to see if the SDK supports a narrower role. Candidate narrower roles:

- `group_admin` — manages group membership
- `workspace_admin` — manages workspace-scoped objects at account level
- `service_principal_manager` — specific to SP management (if it exists)

Check the SDK's `accounts` client for what roles it accepts. The SDK at `github.com/databricks/databricks-sdk-go/service/iam` is the source of truth.

- [ ] **Step 2.6: CHECK-IN #2b with user (post-plan + narrower-role search)**

Report the failing resources + candidate narrower roles, WITH evidence (source citations). User decides whether the spike has produced enough signal to reach a decision, or whether more investigation is needed.

- [ ] **Step 2.7: Revert the comment-out**

Whether spike continues or concludes, revert the comment-out so the working tree is clean:

```bash
git checkout -- terraform/modules/service_principals/main.tf
```

---

## Task 3: Item B — Decision & Implementation

**Goal:** Based on Task 2's evidence, either ship the `account_admin` reduction OR write ADR-006. No apply yet.

**Files (conditional):**
- If reduction shipped: Modify `terraform/modules/service_principals/main.tf:91-95` (remove block)
- If ADR written: Create `docs/superpowers/adrs/ADR-006-account-admin-floor.md`

- [ ] **Step 3.1: Classify the spike outcome**

| Outcome | Criteria | Branch |
|---|---|---|
| Reducible | Task 2 identified a narrower role that passes `terraform plan` for every account-scoped resource | Go to Step 3.2 |
| Irreducible — citable | No narrower role works, AND Task 2 produced at least one of: (a) a GitHub issue URL at `github.com/databricks/terraform-provider-databricks`, (b) a source-code reference in the provider or SDK showing the account_admin requirement, (c) an upstream documentation link stating the role is required | Go to Step 3.4 (write ADR-006) |
| Irreducible — no citation | No narrower role works AND no citable limitation found | Go to Step 3.6 (escalate) |

- [ ] **Step 3.2 (reduction path): Remove `terraform_ci_account_admin` block + replace with narrower role**

If the spike found a narrower role (e.g., `group_admin`), edit `terraform/modules/service_principals/main.tf` to replace the block:

```hcl
# Before (lines 91-95):
resource "databricks_service_principal_role" "terraform_ci_account_admin" {
  provider             = databricks.account
  service_principal_id = databricks_service_principal.terraform_ci.id
  role                 = "account_admin"
}

# After:
resource "databricks_service_principal_role" "terraform_ci_<narrower>" {
  provider             = databricks.account
  service_principal_id = databricks_service_principal.terraform_ci.id
  role                 = "<narrower_role_name>"
}
```

Also update the comment block at `:54-78` to reflect the new floor.

- [ ] **Step 3.3 (reduction path): `terraform plan` to verify clean**

```bash
cd terraform/environments/dev
AWS_PROFILE=devops-agent terraform fmt -recursive
AWS_PROFILE=devops-agent terraform validate
AWS_PROFILE=devops-agent terraform plan -no-color 2>&1 | tail -60
```

Expected: 1 resource destroyed (old role binding) + 1 resource created (new narrower role binding), no other changes. If other changes appear, STOP and investigate.

Go to Step 3.7 (check-in).

- [ ] **Step 3.4 (ADR path): Draft ADR-006**

Create `docs/superpowers/adrs/ADR-006-account-admin-floor.md` using `docs/superpowers/adrs/ADR-TEMPLATE.md`. Complete content:

```markdown
# ADR-006: Terraform CI SP `account_admin` Role Accepted as Floor

| Field | Value |
|---|---|
| **Date** | 2026-04-17 |
| **Status** | Accepted |
| **Deciders** | Karsten Nielsen |

## Context

The Terraform CI service principal (`luxury-lakehouse-terraform-ci-dev`)
currently holds `account_admin` on the Databricks account. SEC-AUDIT-v1.12.0
INF-01 (CWE-250) flagged this as a least-privilege violation. During the
SEC4 cycle (2026-04-17), we investigated whether the role can be reduced.

The CI SP manages 8 account-scoped resources:

<list from Task 1 inventory>

Each of these calls an account-level API that the Databricks Terraform
provider (v1.113) gates on `account_admin`. The spike in Task 2 of this
cycle confirmed no narrower role exists that covers all 8 resources.

## Decision

Accept `account_admin` as the floor for the Terraform CI service principal.
`databricks_group_member.terraform_ci_admin` (workspace admin) is removed;
`databricks_service_principal_role.terraform_ci_account_admin` remains.

## Alternatives considered

| Option | Pros | Cons | Why rejected |
|---|---|---|---|
| A. Reduce to `workspace_admin` | Narrower scope | Does not cover account-scoped resources like federation policy | The 8 account-scoped resources require account-level API access |
| B. Reduce to `group_admin` + per-resource grants | Narrow per-feature | No `group_admin` role exists in the Databricks IAM model as of SDK <version> | Cited: <provider file:line OR issue URL> |
| C. Bootstrap account-scoped resources once, then remove `account_admin` | Smallest steady-state footprint | Provider requires account_admin to even READ these resources during `terraform plan` | Cited: <provider file:line OR issue URL> |
| D. Accept the floor (chosen) | No further provider work needed | Retains one wide privilege | — |

## Consequences

### Positive

- Workspace-admin membership eliminated; the CI SP no longer holds transitive admin on workspace objects.
- Lakebase and other workspace-scoped resources managed via explicit `databricks_permissions`.

### Negative

- The CI SP still holds `account_admin`. Any future compromise of the CI SP's OAuth credential would give the attacker account-level admin.
- Mitigation: OIDC-federated auth (no long-lived secret), GitHub repo pinned to `repository` claim, rotation via federation policy.

### Neutral

- If the provider adds narrower account-level roles in the future, reopening the reduction is straightforward — the code path is the same.

## Related

- **Commits:** <to be filled at commit time>
- **Specs:** `docs/superpowers/specs/2026-04-17-sec4-ci-sp-least-privilege-design.md`
- **Spike evidence:** `docs/superpowers/specs/2026-04-17-sec4-workspace-resource-inventory.md` § Account-scoped resources
- **External references:**
  - Databricks Terraform provider: `github.com/databricks/terraform-provider-databricks@v1.113.0`
  - Issue URL or source citation from Task 2.2 / 2.5: <paste here>
  - Databricks account IAM docs: <link>

## Notes

Spike workplan output from Task 2 of the SEC4 cycle. The per-resource API
classifications are copied to the inventory markdown for ongoing reference.
```

Fill in the `<placeholders>` with exact values from Task 2 evidence. Every Alternative's "Why rejected" row MUST cite either a provider source file:line, a GitHub issue URL, or a documentation URL.

- [ ] **Step 3.5 (ADR path): Verify citation strength**

Before proceeding, re-read the ADR. Every rejection row must satisfy one of:
- A URL to an open/closed GitHub issue at `github.com/databricks/terraform-provider-databricks`.
- A specific file:line reference in the provider source tree.
- A specific file:line reference in the Databricks SDK (`github.com/databricks/databricks-sdk-go`).
- A URL to official Databricks documentation stating the role requirement.

If any rejection row lacks such a citation, go to Step 3.6 (escalate).

- [ ] **Step 3.6 (escalate path): File upstream issue**

If the spike produced no citable evidence for the account_admin floor, the cycle's stricter bar requires filing an issue:

```bash
# Confirm with user before filing publicly
gh issue create \
    --repo databricks/terraform-provider-databricks \
    --title "account_admin required for <resource> — is a narrower role feasible?" \
    --body-file /tmp/sec4_issue_body.md
```

User must approve the body text before the issue is filed. Once filed, capture the issue URL and use it as the citation in ADR-006. Return to Step 3.4.

- [ ] **Step 3.7: CHECK-IN #3 with user**

Present the outcome:
- If reduction: the TF diff + `terraform plan` output (not yet applied).
- If ADR-006: the ADR draft with all citations filled in.
- If escalation was needed: the issue URL + the ADR-006 draft citing it.

User approves before moving to Task 4.

---

## Task 4: Item A — Provider Support Probe for Lakebase `databricks_permissions`

**Goal:** Determine which API path supports ACL management on `databricks_postgres_project` and `databricks_postgres_endpoint`. No TF changes yet; pure research + draft.

**Files:**
- No file changes in this task (drafts may be held in `/tmp/` for reference).

- [ ] **Step 4.1: Check provider docs for `permissions` resource supported `object_type`**

```bash
# Read the provider docs inline (provider cloned in Task 2.1)
cd /tmp/tf-db-provider
find docs/resources -name 'permissions.md' -exec cat {} \;
# Also check if there's a newer permissions.md under the website folder
find . -name 'permissions*.md' | xargs grep -l 'object_type'
```

Scan for the list of supported `object_type` values. Record whether `postgres_project`, `postgres_endpoint`, or any Lakebase-adjacent type appears.

- [ ] **Step 4.2: Grep provider source for Lakebase permissions surface**

```bash
cd /tmp/tf-db-provider
grep -rn 'postgres' --include='*.go' permissions/
grep -rn 'lakebase\|postgres_project\|postgres_endpoint' --include='*.go' | head -30
```

Identify whether the permissions subsystem has any code path that handles Lakebase objects.

- [ ] **Step 4.3: Check `databricks_access_control_rule_set` for Lakebase support**

```bash
cd /tmp/tf-db-provider
grep -rn 'rule_set\|access_control_rule' --include='*.go' | head -30
grep -rn 'lakebase\|postgres' --include='*.go' | grep -i 'rule\|access'
```

The existing `databricks_access_control_rule_set.ingestion_sp_user_role` at `terraform/modules/service_principals/main.tf:36-43` is the pattern to mirror. Check whether the rule-set resource has a `name` template that matches postgres resources.

- [ ] **Step 4.4: Classify the outcome**

| Outcome | Action |
|---|---|
| `databricks_permissions` supports Lakebase directly | Draft primary path (Step 4.5) |
| Only `databricks_access_control_rule_set` works | Draft fallback path (Step 4.6) |
| Neither works — **HARD BLOCKER (Risk R1)** | Go to Step 4.7 (escalate) |

- [ ] **Step 4.5 (primary path): Draft `databricks_permissions` blocks**

Prepare the exact HCL to be added to `terraform/environments/dev/main.tf` (at a position alphabetical-to-existing resources, likely after the `ingestion_job_acl` block around line 234). Draft only — do NOT write to file yet:

```hcl
resource "databricks_permissions" "lakebase_project_acl" {
  postgres_project_id = module.lakebase.project_id

  access_control {
    service_principal_name = module.service_principals.terraform_ci_sp_application_id
    permission_level       = "<MIN_FOR_MANAGE>"
  }
}

resource "databricks_permissions" "lakebase_endpoint_acl" {
  postgres_endpoint_id = module.lakebase.endpoint_name

  access_control {
    service_principal_name = module.service_principals.terraform_ci_sp_application_id
    permission_level       = "<MIN_FOR_MANAGE>"
  }
}
```

Fill in `<MIN_FOR_MANAGE>` by reading the supported `permission_level` values from the provider docs (likely `CAN_MANAGE` by analogy to SQL warehouse). Confirm the attribute names (`postgres_project_id` vs `postgres_project` vs something else) against the provider source.

Also check whether `module.lakebase.project_id` and `module.lakebase.endpoint_name` exist as outputs:

```bash
grep -A3 '^output' terraform/modules/lakebase/outputs.tf
```

If `endpoint_name` is not exposed, add an output in that file:

```hcl
output "endpoint_name" {
  description = "Lakebase primary endpoint name for ACL references"
  value       = databricks_postgres_endpoint.primary.name
}
```

- [ ] **Step 4.6 (fallback path): Draft `databricks_access_control_rule_set` blocks**

If `databricks_permissions` does not support Lakebase, draft rule-set resources mirroring the existing `ingestion_sp_user_role` pattern. The `name` attribute format is documented at:

```
accounts/{account_id}/servicePrincipals/{sp_application_id}/ruleSets/default
accounts/{account_id}/{resource_type}/{resource_id}/ruleSets/default
```

Check the provider source for the exact rule-set name format supported for postgres resources.

- [ ] **Step 4.7 (escalate path): HARD BLOCKER — pause for user decision**

Per spec Risk R1, if no provider path works, do NOT proceed to Phase 1 apply. Prepare an evidence package:

- Provider docs excerpt (what's supported, what isn't).
- Provider source greps showing the gap.
- Two scope-decision options for user:
  - **R1-A**: Keep admins-group membership, rescope SEC4 to "reduced as much as provider allows" + write ADR-007 documenting the gap.
  - **R1-B**: Move Lakebase management out of Terraform entirely (large scope expansion — separate cycle).

CHECK-IN with user. Do NOT make a choice autonomously. Cycle may pause indefinitely here.

- [ ] **Step 4.8: CHECK-IN #4 with user**

Present the provider-support finding, the draft resource blocks (primary or fallback), and a draft `terraform plan` output if possible. User approves before the blocks are written to files in Task 6.

---

## Task 5: Item D — Failing Tests for Lakebase ACL Shape (TDD First)

**Goal:** Add pytest assertions for the Lakebase ACL resources. Run them to confirm they FAIL (resources not yet in TF). This is the TDD "red" step before Task 6 makes them green.

**Files:**
- Modify: `src/tests/test_sec4_ci_sp_job_owner.py`

- [ ] **Step 5.1: Read the existing test file to understand the helper pattern**

```bash
cat src/tests/test_sec4_ci_sp_job_owner.py
```

Confirm that `_extract_resource_body`, `_access_control_principals_in_order`, and `_assert_acl_resource_correctly_shaped` helpers exist and are usable.

- [ ] **Step 5.2: Add two new test functions**

Append to `src/tests/test_sec4_ci_sp_job_owner.py`:

```python
def test_lakebase_project_acl_exists_and_is_correctly_shaped() -> None:
    """SEC4: the Lakebase project ACL must grant CI SP the minimum-necessary
    permission so the admins-group membership can be removed."""
    text = _DEV.read_text(encoding="utf-8")
    body = _extract_resource_body(text, "databricks_permissions", "lakebase_project_acl")
    assert body, "resource databricks_permissions.lakebase_project_acl not found"
    principals = _access_control_principals_in_order(body)
    perms = dict(principals)
    assert _CI_SP_REF in perms, "lakebase_project_acl: CI SP access_control block missing"
    assert perms[_CI_SP_REF] in {"CAN_MANAGE", "IS_OWNER"}, (
        f"lakebase_project_acl: CI SP must be CAN_MANAGE or IS_OWNER, got {perms[_CI_SP_REF]!r}"
    )
    principal_refs = [p for p, _ in principals]
    assert principal_refs == sorted(principal_refs), (
        f"lakebase_project_acl: access_control blocks must be sorted alphabetically by "
        f"service_principal_name; got {principal_refs}"
    )


def test_lakebase_endpoint_acl_exists_and_is_correctly_shaped() -> None:
    """SEC4: the Lakebase endpoint ACL must grant CI SP the minimum-necessary
    permission so the admins-group membership can be removed."""
    text = _DEV.read_text(encoding="utf-8")
    body = _extract_resource_body(text, "databricks_permissions", "lakebase_endpoint_acl")
    assert body, "resource databricks_permissions.lakebase_endpoint_acl not found"
    principals = _access_control_principals_in_order(body)
    perms = dict(principals)
    assert _CI_SP_REF in perms, "lakebase_endpoint_acl: CI SP access_control block missing"
    assert perms[_CI_SP_REF] in {"CAN_MANAGE", "IS_OWNER"}, (
        f"lakebase_endpoint_acl: CI SP must be CAN_MANAGE or IS_OWNER, got {perms[_CI_SP_REF]!r}"
    )
    principal_refs = [p for p, _ in principals]
    assert principal_refs == sorted(principal_refs), (
        f"lakebase_endpoint_acl: access_control blocks must be sorted alphabetically by "
        f"service_principal_name; got {principal_refs}"
    )
```

**Fallback-path adjustment**: If Task 4 chose the `databricks_access_control_rule_set` fallback, these tests must instead parse rule-set resources in `terraform/modules/service_principals/main.tf`. Adjust `_DEV` path to a rule-set-file constant + change the resource type in `_extract_resource_body` calls. Match the implementation from Task 4 Step 4.6.

- [ ] **Step 5.3: Run tests — expect FAIL**

```bash
uv run pytest src/tests/test_sec4_ci_sp_job_owner.py::test_lakebase_project_acl_exists_and_is_correctly_shaped -v
uv run pytest src/tests/test_sec4_ci_sp_job_owner.py::test_lakebase_endpoint_acl_exists_and_is_correctly_shaped -v
```

Expected: 2 FAIL with `resource ... not found` (resources don't exist yet in TF).

If tests PASS unexpectedly, STOP — the resources already exist and this is a duplicate. Investigate.

---

## Task 6: Item A — Implement Lakebase ACL Resources

**Goal:** Write the drafted TF blocks from Task 4 to the actual file. Confirm tests pass. No apply yet.

**Files:**
- Modify: `terraform/environments/dev/main.tf` (primary path) OR `terraform/modules/service_principals/main.tf` (fallback)
- Modify: `terraform/modules/lakebase/outputs.tf` (if new output needed)

- [ ] **Step 6.1: Add the resource blocks**

Insert the drafted HCL from Task 4 Step 4.5 (or 4.6) into the appropriate file. Placement guidance:
- Primary path (`databricks_permissions`): after the `ingestion_job_acl` block (around line 234 in `terraform/environments/dev/main.tf`), alphabetically between the existing blocks if any.
- Fallback path (`databricks_access_control_rule_set`): after the existing `ingestion_sp_user_role` rule-set in `terraform/modules/service_principals/main.tf:36-43`.

Include a comment block above each new resource citing the SEC4 spec and noting the alphabetical-sort convention.

- [ ] **Step 6.2: Add `endpoint_name` output if needed**

If Task 4 determined that `module.lakebase.endpoint_name` is not exposed, append to `terraform/modules/lakebase/outputs.tf`:

```hcl
output "endpoint_name" {
  description = "Lakebase primary endpoint name for ACL references (SEC4)"
  value       = databricks_postgres_endpoint.primary.name
}
```

- [ ] **Step 6.3: `terraform fmt` + `terraform validate`**

```bash
cd terraform/environments/dev
AWS_PROFILE=devops-agent terraform fmt -recursive
AWS_PROFILE=devops-agent terraform validate
```

Expected: `Success! The configuration is valid.`

If validation fails, fix syntax errors and re-run.

- [ ] **Step 6.4: Run Task 5 tests — expect PASS**

```bash
uv run pytest src/tests/test_sec4_ci_sp_job_owner.py -v
```

Expected: ALL tests PASS (existing 4 + new 2 = 6 PASS; add counts for any other tests added by Task 2 outcomes).

- [ ] **Step 6.5: `terraform plan` — observe expected additions**

```bash
cd terraform/environments/dev
AWS_PROFILE=devops-agent terraform plan -no-color 2>&1 | tee /tmp/sec4_phase1_plan.txt | tail -100
```

Expected: plan shows `Plan: N to add, 0 to change, 0 to destroy` where N equals the number of new resources (2 for primary path; 1-2 for fallback path). If reduction is happening in the same phase, the plan also shows `-1 destroy / +1 add` for the account-role swap. No other changes.

If other changes appear (phantom drift, unexpected modifications), STOP and investigate — the two-phase cycle assumes a clean baseline before apply.

---

## Task 7: Phase 1 `terraform apply` + Idempotence Verification

**Goal:** Apply the Phase 1 changes (Lakebase ACLs + item B reduction if chosen) to the live dev workspace. Confirm idempotent re-plan.

**Files:**
- No file changes (infrastructure action).

- [ ] **Step 7.1: Apply Phase 1 changes**

```bash
cd terraform/environments/dev
AWS_PROFILE=devops-agent terraform apply -no-color 2>&1 | tee /tmp/sec4_phase1_apply.txt | tail -60
```

Expected: apply summary matches plan summary — N resources added (+ 1 changed if reduction). No destroys except the old `account_admin` role if reduction is happening.

If apply fails, capture the exact error, revert with `git checkout -- .`, and CHECK-IN with user.

- [ ] **Step 7.2: Idempotence check — re-run plan**

```bash
AWS_PROFILE=devops-agent terraform plan -detailed-exitcode -no-color 2>&1 | tee /tmp/sec4_phase1_replan.txt | tail -40
echo "Exit code: $?"
```

Expected: exit code 0 (no changes). If exit code is 2 (changes detected), the new resources are drifting — investigate before proceeding.

- [ ] **Step 7.3: Verify CI SP retains access via probe**

As CI SP (using `--profile ci-sp-audit` from Task 1), attempt to read the Lakebase project:

```bash
databricks postgres list-projects --profile ci-sp-audit
```

Expected: returns the `soccer-analytics-dev` project. Confirms the new ACL is effective.

- [ ] **Step 7.4: Run all existing tests**

```bash
uv run pytest src/tests/test_sec4_ci_sp_job_owner.py -v
```

Expected: all PASS (no regressions from the apply).

- [ ] **Step 7.5: CHECK-IN #5 with user**

Present:
- `/tmp/sec4_phase1_apply.txt` summary (last 60 lines).
- `/tmp/sec4_phase1_replan.txt` showing exit code 0.
- Test output.
- Any surprises.

User approves before Task 8.

---

## Task 8: Item H — Orphan PG Role Cleanup

**Goal:** Remove PG role `be66af99-5296-4fd9-887a-c081bce38bfa` from Lakebase's backing Postgres. TDD — write test first, then execute SQL, verify.

**Files:**
- Create: `src/tests/test_orphan_pg_role_absent.py`

**Prerequisites:**
- Lakebase connection env vars set in `.env` or `hf_taipy_app/.env` (`LAKEBASE_HOST`, `LAKEBASE_ENDPOINT_NAME`, `LAKEBASE_DATABASE`).
- `databricks-sdk` installed (`uv sync --extra sdk`).

- [ ] **Step 8.1: Inspect the existing Lakebase auth pattern**

```bash
grep -n 'psycopg2\|postgres\|pg_' scripts/run_lakebase_grants.py | head -30
```

Identify the function or pattern used to obtain a Lakebase connection string. The script uses `WorkspaceClient` + base64-decoded token per `feedback_serverless_secrets.md`.

- [ ] **Step 8.2: Write `test_orphan_pg_role_absent.py`**

Create `src/tests/test_orphan_pg_role_absent.py`:

```python
"""SEC4 item H: assert orphan PG role `be66af99-...` is absent from Lakebase.

This role was granted manually during the 2026-04-17 warm-tier incident and
documented in ADR-005 §Neutral (line 69) as pre-existing. SEC4 removes it;
this test guards against re-introduction.

The test connects to Lakebase via the same auth path as
`scripts/run_lakebase_grants.py`. It is skipped when Lakebase creds are not
available (CI without secrets, most developer machines).
"""

from __future__ import annotations

import os
import pytest

_ORPHAN_ROLE = "be66af99-5296-4fd9-887a-c081bce38bfa"


@pytest.mark.skipif(
    not os.getenv("LAKEBASE_HOST"),
    reason="Lakebase creds not available (LAKEBASE_HOST unset)",
)
def test_orphan_pg_role_absent_from_pg_roles() -> None:
    """pg_roles must not contain the orphan UUID. Removed in SEC4 cycle."""
    import psycopg2

    # Connection helpers live in the grants script — import lazily so tests
    # do not force the dependency when the skip kicks in.
    from scripts.run_lakebase_grants import _connect_as_superuser

    with _connect_as_superuser() as conn, conn.cursor() as cur:
        cur.execute(
            "SELECT COUNT(*) FROM pg_roles WHERE rolname = %s",
            (_ORPHAN_ROLE,),
        )
        (count,) = cur.fetchone()
    assert count == 0, (
        f"Orphan PG role {_ORPHAN_ROLE!r} still present in pg_roles — "
        f"SEC4 item H incomplete or re-introduction occurred."
    )
```

**Auth-path note:** if `scripts/run_lakebase_grants.py` does not expose a `_connect_as_superuser()` helper, add one there as a small refactor, OR inline the connection logic in the test. The test file is the source of truth for item H; the grants script is just reusable infrastructure.

- [ ] **Step 8.3: Run test — expect FAIL**

```bash
uv run pytest src/tests/test_orphan_pg_role_absent.py -v
```

Expected: FAIL with "orphan PG role ... still present". If the test is SKIPPED, Lakebase creds are missing — run `source hf_taipy_app/.env` first and retry.

If the test PASSES immediately (role already absent), it means someone else removed it. Investigate via git log + memory before proceeding; DO NOT skip the rest of Task 8.

- [ ] **Step 8.4: Enumerate the orphan role's grants and memberships (pre-state)**

```bash
# One-off exploratory SQL — connection via your preferred method (e.g., psql, or a Python one-liner using the grants script helper).
# Example using psql with ~/.pgpass configured:
psql "$LAKEBASE_CONNECTION_STRING" -c "
  SELECT rolname, rolcanlogin, rolsuper FROM pg_roles WHERE rolname = 'be66af99-5296-4fd9-887a-c081bce38bfa';
  SELECT nspname, relname, privilege_type
  FROM information_schema.role_table_grants
  WHERE grantee = 'be66af99-5296-4fd9-887a-c081bce38bfa';
  SELECT DISTINCT grantor, grantee, privilege_type
  FROM information_schema.role_usage_grants
  WHERE grantee = 'be66af99-5296-4fd9-887a-c081bce38bfa';
  SELECT m.rolname AS member, g.rolname AS in_group
  FROM pg_auth_members am
  JOIN pg_roles m ON m.oid = am.member
  JOIN pg_roles g ON g.oid = am.roleid
  WHERE m.rolname = 'be66af99-5296-4fd9-887a-c081bce38bfa';
"
```

Save the output to `/tmp/sec4_orphan_prestate.txt` and paste the summary (grant list + membership list) into the inventory markdown's item-H section as audit evidence.

- [ ] **Step 8.5: Execute REASSIGN + DROP OWNED + DROP ROLE**

```bash
psql "$LAKEBASE_CONNECTION_STRING" -c "
  REASSIGN OWNED BY \"be66af99-5296-4fd9-887a-c081bce38bfa\" TO databricks_superuser;
  DROP OWNED BY \"be66af99-5296-4fd9-887a-c081bce38bfa\";
  DROP ROLE \"be66af99-5296-4fd9-887a-c081bce38bfa\";
"
```

Expected output: `REASSIGN OWNED`, `DROP OWNED`, `DROP ROLE` — three success lines.

If `DROP ROLE` fails with `role "be66af99-..." cannot be dropped because some objects depend on it`, the REASSIGN+DROP OWNED was insufficient. Capture the error and run:

```bash
psql "$LAKEBASE_CONNECTION_STRING" -c "\du \"be66af99-5296-4fd9-887a-c081bce38bfa\""
```

to see the remaining dependencies. Investigate before proceeding. Do NOT force the drop with `CASCADE` without consulting the user (side effects on other identities possible).

- [ ] **Step 8.6: Run test — expect PASS**

```bash
uv run pytest src/tests/test_orphan_pg_role_absent.py -v
```

Expected: 1 PASS.

- [ ] **Step 8.7: CHECK-IN #6 with user**

Present:
- Pre-state SQL output (from Step 8.4) — what grants the orphan held.
- Execution output (from Step 8.5) — REASSIGN / DROP OWNED / DROP ROLE success lines.
- Test output (from Step 8.6) — PASS.

User approves before Task 9.

---

## Task 9: Core — Remove Admin-Group Membership (TDD)

**Goal:** Remove `databricks_group_member.terraform_ci_admin` and `data.databricks_group.admins` from Terraform. Write regression tests first, then edit TF, verify tests pass. No apply yet.

**Files:**
- Modify: `src/tests/test_sec4_ci_sp_job_owner.py` (add 2 new tests — item C)
- Modify: `terraform/modules/service_principals/main.tf` (remove lines 80-82 + 84-87; rewrite comment block 54-78)

- [ ] **Step 9.1: Write `test_terraform_ci_admin_group_member_absent` and `test_admins_group_not_referenced_anywhere`**

Append to `src/tests/test_sec4_ci_sp_job_owner.py`:

```python
_SERVICE_PRINCIPALS_MAIN = (
    Path(__file__).resolve().parents[2]
    / "terraform"
    / "modules"
    / "service_principals"
    / "main.tf"
)
_TERRAFORM_DIR = Path(__file__).resolve().parents[2] / "terraform"


def test_terraform_ci_admin_group_member_absent() -> None:
    """SEC4: the CI SP must not be a member of the workspace admins group.

    INF-01 (CWE-250) — least privilege. The admins-group membership was
    replaced with explicit per-resource ACLs on workspace-scoped objects.
    See SECURITY.md audit log and ADR-006 (if applicable).
    """
    text = _SERVICE_PRINCIPALS_MAIN.read_text(encoding="utf-8")
    body = _extract_resource_body(text, "databricks_group_member", "terraform_ci_admin")
    assert body is None, (
        "databricks_group_member.terraform_ci_admin must not be declared — "
        "SEC4 removed it to satisfy INF-01 least-privilege."
    )
    # The paired data source must also be removed (no reason for it to exist
    # once the membership resource is gone).
    pattern = re.compile(
        r'^data\s+"databricks_group"\s+"admins"\s*\{', re.MULTILINE
    )
    assert not pattern.search(text), (
        "data.databricks_group.admins is unused after terraform_ci_admin removal "
        "and must be deleted — keeping it invites re-introduction of the "
        "group_member resource."
    )


def test_admins_group_not_referenced_anywhere() -> None:
    """SEC4: no terraform file may reference data.databricks_group.admins.

    Cross-file grep — prevents a future author from re-adding the data source
    in a different module and silently re-establishing admin-group membership.
    """
    offenders: list[str] = []
    for tf_file in _TERRAFORM_DIR.rglob("*.tf"):
        text = tf_file.read_text(encoding="utf-8")
        if "data.databricks_group.admins" in text or 'data "databricks_group" "admins"' in text:
            offenders.append(str(tf_file.relative_to(_TERRAFORM_DIR.parent)))
    assert not offenders, (
        f"admins-group references found in: {offenders}. "
        f"SEC4 removed all admin-group references; re-introduction is forbidden."
    )
```

- [ ] **Step 9.2: Run new tests — expect FAIL**

```bash
uv run pytest src/tests/test_sec4_ci_sp_job_owner.py::test_terraform_ci_admin_group_member_absent \
                src/tests/test_sec4_ci_sp_job_owner.py::test_admins_group_not_referenced_anywhere -v
```

Expected: 2 FAIL (`databricks_group_member.terraform_ci_admin` still present; `admins-group references found in: ['terraform/modules/service_principals/main.tf']`).

- [ ] **Step 9.3: Edit `terraform/modules/service_principals/main.tf` — remove 3 blocks + rewrite comment**

Delete the following:

Lines 80-82:
```hcl
data "databricks_group" "admins" {
  display_name = "admins"
}
```

Lines 84-87:
```hcl
resource "databricks_group_member" "terraform_ci_admin" {
  group_id  = data.databricks_group.admins.id
  member_id = databricks_service_principal.terraform_ci.id
}
```

Then rewrite the comment block at lines 54-78 to reflect the new floor. Replacement content:

```hcl
# ── CI SP Roles: Minimum-Necessary Floor ────────────────────────────────
# After SEC4 (2026-04-17), the Terraform CI SP holds ONLY the privileges
# strictly required for `terraform plan` and `terraform apply` in CI:
#
# 1. Catalog ALL_PRIVILEGES + MANAGE via group ownership
#    (databricks_grant.ci_sp_catalog in environments/dev/main.tf + membership
#    in databricks_group.dbt_owners below):
#    Covers all Unity Catalog objects — schemas, volumes, grants.
#    Verified 2026-04-16: removing the grant breaks terraform plan with
#    "does not have USE CATALOG" / "USE SCHEMA" on 10+ resources.
#
# 2. Per-resource databricks_permissions on workspace-scoped objects:
#    - SQL warehouse CAN_MANAGE (environments/dev/main.tf:184-207)
#    - Ingestion job IS_OWNER (environments/dev/main.tf:218-234)
#    - Lakebase project + endpoint <MIN_LEVEL>
#      (environments/dev/main.tf:<line-range>)
#    These replace the historical admins-group transitive authorization.
#
# 3. Account admin (databricks_service_principal_role.terraform_ci_account_admin):
#    Required for 8 account-scoped resources (federation policy, rule sets,
#    group membership). Per SEC4 spike (2026-04-17), confirmed irreducible
#    in provider v1.113 — see ADR-006 for citations.
#    [OR, if reduction shipped:
#    Narrower role <X> (see ADR-006 or spike evidence), sufficient for the
#    8 account-scoped resources. Reduces attack surface vs historical account_admin.]
#
# NOT present anymore (SEC4 removal, 2026-04-17):
# - databricks_group_member.terraform_ci_admin — workspace admin-group membership
# - data.databricks_group.admins                — no reason to keep once member resource is gone
# Regression guards: src/tests/test_sec4_ci_sp_job_owner.py
```

Fill in the `<MIN_LEVEL>` and `<line-range>` placeholders with the actual values from Task 6. Choose the bracket ending appropriate to Task 3's outcome.

- [ ] **Step 9.4: Run all SEC4 tests — expect PASS**

```bash
uv run pytest src/tests/test_sec4_ci_sp_job_owner.py -v
```

Expected: ALL PASS (existing 4 + Lakebase-ACL 2 + admin-absent 2 = 8 or more PASS).

- [ ] **Step 9.5: `terraform fmt` + `terraform validate`**

```bash
cd terraform/environments/dev
AWS_PROFILE=devops-agent terraform fmt -recursive
AWS_PROFILE=devops-agent terraform validate
```

Expected: `Success! The configuration is valid.`

- [ ] **Step 9.6: `terraform plan` — observe expected removals**

```bash
AWS_PROFILE=devops-agent terraform plan -no-color 2>&1 | tee /tmp/sec4_phase2_plan.txt | tail -40
```

Expected: `Plan: 0 to add, 0 to change, 1 to destroy` (the `databricks_group_member.terraform_ci_admin` resource). The data source removal is state-only and not reported. Any other changes → STOP and investigate.

---

## Task 10: Phase 2 `terraform apply` + Idempotence Verification

**Goal:** Apply the admin-removal change to the live dev workspace. Confirm CI SP still works.

**Files:**
- No file changes (infrastructure action).

- [ ] **Step 10.1: Apply Phase 2**

```bash
cd terraform/environments/dev
AWS_PROFILE=devops-agent terraform apply -no-color 2>&1 | tee /tmp/sec4_phase2_apply.txt | tail -40
```

Expected: `Apply complete! Resources: 0 added, 0 changed, 1 destroyed.` plus the destroyed resource name.

If apply fails, the admin-group removal was rejected (possible cause: SCIM consistency, provider-side validation). Rollback per spec: re-add the two TF blocks (+2 lines each), re-apply. CHECK-IN with user before re-attempting.

- [ ] **Step 10.2: Wait for SCIM propagation**

Databricks SCIM changes may take 30-60 seconds to propagate. Sleep briefly before the next verification step:

```bash
sleep 60
```

- [ ] **Step 10.3: Idempotence check — re-run plan**

```bash
AWS_PROFILE=devops-agent terraform plan -detailed-exitcode -no-color 2>&1 | tee /tmp/sec4_phase2_replan.txt | tail -40
echo "Exit code: $?"
```

Expected: exit code 0 (no changes).

- [ ] **Step 10.4: Verify CI SP can still read state (`plan -refresh-only`)**

```bash
AWS_PROFILE=devops-agent terraform plan -refresh-only -no-color 2>&1 | tail -40
```

Expected: refresh-only completes without errors. Any `403 Forbidden` or `PERMISSION_DENIED` → the CI SP lost an access path; rollback (Step 10.5) and investigate.

- [ ] **Step 10.5 (contingency only — skip if 10.4 passed): Rollback**

If 10.4 shows permission errors, rollback is a 4-line TF edit + re-apply:

```bash
# Re-add the two TF blocks to terraform/modules/service_principals/main.tf
# (use git history to see the exact content: git show HEAD:terraform/modules/service_principals/main.tf | grep -A3 "terraform_ci_admin")
git checkout -- terraform/modules/service_principals/main.tf
# Now the working tree has the admin-group membership back.
cd terraform/environments/dev
AWS_PROFILE=devops-agent terraform apply -auto-approve -no-color
```

Then CHECK-IN with user about the permission error that triggered rollback.

- [ ] **Step 10.6: Run all tests**

```bash
uv run pytest src/tests/test_sec4_ci_sp_job_owner.py src/tests/test_orphan_pg_role_absent.py -v
```

Expected: all PASS.

- [ ] **Step 10.7: CHECK-IN #7 with user**

Present:
- `/tmp/sec4_phase2_apply.txt` — destroy confirmation.
- `/tmp/sec4_phase2_replan.txt` — exit code 0.
- `terraform plan -refresh-only` output — clean.
- Test output — all PASS.

User approves before Task 11.

---

## Task 11: Item E — Documentation Sweep

**Goal:** Update all repo-level docs to reflect SEC4 completion. Remove the TODO entry. Verify no hidden references to admin-group remain.

**Files:**
- Modify: `SECURITY.md`
- Modify: `ARCHITECTURE.md`
- Modify: `TODO.md`
- Verify: `CLAUDE.md` (grep — no changes expected)
- Verify: `AI_GOVERNANCE.md` (grep — no changes expected)
- If ADR-006 exists: verify it's referenced from SECURITY.md + the inventory

- [ ] **Step 11.1: SECURITY.md — add INF-01 close-out line**

Read current `SECURITY.md` to find the audit-log section. Insert a new line under the appropriate heading (likely near line 28 in the executive summary or in a dedicated "## Resolution log" section if one exists). Draft:

```markdown
- **2026-04-17** — `SEC-AUDIT-v1.12.0 INF-01 (CWE-250)` — CLOSED. Terraform CI service principal `luxury-lakehouse-terraform-ci-dev` no longer holds workspace-admins-group membership. Replaced with explicit `databricks_permissions` on SQL warehouse, ingestion job, and Lakebase project + endpoint. Account-admin role [reduced to <X> / accepted as floor per ADR-006]. Commit: <to be filled at commit time>. Regression tests: `src/tests/test_sec4_ci_sp_job_owner.py`.
```

Replace the bracketed alternative with the actual outcome from Task 3.

- [ ] **Step 11.2: ARCHITECTURE.md §6.1 Security — refresh `IAM` row**

Read `ARCHITECTURE.md` at line 755. The current row is:

```markdown
| IAM | Least-privilege; separate service principals per workload |
```

Replace with:

```markdown
| IAM | Least-privilege; separate service principals per workload. Terraform CI SP holds [narrower role `<X>` / `account_admin` accepted per ADR-006]; no workspace-admins-group membership (SEC-AUDIT INF-01 closed 2026-04-17). Explicit `databricks_permissions` on all workspace-scoped objects (SQL warehouse, ingestion job, Lakebase project + endpoint). |
```

- [ ] **Step 11.3: TODO.md — remove SEC4 On-Deck row**

Delete line 34 of `TODO.md` (the SEC4 row). Confirm the row above (PA1) and below (U6) are still in place.

Also update the "Last updated" line at the top of TODO.md (line 5) to:

```markdown
**Last updated**: 2026-04-17 (SEC4 cycle — CI SP workspace-admin-group membership removed; explicit Lakebase ACLs added; orphan PG role `be66af99-...` dropped; account-admin [reduced to <X> / accepted per ADR-006]).
```

- [ ] **Step 11.4: Grep verification — CLAUDE.md**

```bash
grep -n -i 'admins.group\|admins_group\|terraform_ci_admin\|databricks_group_member' CLAUDE.md || echo "no matches"
```

Expected: `no matches`. If matches found, investigate and update.

- [ ] **Step 11.5: Grep verification — AI_GOVERNANCE.md**

```bash
grep -n 'SEC-AUDIT\|INF-01\|terraform_ci_admin\|workspace admin' AI_GOVERNANCE.md | head -20
```

Expected: provenance tag `SEC-AUDIT-v1.12.0 REG-01` is still there (for AI Act governance, not SEC4). No SEC4-specific changes should be required. If INF-01 is referenced as outstanding, update the reference.

- [ ] **Step 11.6: Cross-reference ADR-006 if it exists**

If Task 3.4 produced `docs/superpowers/adrs/ADR-006-account-admin-floor.md`, confirm it is:
- Listed in `SECURITY.md` (step 11.1 above should have inlined the reference).
- Listed in the inventory markdown (Task 1 step 1.6).
- Cross-linked from the `## Related` section in the ADR itself back to the spec + inventory.

- [ ] **Step 11.7: Verify no other repo files reference the removed resources**

```bash
grep -rn 'databricks_group_member.terraform_ci_admin\|data.databricks_group.admins' . \
    --include='*.md' --include='*.py' --include='*.tf' --include='*.yml' --include='*.yaml' \
    | grep -v '.git/' | grep -v 'CHECK-IN' || echo "no matches"
```

Expected: only matches in the SEC4 design/plan docs themselves (which are historical references). No active references elsewhere.

---

## Task 12: Full Verification + Commit Approval Gate

**Goal:** Run the complete pre-commit gate. Present consolidated evidence. Request single-commit approval. Execute the commit if approved.

**Files:**
- No file changes in this task (verification + commit).

- [ ] **Step 12.1: Full test suite**

```bash
uv run pytest src/ -v 2>&1 | tail -30
```

Expected: all green. If any pre-existing failure is present (unrelated to this cycle), capture the list — do not try to fix unrelated failures in this branch.

- [ ] **Step 12.2: Ruff check + format**

```bash
uv run ruff check src/ scripts/
uv run ruff format --check src/ scripts/
```

Expected: zero violations, clean format.

- [ ] **Step 12.3: Pyright type check**

```bash
uv run pyright src/
```

Expected: zero errors.

- [ ] **Step 12.4: Final `terraform plan` — idempotence final check**

```bash
cd terraform/environments/dev
AWS_PROFILE=devops-agent terraform plan -detailed-exitcode -no-color | tail -20
echo "Exit: $?"
```

Expected: exit code 0.

- [ ] **Step 12.5: Git status + diff summary**

```bash
git status --short
git diff --stat main..HEAD
git diff main..HEAD -- terraform/ | head -80
```

Capture output — this goes into the commit approval package.

Expected files changed (reference list — the actual list depends on Task 3/4 outcomes):

| Path | Change type |
|---|---|
| `terraform/modules/service_principals/main.tf` | Modified (admin-group removal, comment rewrite, possibly account_admin reduction) |
| `terraform/environments/dev/main.tf` | Modified (Lakebase ACL additions if primary path) |
| `terraform/modules/lakebase/outputs.tf` | Modified (if new output was needed) |
| `src/tests/test_sec4_ci_sp_job_owner.py` | Modified (4 new tests) |
| `src/tests/test_orphan_pg_role_absent.py` | New |
| `docs/superpowers/specs/2026-04-17-sec4-ci-sp-least-privilege-design.md` | New (from brainstorming) |
| `docs/superpowers/specs/2026-04-17-sec4-workspace-resource-inventory.md` | New |
| `docs/superpowers/plans/2026-04-17-sec4-ci-sp-least-privilege.md` | New (this plan) |
| `docs/superpowers/adrs/ADR-006-account-admin-floor.md` | New (if ADR path chosen in Task 3) |
| `SECURITY.md` | Modified |
| `ARCHITECTURE.md` | Modified |
| `TODO.md` | Modified |

- [ ] **Step 12.6: Draft the commit message**

Prepare the HEREDOC commit message. Template:

```
feat: SEC4 — CI SP least-privilege (INF-01 / CWE-250 closed)

Remove workspace-admin-group membership from the Terraform CI service
principal. Close SEC-AUDIT-v1.12.0 INF-01 / CWE-250.

Changes (workspace-admin elimination):
- Remove `databricks_group_member.terraform_ci_admin` + paired
  `data.databricks_group.admins` from
  terraform/modules/service_principals/main.tf.
- Add explicit `databricks_permissions` on `databricks_postgres_project.soccer_analytics`
  and `databricks_postgres_endpoint.primary` in terraform/environments/dev/main.tf
  (CI SP <LEVEL>; alphabetically sorted access_control blocks).

Changes (account-admin decision — from SEC4 spike):
[Fill in ONE of:]
- OPTION A: Reduce `databricks_service_principal_role.terraform_ci_account_admin`
  to `<narrower_role>` after provider v1.113 added support. See ADR-006 / spike evidence.
[OR]
- OPTION B: Accept `account_admin` as the floor; document per ADR-006
  (docs/superpowers/adrs/ADR-006-account-admin-floor.md) with provider
  source citations at `github.com/databricks/terraform-provider-databricks@v1.113.0`.

Changes (orphan PG role — SEC4 item H):
- Drop role `be66af99-5296-4fd9-887a-c081bce38bfa` from Lakebase
  (pre-existing per ADR-005 §Neutral). REASSIGN OWNED + DROP OWNED + DROP ROLE.
- Add src/tests/test_orphan_pg_role_absent.py regression guard (gated on Lakebase creds).

Changes (regression tests):
- src/tests/test_sec4_ci_sp_job_owner.py: +4 tests
  (test_terraform_ci_admin_group_member_absent,
   test_admins_group_not_referenced_anywhere,
   test_lakebase_project_acl_exists_and_is_correctly_shaped,
   test_lakebase_endpoint_acl_exists_and_is_correctly_shaped).

Changes (documentation):
- docs/superpowers/specs/2026-04-17-sec4-ci-sp-least-privilege-design.md: design spec.
- docs/superpowers/specs/2026-04-17-sec4-workspace-resource-inventory.md: living inventory of all CI SP auth paths.
- docs/superpowers/plans/2026-04-17-sec4-ci-sp-least-privilege.md: implementation plan.
- SECURITY.md: INF-01 closed line added.
- ARCHITECTURE.md §6.1: IAM row refreshed.
- TODO.md: SEC4 On-Deck row removed (per feedback_todo_cleanup_in_commit.md).
- terraform/modules/service_principals/main.tf:54-78 comment block rewritten to document the new floor.

Verification:
- `terraform apply` Phase 1 + Phase 2 against dev workspace, both idempotent (plan -detailed-exitcode = 0).
- `terraform plan -refresh-only` confirms CI SP retains read access post-admin-removal.
- Full pytest suite green; ruff + pyright clean.
- Orphan PG role absent per pg_roles query.

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>
```

Fill in `<LEVEL>` and choose OPTION A or B based on actual Task 3 outcome. Strip the OPTION prefix and the unused branch before writing.

- [ ] **Step 12.7: CHECK-IN #8 with user — request commit approval**

Present to user:
- `git status --short` output.
- `git diff --stat` output.
- Summary of per-task outcomes (Task 3 decision, Task 4 decision, Task 7 apply evidence, Task 10 apply evidence, Task 11 doc updates).
- Draft commit message.
- Explicit request: "Approve single commit + push? If yes, I will run `git add` + `git commit` + `git push`. If you prefer a different commit structure (e.g., split into two), say so."

**DO NOT proceed to Step 12.8 without explicit user approval.**

- [ ] **Step 12.8 (ONLY IF APPROVED): Stage and commit**

Only after the user types a clear commit-authorizing verb (e.g., "commit", "approved, commit"), run:

```bash
# Stage each file explicitly — no `git add -A` or `git add .`, per SEC practice.
git add terraform/modules/service_principals/main.tf \
        terraform/environments/dev/main.tf \
        terraform/modules/lakebase/outputs.tf \
        src/tests/test_sec4_ci_sp_job_owner.py \
        src/tests/test_orphan_pg_role_absent.py \
        docs/superpowers/specs/2026-04-17-sec4-ci-sp-least-privilege-design.md \
        docs/superpowers/specs/2026-04-17-sec4-workspace-resource-inventory.md \
        docs/superpowers/plans/2026-04-17-sec4-ci-sp-least-privilege.md \
        SECURITY.md \
        ARCHITECTURE.md \
        TODO.md

# Add ADR-006 only if Task 3 produced it.
test -f docs/superpowers/adrs/ADR-006-account-admin-floor.md && \
    git add docs/superpowers/adrs/ADR-006-account-admin-floor.md

git commit -m "$(cat <<'EOF'
<full HEREDOC commit message from Step 12.6>
EOF
)"

git status
```

- [ ] **Step 12.9 (separate approval required): Push + PR**

Per `feedback_one_commit_at_a_time.md`, push and PR creation are SEPARATE approvals. Do NOT push automatically after the commit. Request:

"Commit `<sha>` created locally. Push to `origin/feat/sec4-ci-sp-least-privilege` and open a PR?"

Only on explicit approval:

```bash
git push -u origin feat/sec4-ci-sp-least-privilege

gh pr create --title "feat: SEC4 — CI SP least-privilege (INF-01 closed)" --body "$(cat <<'EOF'
## Summary

Close SEC-AUDIT-v1.12.0 INF-01 / CWE-250.

- Remove workspace-admin-group membership from the Terraform CI SP.
- Add explicit `databricks_permissions` on Lakebase project + endpoint.
- [Reduce / accept per ADR-006] `account_admin` role.
- Drop orphan PG role `be66af99-...` from Lakebase (pre-existing per ADR-005).
- Add 5 regression tests + living inventory markdown.

See `docs/superpowers/specs/2026-04-17-sec4-ci-sp-least-privilege-design.md` for design.
See `docs/superpowers/plans/2026-04-17-sec4-ci-sp-least-privilege.md` for implementation.
[See `docs/superpowers/adrs/ADR-006-account-admin-floor.md` for account-admin decision.]

## Test plan

- [x] `uv run pytest src/ -v` — all green
- [x] `uv run ruff check src/ scripts/` + `ruff format --check` — clean
- [x] `uv run pyright src/` — zero errors
- [x] `terraform apply` Phase 1 (Lakebase ACLs + [reduction]) — clean
- [x] `terraform apply` Phase 2 (admin-group removal) — clean
- [x] `terraform plan -detailed-exitcode` — 0 after both applies
- [x] `terraform plan -refresh-only` — CI SP still has read access
- [x] Orphan PG role `be66af99-...` absent from `pg_roles`

🤖 Generated with [Claude Code](https://claude.com/claude-code)
EOF
)"
```

Report the PR URL back to the user.

---

## Rollback plan (per-task — consolidated from spec)

| Task | Failure | Rollback |
|------|---------|----------|
| 4 (probe) | Provider rejects Lakebase `databricks_permissions` | Revert draft; Step 4.7 escalate; cycle pauses |
| 6 (TF edit) | Syntax error | `git checkout -- terraform/` |
| 7 (Phase 1 apply) | Apply fails | `git checkout -- .`; investigate error; CHECK-IN |
| 8 (PG drop) | `DROP ROLE` fails with dependency | Revert no-op; enumerate dependencies via `\du`; CHECK-IN |
| 9 (core edit) | Tests fail after edit | Review diff; re-run `ruff format`; re-test |
| 10 (Phase 2 apply) | Apply fails | Revert TF edits; re-apply; CHECK-IN |
| 10 (refresh-only) | CI SP loses access | Re-add `databricks_group_member.terraform_ci_admin` + data source; `terraform apply`; CHECK-IN |
| 12 (commit) | User rejects | Hold; do not proceed |

---

## Check-in summary

| # | Task | Trigger |
|---|------|---------|
| 1 | 1 (G) | End of audit — inventory markdown + surprises |
| 2a | 2 | After CHANGELOG + source grep |
| 2b | 2 | After plan experiment + narrower-role search |
| 3 | 3 (B decision) | TF diff OR ADR-006 draft |
| 4 | 4-6 (A) | Provider probe + drafted blocks + plan (not applied) |
| 5 | 7 (Phase 1 apply) | Apply evidence + idempotent re-plan |
| 6 | 8 (H) | Pre-state + execution + post-state test |
| 7 | 10 (Phase 2 apply) | Apply evidence + refresh-only OK |
| 8 | 12 (commit) | Full verification + draft commit message |

Plus separate approval for push + PR creation at Step 12.9.
