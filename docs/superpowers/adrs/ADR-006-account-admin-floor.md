# ADR-006: Terraform CI SP `account_admin` Role Accepted as the Floor

| Field | Value |
|---|---|
| **Date** | 2026-04-17 |
| **Status** | Accepted |
| **Deciders** | Karsten Nielsen |

## Context

The Terraform CI service principal `luxury-lakehouse-terraform-ci-dev` (application_id
`521f5d6a-cfd4-4fe1-a5cb-d5b12e247276`, internal id `74583001200167`) runs `terraform
plan` and `terraform apply` against the dev workspace from GitHub Actions via OIDC
federation. Before the SEC4 cycle (2026-04-17), it held two wide privileges:

1. **Workspace admin** — via membership in the workspace `admins` group
   (`terraform/modules/service_principals/main.tf:84-87` before this cycle).
2. **Account admin** — via `databricks_service_principal_role.terraform_ci_account_admin`
   (`terraform/modules/service_principals/main.tf:91-95`).

SEC-AUDIT-v1.12.0 finding INF-01 (CWE-250 — Execution with Unnecessary Privileges)
flagged this as a least-privilege violation. The SEC4 cycle eliminates the
workspace-admin-group membership (see the cycle's design doc at
`docs/superpowers/specs/2026-04-17-sec4-ci-sp-least-privilege-design.md`) and
asks whether `account_admin` can also be reduced or must be accepted.

The CI SP manages 11 account-scoped Terraform resources (enumerated in
`docs/superpowers/specs/2026-04-17-sec4-workspace-resource-inventory.md`):

- 3× `databricks_service_principal` (ingestion, terraform_ci, hf_app)
- 1× `databricks_service_principal_role` (itself)
- 1× `databricks_service_principal_federation_policy` (github_actions OIDC)
- 1× `databricks_access_control_rule_set` (ingestion_sp_user_role)
- 1× `databricks_group` (dbt_owners)
- 3× `databricks_group_member` (dbt_owners_{deployer, ingestion_sp, terraform_ci_sp})
- 1× `databricks_mws_permission_assignment` (dbt_owners_workspace)

During the SEC4 cycle a spike investigated whether a narrower role can replace
`account_admin`. The investigation combined provider-source greps, provider-docs
review, and live empirical probes in which the `account_admin` role was
temporarily removed from the CI SP via SCIM PATCH and each representative
operation was attempted as the CI SP via a short-lived OAuth M2M client secret.

## Decision

Accept `account_admin` as the floor for the Terraform CI service principal.

`databricks_group_member.terraform_ci_admin` (workspace admins-group membership)
IS removed in the SEC4 cycle. Explicit `databricks_permissions` on every
workspace-scoped resource the CI SP manages (SQL warehouse, daily ingestion
job, Lakebase project) replaces the historical transitive authorization.

`databricks_service_principal_role.terraform_ci_account_admin` remains in place.
A future reduction is feasible if Databricks introduces create-scope narrower
account-level roles or if the repository refactors account-scoped resource
creation out of Terraform (see Alternatives).

## Alternatives considered

### A. Reduce to a narrower named role via `databricks_service_principal_role`

| Attribute | Value |
|---|---|
| **Pros** | Smallest code change — just edit the role string |
| **Cons** | No narrower named role exists in the Databricks account IAM model as of provider v1.113.0 / Go SDK v0.127.0 |
| **Why rejected** | Provider docs `docs/resources/service_principal_role.md:40-42` document only `account_admin` as the named role valid for `service_principal_role`. All other role values accepted by that resource are AWS IAM instance-profile ARNs, not Databricks IAM roles. Empirical confirmation: `databricks account users get 76366708804151` shows `roles: [{"type":"direct","value":"account_admin"}]` is the only non-instance-profile role on the account admin user. |

### B. Replace `account_admin` with rule-set-delegated roles (granular delegation)

| Attribute | Value |
|---|---|
| **Pros** | Rule-sets support granular roles: `roles/group.manager`, `roles/servicePrincipal.manager`, `roles/servicePrincipal.user`, `roles/marketplace.admin`, `roles/budgetPolicy.manager`, etc. These could in principle delegate management of specific pre-existing resources. |
| **Cons** | Rule-set roles delegate management of **existing** objects only. They cannot substitute for create authority on new account-level parents. Our Terraform creates 11 account-scoped resources from scratch; each creation requires `account_admin` as the bootstrap permission. |
| **Why rejected** | (1) Provider docs `docs/resources/access_control_rule_set.md:13` state the rule-set resource manages "access rules on specific object resources (service principal, group, budget policies and account)" — i.e., it grants roles on objects, not the right to create them. (2) Empirical: `databricks account groups create --display-name "sec4-probe-..."` as CI SP without `account_admin` returned HTTP error (ReqId `525df54e-8f50-41ca-b5d0-38c3068cd55d`): *"This API is disabled for users without account admin status."* Identical response for `databricks account service-principals create` (implicit, same error class) and `databricks account service-principal-federation-policy create` (ReqId `44b74a79-d542-4942-b7c0-f075b798de24`). |

### C. Move account-scoped resource creation out of Terraform; keep TF for ongoing management only

| Attribute | Value |
|---|---|
| **Pros** | Would eliminate `account_admin` from the CI SP entirely. Account-scoped resources are long-lived and rarely change, so bootstrap-then-delegate is feasible architecturally. Delegation via rule-sets (alternative B's granular roles) works for management of pre-existing resources. |
| **Cons** | (1) Large refactor: 11 resources move from `terraform apply` to a one-off bootstrap runbook. (2) Breaks the "everything in TF" invariant — future account-scoped changes require a human account admin outside the GitOps flow. (3) Significant scope expansion well beyond SEC4's remit. (4) Terraform idempotent-apply guarantees degrade: the authoritative source of truth for account-scoped resources becomes the runbook, not the repo. |
| **Why rejected** | Exceeds SEC4's scope as specified in the cycle's design doc (`docs/superpowers/specs/2026-04-17-sec4-ci-sp-least-privilege-design.md`, §Goals + §Non-goals). The design explicitly scopes SEC4 to either shipping the `account_admin` reduction IF feasible within current Terraform structure, OR accepting the floor via this ADR. Option C remains on the table for a future cycle if the platform acquires stricter compliance requirements. |

### D. Accept `account_admin` as the floor (**chosen**)

| Attribute | Value |
|---|---|
| **Pros** | Preserves the "everything in TF" invariant. No runtime behavior change. Risk posture is the same as before SEC4 for account-scoped operations (which were already account_admin-gated transitively via the admins group). |
| **Cons** | The CI SP retains one broad privilege. Any compromise of the CI SP's OAuth-federation trust would give the attacker account-level admin on the Databricks account. |
| **Why chosen** | (1) No narrower role achieves the same coverage (per A, B above with citations). (2) Option C is out of scope for this cycle. (3) The OIDC federation auth path (no persistent secret; GitHub repo pinned to `repository` claim; tokens per-run) materially reduces the compromise blast radius compared with a static client secret. (4) Removing workspace-admin-group membership (the other half of SEC4) already closes the dominant attack surface — the `admins` group confers broad workspace-level privileges that are independently valuable to an attacker; `account_admin` on an OIDC-federated CI SP has narrower practical compromise paths. |

## Consequences

### Positive

- **Workspace-admin-group membership eliminated** for the CI SP via SEC4's Phase 2.
- **Lakebase project + endpoint, SQL warehouse, daily job** each have explicit `databricks_permissions` / `databricks_grant` — no transitive-admin auth anywhere except the documented `account_admin` floor.
- **Audit posture strengthened**: the CI SP's privilege surface is now explicitly documented in `docs/superpowers/specs/2026-04-17-sec4-workspace-resource-inventory.md` and guarded by regression tests in `src/tests/test_sec4_ci_sp_job_owner.py`.
- **Clear forward path**: if Databricks introduces create-scope narrower account-level roles (e.g., `account_group_admin`, `account_sp_admin`), the reduction path is the simple role-string edit already enabled by `databricks_service_principal_role`.

### Negative

- The CI SP still holds `account_admin`. Compromise of its OAuth federation trust (GitHub repo takeover, OIDC issuer misconfiguration, policy regression) would give the attacker account-level admin.
- Mitigations in place: OIDC federation (no persistent secret), subject-claim pinning to `repository = "karsten-s-nielsen/luxury-lakehouse"` (`terraform/modules/service_principals/main.tf:107-111`), no client-secret rotation required.
- Residual mitigation candidates for a future ADR: (1) split CI SP identity — one for account-scoped resources (used rarely), one for workspace-scoped resources (used per PR); (2) add post-apply regression tests that assert unexpected account-level state changes.

### Neutral

- The provider's `api` field introduced in v1.113.0 (`terraform-provider-databricks` CHANGELOG) allows explicit routing of dual account/workspace resources but does NOT change which role is required at either level.
- The Lakebase permissions surface discovered in SEC4's Task 1 (`databricks_permissions` supports `database_project_name` as of v1.108.0) has no account-level equivalent — Lakebase ACLs are strictly workspace-scoped, confirmed by the audit.

## CLAUDE.md Amendment

None. `CLAUDE.md` does not document specific role requirements for the CI SP,
so accepting this floor does not carve out an exception to a documented rule.

## Related

- **Commits:** filled at commit time — this ADR is part of the SEC4 cycle commit.
- **Specs:** `docs/superpowers/specs/2026-04-17-sec4-ci-sp-least-privilege-design.md`, `docs/superpowers/specs/2026-04-17-sec4-workspace-resource-inventory.md`.
- **Plans:** `docs/superpowers/plans/2026-04-17-sec4-ci-sp-least-privilege.md`.
- **Audit finding:** `SEC-AUDIT-v1.12.0 INF-01 (CWE-250)`.
- **ADRs:** none supersede or are superseded.
- **External references:**
  - Databricks Terraform provider source: `github.com/databricks/terraform-provider-databricks@v1.113.0`.
    - `internal/providers/pluginfw/products/service_principal_federation_policy/resource_service_principal_federation_policy.go:237-244` (AccountClient gating).
    - `mws/resource_mws_permission_assignment.go:47-84` (AccountClient gating).
    - `permissions/resource_access_control_rule_set.go:32,46` (conditional routing — account-scoped rule-set names require account client).
    - `aws/resource_service_principal_role.go:14-57` (SCIM PATCH implementation — role value is arbitrary data, not hardcoded).
  - Provider docs: `docs/resources/service_principal_role.md:40`, `docs/resources/access_control_rule_set.md:13,147-170`.
  - Databricks Go SDK: `github.com/databricks/databricks-sdk-go@v0.127.0` (per v1.113 CHANGELOG — SDK surface fixed at investigation time).

## Notes

### Empirical probe log (2026-04-17, SEC4 cycle Task 2)

A short-lived OAuth M2M client secret was created on the CI SP. The
`account_admin` role was removed via SCIM PATCH:

```json
{
  "schemas": ["urn:ietf:params:scim:api:messages:2.0:PatchOp"],
  "Operations": [{"op": "remove", "path": "roles[value eq \"account_admin\"]"}]
}
```

Four operations were attempted as the CI SP (via `DATABRICKS_AUTH_TYPE=oauth-m2m`):

| Operation | Request ID | Result |
|---|---|---|
| `databricks account users list --filter 'userName eq "karstenskyt@gmail.com"'` | `9345b222-0637-4b21-9943-4a9e5d8398c0` | HTTP 403 — "This API is disabled for users without account admin status" |
| `databricks account groups create --display-name "sec4-probe-delete-me-xyz"` | `525df54e-8f50-41ca-b5d0-38c3068cd55d` | HTTP 403 — same error |
| `databricks account workspace-assignment list 3728070797005055` | `529f562d-ec85-4973-9dbd-ebe403e299d1` | HTTP 403 — same error |
| `databricks account service-principal-federation-policy create 74583001200167 ...` | `44b74a79-d542-4942-b7c0-f075b798de24` | HTTP 403 — same error |

A baseline run WITH `account_admin` succeeded for each probe. The
`databricks account service-principal-federation-policy list` (read) variant
succeeded in both cases, suggesting SPs can read their own federation policies
without account_admin; create/update/delete still require it.

After probes completed, the role was restored via SCIM PATCH:

```json
{
  "schemas": ["urn:ietf:params:scim:api:messages:2.0:PatchOp"],
  "Operations": [{"op": "add", "path": "roles", "value": [{"value": "account_admin"}]}]
}
```

The temporary client secret was deleted
(`databricks account service-principal-secrets delete 74583001200167 <secret_id>`),
and `service-principal-secrets list` confirms zero remaining secrets on the CI SP.
Total disruption window: ~3 minutes. No CI runs occurred during the window.

### Forward-looking refactor sketch (not in scope for this ADR)

Should the project decide to eliminate `account_admin` in a future cycle, the
refactor path is:

1. Bootstrap all 11 account-scoped resources via a one-time runbook executed as
   a human account admin (me). Resources become durable, unowned-by-Terraform.
2. Add rule-set delegations granting the CI SP `servicePrincipal.manager` on
   each managed SP, `group.manager` on `dbt_owners`, etc.
3. Import the resources into Terraform state as managed-but-not-created.
4. Remove `databricks_service_principal_role.terraform_ci_account_admin`.

This sketch is not normative — it is a future-maintainer reference, not a plan.
