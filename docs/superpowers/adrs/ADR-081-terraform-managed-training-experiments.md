# ADR-081: Terraform-managed MLflow training experiments (create, not look up)

| Field | Value |
|---|---|
| **Date** | 2026-08-24 |
| **Status** | Accepted |
| **Deciders** | Karsten Nielsen |

## Context

[ADR-080](ADR-080-training-sp-grants-and-model-ownership.md) codified the training-SP grants so
M2M training delivery survives `terraform apply`. It granted `dbt-owners` `CAN_EDIT` on each
training experiment via `databricks_permissions.*_experiment_acl`, and the ACL's
`experiment_id` came from a **data lookup**:

```hcl
data "databricks_mlflow_experiment" "scoutgpt" { name = "/soccer_analytics/scoutgpt" }
```

A Terraform **data source resolves an object that already exists** — it errors when the object
does not. That is fine on the current dev workspace, whose experiments were created out-of-band
by earlier user-run training. It is **not** fresh-install-correct, and it breaks entirely for a
model that has never been trained:

- **Fresh install:** on a new workspace the four experiments do not exist, so all four
  `data.databricks_mlflow_experiment` lookups **error during plan** — `terraform apply` cannot
  run at all. ADR-080's "a fresh install reproduces the setup from Terraform" claim did not hold
  for experiment *creation*; only the ACL grant was codified, not the thing it grants on.
- **A never-trained model (xt_gk_v2):** its first M2M run failed at
  `mlflow.set_experiment("/soccer_analytics/xt_gk_v2")` with
  `PERMISSION_DENIED: … does not have create permission for tree node … /workspace/…`. MLflow's
  `set_experiment` **creates** the experiment when it is absent, and the ingestion SP
  (`luxury-lakehouse-ingestion-dev`) is **not a workspace admin**, so it cannot. The old
  conformance test deferred xt_gk_v2's ACL "until the experiment exists (ADR-080)" — but nothing
  ever creates it: the SP can't, and a `data` lookup can't. A chicken-and-egg with no exit.

The forcing question was the user's: *does the grant fix reproduce for the next person installing
the lakehouse?* With `data` lookups, no.

## Decision

Make **Terraform own experiment creation**: convert the four `data.databricks_mlflow_experiment`
lookups to `databricks_mlflow_experiment` **resources**, and add a fifth for xt_gk_v2. Terraform
runs as `luxury-lakehouse-terraform-ci-dev`, which **is a member of the `admins` group** (workspace
admin) and can create experiments, so `terraform apply` creates each experiment and the ACL always
has a target.

- **Five resources** (`vaep`, `xg_v2`, `scoutgpt`, `xg_v3`, `xt_gk_v2`) in
  `terraform/environments/dev/main.tf`. Each `*_experiment_acl` references
  `databricks_mlflow_experiment.<name>.id` instead of the data source.
- **`lifecycle { prevent_destroy = true }`** on every experiment — a hard backstop so a
  force-replace (e.g. an unexpected `artifact_location` diff) errors instead of deleting an
  experiment's run history.
- **Adoption of dev's four existing experiments** via config-driven `import {}` blocks (their
  workspace-specific IDs, e.g. `scoutgpt = 451147925512376`). This is why a single `apply` on dev
  imports-not-creates them — a plain `apply` without the imports would try to CREATE them and fail
  on name-conflict. A genuinely fresh workspace has nothing to import: delete the four import
  blocks and the resources create all experiments from scratch.
- xt_gk_v2 has **no import block** — it does not exist, so Terraform creates it. That single
  create is the whole fix.

### Why this is safe where ADR-080 Alternative C was not

ADR-080 rejected importing registered **models** into Terraform because TF would then fight the
trainer over model **versions and tags** on every run. Experiments are different in kind: TF
manages only the experiment **container** (`name` / `artifact_location`), while the trainer creates
**runs inside** it — which TF never reads or writes. There is no attribute for the two to contend
over, so the objection does not transfer.

### Verification (before building)

The mechanism was confirmed against the live, working sibling rather than by reasoning:

- The TF-apply identity is a **workspace admin** → it can create experiments (Risk 1 closed).
- `/soccer_analytics/scoutgpt` already carries the **identical ACL** this change produces
  (`dbt-owners-dev → CAN_EDIT`, `SP → CAN_READ`), the M2M SP is a **member of `dbt-owners-dev`**,
  and that SP **logged to scoutgpt in this same session** (the ScoutGPT retrain). The only thing
  xt_gk_v2 lacked was the experiment itself (Risk 2 closed).

## Alternatives considered

| Option | Pros | Cons | Why rejected |
|---|---|---|---|
| A. Create the experiment by hand (admin) + keep the data-lookup ACL | Unblocks xt_gk_v2 now | Reproduces the un-codified manual bootstrap; a fresh install still errors on the four data lookups | Does not answer "reproduces for the next installer" — the whole question |
| B. Grant the ingestion SP folder-level create on `/soccer_analytics/` | SP self-bootstraps its experiment on first run | Broadens a machine principal's workspace-tree rights; the per-model data-lookup ACLs still error on a fresh apply (must be removed too) | More privilege, less explicit; the data-lookup fresh-install break is unsolved |
| C. `databricks_mlflow_experiment` resources + import (chosen) | Fresh `apply` creates every experiment; run history adopted, not recreated; explicit and reviewable | One-time import blocks are dev-specific; `prevent_destroy` blocks a deliberate future delete | — |

## Consequences

### Positive

- A fresh `terraform apply` now **creates all five training experiments**, so the ADR-080 grants
  finally have targets on a new workspace — the fresh-install correctness that was claimed but not
  delivered.
- xt_gk_v2 gets its experiment from Terraform, with the same ACL scoutgpt already runs — no manual
  create, no out-of-band grant.
- Experiment run history is protected two ways: `import` (adopt, don't recreate) and
  `prevent_destroy` (a force-replace errors instead of deleting).

### Negative

- The four `import {}` blocks carry **dev-workspace-specific IDs** and must be removed on a truly
  fresh workspace (documented inline in `main.tf` and here). They are transitional adoption
  scaffolding, not permanent config.
- `prevent_destroy` means a future *intentional* experiment removal requires temporarily lifting
  the lifecycle guard — deliberate friction, accepted for run-history safety.
- Converting a `data` source to a `resource` + import must be plan-reviewed: **`terraform plan`
  must show the four as imports with ZERO destroy/recreate, plus one create (xt_gk_v2) and five
  ACLs**, before apply. `artifact_location` is pinned to each experiment's current value to avoid a
  force-replace diff; `prevent_destroy` is the backstop if a diff appears anyway.

### Neutral

- xg_v2 stays read-only (retired, ADR-066) but becomes a managed resource for consistency — a
  fresh apply would otherwise still error on its data lookup.
- Experiment *ownership* remains with the creator (inherited `IS_OWNER`); only the ACL is managed,
  which is sufficient to create runs (unchanged from ADR-080).

## CLAUDE.md Amendment

The ADR-080 bullet in the "Training-to-production delivery contract" section is amended: adding a
new training model now requires its **experiment resource** (`databricks_mlflow_experiment`) in
addition to the experiment ACL, the `TRAINING_MODELS` entry, and the
`test_training_grants_conformance.py` case.

## Related

- **ADRs:** completes [ADR-080](ADR-080-training-sp-grants-and-model-ownership.md) — the grants
  were codified there but the experiments they grant on were only *looked up*, not created; this
  makes creation Terraform-owned. Builds on [ADR-079](ADR-079-hf-jobs-m2m-oauth-training-auth.md)
  (M2M auth).
- **Terraform:** `terraform/environments/dev/main.tf` (`databricks_mlflow_experiment.*` resources +
  `import` blocks + `*_experiment_acl`).
- **Tests:** `src/tests/test_training_grants_conformance.py`
  (`test_training_experiments_are_terraform_managed_resources`).

## Notes

Experiment IDs for the dev import blocks: `vaep_model 1644169474913777`,
`xg_model_v2 1644169474913776`, `scoutgpt 451147925512376`, `xg_model_v3 1557416745207844`
(consumed from ADR-080's Notes, which anticipated this migration). xt_gk_v2 is created fresh by the
apply. Apply choreography: merge → CI `terraform apply` imports the four + creates xt_gk_v2 + sets
the five ACLs (plan reviewed for zero destroy/recreate) → the xt_gk_v2 M2M fit re-dispatch then
finds its experiment and registers `@Champion`.
