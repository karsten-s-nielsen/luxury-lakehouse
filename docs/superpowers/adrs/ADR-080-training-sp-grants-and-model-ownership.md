# ADR-080: Training-SP grants + model ownership for M2M training delivery

| Field | Value |
|---|---|
| **Date** | 2026-08-23 |
| **Status** | Accepted |
| **Deciders** | Karsten Nielsen |

## Context

[ADR-079](ADR-079-hf-jobs-m2m-oauth-training-auth.md) moved HF-Jobs training delivery from
a user PAT to **M2M OAuth as the ingestion service principal** (`luxury-lakehouse-ingestion-dev`).
The auth mechanism works, but the first M2M run failed at `mlflow.start_run` with
`PERMISSION_DENIED: User '008b207b…' does not have permission to 'Edit' experiment
/soccer_analytics/vaep_model`. The SP could authenticate and write UC Volume, but it lacked
the **MLflow-experiment and UC-model write** permissions — because training historically ran
as a *user*, so the SP was never granted them.

Two distinct gaps:

1. **Experiment `CAN_EDIT`** — creating an MLflow run needs `CAN_EDIT` on the backing workspace
   experiment. The SP had only `CAN_READ` (for inference artifact-hash checks, ADR-012 §2).
2. **UC registered-model write** — registering a new model version + setting `@Champion`
   requires **ownership** of the UC registered model (UC has no grantable "write model version"
   privilege short of ownership). The models were owned by `karstenskyt@gmail.com`; the SP had
   only `EXECUTE`.

The forcing constraint on the fix: `databricks_permissions` is **authoritative** — the next
`terraform apply` resets each experiment's ACL to exactly what Terraform declares, so an
ad-hoc `CAN_EDIT` grant applied via the API is **reverted** on the next apply. Grants applied
outside Terraform silently drift or disappear.

## Decision

Grant the training-write permissions to the **`dbt-owners-<env>` group** (the ingestion SP is a
member), codified so they survive `terraform apply` and reproduce on a fresh install:

- **Experiment `CAN_EDIT`** for `dbt-owners` on the training experiments (`vaep_model`,
  `scoutgpt`, `xg_model_v3`) — declared in `terraform/environments/dev/main.tf`
  `databricks_permissions.*_experiment_acl`.
- **Schema `CREATE_MODEL`** for `dbt-owners` on `dev_gold` — declared in the
  `terraform/modules/catalog` `dbt_owners_gold_select` grant.
- **UC model ownership → `dbt-owners`** — normalized by the idempotent
  `scripts/normalize_training_model_ownership.py` (ownership is not a Terraform-managed
  attribute for MLflow-created models).

## Alternatives considered

| Option | Pros | Cons | Why rejected |
|---|---|---|---|
| A. Grant the ingestion SP directly (not a group) | Explicit 1:1 identity | Every model owner is a machine principal; humans need a separate grant to inspect/manage; more per-SP churn if the training identity changes | Group keeps humans + SP as co-owners and is the existing grant convention |
| B. Keep the ad-hoc API grants | Zero new code | `databricks_permissions` reverts experiment `CAN_EDIT` on the next apply; no fresh-install path | Not durable — the whole point of the question |
| C. Import every registered model into a `databricks_registered_model` TF resource to manage ownership | Ownership in TF | TF then fights the trainer over the model object (versions, tags) on every run | Wrong tool — models are training artifacts, not infra |
| D. Run training as a user again | No grants needed | Reintroduces the PAT/expiry problem ADR-079 removed | Regresses the migration |
| E. **Group grants in TF + ownership-normalization script (chosen)** | Durable, fresh-install-correct, humans + SP co-own | Model ownership needs one non-TF step (the script) | — |

## Consequences

### Positive

- The M2M training delivery works end-to-end and stays working across `terraform apply` (proven:
  VAEP registered `vaep_model` v13 + `@Champion` verified after the grants landed).
- A fresh install reproduces the setup from Terraform (`CAN_EDIT` + `CREATE_MODEL`) plus one
  documented normalization step — no hand-applied ACLs.
- Humans (in `dbt-owners`) and the SP co-own the training experiments' write path and the models.

### Negative

- Model ownership is normalized by a script, not Terraform, so it is a **run-after-deploy** step
  (and after adding any new training model). Mitigated: the script is idempotent and has a
  `--check` mode; the model list is a single constant kept in sync with the TF experiment ACLs.
- The `dbt-owners` group now carries `CREATE_MODEL` + experiment `CAN_EDIT` — a broader role than
  its original "dbt schema read" purpose. This is deliberate (it is the training-write group) and
  documented here.

### Neutral

- The `xg_v2` experiment stays read-only in TF (retired model, ADR-066) — no training grant.
- Experiment *ownership* is left with the creator (inherited `IS_OWNER`); only the ACL is managed,
  which is sufficient to create runs.

## CLAUDE.md Amendment

None. This extends the existing UC-grants + `databricks_permissions` conventions; no project-wide
rule is waived.

## Related

- **ADRs:** completes [ADR-079](ADR-079-hf-jobs-m2m-oauth-training-auth.md) (M2M auth) with the
  grants/ownership it requires; builds on ADR-012 (training delivery).
- **Terraform:** `terraform/environments/dev/main.tf` (`*_experiment_acl`),
  `terraform/modules/catalog/main.tf` (`dbt_owners_gold_select` → `CREATE_MODEL`).
- **Scripts:** `scripts/normalize_training_model_ownership.py`.
- **Tests:** `src/tests/test_training_grants_conformance.py`.

## Notes

Applied to dev on 2026-08-23 (ad-hoc API, now codified): `dbt-owners-dev` granted experiment
`CAN_EDIT` on vaep_model/scoutgpt/xg_model_v3 and set as owner of the three registered models;
schema `CREATE_MODEL` pending this PR's apply. Experiment IDs for the TF import blocks:
vaep_model `1644169474913777`, scoutgpt `451147925512376`, xg_model_v3 `1557416745207844`.
