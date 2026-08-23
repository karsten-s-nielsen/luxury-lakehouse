# ADR-079: M2M OAuth for HF-Jobs training delivery (finish the PAT→OAuth migration)

| Field | Value |
|---|---|
| **Date** | 2026-08-23 |
| **Status** | Accepted |
| **Deciders** | Karsten Nielsen |

## Context

The ADR-012 training-to-production delivery path authenticates to Databricks (MLflow
registry + UC-Volume weight upload) from inside HF-Jobs training runs. It required a
static `DATABRICKS_TOKEN`, passed at launch via `--secrets` — its own
`require_mlflow_env` error message still read *"DATABRICKS_TOKEN is a PAT from
Workspace > Settings > Developer."* That path was written for long-lived PATs.

The project migrated to **OAuth-only, no PATs** (settled 2026-08-12; see
[[reference_databricks_session_auth_oauth]]). An OAuth **access token** lives ~60 min
(measured: `exp - iat = 60.0 min`), and the value a caller actually holds is the
*cached* CLI token with variable remaining life (observed 21.7 min). A static token
handed to a job is a snapshot that **cannot refresh**.

The forcing function is the first full retrain cycle since that migration. The trainer
timeouts are **VAEP `cpu-xl` 60 min, xtgk-v2 `cpu-basic` 60 min, xG-v3 `l40sx1` GPU
90 min, ScoutGPT `l40sx1` GPU 180 min**. The ADR-012 MLflow-log + UC-Volume upload run
at job *end*, so any run longer than the token's TTL — every GPU trainer — loses its
credential mid-flight and fails delivery. No static token, however freshly minted, can
cover a 90- or 180-minute job. This is the last un-migrated PAT assumption in the stack.

## Decision

`require_mlflow_env` accepts **M2M OAuth service-principal credentials**
(`DATABRICKS_CLIENT_ID` + `DATABRICKS_CLIENT_SECRET`) as an alternative to
`DATABRICKS_TOKEN`. HF-Jobs trainers pass the `luxury-lakehouse-ingestion-dev` SP's
client-id/secret via `--secrets`; the databricks-sdk (and MLflow-on-Databricks, and
`ingestion.databricks_auth.workspace_client()`) mint and **auto-refresh** tokens from
them on demand, so the credential survives an arbitrarily long job.

## Alternatives considered

| Option | Pros | Cons | Why rejected |
|---|---|---|---|
| A. Static minted OAuth token per job | No code change | Access token ≤60 min (often ≪); cannot cover a 90/180-min job | Structurally impossible for the GPU trainers |
| B. Pass the operator's U2M refresh token into the job | Auto-refreshes | Ships a durable *personal* credential to third-party (HF) infra; SDK U2M reads a cache file, not env | Security smell; not best-practice |
| C. Split delivery — job trains, local step registers via operator OAuth | No cred on HF infra | xG-v3/xtgk read *live* Databricks during training, so the job needs Databricks auth regardless; breaks ADR-012 in-job atomicity | Doesn't remove the in-job auth need; larger change |
| D. Re-create a PAT for training | Trivially long-lived | Violates the no-PAT policy (settled 2026-08-12) | Reintroduces the thing we removed |
| E. **M2M OAuth SP creds (chosen)** | Auto-refresh, no ceiling; SDK/MLflow/workspace_client already honor it; SP already has the grants | Trainer job holds a privileged credential (mitigated: workspace-scoped secret, minted per phase, deleted after) | — |

The gate was the only blocker: the SDK's unified auth, MLflow ≥2.17
(`mlflow.set_tracking_uri("databricks")`), and `workspace_client()` (a bare
`WorkspaceClient()` that re-mints per request) all already resolve M2M from env. Verified
empirically before the change: an M2M-only env (no `DATABRICKS_TOKEN`) authenticated as
the ingestion SP, listed the `model_weights` UC Volume, and reached the MLflow registry
(`search_experiments` → `/soccer_analytics/vaep_model`).

## Consequences

### Positive

- HF-Jobs training runs of **any duration** authenticate durably — a 180-min ScoutGPT
  fit no longer risks losing its credential before ADR-012 delivery.
- Completes the PAT→OAuth migration for the training path; removes the last "PAT" from
  the codebase's auth assumptions (the stale `require_mlflow_env` message is corrected).
- `DATABRICKS_TOKEN` still works unchanged (back-compat), so CI paths and any static-token
  caller are unaffected.

### Negative

- The trainer job now holds a privileged SP credential. Mitigated: the secret is
  **workspace-scoped**, minted for the retrain phase, passed only as encrypted per-job
  `--secrets`, and **deleted after the phase** (`service_principal_secrets_proxy.delete`).
- Operators must now source SP creds (via the workspace `service_principal_secrets_proxy`,
  which a workspace admin can mint) rather than reading a stored PAT.

### Neutral

- The SK3-MIG retrain orchestrator `scripts/sk3_mig_b_retrain.py` still requires and
  forwards `DATABRICKS_TOKEN`. It is not on the OAuth-critical path for the Part-B
  retrains (which launch trainers directly via `hf jobs uv run`). Migrating it to M2M is
  a follow-up, not a blocker.
- The football2vec trainers authenticate a SQL-warehouse connection with
  `DATABRICKS_TOKEN` (a different mechanism); not retrained this cycle, unchanged here.

## Related

- **ADRs:** builds on ADR-012 (training-to-production delivery); complements the dbt
  local-OAuth completion in the same PR.
- **Issues / PRs:** the "PAT→OAuth completion" PR (also carries `dbt_project/profiles.yml`
  dev/prod → `auth_type: oauth` + `client_id: databricks-cli`).
- **External references:** Databricks SDK unified auth (M2M `oauth-m2m`); MLflow ≥2.17
  Databricks auth via databricks-sdk.

## Notes

Empirical verification (2026-08-23, `luxury-lakehouse-ingestion-dev`, app-id
`008b207b-…`): minted a workspace-scoped OAuth secret via
`w.service_principal_secrets_proxy.create(service_principal_id=77407294662421)`;
M2M `Config(auth_type="oauth-m2m")` resolved a bearer token and `current_user.me()`
returned the SP; UC Volume `/Volumes/soccer_analytics/dev_gold/model_weights/` listed
(`vaep_cache`, `xg_model_v3`, …); MLflow `search_experiments()` succeeded with no
`DATABRICKS_TOKEN`.
