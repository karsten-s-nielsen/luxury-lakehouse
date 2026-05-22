# ADR-005: Lakebase synced-table grants must be applied explicitly (no auto-inherit)

| Field | Value |
|---|---|
| **Date** | 2026-04-17 |
| **Status** | Accepted |
| **Deciders** | Karsten Nielsen |

## Context

PR #134 merged on 2026-04-17 added three new dbt marts (`fct_heatmap_agg`, `fct_vaep_breakdown_agg`, `fct_gk_actions_detail`) and their Lakebase synced tables. After deploy the staging Taipy app returned "Something went wrong loading the breakdown" on three pages and the verifier probe found that **no service principal had `SELECT` on any of the three new synced tables** — the Taipy app SP (`hf_app_v2`, `1a1dbf08-...`) was locked out.

The project's hardening guidance at the time (CLAUDE.md "Lakebase grants survive synced table recreation") claimed that

> `ALTER DEFAULT PRIVILEGES FOR ROLE databricks_superuser IN SCHEMA dev_gold GRANT SELECT ON TABLES TO <sp_uuid>`

would auto-grant future synced tables because they were "owned by `databricks_superuser`." This claim turned out to be structurally false. Verified against live Lakebase (session 2026-04-17):

| Evidence | Query | Result |
|---|---|---|
| Synced-table owner | `SELECT pg_get_userbyid(relowner) FROM pg_class WHERE relkind='p' AND relnamespace=(SELECT oid FROM pg_namespace WHERE nspname='dev_gold')` | **`databricks_writer_16401`** (all 38 tables) |
| Synced-table kind | Same query, column `relkind` | `'p'` (partitioned table), not `'r'` |
| `pg_default_acl` rules | `SELECT pg_get_userbyid(defaclrole), defaclacl FROM pg_default_acl WHERE defaclnamespace=(SELECT oid FROM pg_namespace WHERE nspname='dev_gold')` | Two rules, both scoped `FOR ROLE databricks_superuser` or `FOR ROLE karstenskyt@gmail.com`. **Neither matches the actual owner** (`databricks_writer_16401`). |
| My role memberships | `SELECT rolname FROM pg_auth_members JOIN pg_roles ON ...` | `databricks_superuser`, one SP alias. **Not `databricks_writer_16401`.** Postgres requires membership to run `ALTER DEFAULT PRIVILEGES FOR ROLE <target>`. |

In short: synced tables are created by an internal Lakebase role we cannot target with `ALTER DEFAULT PRIVILEGES`, and the two rules we do have never fire for synced-table creation. The older synced tables that *do* have SP grants received them via historical, manually invoked `GRANT SELECT ON ALL TABLES IN SCHEMA …` runs — not via any auto-inherit mechanism. The same investigation found **three additional tables (`fct_player_embeddings_career_synced`, `fct_player_embeddings_season_synced`, `fct_pausa_values_synced`) that had silently drifted to zero SP grants** — the Player Similarity and PAUSA pages were broken on staging for an unknown duration. The "same symptom, different table" pattern is the forcing function: every future table recreation reopens this class of failure unless automation closes it.

## Decision

Lakebase synced-table `SELECT` grants are applied explicitly by `scripts/run_lakebase_grants.py`, which is the **canonical and only** mechanism. The script runs in two modes:

1. **Apply mode** — idempotent `GRANT USAGE ON SCHEMA` + `GRANT SELECT ON ALL TABLES IN SCHEMA` for the Taipy app SP (UUID resolved from `terraform output -raw hf_app_sp_application_id`). Covers every synced table present in Lakebase at invocation time.
2. **`--verify` mode** — drift detector. Enumerates `ingestion.refresh_synced_tables.SYNCED_TABLES` as the authoritative expected inventory, cross-checks against `information_schema.role_table_grants`, and exits non-zero with a per-table diff if any `(SP, table)` pair is missing.

The script is wired into two gates:

- **Pre-deploy gate** in `scripts/manage_space.py deploy`: `_preflight()` invokes the verifier before `upload_folder`. A failing gate aborts the deploy with the drift diff. Escape hatch: `--skip-grants-check`.
- **Self-healing via GitHub Actions** (`.github/workflows/lakebase-grants.yml`): scheduled daily at 07:00 UTC (one hour after the 06:00 UTC `data_ingestion` job's `dbt_build` + `refresh_synced_tables` sequence completes), chained after `Terraform Apply` on main (any TF change to the synced_tables module can recreate tables), plus `workflow_dispatch` for manual incident response. Auth is via an admin PAT stored in `secrets.DATABRICKS_TOKEN` — deliberately decoupled from the Databricks job runtime identity because the ingestion SP is not a Lakebase PG role and cannot run `GRANT`. Drift window: ≤24 h for schedule-caught events, ≤minutes for TF-caught events, zero for deploy-path events (pre-deploy gate).

Auto-inherit via `pg_default_acl` is explicitly **not** part of the design. The ADR records why — to prevent future maintainers from re-adding a `FOR ROLE <x>` rule on the assumption it will work.

## Alternatives considered

| Option | Pros | Cons | Why rejected |
|---|---|---|---|
| A. Keep the `FOR ROLE databricks_superuser` rule and hope it works on future tables | Zero code change. | Verified not to fire — owner role is `databricks_writer_<instance_id>`, not `databricks_superuser`. Has already caused two separate "tables missing grants" incidents (PR #134 marts; embeddings + PAUSA drift discovered during this investigation). | Structurally impossible — rule targets the wrong grantor role. |
| B. Add `ALTER DEFAULT PRIVILEGES FOR ROLE databricks_writer_<id>` | Would auto-fire on synced-table creation. | Running this requires membership in `databricks_writer_<id>`; our admin user has no such membership and cannot gain it (internal Lakebase role). Even if it could be granted, the `_<id>` suffix is tied to the database instance and would drift on infrastructure changes. | Postgres-level access control blocks it. |
| C. Grant `SELECT` to `PUBLIC` on dev_gold / observability | One-time grant, never breaks again. | Every Databricks account-users alias in the Lakebase workspace gets read access by default, including principals we haven't vetted. Violates least-privilege. Legally relevant for a system that processes named-individual data under GDPR / EU AI Act. | Unacceptable security posture. |
| D. Explicit grants script (`run_lakebase_grants.py`) + pre-deploy gate + GitHub Actions scheduled self-healing | Deterministic, auditable, idempotent, self-healing. Auth separation of concerns — admin PAT stored in GitHub Secrets, not entangled with the Databricks job runtime identity (the ingestion SP is not a Lakebase PG role and cannot run `GRANT`). Triggers on schedule (post-daily-job), after `Terraform Apply`, and via `workflow_dispatch` for incidents. Verifier uses `SYNCED_TABLES` inventory so it stays in sync with the refresh task. No reliance on Postgres auto-inherit semantics. | New scheduled surface to monitor. Daily cadence means worst-case drift window ≈ 24 h (bounded by the pre-deploy gate for any user-triggered path). | — |

## Consequences

### Positive

- **Verified coverage**: `--verify` mode exited `0` with "SP has SELECT on all 37 synced tables" against live Lakebase 2026-04-17. Drift is no longer invisible.
- **Pre-deploy protection**: shipping a Taipy build with missing grants now fails at `_preflight` with a clear per-table diff, instead of returning opaque "Something went wrong" at runtime. The failure mode the user saw with PR #134 becomes impossible.
- **Self-healing**: GitHub Actions scheduled workflow re-applies grants daily (07:00 UTC, after the data-ingestion daily job completes) and after every `Terraform Apply` on main. A table recreation that drops grants is repaired within 24 h without any human intervention; TF-driven recreations within minutes.
- **Single source of truth**: Taipy SP UUID is resolved from `terraform output -raw hf_app_sp_application_id` at every invocation. Removes the previously hardcoded UUID in `scripts/run_lakebase_grants.py` that was the direct cause of "which SP do we grant to?" ambiguity.
- **Documents the platform constraint**: future maintainers see why auto-inherit is unavailable. A proposal to "just add a `FOR ROLE <x>` rule" will be caught by this ADR before being tried again.

### Negative

- Grants are not truly automatic at the DB layer — they rely on the GitHub Actions workflow running. If the workflow file is silently removed, drift can reaccumulate. Mitigation: the pre-deploy gate is an independent second line of defence — any drift becomes visible at the next deploy and surfaces with a clear per-table diff.
- The `--skip-grants-check` escape hatch in the deploy gate is a foot-gun. Use is logged with a WARNING, but someone ignoring the warning can still deploy a broken build. Acceptable because the daily job's self-healing task catches it on the next run.
- Pre-deploy gate adds `DATABRICKS_HOST` + `DATABRICKS_TOKEN` as practical deploy-time requirements. Already present for any local dev environment; CI deploy paths will need credentials configured.

### Neutral

- The deprecated/orphaned `be66af99-...` SP that was granted historically is **not** included in the new automation. It remains in the Lakebase ACLs from the manual grant I applied during the 2026-04-17 incident; removing it is a separate decision. It is not workspace-visible via SCIM (not a tracked Databricks SP), so the automation cannot grant or revoke to it without an explicit opt-in list.

## CLAUDE.md Amendment

The existing "Lakebase grants survive synced table recreation" bullet under *Project Conventions* is amended: remove the `ALTER DEFAULT PRIVILEGES FOR ROLE databricks_superuser` claim (verified non-functional) and replace with the explicit apply/verify flow documented here. A reference to this ADR is added inline.

## Related

- **Branch:** `fix/lakebase-grants-auto-inherit`
- **Incident:** PR #134 staging deploy 2026-04-17 — "Something went wrong loading the breakdown" on Heat Map, Player Impact (Breakdown), Goalkeeper Analytics, Player Similarity, Pass Timing (all traced to missing `SELECT` on respective synced tables).
- **Scripts:** `scripts/run_lakebase_grants.py` (rewritten), `scripts/lakebase_grants.sql` (updated), `scripts/manage_space.py` (new `_verify_lakebase_grants()` gate).
- **Self-healing workflow:** `.github/workflows/lakebase-grants.yml` — scheduled 07:00 UTC daily (catches drift from the 06:00 UTC `data_ingestion` job's `dbt_build` recreations), chained after `Terraform Apply` completes on main (catches TF-driven synced-table changes), plus manual `workflow_dispatch` for incident response. Auth via admin PAT stored in `secrets.DATABRICKS_TOKEN`. SP UUID read from `vars.HF_APP_SP_APPLICATION_ID` (one-time setup: `terraform output -raw hf_app_sp_application_id`).
- **Tests:** (follow-up) `src/tests/test_lakebase_grants_task_present.py` to enforce the daily-job task remains wired up.
- **External references:** PostgreSQL docs on `ALTER DEFAULT PRIVILEGES` — [`FOR ROLE target_role`](https://www.postgresql.org/docs/current/sql-alterdefaultprivileges.html) requires current user to own or be member of `target_role`.

## Updates

**2026-05-22 (ADR-026):** Synced tables are now SDK-managed via `w.postgres.create_synced_table()`. The "create in Databricks UI, then terraform import" workflow is replaced by `scripts/migrate_synced_tables.py`. Grants and event_log ownership procedures are unchanged — they operate on PG-side objects and DLT pipelines regardless of creation path.

## Notes

Full evidence gathered during investigation (queries, role memberships, SP SCIM lookups) is in the session transcript at
`C:\Users\Karsten\.claude\projects\D--Development-karstenskyt--luxury-lakehouse-d32\51b333c9-665c-409c-ba6f-6232bce13f4b.jsonl`. Summarised here:

```
-- Evidence 1: synced-table owner
SELECT pg_get_userbyid(c.relowner), COUNT(*)
FROM pg_class c JOIN pg_namespace n ON c.relnamespace=n.oid
WHERE n.nspname IN ('dev_gold','observability') AND c.relkind='p'
GROUP BY 1;
-- Returns: databricks_writer_16401  38

-- Evidence 2: pg_default_acl rules (none fire for synced-table creation)
SELECT pg_get_userbyid(defaclrole), n.nspname, defaclobjtype, defaclacl::text
FROM pg_default_acl d LEFT JOIN pg_namespace n ON d.defaclnamespace=n.oid
ORDER BY 1, 2;
-- Returns 6 rules; none scoped FOR ROLE databricks_writer_*

-- Evidence 3: pre-fix verify run (3 drifted tables the bulk-grant had not covered)
-- fct_player_embeddings_career_synced, fct_player_embeddings_season_synced, fct_pausa_values_synced

-- Evidence 4: post-fix verify run
-- "OK: SP 1a1dbf08-... has SELECT on all 37 synced tables present in Lakebase."
```
