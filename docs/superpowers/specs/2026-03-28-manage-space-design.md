# Design Spec: `scripts/manage_space.py`

**Date:** 2026-03-28
**Replaces:** `scripts/deploy_taipy.py`
**Goal:** Single script owning the full HF Space lifecycle — create, deploy, status, rebuild, teardown. Works from scratch with zero dependency on existing Spaces. Same script for staging and production (only the target name differs).

## Rationale

If production goes down, we need the exact same procedure (just a different Space name) to recreate it. This cannot be left to manual steps, tribal knowledge, or partial scripts. `deploy_taipy.py` only handles upload — it assumes the Space already exists, has no create/teardown, and no secret management.

## Subcommands

### `create <target>`

Creates a Space from nothing and configures it fully.

1. **Production safety gate:** If target is `production`, require `--force` flag. Without it, print a warning and exit. Staging has no gate.
2. `delete_repo(repo_id, repo_type="space", missing_ok=True)` — clean slate (idempotent).
3. `create_repo(repo_id, repo_type="space", space_sdk="docker")` — creates the Space.
3. Set secrets via `add_space_secret()`:
   - 5 infrastructure secrets read from environment variables (see Secret Handling below).
   - `DATABRICKS_TOKEN`: if `DATABRICKS_TOKEN` env var is set, use it. Otherwise, print a clear instruction for the user to set it manually and exit with a non-zero code.
4. Verify with `space_info(repo_id)` — confirm the Space exists and returns valid data.
5. Print summary: Space URL, secrets configured, any manual steps remaining.

**Does NOT deploy code.** Create and deploy are separate steps so failures are isolated.

### `deploy <target>`

Uploads the app to an existing Space. Absorbs all logic from `deploy_taipy.py`.

1. Pre-flight: verify Space exists (`space_info`), verify `hf_taipy_app/` and `README.md` exist, verify HF token.
2. Bundle workflow cards (`workflow-cards/` → `hf_taipy_app/workflow-cards/`).
3. Patch `.gitignore` to remove `workflow-cards` exclusion (upload_folder respects `.gitignore`).
4. `upload_folder(folder_path="hf_taipy_app", repo_id=..., repo_type="space", ignore_patterns=IGNORE_PATTERNS, delete_patterns=["**"])`.
5. Restore `.gitignore` in `finally` block.
6. Clean up bundled workflow cards in `finally` block.
7. Post-upload verify: `last_modified` advanced, remote file count.
8. Poll `get_space_runtime()` every 15 seconds, printing stage transitions, until `RUNNING` or timeout (10 minutes). Non-zero exit if timeout.

Flags:
- `--dry-run` — preview files to upload/delete without uploading.
- `--no-clean` — skip `delete_patterns=["**"]` (upload without deleting stale files).
- `--no-wait` — upload but don't poll for RUNNING.

### `status <target>`

Shows current Space state.

1. `space_info(repo_id)` — print stage, hardware, last modified, SDK, URL.
2. `get_space_runtime(repo_id)` — print runtime details (stage, hardware, sleep time).
3. Count remote files via `list_repo_tree()`.

### `rebuild <target>`

Factory reboot without recreating.

1. `restart_space(repo_id, factory_reboot=True)`.
2. Poll `get_space_runtime()` every 15 seconds until `RUNNING` or timeout (10 minutes).

### `teardown <target>`

Complete removal.

1. **Production safety gate:** If target is `production`, require `--force` flag. Without it, print a warning and exit.
2. Attempt `pause_space(repo_id)` — graceful stop (swallow errors if Space is already stopped/broken).
2. `delete_repo(repo_id, repo_type="space", missing_ok=True)`.
3. Verify deletion: `space_info(repo_id)` should raise `RepositoryNotFoundError`.
4. Print confirmation.

## Targets

```python
TARGETS: dict[str, str] = {
    "staging": "luxury-lakehouse/staging",
    "production": "luxury-lakehouse/soccer-analytics-app",
}
```

Same as current `deploy_taipy.py`. Target is a required positional argument.

## Secret Handling

Six secrets, all read from environment variables on the machine running the script:

| Secret | Env var | Sensitive | Notes |
|--------|---------|-----------|-------|
| `DATABRICKS_HOST` | `DATABRICKS_HOST` | No | Workspace URL, same for staging and production |
| `DATABRICKS_TOKEN` | `DATABRICKS_TOKEN` | **Yes** | PAT, expires ~2026-06-14. User may prefer to set manually. |
| `LAKEBASE_HOST` | `LAKEBASE_HOST` | No | Lakebase PG hostname |
| `LAKEBASE_ENDPOINT_NAME` | `LAKEBASE_ENDPOINT_NAME` | No | For OAuth token generation |
| `LAKEBASE_DATABASE` | `LAKEBASE_DATABASE` | No | Default: `databricks_postgres` |
| `GOLD_SCHEMA` | `GOLD_SCHEMA` | No | Default: `dev_gold` |

The `create` subcommand reads these from the local environment and pushes them as HF Space secrets. If `DATABRICKS_TOKEN` is not set, the script prints instructions and exits non-zero (the user sets it manually via `huggingface-cli` or the HF web UI, then re-runs `create`).

A `--skip-secrets` flag on `create` allows re-creating a Space without touching secrets (useful when secrets are already set or when only the Space shell needs recreation).

## Code Structure

Single file: `scripts/manage_space.py`. No new packages or modules.

Reuses from `deploy_taipy.py`:
- `FOLDER_PATH`, `TARGETS`, `IGNORE_PATTERNS` constants.
- `_bundle_workflow_cards()`, `_cleanup_workflow_cards()`, `_list_local_files()`, `_matches_any()` helpers.
- `_preflight()` (modified: no longer calls `sys.exit` — raises exceptions instead).
- `_deploy()` logic (enhanced with polling).
- `_dry_run()` logic.

New functions:
- `_create_space(repo_id, api)` — create + secrets.
- `_teardown_space(repo_id, api)` — pause + delete + verify.
- `_rebuild_space(repo_id, api)` — factory reboot + poll.
- `_status(repo_id, api)` — info dump.
- `_poll_until_running(repo_id, api, timeout_s=600)` — shared polling loop with 15s interval and stage transition logging.

CLI via `argparse` with subcommands:
```
python scripts/manage_space.py create staging
python scripts/manage_space.py deploy staging
python scripts/manage_space.py deploy staging --dry-run
python scripts/manage_space.py status staging
python scripts/manage_space.py rebuild staging
python scripts/manage_space.py teardown staging
```

## Entry Point

Replace the `deploy-taipy` entry point in `pyproject.toml`:

```toml
# Before:
deploy-taipy = "scripts.deploy_taipy:main"

# After:
manage-space = "scripts.manage_space:main"
```

`deploy_taipy.py` is deleted.

## Error Handling

- All subcommands return 0 on success, non-zero on failure.
- HF API errors surface as logged messages with the HTTP status and response body.
- `create` is idempotent — running it twice is safe (deletes and recreates).
- `teardown` is idempotent — running it on a non-existent Space is safe (`missing_ok=True`).
- The polling loop logs every stage transition and exits non-zero on timeout.

## Logging

Same structured logging as `deploy_taipy.py`:
```python
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-7s  %(message)s",
    datefmt="%H:%M:%S",
)
```

## Testing Strategy

The script calls HF Hub API — tests would require either mocking the API or a real HF account. Given this is an infrastructure script (not domain logic), testing is manual:

1. `teardown staging` — verify broken Spaces are deleted.
2. `create staging` — verify Space exists, secrets configured.
3. `deploy staging` — verify code uploaded, Space reaches RUNNING.
4. `status staging` — verify output shows RUNNING, correct file count.
5. `rebuild staging` — verify factory reboot completes.
6. `teardown staging` — verify clean deletion.
7. `create staging` + `deploy staging` again — verify full cycle is repeatable.

## Out of Scope

- Multi-org support (hardcoded to `luxury-lakehouse`).
- Automated secret rotation (PAT expiry is a manual process).
- CI/CD integration (future work — the script is designed to be called from CI).
- Persistent storage management (`request_space_storage` / `delete_space_storage` — not needed currently).
