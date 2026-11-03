# v2 Ops troubleshooting

Problem → cause → fix for the failures seen in setup and operation. For the full deploy procedure and the credential
scopes, see `deploy.md`.

## Hugging Face Space and tokens

| Symptom | Cause | Fix |
| --- | --- | --- |
| Settings page shows `403 Forbidden ... missing permissions: job.read ... /scheduled-jobs/...` | The job-control token (`LAKEHOUSE_HF_JOBS_TOKEN`) is missing, or it is a fine-grained token (fine-grained cannot manage a personal-namespace job) | Set the Space secret `LAKEHOUSE_HF_JOBS_TOKEN` to a classic **write-role** token. Verify: `uv run python scripts/check_hf_token.py jobs --token-env LAKEHOUSE_HF_JOBS_TOKEN --job-id <id>` |
| Settings shows the config but the schedule section reads "Schedule status unavailable" | Same as above — the schedule read was refused | Fix the token as above. The rest of Settings still renders by design; only the schedule section is affected |
| A token check or `whoami` returns `401 Invalid user token` | The token value is wrong — a truncated or mis-pasted copy, or a revoked token | Re-copy the token value exactly and set it again. A valid HF token is `hf_` + 34 characters |
| `check_hf_token.py space` fails: "reads the restricted bucket; it must be denied" | The token is a role token (read/write) or org-wide, not lake-only | Create a **fine-grained** token scoped to the `luxury-lakehouse/lake` bucket only, Write |
| `check_hf_token.py` prints `RESULT: INCONCLUSIVE` (exit 2) | A probe hit a 5xx, a timeout or a network error — not a denial, so the run proves nothing | Re-run. A real denial is HTTP 401/403/404 |
| The Space stays on an old build after an upload | HF rebuilds asynchronously | Poll `HfApi().get_space_runtime("karstenskyt/ops").stage` until `RUNNING`; confirm `.raw["sha"]` matches the upload commit |
| A newly set Space secret has no effect | The running container keeps the old environment | `HfApi().restart_space(repo_id="karstenskyt/ops")`, then poll to `RUNNING` |
| Local sign-in returns `CSRF token missing or wrong` | A stale session cookie — cookies ignore the port, so two local instances on different ports share one cookie | Reload `/signin` (it issues a fresh CSRF token) and submit again; or clear the site cookie |

## Job image and scheduled job

| Symptom | Cause | Fix |
| --- | --- | --- |
| The Job Space reports `RUNTIME_ERROR` | Expected: the Job image's command is the ingest run, so the Space cannot boot as a web app | Not a failure. `RUNTIME_ERROR` means the image built. Use it as a Job image only |
| The Job Space reports `CONFIG_ERROR` | The upload stage had no `README.md`, so HF lost the front matter (`delete_patterns=["**"]` mirrors the stage) | Keep `deploy/job/README.md` in the stage and re-upload |
| A run stops at once with a configuration error (exit 2) naming an S3 path | `LAKEHOUSE_*_S3_PATH` is the wrong form, or does not match its `hf://` root | Use `s3://<namespace>/<name>` — e.g. `s3://luxury-lakehouse/lake`. `s3://lake` and `s3://<ns>--<name>` fail with `NoSuchBucket` / "Missing namespace in request path" |
| A run exits 3 "Skipped" | Another run holds the single-writer lease | Wait for that run to finish, then retry |
| A run on `cpu-basic` is killed near 2 hours | A cold full public ingest needs more than 2 hours | Always pass `--timeout` (production uses `--timeout 21600`). A stopped run resumes: the next run skips committed matches |
| `hf jobs logs` crashes on Windows (console codec error) | The Windows console codec cannot encode the log bytes | `PYTHONIOENCODING=utf-8 hf jobs logs <namespace>/<id> > <file>` |
| A changed job env or secret does not take effect | HF has no in-place edit for a scheduled job's env/secrets | Recreate the job (delete + `hf jobs scheduled run`), then repoint `LAKEHOUSE_HF_SCHEDULED_JOB_ID` (deploy.md, "Job image and scheduled job") |

## Local and lake

| Symptom | Cause | Fix |
| --- | --- | --- |
| A command stops with a missing-root error and writes nothing | The lake folder is unreachable (a disconnected drive), or no source named a root | Reconnect the drive, or pass `--lake` / set `LAKEHOUSE_LAKE_ROOT` (see `README.md`, "Where the lake lives") |
| After moving a lake, paths are wrong | The catalog still holds the old file paths | `uv run lakehouse relocate --from <old root> --to <new root>` |
