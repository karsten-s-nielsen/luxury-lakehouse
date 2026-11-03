# v2 Ops deploy and refresh runbook

This runbook is the canonical procedure to install or refresh the Ops web app on each target without losing
lake data. It is the version-controlled source; the session memory files mirror it for fast lookup.

## Targets (design G11)

1. **Laptop / local** — a developer or operator machine.
2. **Mac Mini staging** — the always-on staging install on the external Development SSD.
3. **Hugging Face Space** — the public Ops Space `karstenskyt/ops`.

## Invariant: data lives outside the code tree

A code refresh must never touch the lake. This holds on every target because the lake and its config live
outside the code:

- **Local and Mac:** the lake root, the restricted root and the config (`bootstrap.json`) are siblings of the
  code tree, not inside it. The config resolves from `LAKEHOUSE_CONFIG_DIR`, the OS config folder, or the
  `--lake` flag / `LAKEHOUSE_LAKE_ROOT` variable.
- **HF:** the lake is two private HF Storage Buckets named by Space variables; it is never in the image.

So refreshing the code by copy, by `git pull`, or by `upload_folder` leaves the data intact, and it does not
matter how the files arrived.

## Local / laptop

```sh
cd v2
uv sync --locked
uv run lakehouse init --lake "<path outside the repo>"   # once; writes bootstrap.json in the OS config dir
uv run lakehouse account create ops                       # once; prompts for a password (a secret)
uv run python -m lakehouse.web                            # serves on http://localhost:7860
```

## Mac Mini staging

- SSH endpoint: `karsten@192.168.68.59` (key-only).
- Install root: `/Volumes/Development/lakehouse`. The code tree is `/Volumes/Development/lakehouse/v2`. The
  lake (`lake/`), the restricted root (`lake-restricted/`), the config (`config-home/bootstrap.json`) and
  `env.sh` are siblings of `v2/`, outside it.
- The restricted root must exist as a directory or the app refuses to start (`storage/roots.py`
  `open_storage`). An empty directory is correct when there are no private matches.

Refresh the code (Windows has no `rsync`, so push with tar over SSH; the target is exactly `.../v2/`, never the
parent, which holds the lake):

```sh
tar -C <windows-v2> -czf - \
  --exclude='./.venv' --exclude='./.git' \
  --exclude='./.pytest_cache' --exclude='./.ruff_cache' \
  --exclude='./.benchmarks' --exclude='./.import_linter_cache' \
  --exclude='./dbt/target' --exclude='./dbt/logs' --exclude='./dbt/dbt_packages' \
  --exclude='*/__pycache__' --exclude='*.pyc' \
  . | ssh karsten@192.168.68.59 'tar xzf - -C /Volumes/Development/lakehouse/v2'
```

`tar` preserves bytes, so verify with `sha256sum` against `shasum -a 256`. Then launch:

```sh
source /Volumes/Development/lakehouse/env.sh
cd /Volumes/Development/lakehouse/v2
uv sync --locked
uv run lakehouse account create ops     # once; the owner runs this (the password is a secret)
nohup uv run python -m lakehouse.web > /Volumes/Development/lakehouse/web.log 2>&1 &
```

### Sign in from another LAN host

The operator app is reached over plain http. In local mode the session cookie is not Secure (HF mode keeps it), so
sign-in works over http from any host, not only `localhost`. For a browser on another LAN host, the Taipy socket must
also accept that host's origin, so list it in `env.sh`:

```sh
export LAKEHOUSE_APP_ORIGINS="http://192.168.68.59:7860"   # comma-separated for several hosts; full scheme://host:port
```

Without it, `localhost:7860` and `127.0.0.1:7860` work on the Mac itself, but a browser at `http://192.168.68.59:7860`
is refused at the socket (`… is not an accepted origin`) and the operator shell stays blank after sign-in. A
matching-port SSH tunnel is the no-config alternative from a workstation (the tunnel port must equal the app port, so
the origin is `localhost:7860`):

```sh
ssh -N -L 7860:127.0.0.1:7860 karsten@192.168.68.59   # then open http://localhost:7860
```

## HF credentials and token scopes

Every HF credential is a Hugging Face User Access Token, or an S3 key derived from one. Each role needs an exact
scope. A wrong scope either fails a write, or lets a read that must be denied succeed (a visibility leak). This
section is the source of truth for the scopes; the Space and Job sections below only name where each one is set.

**Token roles.** A Hugging Face token has one role:

- `read`: read every repository and bucket you can read.
- `write`: read and write every repository and bucket you can write.
- `fine-grained`: an explicit per-resource permission list.

Organization membership widens `read` and `write`. A member of `luxury-lakehouse` with a `read` or `write` token
reaches **every** bucket and repository in the org, which includes `restricted` and the control repository. So a
role-scoped token can never be lake-only. Use a **fine-grained** token for any lake-only role, scoped to the one
bucket. (HF docs: User access tokens; Storage Buckets security and S3 compatibility.)

**Resources.** `luxury-lakehouse/lake` and `luxury-lakehouse/restricted` are HF Storage Buckets
(`hf://buckets/<namespace>/<name>`, AES-256 at rest). The lease ledger `luxury-lakehouse/lake-control` is a private HF
**dataset** repository (not a bucket), named `<lake bucket>-control` (T6). A fine-grained token grants Read or Write
per bucket and per repository; an S3 gateway key inherits the token's scope.

| Credential | Where it is set | Token type and scope |
| --- | --- | --- |
| Ops Space `HF_TOKEN` | `karstenskyt/ops` Space secret | fine-grained, **Write on the `luxury-lakehouse/lake` bucket only** (bucket-level Write covers the Space's config-side writes through `HfApi`; without the gateway key the Space still cannot write catalog or data). No `restricted`, no `lake-control`. The **data** credential: every bucket and page read uses it. |
| Ops Space `LAKEHOUSE_HF_JOBS_TOKEN` | `karstenskyt/ops` Space secret | the **job-control** credential, read ONLY by `HfJobsSchedule.from_env` (the Settings schedule: status, reschedule, suspend, resume, trigger). A scheduled job under a **personal** namespace can only be managed by a classic **`write`-role** token (HF's fine-grained Jobs permission is organisation-only; HF OAuth has no jobs scope). That token can also read the owner's other repos, so it is kept SEPARATE from the lake-only `HF_TOKEN` and is wired only into the Jobs-API seam, enforced by `test_jobs_token_confinement.py`. If the job is under an **org**, use a fine-grained token with org **Jobs** read+write instead, and this secret may be omitted (`from_env` then falls back to `HF_TOKEN`). |
| Job `HF_TOKEN` | the Job's `--secrets` | **Write** on `luxury-lakehouse/lake`, `luxury-lakehouse/restricted` and the `luxury-lakehouse/lake-control` dataset repo. A `write`-role token is enough; a fine-grained token lists the three with Write. |
| Job `HF_S3_ACCESS_KEY_ID` / `HF_S3_SECRET_ACCESS_KEY` | the Job's `--secrets-file` | the S3 gateway key generated from the Job token; it inherits Write. |
| Job `PINING_FOR_THE_DATA_TOKEN` | the Job's `--secrets` | the pining API token (not an HF token). |
| hf_space test `HF_TOKEN` | the `jobs-test` hf_space Job `--secrets` | fine-grained, **Write on the `luxury-lakehouse/lake-test` bucket only**. No `restricted-test`, no `lake-test-control`. The mirror of the Ops Space token. |
| hf_job test `HF_TOKEN` + S3 key | the `jobs-test` hf_job Job | Write on `lake-test`, `restricted-test` and `lake-test-control`, plus an S3 key generated from it. |

**Why the Ops Space DATA token is lake-only.** The Ops UI must never read Confidential restricted-match data (§9) or
the lease ledger (§29.9, T6). Two tests enforce it on the `HF_TOKEN` data credential:
`tests/hf/test_space_hf_live.py::test_space_token_cannot_read_the_restricted_bucket` (the restricted read must return
401, 403 or 404 — a fine-grained token denied an existing private bucket gets 404) and
`::test_space_token_cannot_touch_the_control_repo` (the control repository must return 401, 403 or 404). A `read`/`write`-role token, or any org-wide token, fails both because it reads restricted and the control
repository. That failure is correct, not a test defect.

**Why the job-control token can be broad, and how it is contained.** A personal-namespace scheduled job forces a
classic role token (see the credential table). That token could read restricted, so it never touches the data path:
`HfJobsSchedule.from_env` is its only reader, and `tests/unit/test_jobs_token_confinement.py` fails if any other module
reads `LAKEHOUSE_HF_JOBS_TOKEN`. The lake-only guarantee above is unaffected, because every page and bucket read still
uses the `HF_TOKEN` data credential — the restricted-bucket tests above run against that token and stay green.

**Create a fine-grained token.** Settings, then Access Tokens, then **Create new token**, then **Fine-grained**. Add
the one **bucket** `luxury-lakehouse/lake` (or, for the Job token, the three resources) to the permission list and set
its permission to **Write** (bucket-level Write covers read and write of that bucket). Two traps that silently grant
nothing: a bucket added with an **empty** permission grants no access, so set the Read/Write box on the bucket row; and
a grant on your **user namespace** (`karstenskyt`) does **not** reach an `luxury-lakehouse` **org** bucket, so grant on
the bucket, never on your account. Copy the token value; it shows once.

**Generate the S3 gateway key** (Job and hf_job only). Open the token's dropdown menu in the Access Tokens list, then
**Generate S3 credentials**. Copy the access key id (prefixed `HFAK`) and the secret; the secret shows once. The key
inherits the token's Read or Write scope. No API creates it.

**Verify a token's scope before use.** `HfApi(token=...).whoami()["auth"]["accessToken"]` reports the token's `role`
and name, with no secret value. A `role` of `read` or `write` on a lake-only credential is wrong; a lake-only token
reports `fineGrained`. Read `fineGrained.scoped`: it must list the lake **bucket** with a non-empty permission, and
nothing on your user namespace, `restricted` or `lake-control`. A bucket entry with `permissions: []`, or a grant on
the `karstenskyt` user instead of the bucket, gives a token that cannot read the lake at all (both show as HTTP 404
`BucketNotFoundError`, "Repository not found"). To prove the token end to end, run the hf_space tier against the test
buckets: the reader and config-write tests pass only with Write on the lake bucket, and the two denial tests pass only
when `restricted` and the control repository are absent.

**The repeatable create-and-verify loop.** HF has no API to create a token, and service accounts and resource groups
need a paid org tier (Team or Enterprise), so a token is made by hand in the UI, where the wording changes and is easy
to get wrong. Do not trust the UI alone: create the token, then run the committed checker, which exercises the token
against the live buckets and names the exact defect. Fix what it names and run it again until it prints `PASS`.

```sh
# the Ops Space token (production lake): set HF_TOKEN to the token value. --read-only skips the lake
# write probe, so the check never writes to the production lake (it still proves read + the two denials).
HF_TOKEN=<value> uv run python scripts/check_hf_token.py space --read-only \
  --lake luxury-lakehouse/lake --restricted luxury-lakehouse/restricted \
  --control luxury-lakehouse/lake-control

# the hf_space test token held in lakehouse.env as HF_LAKE_TEST, against the test buckets
uv run python scripts/check_hf_token.py space --token-env HF_LAKE_TEST \
  --lake luxury-lakehouse/lake-test --restricted luxury-lakehouse/restricted-test \
  --control luxury-lakehouse/lake-test-control
```

The checker reads the token from the named environment variable, never from an argument, and never prints it. It
prints the token's role and fine-grained grants, then one line per probe (`lake_read`, `lake_write`,
`restricted_read`, `control_read`), then `PASS` or `FAIL` with the fix. A grant placed on your user account instead of
the bucket reports "cannot read the lake bucket ... grant on the BUCKET resource itself, not your user account"; a
bucket added with an empty permission reports the same. Use role `job` for a Job token (it requires Write on all
three resources).

## Hugging Face Space

- Space: `karstenskyt/ops` (Docker Space, HF OAuth sign-in, allow-list). The Job image is `karstenskyt/jobs`.
- Buckets: `hf://buckets/luxury-lakehouse/lake` and `hf://buckets/luxury-lakehouse/restricted`.
- `HF_TOKEN` comes from the environment and is never printed. `huggingface_hub` 2.3.0 is in the venv.

### Space configuration (the authoritative list)

Set every row below in the Space (Settings → Variables and secrets). This table is the single source of truth; the
credential table above gives the token scopes, and this one gives every name a setup must enter. Nothing else is read.

| Name | Kind | Required | What it is | Verify |
| --- | --- | --- | --- | --- |
| `LAKEHOUSE_LAKE_ROOT` | variable | yes | `hf://buckets/luxury-lakehouse/lake` | — |
| `LAKEHOUSE_RESTRICTED_ROOT` | variable | yes | `hf://buckets/luxury-lakehouse/restricted` | — |
| `OPS_ALLOWED_USERS` | variable | yes | HF usernames allowed to sign in (e.g. `karstenskyt`), comma/space separated | — |
| `LAKEHOUSE_HF_SCHEDULED_JOB_ID` | variable | yes | the owner-created scheduled job's id (set after you create the job) | — |
| `OAUTH_CLIENT_ID`, `OPENID_PROVIDER_URL`, `OAUTH_SCOPES` | variable | auto | the OAuth client | HF sets these from `hf_oauth: true` |
| `OAUTH_CLIENT_SECRET` | secret | auto | the OAuth client secret | HF sets it |
| `HF_TOKEN` | secret | yes | the **data** token: fine-grained, Write on `luxury-lakehouse/lake` only (credential table) | `check_hf_token.py space` |
| `LAKEHOUSE_HF_JOBS_TOKEN` | secret | yes for a **personal-namespace** job (optional for an org job) | the **job-control** token: classic **write-role**; the Settings schedule reads it | `check_hf_token.py jobs --job-id <id>` |
| `LAKEHOUSE_SESSION_SECRET` | secret | yes | a random session-signing key, 32+ bytes | — |
| `PINING_LIST_TOKEN` | secret | optional | the list-only pining token (lists private pining tournaments) | — |

Deploy or update the code:

```python
from huggingface_hub import HfApi

HfApi().upload_folder(
    repo_id="karstenskyt/ops",
    repo_type="space",
    folder_path="<stage>",  # README.md, Dockerfile, pyproject.toml, uv.lock, src/, dbt/
    delete_patterns=["**"],  # mirror: prune repo files not in the stage
)
```

Stage layout: `README.md` from `deploy/space/README.md` (it carries the HF front matter), `Dockerfile` from
`deploy/space/Dockerfile`, plus `pyproject.toml`, `uv.lock`, `src/` and `dbt/`. Exclude `__pycache__`, the
caches and `dbt/{target,logs,dbt_packages}`. HF builds the image from these files; there is no GitHub and no
external registry. After the upload HF rebuilds; poll
`HfApi().get_space_runtime("karstenskyt/ops").stage` until it reports `RUNNING`.

The Space variables and secrets are set server-side and persist across uploads. Do not re-set them on a code
redeploy. The full set is the "Space configuration" table above.

## Job image and scheduled job

The Job image is the Docker Space `karstenskyt/jobs`. It holds the image only; its command is the scheduled run,
so the Space boots into `RUNTIME_ERROR`, which is its expected state. HF Jobs is the only production writer.

Deploy or update the Job image:

- Stage: `README.md` from `deploy/job/README.md`, `Dockerfile` from `deploy/job/Dockerfile`, plus `pyproject.toml`,
  `uv.lock`, `src/`, `dbt/`, `tests/` and `scripts/`. Exclude `__pycache__`, the caches and
  `dbt/{target,logs,dbt_packages}`.
- Keep the README in the stage. The upload mirrors the stage (`delete_patterns=["**"]`); a stage without the README
  removes the Space's front matter and HF reports `CONFIG_ERROR`.
- Upload with `HfApi().upload_folder(repo_id="karstenskyt/jobs", repo_type="space", folder_path=<stage>,
  delete_patterns=["**"])`. Poll `HfApi().get_space_runtime("karstenskyt/jobs").stage`: `RUNTIME_ERROR` means the
  image built; `CONFIG_ERROR` or `BUILD_ERROR` means it failed.
- The scheduled job reads the image at run time, so a code redeploy needs no change to the job.

Job environment (variables, not secrets):

```sh
LAKEHOUSE_LAKE_ROOT=hf://buckets/luxury-lakehouse/lake
LAKEHOUSE_RESTRICTED_ROOT=hf://buckets/luxury-lakehouse/restricted
LAKEHOUSE_LAKE_S3_PATH=s3://luxury-lakehouse/lake
LAKEHOUSE_RESTRICTED_S3_PATH=s3://luxury-lakehouse/restricted
LAKEHOUSE_CONTROL_REPO=luxury-lakehouse/lake-control
```

`LAKEHOUSE_CONTROL_REPO` names the lease ledger (T6): a private HF dataset repository, exactly `<lake bucket>-control`.
The HF S3 gateway does not make create-if-absent atomic under concurrency, so a Job wins the lease only by an HF Hub
commit to this repository whose parent is the head it read. A Job with gateway credentials stops before any work when
the variable is missing (`MissingRootError`), names another repository (`ConfigError`, exit 2), or the repository does
not exist or the Job token cannot read it. Only the Job token writes it; the Ops Space token needs no access.

The S3 path has the form `s3://<namespace>/<name>`. The first path token is the HF namespace, which the S3 gateway
treats as the bucket; the second is the bucket name. `s3://lake` and `s3://<namespace>--<name>` fail with
`NoSuchBucket` ("Repository not found"), and the gateway's own error reads "Missing namespace in request path".
Each S3 path must equal the `s3://<namespace>/<name>` that its `hf://buckets/<namespace>/<name>` root implies. A run
whose paths differ stops before any work, with a configuration error (exit code 2) that names the variable, its value
and the expected path (audit T4).

Job secrets: `HF_TOKEN`, `HF_S3_ACCESS_KEY_ID`, `HF_S3_SECRET_ACCESS_KEY`, `PINING_FOR_THE_DATA_TOKEN`.

- The S3 gateway key exists only through the HF web UI: Access Tokens, the token's menu, "Generate S3
  credentials". The key id starts with `HFAK`; the secret shows once. No API creates it.
- Keep the key pair in an owner-only dotenv file outside the repo and pass it with `--secrets-file`. Pass the
  other secrets with `--secrets NAME`; the CLI reads the value from the environment. A session never reads or
  prints these values.

Scheduled job: name `lakehouse-ingest`, cron `0 3 * * *` UTC, `--no-concurrency`, `cpu-basic`, `--timeout 21600`.
The ops Space variable `LAKEHOUSE_HF_SCHEDULED_JOB_ID` holds its id.

HF has no in-place edit for a scheduled job's environment or secrets. The API offers create, delete, suspend,
resume, trigger, update schedule and update labels only. To change the environment or the secrets, recreate the
job:

1. Dry-run and check the resolved configuration:

   ```sh
   hf jobs scheduled run --dry-run --no-concurrency --flavor cpu-basic --timeout 21600 \
     --label name=lakehouse-ingest --env-file <job.env> --secrets-file <secrets.env> \
     --secrets HF_TOKEN --secrets PINING_FOR_THE_DATA_TOKEN \
     "0 3 * * *" hf.co/spaces/karstenskyt/jobs \
     sh -c "python -m lakehouse.mask -- lakehouse run --trigger schedule"
   ```

2. Run the same command without `--dry-run`. Note the new id.
3. Point the ops variable at it:
   `HfApi().add_space_variable("karstenskyt/ops", "LAKEHOUSE_HF_SCHEDULED_JOB_ID", "<new id>")`.
4. Delete the old job: `HfApi().delete_scheduled_job(scheduled_job_id="<old id>")`.
5. Verify: `HfApi().inspect_scheduled_job(scheduled_job_id="<new id>").job_spec.environment` shows the four paths.

Wrap a container command that holds `--` in `sh -c "..."`; otherwise the CLI consumes the separator.

**Lease-ledger cutover (T6, once).** A Job of an image without the ledger fences on the bucket only and cannot see the
ledger, so the two must never run together:

1. Suspend the scheduled job (`HfApi().suspend_scheduled_job(...)`).
2. Wait until `hf jobs ps` shows no Job of the old image.
3. Create the private dataset repository `luxury-lakehouse/lake-control`.
4. Deploy the Job image; recreate the scheduled job with `LAKEHOUSE_CONTROL_REPO` in its environment (the procedure
   above) and point `LAKEHOUSE_HF_SCHEDULED_JOB_ID` at it.
5. Resume, trigger one run, and read its log: `lease acquired` with `"fence": "ledger"` (the first acquisition starts
   from the highest bucket generation).

After the cutover, every lease taker against the HF lake (`run`, `reparse`, `sweep`, `restore`, `relocate`) runs as an
HF Job on the Job image with the Job token. Never run a CLI of older code against the HF lake: it would fence on the
bucket only. T6 tests run on the separate test image `karstenskyt/jobs-test`, the test buckets and
`luxury-lakehouse/lake-test-control`, never on the production image.

A one-off ingest uses the same image:

```sh
hf jobs run -d --flavor cpu-basic --timeout 21600 --env-file <job.env> --secrets-file <secrets.env> \
  --secrets HF_TOKEN hf.co/spaces/karstenskyt/jobs \
  sh -c "python -m lakehouse.mask -- lakehouse run --trigger schedule"
```

Always pass `--timeout`. On 2026-10-31 the cold public ingest on `cpu-basic` needed more than 2 hours, and a
2-hour timeout stopped it. A stopped run resumes: the next run skips the matches already committed.

On Windows, write the logs to a file: `PYTHONIOENCODING=utf-8 hf jobs logs <namespace>/<id> > <file>`. Without it
the console codec crashes the CLI.

## Catalog checkpoints and version retention

A run publishes its catalog by a checkpoint policy, not after every match (T3,
`docs/specs/2026-10-31-v2-t3-catalog-checkpoint-design.md`). The `checkpoint` block of the saved settings holds:

| Setting | Default | Meaning |
|---|---|---|
| `checkpoint.minutes` (T) | 5 | Publish when this many minutes passed since the last publish. |
| `checkpoint.matches` (M) | 50 | Publish when this many units (match writes, lineage records, listing writes) are pending. |
| `checkpoint.grace_minutes` (G) | 2 | Delete a superseded catalog version only after its successor was `CURRENT` this long, so a page that opened it keeps reading. |
| `checkpoint.move_batch` (B) | 50 | Publish a private-to-public move batch after this many moves. Public-to-private moves always publish at once. |
| `checkpoint.warn_versions` | 200 | Record a `catalog_growth` warning when more versions than this are retained. |
| `checkpoint.warn_catalog_mb` | 1024 | Record a `catalog_growth` warning when a published catalog copy is larger than this. On HF the warning fires at half the single-part limit (2,048 MiB) at the latest, because a larger copy cannot be published. |

Change one with `lakehouse config set checkpoint.<field> <n>` (each value is an integer of at least 1). A crashed or
killed run loses at most one checkpoint interval of work; the next run redoes it. A stopped run (SIGTERM) publishes
its pending work at the run end. The rendered launchd plist (`ExitTimeOut`) and systemd unit (`TimeoutStopSec`) give
it 60 seconds before SIGKILL; reinstall a schedule that was installed earlier (`lakehouse schedule install ...`) to
pick this up. HF Jobs sends SIGKILL about 2 seconds after SIGTERM (measured 2026-10-31; no setting exists), so a
cancelled or timed-out HF run is a crash: the next run redoes the lost interval once the lease has expired (10 minutes
plus a 2-minute takeover grace). Version retention (`retention.version_days`) and snapshot retention
(`retention.snapshot_days`) are unchanged: in-run versions are pruned after G, run-end versions after
`version_days`.

## Run-end file merge and the open-file limit (T12, 2026-11-01)

Every insert into an ops table (and any native dimension or link) writes one DuckLake data file, and v2 never runs
DuckLake's auto-compaction (ADR-001), so these tables accumulate small files. At each run end the run merges them,
under one setting:

| Setting | Default | Meaning |
|---|---|---|
| `maintenance.merge_budget_seconds` | 300 | Merge the small catalog data files at each run end for up to this many seconds; the rest waits for the next run. 0 turns the merge off. |

Change it with `lakehouse config set maintenance.merge_budget_seconds <n>`. Bronze tables are never merged (they keep
`auto_compact = false`; a run stops loudly and records a `maintenance_guard` error if a bronze table lost that option).
The merge deletes no file: the replaced files leave through the normal cleanup once their snapshots expire.

One DuckLake scan of a large table opens hundreds of file descriptors (measured: 300 files → 369–478 open; 10,000 →
834), so **every lakehouse process raises its open-file soft limit** at start (target 65,536, capped by the hard limit
and, on macOS, by `kern.maxfilesperproc`). A command that opens a catalog (`run`, `reparse`, `verify`, `restore`,
`relocate`, `sweep`, `decisions`, `init`, the web app) stops with a clear error when the soft limit is below 4,096,
naming `ulimit -n` (local/launchd), the launchd `NumberOfFiles` key and the systemd `LimitNOFILE` key. The rendered
launchd plist carries `SoftResourceLimits.NumberOfFiles = 65536` and the systemd unit carries
`LimitNOFILE=65536:524288`; **reinstall a schedule installed before T12** (`lakehouse schedule install ...`) to pick
these up, otherwise the scheduled run inherits the platform default (256 on macOS launchd) and stops at the limit.

`lakehouse sweep` gained `--dry-run` (list what a large sweep would delete, write the keys to `--list-file`, delete
nothing) and `--allow-large --expect <list file>` (delete only the keys in the list, and stop before any delete if the
sweep's own computation finds another). Use the dry run to check a large sweep before you lift the 10 % guard.

## Encryption key at rest (per platform)

A **local** lake is encrypted at rest only when `LAKEHOUSE_ENCRYPTION_KEY` is present in the lakehouse process's
environment. The app reads the key from the environment through one seam and never knows how the environment was
populated; the operator sets it per platform, out of band, never through the app and never in a scheduled task
file. The key must be at least 64 characters of the hex/base64url charset (>= 256 bits) — generate it with
`openssl rand -hex 32`. The same one key is generated once on the AI-PC and applied to the local/Mac lake; HF stays
platform-encrypted and takes no key.

- **Local / laptop (Windows):** set it as a user- or machine-scope environment variable
  (`setx LAKEHOUSE_ENCRYPTION_KEY <key>`), or in the shell/profile that launches the app. Re-create the lake with
  the key set (`lakehouse init`); an existing unencrypted lake cannot be encrypted in place in this cycle (the
  migration is deferred — see the S1 spec Non-goals).
- **Mac Mini staging:** store the key in the standard Mac toolchain (for example the login keychain or a
  machine-scope launchd/`launchctl setenv` entry) and export it into the app's environment; recreate the lake with
  the key set. The app itself is agnostic to how the key was retrieved.
  - **This test install (set up 2026-10-30):** the key is generated on the Mac (`openssl rand -hex 32`) and stored
    only in `/Volumes/Development/lakehouse/env.sh` (`LAKEHOUSE_ENCRYPTION_KEY`), beside the pining token. The value
    is never written to the repo. This is the documented plaintext-env tradeoff (audit S1-IMPL-18): the key sits on
    the same volume as the lake. Losing `env.sh` makes the encrypted lake unrecoverable — S1 has no key recovery — so
    copy the value to a password manager for durability. The lake holds pining-only data (the 4 `*_pining` modules,
    `{kind: all}`); re-create it with `lakehouse init` if the key is ever lost.
- **Hugging Face Space:** do **not** set the key. An `hf://` lake resolves no key and ignores the variable; HF
  Storage Buckets are platform-encrypted (AES-256).
- **Scheduled runs (launchd / systemd / Task Scheduler):** the app never writes the key into the task definition.
  The operator supplies it to the scheduled process's environment through platform ops-config, out of band.
  **Keep the key value off the lake disk in plaintext** (owner decision 2: the key is stored separately from where
  it is used). Prefer a mechanism that does not persist the plaintext beside the data it protects: fetch it at
  task start from the macOS keychain / a systemd credential (`LoadCredential=`) / Windows Credential Manager, or,
  if an env file is unavoidable, put it on a **different** volume with `0600` permissions and an owner-only path —
  not in the lake directory.

  **Residual (accepted, audit S1-IMPL-18):** any on-disk env mechanism that holds the key in plaintext (a launchd
  plist `EnvironmentVariables`, a systemd `Environment=`, a world-readable `EnvironmentFile`) weakens at-rest
  protection against an attacker who can read that file — the key and the ciphertext would sit on the same host.
  The S1 design does not harden the scheduled-process key delivery beyond this guidance; a keychain/credential-
  store fetch is the recommended path, and a plaintext env file is a documented tradeoff, not the default.

## Secrets are owner-only

The operator account password and every Space or Job secret are secret values. The owner sets them in the
owner's own terminal or the HF settings UI. A development session never reads, prints, types or forwards a
secret value, and never sets one through a peer session.

## Deploy is an outward action

Creating or deploying the Space, a bucket, or the scheduled job needs the owner's explicit approval each time
(design: "The owner names the Space, bucket and job before deploy").
