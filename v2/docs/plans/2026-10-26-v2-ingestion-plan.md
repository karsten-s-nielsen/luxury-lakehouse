# Lakehouse v2, cycle 1: ingestion — implementation plan

> **For the executing session:** execute natively, task by task, with superpowers:executing-plans. The owner's standing rules forbid subagents and worktrees. All work happens on the one branch `feat/v2-ingestion`. Steps use checkbox (`- [ ]`) syntax. There is **one git commit for the whole cycle**, in Task 35, and only after the owner's explicit yes. No task commits. No push.

**Goal:** build cycle 1 of lakehouse v2 as the spec defines it: nine ingestion modules, typed and complete bronze in a DuckLake catalog with our own tuned Parquet files, the v1 Kimball keys, entity resolution with a review queue, dbt dimensions, authenticated Ops pages, scheduling, and three installs (laptop, HF, Mac Mini staging).

**Architecture:** channel adapters list and fetch, provider parsers write one canonical bronze form, and a single-writer run commits immutable catalog versions to a lake root and a restricted root. A typed services layer is the only entry point for the CLI and the Taipy Ops pages. dbt builds silver and gold from bronze inside the run.

**Tech stack:** Python 3.12, duckdb 2.0.x with DuckLake 1.1, pyarrow 26.0.0, dbt-core 1.12.6 with dbt-duckdb 1.11.0, huggingface_hub 2.3.0, boto3 (HF S3 gateway), xxhash 4.0.2, rapidfuzz 3.14.6, silly-kicks 4.139.1, taipy 4.1.1, pydantic 2, requests, defusedxml, argon2-cffi, authlib, filelock, opentelemetry-api, uv.

**Spec:** `v2/docs/specs/2026-10-26-v2-ingestion-design.md` (owner-approved 2026-10-26, A/B approved in round 7). The spec is the source of truth. "§n" below means a section of the spec. Read the spec section that a task names before you start the task.

## Global constraints

- Python `>=3.12,<3.13`. Exact pins for every runtime and dev dependency in `v2/pyproject.toml`, locked in `v2/uv.lock`.
- `taipy==4.1.1` with `[tool.uv] override-dependencies = ["flask-socketio>=5.5.1"]` (v1 ADR-083), re-checked in Task 28.
- Key formula: `xxhash64(concat_ws('|', provider, cast(native_id as string)))`, seed 42, signed BIGINT. Check value: `('idsse', 'J03WMX')` → `4203055498971744044` (§14.1).
- Provider names in keys: `statsbomb`, `wyscout`, `idsse`, `skillcorner`, `gradientsports`. The channel never enters a key (§14.2).
- Provider rank for canonical pointers: `statsbomb` > `wyscout` > `idsse` > `skillcorner` > `gradientsports` (§14.3).
- Tracking layout: players + frames tables, `int32` centimetres, sort `(period, player_key, frame)`, `DELTA_BINARY_PACKED` on x, y and frame, dictionary on `match_key`, period, `player_key` and flags, zstd level 3 (§10.2).
- Retention: snapshots 8 days, run-end catalog versions 7 days, snapshot retention always longer than version retention (§10.7, §10.8).
- Orphan sweep guards: every key of the `CURRENT` manifest must be listed; abort above 10 % of `data/` without an explicit flag (§10.3).
- Resource cap defaults: DuckDB `threads=4`, `memory_limit='3GB'`, pyarrow CPU count 4 (§10.9).
- HTTP: HTTPS only, `verify=True`, timeouts `(10, 30)`, at most 3 retries with exponential backoff on 429 and 5xx (§20).
- Resolution targets: precision ≥ 0.99 in the automatic band, recall ≥ 0.95 after review, per entity type (§15.4).
- Two storage roots on every install: `lake` and `restricted` (§9, §10.5).
- ruff rules E, W, F, I, N, UP, B, S, BLE, RUF, line length 120; pyright `strict` for `src/lakehouse` (§23.4).
- No secrets in code, config, logs or pages. No `eval`, `exec`, `pickle.loads`, or `subprocess` with `shell=True` (§20).
- STE prose for every doc, ADR, comment and message (owner rule).
- The AI-PC runs only light jobs: one heavy process at a time, the caps above. Production-scale benchmarks, full runs and every parse of a full tracking file go on the Mac Mini (C7, §10.9). On the AI-PC, run the `live` tier only as `-m "live and not heavy"`.
- The session never sees a secret value: no HF token, gateway key or pining owner token passes through a command, a file or a message of the session (§18, §20, §29.5). The owner's `hf auth login` token stays in the HF token cache, and the session never reads or prints it. Every test that needs a secret runs in an HF Job that the owner launches (Task 33).

## Spec amendments (owner-approved)

The owner approved these points on 2026-10-26. The spec records them as §29.9 to §29.14, with §9, §10.2–§10.6, §10.8, §18, §22.3, §23.6, §26 and §28 aligned.

- **§29.9, catalog-control objects on HF.** The lease generations, the catalog versions, the manifests and `CURRENT` go through the S3 gateway with conditional headers, because `HfApi` has no conditional write (§10.6). The amendment aligns §10.5, §26 item 1 and §28. Side effect: on HF, `restore`, `relocate` and `sweep --allow-large` run only inside an owner-launched HF Job, because only a Job holds the gateway key.
- **§29.10, moved keys.** A file name carries a write id, so no object key is ever registered twice (§10.2). Every visibility move writes a move record with its direction. The delete rules apply only to public-to-private records. A restore re-applies every newer record in order, so a match ends with its latest visibility. A guard stops the run if a moved key is still registered (§9, §10.3, §10.8). Two further points close a gap found by the author: when a match changes content and becomes private in the same run, the move runs before the replace, so every old lake-root key gets a record and a restricted copy; and the kept-version file set maps each public-to-private old key to its new key instead of only removing it, so a restore point from before the move keeps a restricted copy of its content. §23.6 "Visibility change moves files" adds the public → private → public case and the same-run case.
- **§29.11, moves per match and the sweep.** A public-to-private move covers every lake-root object of the match that the current catalog registers, a kept version names, or DuckLake still tracks. A restore gives every match its latest visibility and keeps the restored content. Move-before-replace applies to public-to-private only. The orphan sweep never deletes an object that DuckLake still tracks. The Mac Mini system plist in `/Library/LaunchDaemons/` is the one exception to `Development`, installed by the owner with `sudo`. The owner also approved the author's added text in §10.3 step 7, §10.6 and §26 item 2.
- **§29.14, visibility of excluded and inactive-module matches.** "No longer updated" means content only: an excluded match is not downloaded or re-parsed, but its visibility follows the source flag, so it still gets the private move. Every run lists each pining module that holds ingested matches, active or not, for visibility only. A widened filter shows the matches that resume updating.
- **§29.13, StatsBomb 360 option and narrowed filters (owner request).** The two StatsBomb modules get the checkbox "Only matches with 360 freeze frames" (off by default); the preview shows each StatsBomb tournament's exact 360 share; narrowing a filter keeps ingested data and stops its updates.
- **§29.12, restore in two passes, strays, record pruning, raw files.** A restore first swaps every moved lake key for its restricted copy, then places the match by its latest visibility, and checks that every named file exists before it publishes. The private move also covers stray uploads under the match's file-name prefix. A move record is pruned only when DuckLake no longer tracks its key at all. When a match becomes private, the lake-root raw files of its pining deliveries move to the restricted root, except a raw object that a public-channel delivery also has.

## Owner gates (the executing session stops and asks)

| Gate | Where | What needs the owner |
|---|---|---|
| G-A | Task 2 | v1 reference key values: Lakebase access, or approval of a throwaway local `pyspark` venv |
| G-B | Task 33 | each outward HF action, with its own explicit yes: create the lake bucket; create the restricted bucket; create the Space; create the Job-image Space; deploy or update a Space image; set or change each non-secret Space variable. The session uses the owner's own `hf auth login` on the machine and never reads or prints its token. The owner does these personally: create the HFAK gateway keys and the fine-grained tokens (HF web UI), set every Space secret, keep the secrets files, and launch every HF Job that needs a secret, the scheduled job included (§29.5). The session receives only names, job ids and logs. |
| G-C | Tasks 30, 32 and 33 | any §26 item that the task runs and that fails, a gateway key that cannot be limited to the two buckets, or a parse-memory budget that the Mac Mini cannot meet; stop and bring the evidence (§26) |
| G-D | Task 30 | the Mac Mini: SSH access, and any install on it |
| G-E | Task 34 | the truth-set investigation (with or without web research) and the owner's labelling |
| G-F | Task 34 | the pining list-only tier must exist and pass §26 item 16 (§29.8) |
| G-G | Task 35 | the single cycle commit, after the owner sees the file list and diff; no push |

## Review focus

The spec implies these inputs, but no §23.6 row names them. Each has a test in the task that owns the code.

1. **A source answers 200 with an HTML page instead of data** (the GS Drive virus-scan page, a GitHub rate-limit page, a pining gateway error page). Expected: the adapter fails that delivery with `UnexpectedPayloadError` naming the URL kind, and stores nothing. Test: Task 14 `test_html_payload_rejected`, Task 18 `test_drive_confirm_page_followed_or_rejected`.
2. **A truncated or interrupted download** (a 90 MB LFS file cut off, a gzip stream that ends early). Expected: retry within the HTTP rules, then fail loudly; never store a partial file in the raw zone or bronze. Test: Task 14 `test_truncated_body_never_returned`, Task 5 `test_truncated_gzip_raises`.
3. **GitHub API rate limits** for StatsBomb and SkillCorner (60 requests per hour without a token). Expected: honour `Retry-After` / `X-RateLimit-Reset`, show a visible "rate limited until <time>" state, and use `GITHUB_TOKEN` from the environment when it is set. Test: Task 14 `test_github_rate_limit_backoff_and_status`.
4. **A run that resumes after the machine slept past its lease expiry**, while another run took the lease. Expected: the resumed run detects the lost lease at its next catalog commit, or its `CURRENT` compare-and-swap fails; it stops loudly, and no published version or `CURRENT` of the new holder changes. Test: Task 7 `test_lost_lease_blocks_commit`, Task 9 `test_current_cas_rejects_stale_writer`.
5. **Non-ASCII names and paths with spaces** ("Lukáš Hrádecký", a lake root under `C:\Users\Karsten\My Lake`). Expected: names round-trip unchanged through bronze and dims, and canonical keys and file paths work on all three operating systems. Test: Task 3 `test_canonical_key_with_spaces_and_unicode`, Task 16 `test_idsse_names_unicode_roundtrip`.

## File structure

```
.github/workflows/v2-ci.yml        NEW root-level file (§23.4 CI); path filter v2/**; v1 workflows untouched
v2/
  AGENTS.md                        v2 class-1 rules (§23.2)
  pyproject.toml, uv.lock          package "lakehouse", tool configs, import-linter contracts
  reference/iris/                  temporary copy of the IRIS handover subtrees (§24); git-ignored, excluded from tooling; removed in Task 35
  src/lakehouse/
    config/       model.py, defaults.yaml, store.py
    ids/          native.py, keys.py, channels.py
    hashing.py    exact.py    errors.py    logging.py    telemetry.py    mask.py
    net/http.py
    storage/      base.py, local.py, hf.py, keys.py, roots.py, lease.py
    catalog/      working.py, publish.py, reader.py, manifest.py, restore.py, relocate.py, moves.py, cleanup.py
    bronze/       schemas.py, writer.py, tracking.py, listings.py
    raw/archive.py
    lineage/      model.py, precedence.py
    sources/      base.py, github.py, statsbomb_public.py, wyscout_public.py, idsse_public.py,
                  skillcorner_public.py, gradientsports_public.py, pining.py
    parsers/      base.py, inventory.py, statsbomb.py, wyscout.py, idsse.py, skillcorner.py, gradientsports.py
    runs/         run.py, progress.py, history.py
    resolve/      nway.py, players.py, teams.py, competitions.py, matches.py, bands.py, decisions.py, truthset.py
    services/     modules.py, filters.py, runs.py, review.py, schedule.py, settings.py
    schedule/     base.py, windows.py, launchd.py, systemd.py, cron.py, hf_jobs.py
    web/          app.py, auth_hf.py, auth_local.py, csrf.py
    ui/           shell.py, styles.css, pages/{modules,filters,runs,review,schedule,settings}.py
    cli.py
  dbt/            dbt_project.yml, profiles.yml.j2, models/{staging,gold}/..., tests/
  deploy/space/   Dockerfile, README.md (Space metadata)
  deploy/job/     Dockerfile
  tests/          unit/, contract/, e2e/, live/, hf/, bench/, os/, fixtures/
  docs/adrs/      ADR-001 … ADR-010
```

Test markers (registered in `pyproject.toml`): no marker = default suite; `live` = public network sources; `heavy` = downloads or parses a full tracking file (Mac Mini only, always together with `live` or `bench`); `hf` = real HF resources (Gate G-B), always with `hf_job` (runs inside an owner-launched HF Job with the Job secrets) or `hf_space` (runs inside an owner-launched HF Job that holds only the Space token); `bench` = production-scale benchmarks (Mac Mini); `windows`, `macos`, `linux` = OS-specific backends.

**The v2 check set** ("run the check set" below), run from `v2/`:

```bash
uv run ruff check . && uv run ruff format --check . && uv run pyright && uv run lint-imports && uv run pytest -m "not live and not hf and not bench" && uv run pip-audit
```

Expected: every command exits 0. pyright reports `0 errors`.

---

## Phase 0 — foundation

### Task 1: Project skeleton, tooling and the IRIS reference copy

**Files:**
- Create: `v2/pyproject.toml`, `v2/uv.lock`, `v2/AGENTS.md`, `v2/.gitignore`, `v2/src/lakehouse/__init__.py`, `v2/src/lakehouse/errors.py`, `v2/src/lakehouse/logging.py`, `v2/src/lakehouse/telemetry.py`, `v2/src/lakehouse/mask.py`, `v2/tests/unit/test_skeleton.py`, `.github/workflows/v2-ci.yml`
- Create: `v2/reference/iris/` (copy of three handover subtrees; see step 1)

**Interfaces:**
- Produces: `lakehouse.errors` with `LakehouseError` (base), `UnmappedFieldError`, `InexactValueError`, `UnexpectedPayloadError`, `LeaseLostError`, `ConflictError`, `MissingRootError`, `RootMismatchError`, `StaleSaveError`, `GuardStopError`, `IntegrityError`.
- Produces: `lakehouse.logging.install_secret_masking(env: Mapping[str, str]) -> None`. It masks, as `***`, the values of `HF_TOKEN`, `HF_S3_ACCESS_KEY_ID`, `HF_S3_SECRET_ACCESS_KEY`, `PINING_FOR_THE_DATA_TOKEN`, `PINING_LIST_TOKEN`, `GITHUB_TOKEN` and `OAUTH_CLIENT_SECRET` in every log record (a logging filter) and in everything written to `sys.stdout` and `sys.stderr` (a wrapper), so that DuckDB error texts that echo a `CREATE SECRET` statement are masked too. The CLI `main` (Task 26) and the web app (Task 27) call it first.
- Produces: the masking wrapper `python -m lakehouse.mask -- <command> [args...]`, which covers output that no in-process filter reaches (pytest's own report, captured output, child processes, C-level prints). It runs the command as a child process with piped stdout and stderr, masks every secret value line by line (keeping an overlap of the longest secret for a line without a newline), writes the result to its own stdout and stderr, and exits with the child's exit code. It forwards `SIGTERM` and `SIGINT` to the child and waits for it, so a cancelled HF Job still lets the run release its lease (Task 21). Every HF Job command runs under it (Task 33). Every type that holds a secret (`S3Credentials`, `PiningToken`, the HF token) stores it as pydantic `SecretStr`, so its `repr` is masked.
- Produces: `lakehouse.logging.get_logger(name: str) -> logging.Logger` (JSON lines on stdout; fields `ts`, `level`, `logger`, `msg`, plus extras such as `source`, `rows`, `elapsed_ms`).
- Produces: `lakehouse.telemetry.tracer` and `lakehouse.telemetry.meter` from `opentelemetry-api`, plus `SPAN_NAMES: Final[frozenset[str]]` and `ATTRIBUTE_NAMES: Final[frozenset[str]]` (the stable names of §21).

- [ ] **Step 1: Copy the IRIS handover subtrees into `v2/reference/iris/`.** Source: `C:\Users\Karsten\AppData\Local\Temp\claude\D--Development-karstenskyt--luxury-lakehouse-skunk\7c2a8737-2bc5-4d79-9198-78a3ac16b9d6\scratchpad\handover\iris-lakehouse-handover\`. Copy only the subtrees that §24 names: `src/` (it holds `storage_lab.py`), `dbt_project/` and `docs/notes/`. Do not copy `repo-history.bundle`, the PDF, `docker-compose.yml`, `config/` or `scripts/`. If the folder is gone, stop and ask the owner for the zip (§24). Add `reference/` to `v2/.gitignore` and to the ruff, pyright and pytest excludes.
- [ ] **Step 2: Write `test_skeleton.py`.** Assert: `test_mask_wrapper_covers_failing_pytest`: `lakehouse.mask` runs a sub-`pytest` on a deliberately failing test that asserts on, prints and logs (through a child logger) the value `hf_abc123`; the wrapper's output holds the failure report but no `hf_abc123`, and its exit code is the child's; `test_mask_wrapper_forwards_sigterm` (POSIX): a `SIGTERM` to the wrapper reaches the child, which exits through its handler; `test_secret_reprs_masked`: `repr(S3Credentials(...))`, `repr(PiningToken(...))` and `repr(HfStorage(...))` hold no secret (the last runs in Task 8, which creates `HfStorage`); `test_secret_values_masked`: after `install_secret_masking({"HF_TOKEN": "hf_abc123"})`, a log record, a `print` and a raised exception text that contain `hf_abc123` all reach the output as `***`; `lakehouse.__version__` is a string; every name in `lakehouse.errors.__all__` subclasses `LakehouseError`; `get_logger("x").info("m", extra={"rows": 3})` writes one line to stdout that `json.loads` parses, with `rows == 3`; every span name used anywhere under `src/lakehouse` (found with an AST scan of `tracer.start_as_current_span("...")` calls) is in `SPAN_NAMES`.
- [ ] **Step 3: Run the test.** Run: `cd v2 && uv run pytest tests/unit/test_skeleton.py -v`. Expected: FAIL (module not found).
- [ ] **Step 4: Write `pyproject.toml`.** Pin the tech stack, plus the dev tools (`pytest`, `pytest-benchmark`, `psutil`, `ruff`, `pyright`, `import-linter`, `pip-audit`). Add the ADR-083 override. Configure ruff (the global rule set), pyright (`strict`, `pythonVersion = "3.12"`, `include = ["src", "tests"]`), pytest (markers above, `testpaths = ["tests"]`), and import-linter contracts:
  - `lakehouse.ui` and `lakehouse.web` may import `lakehouse.services`, and nothing below it except `lakehouse.config.model`, `lakehouse.errors` and `lakehouse.logging`;
  - nothing under `lakehouse` except `lakehouse.ui` and `lakehouse.web` imports `taipy` or `flask`;
  - only `lakehouse.storage` imports `boto3`; only `lakehouse.storage` and `lakehouse.schedule.hf_jobs` import `huggingface_hub` (the HF Jobs API is not storage, so the scheduler adapter owns that one use);
  - `lakehouse.parsers` does not import `lakehouse.sources`, and `lakehouse.sources` does not import `lakehouse.parsers` (§6).
  Run `uv lock`.
- [ ] **Step 5: Write `errors.py`, `logging.py`, `telemetry.py` and `__init__.py`.** `telemetry.py` uses only `opentelemetry.trace.get_tracer` and `opentelemetry.metrics.get_meter`, with no SDK.
- [ ] **Step 6: Write `v2/AGENTS.md`.** Follow the pattern of `hf_taipy_app/AGENTS.md`. Class-1 rules only, each with a pointer to the spec section: Python 3.12; DuckDB 2.0.x and DuckLake 1.1; the write protocol (§10.3); the key formula (§14.1); completeness (§12); the two roots (§9); the security rules (§20); one commit per cycle with the owner's yes; v1 Databricks, Lakebase and Spark rules do not apply in `v2/`.
- [ ] **Step 7: Write `.github/workflows/v2-ci.yml`.** Trigger on `push` and `pull_request` with `paths: ["v2/**", ".github/workflows/v2-ci.yml"]`. Matrix `windows-latest`, `macos-latest`, `ubuntu-latest`, Python 3.12. Run the v2 check set. Do not touch the v1 workflows.
- [ ] **Step 8: Run the check set.** Expected: green. Also run `uv run pytest` at the repo root for v1 (its `testpaths`) in the background, because it takes longer than 30 s, and poll its progress. Confirm that it passes and that its collection does not reach `v2/`.

### Task 2: Identity — native ids, channel mapping and surrogate keys

**Files:**
- Create: `v2/src/lakehouse/ids/native.py`, `v2/src/lakehouse/ids/channels.py`, `v2/src/lakehouse/ids/keys.py`
- Create: `v2/tests/contract/test_format_contract.py`, `v2/tests/contract/test_key_parity.py`, `v2/tests/fixtures/keys/v1_parity.json`, `v2/tests/fixtures/channels/inventory.json`
- Reference: v1 `src/shared/identifiers.py`, `src/tests/test_format_contract.py`, `dbt_project/macros/generate_*_key.sql`; pining `docs/api-reference.md` and `src/canonical/models.py` (entry `id: str`, pattern `^[a-zA-Z0-9][a-zA-Z0-9_-]*$`)

**Interfaces:**
- Produces: `Provider = Literal["statsbomb", "wyscout", "idsse", "skillcorner", "gradientsports"]`, `Channel = Literal["public", "pining"]`, `PROVIDER_RANK: Final[tuple[Provider, ...]]` in `ids/native.py`.
- Produces: the v1 generators, copied with their v1 names and errors (for example `idsse_native_match_id(raw: str) -> str`, `statsbomb_native_match_id(raw: int) -> str`), plus `native_season_id(provider: Provider, native_competition_id: str, season: str) -> str`, which returns `f"{native_competition_id}:{season}"` and rejects `|` or `:` inside either part.
- Produces: `canonical_native_match_id(provider: Provider, channel: Channel, raw_id: str) -> str` in `ids/channels.py` (for example `("idsse", "pining", "DFL-MAT-J03WMX")` → `"J03WMX"`).
- Produces: `surrogate_key(provider: Provider, native_id: str | None) -> int | None` and `match_key(provider: Provider, native_match_id: str) -> int` (raises `ValueError` on an empty id) in `ids/keys.py`. The named keys `player_key`, `team_key`, `competition_key` and `season_key` call `surrogate_key`.

- [ ] **Step 1: Gate G-A, the v1 reference values.** Ask the owner for one of: (a) a read of known `(provider, native_id, key)` rows from the v1 Lakebase dims (`dim_matches`, `dim_players`, `dim_teams`, `dim_competitions`), or (b) approval to run Spark `xxhash64(concat_ws('|', ...))` once in a throwaway `pyspark` venv in the scratchpad. Record at least 3 values per entity, including a NULL native id for player, team and competition, in `v1_parity.json` with the source of each value.
- [ ] **Step 2: Inventory the channel id forms (§7.3).** For each provider that two channels deliver (`statsbomb`, `idsse`, `skillcorner`, `gradientsports`), record the id form of each channel with at least 3 sample ids of public entries and the evidence source in `inventory.json`. For a private entry, record only the id pattern, never an id value (§23.4: no private data in the repository). Sources:
  - pining: the live list endpoint with the public token, and with the owner token when `PINING_FOR_THE_DATA_TOKEN` is set in the environment (the session never prints it); `docs/api-reference.md`; `models.py`;
  - public: StatsBomb open-data `matches/<competition>/<season>.json` (`match_id`, an integer), HF `pysport/idsse-data` file names, SkillCorner `matches.json`, and the GS Drive file names.
  Where a pair can overlap (for example the pining `statsbomb` `redistributed` provenance, which is open-data 360), the pair gets a mapping contract. Where the evidence shows that a pair cannot overlap, the inventory records that evidence, and a test asserts the disjointness on the recorded samples. Wyscout has no second channel.
- [ ] **Step 3: Write the failing tests.**
  - `test_key_parity.py::test_v1_parity_values`: every fixture row satisfies `surrogate_key(p, n) == key`, and `None` gives `None`.
  - `::test_known_check_value`: `match_key("idsse", "J03WMX") == 4203055498971744044`.
  - `::test_match_key_rejects_empty`: `match_key("idsse", "")` raises `ValueError`.
  - `test_format_contract.py`: port every v1 format-contract case. Add `test_season_grain`: `native_season_id("statsbomb", c, "281")` for the five competitions that share season 281 in `competitions.json` gives five different `season_key` values. Add one channel test per provider pair:
    - `test_channel_mapping_idsse`: `("idsse", "pining", "DFL-MAT-J03WMX")` → `"J03WMX"` and `("idsse", "public", "J03WMX")` → `"J03WMX"`;
    - `test_channel_mapping_skillcorner_identity`: `"1899585"` → `"1899585"` for both channels;
    - `test_channel_mapping_statsbomb`: every recorded pining id and its open-data `match_id` integer map to the same canonical string (for example the integer `3788741` and the string `"3788741"` both give `"3788741"`);
    - `test_channel_mapping_gradientsports`: the recorded pining string ids and the public Drive ids map to one canonical form, or the disjointness assertion of step 2;
    - `test_inventory_covers_every_pair`: `inventory.json` has an entry for each of the 4 pairs, each with samples and an evidence source.
- [ ] **Step 4: Run them.** Run: `cd v2 && uv run pytest tests/contract/test_key_parity.py tests/contract/test_format_contract.py -v`. Expected: FAIL (import errors).
- [ ] **Step 5: Implement `ids/native.py`, `ids/channels.py` and `ids/keys.py`.** `surrogate_key` computes `xxhash.xxh64_intdigest(f"{provider}|{native}".encode("utf-8"), seed=42)` and converts it to signed 64 bit. Copy the v1 generators verbatim, with their docstrings, then add the season and channel functions.
- [ ] **Step 6: Run the tests and the check set.** Expected: PASS, and the check set is green.

### Task 3: Canonical object keys

**Files:**
- Create: `v2/src/lakehouse/storage/keys.py`, `v2/tests/unit/test_canonical_keys.py`

**Interfaces:**
- Produces: `Root = Literal["lake", "restricted"]`; `@dataclass(frozen=True) class CanonicalKey: root: Root; path: str` with `__str__` → `"lake:data/..."` and `CanonicalKey.parse(text: str) -> CanonicalKey`.
- Produces: `@dataclass(frozen=True) class RootMap: lake: StorageRootInfo; restricted: StorageRootInfo`, where `StorageRootInfo` holds `local_path: Path | None`, `hf_bucket: str | None` (`"<ns>/<bucket>"`) and `s3_bucket_path: str | None`.
- Produces: `canonical_key(ref: str, *, roots: RootMap, relative_to_data_path: bool) -> CanonicalKey`, which follows the mapping rules of §10.3.1 and raises `ValueError` for a reference under neither root.

- [ ] **Step 1: Write the failing tests.** One test per §10.3.1 rule:
  - `test_relative_native_path` → `lake:data/main/t/f.parquet`;
  - `test_hf_uri_lake` and `test_hf_uri_restricted`;
  - `test_s3_uri`;
  - `test_local_windows_path`, with backslashes and a drive letter, gives forward slashes;
  - `test_local_posix_path`;
  - `test_outside_roots_raises`;
  - `test_canonical_key_with_spaces_and_unicode` (Review focus 5): a root `C:\Users\Karsten\My Lake` and a file `data/bronze/Lukáš Hrádecký.parquet` round-trip through `str` and `parse`.
- [ ] **Step 2: Run them.** Expected: FAIL.
- [ ] **Step 3: Implement `canonical_key`.** Normalise with `PureWindowsPath` or `PurePosixPath` by the form of the reference, never by the host OS.
- [ ] **Step 4: Run the tests and the check set.** Expected: PASS.

### Task 4: Exactness to integer centimetres

**Files:**
- Create: `v2/src/lakehouse/exact.py`, `v2/tests/unit/test_exact.py`

**Interfaces:**
- Produces: `text_to_cm(text: str) -> int`, `float_to_cm(value: float, dtype: Literal["float32", "float64"]) -> int`, `column_to_cm(values: pa.Array, *, source: Literal["text", "float32", "float64"]) -> pa.Int32Array`. All raise `InexactValueError` naming the value when it has more than 2 decimals.
- Produces: `check_xml_attribute_decimals(path: Path, *, element: str, attributes: tuple[str, ...]) -> None`, a streaming check with `defusedxml` `iterparse` that raises `InexactValueError` with the line number (for the IDSSE `X`/`Y` attributes, §10.2).

- [ ] **Step 1: Write the failing tests** (the §23.6 "Exactness" and "IDSSE exactness" rows):
  - `test_text_two_decimals_ok`: `"12.34"` → `1234`;
  - `test_text_three_decimals_fails`: `"12.345"` raises;
  - `test_float32_shortest_text_ok`: `np.float32(12.34)` passes as `float32`;
  - `test_float64_three_decimals_fails`: `12.345` raises;
  - `test_negative_and_zero`: `"-0.01"` → `-1`, `"0"` → `0`;
  - `test_xml_attribute_rounding_trap`: a 3-line positions XML with `X="12.340049"` fails, although `round(12.340049, 4)` would pass a check on the output.
- [ ] **Step 2: Run them.** Expected: FAIL.
- [ ] **Step 3: Implement.** For floats, take the shortest round-trip text in the value's own dtype (`np.format_float_positional(np.float32(v), unique=True)` or `repr(float(v))`) and apply the text rule.
- [ ] **Step 4: Run the tests and the check set.** Expected: PASS.

### Task 5: Hashing and transport wrappers

**Files:**
- Create: `v2/src/lakehouse/hashing.py`, `v2/tests/unit/test_hashing.py`

**Interfaces:**
- Produces: `Wrapper = Literal["none", "gzip", "bz2"]`; `decode_transport(data: bytes) -> tuple[bytes, Wrapper]`, which detects by magic bytes only (`\x1f\x8b`, `BZh`) and enforces `MAX_DECODED_BYTES` (config, default 4 GiB); `content_sha256(data: bytes) -> str` (hash of the decoded bytes); `delivery_sha256(data: bytes) -> str` (hash of the bytes as delivered).

- [ ] **Step 1: Write the failing tests:**
  - `test_same_content_same_hash`: a payload and its gzip form give one `content_sha256` and two `delivery_sha256` (§23.6 "Same content, same hash");
  - `test_bz2_detected`;
  - `test_plain_passthrough`;
  - `test_truncated_gzip_raises` (Review focus 2): a gzip stream cut at 60 % raises `IntegrityError`;
  - `test_decompression_bomb_refused`: output above the limit raises.
- [ ] **Step 2: Run them.** Expected: FAIL.
- [ ] **Step 3: Implement.** Use streaming decompression with a byte counter.
- [ ] **Step 4: Run the tests and the check set.** Expected: PASS.

### Task 6: Config model, defaults and the config store

**Files:**
- Create: `v2/src/lakehouse/config/model.py`, `v2/src/lakehouse/config/defaults.yaml`, `v2/src/lakehouse/config/store.py`
- Create: `v2/tests/unit/test_config_model.py`, `v2/tests/unit/test_config_store.py`
- Depends on: Task 8 (storage interface). Implement the model first (steps 1–5), then the store after Task 8 (steps 6–9).

**Interfaces:**
- Produces (in `model.py`, pydantic v2, `frozen=True`):
  - `CompetitionClause(competition_id: str, seasons: list[str] | Literal["all"])` and `MatchClause(match_ids: list[str])`, with the alias `Clause = CompetitionClause | MatchClause`;
  - `ModuleConfig(module_id: ModuleId, active: bool, filter: list[Clause], statsbomb_360_only: bool = False)`, with a validator that refuses `statsbomb_360_only=True` for any module other than `statsbomb_public` and `statsbomb_pining` (§8.1, §29.13);
  - `ModuleId = Literal["statsbomb_public", "wyscout_public", "idsse_public", "skillcorner_public", "gradientsports_public", "idsse_pining", "statsbomb_pining", "skillcorner_pining", "gradientsports_pining"]`;
  - `ScheduleConfig(enabled: bool, cron: str | None)`;
  - `RetentionConfig(snapshot_days: int = 8, version_days: int = 7)`, with a validator that requires `snapshot_days > version_days` (§10.7);
  - `ResourceCaps(threads: int = 4, memory_limit: str = "3GB", arrow_cpus: int = 4)`;
  - `Settings(schema_version: Literal[1], modules: list[ModuleConfig], schedule: ScheduleConfig, retention: RetentionConfig, caps: ResourceCaps, raw_archive: bool, auto_accept_recommended: bool, sweep_max_fraction: float = 0.10, pining_base_url: str, truthset_owner: str | None = None)`. `truthset_owner` is the operator name that `validate_truthset` and `truthset_rows_from_decisions` accept as the owner (Task 22).
- Produces: `load_defaults(install_kind: Literal["laptop", "hf", "staging"]) -> Settings`.
- Produces (in `store.py`): `ConfigStore(storage: Storage)` with `load() -> tuple[Settings, int]`, `save(draft: Settings, *, expected_version: int, operator: str) -> int` (raises `StaleSaveError` naming the newer version and its saver), and `history(limit: int) -> list[ConfigVersionInfo]`, where `ConfigVersionInfo(version: int, saved_by: str, saved_at: datetime, changes: list[str])`. It locks with `threading.Lock` in-process, plus `filelock.FileLock` on local storage (§8.3). `ConfigLock` (the same lock) is exported for the other config-side stores (Task 22).
- Rule: clause ids go only into parameterised SQL and JSON values. They never form a storage key, a file path or SQL text.

- [ ] **Step 1: Write the failing model tests:**
  - `test_statsbomb_360_option_only_for_statsbomb` (§23.6 "StatsBomb 360 option"): `statsbomb_360_only=True` is accepted for both StatsBomb modules and raises for every other module;
  - `test_defaults_laptop` asserts the §8.2 table exactly: StatsBomb active with clauses `9/281`, `43/3`, `43/106` and `statsbomb_360_only` off; Wyscout active with one WC 2018 clause; IDSSE and SkillCorner active with "all"; GS inactive; all pining modules inactive; `auto_accept_recommended is True`; `raw_archive is False`; schedule off;
  - `test_defaults_hf_and_staging`: `raw_archive is True`, `auto_accept_recommended is False`, schedule off;
  - `test_retention_rule`: `snapshot_days=7, version_days=7` raises;
  - `test_unknown_schema_version_rejected`;
  - `test_clause_ids_validated`: an id with `'; drop` fails the §20 regex `^[A-Za-z0-9_:\-/.]+$`, and an id with a `..` part (`a/../b`, `..`) fails as well.
- [ ] **Step 2: Run them.** Expected: FAIL.
- [ ] **Step 3: Implement `model.py` and `defaults.yaml`.** In `defaults.yaml`, record each default id with its name as a comment. Task 14 and Task 15 confirm the Wyscout WC 2018 competition id against the live source.
- [ ] **Step 4: Run them.** Expected: PASS.
- [ ] **Step 5: Run the check set.**
- [ ] **Step 6 (after Task 8): Write the failing store tests:**
  - `test_stale_save_rejected` (§23.6): two threads save from version 1; exactly one gets version 2, and the other raises `StaleSaveError` whose message contains `"version 2"` and the first operator's name;
  - `test_history_keeps_previous`;
  - `test_cli_and_app_lock_locally`: two processes, through `multiprocessing`, against one local root, lose no save.
- [ ] **Step 7: Run them.** Expected: FAIL.
- [ ] **Step 8: Implement `store.py`.** Objects are `config/settings.json` and `config/history/<version>.json` in the lake root.
- [ ] **Step 9: Run the tests and the check set.** Expected: PASS.

## Phase 1 — storage and catalog

### Task 7: Lease and skip records

**Files:**
- Create: `v2/src/lakehouse/storage/lease.py`, `v2/tests/unit/test_lease.py`
- Depends on: Task 8.

**Interfaces:**
- Consumes: `Storage.put_if_absent`, `Storage.exists`, `Storage.stat`, `Storage.list`, `Storage.delete` and `Storage.put` (Task 8).
- Produces: `Lease.acquire(storage: Storage, *, holder_id: str, run_id: str) -> Lease | None`, `Lease.generation: int`, `Lease.started_storage_time: datetime` (the storage's own `modified` time of the new generation object, from `Storage.stat`; the sweep's age fence uses it, Task 12), `Lease.renew() -> None`, `Lease.assert_held() -> None` (raises `LeaseLostError`), `Lease.release() -> None`.
- Produces: `SkipRecord(run_id: str, reason: str, at: datetime, blocking_run_id: str | None)`; `write_skip_record(storage: Storage, *, run_id: str, reason: str, blocking_run_id: str | None) -> None` (to `runs/skipped/<run_id>.json` in the lake root); `fold_skip_records(storage: Storage) -> list[SkipRecord]` (reads them, and removes each one only after the caller has written it into run history, Task 21).
- Constants in `lease.py` (author's call; the lease is taken before the config is read, so these are not settings): `LEASE_TTL = 10 min`, `HEARTBEAT = 60 s`, `TAKEOVER_GRACE = 2 min` (tolerates clock skew between machines), `COMMIT_MARGIN = 60 s`.

**Mechanism (§10.6, closes the takeover race).** The lease is a chain of generation objects `runs/lease/<generation>.json` (12-digit zero-padded), each with holder id, run id, start, expiry, last heartbeat and a `released` flag. A generation object is only ever created with `put_if_absent`, so for each generation exactly one process can win:
- **Acquire:** list `runs/lease/` and read the highest generation N. If none exists, or N is released, or N's expiry plus `TAKEOVER_GRACE` has passed, create generation N+1 with `put_if_absent`. `False` means another taker won generation N+1: write a skip record and return `None`. `True` is followed by a fresh listing: if any generation above N+1 exists, the listing was stale; mark N+1 released, write a skip record and return `None`. Otherwise this run holds the lease.
- **Renew:** only the holder writes its own generation object (expiry and heartbeat), every `HEARTBEAT`, with a plain `put` (no conditional header).
- **`assert_held()`:** passes only if generation N+1 does not exist (an `exists` call, not a listing), the own object still names this run, and the holder's own clock shows at least `COMMIT_MARGIN` before expiry.
- **Release:** stop the heartbeat first, then write `released` with a plain `put`, so no late heartbeat can clear the flag. The next acquirer can take N+1 at once.
- **Pruning:** the holder deletes a generation only when it is not among the newest 20 and its expiry is more than 30 days old. A listing that old does not occur, and the post-create listing above catches one that does.
- A winner that crashes leaves only its own generation, which expires, so the chain never deadlocks.
- **Fencing beyond the lease.** A stale holder that misses all of these checks is still fenced. At the catalog, version objects are created with `put_if_absent`, and `CURRENT` changes only by compare-and-swap (Task 9). At the orphan sweep, only objects older than the run's `started_storage_time` are deleted, and `assert_held()` runs before the listing and before each delete batch (Task 12). So a stale holder never deletes a newer holder's files.

- [ ] **Step 1: Write the failing tests:**
  - `test_two_writers_one_wins` (§23.6 "Two writers", local): two processes call `acquire` together on an empty lake, and exactly one gets a lease;
  - `test_two_takers_one_wins`: generation 1 is expired; two processes, started together behind a `multiprocessing.Barrier`, call `acquire`; exactly one holds generation 2, and the other returns `None` and writes one skip record;
  - `test_released_lease_taken_at_once`;
  - `test_takeover_after_winner_crash`: the winner of generation 2 never renews; after expiry plus grace, a third run takes generation 3;
  - `test_lost_lease_blocks_commit` (Review focus 4): holder A's lease expires, holder B acquires it, then `A.assert_held()` raises `LeaseLostError`;
  - `test_commit_margin`: with less than `COMMIT_MARGIN` left on its own clock, `assert_held()` raises;
  - `test_release_stops_heartbeat_first`: a heartbeat scheduled during `release` never clears `released`;
  - `test_stale_listing_abandons`: a listing stub that hides generation 5 makes a taker create generation 3; the fresh listing shows 5, so the taker marks 3 released, writes a skip record and returns `None`;
  - `test_pruning_keeps_recent`: generations among the newest 20, or younger than 30 days, are never deleted;
  - `test_skip_record_written_and_folded` (§23.6 "Skip record"): a failed acquire writes exactly one skip object and no catalog object, and `fold_skip_records` returns it. Task 21 asserts the fold into run history.
- [ ] **Step 2: Run them.** Expected: FAIL.
- [ ] **Step 3: Implement `lease.py`** with the mechanism above. Inject the clock, so tests do not sleep.
- [ ] **Step 4: Run the tests and the check set.** Expected: PASS.

### Task 8: Storage interface — local and HF

**Files:**
- Create: `v2/src/lakehouse/storage/base.py`, `v2/src/lakehouse/storage/local.py`, `v2/src/lakehouse/storage/hf.py`, `v2/src/lakehouse/storage/roots.py`
- Create: `v2/tests/unit/test_storage_local.py`, `v2/tests/unit/test_storage_hf_mocked.py`, `v2/tests/unit/test_roots.py`, `v2/tests/hf/test_storage_hf_live.py`

**Interfaces:**
- Produces: `class Storage(Protocol)` with:
  - `put(root: Root, key: str, data: bytes | Path) -> None`;
  - `put_if_absent(root: Root, key: str, data: bytes | Path) -> bool` (the only owner of this primitive; Tasks 7 and 9 consume it);
  - `get(root: Root, key: str) -> bytes` and `get_versioned(root: Root, key: str) -> tuple[bytes, str]` (the bytes and an opaque version tag);
  - `replace_if_match(root: Root, key: str, data: bytes, *, expected_tag: str) -> str` (compare-and-swap; returns the new tag, raises `ConflictError` when the stored tag differs);
  - `list(root: Root, prefix: str) -> Iterator[ObjectInfo]`, `stat(root: Root, key: str) -> ObjectInfo`, `delete(root: Root, key: str, *, missing_ok: bool = False) -> None`, `exists(root: Root, key: str) -> bool`;
  - `read_uri(root: Root, key: str) -> str` (the URI that readers and registrations use), `duckdb_secrets() -> list[str]` (the `CREATE SECRET` statements a reader needs) and `roots: RootMap`.
- Produces: `ObjectInfo(key: str, size: int, modified: datetime)`; `LocalStorage(roots: RootMap)`; `HfStorage(roots: RootMap, *, hf_token: SecretStr, s3_key: S3Credentials | None)`. `ObjectInfo.modified` is always the storage's own time (file system `mtime`, or the S3 `LastModified`). Every `put` and `put_if_absent` gives the stored object a fresh time: a local put copies the bytes into a new temp file (never `shutil.copy2`, which keeps the source `mtime`), then renames it. Local: `put_if_absent` writes a temp file, then `os.link` to the final name, which fails when the name exists. On a file system without hard links (exFAT, FAT32, many SMB shares), `os.link` raises, and the fallback opens the final name with `O_CREAT | O_EXCL`, writes, `fsync`s and closes; a reader treats an unparsable lease generation as held until its `mtime` plus `LEASE_TTL` plus `TAKEOVER_GRACE`. The tag is the SHA-256 of the content; `replace_if_match` holds a `filelock.FileLock` on `<key>.lock` around the compare and the `os.replace`. `HfStorage` returns `hf://buckets/...` read URIs and routes writes by key:
  - through boto3 to `https://s3.hf.co` (path style, `us-east-1`), only when `s3_key` is set: data files under `data/`, raw files under `raw/`, and the **catalog-control objects** that need a conditional write: the lease generations `runs/lease/*` (create with `If-None-Match: *`; renew and release are plain PUTs of the holder's own object), the version and manifest objects `catalog/v/*` (`If-None-Match: *`, multipart included) and `catalog/CURRENT` (`If-Match: <ETag>`; the tag is the ETag, so the Job's `get_versioned` also reads through the gateway; every other HF read goes through `hf://`);
  - through `HfApi.batch_bucket_files`: every other object (config-side objects, progress, skip records, run results);
  - without `s3_key` (the Space), any write to a gateway-routed key raises `PermissionError`.
  - `list(root, prefix)` under `data/` or `raw/` goes through the gateway's `ListObjectsV2` with `Prefix`, so the Job can list a narrow file-name prefix such as `data/bronze/<table>/<match_key>-`; other listings use `HfApi`.
- Produces (in `roots.py`): `ResolvedRoots(roots: RootMap, source: Literal["cli", "env", "bootstrap"])` and `resolve_roots(*, cli_lake: str | None, cli_restricted: str | None, env: Mapping[str, str], bootstrap_file: Path) -> ResolvedRoots`. Rules (author's call; the roots cannot live in the settings, because the settings live in the lake root):
  - A root value is a local path or `hf://buckets/<ns>/<bucket>`. The gateway S3 paths are `LAKEHOUSE_LAKE_S3_PATH` and `LAKEHOUSE_RESTRICTED_S3_PATH`. They are required when the roots are on HF and gateway credentials are present (the Job), and ignored otherwise. No rule derives them from the `hf://` URI, because the mapping is verified only in Task 33 item 15.
  - **The roots resolve as a pair from one source.** The first source that names a lake root wins: the CLI flag `--lake`, then the environment variable `LAKEHOUSE_LAKE_ROOT`, then the bootstrap file. The restricted root comes only from that same source (`--restricted`, `LAKEHOUSE_RESTRICTED_ROOT`, or the bootstrap entry). A `--restricted` or `LAKEHOUSE_RESTRICTED_ROOT` without its lake raises `ValueError`.
  - If that source names no restricted root, a local lake gets the sibling `<lake>-restricted` (§10.5), and an HF lake raises `MissingRootError`.
  - The bootstrap file is `bootstrap.json` in the user config folder: `%APPDATA%\lakehouse\` on Windows, `~/Library/Application Support/lakehouse/` on macOS, `$XDG_CONFIG_HOME/lakehouse/` (default `~/.config/lakehouse/`) on Linux, unless the environment variable `LAKEHOUSE_CONFIG_DIR` names another folder. `lakehouse init` writes it.
  - No lake root from any source raises `MissingRootError("no lake root configured: run lakehouse init --lake <path>")`.
  - **Check against the catalog.** `WorkingCatalog.open` and `CatalogReader` compare the resolved roots (local paths, or HF bucket names; S3 paths excluded) with `CURRENT.storage_roots`, after normalising both sides with `normalise_root(value: str) -> str`: a local path becomes `Path(value).resolve()` (symlinks such as `/Volumes` aliases followed), with `/` separators and no trailing separator, case-folded on Windows and on macOS case-insensitive volumes; an `hf://buckets/<ns>/<bucket>` URI loses a trailing `/` only, and raise `RootMismatchError` naming both pairs on any difference. Only `relocate` and a restore across a relocation skip this check (§10.5.1, §10.8).
  - The Space and the Job get their roots as non-secret environment variables (Task 29).
- Produces: `open_storage(resolved: ResolvedRoots, env: Mapping[str, str]) -> Storage` (`env` gives the tokens and the S3 credentials). It raises `MissingRootError` when a resolved local root does not exist (§10.5). `init_lake(resolved: ResolvedRoots, *, bootstrap_file: Path) -> Storage` is the only function that creates roots.
- Produces: `S3Credentials(access_key_id: SecretStr, secret_access_key: SecretStr)`, read from the job secrets `HF_S3_ACCESS_KEY_ID` and `HF_S3_SECRET_ACCESS_KEY`. Every key passed to a `Storage` method is validated: it is relative, uses `/`, and has no `..` part. A bad key raises `ValueError` (§20).

- [ ] **Step 1: Write the failing tests:**
  - `test_missing_root_fails` (§23.6): `open_storage` on a missing folder raises `MissingRootError` and creates nothing;
  - `test_init_creates_both_roots`;
  - `test_put_get_list_delete_roundtrip` for both roots;
  - `test_put_if_absent_never_overwrites`: the second call returns `False` and the bytes stay those of the first call, for `bytes` and for a `Path`;
  - `test_replace_if_match_cas`: a stale tag raises `ConflictError` and leaves the object unchanged; two processes that read the same tag and replace together give exactly one success;
  - `test_delete_missing_ok_only_when_asked`;
  - `test_read_uri_forms`: local gives an absolute path, HF gives `hf://buckets/<ns>/<bucket>/<key>`;
  - `test_hf_routing` (mocked boto3 and `HfApi`): a `list` of `data/bronze/t/123-` calls `list_objects_v2` with that exact `Prefix`; `data/`, `raw/`, `runs/lease/`, `catalog/v/` and `catalog/CURRENT` go to boto3 with the conditional headers above; `config/` and `runs/progress/` go to `batch_bucket_files`; without `s3_key`, a write to `catalog/CURRENT` raises `PermissionError`;
  - `test_key_traversal_refused`: the keys `../x`, `/abs` and `C:\x` raise `ValueError`;
  - `test_put_if_absent_without_hard_links`: with `os.link` patched to raise `OSError`, the `O_EXCL` fallback still gives exactly one winner between two processes, and the second call returns `False`;
  - `test_roots.py`:
    - `test_roots_precedence` (CLI over environment over bootstrap, for the pair);
    - `test_roots_resolve_as_pair`: `--lake D:\LakeB` with a bootstrap that names lake A and `A-restricted` gives `LakeB` and `LakeB-restricted`, never `A-restricted`; the same holds with `LAKEHOUSE_LAKE_ROOT` alone;
    - `test_restricted_without_lake_refused`;
    - `test_normalise_root`: `C:\Lake`, `c:/lake/` and `C:\Lake\` compare equal on Windows; a symlinked path equals its target; two different local paths never compare equal;
    - `test_config_dir_override`: with `LAKEHOUSE_CONFIG_DIR` set, `init` writes and `resolve_roots` reads `bootstrap.json` only there;
    - `test_restricted_defaults_to_sibling_locally`; `test_hf_requires_explicit_restricted`; `test_job_requires_s3_paths`; `test_no_root_configured_fails`; `test_bootstrap_unicode_path_roundtrip` (Review focus 5).
- [ ] **Step 2: Run them.** Expected: FAIL.
- [ ] **Step 3: Implement `base.py`, `local.py`, `hf.py` and `roots.py`.** Local writes use a temp file plus `os.replace`. On Windows, retry a locked replace with backoff (at most 10 tries, then a loud failure, §10.4).
- [ ] **Step 4: Write `tests/hf/test_storage_hf_live.py`** (markers `hf` and `hf_job`; runs only in Task 33, inside an owner-launched HF Job). It covers a roundtrip on both buckets; `put_if_absent` racing, single-part and multipart; a lease renew and release as plain PUTs after an `If-None-Match` create; `replace_if_match` with a stale ETag (expects HTTP 412); whether an `s3://` write and its `hf://` read name the same object; and whether an `hf://` read sees a gateway write at once.
- [ ] **Step 5: Run the default tests and the check set.** Expected: PASS. The `hf` tests are not run here.
- [ ] **Step 6: Complete Task 6 steps 6–9 and Task 7.**

### Task 9: Working catalog, publication, manifests and readers

**Files:**
- Create: `v2/src/lakehouse/catalog/working.py`, `v2/src/lakehouse/catalog/publish.py`, `v2/src/lakehouse/catalog/manifest.py`, `v2/src/lakehouse/catalog/reader.py`
- Create: `v2/tests/unit/test_publish.py`, `v2/tests/unit/test_reader.py`, `v2/tests/e2e/test_read_during_run.py`

**Interfaces:**
- Produces: `WorkingCatalog.open(storage: Storage, settings: Settings, lease: Lease) -> WorkingCatalog`. It always builds the working copy from the version that `CURRENT` names, or creates a new catalog on an empty lake. `WorkingCatalog.conn: duckdb.DuckDBPyConnection` is attached as `lake`, with `data_path` set to the lake root's data path and with the caps applied. Every DuckDB connection of v2 (here, in `CatalogReader` and in the dbt profile) sets `extension_directory` from `LAKEHOUSE_DUCKDB_EXTENSION_DIR` when it is set (`test_extension_directory_from_env`). `WorkingCatalog.commit(kind: Literal["in_run", "run_end"]) -> VersionInfo`, `detach() -> Path` and `attach() -> None`. `commit` calls `lease.assert_held()` first (§10.6).
- Produces: `VersionInfo(seq: int, kind: Literal["in_run", "run_end"], key: CanonicalKey, sha256: str, size: int, published_at: datetime)`.
- Produces: the `CURRENT` JSON `{seq, version_key, size, sha256, parent_seq, lease_generation, storage_roots, self_checksum}`; `CurrentPointer` (the parsed JSON plus its storage tag); `read_current(storage: Storage) -> CurrentPointer` (checks `self_checksum`, retries 3 times, then raises `IntegrityError`).
- Produces: the manifest JSON `{seq, keys: list[str]}` of canonical keys (Task 3) at `catalog/v/<seq>.files.json`; `build_manifest(conn, roots: RootMap) -> list[CanonicalKey]` (from `ducklake_list_files` on every table).
- Produces: `publish_current(storage: Storage, pointer: CurrentPointer, *, expected: CurrentPointer | None) -> CurrentPointer`, the only writer of `CURRENT` (used by `commit` and by `restore`, Task 11). It calls `replace_if_match` with the tag of `expected`, or `put_if_absent` on an empty lake (`expected is None`). It turns `ConflictError` or a `False` into `LeaseLostError`.
- **Fencing rules (with Task 7, closes the two-writer race at the catalog).**
  - The first sequence of a working copy is `max(listed version sequences, CURRENT.seq) + 1`. Later commits add 1. So a leftover version from a crashed run is skipped, never reused.
  - Version and manifest objects are created with `put_if_absent`. On a `False`, the run calls `assert_held()`: a lost lease raises `LeaseLostError`; a held lease raises `ConflictError("version <seq> already exists: another writer created it")`. Either way the run stops. A published version is therefore never overwritten (§10.4).
  - `CURRENT` changes only through `publish_current`, with the pointer that this writer last read or wrote as `expected`. A changed `CURRENT` raises `LeaseLostError`, and the new version stays unreferenced until version retention deletes it (Task 12).
- Produces: `CatalogReader(storage: Storage)` with `session() -> ContextManager[duckdb.DuckDBPyConnection]`, which reuses one attachment per version and re-attaches only when `CURRENT` changes. It attaches with `(read_only, data_path '<lake data uri>', override_data_path true)`.

- [ ] **Step 1: Write the failing tests:**
  - `test_commit_publishes_version_manifest_and_current`: after `commit`, `catalog/v/1.ducklake`, `catalog/v/1.files.json` and `CURRENT` exist; the size and SHA-256 in `CURRENT` match the object;
  - `test_first_current_on_empty_lake_once`: two writers that both see an empty lake cannot both create `CURRENT`; the second raises `LeaseLostError`;
  - `test_current_self_checksum_detects_torn`: a corrupted `CURRENT` raises after 3 tries;
  - `test_working_copy_from_current_not_leftover`: a stale local working file is ignored;
  - `test_commit_requires_lease`: with a lost lease, `commit` raises `LeaseLostError` and publishes nothing;
  - `test_current_cas_rejects_stale_writer` (Review focus 4): two working copies open from the same `CURRENT`, both with a lease stub whose `assert_held` passes (the stale-holder case); the first commit publishes and moves `CURRENT`; the second raises `LeaseLostError`; `CURRENT` still names the first version, and that version's bytes and SHA-256 are unchanged;
  - `test_version_never_overwritten`: a crafted existing `catalog/v/<seq>.ducklake` makes `commit` raise `ConflictError` while the lease is held (and `LeaseLostError` when it is not), and the existing object is unchanged;
  - `test_roots_mismatch_with_current`: a lake whose `CURRENT.storage_roots` names other roots makes `WorkingCatalog.open` and `CatalogReader` raise `RootMismatchError`;
  - `test_leftover_version_skipped`: a leftover `catalog/v/<CURRENT.seq + 1>.ducklake` with no `CURRENT` change makes the next working copy publish `CURRENT.seq + 2`;
  - `test_published_copy_attaches_with_same_snapshots` (§26 item 3, bullet 1): the published version, attached read-only from storage, lists the same `ducklake_snapshots()` rows as the working copy before detach;
  - `test_reader_reuses_attachment`: two sessions on one version attach once (count attach calls);
  - `test_reader_reattaches_on_new_current`;
  - `test_reader_rerooted_native_files` (measured in `SPIKE-FINDINGS.md` §6): a DuckLake-written table reads all rows after the lake moves to a new local root.
  - `e2e/test_read_during_run.py` (§23.6 "Read during a run", "Reader across catalog commits"): process A holds the working copy and makes 5 catalog commits; process B runs `CatalogReader.session()` queries between and during them; every query returns either a consistent version's rows or raises loudly. It runs on all three OS in CI.
- [ ] **Step 2: Run them.** Expected: FAIL.
- [ ] **Step 3: Implement.** Commit order: `assert_held`, `CHECKPOINT`, detach, create the version with `put_if_absent`, create the manifest with `put_if_absent`, verify the full SHA-256 of the uploaded version, `assert_held` again, `publish_current`, re-attach (§10.3 step 5, §10.4).
- [ ] **Step 4: Run the tests and the check set.** Expected: PASS.
- [ ] **Step 5: Write ADR-001** (storage engine: DuckLake 1.1 + own Parquet, with the §25.2 numbers) and **ADR-007** (single-writer lease generations, immutable versions created with `put_if_absent`, the `CURRENT` compare-and-swap, catalog safety) in `v2/docs/adrs/`, in Nygard format and STE.

### Task 10: Tuned Parquet writer, bronze registration and root placement

**Files:**
- Create: `v2/src/lakehouse/bronze/schemas.py`, `v2/src/lakehouse/bronze/tracking.py`, `v2/src/lakehouse/bronze/writer.py`, `v2/src/lakehouse/bronze/listings.py`
- Create: `v2/tests/unit/test_tracking_layout.py`, `v2/tests/unit/test_bronze_writer.py`, `v2/tests/unit/test_listing_split.py`

**Interfaces:**
- Produces: `TRACKING_PLAYERS_CORE` and `TRACKING_FRAMES_CORE` (pyarrow schemas with the §11.5 core columns), and `BronzeTable = NewType("BronzeTable", str)` (names like `skillcorner_tracking_players`).
- Produces: `write_tracking_file(table: BronzeTable, data: pa.Table, *, match_key: int, content_version: str, out_dir: Path) -> Path`. It sorts `(period, player_key, frame)` for players and `(period, frame)` for frames, and applies the §10.2 encodings and zstd 3. File name: `<table>/<match_key>-<content_version>-<write_id>.parquet`, where `write_id` is a new `uuid4().hex` for every file write, a move copy included. So no object key is ever registered twice (§29.10).
- Produces: `write_bronze_file(...)` with the same contract for non-tracking per-match tables, with the audit columns `_ingested_at`, `_run_id`, `_source_hash` (§11.2).
- Produces: `WrittenFile(table: BronzeTable, root: Root, key: CanonicalKey, local_path: Path, rows: int, content_version: str)`.
- Produces: `place_root(visibility: Literal["public", "private"]) -> Root`.
- Produces: `split_listing(records: pa.Table, *, visibility_column: str | None, source_visibility: Literal["public", "private"]) -> dict[Root, pa.Table]` (§10.5 listings).
- Produces: `register_match_files(cat: WorkingCatalog, files: Sequence[WrittenFile]) -> None` and `replace_match_files(cat, table: BronzeTable, match_key: int, files: Sequence[WrittenFile]) -> None` (one transaction: `delete … where match_key = ?`, then `ducklake_add_data_files` with the reader URI). Each new table gets `auto_compact = false`.

- [ ] **Step 1: Write the failing tests:**
  - `test_tracking_encodings`: Parquet metadata of a written players file shows `DELTA_BINARY_PACKED` on `x_cm`, `y_cm` and `frame`, dictionary on `player_key`, and zstd level 3;
  - `test_tracking_sorted`;
  - `test_write_id_unique`: two writes of the same match, table and content give two different keys;
  - `test_rowcount_and_schema_checked`: a mismatched schema raises before any write;
  - `test_replace_drops_old_file`: after `replace_match_files`, `ducklake_list_files` names only the new file;
  - `test_auto_compact_off`;
  - `test_private_match_goes_to_restricted` and `test_public_match_goes_to_lake` (§23.6 "Restricted root placement");
  - `test_mixed_listing_split`: a players list with 3 public and 2 private records gives a lake table with exactly the 3 and a restricted table with exactly the 2;
  - `test_no_native_writer_for_bronze`: a guard function `assert_bronze_registered_only(cat)` fails when a bronze table has a file under the DuckLake `data_path` `main/` prefix.
- [ ] **Step 2: Run them.** Expected: FAIL.
- [ ] **Step 3: Implement the four modules.**
- [ ] **Step 4: Run the tests and the check set.** Expected: PASS. The production-scale parse and write benchmark needs the parsers, so Task 17 writes it (§23.4).

### Task 11: Restore, relocate and move records

**Files:**
- Create: `v2/src/lakehouse/catalog/restore.py`, `v2/src/lakehouse/catalog/relocate.py`, `v2/src/lakehouse/catalog/moves.py`
- Create: `v2/tests/unit/test_restore.py`, `v2/tests/unit/test_relocate.py`, `v2/tests/unit/test_moves.py`

**Interfaces:**
- Produces: `MoveRecord(match_key: int, direction: Literal["to_private", "to_public"], old_key: CanonicalKey, new_key: CanonicalKey | None, at: datetime, run_id: str)`; `record_move(cat: WorkingCatalog, rec: MoveRecord) -> None` (into table `lake.ops_move_records`, inside the move's catalog commit); `read_moves(conn) -> list[MoveRecord]`. A move in either direction writes one record per file (§29.10). `new_key` is `None` only for a `to_private` record of an object that only DuckLake still tracks (§29.11).
- Produces: `match_lake_objects(cat, storage, match_key: int) -> MatchObjects`, where `MatchObjects(registered: frozenset[CanonicalKey], kept: frozenset[CanonicalKey], tracked_only: frozenset[CanonicalKey], stray: frozenset[CanonicalKey])`: the match's lake-root bronze keys that the current catalog registers, that a kept version's manifest names, that only DuckLake still tracks (`ducklake_data_file` current or ended rows, `ducklake_files_scheduled_for_deletion`), and that only exist in storage (for example an upload of a crashed run), found by listing the narrow prefix `data/bronze/<table>/<match_key>-` of each bronze table. Bronze file names carry the match key (Task 10), so it filters manifests and DuckLake's tables by it.
- Produces: `move_match_files(cat, storage, match_key: int, to_root: Root) -> list[MoveRecord]`. **To private (per match, §29.11):** for every key in `registered | kept`, copy the object to the restricted root under a new write id and write a `to_private` record; for every key in `tracked_only | stray`, write a `to_private` record with `new_key=None`; register the new copies of the `registered` files, remove the old registrations, catalog commit, then delete every lake-root object of the set. **Raw files (§29.12):** the same move copies each lake-root raw file of the match's pining deliveries (from `ops_lineage`) to the restricted root with `RawArchive.copy_to` (Task 13) and writes a `to_private` record for its key; it deletes the lake-root raw object only if no public-channel lineage row has the same `content_sha256`. **To public:** copy the current restricted files to the lake root under new write ids, write `to_public` records, register, remove the old registrations, catalog commit; the old restricted copies stay for the normal cleanup (§9). Move-before-replace applies to `to_private` only: when a match changes content and becomes private in the same run, the run moves first and replaces after.
- Produces: `delete_moved_leftovers(storage: Storage, moves: Sequence[MoveRecord], *, current_manifest: frozenset[CanonicalKey]) -> int`. It runs at the start of every run, acts only on `to_private` records, and accepts "not found". It skips a raw key (`raw/<content_sha256>.zst`) whose content a public-channel lineage row has, because that object is public at its source; a raw record is pruned once its object is deleted or skipped for that reason. Before any delete it calls `assert_no_moved_key_registered(moves, current_manifest)`, which raises `GuardStopError` and deletes nothing if any `to_private` old key is in the `CURRENT` manifest. Task 12 calls the same guard before its kept-set subtraction and its sweep.
- Produces: `RestoreRecord(operator: str, at: datetime, restored_seq: int, new_seq: int, dropped_seqs: list[int], visibility_fixes: int)`, stored in table `ops_restores`, and `restore(storage: Storage, *, target_seq: int, operator: str) -> RestoreRecord`. Steps:
  1. Take the lease and read every move record from the pre-restore `CURRENT`.
  2. Copy the target version, and rewrite paths if the storage roots differ (relocate mechanism).
  3. Give every match its latest visibility and keep the restored content (§29.11). The latest visibility comes from the pre-restore `CURRENT`. For each match, two passes over the files that the copy names: (a) every lake-root file that a `to_private` record names as its old key is replaced by that record's restricted copy, whatever the match's latest visibility (a `new_key=None` record raises `GuardStopError` and publishes nothing); (b) the match is placed by its latest visibility: now private → every file must now be in the restricted root, else `GuardStopError`; now public → each restricted file is copied to the lake root under a new write id, registered, and a `to_public` record is written. Then set the match's visibility flag.
  4. Write all move records into the copy.
  5. Write the restore record into the copy, with `new_seq` and `dropped_seqs` = every version with `target_seq < seq < new_seq`.
  6. Check that every file the copy names exists in storage (`exists` per manifest key); a missing file raises `IntegrityError` and publishes nothing. Then publish the copy as version `new_seq` with `put_if_absent` and `publish_current` (Task 9).
  7. Delete the dropped version objects. If a crash stops this step, `prune_versions` still treats every `dropped_seqs` entry of `ops_restores` as not kept (Task 12).
- Produces: `relocate(storage: Storage, *, old_roots: RootMap, new_roots: RootMap) -> VersionInfo`. It opens a copy of the working catalog as a plain DuckDB database, refuses any DuckLake format version other than `1.1`, and updates `ducklake_data_file.path` (and `ducklake_delete_file.path`) in one transaction (§10.5.1).

- [ ] **Step 1: Write the failing tests** (the §23.6 rows "Catalog restore", "Restore across a relocation", "Lake relocation", "Visibility change moves files"):
  - `test_restore_takes_lease_and_records`;
  - `test_restore_drops_newer_versions`;
  - `test_next_run_starts_from_restored`;
  - `test_relocate_reads_by_value`: after the move and `relocate`, `select sum(x_cm)` succeeds on every table (not only `count(*)`);
  - `test_relocate_refuses_other_format_version`;
  - `test_restore_across_relocate`;
  - `test_public_to_private_move_deletes_lake_copy`: right after the move's catalog commit, the lake object is gone and a move record exists;
  - `test_private_public_private_roundtrip` (§23.6 "Visibility change moves files", "and back"): a match moves private to public, then public to private; after each move its files are only in the root that its visibility selects, and `select sum(x_cm)` on its tracking tables succeeds by value;
  - `test_two_successive_restores_keep_match_private`: restore to version N−1, then to N−2; both give a private match readable only from the restricted root, and both copies carry every move record;
  - `test_crash_between_commit_and_delete`: simulate a crash after the commit; `delete_moved_leftovers` at the next run's start removes the copy;
  - `test_move_back_gets_new_key` (§29.10): public → private → public with unchanged content; the final lake-root key differs from the `to_private` record's old key, and both moves have records;
  - `test_restore_across_both_moves_latest_visibility` (§29.10): a restore to the version before the first move gives a public match whose files are in the lake root and read by value;
  - `test_moved_key_registered_guard`: a crafted `to_private` record whose old key is in the `CURRENT` manifest makes `delete_moved_leftovers` raise `GuardStopError`, and no object is deleted;
  - `test_replace_then_private_later_run` (§29.11, the A-SPEC-48 / B26 case): run k replaces the content (`Lx` → `Ly`, a kept version still names `Lx`); run k+1 makes the match private; right after that commit, neither `Lx` nor `Ly` exists in the lake root, and both have `to_private` records with restricted copies; a restore to the version before run k gives a private match whose run-k−1 content (`Lx`'s bytes) reads by value from the restricted root only;
  - `test_tracked_only_object_gets_record`: a lake-root object of the match that only DuckLake still tracks is deleted by the move and has a `to_private` record with `new_key=None`;
  - `test_restore_private_to_public_copies_from_restricted`: a match that was private in the target version and is public now gets a lake-root copy of the target version's restricted file, with the target version's content;
  - `test_change_and_private_same_run` (§29.10): a match changes content and becomes private in one run; right after the commit, no lake-root object of the match exists; a restore to the version before that run gives a private match whose old content reads by value from the restricted root;
  - `test_restore_resolves_deleted_lake_key_when_public_again` (§29.12): public → private → public; a restore to the version before the first move names no deleted file: the deleted lake key is resolved to its restricted copy, then copied back to the lake root, and the match reads by value;
  - `test_restore_checks_every_named_file_exists`: a crafted copy that names a missing file raises `IntegrityError`, and `CURRENT` is unchanged;
  - `test_stray_upload_of_newly_private_match_deleted` (§29.12): an unregistered upload of the match under `data/bronze/<table>/<match_key>-` is deleted by the private move and gets a `new_key=None` record;
  - `test_private_move_archive_off_no_raw` (§29.12): with the raw archive off, a private move writes no raw record and makes no raw call;
  - `test_private_move_moves_pining_raw` (§29.12): with the raw archive on, a pining delivery's lake-root raw file is copied to `restricted:raw/`, recorded, and deleted from the lake root when the match becomes private;
  - `test_shared_raw_object_stays` (§29.12): a raw object whose content a public-channel delivery also has gets a restricted copy but stays in the lake root, and the next run-start delete leaves it there;
  - `test_restore_record_in_copy`: the published restore copy holds the `ops_restores` row, and `dropped_seqs` is exactly the versions between the target and the copy.
  The cleanup halves of the "Catalog restore" and "Visibility change moves files" rows are in Task 12, because they need `run_cleanup`.
- [ ] **Step 2: Run them.** Expected: FAIL.
- [ ] **Step 3: Implement the three modules.**
- [ ] **Step 4: Run the tests and the check set.** Expected: PASS.

### Task 12: Cleanup — retention, guarded DuckLake cleanup and the orphan sweep

**Files:**
- Create: `v2/src/lakehouse/catalog/cleanup.py`, `v2/tests/unit/test_cleanup.py`, `v2/tests/e2e/test_cleanup_multirun.py`

**Interfaces:**
- Consumes: manifests (Task 9), move records and `assert_no_moved_key_registered` (Task 11), `Storage.list` (Task 8), `Lease.started_storage_time` and `Lease.assert_held` (Task 7).
- Produces: `prune_versions(storage: Storage, retention: RetentionConfig, *, dropped: frozenset[int], now: datetime) -> list[int]` (run-end versions for 7 days; in-run versions until superseded and never longer than 7 days, except `CURRENT`; every sequence in `dropped`, the union of `ops_restores.dropped_seqs`, is not kept).
- Produces: `kept_file_set(storage: Storage, moves: Sequence[MoveRecord]) -> frozenset[CanonicalKey]`: the union of kept manifests, in which each `to_private` old key is replaced by its record's new key, or removed when `new_key` is `None` (§29.10, §29.11). Before it builds the set, it calls `assert_no_moved_key_registered` (Task 11).
- Produces: `run_cleanup(cat: WorkingCatalog, storage: Storage, *, settings: Settings, lease: Lease, allow_large_sweep: bool, now: datetime) -> CleanupReport`, where `CleanupReport(expired_snapshots: int, pruned_versions: list[int], ducklake_deleted: list[CanonicalKey], swept: list[CanonicalKey], moved_not_found: list[CanonicalKey])`. It runs §10.3 steps 6–7: expire snapshots older than `snapshot_days`; check DuckLake's dry-run list against the kept set (any overlap raises `GuardStopError`, deleting nothing); delete the listed files, accepting "not found" only for keys that a `to_private` move record names; run the non-dry cleanup; sweep `data/` of both roots with the two fail-closed guards. It never lists `raw/`, `catalog/`, `config/` or `runs/`.
- **The sweep skips DuckLake's files (§10.3 step 7, §29.11).** It never deletes an object whose key DuckLake still tracks: any path in the working catalog's `ducklake_data_file` (current or ended rows) or `ducklake_files_scheduled_for_deletion`, mapped through `canonical_key`. DuckLake deletes those itself in the DuckLake-list part, so that delete never meets "not found" for a key the sweep removed first.
- **Sweep age fence (§10.3 step 7, "older than the run's start").** The sweep deletes an unlisted object only when its `ObjectInfo.modified` is older than `lease.started_storage_time`. Both times come from the storage clock, so no skew tolerance is needed. This keeps the current run's own files that no manifest names yet (DuckLake-written dims, links and ops files before the run-end commit), and every file of a newer holder. `lease.assert_held()` runs before the listing and before each delete batch.
- Produces: `prune_move_records(conn, storage) -> int`. It prunes a record only when no kept version names its old key **and** DuckLake no longer tracks that key at all: no `ducklake_data_file` row (current or ended) and no `ducklake_files_scheduled_for_deletion` row. A key that has not yet reached the scheduled-deletion list is still tracked, so its record stays. Raw-file records (`raw/` keys, which DuckLake never tracks) are excluded: only `delete_moved_leftovers` prunes them, after their lake-root object is deleted or skipped (Task 11, §10.8).

- [ ] **Step 1: Write the failing tests:**
  - `test_guard_stops_on_kept_file`: a crafted dry-run list with a kept key raises `GuardStopError`, and no object is deleted;
  - `test_sweep_removes_unlisted_orphan`;
  - `test_sweep_skips_ducklake_tracked` (§29.11): a replaced file that no kept manifest names any more (day 7) but that DuckLake still tracks survives the sweep; on day 8 DuckLake's list names it and its delete succeeds;
  - `test_sweep_keeps_young_unlisted` (§10.3 step 7): an unlisted object whose storage `modified` is newer than the run's `started_storage_time` survives the sweep; so does a move copy that `put` made from an old local file (its `modified` is fresh, not the source's);
  - `test_stale_holder_sweep_spares_new_holder_files`: run A starts, its lease expires, run B takes the next generation and uploads unlisted data files; A then runs its sweep: `assert_held()` raises and nothing is deleted; with `assert_held` stubbed to pass, the age fence alone still keeps every file of B;
  - `test_moved_key_registered_guard_in_cleanup`: a `to_private` old key in the `CURRENT` manifest stops cleanup with `GuardStopError` before any delete;
  - `test_dropped_versions_not_kept`: versions named in `ops_restores.dropped_seqs` that a crash left in storage are pruned, and their files are swept;
  - `test_sweep_never_touches_other_prefixes`: objects under `raw/`, `catalog/`, `config/` and `runs/` survive;
  - `test_sweep_all_path_forms_local` (§23.6 "Sweep with all path forms", local half): the roots map an `hf://` bucket and an `s3://` path onto local folders; the `CURRENT` manifest names one DuckLake-written relative file, one `hf://`-registered file and one `s3://`-form file, all present; the sweep deletes only an unlisted orphan and none of the three;
  - `test_sweep_aborts_on_broken_mapping` (§23.6 "Sweep with all path forms"): with a monkeypatched `canonical_key` that drops the root prefix, the `CURRENT`-manifest check aborts before any delete;
  - `test_sweep_aborts_above_ten_percent`, then succeeds with `allow_large_sweep=True`;
  - `test_sweep_after_restore` (§23.6 "Catalog restore", last sentence): after a restore to the version before the last run, the next sweep deletes the undone run's files and keeps every file that a kept version's manifest names;
  - `test_moved_key_not_found_accepted` and `test_other_missing_key_fails`;
  - `test_unreferenced_version_pruned`: a version left by a failed `CURRENT` compare-and-swap is never named by `CURRENT` and is deleted by version retention like an in-run version of a crashed run;
  - `test_raw_record_survives_failed_delete` (§10.8): the private move's raw delete fails without stopping the run; that run's cleanup keeps the raw record; the next run start deletes the lake-root raw object and only then prunes the record;
  - `test_move_record_survives_daily_runs`: after a move on day 0, daily runs on days 1–10; the record (and a `new_key=None` record) survives every run until DuckLake's cleanup has removed the key, and no cleanup meets an unexempted "not found";
  - `test_move_record_not_pruned_early`: with no run for 10 days, the record survives until DuckLake's cleanup removed the key;
  - `test_version_cap_after_crash` (§23.6 "Crashed-run version cap");
  - `e2e/test_cleanup_multirun.py`:
    - `test_linear_multirun_cleanup` (§23.6 "Linear multi-run cleanup"): a replace on day 0, then runs on simulated days 1–10 (inject `now`); every cleanup succeeds, the sweep never deletes the replaced file while DuckLake still tracks it, and DuckLake's own cleanup deletes it after it leaves both retentions;
    - `test_private_to_public_with_replace_then_restore` (§29.11, the A P-21 case): a private match becomes public and changes content in one run; runs continue with sweeps on simulated days 1–3; a restore to the version before that run names only existing files, and every table reads by value;
    - `test_public_private_public_survives_runs` (§29.10, the A-SPEC-47 / B14 case): public → private → public with unchanged content; then a full run start, a cleanup and a sweep on simulated days 1–10; every file that `CURRENT` names exists and reads by value, and no guard stops;
    - `test_move_flow_cleanup` (§23.6 "Visibility change moves files", cleanup clauses): a public-to-private move on day 0; the next cleanup succeeds without a guard stop; runs continue on simulated days 1–10; on the day that DuckLake's dry-run list first names the moved key, cleanup accepts its "not found"; a second, unrelated key deleted by hand makes that cleanup fail loudly.
- [ ] **Step 2: Run them.** Expected: FAIL.
- [ ] **Step 3: Implement `cleanup.py`.**
- [ ] **Step 4: Run the tests and the check set.** Expected: PASS.

### Task 13: Raw archive and lineage

**Files:**
- Create: `v2/src/lakehouse/raw/archive.py`, `v2/src/lakehouse/lineage/model.py`, `v2/src/lakehouse/lineage/precedence.py`
- Create: `v2/tests/unit/test_raw_archive.py`, `v2/tests/unit/test_precedence.py`

**Interfaces:**
- Produces: `Delivery(provider: Provider, channel: Channel, native_match_id: str, artifact: str, raw: bytes, decoded: bytes, wrapper: Wrapper, delivery_sha256: str, content_sha256: str, visibility: Literal["public", "private"], change_signal: str | None, fetched_at: datetime)`.
- Produces: `RawArchive(storage: Storage, *, enabled: bool)` with `store(d: Delivery) -> CanonicalKey | None`. It writes `raw/<content_sha256>.zst` to the root of the delivery's own visibility and never overwrites. `copy_to(content_sha256: str, root: Root) -> CanonicalKey | None` copies an archived object to the other root with `put_if_absent`, and returns `None` without writing when no archived object exists (raw archive off, or a delivery that was never archived); Task 11 uses it for pining raw files, existing objects only (§29.12). `locate(content_sha256: str) -> CanonicalKey | None` looks in the restricted root first, then in the lake root; the object is named by its content hash, so its bytes are the same wherever it is found. `read(content_sha256: str) -> bytes` uses `locate` and raises `IntegrityError` when neither root holds it. Offline reparse (Task 26) reads only through `read`.
- Produces: the lineage row schema `ops_lineage` (match key, channel, channel id, change signal, `delivery_sha256`, `content_sha256`, wrapper, first seen, last listed, visibility, kept, reason).
- Produces: `CopyInfo(channel: Channel, visibility: Literal["public", "private"], content_sha256: str, nonnull_fields: Mapping[str, bool])`; `extra_fields(pining_nonnull: Mapping[str, bool], public_fields: Set[str]) -> frozenset[str]`; `choose_copy(public: CopyInfo | None, pining: CopyInfo | None) -> Choice`, where `Choice(kept: Channel, visibility: Literal["public", "private"], reason: str)`. The visibility is the kept copy's own flag (§9, §29.2).

- [ ] **Step 1: Write the failing tests** (the §23.6 rows "Precedence is order-independent", "Kept-copy visibility", "Raw-file visibility", "Same content, same hash"):
  - `test_order_independent`: every permutation of arrival and update gives the same `Choice`;
  - `test_public_pining_with_extras_stays_public`;
  - `test_private_pining_with_extras_private`;
  - `test_no_extras_public_wins`;
  - `test_locate_prefers_restricted`: after `copy_to(..., "restricted")` and the lake-root delete, `locate` returns the restricted key and `read` returns the same bytes;
  - `test_copy_to_without_archived_object`: with the archive off, `copy_to` returns `None` and writes nothing;
  - `test_raw_file_keeps_delivery_flag`: a private delivery stored while the public copy wins is under `restricted:raw/`;
  - `test_same_content_one_raw_object`;
  - `test_archive_off_stores_nothing`.
- [ ] **Step 2: Run them.** Expected: FAIL.
- [ ] **Step 3: Implement.**
- [ ] **Step 4: Run the tests and the check set.** Expected: PASS.
- [ ] **Step 5: Write ADR-005** (raw archive) **and ADR-006** (visibility, channel precedence, the restricted root and moves).

## Phase 2 — sources and parsers

### Task 14: HTTP client and the source adapter contract

**Files:**
- Create: `v2/src/lakehouse/net/http.py`, `v2/src/lakehouse/sources/base.py`, `v2/src/lakehouse/sources/github.py`, `v2/src/lakehouse/parsers/base.py`, `v2/src/lakehouse/parsers/inventory.py`
- Create: `v2/tests/unit/test_http.py`, `v2/tests/unit/test_github.py`

**Interfaces:**
- Produces: `HttpClient(session_factory=requests.Session)` with `get(url: str, *, headers: Mapping[str, str] = {}, max_bytes: int, expect: Literal["binary", "json", "text"]) -> HttpResult`, where `HttpResult(status: int, body: bytes, headers: Mapping[str, str], final_url: str)`. Rules:
  - reject `http://`; `verify=True`; timeouts `(10, 30)`; at most 3 retries with exponential backoff on 429 and 5xx;
  - `stream=True` with raw reads, and no transparent `Content-Encoding` decoding;
  - redirects followed manually, and `Authorization` dropped on any cross-host redirect;
  - `expect` checks the payload kind: an HTML body where `binary` or `json` is expected raises `UnexpectedPayloadError`.
- Produces: `TreeEntry(path: str, blob_sha: str, size: int, lfs_oid: str | None, lfs_size: int | None)` and `GitHubClient(http: HttpClient, token: str | None)` with `tree(repo: str, ref: str) -> list[TreeEntry]`, `raw(repo: str, ref: str, path: str) -> bytes` and `lfs_object(repo: str, oid: str, size: int) -> bytes`. It honours `Retry-After` and `X-RateLimit-Reset`, and exposes `rate_limited_until: datetime | None`.
- Produces: `SourceEntry(module_id: ModuleId, native_match_id: str, competition_id: str | None, competition_name: str | None, season_id: str | None, season_name: str | None, visibility: Literal["public", "private"], artifacts: Mapping[str, ArtifactRef], listed_at: datetime, has_360: bool | None = None)` (`has_360` is set by the StatsBomb adapters only, §29.13) and `ArtifactRef(key: str, change_signal: str | None, expected_sha256: str | None, size: int | None)`.
- Produces: `class ChannelAdapter(Protocol)`: `module_id`, `provider`, `channel`; `list_entries() -> Iterator[SourceEntry]`; `preview() -> PreviewResult`; `fetch(entry: SourceEntry, artifact: str) -> Delivery`.
- Produces: `class ProviderParser(Protocol)`: `provider`; `parse(entry: SourceEntry, deliveries: Mapping[str, Delivery], *, run_id: str) -> ParsedMatch`; `field_inventory(artifact: str, decoded: bytes) -> frozenset[str]`; `bronze_columns(artifact: str) -> frozenset[str]`. Here `ParsedMatch(tables: Mapping[BronzeTable, pa.Table], coordinate_system: CoordinateSystem, names: NamesBundle)`, `CoordinateSystem(origin: Literal["centre", "top_left", "bottom_left"], units: Literal["m", "cm", "yd", "pct"], pitch_length: float | None, pitch_width: float | None, x_direction: str, y_direction: str, play_direction: str | None, source_ref: str)` and `NamesBundle(players: pa.Table, teams: pa.Table, competitions: pa.Table)` (native ids with names, for the dims). An unmapped field raises `UnmappedFieldError(artifact, field_paths)`.
- Produces: `PreviewResult(tournaments: list[PreviewTournament], fetched_at: datetime, notes: list[str])` and `PreviewTournament(competition_id, competition_name, season_id, season_name, match_count: int | None, count_note: str | None, match_count_360: int | None = None, share_360_note: str | None = None, competition_available_360: datetime | None = None)`. `competition_available_360` is the competition-season's `match_available_360` from StatsBomb `competitions.json` (public module only; the preview shows "360 data since <date>", or "no 360 data for this season" when it is null). The display text of the 360 share is `"<n> matches · <m> with 360 (<p> %)"`, with `p` rounded to a whole percent, or the `share_360_note` ("360 share not available" and why) when a count is unknown; never a guessed number.

- [ ] **Step 1: Write the failing tests:**
  - `test_rejects_http`; `test_timeouts_set`;
  - `test_retry_429_then_ok`; `test_retry_gives_up_after_3`;
  - `test_auth_dropped_on_cross_host_redirect` (the pining 302 to S3, §7.2);
  - `test_no_content_encoding_decode`: a gzip body with `Content-Encoding: gzip` is returned compressed;
  - `test_html_payload_rejected` (Review focus 1);
  - `test_truncated_body_never_returned` (Review focus 2): a body shorter than its `Content-Length` retries, then raises `IntegrityError`;
  - `test_github_rate_limit_backoff_and_status` (Review focus 3): a 403 with `X-RateLimit-Remaining: 0` and a reset time sets `rate_limited_until` and raises a typed error that the run reports visibly;
  - `test_github_token_used_when_set`.
- [ ] **Step 2: Run them.** Expected: FAIL.
- [ ] **Step 3: Implement the five modules.**
- [ ] **Step 4: Run the tests and the check set.** Expected: PASS.

### Tasks 15–19: Public adapters and provider parsers

Each of Tasks 15–19 has the same shape. The table gives the provider-specific facts; the steps follow it.

| Task | Provider | Adapter file | Parser file | Source (§7.1) | Change signal | Fixtures in the repo |
|---|---|---|---|---|---|---|
| 15 | StatsBomb (+360) | `sources/statsbomb_public.py` | `parsers/statsbomb.py` | GitHub `statsbomb/open-data`: competitions, matches, events, lineups, three-sixty | tree blob SHA | synthetic only (`NOASSERTION`) |
| 16 | IDSSE | `sources/idsse_public.py` | `parsers/idsse.py` | HF `pysport/idsse-data`: metadata XML, events XML, positions XML | LFS sha256 | small real slice, CC-BY-4.0, with attribution |
| 17 | SkillCorner | `sources/skillcorner_public.py` | `parsers/skillcorner.py` | GitHub `SkillCorner/opendata`: match.json, tracking JSONL (LFS), dynamic events, phases | LFS `oid sha256` | small real slice, MIT, with attribution |
| 18 | Gradient Sports | `sources/gradientsports_public.py` | `parsers/gradientsports.py` | Drive folder `1_a_q1e9CXeEPJ3GdCv_3-rNO3gPqacfa`: tracking `.jsonl.bz2`, events in dated drop folders, metadata, rosters | Drive id + day, then `content_sha256` after download | synthetic only (no licence) |
| 19 | Wyscout | `sources/wyscout_public.py` | `parsers/wyscout.py` | figshare collection 4415000: events, matches, players, teams, competitions | md5 + article version | small real slice, CC BY 4.0, with attribution |

Common files for each task: `tests/contract/test_<provider>_inventory.py`, `tests/unit/test_<provider>_parser.py`, `tests/live/test_<provider>_live.py` and `tests/fixtures/<provider>/`.

Steps for each of Tasks 15–19:

- [ ] **Step 1: Reuse audit.** Read the IRIS (`v2/reference/iris/src/iris_lake/ingest/`, `providers/`) and v1 (`src/ingestion/`) code for this provider. List every raw field from a real sample (Task 15 reads the live open-data repo; Task 18 reads the live Drive folder) and record the list in the test module as `EXPECTED_FIELDS`. Note every field that IRIS or v1 drops (§12.6).
- [ ] **Step 2: Write the failing tests:**
  - `test_field_inventory_complete` (contract): for each artifact, `field_inventory(artifact, fixture) <= bronze_columns(artifact)`;
  - `test_unmapped_field_fails_loudly`: a fixture with one extra key raises `UnmappedFieldError` naming it;
  - `test_types_and_exactness`: coordinates become `int32` cm through Task 4 functions, and timestamps round-trip;
  - `test_listing_entries`: `list_entries()` on a recorded listing gives the expected native ids, competition and season, visibility `public`, and change signals;
  - `test_preview_counts`: the preview gives one tournament per competition-season, with counts where cheap and `count_note` otherwise;
  - `test_coordinate_system_<provider>` (§11.4): the parser's `CoordinateSystem` for each artifact kind equals the values in the provider's documentation or data, and `source_ref` names that source;
  - `test_live_inventory` (marker `live`): the same inventory test on a live sample. A live test that downloads or parses a full tracking file also carries the marker `heavy` and runs only on the Mac Mini.
  Provider-specific tests:
  - **Task 15:**
    - `test_has_360_from_tree` (§29.13): `has_360` is `True` exactly for the fixture matches with a `three-sixty/<match_id>.json` tree entry;
    - `test_360_status_mismatch_warns`: a match whose `match_status_360` disagrees with the tree gives a visible preview note and a run warning row;
    - `test_preview_360_share` (§23.6 "StatsBomb 360 option"): for a recorded competition-season, `match_count_360` and the display text equal the fixture count ("64 matches · 64 with 360 (100 %)"); a match list that cannot be fetched gives `share_360_note` and no number;
    - `test_preview_competition_available_360` (§29.13): a recorded `competitions.json` gives `competition_available_360` for Bundesliga 2023/24 and WC 2022, and `None` with the text "no 360 data for this season" for WC 2018 (§8.2);
    - `test_match_lists_cached_by_blob_sha`: a second preview with unchanged tree blob SHAs downloads no match list;
    - `test_three_sixty_frames_kept`; `test_season_281_five_competitions` (season keys, Task 2); `test_defaults_ids_match_live` (marker `live`): `9/281`, `43/3` and `43/106` exist with the expected names.
  - **Task 16:**
    - `test_metadata_season_and_competition`: J03WPY gives `SeasonId="DFL-SEA-0001K6"`, `Season="2022/2023"` and `CompetitionId="DFL-COM-000002"`;
    - `test_idsse_names_unicode_roundtrip` (Review focus 5);
    - `test_t_equals_ball_t_proof`: dropping `ball_t` is allowed only after this test proves `t == ball_t` on every row of the fixture (§10.2);
    - `test_xml_attribute_exactness` (Task 4 streaming check on the positions file);
    - `test_coordinate_system_idsse` (the common test, with these values): tracking is centre-origin metres, events are pitch-origin (§11.4);
    - `test_list_all_seven_competitions` (marker `live`): record the competition and season of all 7 matches.
  - **Task 17:**
    - `test_possession_and_image_corners_kept`;
    - `test_prematch_frames_kept`;
    - `test_lfs_pointer_signal`;
    - `test_tracking_players_frames_split` (the §11.5 core columns);
    - `bench/test_bench_tracking.py` (markers `bench` and `heavy`; §23.4, §10.9, §26 item 13): pytest-benchmark of parse plus write of one full IDSSE match (J03WPY, 3.2 M player rows) and one full SkillCorner match (1899585). A session fixture fetches the raw files with the v2 adapters into a cache folder (`LAKEHOUSE_BENCH_CACHE`), so the bench needs no spike data. It records MB per match, the parse time and the peak RSS of each parse (sampled in a child process with `psutil`), and fails above the budget that Task 30 records in `v2/tests/bench/budget.json`. A `--bench-record` option (in `tests/bench/conftest.py`) measures and writes the budget instead of checking it. It runs only on the Mac Mini (Task 30).
  - **Task 18:**
    - `test_drive_confirm_page_followed_or_rejected` (Review focus 1): the virus-scan page is followed once, or rejected with `UnexpectedPayloadError`; never parsed as data;
    - `test_bz2_wrapper_recorded`;
    - `test_content_hash_decides_reparse`;
    - `test_drive_folder_listing` (marker `live`): records the file count and total size (§26 item 8).
  - **Task 19:** `test_wc2018_competition_id_live` (marker `live`): confirms the Wyscout WC 2018 id used in `defaults.yaml`.
- [ ] **Step 3: Run them.** Run: `cd v2 && uv run pytest tests/contract/test_<provider>_inventory.py tests/unit/test_<provider>_parser.py -v`. Expected: FAIL.
- [ ] **Step 4: Implement the adapter and the parser.** Adapters use `HttpClient` or `GitHubClient` only, map ids with `canonical_native_match_id` (Task 2), and fill `ArtifactRef.change_signal`. Parsers write every field as a typed column, add `_ingested_at`, `_run_id` and `_source_hash` to non-tracking tables, and fill `CoordinateSystem` from provider documentation or data only (§11.4). IDSSE tracking goes through `silly_kicks.providers.sportec.parse_dfl_tracking`; names come from the metadata XML (§12.7).
- [ ] **Step 5: Document every bronze column** in `v2/dbt/models/staging/<provider>/_<provider>__sources.yml` (§12.8).
- [ ] **Step 6: Run the tests and the check set.** Expected: PASS.

### Task 20: Pining channel adapter (four modules)

**Files:**
- Create: `v2/src/lakehouse/sources/pining.py`, `v2/tests/unit/test_pining.py`, `v2/tests/contract/test_channel_equivalence.py`, `v2/tests/live/test_pining_live.py`, `v2/tests/fixtures/pining/`

**Interfaces:**
- Produces: `PiningAdapter(provider: Provider, *, http: HttpClient, base_url: str, token: PiningToken)`, where `PiningToken(value: SecretStr, tier: Literal["public", "list", "owner"])`. `token_from_env(env) -> PiningToken` reads `PINING_FOR_THE_DATA_TOKEN` for the owner tier, falls back to the public token `test-token-pining-for-the-data` (add a `# pragma: allowlist secret` comment), and reads a separate variable `PINING_LIST_TOKEN` for the list tier.
- Consumes: Task 14 contracts. The run (Task 21) hands pining deliveries to the same Tasks 15–19 parsers as public deliveries (§6); `lakehouse.sources` never imports `lakehouse.parsers` (Task 1 contract). Only the tests import both.

- [ ] **Step 1: Write the failing tests:**
  - `test_updated_since_format`: the query value matches `^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}Z$`, and a local non-UTC clock still gives UTC (Review focus: clock handling);
  - `test_bare_artifact_key_used`;
  - `test_visibility_from_entry_flag` (§23.6 "Visibility comes from the source flag"): an owner token with a public-flagged entry gives `public`; no code path reads the token tier to set visibility (an AST check: no reference to `PiningToken.tier` inside visibility code);
  - `test_public_token_nothing_private` (same row): with the public token and a recorded public-tier listing, every entry, every stored bronze file and every preview tournament is public; private pining tournaments appear only as the "not listed here: needs the list-only token" marker;
  - `test_artifact_meta_skip` (§23.6 "Pining artifact hash"): equal hash means no download; a changed hash downloads; received bytes with a different SHA-256 raise `IntegrityError`; no `artifact_meta` falls back to download and `content_sha256`;
  - `test_noop_updated_at_no_reparse` (§23.6);
  - `test_competition_season_fields_used_and_missing_group` (§29.1): a missing field puts the entry in the "competition not given by source" group;
  - `test_pining_statsbomb_has_360_from_artifacts` (§29.13): a `statsbomb` entry with a `freeze_frames` artifact gives `has_360=True`, one without it `False`, and the preview counts the share from the one list call;
  - `test_list_fields_equal_metadata` (§23.6 "Pining list fields", contract): for every pining fixture match, the list entry's competition and season equal the values that the provider parser reads from the metadata artifact. The run applies the same check to every ingested pining match. A mismatch does not fail the match: the match commits with the metadata values (the artifact is the source of truth for the match), and the run writes a visible warning row (`kind = "list_metadata_mismatch"`) that the Runs page shows (§19.5, author's call);
  - `test_gs_season_name_only`: `season_name="2022"` with no `season_id` gives the season key from `<competition>:2022`;
  - `test_401_retry_during_rotation`;
  - `test_list_token_cannot_fetch_artifact` (behaviour expected from the list tier: a 404 on fetch is reported as "artifact not available with this token").
  - `test_channel_equivalence.py` (§23.6 "Channel equivalence", §12.5): for every overlapping pair of the Task 2 inventory (`statsbomb`, `idsse`, `skillcorner`, `gradientsports`), a public fixture and a pining fixture of the same match give the same native ids, the same `content_sha256` where the files are equal, and identical canonical bronze; a pair that the inventory proves disjoint has a test that asserts the recorded evidence; for SkillCorner, JSONL and `format_version` 2 Parquet give identical bronze.
  - `test_pining_live.py` (marker `live`): public tier counts 0/7/20/0 or more; with the public token, every listed entry is public, and a fetch of a private entry returns 404 (the private id is read at test time from an owner-token listing when `PINING_FOR_THE_DATA_TOKEN` is set, and is never written to a file; without that variable, this check skips with a visible reason); the new fields and `artifact_meta` exist (§7.2; if they are absent, the test skips with a visible reason and the fallbacks apply); `test_list_fields_equal_metadata_live` runs the contract over every ingested pining match of the test lake.
- [ ] **Step 2: Run them.** Expected: FAIL.
- [ ] **Step 3: Implement `pining.py`.**
- [ ] **Step 4: Run the tests and the check set.** Expected: PASS.

## Phase 3 — runs, resolution and dbt

### Task 21: Run orchestration, progress and run history

**Files:**
- Create: `v2/src/lakehouse/runs/run.py`, `v2/src/lakehouse/runs/progress.py`, `v2/src/lakehouse/runs/history.py`
- Create: `v2/tests/unit/test_run_steps.py`, `v2/tests/e2e/test_run_local.py`, `v2/tests/e2e/test_crash_recovery.py`

**Interfaces:**
- Produces: `Trigger = Literal["schedule", "button", "cli"]`; `run_ingestion(*, storage: Storage, trigger: Trigger, adapters: Mapping[ModuleId, ChannelAdapter], parsers: Mapping[Provider, ProviderParser], now: Callable[[], datetime] = utcnow) -> RunResult`. Here `RunResult(run_id: str, status: Literal["ok", "failed", "skipped"], counts: RunCounts, errors: list[RunError])`.
- Steps, in order (§17.2): take the lease, or write a skip record and return `skipped`; fold skip records into run history; `delete_moved_leftovers` with the `CURRENT` manifest; read the saved config once; list every inactive pining module that holds ingested matches for visibility only (one list call, no fetch) and apply the move of Task 11 to each of its matches whose flag changed (§9, §17.2, §29.14). A pining module that holds ingested matches, active or inactive, is always listed in full, without `updatedSince`, so a visibility flip is never missed whatever pining does with `updated_at`; change detection still uses `artifact_meta.sha256` (§17.3). A listing that fails (missing or expired token, 5xx after the retries) writes a visible warning row (`kind = "visibility_check_failed"`, with the module name), and the run continues and ends with status `failed`, never a silent skip; for each active module with a non-empty filter: list, filter (with `statsbomb_360_only` on, drop every entry whose `has_360` is not `True` before any fetch, §29.13), classify against lineage (a lineage match that the filter now excludes is `excluded`: kept, its content not updated, counted; its visibility still follows the listed flag, so a flag change applies the move of Task 11 with no download, §29.14), fetch, archive, parse; for a match that becomes private, apply the move first (Task 11, per match); for a match that becomes public, write the new content to the lake root and then move the remaining restricted files; then write, register, then make an in-run catalog commit per match and delete the lake-root copies of any `to_private` move; resolution (Task 22); the dbt hand-off (Task 23); cleanup (Task 12); the run-end commit; release the lease.
- Produces: the progress object `runs/progress/<run_id>.json` and the run-history tables `ops_runs`, `ops_module_runs`, `ops_match_steps` and `ops_errors` (including unmapped fields), with stable ids (§17.4). Exit code: 0 for `ok`, 1 for `failed`, 3 for `skipped`. `RunCounts(added: int, changed: int, unchanged: int, failed: int, skipped: int, excluded: int)` and `RunError(module_id, native_match_id, kind, message)`.
- Produces: the match metadata table `ops_matches` (§11.3: keys, natural keys, visibility, kept channel and reason, last listed per source, coordinate-system reference, `has_360_frames` for StatsBomb matches (true exactly when the match has rows in its 360 bronze table, the same source as `dim_matches`; `SourceEntry.has_360` is only a cross-check, and a mismatch writes a warning row `kind = "has_360_mismatch"`, §29.13), and the file registry with `_ingested_at`, `_run_id`, `_source_hash` and provider per file), the coordinate-system table `bronze_coordinate_systems` (§11.4, from `ParsedMatch.coordinate_system`) and the per-match roster tables that map `player_key` to the native id (§11.2).
- Every step is wrapped in `tracer.start_as_current_span(<name in SPAN_NAMES>)` and logs JSON with `source`, `rows` and `elapsed_ms` (§21). The run copies dbt `target/run_results.json` to `runs/dbt/<run_id>/run_results.json`.

- [ ] **Step 1: Write the failing tests:**
  - `test_empty_filter_downloads_nothing` (§23.6): no adapter `fetch` call, and a warning row;
  - `test_ops_matches_has_360_frames` (§11.3, §29.13): `ops_matches.has_360_frames` and `dim_matches.has_360_frames` both equal the presence of 360 bronze rows for every StatsBomb match, and are null for other providers; a match whose `SourceEntry.has_360` disagrees with its bronze gives a `has_360_mismatch` warning row;
  - `test_360_only_skips_download` (§23.6 "StatsBomb 360 option"): with `statsbomb_360_only` on, no `fetch` call happens for a match without 360 data, events included;
  - `test_excluded_match_turning_private_moves` (§23.6 "StatsBomb 360 option", §29.14): an ingested public pining match is excluded by a narrowed filter, then the listing flags it private; the next run makes no `fetch` call for it, moves it, and leaves no lake-root object of the match (bronze or pining raw);
  - `test_visibility_listing_is_full`: every pining listing of a module that holds ingested matches is sent without `updatedSince`; a flip whose entry kept its old `updated_at` is still moved;
  - `test_visibility_listing_failure_visible`: a 401 or 5xx on the visibility-only listing gives a `visibility_check_failed` row naming the module, the other modules still run, and the run ends `failed` with exit code 1;
  - `test_excluded_match_turning_public_moves` and `test_inactive_pining_module_public_flip` (the private-to-public direction): an excluded, and an inactive-module, private pining match that the listing flags public gets its files copied to the lake root with no `fetch` call;
  - `test_inactive_pining_module_visibility_only` (§29.14): a pining module with ingested matches is switched inactive; the next run lists it once, makes no `fetch` call, and moves the one match whose flag turned private; an inactive pining module with no ingested matches is not listed; a direct public module is never listed for this;
  - `test_narrowed_filter_keeps_data` (§8.1, §29.13): after a clause is removed, the excluded match's bronze, dims and links stay, the next run neither updates nor deletes them, and run history counts them as `excluded`;
  - `test_unmapped_field_fails_match_continues_run`: the match fails, the other matches commit, the status is `failed` and the exit code is 1 (§12.3);
  - `test_saved_config_only`: a draft saved after the run starts is not seen;
  - `test_progress_object_updates`;
  - `test_trigger_recorded`;
  - `test_skip_records_folded_into_history` (§23.6 "Skip record", last clause): after a skipped run, the next lease holder writes one `ops_runs` row with status `skipped`, the skip run id and its reason, and only then removes `runs/skipped/<run_id>.json`;
  - `e2e/test_run_local.py`: a full run on fixtures through a local lake gives bronze, links and dims;
  - `test_sigterm_releases_lease`: a `SIGTERM` during a match stops the run at the next safe point (no half-written catalog commit), marks the run `failed` with reason `terminated`, and releases the lease;
  - `e2e/test_crash_recovery.py` (§23.6 "Crash loses at most one match"): kill the run process during match 3 of 5; matches 1–2 are committed, and the next run re-does only match 3 onward.
- [ ] **Step 2: Run them.** Expected: FAIL.
- [ ] **Step 3: Implement the three modules.**
- [ ] **Step 4: Run the tests and the check set.** Expected: PASS.
- [ ] **Step 5: Write ADR-009** (the OTel API seam: the names in `SPAN_NAMES` and `ATTRIBUTE_NAMES`).

### Task 22: Entity resolution, decisions and the review-queue logic

**Files:**
- Create: `v2/src/lakehouse/resolve/nway.py`, `players.py`, `teams.py`, `competitions.py`, `matches.py`, `bands.py`, `decisions.py`, `truthset.py`
- Create: `v2/tests/unit/test_nway.py`, `test_resolve_players.py`, `test_resolve_teams.py`, `test_resolve_matches.py`, `test_decisions.py`, `test_truthset.py`, `v2/tests/live/test_resolution_quality.py`, `v2/tests/fixtures/truthset/` (ids only)
- Reference: `v2/reference/iris/src/iris_lake/analytics/`

**Interfaces:**
- Produces: `Entity = Literal["player", "team", "competition", "match"]` and `RecordRef(provider: Provider, native_id: str)`.
- Produces: the decision objects under `config/decisions/` and the request objects under `config/requests/`:
  - `MustLink(entity: Entity, a: RecordRef, b: RecordRef, decided_by: str, decided_at: datetime, reason: str)` and `CannotLink` with the same fields;
  - `AcceptRequest(request_id: str, entity: Entity | None, batch_filter: str | None, requested_by: str, requested_at: datetime)` and `UndoRequest(request_id: str, batch_id: str, requested_by: str, requested_at: datetime)` (an undo also creates holds);
  - `DecisionSet(must_links: list[MustLink], cannot_links: list[CannotLink], accept_requests: list[AcceptRequest], undo_requests: list[UndoRequest])`, read once per run;
  - `DecisionStore(storage: Storage)` with `load() -> DecisionSet`, `add(item: MustLink | CannotLink | AcceptRequest | UndoRequest) -> None`, `export(path: Path) -> int` and `import_(path: Path) -> int` (counts); it uses `ConfigLock` (Task 6). `applied_requests(conn) -> set[str]` reads table `ops_applied_requests`.
- Produces: `resolve_all(conn, *, decisions: DecisionSet, bands: Mapping[Entity, Bands | None], settings: Settings, run_id: str) -> ResolutionResult`, where `ResolutionResult(links_written: Mapping[Entity, int], queued: Mapping[Entity, int], accepted_batches: list[str], pointer_moves: int)`. It writes the link tables `gold_links_<entity>` (confidence, layer, method, accepted_by, batch_id) and the pointer-move log `ops_pointer_moves`.
- Produces: `Bands(auto_link_min: float, reject_max: float, margin: float)`; `load_bands(conn, entity) -> Bands | None` (measured values only; `None` means no automatic link).
- Produces: `TruthRow(provider_a, native_id_a, provider_b, native_id_b, label, labelled_by, labelled_at, evidence_ref)`; `validate_truthset(rows) -> None` (rejects a row without the owner as `labelled_by`); `measure(conn, rows, entity) -> QualityReport(precision, recall, auto_band_precision)`.
- Produces: `truthset_rows_from_decisions(decisions: DecisionSet, *, owner: str) -> list[TruthRow]` (§15.5 "Hand-checked decisions also grow the truth set"). It takes only must-link and cannot-link decisions whose `decided_by` is the owner, with `evidence_ref = "decision:<id>"`. It never takes a link accepted as recommended (§15.6).

- [ ] **Step 1: Write the failing tests:**
  - `test_nway_same_source_guard`; `test_canonical_pointer_rank` (the §14.3 rank);
  - `test_pointer_move_logged` (§23.6): one log row with old value, new value, run id and reason;
  - `test_teams_all_pairs` (more than StatsBomb ↔ Wyscout);
  - `test_matches_by_date_and_teams`;
  - `test_no_margin_no_auto_link` (§15.4);
  - `test_order_of_application` (§15.6: hand decisions, then undo, then accept, then the setting);
  - `test_undo_holds_pairs` (§23.6 "Accept recommended": the run after an undo does not re-accept with the setting on);
  - `test_tie_stays_in_queue`; `test_cannot_link_blocks_accept`;
  - `test_run_never_writes_request_objects` and `test_operator_save_during_batch` (§23.6 "Operator save during a run batch");
  - `test_truthset_purity` (§23.6): a recommended link never enters the truth set, and a row without an owner `labelled_by` fails;
  - `test_hand_decisions_grow_truthset` (§15.5): an owner must-link and an owner cannot-link become two truth rows; a decision by another operator and a batch accepted as recommended become none;
  - `test_resolution_quality` (marker `live`): measures per entity against the truth set; it skips with a visible reason until Task 34 creates the labels.
- [ ] **Step 2: Run them.** Expected: FAIL.
- [ ] **Step 3: Port and extend the resolver.** Port the IRIS resolver, and extend teams to every pair plus competitions and matches.
- [ ] **Step 4: Run the tests and the check set.** Expected: PASS.
- [ ] **Step 5: Write ADR-003** (Kimball keys carried over), **ADR-004** (the canonical pointer rule and the move log) **and ADR-010** (the quality gate and the review queue; the target values; the bands to be filled after Task 34).

### Task 23: dbt project and the hand-off

**Files:**
- Create: `v2/dbt/dbt_project.yml`, `v2/dbt/profiles.yml.j2`, `v2/dbt/models/staging/**`, `v2/dbt/models/gold/{dim_competitions,dim_seasons,dim_teams,dim_players,dim_matches}.sql`, `v2/dbt/models/gold/tracking_cross_provider.sql`, `v2/dbt/models/gold/_gold__models.yml` (contracts)
- Create: `v2/src/lakehouse/runs/dbt.py`, `v2/tests/unit/test_dbt_materialisation.py`, `v2/tests/e2e/test_dbt_handoff.py`
- Reference: v1 `dbt_project/models/marts/dim_*.sql`, `int_player_xref.sql`, `int_team_xref.sql`; IRIS `dbt_project/`

**Interfaces:**
- Produces: `run_dbt(cat: WorkingCatalog, *, run_id: str, select: str | None = None) -> DbtResult`, where `DbtResult(success: bool, results: list[DbtNodeResult], run_results_key: str)` and `DbtNodeResult(unique_id: str, status: str, message: str | None)`. It detaches the working copy, renders `profiles.yml` with the DuckLake attach and the caps, runs `dbtRunner().invoke(["build", ...])`, copies `run_results.json`, and re-attaches.
- Rules: staging and every row-level model are `view`. Only `dim_*`, link, lineage and ops models may be `table`. Every gold model has `contract: {enforced: true}`. Every dimension keeps `(provider, native_*_id)` and the canonical pointer. dbt never hashes.

- [ ] **Step 1: Write the failing tests:**
  - `test_row_level_models_are_views` (§23.6 "Row-level models are views"): parse `target/manifest.json` and fail if any model outside the allowlist `{dim_*, *_links, ops_*, lineage_*}` is `table` or `incremental`; a test-only extra table model makes it fail;
  - `test_gold_contracts_enforced`;
  - `test_dims_keep_natural_keys`;
  - `test_dim_matches_has_360_frames` (§11.3, §29.13): `dim_matches.has_360_frames` is `true` exactly for StatsBomb matches with 360 bronze rows, and null for other providers;
  - `test_v1_dim_parity_shape`: the columns of `dim_players` include `player_key`, `provider`, `native_player_id` and `canonical_player_key`, and exclude `canonical_player_id`;
  - `e2e/test_dbt_handoff.py` (§23.6 "dbt hand-off"): dbt builds during a run, then the run continues and commits.
  - `e2e/test_no_private_rows_in_lake_native_files` (§23.6 "Restricted root placement", last clause): a full fixture run with one private match; every DuckLake-written file in the lake root (from `ducklake_list_files` on every table, and from the storage listing of `data/main/`) belongs to a table of the allowlist `{dim_*, *_links, ops_*, lineage_*}`; and no such file, read with `read_parquet`, has a row-level column (the §11.5 core names such as `frame`, `x_cm` and `y_cm`, or an event or lineup payload column) on a row with the private match's `match_key`.
- [ ] **Step 2: Run them.** Expected: FAIL.
- [ ] **Step 3: Write the dbt project and `runs/dbt.py`.** Port the v1 dimension and pointer logic to DuckDB. The cross-provider tracking view joins the per-provider players tables on the §11.5 core.
- [ ] **Step 4: Run the tests and the check set.** Expected: PASS.

## Phase 4 — scheduling, services, CLI and UI

### Task 24: Scheduling backends

**Files:**
- Create: `v2/src/lakehouse/schedule/{base,windows,launchd,systemd,cron,hf_jobs}.py`, `v2/tests/unit/test_schedule_render.py`, `v2/tests/os/test_schedule_{windows,macos,linux}.py`

**Interfaces:**
- Produces: `ScheduleStatus(backend: Literal["windows", "launchd", "systemd", "cron", "hf_jobs"], installed: bool, enabled: bool, cron: str | None, next_run: datetime | None, last_result: str | None)`.
- System mode runs as the operator user, never as root (§20 least privilege): the LaunchDaemon plist sets `UserName` and `GroupName`, the systemd system unit sets `User=` and `Group=`, and the Task Scheduler task runs as the operator account with "run whether user is logged on or not" and the `S4U` logon type, so no password is stored (outbound HTTPS still works). `install` with `system=True` only writes and loads the task definition; it never opens the lake, so a root process creates no file there. Installing a system-level task needs admin rights, so the owner runs that one command with `sudo` (Task 32); the session never handles a password.
- Produces: `class ScheduleBackend(Protocol)` with `install(cron: str, *, system: bool, env: Mapping[str, str], run_as: str | None = None) -> None` (`run_as` is required when `system=True`; idempotent; writes the given non-secret variables into the task definition, for example the roots and the `Development`-volume cache variables of Task 30; a key that looks like a secret, such as `*_TOKEN`, `*_SECRET*` or `*_KEY*`, raises `ValueError`), `uninstall() -> None` and `status() -> ScheduleStatus`; `backend_for_host() -> ScheduleBackend` (Windows: `schtasks`; macOS: a launchd plist as a LaunchAgent, or a LaunchDaemon with `system=True`; Linux: a systemd timer for the user or the system, or cron when systemd is missing). Commands run through `subprocess.run([...], shell=False, check=True)`.
- Produces: `HfJobsSchedule(api: HfApi, scheduled_job_id: str)` with `reschedule(cron: str) -> None`, `suspend() -> None`, `resume() -> None`, `trigger() -> str` (the new job id) and `status() -> ScheduleStatus`. It never creates a job and never sends a secret (§18, §29.5). The scheduled job id comes from the Space variable `LAKEHOUSE_HF_SCHEDULED_JOB_ID` (Task 33).

- [ ] **Step 1: Write the failing tests:**
  - `test_render_*` (pure, all OS): the rendered task XML, plist, timer unit and crontab line contain the CLI path and the cron schedule;
  - `test_install_idempotent` (mocked `subprocess.run`);
  - `test_render_env_no_secrets`: the rendered task definitions carry the given variables, and a secret-like key raises before anything is written;
  - `test_render_system_runs_as_user`: the system-mode plist has `UserName`/`GroupName`, the system unit has `User=`/`Group=`, and the Windows task names the account with `<LogonType>S4U</LogonType>`; `system=True` without `run_as` raises; `install(system=True)` touches no path under the lake roots (patched storage raises on any call);
  - `test_missed_runs_coalesce` (§17.5): the Task Scheduler XML sets `StartWhenAvailable` and `MultipleInstancesPolicy=IgnoreNew`, the systemd timer sets `Persistent=true`, and the launchd plist uses `StartCalendarInterval`, which runs at most once on wake;
  - `test_hf_schedule_never_sends_secrets` (mocked `HfApi`: no call passes `secrets`, and no method creates a job);
  - OS tests (markers `windows`, `macos`, `linux`): real install, status and uninstall on the host, user level.
- [ ] **Step 2: Run them.** Expected: FAIL.
- [ ] **Step 3: Implement the six modules.**
- [ ] **Step 4: Run the default tests and the check set.** Expected: PASS. The OS tests run in CI and in Task 32.

### Task 25: Services layer and previews

**Files:**
- Create: `v2/src/lakehouse/services/{modules,filters,runs,review,schedule,settings}.py`, `v2/tests/unit/test_services.py`

**Interfaces:**
- Consumes: `ConfigStore`, `ConfigVersionInfo`, `Clause` (Task 6); `ResolvedRoots` (Task 8); `CatalogReader` (Task 9); `ChannelAdapter.preview` and `PreviewResult` (Task 14); `DecisionStore` and `Entity` (Task 22); `RunCounts`, `Trigger` (Task 21); `ScheduleBackend`, `HfJobsSchedule`, `ScheduleStatus` (Task 24).
- Produces (each type in the service module that returns it):
  - `ModuleService.list_modules() -> list[ModuleStatus]`, where `ModuleStatus(module_id: ModuleId, display_name: str, active: bool, filter_summary: str, match_count: int | None, last_run: RunSummary | None, token_status: Literal["present", "missing", "public-only"] | None, warning: str | None)`. The token status never carries the value.
  - `FilterService.preview(module_id: ModuleId) -> PreviewResult` and `FilterService.diff(module_id: ModuleId, draft: list[Clause]) -> FilterDiff`, where `FilterDiff(added: int, stop_updating: int, resume_updating: int)`; a narrowed filter never deletes data, so the diff has no "removed" count, and a widened filter re-includes kept matches, which then resume updating (§8.1, §29.13). The diff applies `statsbomb_360_only` from the draft as well.
  - `SettingsService.load() -> tuple[Settings, int]`, `.save(draft: Settings, *, expected_version: int, operator: str) -> int`, `.history(limit: int = 500) -> list[ConfigVersionInfo]` and `.storage_roots() -> ResolvedRoots` (read-only; the Settings page shows the roots and their source). Every config save (active switches, filters, the schedule setting, the settings) goes through `SettingsService.save`.
  - `RunService.history(limit: int = 500) -> list[RunSummary]`, `.progress(run_id: str) -> Progress` and `.trigger() -> TriggerResult`, where:
    - `RunSummary(run_id: str, trigger: Trigger, status: Literal["ok", "failed", "skipped", "running"], started_at: datetime, ended_at: datetime | None, counts: RunCounts, error_count: int)`;
    - `Progress(run_id: str, step: str, module_id: ModuleId | None, match_name: str | None, done: int, total: int | None, updated_at: datetime)`;
    - `TriggerResult(started: bool, run_id: str | None, message: str)`. Locally it starts a detached CLI process. On HF it calls `HfJobsSchedule.trigger()`. A trigger that HF refuses (for example under `--no-concurrency` while a run is active) gives `started=False` with the message "not started: a run is already active", and the Ops page shows it (§19.5); a press of "Run now" is never lost silently.
  - `ReviewService.queue(limit: int = 500) -> list[QueueItem]`, `.decide(item_id: str, action: Literal["confirm", "reject", "later"], *, operator: str, reason: str) -> None`, `.accept_recommended(entity: Entity | None, batch_filter: str | None, *, operator: str) -> str` (the request id), `.undo_batch(batch_id: str, *, operator: str) -> str` (the request id), `.pending_requests() -> list[str]` and `.unreviewed_count() -> int`, where `QueueItem(item_id: str, entity: Entity, left: RecordView, right: RecordView, score: float, score_meaning: str, recommended: bool, held: bool)` and `RecordView(name: str, birth_date: date | None, team: str | None, jersey: str | None, position: str | None, matches: int | None)`.
  - `ScheduleService.get() -> ScheduleStatus` and `.set(cfg: ScheduleConfig, *, expected_version: int, operator: str) -> ScheduleStatus` (it saves the setting, then installs, reschedules, suspends or resumes through the backend).
- Rules: every user-facing query has a LIMIT (500 ranking, 2000 timeline), and no raw ids are returned (§19.5). On HF the services never query a bronze table of a private match: they read dims, links, ops tables and catalog statistics only (§20). A read that fails with `IO Error: Cannot open file` after a move re-reads `CURRENT` and shows an error, never missing data (§9).

- [ ] **Step 1: Write the failing tests:**
  - `test_no_raw_ids_in_outputs`: no returned string matches a native-id pattern;
  - `test_limits_applied`;
  - `test_token_value_never_returned`;
  - `test_preview_never_ingests`: no storage write during a preview;
  - `test_pining_preview_public_only_marker` (§7.2 interim);
  - `test_services_without_restricted_access`: with a storage whose `restricted` root raises on read, every service method works (§23.6 "Space without restricted access", local half);
  - `test_stale_reader_after_move_shows_error`;
  - `test_diff_widened_resumes`: turning `statsbomb_360_only` off again gives `resume_updating` = the kept matches without 360 data, and `added` counts only matches never ingested;
  - `test_diff_with_360_option`: turning `statsbomb_360_only` on in the draft gives `stop_updating` = the ingested matches without 360 data, and `added` = 0;
  - `test_trigger_refused_shown`: a mocked `HfJobsSchedule.trigger` that HF refuses gives `started=False` and the visible message;
  - `test_storage_roots_shown_with_source`.
- [ ] **Step 2: Run them.** Expected: FAIL.
- [ ] **Step 3: Implement the six service modules.**
- [ ] **Step 4: Run the tests and the check set.** Expected: PASS.

### Task 26: CLI

**Files:**
- Create: `v2/src/lakehouse/cli.py`, `v2/tests/unit/test_cli.py`; add `[project.scripts] lakehouse = "lakehouse.cli:main"` to `pyproject.toml`

**Interfaces:**
- On HF, `restore`, `relocate` and `sweep --allow-large` write catalog-control objects, so they run only inside an HF Job that the owner launches with the Job secrets (§29.9). Without gateway credentials they stop with a message that says so.
- Produces: `main(argv: list[str] | None = None) -> int` (argparse) with the global options `--lake <root>` and `--restricted <root>` (Task 8 precedence) and the subcommands `init --lake <root> [--restricted <root>]` (creates both roots and writes the bootstrap file), `run [--trigger schedule|button|cli]`, `restore --to <seq>`, `verify`, `relocate --from <root> --to <root>`, `reparse [--match <id>|--all]`, `sweep --allow-large`, `schedule install|uninstall|status [--system --run-as <user>]` (`--system` requires `--run-as`; install passes the allowlisted non-secret variables `LAKEHOUSE_*`, `UV_CACHE_DIR`, `UV_PYTHON_INSTALL_DIR`, `HF_HOME` and `TMPDIR` from its own environment to the backend), `config show|set`, `decisions export|import <file>` and `account create` (the local operator). Every subcommand calls the services layer or the Task 9–12 functions, never a store directly.

- [ ] **Step 1: Write the failing tests.** For each subcommand, one test with a fake services layer that asserts the call and the exit code. Add:
  - `test_reparse_from_archive_offline`: no network call, and only the changed matches are re-parsed (§13.2);
  - `test_reparse_after_private_move` (a real local lake, not fake services): after a match's pining raw file moved to `restricted:raw/`, `lakehouse reparse --match <id>` reads it there and rebuilds identical bronze;
  - `test_init_writes_bootstrap`: `init --lake <tmp>/My Lake` creates both roots and a bootstrap file that `resolve_roots` reads back;
  - `test_global_root_flags_override_env`;
  - `test_hf_catalog_commands_need_gateway`: `restore`, `relocate` and `sweep --allow-large` on HF roots without gateway credentials exit non-zero with the message, and write nothing.
- [ ] **Step 2: Run them.** Expected: FAIL.
- [ ] **Step 3: Implement `cli.py`.**
- [ ] **Step 4: Write `v2/README.md`** in STE: install with uv on Windows, macOS and Linux; `lakehouse init`; `lakehouse account create`; the default modules; `lakehouse run`; the schedule commands; where the lake lives and how the roots are resolved; the licences of the public sources (§7.1).
- [ ] **Step 5: Run the tests and the check set.** Expected: PASS.

### Task 27: Web server and sign-in

**Files:**
- Create: `v2/src/lakehouse/web/{app,auth_hf,auth_local,csrf}.py`, `v2/tests/unit/test_web_auth.py`
- Reference: v1 `hf_taipy_app/src/admin_api.py`

**Interfaces:**
- Produces: `create_app(settings: Settings, *, mode: Literal["hf", "local"]) -> flask.Flask`, with `@login_required` on every Ops route and every Taipy page. HF mode: an OAuth authorisation-code flow with `authlib` using the Space variables `OAUTH_CLIENT_ID`, `OAUTH_CLIENT_SECRET`, `OPENID_PROVIDER_URL` and `OAUTH_SCOPES`, plus an allow-list from `OPS_ALLOWED_USERS`. Local mode: one operator with an Argon2id password hash in `config/account.json`. Sessions: `HttpOnly`, `Secure` over HTTPS, `SameSite=Lax`. CSRF tokens on every state-changing request.

- [ ] **Step 1: Write the failing tests:**
  - `test_anonymous_redirected_to_signin`;
  - `test_user_not_on_allow_list_refused`;
  - `test_local_password_argon2`;
  - `test_csrf_required_on_post`;
  - `test_session_cookie_flags`;
  - `test_no_secret_in_rendered_pages` (scan the responses for known token values).
- [ ] **Step 2: Run them.** Expected: FAIL.
- [ ] **Step 3: Implement the four modules.**
- [ ] **Step 4: Run the tests and the check set.** Expected: PASS.

### Task 28: Ops pages on the copied Taipy shell, and the install path

> Scope note (owner decision, spec §29.17): this task delivers the pages **render-only** (Markdown over the services). The functional actions of spec §19.3 — the active switch, "Run now", "Preview" and "Save" — move to cycle 2. The CLI is cycle 1's operator-action path.

**Files:**
- Create: `v2/src/lakehouse/ui/shell.py`, `v2/src/lakehouse/ui/styles.css`, `v2/src/lakehouse/ui/pages/{modules,filters,runs,review,schedule,settings}.py`, `v2/tests/unit/test_pages.py`, `v2/tests/e2e/test_page_latency.py`
- Reference: copy the shell patterns from v1 `hf_taipy_app/src/page_template.py` and its styles. v1 files stay unchanged (§19.1).

**Interfaces:**
- Consumes: Task 25 services only (enforced by the Task 1 import-linter contract).
- Produces: `build_gui(services: Services) -> taipy.gui.Gui`, mounted on the Task 27 Flask app. `Services` is a frozen dataclass that holds one instance of each Task 25 service.

- [ ] **Step 1: Re-check the ADR-083 override.** Resolve the lock without `override-dependencies`. If it resolves to `flask-socketio>=5.5.1` without the override, remove the override and record why in ADR-008; otherwise keep it. Run `uv sync` on Windows now; macOS follows in Task 30 and Linux in Task 32.
- [ ] **Step 2: Write the failing tests:**
  - `test_every_page_requires_login`;
  - `test_pages_show_names_not_ids`;
  - `test_resolution_score_has_help_text` (§19.5);
  - `test_fallback_markers_visible` (the "count not available", "not listed here" and "N links accepted without review" texts);
  - `test_run_now_non_blocking`;
  - `test_filter_diff_text` (`"+34 matches would be added · 0 would stop updating · 0 would resume updating"` style, with "(their data stays)" whenever the stop count is above 0, and "no change" when all three are zero).
  - `test_statsbomb_360_checkbox_and_share`: the StatsBomb filter page shows the checkbox "Only matches with 360 freeze frames" and each tournament's 360 share text; no other module's page shows the checkbox; the Modules page shows "360 only" when it is on.
  - `e2e/test_page_latency.py`: first load ≤ 3 s and cached interaction ≤ 500 ms on a fixture lake (local; HF in Task 33 item 3).
- [ ] **Step 3: Run them.** Expected: FAIL.
- [ ] **Step 4: Implement the shell and the six pages.** Use goal-oriented navigation labels.
- [ ] **Step 5: Run the tests and the check set.** Expected: PASS.
- [ ] **Step 6: Write ADR-008** (Taipy with the four guards, and the result of the override re-check).

### Task 29: Space and Job images

**Files:**
- Create: `v2/deploy/space/Dockerfile`, `v2/deploy/space/README.md` (Space metadata: `sdk: docker`, `hf_oauth: true`, `app_port`), `v2/deploy/job/Dockerfile`, `v2/tests/unit/test_deploy_files.py`

**Interfaces:**
- The images use `python:3.12-slim` pinned by digest, install the uv-locked set with `--no-deps`, and run as a non-root user. The Space runs `lakehouse.web`, and the Job runs `python -m lakehouse.mask -- lakehouse run --trigger schedule` (Task 1 masking wrapper).
- Both read their roots from the non-secret variables `LAKEHOUSE_LAKE_ROOT` and `LAKEHOUSE_RESTRICTED_ROOT` (Task 8). The Job also reads `LAKEHOUSE_LAKE_S3_PATH` and `LAKEHOUSE_RESTRICTED_S3_PATH`. The Space also reads `OPS_ALLOWED_USERS` and `LAKEHOUSE_HF_SCHEDULED_JOB_ID`. Secrets come only from the platform secrets at run time (Task 33).
- The Job image is published as a second, private Docker Space that HF builds (`hf.co/spaces/<ns>/<job-image-space>`), so no external registry login is needed. Task 33 item 14 verifies that a scheduled job runs that image. The same image holds the `tests/` tree, so the `hf_job` and `hf_space` tests run inside it.

- [ ] **Step 1: Write the failing test** `test_deploy_files`: both Dockerfiles pin a digest, use `--no-deps`, define a non-root user and contain no secret values and no `ENV` line for a secret name; the Space README front matter has `hf_oauth: true`.
- [ ] **Step 2: Run it.** Expected: FAIL.
- [ ] **Step 3: Write the files.**
- [ ] **Step 4: Run the tests and the check set.** Expected: PASS. Do not build or push any image yet (Gate G-B, Task 33).

## Phase 5 — verification, labelling and close

Task 30 comes first, so that the parse-memory budget exists before any full-size parse (C7, §10.9). The AI-PC never parses a full tracking file in cycle 1.

### Task 30: Mac Mini staging install, benchmarks and the parse-memory budget (§26 items 5, 6, 13) — Gates G-D and G-C

- [ ] **Step 1: Gate G-D.** SSH from the AI-PC is set up and key-only (2026-10-26). The host details (address, operator user, host-key fingerprint) stay in the session's notes and out of this repository. Already verified on 2026-10-26: macOS 26.2, auto-login of the operator user, the dock SSD erased as the APFS volume `Development` (`/Volumes/Development`, USB, owner-approved wipe of the old exFAT `Data`), and `AutomountDisksWithoutUserLogin` = `1` (§22.3). Read-only checks before any install:
  - `diskutil info /Volumes/Development` still shows `APFS`;
  - free space on `Development`;
  - done on 2026-10-26: after an owner-started restart, the auto-login session started and `Development` was mounted about 10 s after boot (1.4 s after the auto-login). The owner declined a separate proof of the no-login case and accepted the missing-root guard (§10.5) for it.
  Ask before any install.
- [ ] **Step 2: Lay out v2 on the `Development` volume** (owner instruction, §22.3). Everything of v2 lives under one folder, `/Volumes/Development/lakehouse/`. The session never writes or deletes anything else on the volume. Layout:
  - `v2/`: the code. Copy the AI-PC working tree with `tar` over SSH, excluding `.venv/`, `reference/` and caches. Never `git push` (owner rule); repeat the copy after each change.
  - `cache/uv`, `cache/hf`, `cache/bench`, `cache/duckdb-ext`, `tmp/`: set through `UV_CACHE_DIR`, `UV_PYTHON_INSTALL_DIR`, `HF_HOME`, `LAKEHOUSE_BENCH_CACHE`, `LAKEHOUSE_DUCKDB_EXTENSION_DIR`, `LAKEHOUSE_CONFIG_DIR` (`/Volumes/Development/lakehouse/config-home`) and `TMPDIR`, in one non-secret file `/Volumes/Development/lakehouse/env.sh` that every SSH command sources. The virtual environment is `v2/.venv`, so it is on `Development` as well.
  - `logs/` with the two files `logs/schedule.out.log` and `logs/schedule.err.log`: created by the operator user before any system install, so the launchd `StandardOutPath` and `StandardErrorPath` files belong to the operator; the system install checks that both files exist and never creates them;
  - `lake/` and `lake-restricted/`: the staging roots, created with `lakehouse init --lake /Volumes/Development/lakehouse/lake`.
- [ ] **Step 3: Install v2 on the Mac Mini.** Run `uv sync` (the macOS half of the Task 28 override check, §26 item 14), `install ducklake; install httpfs` into `cache/duckdb-ext` (item 5), and confirm the DuckDB version. Confirm that no file of v2 was written outside `/Volumes/Development/lakehouse/` (check `~/.cache`, `~/.duckdb`, `~/.config`, `~/Library/Caches`, `~/Library/Application Support` and `/tmp` for new v2 entries). The only exception is, from Task 32, the system plist in `/Library/LaunchDaemons/` (§22.3, §29.11). If DuckDB 2.0.1 has shipped, move to it and re-run the storage tests (item 6).
- [ ] **Step 4: Measure the parse-memory budget (item 13).** Run `uv run pytest -m bench tests/bench/test_bench_tracking.py --bench-record` (a test option that measures and writes, instead of checking). It records the peak RSS of one IDSSE parse and one SkillCorner parse, the parse and write times and the bytes per match with the final schema (§11.2). Write the budget (the measured peak plus 25 % headroom, author's call) to `tests/bench/budget.json` and to ADR-002 next to §25.1.
- [ ] **Step 5: Run the benchmarks in check mode.** Run: `uv run pytest -m bench -v`. Expected: PASS against the budget. Then set the budget 10 % below the measured peak in a scratch copy and confirm that the bench fails (a guard must be seen to fail).
- [ ] **Step 6: Gate G-C.** If any §26 item that this task runs fails (items 5, 6 and 13, and the macOS half of item 14), stop and bring the evidence to the owner.

### Task 31: Live end-to-end on public sources (on the Mac Mini)

**Files:**
- Create: `v2/tests/live/test_e2e_public_small.py` (markers `live` and `heavy`)

- [ ] **Step 1: Write a live e2e test.** On a fresh local lake with the laptop defaults restricted to 2 matches per module (a test-only filter), run `lakehouse run`. Assert: bronze for every active module; the field-inventory live tests pass; dims exist; the review queue holds items or links; a second run downloads nothing and re-parses nothing (§G2).
- [ ] **Step 2: Run it on the Mac Mini.** Run: `cd v2 && uv run pytest -m live tests/live -v`, with the caps of §10.9 and one process. Expected: PASS. On the AI-PC, run only `uv run pytest -m "live and not heavy" tests/live -v`. Expected: PASS.
- [ ] **Step 3: Record the results.** Write the counts and timings into `v2/docs/adrs/ADR-002-tuned-layout.md`.

### Task 32: Mac Mini full ingestion, catalog at scale and the three-OS proof (§26 items 4, 7, 8, 12) — Gate G-C

- [ ] **Step 1: Install the system-level schedule (Gate G-D, an install on the Mac Mini).** The session prepares the command; the owner runs it on the Mac with `sudo`, because it needs admin rights, and types the password personally: `sudo -E env PYTHONDONTWRITEBYTECODE=1 /Volumes/Development/lakehouse/v2/.venv/bin/lakehouse schedule install --system --run-as <operator>` (with `env.sh` sourced), so Python writes no root-owned bytecode into `.venv`. Right after it, check that no file under `/Volumes/Development/lakehouse/` is owned by `root`. The task runs as the operator user, carries only non-secret variables, and writes its stdout and stderr under `/Volumes/Development/lakehouse/logs/`. The staging schedule runs only modules that need no secret.
- [ ] **Step 2: Run a full public ingestion** on the staging lake through that schedule. Afterwards, confirm that no file under `/Volumes/Development/lakehouse/` is owned by `root`. Record the SkillCorner LFS bandwidth (item 7) and the GS Drive behaviour and size (item 8).
- [ ] **Step 3: Coordinate precision (item 12).** Ask the owner to set `PINING_FOR_THE_DATA_TOKEN` in the Mac Mini environment personally; the session never sees the value. Then run the live equivalence test on a real `format_version` 2 SkillCorner sample, record its coordinate dtype and the GS coordinate precision, and run `test_list_fields_equal_metadata_live` over every ingested pining match.
- [ ] **Step 4: Run the three-OS proof.** Run the check set and the OS-marked tests on Windows (AI-PC, light), macOS (Mac Mini) and Linux (WSL on the Media-PC or the DGX Spark). Include `uv sync` on Linux (the Linux half of §26 item 14). Record the outputs.
- [ ] **Step 5: Run catalog at scale (item 4)** on the staging lake: catalog size and attach time at 1,000 matches, the `CHECKPOINT` effect, the version-retention storage, and the native and inlined sizes.
- [ ] **Step 6: Gate G-C.** If any §26 item that this task runs fails (items 4, 7, 8 and 12, the Linux half of item 14, and the three-OS read-during-run bullet of item 3; for example an attach time that breaks the §19.5 budget, or a dtype that breaks exactness), stop and bring the evidence to the owner. Do not change the layout or the caps to work around it.

### Task 33: HF verifications (§26 items 1–3, 9–11, 14–16) — Gates G-B and G-C

- [ ] **Step 1: Gate G-B, the session's actions.** Ask the owner for the names of the two buckets, the Ops Space, the Job-image Space and the scheduled job. The session uses the owner's own `hf auth login` on this machine, which the owner sets up in their own terminal; the session never reads or prints its token. Ask for a separate explicit yes immediately before each action, and do nothing outward without it:
  - create the private lake bucket;
  - create the private restricted bucket;
  - create the Ops Space;
  - create the private Job-image Space (Task 29);
  - deploy a Space image (each first deploy and each later redeploy);
  - set each non-secret Space variable: `LAKEHOUSE_LAKE_ROOT`, `LAKEHOUSE_RESTRICTED_ROOT`, `OPS_ALLOWED_USERS`, `LAKEHOUSE_HF_SCHEDULED_JOB_ID`.
- [ ] **Step 2: Gate G-B, the owner's actions.** The session never sees a secret value. Ask the owner to do these personally:
  - create in the HF web UI: the HFAK gateway keys (scoped to the two buckets if HF allows it; else see step 4), the Space token (lake bucket read and write, plus job management) and the Job token (both buckets);
  - set the Space secrets: the Space token, and the pining list-only token when the tier exists (§29.8);
  - keep two secrets files outside the repository: `job.secrets` (`HF_TOKEN` = the Job token, `HF_S3_ACCESS_KEY_ID`, `HF_S3_SECRET_ACCESS_KEY`, `PINING_FOR_THE_DATA_TOKEN`) and `space.secrets` (`HF_TOKEN` = the Space token, plus `PINING_LIST_TOKEN` when the tier exists);
  - run every command that passes a secrets file in the owner's own terminal, never through this session. The session prepares the commands with placeholders and no values. First check the options with `hf jobs run --help` and `hf jobs scheduled run --help` (`--no-concurrency` included):

    ```bash
    # the two test jobs, started together, so that the reader test has a live writer
    hf jobs run --flavor cpu-basic --secrets-file <job.secrets> \
      --env LAKEHOUSE_LAKE_ROOT=hf://buckets/<ns>/<lake-bucket> \
      --env LAKEHOUSE_RESTRICTED_ROOT=hf://buckets/<ns>/<restricted-bucket> \
      --env LAKEHOUSE_LAKE_S3_PATH=<lake-s3-path> --env LAKEHOUSE_RESTRICTED_S3_PATH=<restricted-s3-path> \
      hf.co/spaces/<ns>/<job-image-space> python -m lakehouse.mask -- pytest -m "hf and hf_job" -v
    hf jobs run --flavor cpu-basic --secrets-file <space.secrets> \
      --env LAKEHOUSE_LAKE_ROOT=hf://buckets/<ns>/<lake-bucket> \
      --env LAKEHOUSE_RESTRICTED_ROOT=hf://buckets/<ns>/<restricted-bucket> \
      --env LAKEHOUSE_HF_TEST_SCHEDULED_JOB_ID=<throwaway-scheduled-job-id> \
      hf.co/spaces/<ns>/<job-image-space> python -m lakehouse.mask -- pytest -m "hf and hf_space" -v

    # the scheduled job, created once (§29.5)
    hf jobs scheduled run --suspend --no-concurrency --flavor cpu-basic --name <job-name> \
      --secrets-file <job.secrets> \
      --env LAKEHOUSE_LAKE_ROOT=hf://buckets/<ns>/<lake-bucket> \
      --env LAKEHOUSE_RESTRICTED_ROOT=hf://buckets/<ns>/<restricted-bucket> \
      --env LAKEHOUSE_LAKE_S3_PATH=<lake-s3-path> --env LAKEHOUSE_RESTRICTED_S3_PATH=<restricted-s3-path> \
      "<cron>" hf.co/spaces/<ns>/<job-image-space> python -m lakehouse.mask -- lakehouse run --trigger schedule
    ```

    The S3 paths come from the HF gateway documentation for the two buckets; the `hf_job` tests verify them (item 15). The owner gives the session only the job ids. The session reads the logs with `hf jobs logs <id>`. A `tests/hf/conftest.py` filter masks the value of every secret variable in all test output.
- [ ] **Step 3: Read the `hf` test results** from the two job logs, then do the checks on the deployed Space. Where each item runs:
  - `hf_job` (the Job secrets):
    - item 1, Job half: bucket access;
    - item 2 (§23.6 "Two writers", HF half): two processes started together, exactly one writes; two takers of one expired lease, exactly one holds the next generation; `If-None-Match` on lease generations and on single-part and multipart version uploads; renew and release as plain PUTs; `If-Match` on `CURRENT` with a stale ETag gives HTTP 412; an `hf://` read sees a gateway write at once;
    - item 3, writer half: a published version attached from `hf://` lists the same snapshots as the working copy; N catalog commits while the `hf_space` job reads;
    - item 15: dbt builds a contract table, the run inserts a run-history row, and the Job registers a tracking file under its `hf://` URI; `s3://` and `hf://` name the same object; DuckDB writes, multipart uploads and conditional writes through the gateway; the key scope; the sweep with all path forms on HF, including the age fence (an unlisted object younger than the lease survives); a live `list` of the narrow prefix `data/bronze/<table>/<match_key>-` through the gateway returns exactly the expected keys and no key of another match; `list` and `stat` report the same storage clock, at the same precision, for gateway-written and `HfApi`-written objects;
    - secret masking: a forced DuckDB error on a statement that holds a secret, and a log line that holds a secret value, both appear masked in `hf jobs logs`;
    - item 9: the run time and the memory of a full public run on `cpu-basic`, from `hf jobs inspect` and the job's own peak RSS log line.
  - `hf_space` (only the Space token):
    - item 1, Space half: config-side writes through `HfApi`, reads through `hf://`;
    - item 3, reader half: a versioned `hf://` attach; a reader across the writer's N commits gets consistent results or fails loudly. The overlap is guaranteed, not left to timing: the reader writes `runs/test/reader-ready` and reads until the writer writes `runs/test/writer-done`; the writer waits for `reader-ready` before its first commit, at most 15 minutes, and the reader waits for `writer-done` at most 30 minutes; each timeout fails loudly. The reader fails (never skips) unless it saw at least 2 distinct `CURRENT` sequences and the writer reports N commits. Also a re-rooted read on `hf://`; the cached interaction time of every Ops page within 500 ms (Flask test client, second render);
    - item 15, schedule half (`HfJobsSchedule` with the Space token, §18): `reschedule`, `suspend`, `resume` and `trigger` on a throwaway suspended scheduled job that the owner creates with a dummy secrets file (`LAKEHOUSE_TEST_SECRET=dummy`, no real secret), `--no-concurrency`, and a command that writes nothing and stays active for 10 minutes (`python -c "import time; time.sleep(600)"`); the owner passes its id to this job as `--env LAKEHOUSE_HF_TEST_SCHEDULED_JOB_ID=<id>` and deletes it afterwards; `trigger` starts the paused job; a later `inspect` still lists the secret names; the test triggers it, polls every 5 s until its run is `RUNNING` (at most 10 minutes, then it fails loudly), triggers again within 30 s of seeing `RUNNING`, and cancels the run afterwards; the second `trigger` while a run of that job is active returns a result that the Ops page shows as "not started: a run is already active" (`--no-concurrency`), never silently;
    - item 15, reader half: the dbt table, the run-history row and the tracking table read by value;
    - item 16 (§23.6 "Space without restricted access", HF half): a read of a restricted-bucket object fails; every Ops page renders and works (Flask test client, signed-in test user); a services-layer query log shows no query on a private match's bronze table; the token prefix scoping; an `inspect` of the scheduled job returns secret names only, never values.
  - On the deployed Space, with the owner:
    - item 10: the owner signs in through HF OAuth, and a user who is not on `OPS_ALLOWED_USERS` is refused;
    - item 11: the Space runtime reports one replica;
    - item 14: both Space images build and run, and the scheduled job runs the Job-image Space image.

  Three build decisions amend this step, recorded in spec §29.18 (owner, 2026-10-29): item 16's query check is met by a behavioural proof, not a query log; the §19 page budgets apply at two levels (lake pages ≤ 500 ms cached, the external `filters`/`schedule` pages ≤ 3 s); and the barrier, run and sweep tests run against a throwaway `*-test` bucket pair, never the production lake.
- [ ] **Step 4: Gate G-C.** Stop and bring the evidence to the owner if any item fails; if a gateway key cannot be limited to the two buckets (before its first use, §10.5.1); if the gateway ignores or rejects a conditional header (then ADR-007's lease and fencing cannot hold on HF, §10.6); or if HF Jobs cannot run a Space image (then the owner names a registry, and its login is the owner's). Do not switch to design (b) without approval (§29.4).
- [ ] **Step 5: Record the results** in ADR-001 and ADR-007 (the lease and fencing mechanism as verified).

### Task 34: Truth set and the cycle-close prerequisite — Gates G-E and G-F

- [ ] **Step 1: Gate G-E.** Ask the owner to start the truth-set investigation session (§15.4), with or without web research. Provide the candidate pairs from the Mac Mini staging lake: every layer and score band, and the exhaustive scopes of §15.4 for players, teams, competitions and matches.
- [ ] **Step 2: After the owner's labels exist**, store them as ids only in `v2/tests/fixtures/truthset/`. Add the rows that `truthset_rows_from_decisions` gives for the owner's hand-checked queue decisions (§15.5), after the owner confirms that list. Run `test_resolution_quality` (marker `live`). Compute the bands and margins per entity, write them to the catalog, and record them in ADR-010. If a target is missed after band tuning, evaluate splink 5.0.0 on the same truth set and bring the result to the owner (§15.4).
- [ ] **Step 3: Gate G-F.** Check that the pining list-only tier exists and that `test_list_token_cannot_fetch_artifact` and §26 item 16's list-token check pass against the live API. If not, stop and ask the owner (§29.8).

### Task 35: Final verification and the single cycle commit — Gate G-G

- [ ] **Step 1: Remove `v2/reference/`.** Every reused piece now lives in `v2/src` or `v2/dbt`, and the ADRs record its source.
- [ ] **Step 2: Run the v2 check set and every marker tier recorded as passed** (`live`, `heavy`, `hf`, `bench`, OS). Expected: all green; paste the outputs into the hand-over message.
- [ ] **Step 3: Run the v1 seven checks at the repo root**, exactly as `docs/context/code-quality.md` lists them:

  ```bash
  uv run ruff check src/ scripts/
  uv run ruff format --check src/ scripts/
  uv run lint-imports
  uv run python scripts/bump_wheel.py --check
  uv run python scripts/pip_audit_ignores.py --check
  uv run pyright src/ hf_taipy_app/src/ scripts/_tf_env_pins.py scripts/sync_tf_env_pins.py
  uv run pytest src/tests/ -v
  ```

  Expected: all green, unchanged from `main`. Run the full v1 pytest in the background, because it takes longer than 30 s. Also run `pre-commit run --files $(git ls-files --others --exclude-standard v2 .github/workflows/v2-ci.yml)`. Expected: pass, including `detect-secrets` and `check-added-large-files`.
- [ ] **Step 4: Check the spec coverage.** Confirm that every assertion of every §23.6 row has a passing test, by name, and that every §26 item has a recorded result. List the results in the hand-over message. A skipped test that carries a §23.6 assertion or a §26 item counts as not passed: list each one with its skip reason, and stop and ask the owner before Gate G-G.
- [ ] **Step 5: Gate G-G.** Show the owner `git status --short`, the file list and `git diff --stat` for `v2/` and `.github/workflows/v2-ci.yml`. Ask for an explicit yes for this one commit. Commit only after the yes, with a Conventional Commits message ending in the attribution lines. Do not push.

---

## Self-review notes (author)

- **Spec coverage.** Each spec section maps to a task: §7 → 2, 14–20; §8 → 6, 8; §9 → 10, 11, 13, 23; §10 → 7–12; §11 → 10, 15–19, 21, 23; §12 → 15–20; §13 → 5, 13, 26; §14 → 2; §15 → 22, 25, 28, 34; §16 → 23; §17 → 21, 24; §18 → 24, 33; §19 → 25, 27–29; §20 → 8, 14, 25, 27; §21 → 1, 21; §22 → 26, 30–33; §23 → 1, 35; §26 → 9, 30, 32, 33, 34; §29 → 6, 20, 22, 33, 34; §29.9 and §29.10 → Tasks 8, 10, 11, 12, 26, 33.
- **§23.6 coverage.** Each row, with every assertion, is named in the task that owns it. A row split across tasks: "Two writers" (7, 33), "Skip record" (7, 21), "Catalog restore" (11, 12), "Visibility change moves files" (11, 12; with the §29.10 cases), "Sweep with all path forms" (12, 33), "Restricted root placement" (10, 23), "Space without restricted access" (25, 33), "Visibility comes from the source flag" (20), "Pining list fields" (20, 32).
- **Types.** Every type that a later task consumes is defined in the task that produces it: `Provider`, `Channel` (Task 2); `CanonicalKey`, `RootMap`, `StorageRootInfo` (Task 3); `Settings`, `Clause` and the config models, `ConfigVersionInfo`, `ConfigLock` (Task 6); `Lease`, `SkipRecord` (Task 7); `Storage`, `ObjectInfo`, `S3Credentials`, `ResolvedRoots` (Task 8); `VersionInfo`, `CurrentPointer` (Task 9); `WrittenFile` (Task 10); `MoveRecord`, `RestoreRecord` (Task 11); `CleanupReport` (Task 12); `Delivery`, `CopyInfo`, `Choice` (Task 13); `HttpResult`, `TreeEntry`, `SourceEntry`, `ArtifactRef`, `ParsedMatch`, `CoordinateSystem`, `NamesBundle`, `PreviewResult`, `PreviewTournament` (Task 14); `PiningToken` (Task 20); `RunResult`, `RunCounts`, `RunError`, `Trigger` (Task 21); `Entity`, `RecordRef`, `MustLink`, `CannotLink`, `AcceptRequest`, `UndoRequest`, `DecisionSet`, `ResolutionResult`, `Bands`, `TruthRow`, `QualityReport` (Task 22); `DbtResult`, `DbtNodeResult` (Task 23); `ScheduleStatus`, `ScheduleBackend`, `HfJobsSchedule` (Task 24); `ModuleStatus`, `FilterDiff`, `RunSummary`, `Progress`, `TriggerResult`, `QueueItem`, `RecordView` (Task 25); `Services` (Task 28).
- **Order dependencies.** Tasks 6, 7 and 8 interleave as written: model, then storage, then the store and the lease. The parser tasks need Task 14. Task 21 needs Tasks 9–23 for the e2e test, so write its unit tests first and complete its e2e after Task 23. Task 24 comes before Task 25, because the services consume the schedule types. Task 30 comes before every full-size parse.
- **Owner decisions recorded after plan review rounds 2 to 6.** §29.13 (the StatsBomb 360 option, owner request) and §29.14 (visibility of excluded and inactive-module matches) also apply. §29.9 (catalog-control objects through the gateway), §29.10 (moved keys) and §29.11 (moves per match, restore to the latest visibility, the sweep skips DuckLake's files, the author's added spec text, the system plist exception) and §29.12 (two-pass restore, strays, precise record pruning, raw files of pining deliveries), all approved on 2026-10-26; the Mac Mini `Development` volume for everything of v2 (§22.3, Task 30); and no separate proof of the no-login mount.
- **Author's calls for the owner to check.** The lease generations, their timing constants and the 30-day pruning (Task 7). The paired root resolution, the bootstrap file and the root check against `CURRENT` (Task 8). The `O_EXCL` fallback on file systems without hard links (Task 8). A list-and-metadata mismatch is a visible warning, not a match failure (Task 20). The Job image as a private HF Space, and the `hf_job` / `hf_space` test placement in owner-launched jobs (Tasks 29, 33). The parse budget headroom of 25 % (Task 30).
- **No commits before Task 35.** This replaces the skill's per-task commit step, by the owner's rule.
