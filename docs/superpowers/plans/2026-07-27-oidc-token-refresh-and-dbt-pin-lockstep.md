# OIDC credential lifetime + dbt environment lockstep — Implementation Plan

**Revision 3** — review round 2 applied (B1, B2 blocking; T7–T10, S-f/g/h structural). Architecture
unchanged from rev 2. Rev 1's auth fix (composite action + per-step mint + lifecycle `client_factory`)
remains withdrawn. See § Review disposition.

**Goal:** Stop the three scheduled workflows failing, by (1) never materialising the OIDC bearer into a
dead string — by **any** transport, and (2) giving the live-CI dbt job the declared serverless
environment the production path already uses, deleting the runtime pip install rather than pinning it.

**Architecture:**
*Auth* — delete every mint step; move `DATABRICKS_AUTH_TYPE: github-oidc` to job level. Each
`WorkspaceClient()` then holds a live, self-refreshing credential (Tier 1). A `CredentialsStrategy` in
`src/ingestion/databricks_auth.py` caches the *exchanged* bearer so this does not cost two extra HTTPS
round-trips per API call (Tier 2).
*dbt* — declare `dependencies` on the submitted serverless environment; delete `_DBT_PIN`,
`_DBT_DATABRICKS_PIN` and the runtime `pip install` outright.

**Tech Stack:** Python 3.10, databricks-sdk 0.121.0 (`pyproject.toml:90`), pytest, ruff, pyright, GitHub Actions.

**Amends:** ADR-071 (CI PAT → GitHub OIDC), ADR-046 (exact env pins).

---

## 1. Diagnosis — measured, not inferred

Single run, `lakebase-grants` **30259317817**. One token, one API, only elapsed time varies:

| Time | Since mint | Call | Result |
|---|---|---|---|
| 10:47:06 | — | OIDC mint step | — |
| 10:47:11–36 | +0:30 | `heal_synced_tables` → `postgres.get_synced_table` ×12 | ✅ `heal scan: 0 stranded of 12 candidates` |
| 10:47:36–48:49 | +1:47 | `fix_event_log_ownership.py` → same API | ✅ |
| 10:48:49 | +1:43 | `run_lakebase_grants.py` → `POST /api/2.0/postgres/credentials` | ✅ |
| 10:51:05 | **+3:59** | `grant_synced_table_permissions.py` → 41 grants | ✅ `ci_sp_complete, ci_sp_grants: 41` |
| 10:52:19–26 | **+5:13** | `test_synced_tables_online` → same `postgres.get_synced_table` | ❌ `403 Invalid Token` |

Valid at +3:59, rejected at +5:13. The same API, invoked the same way, succeeded three times earlier in
the same job — so neither a permission gap nor an environment difference. The CI SP already holds
`CAN_MANAGE` on the Lakebase project (`terraform/environments/dev/main.tf:328-332`).

| Workflow | Duration | Outcome |
|---|---|---|
| Data Quality CI | 4m11s–4m52s | ✅ finishes inside the window |
| Lakebase Maintenance | 6m03s–6m45s | ❌ fails at its last step |
| Synced-Table Heal E2E | 11m45s–15m52s | ❌ fails mid-poll |
| Terraform Apply | 45–70s | ✅ |

**Blast radius:** all 41 synced tables ONLINE; every maintenance action completes before the token dies;
production `dbt_build` runs the TF-pinned env and is green. **Verification is broken, not data.**

### 1.1 Root cause — the SDK never needed a mint step

Read from the installed SDK (`databricks-sdk==0.121.0`), verified independently this session:

| # | File:line | Fact |
|---|---|---|
| 1 | `_base_client.py:84` | `self._session.auth = self._authenticate` — invoked per prepared request |
| 2 | `_base_client.py:105-110` | `_authenticate` calls `self._header_factory()` **every request** |
| 3 | `config.py:367-369` | `authenticate()` → `self._header_factory()`; *"Returns a list of fresh authentication headers"* |
| 4 | `credentials_provider.py:494-497` | github-oidc `refreshed_headers()` = `token_source_for(audience).token()` |
| 5 | `credentials_provider.py:473-491` | builds a **new** `ClientCredentials` per call |
| 6 | `oidc_token_supplier.py:16-32` | live `requests.get` to `ACTIONS_ID_TOKEN_REQUEST_URL` |

⇒ A reused `WorkspaceClient(auth_type="github-oidc")` re-mints on every call. **Staleness is structurally
impossible for it.** The failure is manufactured by the mint steps, which snapshot a live source into a
dead string, plus the job-level `DATABRICKS_AUTH_TYPE: pat` (`lakebase-grants.yml:87-99`,
`synced-table-heal-e2e.yml:57`) forcing downstream clients to prefer that dead string.

### 1.2 The dbt failure (independent)

Producer: runner, uv.lock **dbt-core 1.11.12** → `dbt parse`. Consumer:
`scripts/ci/run_dbt_in_databricks.py:43-44` pip-installs `dbt-core>=1.10.0,<1.12.0` into the live job →
resolved **1.11.8** → `Field "macros" … in WritableManifest has invalid value` → `dbt exited with code 2`.
Last green 2026-07-22 11:11; `#488` merged/applied 13:35 the same day.

The task log does show pip conflicts (`mlflow-skinny 2.11.4 requires protobuf<5,>=3.12.0, but you have
protobuf 6.33.6`). Rev 1 treated protobuf as the **cause** of the 1.11.8 resolution; that link was never
established and the inference is withdrawn. Why the resolver chose 1.11.8 is **not yet known** — and
after Task 5 it stops mattering, because the runtime install is deleted.

---

## 2. Decisions

| # | Decision | Rationale |
|---|---|---|
| **D1** | **Delete `DATABRICKS_TOKEN` materialisation — by any transport.** | §1.1: the export *is* the bug. Rev 1 kept it citing "34 skip-guarded files"; recounted it is **11** (rev 1's grep counted `__pycache__/*.pyc` and unrelated `skipif`s). `create_indexes.py:589-593` already uses `WorkspaceClient()` + `config.authenticate()`, raw token only in the `except` at `:594-597`. |
| **D2** | **`synced_table_lifecycle.py` gets zero diff.** | Credential lifetime is a property of client construction, not of the synced-table domain; that module's docstring (`:1-19`) sells a hexagonal seam. Under D1 there is nothing to refresh. |
| **D3** | **Five workflows**, not four (B1). | `terraform-apply.yml:101` materialises via `$GITHUB_OUTPUT`. It already sets job-level `github-oidc` (`:24`), so its mint step is redundant today. |
| **D4** | **Amend ADR-071; no new ADR.** | Amendment content: *never materialise the bearer into env or step outputs*. |
| **D5** | **Tier 2 ships in this PR** (owner, 2026-07-27). | Tier 1 alone pays a GitHub OIDC fetch + token exchange per call, since `token_source_for` rebuilds `ClientCredentials` per call so `Refreshable`'s cache never engages. Additive to Tier 1. |
| **D6** | **Delete the runtime dbt install; declare the environment** (owner, 2026-07-27). | `trigger_dbt_job.py:85-95` already sends an `environments` block carrying no `dependencies`. |
| **D7** | **`pyproject.toml` is a consistency *check*, never a rewrite target** (T10). | pyproject is the **input** to `uv.lock`; making it a sync target creates pyproject → lock → pyproject. ADR-046 only inverts this for `databricks-sdk` (`ExemptStrategy.SDK_EXTRA`, `_tf_env_pins.py:117-126`) because the dbt fork genuinely needs a different version than the sdk extra. dbt has no such conflict, so the normal direction holds: pyproject floor → `uv lock` → `==` at the derived sites. The parity test asserts **floor ≤ lock version** and writes nothing. |

---

## 3. File structure

| File | Responsibility | Status |
|---|---|---|
| `src/tests/test_ci_workflow_oidc_auth.py` | + no-materialisation rule (any transport) | Modify (**first**) |
| `.github/workflows/{lakebase-grants,synced-table-heal-e2e,data-quality-ci,python-ci,terraform-apply}.yml` | Job-level `github-oidc`; delete mint steps, exports, `pat` pins | Modify |
| `src/ingestion/databricks_auth.py` | `has_databricks_auth()`, `workspace_client()`, Tier-2 `CredentialsStrategy` | **Create** |
| 11 test files (Task 3 Step 3) | Skip guards → `has_databricks_auth()` | Modify |
| `scripts/patch_job_retries.py` | Header from `ws.config.authenticate()` | Modify |
| `scripts/trigger_dbt_job.py` | Declare `dependencies` on the submitted environment | Modify |
| `scripts/ci/run_dbt_in_databricks.py` | Delete `_DBT_PIN`, `_DBT_DATABRICKS_PIN`, `install_dbt()`; module invocation | Modify |
| `scripts/_tf_env_pins.py`, `scripts/sync_tf_env_pins.py` | Remaining dbt sites | Modify |
| `src/tests/test_databricks_auth.py`, `src/tests/test_ci_dbt_pin_parity.py` | New gates | **Create** |

**Branch:** `fix/ci-oidc-credential-lifetime` off `origin/main` (`abc97d1e`). One squashed commit.
**No commit, push, tag, or PR without explicit approval.**

---

## 4. Tasks

### Task 0 — Branch and baseline
- [ ] `git fetch origin && git checkout main && git pull --ff-only && git checkout -b fix/ci-oidc-credential-lifetime`
- [ ] Record the **command, pass/fail counts, and exit code** of `uv run pytest src/tests/ -q`. A
      collection error also changes a bare count and would read as "tests were removed."

### Task 1 — The guard, FIRST, red against today's YAML (B1)
**Files:** `src/tests/test_ci_workflow_oidc_auth.py`

- [ ] **Step 1** — `test_no_workflow_materialises_a_databricks_token()`. The rule bans the
      **anti-pattern**, not one transport of it:

      > No workflow may assign `DATABRICKS_TOKEN` from anything other than a hardcoded placeholder
      > literal. Deny `>> "$GITHUB_ENV"`, `>> "$GITHUB_OUTPUT"` + `steps.*.outputs.*`, and `secrets.*`.

      Rev 2's `$GITHUB_ENV`-only rule missed a live in-repo instance (`terraform-apply.yml:101`) — a guard
      whose purpose is "this cannot reopen" that already misses a real case is not that guard. Step
      outputs are arguably the worse transport: the value lands in run metadata, not just an env file.
- [ ] **Step 2 — verify RED, expecting exactly FIVE sites:** `data-quality-ci.yml:86`,
      `lakebase-grants.yml:160`, `python-ci.yml:118`, `synced-table-heal-e2e.yml:108`,
      `terraform-apply.yml:101`. Fewer than five ⇒ the detector under-scans; fix it before the workflows.
- [ ] **Step 3** — the allowed case is a hardcoded placeholder (Task 3's dbt-deps steps, and
      `dbt-live-ci.yml:51`). Keep the file's existing shape: negative-proof
      (`test_detector_flags_the_pre_fix_pattern:93`) and non-vacuity
      (`test_guard_actually_scans_workflows:120`). Precedent for the "anchor on a known name" shape:
      `src/tests/test_terraform_env_dep_parity.py:188-195`.

### Task 2 — `has_databricks_auth()` (TDD)
**Files:** create `src/ingestion/databricks_auth.py`, `src/tests/test_databricks_auth.py`

- [ ] **Step 1 — failing tests.** True when `DATABRICKS_HOST` is set **and** (a token is present **or**
      OIDC env — `ACTIONS_ID_TOKEN_REQUEST_TOKEN` + `_URL`, per `oidc_token_supplier.py:17` — is
      available); False otherwise. **The False test's docstring must name the fork-PR case (S-h):**
      GitHub issues no OIDC to fork PRs, so both vars are absent and the 11 live tests skip. That is
      correct, and it is the scenario most likely to decay into a vacuous green later.

      **Host is folded into the predicate, not repeated at 11 call sites (round-3 note).** Every live
      path needs host **and** a credential; the SDK-only guards today check both (e.g.
      `test_action_context_live_ddl_parity.py:29`: `_REQUIRED_ENV = ("DATABRICKS_HOST",
      "DATABRICKS_TOKEN")`). Without it, an absent host attempts a live call against nothing and fails
      confusingly instead of skipping. **Deliberately an env-var check, not SDK-config resolution:** a
      host resolved from `~/.databrickscfg` would turn a safe skip into a live call locally. This is
      behaviour-preserving — those guards already required the env var. Moot in CI, where Task 3 Step 1
      sets `DATABRICKS_HOST` job-level in all five workflows.
- [ ] **Step 2 — verify RED:** `ModuleNotFoundError: ingestion.databricks_auth`.
- [ ] **Step 3 — implement.** Lazy SDK import (optional `--extra sdk`).

### Task 3 — Tier 1: stop materialising the bearer (five workflows)
**Files:** the five workflows, 11 test files, `scripts/patch_job_retries.py`

**Precedent — cite it, this is already running in production.** `dbt-live-ci.yml:42-51` runs job-level
`DATABRICKS_HOST` + `DATABRICKS_CLIENT_ID` + `DATABRICKS_AUTH_TYPE: github-oidc` **together with** a
placeholder `DATABRICKS_TOKEN`, and does not fail. That is exactly the `#489` condition (CLIENT_ID and
TOKEN both present job-wide), disambiguated by the explicit `github-oidc`. `terraform-plan.yml:24` is a
second, simpler instance. Step 2 below is therefore an observed configuration, not an inference.

- [ ] **Step 1** — per workflow: `DATABRICKS_HOST`, `DATABRICKS_CLIENT_ID`,
      `DATABRICKS_AUTH_TYPE: github-oidc` at **job** level; delete the mint step and its export
      (`$GITHUB_ENV` in four, `$GITHUB_OUTPUT` in terraform-apply).
- [ ] **Step 2 — ⚠ DO NOT delete the four dbt-deps placeholder steps (B2).** `dbt_project/profiles.yml`
      uses `env_var('DATABRICKS_TOKEN')` with **no default** at `:10`, `:19`, `:28`, `:53` — unset is a
      hard compilation error (`Env var required but not provided`). These stay:
      `lakebase-grants.yml:129-132`, `data-quality-ci.yml:48-51`, `python-ci.yml:70-73`,
      `synced-table-heal-e2e.yml:79-82` (each `DATABRICKS_TOKEN: dummy`, for `dbt deps`, which never
      contacts the warehouse). They are Task 1's allowed placeholder case. "Delete the `DATABRICKS_TOKEN`
      export" applied one step too broadly breaks `dbt deps` in all four before any Databricks call.
- [ ] **Step 3** — delete the job-level `DATABRICKS_AUTH_TYPE: pat` from `lakebase-grants.yml:87-99` and
      `synced-table-heal-e2e.yml:57`. **Asymmetry:** `data-quality-ci.yml`'s `live-tests` job (`:35`) has
      no job-level `env:` at all; `python-ci.yml`'s (`:46-50`) carries only `SILLY_KICKS_ASSERT_INVARIANTS`.
      The workflows share the mint step, not the auth config.
- [ ] **Step 4 — migrate the 11 skip guards** to `has_databricks_auth()`. The split is decided here, not
      per-file at implementation time (S-g):
      - **Keep the `DATABRICKS_HTTP_PATH` half** (warehouse-backed, 7): `tests/data_quality/`
        `test_bronze_live_schema.py:61-62`, `test_dbt_xg_v2_mart.py:20-21`,
        `test_int_player_xref_invariants.py:17-18`, `test_int_team_xref_invariants.py:12-13`,
        `test_marts_kimball_contracts.py:49-50`, `test_pausa_adr013_compliance.py:25-27`,
        `test_staging_rowcount_vs_bronze.py:60-61`.
      - **`has_databricks_auth()` alone** (SDK-only, 4): `tests/data_quality/test_synced_tables_online.py:20-22`,
        `src/tests/test_orphan_pg_role_absent.py:28-29`,
        `src/tests/test_sk3_mig_b_orchestrator_invariants.py:162-163`,
        `src/tests/test_action_context_live_ddl_parity.py:29-33` (verified: `_REQUIRED_ENV =
        ("DATABRICKS_HOST", "DATABRICKS_TOKEN")`).
- [ ] **Step 5 — subprocess / raw-`requests` boundaries in CI scope only:** take the header from
      `ws.config.authenticate()` at the point of use. `create_indexes.py:589-593` already does;
      `run_lakebase_grants.py:244` (headers built at `:156`/`:172`) and `patch_job_retries.py:89-94` do not.
      **Scope note for the PR body:** the raw-`DATABRICKS_TOKEN`→Bearer idiom exists in 15–20+ files
      (`src/ingestion/utils.py:990-991,1019`, `src/analytics/databricks_sql_fetch.py:27,43`,
      `scripts/ensure_warehouse.py:80`, a duplicated `query_databricks_sql` across `scripts/publish_*_hf.py`).
      **Only CI-reachable ones are in scope**; the rest are pre-existing and unchanged. State that rather
      than implying the surface is two files.
- [ ] **Step 6 — while in `terraform-apply.yml`: pin its SDK install (S-f).** `:88` runs
      `pip install -q databricks-sdk` unpinned; pin to `pyproject.toml:90`'s `==0.121.0`. An unpinned SDK
      install in the infrastructure-applying workflow sits oddly beside an ADR-046 amendment in the same PR.
- [ ] **Step 7** — Task 1's guard must now be GREEN. If not, stop.

### Task 4 — Tier 2: cache the exchanged bearer (D5, TDD)
**Files:** `src/ingestion/databricks_auth.py`, `src/tests/test_databricks_auth.py`

Verified seams: `CredentialsStrategy` ABC with `auth_type()` + `__call__(cfg)`
(`credentials_provider.py:55-63`); `Config(credentials_strategy=…)` (`config.py:274`); `WorkspaceClient`
(`__init__.py:318`). `Refreshable` (`oauth.py:246`) supplies a 40-second pre-expiry skew
(`oauth.py:114-119`, verbatim: *"Azure Databricks rejects tokens that expire in 30 seconds or less, so we
refresh the token 40 seconds before it expires"*), FRESH/STALE (`:241-242`), stale-after
`min(ttl // 2, 20 min)` (`:253`, `:313`), 1-minute post-failure backoff (`:255`).

- [ ] **Step 1 — failing tests, offline, injected supplier + fake clock.** Assert:
  1. two calls inside the freshness window perform **one** token exchange (the cache engages — the entire
     point of Tier 2);
  2. a call after expiry performs a **new GitHub subject-JWT fetch**, not merely a token re-exchange.
     `ClientCredentials.refresh()` (`oauth.py:834+`) re-posts its original `endpoint_params`, which embed
     the now-expired `subject_token`. A cache that misses this works until the GitHub JWT expires, then
     fails exactly the way we are fixing. **Assert the supplier was re-invoked**, not that a token came back.
- [ ] **Step 2 — verify RED:** `ImportError` on the strategy class name.
- [ ] **Step 3 — implement** `_GitHubOidcTokenSource(oauth.Refreshable)` whose `refresh()` fetches a fresh
      subject JWT then performs the token-exchange grant (as `credentials_provider.py:479-491`), plus the
      `CredentialsStrategy` wrapper. The docstring must state *why* it exists — the stock strategy's
      per-call rebuild — or the next reader deletes it as redundant.
- [ ] **Step 4 — add `workspace_client()` and enumerate its adopters (T8).** Rev 2 said "bare sites stay
      bare; wire where CI constructs clients", which contradicts itself — the CI-reachable sites **are**
      the bare ones. The four that adopt `workspace_client()`: `heal_synced_tables.py:141`,
      `refresh_synced_tables.py:134`, `run_lakebase_grants.py:112`,
      `grant_synced_table_permissions.py:351`. Four names is a decidable list; "the sites CI uses" is not.

### Task 5 — dbt: declare the environment, delete the runtime install (D6)
**Files:** `scripts/trigger_dbt_job.py`, `scripts/ci/run_dbt_in_databricks.py`, `scripts/_tf_env_pins.py`,
`scripts/sync_tf_env_pins.py`, create `src/tests/test_ci_dbt_pin_parity.py`

- [ ] **Step 1 — failing parity test.** Sites must equal uv.lock (**dbt-core 1.11.12**,
      **dbt-databricks 1.12.2**): `trigger_dbt_job.py` (new), the four `uvx --from` lines
      (`data-quality-ci.yml:52`, `lakebase-grants.yml:133`, `python-ci.yml:74`,
      `synced-table-heal-e2e.yml:83`), and `terraform/modules/workflows/main.tf:1530-1533` (already exact,
      already covered by `test_tf_exact_pins_match_uv_lock`). **`pyproject.toml:85-88` is asserted for
      consistency only — floor ≤ lock — and never rewritten (D7).** Assert non-vacuity: the scanner must
      find every expected site, or a path typo passes forever.
- [ ] **Step 2 — verify RED** on the `>=` assertions, not a collection error.
- [ ] **Step 3 — add `dependencies` to the submitted environment.** `trigger_dbt_job.py:85-95` sends
      `{"environment_version": "2"}` only. **⚠ Add `dependencies` alongside `environment_version`; do NOT
      also add `client`.** The comment at `:86-92` records that sending both was accepted from 2026-04-26
      until the 2026-06-10 Databricks rollout rejected the pair with `INVALID_PARAMETER_VALUE` — breaking
      this exact workflow. (`submit_ac1_oneshot.py:236-241` is the precedent for declared deps but uses the
      legacy `client="1"`; copy the shape, not the field.)
- [ ] **Step 4 — delete** `_DBT_PIN`, `_DBT_DATABRICKS_PIN` (`run_dbt_in_databricks.py:43-44`), the
      `pip install` subprocess (`:131-137`), and the stale *"keep in sync with pyproject.toml [dbt]"*
      comment at `:42` (already divergent: `<1.12.0` vs an uncapped floor).
      **Also switch both dbt invocations to module form (T7):** `:147` and `:157` currently run
      `["dbt", …]` under `# noqa: S607 — dbt is installed on PATH by install_dbt()`. Deleting
      `install_dbt()` orphans that justification and leaves "dbt is on PATH" an unverified assumption
      about serverless console-script installation — failing at the *second* call, deep in the job, with a
      bare `FileNotFoundError`. Use `[sys.executable, "-m", "dbt.cli.main", …]` and **delete both S607
      noqas** rather than rewriting them; `sys.executable` is absolute, which is what S607 is about.
- [ ] **Step 5 — teach `sync_tf_env_pins.py` to rewrite the derived sites**, reusing
      `parse_lock_versions()` (`_tf_env_pins.py:88`) so fixer ≡ checker. It does **not** route through
      `parse_tf_env_deps()` — that parser is TF-`environment`-block-shaped; these are a Python dict and
      YAML strings. Shared lock parsing, separate site scanner; state the seam in the docstring.
- [ ] **Step 6 — risk, downgraded.** `terraform/modules/workflows/main.tf:1530-1533` pins
      `dbt-core==1.11.12` + `dbt-databricks==1.12.2` on serverless today, so those versions install there.
      Mechanisms differ (declared env spec vs a submit-time `environments` block) — strong evidence, not
      proof. **Verify by dispatch (Task 7); do not relax to a range if it fails.** Escalate instead.

### Task 6 — Docs
- [ ] **ADR-071 amendment:** never materialise the bearer into env **or step outputs**; the SDK re-mints
      per request; the measured (3:59, 5:13] window; why `#489`'s `pat` pin is removed rather than
      retained. **Cite `dbt-live-ci.yml:42-51` and `terraform-plan.yml:24`** — an amendment that points at
      two workflows already running the rule is far harder to regress than one arguing from SDK internals.
- [ ] ADR-046: the dbt pins ride the same lockstep; the runtime install is gone; D7's direction rule.
- [ ] `docs/engineering/conventions.md` → Databricks Dev Flow.

### Task 7 — Verification, then STOP
- [ ] `uv run ruff check src/ scripts/ && uv run ruff format --check src/ scripts/`
- [ ] `uv run pyright src/` — capture the exit code; never read through `| tail`.
- [ ] `uv run pytest src/tests/ -q` — full suite vs Task 0's baseline.
- [ ] **Freshness e2e — asserts the refresh, not an elapsed-time proxy (T9).** Rev 2's version slept 420 s
      and asserted the second call succeeded; that is valid only while `420 s > lifetime`, so a longer
      Databricks lifetime would make it silently pass on a frozen token — the vacuous green this plan
      guards against everywhere else. It also **restores the lifetime measurement rev 2 dropped**, closing
      §1's open number. Host in `synced-table-heal-e2e.yml` (already 12–15 min):
      ```python
      from datetime import datetime
      import time
      from databricks.sdk import WorkspaceClient

      ws = WorkspaceClient()
      t0 = ws.config.oauth_token()                       # config.py:329-339
      ttl = (t0.expiry - datetime.now(tz=t0.expiry.tzinfo)).total_seconds()
      print(f"::notice::measured OIDC bearer lifetime: {ttl:.0f}s")
      ws.current_user.me()
      time.sleep(420)
      t1 = ws.config.oauth_token()
      assert t1.expiry > t0.expiry, "token did not refresh — the SDK returned a frozen bearer"
      ws.current_user.me()
      ```
- [ ] **Measure Tier 2 (risk 2).** One dispatch with SDK debug logging; count GitHub OIDC fetches inside
      `grant_synced_table_permissions` (41 grants, one process). Expected ~1 per token lifetime, not ~1
      per call. If ~1 per call, the cache is not engaging and Task 4 Step 1's assertion was too weak.
- [ ] **Live dispatch:** `lakebase-grants` watched to completion (**all** steps), then `heal-e2e`.
- [ ] **STOP.** Report and await approval.

---

## 5. Risks

1. **`ACTIONS_ID_TOKEN_REQUEST_*` must stay valid job-wide** (`oidc_token_supplier.py:17`). Load-bearing
   for both Tiers. Believed true (`id-token: write` is job-scoped), **unverified**; Task 7's e2e tests it.
2. **GitHub OIDC endpoint call volume.** Tier 2's cache is **per-process**, and every CI step is a fresh
   `uv run python` — so the exposure was never uniformly "hundreds per job": it is hundreds *inside* the
   multi-call processes (41 grants in one; ~60 polls in another), plus at least one mint per step
   regardless. Tier 2 targets exactly those processes. No verified figure for GitHub's limit; Task 7's
   measurement turns this into a number. Fails loud if hit.
3. **Task 5 Step 6** — submit-time `environments.dependencies` is not the same mechanism as the
   TF-declared env. Verified by dispatch, not assumed.
4. **Deleting the `pat` pin re-opens #489 if a PAT reappears.** Task 1's widened guard is the control.

---

## 6. Review disposition

### Round 1 (rev 1 → rev 2)
**Accepted:** S1 (auth approach changed entirely), T1 (lifecycle zero diff), T2 (guard before fix),
T3 (automated e2e), T4 (count is 11 not 34; `create_indexes.py` primary path already correct;
`run_lakebase_grants.py` headers at `:156`/`:172`; raw-token surface 15–20+ files), T5 (risk downgraded),
T6 (folded in), S-a, S-b, S-c, S-d, S-e.
**Pushed back, accepted by the reviewer:** the protobuf conflicts *are* in the live task output; a
repo-scoped grep would not find them. Their substantive point — the causal claim was unsupported — is
applied in §1.2.

### Round 2 (rev 2 → rev 3)
**Accepted:** B1 (guard widened to any transport; RED = 5; `terraform-apply` folded in → five workflows),
B2 (the four dbt-deps placeholders are load-bearing and stay), the `dbt-live-ci.yml:42-51` /
`terraform-plan.yml:24` precedent, T7 (module invocation; both S607 noqas deleted), T8
(`workspace_client()` + four enumerated adopters; risk 2 sharpened to per-process), T9 (assert
`t1.expiry > t0.expiry`; restore the lifetime measurement), S-f, S-g (split decided in-plan), S-h.
**Decided, not deferred:** T10 → D7 (pyproject is a consistency check, never a rewrite target).
