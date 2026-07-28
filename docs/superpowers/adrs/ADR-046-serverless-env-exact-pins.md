# ADR-046: Exact (==) dependency pins in serverless job environments, lock-parity enforced

| Field | Value |
|---|---|
| **Date** | 2026-06-10 |
| **Status** | Accepted |
| **Deciders** | Karsten Nielsen |

## Context

Databricks serverless job environments are rebuilt whenever their dependency spec
changes — which includes every wheel bump, i.e. every release. At build time the env
re-resolves its dependency specs against PyPI. With floor specs (`silly-kicks>=4.20.1,<5`)
that made production silently track PyPI: on 2026-06-09/10 prod ran **three different
silly-kicks versions in one day** (4.21.0 → 4.21.1 → 4.21.2), none of them the
lock-tested 4.20.1 that the goldens and the ADR-045 perf A/B validated. The drift was
only visible because the executor env fingerprint (ADR-031/044) prints
`silly_kicks_version` per process.

This is the same class as the ADR-044 incident (prod environment diverging from what was
tested) — but a *newer-than-tested* version passes ADR-044's floor-based guard by design,
and the pre-existing `test_terraform_env_specs_align_with_pyproject` overlap test cannot
catch it either (a floor always overlaps a floor).

A tempting "cleanup" was also investigated and **rejected**: deduplicating env deps
against the serverless base image. Environment version 1's base ships numpy 1.23.5,
scipy 1.10.0, matplotlib 3.7.0, pandas 1.5.3 — far older than anything the lakehouse
runs. Dropping a "redundant-looking" `scipy` pin would silently DOWNGRADE prod to 1.10.

## Decision

Every PyPI dependency in every `environment` block of
`terraform/modules/workflows/main.tf` is an **exact `==` pin mirroring `uv.lock`** (the
versions local tests, goldens, and CI actually ran). Three sentinels in
`src/tests/test_terraform_env_dep_parity.py` enforce it: (1) `==`-only specs, (2) pin ==
uv.lock version (documented exemptions: `databricks-sdk`, whose source of truth is the
pyproject `[sdk]` extra — asserted by its own lockstep test; `statsbombpy`, terraform-only),
and (3) the overlap test treats an exact pin satisfying pyproject's range as the intended
state. Bumping a library version in prod now REQUIRES bumping pyproject + `uv lock` +
terraform together — the sentinel makes a partial bump fail CI.

## Alternatives considered

| Option | Pros | Cons | Why rejected |
|---|---|---|---|
| A. Keep floors, rely on the ADR-044 executor guard | no workflow change | the guard checks floor + install integrity — a newer-than-tested version passes by design | doesn't address the class |
| B. Dedupe deps against the serverless base image (build-speed play) | smaller installs | base image versions are years older (scipy 1.10.0) — removing a pin silently downgrades prod | dangerous; documented as a Chesterton fence in-file |
| C. Floor + upper-bound straddling the lock (`>=4.20.1,<4.21`) | narrower drift | still drifts within the band; resolution still nondeterministic per build | half-measure |
| D. Exact pins + lock-parity sentinels (chosen) | prod runs exactly what was tested; deterministic builds; faster resolution; version bumps become deliberate, reviewed events | every dep bump now touches terraform too (sentinel-guided, one-line) | — |

## Consequences

### Positive

- Prod can no longer run a library version that local tests/goldens never saw; the
  silent-drift class (both directions) is closed at CI time, complementing ADR-044's
  runtime guard.
- Env builds resolve deterministically (and marginally faster — no PyPI metadata walk).
- A wheel bump alone no longer changes any library version in prod.

### Negative

- Routine library upgrades gain one mandatory step (terraform pin edit) — by design;
  the sentinel error message names the exact line to change.
- `statsbombpy` is pinned outside the lock (terraform-only dep); its upgrades are manual
  and unguarded by lock-parity (exempted with reason).

### Neutral

- The pins moved prod *down* from silly-kicks 4.21.2 → 4.20.1 and numba 0.65.1 → 0.64.0
  (the tested versions; the ADR-045 A/B measured the newer ones at +7–8% slower anyway).
- The lakebase env's `databricks-sdk==0.114.0` was already exact; it gains a lockstep
  test against the pyproject `[sdk]` extra.

## Related

- **ADRs:** ADR-044 (executor env-drift guard — runtime complement), ADR-045 (the perf
  verification this drift confounded)
- **Code:** `terraform/modules/workflows/main.tf`,
  `src/tests/test_terraform_env_dep_parity.py`
- **External:** Databricks serverless environment version 1 package list (base-image
  versions cited above)

## Addendum — 2026-07-22: automate the lockstep, guard cross-env consistency

`==` exactness is **reaffirmed**; relaxing to `~=` was considered and **rejected** —
because envs re-resolve at build time, `~=X.Y.Z` (`>=X.Y.Z,<X.(Y+1).0`) permits the
patch drift (4.21.0 → 4.21.2) this ADR exists to stop, and *increases* intra-build drift.

Two additions keep `==` while removing the manual toil and strengthening consistency:

1. **Shared pure core `scripts/_tf_env_pins.py`** — one module owns the TF/lock/[sdk]
   parsers, the exemption policy, `resolve_desired_version` (exempt-first, then single
   lock version, then fork → `PinForkError`, then missing → `PinResolutionError`),
   `find_pin_drift`, and the cross-env invariant. Both the parity sentinel
   (`test_terraform_env_dep_parity.py`) and the sync CLI import it, so fixer ≡ checker by
   construction. Lives in `scripts/` (dev tooling; not `src/shared` which is stdlib-only,
   not `src/ingestion` which ships in the wheel); importable from tests via
   `pythonpath = ["."]`.
2. **`scripts/sync_tf_env_pins.py`** — human-invoked (never a CI autofix; the sentinel
   stays the gate). Bump workflow: `edit pyproject → uv lock → python scripts/sync_tf_env_pins.py`.
   It rewrites only the version substring inside each env block's `dependencies = [...]`
   span, and only the **code portion** of each line — a trailing or full-line comment
   (e.g. a version-shaped string in a rationale comment) is split off and preserved
   verbatim — keeping extras/comments/`concat`/formatting intact.
3. **Cross-env consistency guard** (`test_cross_env_pin_consistency`) — a package pinned
   in ≥2 env blocks must carry the same version. For **lock-managed** pins this is implied
   by lock-parity (each == the single lock value); its unique coverage is the **exempt**
   packages (`databricks-sdk`, `statsbombpy`), for which lock-parity is off.

**uv.lock version forks:** `parse_lock_versions` returns a *set* per package;
`resolve_desired_version` **fails loud** on a non-exempt multi-version fork rather than
guessing file-order-last. Today only `databricks-sdk` forks (0.117.0 dbt / 0.121.0
sdk/lakebase) and it is exempt (resolves from the `[sdk]` extra), so no pin trips it.

**Alternatives rejected:** (a) `python-hcl2` parse→modify→emit and (b) generating pins
into `*.auto.tfvars.json` — both **lossy on the inline comments** that carry this ADR's
per-pin rationale (the numba footgun, `xgboost-cpu`'s GPU-lib omission, the base-image
downgrade fence). Surgical version-substring rewrite is therefore the *correct* choice.

**Out of scope:** transitive cross-env forks (the `databricks-sdk` split lives in
different task envs, is constraint-driven, and detecting it needs full per-env resolution)
— documented limitation, not built.

---

## Amendment — 2026-07-27: CI dbt joins the lockstep; the runtime install is deleted

`dbt-live-ci` failed nightly from 2026-07-22 for this ADR's exact reason, at a site the sync tool
did not cover. The runner produced `manifest_main.json` with uv.lock's dbt-core (1.11.12) while
`scripts/ci/run_dbt_in_databricks.py` pip-installed a **range** (`>=1.10.0,<1.12.0`) into the live
job, which resolved 1.11.8. dbt rejected the newer manifest — `Field "macros" of type
Mapping[str, Macro] in WritableManifest has invalid value` — and exited 2.

Rather than pin the runtime install, it is **deleted**. `scripts/trigger_dbt_job.py` already
submitted an `environments` block; it now declares `dependencies` there, so dbt arrives with the
environment, version-locked, and `_DBT_PIN` / `_DBT_DATABRICKS_PIN` / `install_dbt()` are gone. A
pinned runtime install would still need hand-syncing; a declared environment does not.

Send `dependencies` alongside `environment_version`, never with `client` — the 2026-06-10
Databricks rollout rejects that pair with `INVALID_PARAMETER_VALUE` and broke this workflow once
already.

Remaining dbt pin sites ride `scripts/sync_tf_env_pins.py` and are checked by
`src/tests/test_ci_dbt_pin_parity.py`: the submitted environment, and the four `uvx --from` runner
invocations. **`pyproject.toml` is a consistency check only, never a rewrite target** — it is the
*input* to `uv.lock`, so syncing it would create pyproject → lock → pyproject. This ADR inverts
that direction for `databricks-sdk` alone (`ExemptStrategy.SDK_EXTRA`) because the dbt extra forks
that package in the lock; dbt has no equivalent fork, so the normal direction holds and the test
asserts floor ≤ lock.

### Follow-up — 2026-07-28: the fix above did not reach the job

The amendment landed and the very next nightly failed **identically**. The deleted
`install_dbt()` ran anyway, because the Databricks task does not execute
`scripts/ci/run_dbt_in_databricks.py` from the checkout — it executes a copy at
`/Workspace/Shared/luxury-lakehouse-ci/run_dbt_in_databricks.py`, uploaded by hand on
**2026-04-23** and never again. Measured on the failing run: deployed copy still contained
`_DBT_PIN` and `def install_dbt`; the repo file already contained neither. `scripts/upload_ci_shim.py`
existed, its docstring said "re-run when the shim changes", and **no workflow ran it**.

A pin is only as good as its delivery. `dbt-live-ci.yml` now runs the uploader immediately before
triggering, making the deployed shim a function of the triggering commit;
`test_dbt_live_ci_deploys_the_shim_before_triggering` asserts both its presence and its ordering
(uploading after the trigger would deploy for the *following* run).

Generalisation worth carrying: **an artifact executed from a mutable location outside the repo is
not covered by any repo-side test.** The test suite, the ADR and the diff all agreed the pin was
fixed; the running system disagreed for two nightlies, silently, because nothing compared them.
When a change targets code that runs elsewhere, verify the *deployed* copy, not the source.
