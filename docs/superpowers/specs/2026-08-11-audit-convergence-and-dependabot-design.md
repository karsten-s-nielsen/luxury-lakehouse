# Audit the production surface, and resolve Dependabot — design

**Status:** approved (design, rev 4), plan pending
**Date:** 2026-08-11
**Author:** Karsten Nielsen
**Follows:** [ADR-075](../adrs/ADR-075-expiring-exceptions-and-single-construction-sites.md) and its 2026-08-11 amendment
**Rev 2:** an external review measured three claims that invalidated rev 1's unit 2. See §8.
**Rev 3:** a second review found unit 2 audited the wrong artifact and named a remedy the tooling
cannot perform; §7's precondition was measured and is backwards. See §8.
**Rev 4:** a partial review ran the probe unit 2 was going to build and **flask is fixable today**,
not permanently capped. Unit 2's output inverts from ignore to fix. See §8.
**Sibling specs (separate cycles):** Taipy `129fd40` vendoring; lock/production convergence (§7)

---

## 1. Why now

Last cycle shipped `scripts/audit_resolutions.py` claiming CI audits **every** `uv.lock` resolution
rather than the one environment it installs. Measurement since has found the job does not run, the
claim is wrong in a second way, and — underneath both — that **production deployment is not
reproducible at all.**

### 1.1 The job does not run

First execution on a GitHub runner:

```
FileNotFoundError: Forced include not found: .../dbt_project/dbt_packages
...
FAIL: unignored findings in 4 resolution(s): base, taipy-app, dbt, sdk
```

Four defects, not the two rev 1 named:

1. `audit()` invokes `uv run pip-audit` **without `--no-project`**, so uv builds the editable
   install and hits the hatchling force-include of `dbt_project/dbt_packages` — gitignored, and
   materialised only by an explicit `dbt deps` step. It passed locally because the dev machine
   already had that directory. (The outer invocation in `cve-blocker-review.yml` already passes
   `--no-project`; only the inner call is wrong.)
2. `main()` maps **any** non-zero exit to "unignored findings", so a failure to *run* is reported as
   *vulnerabilities found*. This is the `BLOCKED`/`UNKNOWN` distinction built into
   `check_cve_blockers.py` in the same cycle — *"unverifiable is not the same as verified-good"* —
   not carried across to its sibling.
3. `uv export` includes default dependency groups, so the audit treats **dev tooling as
   production**. Measured: 237 packages exported, **141** with `--no-dev`; the 96-package delta is
   `torch`, the `nvidia-*-cu12` CUDA stack, `pytest`, `ruff`, `pyright`, `scikit-learn`,
   `openevolve`, `pip-audit` itself.
4. pip-audit exits **0** when a dependency cannot be collected (`Dependency not found on PyPI and
   could not be audited`). Packages that fail collection are reported clean — the same rule as
   defect 2, applied to a case it does not cover.

   **This defect is latent, not currently firing.** The obvious instance — torch's `+cu128` local
   version — is already mitigated: `strip_local_versions` rewrites it before pip-audit sees it, so
   torch collects and its advisory is found. `--strict` is therefore a **guard against a future
   instance, not a repair of a present one**, and §4's "observe RED first" cannot be satisfied for
   it. Fixing defect 3 also retires the local-version proxy from the production targets entirely:
   torch is dev-group, so after `--no-default-groups` the proxy applies only to the dev target.

### 1.2 Production is not reproducible

`hf_taipy_app/requirements.txt` is gitignored and regenerated **at deploy time** by
`manage_space.py` via `uv pip compile pyproject.toml --extra taipy-app`, which resolves fresh and
always takes the newest satisfying release. **Two deploys of the same commit can ship different
versions.** The lock lags because nothing routinely refreshes transitive pins; a fresh compile never
lags. The divergence is structural, not accidental.

Measured, 2026-08-11 (lock export with `--no-dev`, vs the Space's compile):

| | packages |
|---|---|
| `uv export --extra taipy-app --no-dev` | 141 |
| `uv pip compile … --python-version 3.10 --python-platform linux` | 136 |
| version differences | **51** |
| **direction: backwards / forwards** | **50 / 1** |

Environment markers filter essentially nothing (141 → 140 evaluated for linux/CPython 3.10/x86_64).
The remaining 4-package gap is downstream of the version moves, not a defect.

### 1.3 The production surface has zero coverage, by construction

Not "is under-covered" — **no mechanism can see it at all**, and each exclusion is deliberate:

- `.github/dependabot.yml` declares three ecosystems: `uv` (`/`), `github-actions` (`/`), and
  `terraform` (`/terraform/environments/dev`). **None covers `hf_taipy_app/`.**
- `hf_taipy_app/requirements.txt` is gitignored *specifically* so dependabot-pip cannot see it —
  `.gitignore` records the reason: committing it creates unmanageable false-positive PRs.
- The `uv` ecosystem and `audit_resolutions.py` both read `uv.lock`, which §1.2 proves is a
  different resolution.
- And per §1.2 the surface **mutates without a commit**, so no repo-triggered gate can ever see it.

This is the only thing users actually run, and it is observed by nothing. That — not the specific
advisories below — is why unit 2 exists.

What is currently exposed:

- **`flask==3.1.1`** in production carries live advisory `PYSEC-2026-2151` (fixed in 3.1.3). The
  lock has 3.1.3, so the lock-based audit cannot see it.
- Production runs **`cryptography==50.0.0`** — the *fixed* version — because `mlflow` (which caps
  `<50`) is not in the `taipy-app` extra. The lock has 49.0.0.
- `taipy-gui` is **4.1.2** in production against 4.1.1 in the lock.

## 2. Scope

Three units, in this order. Unit 1 is the gate that makes unit 3 safe.

### Unit 1 — make the audit trustworthy (four defects)

- `audit()` runs `uv run --no-project --with "pip-audit==<pinned>" pip-audit …`. pip-audit audits a
  requirements FILE; it never needed the project, so the editable build disappears and
  `dbt_packages` stops mattering. The tool is **pinned** — an unpinned security tool drifts silently.
- Export with `--no-default-groups`, and audit **production surfaces and dev tooling as separate,
  labelled targets**. A dev-only advisory must not be reported against a production claim.
- Three outcomes — **CLEAN / FINDINGS / UNKNOWN** — classified from `pip-audit -f json`
  **structurally**, not by matching English prose. Non-zero exit alone must never mean "findings".
- `--strict`, so a dependency that could not be collected fails rather than passing as clean.
- UNKNOWN fails the job, distinguishably from FINDINGS.

### Unit 2 — audit the DEPLOYED artifact

Audit the `requirements.txt` **fetched from the Space**, not a fresh compile:

```python
hf_hub_download(repo_id="luxury-lakehouse/soccer-analytics-app",   # and luxury-lakehouse/staging
                repo_type="space", filename="requirements.txt")
# repo ids per manage_space.py: luxury-lakehouse/soccer-analytics-app (prod), luxury-lakehouse/staging
```

Re-running `uv pip compile` at audit time would be a *third* fresh resolve, at a third moment,
matching neither the last deploy nor the next — §1.2's own argument applied to §2's mechanism. The
job would certify a pin set that may never have existed in production. `manage_space.py` uploads
via `upload_folder` and `requirements.txt` is **not** in `IGNORE_PATTERNS` (verified), so the real
pin set for each target is one call away. Auditing both Spaces additionally detects
**staging/production drift**, which a compile-at-audit-time target structurally cannot see.

Ordering inside unit 2 is RED-first, same as unit 1: add the target → observe flask FINDINGS → add
the entry → GREEN. Landing both together is a gate first seen green.

**flask is FIXED, not ignored.** Rev 3 planned an ignore entry on the belief that
`taipy-rest 4.1.1`'s `flask<=3.1.1` was a permanent upstream cap. Running the probe — which is the
thing unit 2 was going to build — disproves it. Measured:

```
uv pip compile pyproject.toml --extra taipy-app --python-version 3.10     --python-platform linux -c <(echo 'flask>=3.1.3')      # 136 packages, exit 0
```

**136 before, 136 after. Nothing added, nothing removed.** Four versions move:

```
flask                 3.1.1 -> 3.1.3     <-- the fix lands
taipy-rest            4.1.1 -> 4.1.0
apispec               6.8.2 -> 6.6.1
apispec-webframeworks 1.2.0 -> 1.1.0
```

Under `check_cve_blockers.py`'s taxonomy that is **COLLATERAL, not BLOCKED** — and the collateral is
inert for this application: `taipy-rest` and `apispec` appear **nowhere** in `hf_taipy_app/`
(verified), arriving only as transitives of the `taipy` metapackage. The Space is a GUI app that
never touches the REST layer.

So the fix is **one line in `[tool.uv] constraint-dependencies`**, beside the existing
`pyasn1>=0.6.4`. That block is honoured by the production compile — uv annotates the output
`# via -c (workspace)` — which also means `check_cve_blockers.py`'s design principle, *"the probe
mechanism IS the fix mechanism"*, survives intact at production scope.

### Unit 3 — resolve 5 Dependabot PRs

`#512` dev-tooling group, `#511` hashicorp/aws, `#510` databricks/databricks, `#505` openevolve,
`#503` optuna-integration[mlflow]. One at a time; prod deps are deliberately ungrouped because
batching perturbs the conflict-fork resolution. **Re-derive the open list at plan time** — ~14
`dependabot/*` branches exist and several are superseded.

## 3. Decisions

**D1. `--no-project`, not a dbt-deps step.** The rejected alternative materialises `dbt_packages`
before the audit, paying a dbt-core tool install and ~1 min per run to satisfy a force-include for a
build we do not want. pip-audit needs a file, not a project.

**D2. Classify structurally, and pin the tool.** `-f json` parsed for a `dependencies` array decides
ran-vs-did-not; prose matching would make a security gate depend on an upstream tool's wording,
which is not an API. `--strict` closes the silent-skip case. `pip-audit` is pinned so the gate's
behaviour does not drift under it.

**D3. Audit the production surface; do NOT converge the resolutions this cycle.** Rev 1 proposed
making the Space install from the lock. Measured, that trades one CVE fix for **50 production
downgrades**, including `cryptography 50.0.0 → 49.0.0`, which **re-opens PYSEC-2026-3552 in
production**. Auditing the compiled surface achieves the visibility goal and keeps production's
better cryptography.

**D4. The flask fix is not available by convergence, and the reason is load-bearing.**
`taipy-rest 4.1.1` requires `flask<=3.1.1`; `4.1.0` requires `flask<3.2`. `taipy-gui` is **not** the
cap — 4.1.1 and 4.1.2 both allow `<3.2`. The lock reaches flask 3.1.3 only because it holds the
*older* taipy-rest. So convergence buys the flask fix with a taipy-rest downgrade, and raising the
Taipy family to production versions re-imposes the cap and cancels it. Ignore-with-`blocked_by` is
the correct remedy.

**D5. No production package moves backwards — except as the measured, named price of a security
fix.** The carve-out is necessary and narrow: taking flask 3.1.3 moves `taipy-rest` and the two
`apispec` packages back, and without the clause D5 would auto-forbid the best available action in
this cycle. The price must be *measured* (a full before/after resolution diff), *named* in the
commit and the entry, and *bounded* (no packages added or removed). Convenience never qualifies. Any change to what the Space installs is gated by an
explicit no-package-moves-backwards assertion, run **before** the change is designed, not after.
Rev 1 stated this rule and then proposed a unit that violated it on 50 packages, because direction
was never measured. **Measuring direction is a hard gate, not an intention.**

**D6. silly-kicks `#506` stays open and held.** Operator decision: parallel work on that library
continues.

**D7. Terraform and uv PRs are separate surfaces.** `#510`/`#511` touch the terraform lockfile and
are verified by Terraform Plan; they share nothing with `uv.lock`.

**D8. Every Dependabot PR is incomplete as-opened.** Under ADR-046 the terraform env pins mirror
`uv.lock`, and Dependabot does not run `scripts/sync_tf_env_pins.py`. Each uv PR is rebase →
`uv lock` → `sync_tf_env_pins.py` → gates.

**D10. `scope:` DISPATCHES the probe; it never exempts an entry from being probed.**
`.pip-audit-ignores.yml` entries gain `scope:` (default `lock`). `scope: production` runs the probe
against the production resolution instead of the lock:

```
uv pip compile pyproject.toml --extra taipy-app --python-version 3.10     --python-platform linux -c <one-line floor>
```

Rev 3 had `scope:` *exempt* production entries from probing. That was wrong on three counts. It is
**more machinery than the thing it defers** — the compile probe is simpler than the existing lock
probe (no `pyproject.toml` splicing, no `_CONSTRAINTS_ANCHOR`, no finally-block restore, no
"confirm the probe left no residue" workflow step, because nothing in the repo is mutated) and
resolves in ~360 ms, whereas the exemption route costs a field *plus* an exclusion set *plus* a
pinned `_EXPECTED_*` set *plus* a standing explanation. It **reintroduces the ADR-075 failure mode**
— an exempt entry is a hand-verified claim that ages silently, which is precisely what
`check_cve_blockers.py` exists to prevent, in a spec whose `Follows:` is ADR-075. And it would ship
an entry **unverifiable from day one** in a gate whose premise is that exceptions re-prove
themselves.

Classification reuses the existing BLOCKED / COLLATERAL / MOVED / UNKNOWN logic verbatim, against a
name→version dict instead of a lock dict. Generalising the prober to arbitrary targets belongs in
§7; the first instance of it belongs here.

**D9. `#504` (dbt-core 1.12.0) is excluded.** It is not a version bump. Its branch removes
`dbt-semantic-interfaces` and adds `metricflow`, `sqlglot`, and
**`dbt-core-experimental-parser==2.0.0a5` — a pre-release alpha — into the production lock**, which
under ADR-046 feeds the serverless environment. Its only real validation surface, `dbt-live-ci.yml`,
is `schedule:` + `workflow_dispatch` with **no `pull_request` trigger**, so a parse regression lands
green and breaks the next morning. It needs its own cycle with a live `dbt parse`/`dbt build` and an
explicit decision on the alpha parser. `dbt-databricks` has a compatible 1.12.3, so this is a
scheduling decision, not a compatibility wall.

## 4. Constraints carried

- **Observe the gate RED first.** Unit 1's fix must be demonstrated failing against today's broken
  state before it is made to pass. Target the inner `audit()` call — the outer workflow invocation
  is already correct.
- **No wheel bump unless a packaged module changes** (`src/{ingestion,analytics,shared,workflows,evolve}`
  plus `dbt_project/**` force-includes).
- **`hf_taipy_app/requirements.txt` is generated, never committed.** Committing it was rejected
  previously because it generates unmanageable dependabot-pip PRs. Unit 2 reads a generator's
  output; it does not add a file.
- **Gate scripts redirect, never pipe** — a pipe reports the exit of the last stage.
- **Live-Databricks tests need a fresh OAuth token.** A token older than ~1 h produces failures
  indistinguishable from regressions.
- Each commit, push, PR and merge requires **separate explicit operator approval**.

## 5. Open questions

**OQ-1 — what should the lock/production relationship be?** Deferred to its own cycle (§7), but the
question belongs on the record now: production currently resolves fresh at deploy time, which is
non-reproducible. The answer is not obviously "pin to the lock" — see M4/§7.

**OQ-2 — does `dbt parse`/`dbt build` succeed under the metricflow + sqlglot stack?** For `#504`'s
cycle. The historical failure was producer/consumer *skew*, which D8's lockstep eliminates by
construction; the uncovered risk is an **alpha parser changing parse behaviour on the real project**.

## 6. Risks

| Risk | Mitigation |
|---|---|
| Unit 3 lands while the gate is broken | ordering: unit 1 first, non-negotiable |
| Dev-only advisories reported as production exposure | D2 labelled targets |
| A collection failure passing as clean | `--strict` |
| pip-audit behaviour drifting under the gate | pinned version |
| Superseded dependabot branches confusing the list | re-derive at plan time |
| Auditing a pin set production never ran | unit 2 fetches the **deployed** file, not a fresh compile |
| flask entry reading MOVED weekly, training readers to ignore the gate | D10 `scope:` + explicit pinned exclusion |
| Two mechanisms in one script (`uv export` targets vs a fetched file) | label the targets; they are separate code paths with separate failure modes |
| Network-resolved target is non-deterministic run to run | fetching the deployed artifact removes this; a fresh compile would not |

## 7. Out of scope — and why each is deferred rather than dropped

**Lock/production convergence.** Reproducible deployment is the right destination: the artifact you
tested should be the artifact you ship, and today it is not. The precondition rev 2 stated —
"refresh the lock forward first" — has since been **measured and is backwards**.

`uv lock --upgrade --dry-run` (non-destructive, tree verified clean afterwards): **92 forward, 6
backward**, and the backward ones are exactly the ones that matter:

```
flask          3.1.3 -> 3.1.1     <-- the lock LOSES the fix this cycle exists to surface
mlflow         3.15.1 -> 3.2.0    (+ mlflow-skinny, mlflow-tracing)
gunicorn       25.3.0 -> 23.0.0
cachetools     7.1.6  -> 6.2.6
flask-cors     v5.0.1, v6.0.2 -> v5.0.1   (drops 6.0.2)
```

The flask regression is D4's mechanism confirmed by execution: `--upgrade` takes
`taipy-rest 4.1.0 -> 4.1.1`, which re-imposes `flask<=3.1.1`. It also takes
`cryptography 49.0.0 -> 50.0.0` at exactly the COLLATERAL price `check_cve_blockers.py` already
records for that floor — the mlflow backtrack, gunicorn and cachetools downgrades, and flask-cors
losing 6.0.2, re-opening three other alerts.

And a blanket `--upgrade` is **not a neutral refresh**: the same dry run pulls
`dbt-core 1.12.0` + `dbt-core-experimental-parser v2.0.0b1` (D9 excludes) and
`silly-kicks 4.43.0 -> 4.79.0` (D6 holds). It merges the Dependabot queue and overrides two explicit
operator decisions.

So the convergence cycle's precondition is **targeted `--upgrade-package` moves with direction
asserted per package**, never a blanket `--upgrade`. That proposal has now been executed rather than
asserted: 50 packages probed individually with `--upgrade-package <pkg> --dry-run`, giving **39
clean single-package forward upgrades with zero collateral**, 5 no-ops, 5 benign, and **1 genuine
hazard**. Two results matter beyond the tally:

- **`cryptography` is a no-op under targeting** — uv will not take 50.0.0 without the mlflow
  backtrack, and targeted mode declines to pay it. Targeted upgrades are therefore *conservative by
  construction*: they cannot silently buy the collateral a blanket `--upgrade` buys.
- **`taipy-rest` is the single hazard**, and it is the same poison: `--upgrade-package taipy-rest`
  forks flask and puts 3.1.1 into the taipy-app fork. It must never be upgraded in the lock without
  a flask floor present — which unit 2's `flask>=3.1.3` constraint converts from a silent regression
  into a hard resolution error.

**Stated limit:** each was measured individually against today's lock. Applying 39 sequentially
moves the baseline, so this proves *no single targeted upgrade is individually poisonous*, not that
all 39 together are safe. Direction must be re-asserted after each batch, per D5. That cycle must also weigh a coupling cost this
spec does not solve: pinning production to the lock makes the Space **inherit ceilings from packages
it never installs** — `mlflow` caps `cryptography <50`, and today's fresh compile escapes that and
lands the fixed 50.0.0.

**Extending the blocker probe to production scope.** D10 excludes `scope: production` entries from
the lock probe. Teaching `check_cve_blockers.py` to attempt floors against a production target would
make them re-provable and generalises to any future production-surface entry. Deferred, not dropped.

**Taipy `129fd40` vendoring.** Its own cycle. It depends on nothing here now that D3 leaves
production on `taipy-gui 4.1.2` — that is the version to patch.

**`#504` dbt-core 1.12.0** — D9. **silly-kicks `#506`** — D6. **BENCH-1** — recorded in `TODO.md`.

## 8. What rev 1 got wrong

Recorded because the failure mode is instructive, not for completeness.

Rev 1 measured the lock/Space divergence **without `--no-dev`**, so its headline numbers (237 vs 136,
"101 present only here (other platforms/markers)") were inflated by 96 dev-group packages. Two of its
four "consequence classes" — `setuptools` and `torch` absent from production — were artifacts of that
mistake, not evidence of divergence; both are dev-group packages that were never in the Space. The
correct reading is defect 3 in §1.1.

Rev 1 also never measured the **direction** of the 51 version moves before proposing convergence, and
stated D5 ("no package moves backwards") in the same document as a unit that violates it on 50
packages. And it presented the flask fix as a free consequence of convergence without checking which
package caps flask — it is `taipy-rest`, whose *older* version is what lets the lock reach 3.1.3.

The general lesson, now D5: **measure direction before proposing a resolution change.** A version
difference is not a version improvement.

### Rev 2

Rev 2 argued in §1.2 that a fresh compile is non-reproducible — and then had unit 2 audit **a fresh
compile**, generated "the same way `manage_space.py` generates it". The document's own best insight
was not applied to its own mechanism one section later. The audited pin set would have matched
neither the last deploy nor the next. Fixed by auditing the artifact actually deployed to the Space.

Rev 2 also asserted the flask entry would be "re-provable by `check_cve_blockers.py`" without
checking what that tool probes. It probes the **lock**, where flask is already 3.1.3 — so the entry
would have returned MOVED every week, recommending a fix production cannot take, and would have
flipped to BLOCKED after any lock upgrade. A blocker whose verdict depends on which resolution you
happen to probe is the wrong shape for that machinery; hence D10's `scope:` field.

And rev 2's §7 precondition — "refresh the lock forward first" — was stated as unverified and turned
out to be **backwards**: `uv lock --upgrade` moves flask 3.1.3 → 3.1.1, destroying the fix, and
merges two PRs that D6 and D9 explicitly exclude.

### Rev 3

Rev 3 wrote off `PYSEC-2026-2151` as a permanent upstream cap and designed an ignore entry, a new
`scope:` schema field, an exclusion set and a test-suite pin around that belief — **without once
running the probe it was specifying**. One 360 ms command disproves it: the fix is available for
three inert package moves, and the correct output is a one-line constraint, not an exception. The
elaborate machinery existed to manage a problem that was not there.

Rev 3 also justified unit 2 by restating three already-known advisories, which invited the fair
challenge "why build a gate for one advisory?" The real justification — that the production surface
is observed by **nothing**, deliberately, and mutates without a commit — was in §1.2 all along and
never carried into §1.3.

And rev 3 asserted a staging Space at `…-app-staging` from memory. It does not exist; the id is
`luxury-lakehouse/staging`. When the first check 404'd I concluded staging did not exist — wrong
again, in the opposite direction, from the same habit.

The pattern across all four revisions: **an assertion about a tool's behaviour, made without
running the tool.** Rev 1 measured the wrong thing (dev group included, direction unmeasured); rev 2
did not run `uv lock --upgrade` before naming it a precondition, and did not read
`check_cve_blockers.py` before naming it a remedy. Every correction came from execution.
