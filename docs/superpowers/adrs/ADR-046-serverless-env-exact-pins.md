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
