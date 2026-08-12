# ADR-076: Deploy the requirements artifact that was validated, not a fresh resolve

| Field | Value |
|---|---|
| **Date** | 2026-08-11 |
| **Status** | Accepted |
| **Deciders** | Karsten Nielsen |

## Context

`scripts/manage_space.py` generates the Taipy Space's `requirements.txt` at deploy time by running
`uv pip compile pyproject.toml --extra taipy-app --python-version 3.10 --python-platform linux`.
That call sits **inside the upload path**, at two separate sites: `_dry_run` (`:467`) and `_deploy`
(`:521`, immediately before `upload_folder`).

A `uv pip compile` takes the newest release satisfying each constraint *at the moment it runs*. It
is therefore not reproducible across moments, which is the same property that made auditing a
freshly-compiled requirements file useless and forced the audit to fetch the deployed file instead
(see the 2026-08-11 audit-convergence spec §1.2). Measured 2026-08-11, `uv.lock` and a fresh
production compile differ on **51 package versions — 50 of them backwards** relative to the lock,
so the two resolutions are meaningfully distinct rather than incidentally so.

The consequence is that a `deploy staging` followed by `deploy production` performs **two
independent resolutions**, and a preceding `--dry-run` a third. Staging validates one pin set;
production ships another. Any pre-deploy check — including the D5 no-package-moves-backwards
assertion this project requires before changing what the Space installs — inspects a file that is
overwritten before production receives anything. Staging reads as a rehearsal and is not one.

The forcing function is the flask fix. Production runs `flask==3.1.1` (`PYSEC-2026-2151`, fixed in
3.1.3), and taking the fix moves `taipy-rest`, `apispec` and `apispec-webframeworks` backwards as
its measured price. Shipping that deliberately requires knowing that the artifact inspected is the
artifact deployed. Today it is not.

## Decision

`manage_space.py` compiles requirements from exactly one place, and gains a `--no-compile`
/ `--expect-sha256` pair so a single compiled pin set can be validated once and then shipped to
both Spaces. `--no-compile` without `--expect-sha256` is rejected by the argument parser.

## Alternatives considered

| Option | Pros | Cons | Why rejected |
|---|---|---|---|
| A. Leave it; assert after the fact that both Spaces' `requirements.txt` are identical | Zero code change; reuses the audit's existing fetcher | Detects a divergence only once production already has it; cannot gate | Detective, not preventive — the whole point of a staging deploy is to fail *before* production |
| B. Commit `hf_taipy_app/requirements.txt` to the repo and deploy it verbatim | Fully reproducible; reviewable in PRs; achieves this ADR's own goal *without* flags, hashes or operator discipline | Creates a second pin set to keep in step with `uv.lock`, with no `sync_tf_env_pins.py`-style tool for it; the in-repo copy drifts from what deploy produces unless CI regenerates and diffs it, and **no such CI check exists**; reverses #198 and #450's direction of travel, around which `allow: direct` in `dependabot.yml` was tuned | The drift is unpoliced: we would trade a deploy-time guarantee for a CI-time one we would first have to build |
| C. Pin the Space to `uv.lock` instead of compiling | One resolution for the whole project; no divergence to manage | Makes the Space inherit ceilings from packages it never installs — `mlflow` caps `cryptography <50`, and today's fresh compile escapes that and lands the **fixed** 50.0.0 | Would import a security ceiling from a package absent from production |
| D. `--no-compile` + `--expect-sha256`, single `_prepare_requirements()` site | Preventive; the validated artifact is provably the shipped one; no change to the default path | Adds two flags and a hash to the deploy runbook; a stale on-disk file is possible if the hash check is skipped | — |

Option A is retained *in addition* to D, as an end-to-end confirmation on the live artifacts (see
Consequences → Neutral).

**Option B is rejected on maintenance grounds, NOT on Dependabot noise — that reason has expired.**
`hf_taipy_app/requirements.txt` was un-committed at **#198** because the `pip` ecosystem generated
transitive-CVE spam (`#138`, `#140`–`#143`, `#186`, `#187`, `#189`, `#193`–`#195`, `#197`). That
ecosystem was then **removed at #450** in favour of `uv`. `.github/dependabot.yml` today declares
only `uv` (`/`), `github-actions` (`/`) and `terraform` (`/terraform/environments/dev`); Dependabot
scans what is configured and does not auto-discover requirements files, so committing that file now
would produce **zero** Dependabot PRs. The file's own `.gitignore` comment still asserts the old
reason, and `dependabot.yml:17-19` already contradicts it in the past tense — *"it is no longer
committed (.gitignore), and uv.lock does not have that flaw."*

This is recorded because option B is the strongest competitor to option D, not a fringe idea: a
committed, reviewed, verbatim-deployed pin set delivers *the artifact you validated is the artifact
you ship* more completely than D does. Anyone revisiting this decision should weigh it on the drift
and two-pin-sets arguments above, and must not re-reject it by re-deriving the Dependabot reason
from the stale `.gitignore` comment.

The stale-file risk in D is closed by construction rather than by documentation: `--no-compile`
alone would ship whatever happens to be on disk, so the parser refuses it without an expected
digest. Making the safe path the only path is preferred here over making it the documented one,
consistent with this project's fail-closed posture on security controls.

## Consequences

### Positive

- The artifact validated by the pre-deploy D5 manifest is provably the artifact production
  receives — **provided the expected digest is emitted by the manifest itself**, not recomputed
  from the file at deploy time. Recomputing asserts `sha256(file) == sha256(file)` in the one mode
  that never rewrites the file: a comparison that cannot fail, and a guarantee that would be
  assumed rather than proven. The manifest prints `VALIDATED_SHA256=` for exactly this reason.
- Staging becomes a real gate for the dependency set: both Spaces receive byte-identical
  `requirements.txt`.
- A pre-deploy check acquires meaning it did not previously have. The D5 assertion — required
  before any change to what the Space installs — can now be trusted, because nothing recompiles
  between the check and the upload.
- `_compile_requirements()` returning its digest gives the deploy log a stable artifact identity,
  which is what makes "same pin set" checkable after the fact.

### Negative

- Two more flags on the deploy runbook, and a hash that must be carried between commands. A
  routine deploy that does not care about artifact identity now has a second, longer form.
- Every production deploy now requires two extra flags. A `deploy production` without
  `--no-compile` is refused outright, following the existing `_require_force_for_production`
  convention (`manage_space.py:299-303`) — so the guarantee is structural on production, at the
  cost of a longer runbook for the one target where the runbook matters most.
- Staging deliberately keeps the compiling path: staging is where the artifact is *produced*, and
  forcing the flags there would leave nowhere to produce one. The guarantee is therefore structural
  on production and opt-in on staging, which is the asymmetry the risk actually has.
- One more place that must stay in sync if the requirements location ever moves.

### Neutral

- The staging≡production assertion (option A) is kept even though D makes it hold by construction.
  It is the only check that would catch `--no-compile` silently not being honoured, and it measures
  the live artifacts rather than mocks.
- Staging remains **158 files** to production's **156**. This ADR makes the *pin set* identical; it
  does not make the file sets identical, and staging is still an approximation in other respects.
- `_compile_requirements()`'s relative `-o` argument is left untouched. Its docstring
  (`manage_space.py:163-165`) attributes the relative form to "CI's freshness check", and no
  workflow in `.github/workflows/` currently references `requirements.txt` — so that check has
  either moved or lapsed. Determining which is a separate question from this decision, and the
  fence stays up until someone answers it.

## Related

- **Specs:** `docs/superpowers/specs/2026-08-11-audit-convergence-and-dependabot-design.md` (§1.2
  establishes that a fresh compile is non-reproducible; D5 requires the no-backwards-move assertion
  this ADR makes trustworthy)
- **Plans:** `docs/superpowers/plans/2026-08-11-audit-production-surface-and-dependabot.md` —
  Task 5a implements the seam, Task 5b consumes it
- **ADRs:** builds on `ADR-075` (an exception without a revisit condition, and a cross-cutting
  concern without a single construction site, are both states that nothing observes). The two
  compile call sites are precisely a cross-cutting concern without a single construction site;
  `_prepare_requirements()` is that site.

## Notes

The structural guarantee is a test, not a convention. `src/tests/test_manage_space_requirements.py`
**parses `manage_space.py` with `ast`** and asserts there is exactly one `ast.Call` to
`_compile_requirements`, and that its enclosing function is `_prepare_requirements`. The defect
being prevented was *two call sites*, so a test that only exercised flag behaviour would not
prevent its return.

**Do not "simplify" that to `source.count("_compile_requirements()")`.** A count assertion is
broken by any docstring that names the function — including the one on `_prepare_requirements`
explaining why a single call site exists. That formulation was written, found broken, and replaced
before this ADR was accepted; the AST form follows the in-repo precedent in
`src/tests/_delta_write_ast.py`.

Measurement behind the flask carve-out, 2026-08-11:

```
$ printf 'flask>=3.1.3\n' > c.txt
$ uv pip compile pyproject.toml --extra taipy-app --python-version 3.10 \
      --python-platform linux -c c.txt
Resolved 136 packages           # 136 before, 136 after — nothing added, nothing removed
  flask                 3.1.1 -> 3.1.3      (the fix)
  taipy-rest            4.1.1 -> 4.1.0      (the price)
  apispec               6.8.2 -> 6.6.1
  apispec-webframeworks 1.2.0 -> 1.1.0
```

`taipy-rest` and `apispec` appear nowhere in `hf_taipy_app/`; both arrive only as transitives of
the `taipy` metapackage, and the Space is a GUI app that never touches the REST layer.
