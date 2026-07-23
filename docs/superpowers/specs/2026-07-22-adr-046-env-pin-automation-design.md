# ADR-046 env pins — automate the fence, make it taller

| Field | Value |
|---|---|
| **Date** | 2026-07-22 |
| **Status** | Design — approved, pre-implementation |
| **Author** | Karsten Nielsen (with Claude) |
| **Amends** | [ADR-046](../adrs/ADR-046-serverless-env-exact-pins.md) |
| **Review** | Incorporates parallel-critic (d32) M1–M9 (round 1) + R1–R4 (round 2), verified against the tree 2026-07-22 |

## Context

ADR-046 mandates that every PyPI dependency in every `environment` block of
`terraform/modules/workflows/main.tf` is an **exact `==` pin mirroring `uv.lock`**,
enforced by three sentinels in `src/tests/test_terraform_env_dep_parity.py`. Its
founding incident was *patch*-level drift: because serverless **rebuilds each env and
re-resolves specs against PyPI on every wheel bump**, floor specs let prod run
silly-kicks **4.21.0 → 4.21.1 → 4.21.2 in one day**, none of them the lock-tested
version.

Two frictions were raised for re-examination (the "Task 6" backlog item, originally
framed as "relax `==` → `~=`"):

1. **Drift / inconsistency is the top concern** — the same package resolving to
   different versions in different parts of a deployed build.
2. **Routine-bump toil** — every library bump requires a manual three-way lockstep
   (`pyproject.toml` + `uv lock` + `main.tf`).

### Why `~=` is rejected

`~=X.Y.Z` means `>=X.Y.Z,<X.(Y+1).0` — it *permits* patch drift (`~=4.21.0` allows
4.21.0 → 4.21.2). Because envs re-resolve at build time, any `~=` lets an untested
newer patch land in prod that no test/golden ever saw. A blanket `==`→`~=` relaxation
therefore **re-opens the exact hole ADR-046 was built to close**, and *increases*
intra-build drift — the direct opposite of concern (1). `~=` is off the table.

The correct response is to **keep `==` exactness**, remove the *manual* toil (concern 2)
with automation, and **strengthen** the consistency guarantee (concern 1) with a new
guard. silly-kicks stays strictly exact — its bumps are deliberate functional events
that require table recalculation, and it is the single most important pin to keep
consistent across the build.

## Goals

- Keep `==` exactness on every serverless-env pin (no safety regression).
- Reduce routine-bump friction: replace hand-editing `main.tf` with one command.
- Strengthen intra-build consistency: enforce "same package → same version across env
  blocks" at CI time, surfacing intentional splits for explicit review.
- **Fixer ≡ checker by construction** — the automation and the sentinel share one pure
  policy core, so they cannot silently disagree.

## Non-goals

- No `~=` / floor relaxation anywhere.
- No change to silly-kicks handling (stays exact + deliberate).
- No CI autofix-commit (the sync tool is human-invoked; the parity tests remain the
  gate). This preserves the deliberateness of every version bump.
- No transitive-closure resolution in tests (see Scope & limitations). Note this is
  distinct from M2's within-tool fork policy.

## Design

### Component 0 — shared pin-policy core `scripts/_tf_env_pins.py` (the M1 keystone)

A **pure** module (no Databricks/Spark; stdlib + `packaging` + file reads) that owns the
policy exactly once. Both the sentinel test and the sync CLI import it, so the fixer and
checker are the *same* logic rather than two implementations kept in sync by a test.

Public surface (all pure — text/data in, values out; file I/O lives in the adapters):
- `parse_tf_env_deps(text) -> dict[env_key, dict[pkg, spec]]` — moved verbatim from the
  test (with its `concat(...)`/inline-list tolerance and `var.wheel_path` skip).
- `parse_lock_versions(text) -> dict[pkg, set[str]]` — **returns the set of distinct
  versions** per package (M2: no longer collapses a fork to file-order-last).
- `parse_sdk_extra_pin(pyproject_text) -> str` **(R2)** — reads the `[sdk]` extra's
  `databricks-sdk==X`; replaces the inline regex in today's
  `test_lakebase_sdk_pin_matches_pyproject_extra`, so the pyproject-side source of truth
  is parsed in the core, not re-regexed in an adapter.
- `EXEMPT: dict[pkg, ExemptRule]` **(R3)** — maps each exempt package to a *resolution
  strategy* (`SDK_EXTRA` | `LEAVE_AS_IS`) plus a reason string, so adding a third exempt
  package is a one-line data entry, not branching edited in two places. Today:
  `databricks-sdk → (SDK_EXTRA, …)`, `statsbombpy → (LEAVE_AS_IS, …)`.
- `resolve_desired_version(pkg, *, lock, sdk_extra_pin) -> str | None` — the single
  source-of-truth policy, checked **in this order (the order is the subtle part — R1):**
  1. **exempt first** — if `pkg` in `EXEMPT`, apply its strategy (`SDK_EXTRA` → the
     `sdk_extra_pin`; `LEAVE_AS_IS` → `None`) and return **without consulting the lock**.
     So an exempt package that also happens to be a lock fork (today's `databricks-sdk`)
     never reaches fork detection;
  2. lock-managed with exactly one lock version → that version;
  3. **lock-managed with >1 distinct lock version → raise `PinForkError`** (M2 fail-loud:
     never guess a fork; require an explicit `EXEMPT` entry);
  4. not in lock and not exempt → `None` with a reported reason (the zero-match case).
- `find_pin_drift(tf_text, lock, sdk_extra_pin) -> list[Drift]` **(R4)** — the parity check
  itself, pure over injected text/data. **Both** the zero-arg sentinel (reads the real
  files, then calls this) **and** the M8 drift-fixture e2e (calls it on fixture text) go
  through it — so the e2e cannot be vacuous against hardcoded module paths.
- Name normalization is centralized (reuse the existing `_parse_dep_line` `_`→`-`
  lowercasing) so `huggingface_hub` (TF) resolves against `huggingface-hub` (lock) — M6.

**Location rationale** (state in the ADR): not `src/shared/` (stdlib-only; this needs
`packaging`), not `src/ingestion/` (ships in the wheel; this is dev tooling). `scripts/`
is dev tooling already covered by ruff + pyright and already importable from tests
(`pythonpath = ["."]`; `scripts/__init__.py` exists; e.g. `from scripts.dbt_build_and_refresh
import main`). The `_`-prefix marks it internal (imported, not a CLI entry point).

**This is a test-first refactor of *passing* sentinels:** extract the three functions +
exempt map out of `test_terraform_env_dep_parity.py` into the core, have the test import
them back, and confirm the existing suite stays green — the current green tests are the
regression net.

### Component 1 — `scripts/sync_tf_env_pins.py` (the rewrite adapter)

The CLI is a thin adapter over Component 0: it asks the core for each pin's desired
version and rewrites `main.tf` to match.

- For each `"pkg==X"` entry inside every `environment { spec { dependencies = [...] } }`
  block, rewrite **only the version `X`** to `resolve_desired_version(pkg, …)`.
- **Rewriting is confined to the parsed `dependencies = [...]` span** (reuse the exact
  block spans `parse_tf_env_deps` already locates) — **never a whole-file regex** (M4).
  This provably protects the version-shaped strings living in comments (e.g. `main.tf`'s
  `4.21.0 → 4.21.1 → 4.21.2 … 4.20.1` rationale block).
- **Surgical** — touches only the version substring: preserves extras
  (`silly-kicks[das,ghost-gk,parse-dfl]`), comments, `concat([var.wheel_path], [...])`
  wrappers, ordering, indentation, quote style.
- **`resolve_desired_version(...) is None` ⇒ leave untouched** (`statsbombpy`,
  `var.wheel_path`); a fork raises `PinForkError` and aborts loudly (M2).
- **Modes**: default **apply** (rewrite in place; print a per-pin `pkg: old → new` diff +
  an "N updated / already in sync" summary; **idempotent** — a second apply is a zero-diff
  no-op, M5); `--check` (exit non-zero listing out-of-sync pins, no write — local
  pre-push convenience; CI still gates via the sentinel).

**New bump workflow** (documented in `conventions.md`):
`edit pyproject → uv lock → python scripts/sync_tf_env_pins.py → review diff → commit`.
Same spirit as `scripts/bump_wheel.py` (wheel version across ~30 consumers); disjoint
responsibility (this handles *library dep pins* in TF env blocks).

### Component 2 — sentinel refactor + cross-env consistency guard (the assert adapter)

- `test_tf_exact_pins_match_uv_lock` and friends are refactored to call the Component 0
  core (`resolve_desired_version`) instead of their own inline `lock[pkg]` lookup — so the
  sentinel now also inherits M2's fail-loud fork policy for free.
- **New cross-env consistency guard:** any package with an explicit `==` pin in **≥2 env
  blocks must carry an identical version**, else the test fails naming the divergent
  `(env, version)` pairs. Intentional splits live in a documented allowlist
  `_CROSS_ENV_SPLIT_ALLOWED: dict[pkg, str]` (starts empty).

  **Honest framing (M3):** for **lock-managed** pins this guard is *implied* by
  lock-parity — every non-exempt pin already equals the single lock version, so two envs
  pinning the same lock-managed package are provably equal (today's `huggingface_hub==1.6.0`
  in `embeddings` + `hf` is already locked by lock-parity, not by this guard). The guard's
  **unique, non-redundant** coverage is the **lock-parity-exempt** packages
  (`databricks-sdk`, `statsbombpy`): lock-parity is off for them, so a future second
  control-plane task that also pins `databricks-sdk` at a divergent version would be caught
  *only* here. That narrow-but-real job — and why the allowlist machinery exists when the
  visible baseline is already covered elsewhere — is stated in the test docstring.

### silly-kicks handling

Unchanged. It stays an exact `==` pin. The sync script only *mirrors* an
already-deliberate `pyproject` + `uv lock` bump into `main.tf`; the deliberate decision
(and the table recalculation it triggers) lives in the pyproject/lock edit, which stays
manual and reviewed. Because the script never runs as a CI autofix-commit, it can never
silently advance silly-kicks.

### Docs & ADR

- **Amend ADR-046** with a dated addendum (2026-07-22) recording: (a) `==` reaffirmed and
  `~=` considered-and-rejected (re-opens the patch-drift class, increases intra-build
  drift); (b) the shared core + `sync_tf_env_pins.py` as the sanctioned friction fix;
  (c) the cross-env consistency guard (with the M3 framing); (d) **alternatives rejected
  (M7):** `python-hcl2` parse→modify→emit and generating pins into `*.auto.tfvars.json`
  — both **lossy on the inline comments** that carry ADR-046's per-pin rationale (the
  numba footgun, `xgboost-cpu` GPU-lib omission, the base-image-downgrade fence), so the
  surgical version-substring rewrite is the *correct* choice, not a shortcut.
- **Update the CLAUDE.md ADR-046 bullet** to reference the sync script as the bump
  mechanism (the "bump pyproject + uv lock + terraform together" contract becomes
  "…terraform via `sync_tf_env_pins.py`").
- **Update `docs/engineering/conventions.md`** with the new bump workflow.

## Testing (TDD — red first where noted)

- **Core unit tests** (`scripts/` covered by ruff/pyright; test in `src/tests/test_sync_tf_env_pins.py`):
  - `resolve_desired_version` single-lock-version → that version; `statsbombpy` →
    `None` (`LEAVE_AS_IS`); name normalization `huggingface_hub` ↔ `huggingface-hub` (M6).
  - **Fork precedence (R1 — the precedence is the point):** a **non-exempt** synthetic
    package with a two-version lock set (`{"0.117.0","0.121.0"}`) → `PinForkError`
    (red-first, M2); **and** `databricks-sdk` given that *same* multi-version lock set →
    resolves to the `sdk_extra_pin` **without raising** (proves the exempt check precedes
    fork detection — `databricks-sdk` cannot reach the fork branch).
- **Sync CLI tests**, on a temp copy of a representative `main.tf` fixture:
  - apply rewrites a stale pin to the resolved version; extras/comments/`concat`/ordering
    preserved byte-for-byte except the intended version substring;
  - **a comment containing a `pkg==X.Y.Z`-shaped string survives untouched (red-first, M4)**;
  - **idempotency: apply twice ⇒ second run zero diff (M5)**;
  - `--check` exits non-zero on drift, zero when in sync.
- **Consistency guard test (red-first):** fails on a synthetic two-env divergent *exempt*
  pin; passes on the current tree; an allowlisted split passes.
- **True drift e2e (M8 + R4, not just round-trip):** a **deliberately drifted** fixture
  (stale TF pin + newer lock) → run the sync rewrite → assert `find_pin_drift(fixture_after,
  lock, sdk_pin)` is empty. **Drive the core `find_pin_drift` on the fixture text — not the
  zero-arg `test_tf_exact_pins_match_uv_lock`, which reads hardcoded `_REPO` paths and would
  make the assertion vacuous (R4).** Exercises the *fix* path; the round-trip on the real
  already-synced tree only exercises the no-op path.
- **Full parity suite** stays green after the refactor (the extraction's regression net).

## Scope & limitations (YAGNI)

- The consistency guard covers **explicit** TF pins. **Transitive** cross-env version
  forks are out of scope: `databricks-sdk` resolves to `0.117.0` in the dbt fork
  (constrained by dbt-databricks 1.12.2) vs `0.121.0` in sdk/lakebase — intentional,
  constraint-driven, in different task envs; detecting it needs each env's full transitive
  closure (a resolve-time problem). Documented as a known limitation in the ADR addendum
  and the test docstring.
  **This is distinct from M2:** M2 is a *within-tool* correctness rule (the resolver must
  not silently pick a fork's arbitrary version) — a bug fix, not the out-of-scope
  transitive-drift feature.
- The sync script targets the workflows-module `main.tf` only (the sole location of
  serverless env blocks); it is not a general-purpose terraform formatter.
- **Optional (M9), not blocking:** wiring the **non-mutating** `--check` as a pre-commit
  hook shifts the catch left of CI without violating the no-autofix/stay-deliberate
  stance. Considered a follow-up, explicitly *not* an autofix-commit.

## Rollout

Single PR: the shared core + the extraction refactor + the sync CLI + its tests + the
cross-env guard + the ADR addendum + doc updates, bundled together (specs commit with
their implementation PR). No prod behaviour change on merge — the tool is inert until
invoked, the pins are already in sync, and the new consistency test is green on day one.

**Process note:** because the PR moves the ADR-046 *enforcement sentinels themselves*
(the safety net) into Component 0 alongside new features, the squashed commit description
must state that **the Component 0 extraction is behaviour-preserving — the parity suite is
green before and after** — so a reviewer seeing the sentinel internals move understands the
diff's intent. The pre-existing green tests are the regression net for the extraction.
