# ADR-046 env-pin automation Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Keep ADR-046's exact `==` env pins, but replace the manual terraform lockstep with a one-command sync tool and add a cross-env consistency guard — with fixer and checker sharing one pure policy core so they cannot silently diverge.

**Architecture:** Hexagonal. A pure `scripts/_tf_env_pins.py` core owns all parsing + the pin-resolution policy (exemptions, fork-detection, drift-finding). Two thin adapters import it: the parity **sentinel** (`test_terraform_env_dep_parity.py`, the "assert" side) and the **sync CLI** (`scripts/sync_tf_env_pins.py`, the "rewrite" side). The CLI rewrites only version substrings inside each env block's `dependencies = [...]` span.

**Tech Stack:** Python 3.10, stdlib + `packaging`, pytest, ruff, pyright. No Databricks/Spark (pure dev tooling under `scripts/`, importable via `pythonpath = ["."]`).

**Spec:** `docs/superpowers/specs/2026-07-22-adr-046-env-pin-automation-design.md`

---

## File Structure

- **Create** `scripts/_tf_env_pins.py` — pure policy core (parsers, resolver, drift-finder, exemption map). Imported by both adapters.
- **Create** `scripts/sync_tf_env_pins.py` — the rewrite adapter (CLI).
- **Create** `src/tests/test_sync_tf_env_pins.py` — core unit tests + CLI tests + drift e2e.
- **Modify** `src/tests/test_terraform_env_dep_parity.py` — import the core; add the cross-env guard; drop the now-relocated private functions.
- **Modify** `docs/superpowers/adrs/ADR-046-serverless-env-exact-pins.md` — dated addendum.
- **Modify** `CLAUDE.md` — ADR-046 bullet points at the sync script.
- **Modify** `docs/engineering/conventions.md` — new bump workflow.

**Do all work on a branch off `origin/main`** (e.g. `feat/adr-046-env-pin-sync`). One squashed commit at the end (Task 8) — do not push/PR without explicit user approval.

---

### Task 1: Extract the TF parsers into the pure core (behavior-preserving)

Move the TF-side parsers out of the sentinel test into the new core, and have the test import them back. No behaviour change — the existing suite is the regression net.

**Files:**
- Create: `scripts/_tf_env_pins.py`
- Modify: `src/tests/test_terraform_env_dep_parity.py` (imports + delete moved functions)

- [ ] **Step 1: Create the core module with the extracted TF parsers**

`scripts/_tf_env_pins.py`:

```python
"""Pure pin-policy core shared by the ADR-046 parity sentinel and the sync CLI.

Single source of truth for parsing TF env dep blocks, uv.lock versions, and the
pyproject [sdk] extra pin; the exemption policy; resolving a pin's desired version;
and finding drift. Both src/tests/test_terraform_env_dep_parity.py (the "assert"
adapter) and scripts/sync_tf_env_pins.py (the "rewrite" adapter) import these, so the
fixer and checker cannot silently diverge (ADR-046 addendum 2026-07-22).

Pure: text/data in, values out. No file I/O — adapters read files and pass text in.
"""

from __future__ import annotations

import re
from collections.abc import Iterator

_DEP_LINE_RE = re.compile(r"^([A-Za-z0-9._-]+)(?:\[[^\]]*\])?\s*([<>=!~,\d\s.\w-]*)$")
_TF_ENV_RE = re.compile(
    r'environment\s*\{\s*\n\s*environment_key\s*=\s*"([^"]+)"\s*\n\s*spec\s*\{(.*?)\n\s*\}\s*\n\s*\}',
    re.DOTALL,
)
_DEP_BLOCK_RE = re.compile(
    r"dependencies\s*=\s*(?:concat\s*\(\s*\[[^\]]*\]\s*,\s*)?\[(.*?)\n\s*\]",
    re.DOTALL,
)


def normalize(name: str) -> str:
    """Canonicalize a package name (lowercase, ``_`` -> ``-``)."""
    return name.lower().replace("_", "-")


def parse_dep_line(line: str) -> tuple[str | None, str | None]:
    """Parse ``'silly-kicks[das]==4.43.0'`` -> ``('silly-kicks', '==4.43.0')``."""
    line = line.strip().strip(",").strip('"')
    m = _DEP_LINE_RE.match(line)
    if not m:
        return None, None
    return normalize(m.group(1)), m.group(2).strip()


def iter_dep_block_spans(text: str) -> Iterator[tuple[str, tuple[int, int] | None]]:
    """Yield ``(env_key, span)`` for every ``environment`` block; ``span`` is the absolute
    ``(start, end)`` of its ``dependencies = [...]`` body, or ``None`` when the block has no
    dependencies list. **Single owner of the env/dep-block traversal** — consumed by both
    ``parse_tf_env_deps`` (checker) and ``sync_tf_env_pins.rewrite_tf_text`` (fixer), so the
    CLI needs no private-regex imports (P3)."""
    for env_m in _TF_ENV_RE.finditer(text):
        dep_m = _DEP_BLOCK_RE.search(env_m.group(2))
        if dep_m:
            base = env_m.start(2)
            yield env_m.group(1), (base + dep_m.start(1), base + dep_m.end(1))
        else:
            yield env_m.group(1), None


def parse_tf_env_deps(text: str) -> dict[str, dict[str, str]]:
    """Return ``{env_key: {pkg: spec}}`` for each ``environment`` block in main.tf."""
    envs: dict[str, dict[str, str]] = {}
    for env_key, span in iter_dep_block_spans(text):
        if span is None:
            envs[env_key] = {}
            continue
        start, end = span
        env_deps: dict[str, str] = {}
        for line in text[start:end].splitlines():
            stripped = line.strip().rstrip(",").strip()
            if not stripped or stripped.startswith("#") or "var." in stripped:
                continue
            pkg, spec = parse_dep_line(stripped)
            if pkg and pkg != "luxury-lakehouse" and spec:
                env_deps[pkg] = spec
        envs[env_key] = env_deps
    return envs
```

- [ ] **Step 2: Point the sentinel test at the core**

In `src/tests/test_terraform_env_dep_parity.py`: add at the top of the imports
`from scripts._tf_env_pins import parse_tf_env_deps, parse_dep_line`. Then **delete** the
local `_parse_dep_line` and `_parse_tf_env_deps` function definitions, and replace every
call `_parse_tf_env_deps()` with `parse_tf_env_deps(_TF.read_text(encoding="utf-8"))` and
`_parse_dep_line(` with `parse_dep_line(`. Keep `_parse_pyproject_deps`, `_parse_lock_versions`,
`_LOCK_PARITY_EXEMPT`, and all test functions unchanged for now (Task 3 relocates the rest).

- [ ] **Step 3: Run the full parity suite — must stay green (regression net)**

Run: `uv run pytest src/tests/test_terraform_env_dep_parity.py -v`
Expected: PASS (same tests, now sourcing the parser from the core).

- [ ] **Step 4: Lint + type-check the new module**

Run: `uv run ruff check scripts/_tf_env_pins.py && uv run ruff format --check scripts/_tf_env_pins.py && uv run pyright scripts/_tf_env_pins.py`
Expected: clean.

---

### Task 2: Add the resolver policy to the core (TDD, red-first)

**Files:**
- Modify: `scripts/_tf_env_pins.py`
- Test: `src/tests/test_sync_tf_env_pins.py` (create)

- [ ] **Step 1: Write the failing resolver tests**

`src/tests/test_sync_tf_env_pins.py`:

```python
"""Tests for the ADR-046 pin-policy core + sync CLI."""

from __future__ import annotations

import pytest

from scripts._tf_env_pins import (
    EXEMPT,
    ExemptStrategy,
    PinForkError,
    PinResolutionError,
    parse_lock_versions,
    parse_sdk_extra_pin,
    resolve_desired_version,
)

_SDK_PIN = "0.121.0"


def test_resolve_single_lock_version() -> None:
    lock = {"numba": {"0.66.0"}}
    assert resolve_desired_version("numba", lock=lock, sdk_extra_pin=_SDK_PIN) == "0.66.0"


def test_resolve_name_normalization() -> None:
    # TF underscore form must resolve against the canonical hyphen lock name (M6).
    lock = {"huggingface-hub": {"1.6.0"}}
    assert resolve_desired_version("huggingface_hub", lock=lock, sdk_extra_pin=_SDK_PIN) == "1.6.0"


def test_resolve_leave_as_is_returns_none() -> None:
    assert EXEMPT["statsbombpy"].strategy is ExemptStrategy.LEAVE_AS_IS
    assert resolve_desired_version("statsbombpy", lock={}, sdk_extra_pin=_SDK_PIN) is None


def test_resolve_missing_and_not_exempt_raises() -> None:
    with pytest.raises(PinResolutionError):
        resolve_desired_version("no-such-pkg", lock={}, sdk_extra_pin=_SDK_PIN)


def test_resolve_nonexempt_fork_raises() -> None:
    # R1: the package under test must be NON-exempt to reach the fork branch.
    lock = {"forked-pkg": {"0.117.0", "0.121.0"}}
    with pytest.raises(PinForkError):
        resolve_desired_version("forked-pkg", lock=lock, sdk_extra_pin=_SDK_PIN)


def test_resolve_exempt_beats_fork() -> None:
    # R1: databricks-sdk with the SAME multi-version lock set resolves via the [sdk]
    # pin WITHOUT raising — proves exempt-check precedes fork detection.
    lock = {"databricks-sdk": {"0.117.0", "0.121.0"}}
    assert EXEMPT["databricks-sdk"].strategy is ExemptStrategy.SDK_EXTRA
    assert resolve_desired_version("databricks-sdk", lock=lock, sdk_extra_pin=_SDK_PIN) == _SDK_PIN


def test_parse_lock_versions_keeps_forks_as_sets() -> None:
    text = (
        '[[package]]\nname = "databricks-sdk"\nversion = "0.117.0"\n'
        'source = { registry = "x" }\n\n'
        '[[package]]\nname = "databricks-sdk"\nversion = "0.121.0"\n'
        'source = { registry = "x" }\n'
    )
    assert parse_lock_versions(text)["databricks-sdk"] == {"0.117.0", "0.121.0"}


def test_parse_sdk_extra_pin() -> None:
    assert parse_sdk_extra_pin('  "databricks-sdk==0.121.0",\n') == "0.121.0"
```

- [ ] **Step 2: Run to verify failure**

Run: `uv run pytest src/tests/test_sync_tf_env_pins.py -v`
Expected: FAIL — `ImportError` (EXEMPT / resolve_desired_version / … not defined).

- [ ] **Step 3: Implement the resolver policy in the core**

Append to `scripts/_tf_env_pins.py` (add `from dataclasses import dataclass` and
`from enum import Enum` to the imports; add `_LOCK_PKG_RE` and `_SDK_EXTRA_RE` near the
other module-level regexes):

```python
_LOCK_PKG_RE = re.compile(r'\[\[package\]\]\nname = "([^"]+)"\nversion = "([^"]+)"')
_SDK_EXTRA_RE = re.compile(r'"databricks-sdk==([\w.\-]+)"')


class PinResolutionError(RuntimeError):
    """A TF pin cannot be resolved to a single desired version."""


class PinForkError(PinResolutionError):
    """A lock-managed package resolves to >1 distinct version in uv.lock."""


def parse_lock_versions(text: str) -> dict[str, set[str]]:
    """Return ``{pkg: {version, ...}}`` — a SET per package so lock forks stay visible."""
    out: dict[str, set[str]] = {}
    for m in _LOCK_PKG_RE.finditer(text):
        out.setdefault(normalize(m.group(1)), set()).add(m.group(2))
    return out


def parse_sdk_extra_pin(pyproject_text: str) -> str:
    """Return the ``databricks-sdk==X`` version pinned in the pyproject [sdk] extra."""
    m = _SDK_EXTRA_RE.search(pyproject_text)
    if m is None:
        # P7: typed error so every "can't resolve a pin" path shares one catchable base.
        raise PinResolutionError("no 'databricks-sdk==X' pin found in pyproject [sdk] extra")
    return m.group(1)


class ExemptStrategy(Enum):
    SDK_EXTRA = "sdk_extra"      # resolve from the pyproject [sdk] extra pin
    LEAVE_AS_IS = "leave_as_is"  # not lock-managed; sync leaves it untouched


@dataclass(frozen=True)
class ExemptRule:
    strategy: ExemptStrategy
    reason: str


# Packages whose source of truth is NOT uv.lock's default resolution. Adding a third
# exempt package is a one-line data entry here (strategy + reason).
EXEMPT: dict[str, ExemptRule] = {
    "databricks-sdk": ExemptRule(
        ExemptStrategy.SDK_EXTRA,
        "pyproject [sdk] extra is the source of truth (lock default resolution is an extras-conflict artifact)",
    ),
    "statsbombpy": ExemptRule(
        ExemptStrategy.LEAVE_AS_IS,
        "terraform-only ingestion API client; exact pin required but no uv.lock entry",
    ),
}


def resolve_desired_version(pkg: str, *, lock: dict[str, set[str]], sdk_extra_pin: str) -> str | None:
    """Desired version for a TF pin, or ``None`` when the pin must be left as-is.

    Order is load-bearing: exempt FIRST (so an exempt package that is also a lock fork
    never reaches fork detection), then single lock version, then fork -> raise, then
    missing -> raise.
    """
    pkg = normalize(pkg)
    rule = EXEMPT.get(pkg)
    if rule is not None:
        return sdk_extra_pin if rule.strategy is ExemptStrategy.SDK_EXTRA else None
    versions = lock.get(pkg, set())
    if not versions:
        raise PinResolutionError(
            f"{pkg} is not in uv.lock and not EXEMPT — add it to pyproject + `uv lock`, "
            "or add an EXEMPT entry with a reason"
        )
    if len(versions) > 1:
        raise PinForkError(
            f"{pkg} resolves to multiple versions in uv.lock ({sorted(versions)}) — "
            "add an EXEMPT entry with an explicit source (see databricks-sdk)"
        )
    return next(iter(versions))
```

- [ ] **Step 4: Run to verify pass**

Run: `uv run pytest src/tests/test_sync_tf_env_pins.py -v`
Expected: PASS (8 tests).

- [ ] **Step 5: Lint + type-check**

Run: `uv run ruff check scripts/ src/tests/test_sync_tf_env_pins.py && uv run pyright scripts/_tf_env_pins.py`
Expected: clean.

---

### Task 3: Add `find_pin_drift` and refactor the lock-parity sentinels onto the core (TDD)

**Files:**
- Modify: `scripts/_tf_env_pins.py`
- Modify: `src/tests/test_terraform_env_dep_parity.py`
- Test: `src/tests/test_sync_tf_env_pins.py`

- [ ] **Step 1: Write the failing drift-finder tests**

Append to `src/tests/test_sync_tf_env_pins.py`:

```python
from scripts._tf_env_pins import Drift, find_pin_drift

_TF_FIXTURE = '''
  environment {
    environment_key = "analytics"
    spec {
      client = "1"
      dependencies = [
        var.wheel_path,
        "numba==0.66.0",
        "scipy==1.15.3"
      ]
    }
  }
'''


def test_find_pin_drift_flags_stale_pin() -> None:
    lock = {"numba": {"0.66.0"}, "scipy": {"1.99.0"}}  # scipy drifted
    drifts = find_pin_drift(_TF_FIXTURE, lock, _SDK_PIN)
    assert drifts == [Drift(env_key="analytics", pkg="scipy", current="1.15.3", desired="1.99.0")]


def test_find_pin_drift_empty_when_in_sync() -> None:
    lock = {"numba": {"0.66.0"}, "scipy": {"1.15.3"}}
    assert find_pin_drift(_TF_FIXTURE, lock, _SDK_PIN) == []
```

- [ ] **Step 2: Run to verify failure**

Run: `uv run pytest src/tests/test_sync_tf_env_pins.py::test_find_pin_drift_flags_stale_pin -v`
Expected: FAIL — `ImportError` (Drift / find_pin_drift not defined).

- [ ] **Step 3: Implement `find_pin_drift` in the core**

Append to `scripts/_tf_env_pins.py`:

```python
@dataclass(frozen=True)
class Drift:
    env_key: str
    pkg: str
    current: str
    desired: str


def find_pin_drift(tf_text: str, lock: dict[str, set[str]], sdk_extra_pin: str) -> list[Drift]:
    """Pins whose TF version != resolved desired version. Raises PinForkError /
    PinResolutionError (fail-loud) on a fork or a non-exempt pin missing from the lock."""
    drifts: list[Drift] = []
    for env_key, deps in parse_tf_env_deps(tf_text).items():
        for pkg, spec in deps.items():
            desired = resolve_desired_version(pkg, lock=lock, sdk_extra_pin=sdk_extra_pin)
            if desired is None:
                continue
            current = spec.replace(" ", "").removeprefix("==")
            if current != desired:
                drifts.append(Drift(env_key, pkg, current, desired))
    return drifts
```

- [ ] **Step 4: Run to verify pass**

Run: `uv run pytest src/tests/test_sync_tf_env_pins.py -v`
Expected: PASS (10 tests).

- [ ] **Step 5: Refactor the lock-parity sentinels onto the core**

In `src/tests/test_terraform_env_dep_parity.py`:

Replace the import line from Task 1 with:
```python
from scripts._tf_env_pins import find_pin_drift, parse_dep_line, parse_lock_versions, parse_sdk_extra_pin, parse_tf_env_deps
```

**Delete** the local `_parse_lock_versions`, `_LOCK_PARITY_EXEMPT`, and the `_LOCK` line's
duplicate parser usage. Keep `_LOCK`, `_TF`, `_PYPROJECT` path constants.

Replace `test_tf_exact_pins_match_uv_lock` with:
```python
def test_tf_exact_pins_match_uv_lock() -> None:
    """Each non-exempt TF pin must equal uv.lock's single resolved version (ADR-046).
    Fail-loud on forks / missing pins via the shared core."""
    drifts = find_pin_drift(
        _TF.read_text(encoding="utf-8"),
        parse_lock_versions(_LOCK.read_text(encoding="utf-8")),
        parse_sdk_extra_pin(_PYPROJECT.read_text(encoding="utf-8")),
    )
    assert not drifts, "TF env pins drifted from uv.lock; run `python scripts/sync_tf_env_pins.py`:\n" + "\n".join(
        f"  [{d.env_key}] {d.pkg}: TF pins {d.current} but resolved {d.desired}" for d in drifts
    )
```

Replace the `re.search` in `test_lakebase_sdk_pin_matches_pyproject_extra` with
`parse_sdk_extra_pin(py_text)`:
```python
def test_lakebase_sdk_pin_matches_pyproject_extra() -> None:
    tf_envs = parse_tf_env_deps(_TF.read_text(encoding="utf-8"))
    tf_pin = tf_envs.get("lakebase", {}).get("databricks-sdk", "").replace(" ", "").removeprefix("==")
    assert tf_pin, "lakebase env must pin databricks-sdk"
    extra_pin = parse_sdk_extra_pin(_PYPROJECT.read_text(encoding="utf-8"))
    assert tf_pin == extra_pin, (
        f"lakebase env pins databricks-sdk=={tf_pin} but pyproject's extra pins =={extra_pin} — keep them in lockstep"
    )
```

`test_dependabot_only_dev_deps_grouped` already calls `_parse_tf_env_deps()`; change that
call to `parse_tf_env_deps(_TF.read_text(encoding="utf-8"))`. Leave
`test_tf_env_deps_are_exact_pins`, `test_terraform_env_specs_align_with_pyproject`,
`test_refresh_synced_tables_env_ships_databricks_sdk`, and `test_parser_finds_known_analytics_deps`
otherwise intact (switch any `_parse_tf_env_deps()` / `_parse_dep_line` calls to the imported names).

- [ ] **Step 6: Run the full parity suite — must stay green**

Run: `uv run pytest src/tests/test_terraform_env_dep_parity.py -v`
Expected: PASS. Behaviour-preserving **on outcome** (real tree already in sync → `find_pin_drift` returns `[]`), with two deliberate changes to note:
- **P5 (coverage-expanding for `databricks-sdk`):** `test_tf_exact_pins_match_uv_lock` now *checks* `databricks-sdk` against the `[sdk]` extra pin (via `resolve_desired_version`'s `SDK_EXTRA` strategy) instead of *skipping* it. Green today (lakebase `0.121.0` == `[sdk]` `0.121.0`); it now also enforces what `test_lakebase_sdk_pin_matches_pyproject_extra` did (harmless overlap).
- **P6 (missing-pin failure mode):** a non-exempt pin absent from `uv.lock` now raises `PinResolutionError` (the test *errors*, fail-loud) rather than a clean assertion failure. Neither fires on the current tree (all 16 non-exempt pins present with a single lock version).

---

### Task 4: Cross-env consistency guard — policy in the core (TDD, red-first)

Per P4, "same package → same version across envs" is a pin-policy invariant, so it lives in
Component 0, not the test. Unit-tested with a plain `dict` — **no monkeypatch** (this
dissolves P1's namespace fragility).

**Files:**
- Modify: `scripts/_tf_env_pins.py`
- Modify: `src/tests/test_terraform_env_dep_parity.py`
- Test: `src/tests/test_sync_tf_env_pins.py`

- [ ] **Step 1: Write the failing core test**

Append to `src/tests/test_sync_tf_env_pins.py`:

```python
from scripts._tf_env_pins import find_cross_env_divergences


def test_find_cross_env_divergences_flags_split() -> None:
    envs = {"env_a": {"databricks-sdk": "==0.117.0"}, "env_b": {"databricks-sdk": "==0.121.0"}}
    assert find_cross_env_divergences(envs) == {"databricks-sdk": {"env_a": "0.117.0", "env_b": "0.121.0"}}


def test_find_cross_env_divergences_ignores_agreement() -> None:
    envs = {"env_a": {"huggingface-hub": "==1.6.0"}, "env_b": {"huggingface-hub": "==1.6.0"}}
    assert find_cross_env_divergences(envs) == {}
```

- [ ] **Step 2: Run to verify failure**

Run: `uv run pytest src/tests/test_sync_tf_env_pins.py -k cross_env -v`
Expected: FAIL — `ImportError` (find_cross_env_divergences not defined).

- [ ] **Step 3: Implement in the core**

Append to `scripts/_tf_env_pins.py`:

```python
# Deliberate cross-env version splits (pkg -> reason). Lock-parity is OFF for EXEMPT
# packages, so this guard is their ONLY cross-env consistency check. Starts empty.
CROSS_ENV_SPLIT_ALLOWED: dict[str, str] = {}


def find_cross_env_divergences(envs: dict[str, dict[str, str]]) -> dict[str, dict[str, str]]:
    """Return ``{pkg: {env: version}}`` for packages pinned in >=2 env blocks with >1
    distinct version. Pure over the parsed env map (no file I/O)."""
    seen: dict[str, dict[str, str]] = {}
    for env_key, deps in envs.items():
        for pkg, spec in deps.items():
            seen.setdefault(pkg, {})[env_key] = spec.replace(" ", "").removeprefix("==")
    return {pkg: e for pkg, e in seen.items() if len(set(e.values())) > 1}
```

- [ ] **Step 4: Run to verify pass**

Run: `uv run pytest src/tests/test_sync_tf_env_pins.py -k cross_env -v`
Expected: PASS.

- [ ] **Step 5: Wire the sentinel adapter (two lines, no monkeypatch)**

Extend the core import in `src/tests/test_terraform_env_dep_parity.py` with
`CROSS_ENV_SPLIT_ALLOWED, find_cross_env_divergences`, and add:

```python
def test_cross_env_pin_consistency() -> None:
    """A package pinned in >=2 env blocks must carry the SAME version. For lock-managed pins
    this is IMPLIED by lock-parity (each == the single lock value — today's huggingface_hub is
    already covered there); the unique coverage here is the lock-parity-EXEMPT packages
    (databricks-sdk, statsbombpy), whose cross-env divergence nothing else would catch.
    Intentional splits: CROSS_ENV_SPLIT_ALLOWED (core)."""
    envs = parse_tf_env_deps(_TF.read_text(encoding="utf-8"))
    offenders = {p: e for p, e in find_cross_env_divergences(envs).items() if p not in CROSS_ENV_SPLIT_ALLOWED}
    assert not offenders, "cross-env pin divergence (same package, different versions):\n" + "\n".join(
        f"  {pkg}: {divs}" for pkg, divs in offenders.items()
    )
```

- [ ] **Step 6: Run the parity suite — real tree green**

Run: `uv run pytest src/tests/test_terraform_env_dep_parity.py -v`
Expected: PASS (today's only ≥2-env pin, `huggingface_hub==1.6.0`, agrees in both envs).

---

### Task 5: The sync CLI rewrite adapter (TDD)

**Files:**
- Create: `scripts/sync_tf_env_pins.py`
- Test: `src/tests/test_sync_tf_env_pins.py`

- [ ] **Step 1: Write the failing CLI/rewrite tests**

Append to `src/tests/test_sync_tf_env_pins.py`:

```python
from scripts.sync_tf_env_pins import rewrite_tf_text

_TF_WITH_COMMENT = '''
  environment {
    environment_key = "analytics"
    spec {
      client = "1"
      dependencies = [
        var.wheel_path,
        # historical drift: "scipy==9.9.9" must NOT be rewritten (it is a comment)
        "silly-kicks[das,ghost-gk]==4.43.0",
        "scipy==1.15.3"
      ]
    }
  }
'''


def test_rewrite_updates_stale_and_preserves_extras() -> None:
    lock = {"silly-kicks": {"4.44.0"}, "scipy": {"1.15.3"}}
    new_text, changes = rewrite_tf_text(_TF_WITH_COMMENT, lock=lock, sdk_extra_pin=_SDK_PIN)
    assert '"silly-kicks[das,ghost-gk]==4.44.0"' in new_text  # bumped, extras preserved
    assert '"scipy==1.15.3"' in new_text                       # untouched (in sync)
    assert [(c.pkg, c.current, c.desired) for c in changes] == [("silly-kicks", "4.43.0", "4.44.0")]


def test_rewrite_never_touches_comment_versions() -> None:  # M4 (full-line comment)
    lock = {"silly-kicks": {"4.43.0"}, "scipy": {"1.15.3"}}
    new_text, changes = rewrite_tf_text(_TF_WITH_COMMENT, lock=lock, sdk_extra_pin=_SDK_PIN)
    assert '"scipy==9.9.9"' in new_text   # the comment's version string survives verbatim
    assert changes == []


_TF_TRAILING_COMMENT = '''
  environment {
    environment_key = "analytics"
    spec {
      client = "1"
      dependencies = [
        var.wheel_path,
        "scipy==1.15.3"  # historical: was "scipy==9.9.9"
      ]
    }
  }
'''


def test_rewrite_never_touches_trailing_comment_versions() -> None:  # M4 / P2 (trailing inline comment)
    lock = {"scipy": {"1.20.0"}}  # the real (code) pin should bump
    new_text, changes = rewrite_tf_text(_TF_TRAILING_COMMENT, lock=lock, sdk_extra_pin=_SDK_PIN)
    assert '"scipy==1.20.0"' in new_text  # code pin bumped
    assert '"scipy==9.9.9"' in new_text   # trailing-comment pin survives verbatim
    assert [(c.pkg, c.current, c.desired) for c in changes] == [("scipy", "1.15.3", "1.20.0")]


def test_rewrite_is_idempotent() -> None:  # M5
    lock = {"silly-kicks": {"4.44.0"}, "scipy": {"1.15.3"}}
    once, _ = rewrite_tf_text(_TF_WITH_COMMENT, lock=lock, sdk_extra_pin=_SDK_PIN)
    twice, changes2 = rewrite_tf_text(once, lock=lock, sdk_extra_pin=_SDK_PIN)
    assert twice == once and changes2 == []


def test_rewrite_leaves_statsbombpy_untouched() -> None:
    tf = '''
  environment {
    environment_key = "statsbomb"
    spec {
      client = "1"
      dependencies = [
        var.wheel_path,
        "statsbombpy==1.19.0"
      ]
    }
  }
'''
    new_text, changes = rewrite_tf_text(tf, lock={}, sdk_extra_pin=_SDK_PIN)
    assert new_text == tf and changes == []
```

- [ ] **Step 2: Run to verify failure**

Run: `uv run pytest src/tests/test_sync_tf_env_pins.py -k rewrite -v`
Expected: FAIL — `ModuleNotFoundError: scripts.sync_tf_env_pins`.

- [ ] **Step 3: Implement the CLI**

`scripts/sync_tf_env_pins.py`:

```python
"""Sync main.tf serverless-env `==` pins to uv.lock (ADR-046). Human-invoked; never a
CI autofix — the parity sentinel remains the gate.

    python scripts/sync_tf_env_pins.py           # apply (rewrite main.tf in place)
    python scripts/sync_tf_env_pins.py --check    # exit 1 if any pin is out of sync (no write)

Surgical: rewrites ONLY the version substring inside each env block's
`dependencies = [...]` span, confined to non-comment lines, preserving extras, comments,
concat() wrappers, ordering, and formatting.
"""

from __future__ import annotations

import argparse
import re
import sys
from pathlib import Path

# Enable `python scripts/sync_tf_env_pins.py`: when run directly the repo root is not on
# sys.path (only scripts/ is), so `import scripts` fails. Under pytest, pythonpath=["."]
# already provides the repo root, making this insert a harmless duplicate.
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from scripts._tf_env_pins import (
    Drift,
    iter_dep_block_spans,
    normalize,
    parse_lock_versions,
    parse_sdk_extra_pin,
    resolve_desired_version,
)

_REPO = Path(__file__).resolve().parents[1]
_TF = _REPO / "terraform" / "modules" / "workflows" / "main.tf"
_LOCK = _REPO / "uv.lock"
_PYPROJECT = _REPO / "pyproject.toml"

_PIN_RE = re.compile(r'"([A-Za-z0-9._-]+)(\[[^\]]*\])?==([\w.\-]+)"')


def _strip_trailing_comment(line: str) -> tuple[str, str]:
    """Split ``line`` into ``(code, comment)`` at the first ``#`` or ``//`` — protecting BOTH
    full-line AND trailing inline comments (M4 / P2). The TF env pins here are simple
    ``pkg==ver`` strings; a URL-style dep (``@ https://…``) would split at its ``//`` yet
    reassemble losslessly (``line[:cut] + line[cut:] == line``), merely skipping its rewrite."""
    marks = [i for i in (line.find("#"), line.find("//")) if i != -1]
    if not marks:
        return line, ""
    cut = min(marks)
    return line[:cut], line[cut:]


def _rewrite_block(block: str, lock: dict[str, set[str]], sdk_pin: str, changes: list[Drift]) -> str:
    def _sub(m: re.Match[str]) -> str:  # defined once per block (P8), not per line
        raw, extras, old = m.group(1), m.group(2) or "", m.group(3)
        desired = resolve_desired_version(normalize(raw), lock=lock, sdk_extra_pin=sdk_pin)
        if desired is None or desired == old:
            return m.group(0)
        changes.append(Drift("", normalize(raw), old, desired))
        return f'"{raw}{extras}=={desired}"'

    out: list[str] = []
    for line in block.splitlines(keepends=True):
        code, comment = _strip_trailing_comment(line)
        out.append(_PIN_RE.sub(_sub, code) + comment)  # only the code portion is eligible
    return "".join(out)


def rewrite_tf_text(tf_text: str, *, lock: dict[str, set[str]], sdk_extra_pin: str) -> tuple[str, list[Drift]]:
    """Return (new_text, changes) — version substrings synced, confined to dep-list spans."""
    spans = [span for _env, span in iter_dep_block_spans(tf_text) if span is not None]
    changes: list[Drift] = []
    out = tf_text
    for start, end in sorted(spans, reverse=True):  # right-to-left keeps earlier offsets valid
        out = out[:start] + _rewrite_block(out[start:end], lock, sdk_extra_pin, changes) + out[end:]
    return out, changes


def main() -> int:
    parser = argparse.ArgumentParser(description="Sync main.tf env pins to uv.lock (ADR-046)")
    parser.add_argument("--check", action="store_true", help="exit 1 if out of sync; do not write")
    args = parser.parse_args()

    tf_text = _TF.read_text(encoding="utf-8")
    lock = parse_lock_versions(_LOCK.read_text(encoding="utf-8"))
    sdk_pin = parse_sdk_extra_pin(_PYPROJECT.read_text(encoding="utf-8"))

    new_text, changes = rewrite_tf_text(tf_text, lock=lock, sdk_extra_pin=sdk_pin)

    if not changes:
        print("All TF env pins already in sync with uv.lock.")
        return 0
    for c in changes:
        print(f"  {c.pkg}: {c.current} -> {c.desired}")
    if args.check:
        print(f"{len(changes)} pin(s) out of sync (--check).", file=sys.stderr)
        return 1
    _TF.write_text(new_text, encoding="utf-8")
    print(f"{len(changes)} pin(s) updated in {_TF}.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
```

Note (P3): the CLI imports **zero** private names — the env/dep-block traversal lives once
in the core's public `iter_dep_block_spans`, consumed by both `parse_tf_env_deps` (checker)
and `rewrite_tf_text` (fixer).

- [ ] **Step 4: Run to verify pass**

Run: `uv run pytest src/tests/test_sync_tf_env_pins.py -v`
Expected: PASS (all core + rewrite tests).

- [ ] **Step 5: Lint + type-check**

Run: `uv run ruff check scripts/ src/tests/test_sync_tf_env_pins.py && uv run ruff format --check scripts/ && uv run pyright scripts/sync_tf_env_pins.py scripts/_tf_env_pins.py`
Expected: clean. (If `ruff format --check` reports reformat, run `uv run ruff format scripts/ src/tests/test_sync_tf_env_pins.py` and re-check.)

---

### Task 6: True drift e2e — exercise the fix path (M8)

**Files:**
- Test: `src/tests/test_sync_tf_env_pins.py`

- [ ] **Step 1: Write the drift e2e (drives the core `find_pin_drift` on the fixture, per R4)**

Append to `src/tests/test_sync_tf_env_pins.py`:

```python
def test_drift_fixture_synced_then_parity_clean() -> None:  # M8 + R4
    """A deliberately-drifted fixture -> sync rewrite -> find_pin_drift is empty.
    Drives the core drift-finder on the FIXTURE text (not the zero-arg sentinel that
    reads hardcoded repo paths, which would be vacuous)."""
    drifted = _TF_WITH_COMMENT  # pins silly-kicks==4.43.0, scipy==1.15.3
    lock = {"silly-kicks": {"4.50.0"}, "scipy": {"1.15.3"}}  # silly-kicks bumped in "lock"

    before = find_pin_drift(drifted, lock, _SDK_PIN)
    assert before == [Drift(env_key="analytics", pkg="silly-kicks", current="4.43.0", desired="4.50.0")]

    synced, _ = rewrite_tf_text(drifted, lock=lock, sdk_extra_pin=_SDK_PIN)
    after = find_pin_drift(synced, lock, _SDK_PIN)
    assert after == []  # the fix path actually closed the drift
```

- [ ] **Step 2: Run to verify pass**

Run: `uv run pytest src/tests/test_sync_tf_env_pins.py::test_drift_fixture_synced_then_parity_clean -v`
Expected: PASS.

---

### Task 7: Docs — ADR addendum, CLAUDE.md, conventions

**Files:**
- Modify: `docs/superpowers/adrs/ADR-046-serverless-env-exact-pins.md`
- Modify: `CLAUDE.md`
- Modify: `docs/engineering/conventions.md`

- [ ] **Step 1: Append the ADR-046 addendum**

Add to the end of `docs/superpowers/adrs/ADR-046-serverless-env-exact-pins.md`:

```markdown

## Addendum — 2026-07-22: automate the lockstep, guard cross-env consistency

`==` exactness is **reaffirmed**; relaxing to `~=` was considered and **rejected** —
because envs re-resolve at build time, `~=X.Y.Z` (`>=X.Y.Z,<X.(Y+1).0`) permits the
patch drift (4.21.0 → 4.21.2) this ADR exists to stop, and *increases* intra-build drift.

Two additions keep `==` while removing the manual toil and strengthening consistency:

1. **Shared pure core `scripts/_tf_env_pins.py`** — one module owns the TF/lock/[sdk]
   parsers, the exemption policy, `resolve_desired_version` (exempt-first, then single
   lock version, then fork → `PinForkError`, then missing → `PinResolutionError`), and
   `find_pin_drift`. Both the parity sentinel (`test_terraform_env_dep_parity.py`) and the
   sync CLI import it, so fixer ≡ checker by construction. Lives in `scripts/` (dev
   tooling; not `src/shared` which is stdlib-only, not `src/ingestion` which ships in the
   wheel); importable from tests via `pythonpath = ["."]`.
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
```

- [ ] **Step 2: Update the CLAUDE.md ADR-046 bullet**

In `CLAUDE.md`, find the ADR-046 bullet ("Serverless env deps are EXACT pins synced to
uv.lock"). Append to it: ` Bumping a library = pyproject + uv lock + `scripts/sync_tf_env_pins.py` (the sync tool mirrors uv.lock into the terraform env pins; never hand-edit them). Cross-env pin consistency + lock forks guarded by the shared core `scripts/_tf_env_pins.py`.`

- [ ] **Step 3: Update conventions.md**

In `docs/engineering/conventions.md`, under the dependency/serverless-env area, add:

```markdown
- **Bumping a serverless-env-pinned library (ADR-046):** edit the pin in `pyproject.toml`,
  run `uv lock`, then `python scripts/sync_tf_env_pins.py` to mirror the resolved version
  into `terraform/modules/workflows/main.tf` (never hand-edit the TF pins). Use
  `--check` for a non-mutating drift check. The tool is human-invoked; CI enforces via
  `test_terraform_env_dep_parity.py`. silly-kicks bumps stay deliberate (they trigger
  table recalculation) — the tool only mirrors an already-decided lock change.
```

- [ ] **Step 4: Verify the docs render / no broken refs**

Run: `uv run pytest src/tests/test_architecture_md_appendix.py src/tests/test_ai_governance_md.py -q` (if either references ADR/doc structure; otherwise skip)
Expected: PASS or not-applicable.

---

### Task 8: Full integration + bundle commit

**Files:** none new — verification + commit.

- [ ] **Step 1: Confirm the real tree is already in sync (no-op apply)**

Run: `uv run python scripts/sync_tf_env_pins.py --check`
Expected: exit 0, "All TF env pins already in sync with uv.lock." (Proves the tool agrees with the committed pins; if it reports drift, that is a pre-existing real drift — investigate before committing.)

- [ ] **Step 2: Full quality gate**

Run: `uv run ruff check src/ scripts/ && uv run ruff format --check src/ scripts/ && uv run pyright src/ scripts/_tf_env_pins.py scripts/sync_tf_env_pins.py && uv run pytest src/tests/test_terraform_env_dep_parity.py src/tests/test_sync_tf_env_pins.py -v`
Expected: all clean / PASS.

- [ ] **Step 3: Full suite (catch any incidental breakage from the test refactor)**

Run: `uv run pytest src/tests/ -q`
Expected: PASS (no regressions from relocating the sentinel internals).

- [ ] **Step 4: Gate the keystone core in CI pyright (conscious choice — round-4 nit 1)**

CI's "Type check with pyright" step runs `pyright src/ hf_taipy_app/src/`
(`.github/workflows/python-ci.yml`). `scripts/` is on pyright's `extraPaths` (imports
resolve) but is NOT in CI's *check scope*, so the shared core would otherwise be
type-gated only by Step 2's local run — leaving the design's keystone untyped in CI
long-term. Append the two new files to that CI pyright invocation:

Edit the pyright `run:` line in `.github/workflows/python-ci.yml` to end with
` scripts/_tf_env_pins.py scripts/sync_tf_env_pins.py`.

This gates **only these two new files**, not all of `scripts/`, so it cannot surface
unrelated pre-existing type debt. (If you prefer a feature-only PR, skip this step — ruff
+ the `src/` tests + Step 2 still cover the core; this is the deliberate long-term-typing
choice, called out so it is not an accident either way.)

Run (mirror what CI will run): `uv run pyright src/ hf_taipy_app/src/ scripts/_tf_env_pins.py scripts/sync_tf_env_pins.py`
Expected: 0 errors.

- [ ] **Step 5: Bundle commit** (only after user approval to commit)

Stage the spec, plan, core, CLI, tests, ADR, CLAUDE.md, conventions. Commit message body
MUST state the behaviour-preserving refactor explicitly:

```
feat(ci): sync tool + shared core for ADR-046 env pins; cross-env consistency guard

Keep == exactness; replace the manual terraform lockstep with
scripts/sync_tf_env_pins.py and add a cross-env consistency guard. A shared pure
core scripts/_tf_env_pins.py owns parsing + pin policy; the parity sentinel and the
sync CLI both import it, so fixer == checker by construction.

The Component 0 extraction is behaviour-preserving ON OUTCOME: the ADR-046 parity suite
is green before and after the sentinel internals moved into the core (the pre-existing
tests are the regression net). Two deliberate deltas: the exact-pin sentinel now also
checks databricks-sdk against the [sdk] extra pin (coverage-expanding, green today), and
a non-exempt pin missing from uv.lock now fails loud via PinResolutionError (was a plain
assertion). parse_lock_versions returns a set per package and resolve_desired_version
raises PinForkError on a non-exempt uv.lock fork.

Spec: docs/superpowers/specs/2026-07-22-adr-046-env-pin-automation-design.md
Plan: docs/superpowers/plans/2026-07-22-adr-046-env-pin-automation.md
```

Do **not** push / open a PR without separate explicit approval.

---

## Self-Review

**Spec coverage:** Component 0 core → Tasks 1–3; sync CLI → Task 5; cross-env guard →
Task 4; `parse_sdk_extra_pin` (R2) → Task 2/3; `ExemptRule` strategy (R3) → Task 2; fork
fail-loud + R1 precedence → Task 2; M4 comment-safety → Task 5; M5 idempotency → Task 5;
M6 normalization → Task 2; M8+R4 drift e2e → Task 6; ADR M7 alternatives + docs → Task 7;
behaviour-preserving process note → Task 8. All spec sections mapped.

**Placeholder scan:** no TBD/TODO; every code step shows complete code; every run step
shows the command + expected result.

**Type consistency:** `resolve_desired_version(pkg, *, lock: dict[str,set[str]], sdk_extra_pin: str) -> str | None`,
`find_pin_drift(tf_text, lock, sdk_extra_pin) -> list[Drift]`, `Drift(env_key, pkg, current, desired)`,
`rewrite_tf_text(tf_text, *, lock, sdk_extra_pin) -> tuple[str, list[Drift]]`,
`EXEMPT: dict[str, ExemptRule]`, `ExemptStrategy.{SDK_EXTRA,LEAVE_AS_IS}` — names and
signatures are consistent across Tasks 2–6 and the docs.

**Round-3 review (P1–P8) incorporated:** P1 (self-test monkeypatch no-op) is **dissolved** —
Task 4 now moves the divergence detector into the core (`find_cross_env_divergences`) and
unit-tests it with a plain `dict`, so there is no monkeypatch and no namespace fragility.
P2 (comment safety) now protects trailing *and* full-line comments via `_strip_trailing_comment`,
with a dedicated trailing-comment test. P3 (`iter_dep_block_spans` — the CLI imports zero
private names) and P4 (policy in the core) fold into the same refactor. P5/P6 (coverage-
expansion + fail-loud mode) are documented in Task 3 Step 6 and the commit message. P7 (typed
`PinResolutionError` in `parse_sdk_extra_pin`) and P8 (`_sub` hoisted once per block) applied.
No open verification points remain.
