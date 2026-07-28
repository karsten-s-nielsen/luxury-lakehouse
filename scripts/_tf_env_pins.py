"""Pure pin-policy core shared by the ADR-046 parity sentinel and the sync CLI.

Single source of truth for parsing TF env dep blocks, uv.lock versions, and the pyproject
[sdk] extra pin; the exemption policy; resolving a pin's desired version; drift-finding;
and the cross-env consistency invariant. Both ``src/tests/test_terraform_env_dep_parity.py``
(the "assert" adapter) and ``scripts/sync_tf_env_pins.py`` (the "rewrite" adapter) import
these, so the fixer and checker cannot silently diverge (ADR-046 addendum 2026-07-22).

Pure: text/data in, values out. No file I/O — adapters read files and pass text in.
"""

from __future__ import annotations

import re
from collections.abc import Iterator
from dataclasses import dataclass
from enum import Enum

_DEP_LINE_RE = re.compile(r"^([A-Za-z0-9._-]+)(?:\[[^\]]*\])?\s*([<>=!~,\d\s.\w-]*)$")
_TF_ENV_RE = re.compile(
    r'environment\s*\{\s*\n\s*environment_key\s*=\s*"([^"]+)"\s*\n\s*spec\s*\{(.*?)\n\s*\}\s*\n\s*\}',
    re.DOTALL,
)
_DEP_BLOCK_RE = re.compile(
    r"dependencies\s*=\s*(?:concat\s*\(\s*\[[^\]]*\]\s*,\s*)?\[(.*?)\n\s*\]",
    re.DOTALL,
)
_LOCK_PKG_RE = re.compile(r'\[\[package\]\]\nname = "([^"]+)"\nversion = "([^"]+)"')
_SDK_EXTRA_RE = re.compile(r'"databricks-sdk==([\w.\-]+)"')


class PinResolutionError(RuntimeError):
    """A TF pin cannot be resolved to a single desired version."""


class PinForkError(PinResolutionError):
    """A lock-managed package resolves to >1 distinct version in uv.lock."""


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
    dependencies list. Single owner of the env/dep-block traversal — consumed by both
    ``parse_tf_env_deps`` (checker) and ``sync_tf_env_pins.rewrite_tf_text`` (fixer)."""
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
        raise PinResolutionError("no 'databricks-sdk==X' pin found in pyproject [sdk] extra")
    return m.group(1)


class ExemptStrategy(Enum):
    SDK_EXTRA = "sdk_extra"  # resolve from the pyproject [sdk] extra pin
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


# ---------------------------------------------------------------------------
# CI dbt pins (ADR-046 lockstep, 2026-07-27)
# ---------------------------------------------------------------------------
#
# Same policy as the TF env pins, different surface, so it reuses this module's
# `parse_lock_versions` rather than `parse_tf_env_deps` (that parser is shaped for TF
# `environment { spec { dependencies } }` blocks; these are a Python dict literal and YAML
# `uvx --from` strings). Shared lock parsing, separate site scanner -- deliberate, so the
# fixer and the checker (src/tests/test_ci_dbt_pin_parity.py) cannot diverge.
#
# pyproject.toml is NOT a site here: it is the INPUT to uv.lock, so rewriting it would
# create pyproject -> lock -> pyproject. The parity test asserts floor <= lock instead.

CI_DBT_UVX_WORKFLOWS = (
    ".github/workflows/data-quality-ci.yml",
    ".github/workflows/lakebase-grants.yml",
    ".github/workflows/python-ci.yml",
    ".github/workflows/synced-table-heal-e2e.yml",
)
CI_DBT_SUBMIT_SCRIPT = "scripts/trigger_dbt_job.py"

_UVX_ANY_RE = re.compile(r'(uvx --from ")dbt-core[^"]*(")')
_SUBMIT_DEP_ANY_RE = re.compile(r'"(dbt-core|dbt-databricks)[^"]*"')


def rewrite_ci_dbt_text(path: str, text: str, lock: dict[str, set[str]]) -> tuple[str, list[Drift]]:
    """Rewrite one CI dbt pin site to uv.lock's versions. Pure: returns (new_text, drifts)."""

    def _one(pkg: str) -> str:
        versions = lock.get(pkg, set())
        if len(versions) != 1:
            raise PinForkError(f"{pkg} has {len(versions)} versions in uv.lock: {sorted(versions)}")
        return next(iter(versions))

    drifts: list[Drift] = []
    if path in CI_DBT_UVX_WORKFLOWS:
        want = f"dbt-core=={_one('dbt-core')}"

        def _sub(m: re.Match[str]) -> str:
            found = m.group(0)[len(m.group(1)) : -len(m.group(2))]
            if found != want:
                drifts.append(Drift(env_key="ci-uvx", pkg="dbt-core", current=found, desired=want))
            return f"{m.group(1)}{want}{m.group(2)}"

        text = _UVX_ANY_RE.sub(_sub, text)
    elif path == CI_DBT_SUBMIT_SCRIPT:

        def _sub_dep(m: re.Match[str]) -> str:
            pkg = m.group(1)
            want = f"{pkg}=={_one(pkg)}"
            found = m.group(0).strip('"')
            if found != want:
                drifts.append(Drift(env_key="ci-submit", pkg=pkg, current=found, desired=want))
            return f'"{want}"'

        text = _SUBMIT_DEP_ANY_RE.sub(_sub_dep, text)
    return text, drifts
