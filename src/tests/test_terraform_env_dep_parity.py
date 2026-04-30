"""TF env-spec deps must align with pyproject.toml deps.

Catches the silent dependency drift class: ``pyproject.toml``'s pin advances
(e.g., silly-kicks 2.0.0 → 2.5.0 for the NaN-safety contract) but the
Terraform Databricks env spec (``terraform/modules/workflows/main.tf``)
keeps an older pin. The wheel deploys to UC Volume but the TF env installs
the older package, and production runs with the wrong version.

Production failure that motivated this test: silly-kicks 2.0.0 → 2.5.0
bump in ``pyproject.toml`` left the TF analytics env at ``>=1.0.0,<2.0`` →
``compute_spadl_vaep`` ran silly-kicks 1.x in production, hit a NaN bug
2.5.0 was supposed to fix. The drift went undetected for 24+ hours
(2026-04-29 PR-LL2 silly-kicks 2.0.0 bump → 2026-04-30 daily-job verify).

This test fails CI if any package listed in BOTH pyproject and a TF env
spec has version constraints whose intersection is empty.
"""

from __future__ import annotations

import re
from pathlib import Path

from packaging.specifiers import SpecifierSet
from packaging.version import Version

_REPO = Path(__file__).resolve().parents[2]
_PYPROJECT = _REPO / "pyproject.toml"
_TF = _REPO / "terraform" / "modules" / "workflows" / "main.tf"


def _parse_dep_line(line: str) -> tuple[str | None, str | None]:
    """Parse ``'silly-kicks>=2.5.0,<3.0'`` → ``('silly-kicks', '>=2.5.0,<3.0')``."""
    line = line.strip().strip(",").strip('"')
    m = re.match(r"^([A-Za-z0-9._-]+)\s*([<>=!~,\d\s.\w-]*)$", line)
    if not m:
        return None, None
    name = m.group(1).lower().replace("_", "-")
    spec = m.group(2).strip()
    return name, spec


def _parse_pyproject_deps() -> dict[str, str]:
    """Read ``pyproject.toml`` [project] and [project.optional-dependencies].*
    sections, returning ``{pkg: spec_string}`` for all explicitly-listed packages.

    First occurrence wins (matches PEP 621 semantics — extras don't
    override base dependencies).
    """
    text = _PYPROJECT.read_text(encoding="utf-8")
    deps: dict[str, str] = {}
    for block_match in re.finditer(
        r"^\s*(?:dependencies|[\w-]+)\s*=\s*\[\s*\n([^\]]+)\]",
        text,
        re.MULTILINE,
    ):
        block = block_match.group(1)
        for line in block.splitlines():
            pkg, spec = _parse_dep_line(line)
            if pkg and spec:
                deps.setdefault(pkg, spec)
    return deps


def _parse_tf_env_deps() -> dict[str, dict[str, str]]:
    """Read each ``environment`` block in ``terraform/modules/workflows/main.tf``,
    returning ``{env_key: {pkg: spec}}``.

    Tolerates both inline dep lists and ``concat([...], [...])`` wrappers.
    Skips ``var.wheel_path`` (path reference, not a versioned package) and
    blank/comment lines.
    """
    text = _TF.read_text(encoding="utf-8")
    envs: dict[str, dict[str, str]] = {}
    for env_match in re.finditer(
        r'environment\s*\{\s*\n\s*environment_key\s*=\s*"([^"]+)"\s*\n\s*spec\s*\{(.*?)\n\s*\}\s*\n\s*\}',
        text,
        re.DOTALL,
    ):
        env_key = env_match.group(1)
        body = env_match.group(2)
        dep_match = re.search(
            r"dependencies\s*=\s*(?:concat\s*\(\s*\[[^\]]*\]\s*,\s*)?\[(.*?)\]",
            body,
            re.DOTALL,
        )
        if not dep_match:
            envs[env_key] = {}
            continue
        block = dep_match.group(1)
        env_deps: dict[str, str] = {}
        for line in block.splitlines():
            stripped = line.strip().rstrip(",").strip()
            if not stripped or stripped.startswith("#") or "var." in stripped:
                continue
            pkg, spec = _parse_dep_line(stripped)
            if pkg and pkg != "luxury-lakehouse" and spec:
                env_deps[pkg] = spec
        envs[env_key] = env_deps
    return envs


def _representative_lower_version(spec_set: SpecifierSet) -> Version | None:
    """Return the highest ``>=``/``==``/``>`` lower bound in the spec set.

    Returns ``None`` when the spec has no explicit lower bound (e.g., ``<2.0``);
    callers treat this as 'no opinion on lower' and skip that side of the
    overlap check.
    """
    lowers: list[Version] = []
    for s in spec_set:
        if s.operator in (">=", "==", ">"):
            lowers.append(Version(s.version))
    return max(lowers) if lowers else None


def _check_overlap(py_spec_str: str, tf_spec_str: str) -> tuple[str, str] | None:
    """Return ``(direction, detail)`` if the specs are mutually incompatible;
    ``None`` if they overlap.

    Bidirectional check: pyproject's lower bound must be acceptable to TF
    (otherwise TF rejects what pyproject says is the minimum), AND TF's
    lower bound must be acceptable to pyproject (otherwise TF allows
    versions older than pyproject's intended floor).
    """
    py_set = SpecifierSet(py_spec_str)
    tf_set = SpecifierSet(tf_spec_str)
    py_lower = _representative_lower_version(py_set)
    tf_lower = _representative_lower_version(tf_set)

    if py_lower is not None and Version(str(py_lower)) not in tf_set:
        return (
            "py_lower_rejected_by_tf",
            f"TF spec rejects pyproject's lower bound {py_lower}",
        )
    if tf_lower is not None and Version(str(tf_lower)) not in py_set:
        return (
            "tf_lower_rejected_by_py",
            f"pyproject spec rejects TF's lower bound {tf_lower}",
        )
    return None


def test_terraform_env_specs_align_with_pyproject() -> None:
    """For each ``terraform/modules/workflows/main.tf`` environment.spec.dependencies
    entry, if the package is also listed in ``pyproject.toml``, the TF spec
    and pyproject spec must have a non-empty version overlap.
    """
    py_deps = _parse_pyproject_deps()
    tf_envs = _parse_tf_env_deps()
    drifts: list[str] = []
    for env_key, env_deps in tf_envs.items():
        for pkg, tf_spec in env_deps.items():
            if pkg not in py_deps:
                # TF-only dep (statsbombpy, kloppy, etc.) — by design.
                continue
            py_spec = py_deps[pkg]
            err = _check_overlap(py_spec, tf_spec)
            if err:
                _direction, detail = err
                drifts.append(f"  [{env_key}] {pkg}: {detail} (TF='{tf_spec}', pyproject='{py_spec}')")
    assert not drifts, (
        "TF env-spec drift from pyproject.toml — production may install "
        "different versions than local dev tested with:\n" + "\n".join(drifts)
    )


def test_parser_finds_known_analytics_deps() -> None:
    """Sanity anchor: a parser regression that produces empty results would
    silently pass the parity test. Anchor against known-stable contents."""
    tf_envs = _parse_tf_env_deps()
    assert "analytics" in tf_envs, "analytics env not parsed"
    assert "silly-kicks" in tf_envs["analytics"], "silly-kicks not parsed from analytics env — parser regression?"
    py_deps = _parse_pyproject_deps()
    assert "silly-kicks" in py_deps, "silly-kicks not parsed from pyproject — parser regression?"
