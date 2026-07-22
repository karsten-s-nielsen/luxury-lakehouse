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
    m = re.match(r"^([A-Za-z0-9._-]+)(?:\[[^\]]*\])?\s*([<>=!~,\d\s.\w-]*)$", line)
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
        r"^\s*(?:dependencies|[\w-]+)\s*=\s*\[\s*\n((?:.*\n)*?)\s*\]",
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
        # The list-closing "]" sits alone on its own line — anchor on "\n + whitespace + ]"
        # so inline brackets inside dep strings (e.g. "silly-kicks[das,ghost-gk]==4.20.1")
        # don't truncate the captured block.
        dep_match = re.search(
            r"dependencies\s*=\s*(?:concat\s*\(\s*\[[^\]]*\]\s*,\s*)?\[(.*?)\n\s*\]",
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

    Exact-pin TF specs (``==X``, the norm since 2026-06-10) only need the
    pinned version to SATISFY pyproject's spec — a pin above pyproject's
    floor is exactly the intended state (lock-resolved version), not drift.
    """
    py_set = SpecifierSet(py_spec_str)
    tf_set = SpecifierSet(tf_spec_str)

    tf_exact = [s for s in tf_set if s.operator == "=="]
    if len(tf_exact) == 1 and len(list(tf_set)) == 1:
        pinned = Version(tf_exact[0].version)
        if pinned not in py_set:
            return (
                "tf_pin_rejected_by_py",
                f"pyproject spec rejects TF's exact pin {pinned}",
            )
        return None

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


def _env_key_for_task(task_key: str) -> str | None:
    """Return the ``environment_key`` assigned to the named ``task_key`` in main.tf.

    Captures the first ``environment_key = "..."`` after the task's ``task_key = "..."`` line.
    """
    text = _TF.read_text(encoding="utf-8")
    m = re.search(
        rf'task_key\s*=\s*"{re.escape(task_key)}"(.*?)environment_key\s*=\s*"([^"]+)"',
        text,
        re.DOTALL,
    )
    return m.group(2) if m else None


def test_refresh_synced_tables_env_ships_databricks_sdk() -> None:
    """``refresh_synced_tables`` calls ``ws.postgres.*`` (databricks-sdk PostgresAPI). Its task
    env MUST install databricks-sdk — otherwise the task falls back to the serverless runtime's
    bundled SDK, which lacks ``.postgres``, and every synced-table refresh fails with SystemExit:1.

    Regression guard for the 2026-06-05 all-42-syncs-fail incident: the task ran in the bare
    ``default`` env (wheel only), and base ``[project.dependencies]`` does not include databricks-sdk
    (it lives only in the ``[sdk]`` / ``[taipy-app]`` extras). ``test_terraform_env_specs_align_with_pyproject``
    only checks version OVERLAP for shared packages — it cannot catch a task whose env is MISSING a
    package it imports. This test closes that gap for the one task whose contract requires the SDK.
    """
    env_key = _env_key_for_task("refresh_synced_tables")
    assert env_key is not None, "could not locate refresh_synced_tables task / environment_key in main.tf"
    tf_envs = _parse_tf_env_deps()
    assert env_key in tf_envs, f"refresh_synced_tables references env '{env_key}' not defined in main.tf"
    assert "databricks-sdk" in tf_envs[env_key], (
        f"refresh_synced_tables runs in env '{env_key}' which does NOT ship databricks-sdk — "
        "ws.postgres.* will fail against the runtime's bundled SDK. Add databricks-sdk to that env."
    )


def test_parser_finds_known_analytics_deps() -> None:
    """Sanity anchor: a parser regression that produces empty results would
    silently pass the parity test. Anchor against known-stable contents."""
    tf_envs = _parse_tf_env_deps()
    assert "analytics" in tf_envs, "analytics env not parsed"
    assert "silly-kicks" in tf_envs["analytics"], "silly-kicks not parsed from analytics env — parser regression?"
    py_deps = _parse_pyproject_deps()
    assert "silly-kicks" in py_deps, "silly-kicks not parsed from pyproject — parser regression?"


# ---------------------------------------------------------------------------
# Exact-pin enforcement (2026-06-10)
# ---------------------------------------------------------------------------
#
# Serverless REBUILDS each env on every wheel bump and re-resolves the dep specs
# against PyPI at build time. With floor specs (>=) that made prod silently run
# THREE different silly-kicks versions in one day (4.21.0 → 4.21.1 → 4.21.2 —
# none of them the lock-tested 4.20.1; observed via the executor env fingerprint).
# The overlap test above cannot catch this: a floor always "overlaps". The two
# tests below close it: every TF env dep must be an EXACT pin, and the pin must
# equal the version local tests + goldens actually ran (uv.lock), so a version
# bump in prod is always a deliberate pyproject+lock+terraform change.

_LOCK = _REPO / "uv.lock"

# Pins whose source of truth is NOT uv.lock's default resolution — each with the reason.
_LOCK_PARITY_EXEMPT: dict[str, str] = {
    # The dev lock's default resolution pins databricks-sdk 0.77.0 (extras-conflict
    # artifact — see memory reference_local_venv_sdk_077_migrate_test_fail). The
    # lakebase env intentionally runs the [sdk]-extra pin (postgres APIs); parity
    # with pyproject's ==pin is asserted separately below.
    "databricks-sdk": "pyproject [sdk] extra is the source of truth",
    # TF-only ingestion API client — not declared in pyproject, so not lock-managed.
    "statsbombpy": "terraform-only dep; exact pin required but no lock entry",
}


def _parse_lock_versions() -> dict[str, str]:
    """Parse uv.lock ``[[package]]`` blocks → {normalized_name: version}."""
    text = _LOCK.read_text(encoding="utf-8")
    out: dict[str, str] = {}
    for m in re.finditer(r'\[\[package\]\]\nname = "([^"]+)"\nversion = "([^"]+)"', text):
        out[m.group(1).lower().replace("_", "-")] = m.group(2)
    return out


def test_tf_env_deps_are_exact_pins() -> None:
    """Every PyPI dep in every TF env spec must use ``==`` — floors re-resolve to
    whatever PyPI has at env-build time, putting untested versions in prod."""
    tf_envs = _parse_tf_env_deps()
    offenders: list[str] = []
    for env_key, env_deps in tf_envs.items():
        for pkg, spec in env_deps.items():
            if not re.fullmatch(r"==[\w.\-]+", spec.replace(" ", "")):
                offenders.append(f"  [{env_key}] {pkg}: '{spec}'")
    assert not offenders, (
        "TF env deps must be EXACT pins (==) — floors let serverless env rebuilds "
        "install untested versions (the 4.21.0/4.21.1/4.21.2-in-one-day incident):\n" + "\n".join(offenders)
    )


def test_tf_exact_pins_match_uv_lock() -> None:
    """Each TF ``pkg==X`` must equal uv.lock's resolved version for ``pkg`` — prod
    runs exactly what local tests + goldens validated. Exemptions (with reasons)
    in ``_LOCK_PARITY_EXEMPT``."""
    tf_envs = _parse_tf_env_deps()
    lock = _parse_lock_versions()
    drifts: list[str] = []
    for env_key, env_deps in tf_envs.items():
        for pkg, spec in env_deps.items():
            if pkg in _LOCK_PARITY_EXEMPT:
                continue
            pinned = spec.replace(" ", "").removeprefix("==")
            if pkg not in lock:
                drifts.append(
                    f"  [{env_key}] {pkg}=={pinned}: not in uv.lock — add to pyproject or exempt with a reason"
                )
            elif lock[pkg] != pinned:
                drifts.append(f"  [{env_key}] {pkg}: TF pins {pinned} but uv.lock (tested) has {lock[pkg]}")
    assert not drifts, (
        "TF env pins drifted from uv.lock — prod would run versions local tests never saw. "
        "Bump pyproject + `uv lock` + terraform together:\n" + "\n".join(drifts)
    )


def test_lakebase_sdk_pin_matches_pyproject_extra() -> None:
    """databricks-sdk is lock-parity-exempt; its parity contract is the pyproject
    ``[sdk]`` extra's exact pin instead."""
    tf_envs = _parse_tf_env_deps()
    tf_pin = tf_envs.get("lakebase", {}).get("databricks-sdk", "").replace(" ", "").removeprefix("==")
    assert tf_pin, "lakebase env must pin databricks-sdk"
    py_text = _PYPROJECT.read_text(encoding="utf-8")
    m = re.search(r'"databricks-sdk==([\w.\-]+)"', py_text)
    assert m is not None, "pyproject must carry an exact databricks-sdk pin in the [sdk] extra"
    assert tf_pin == m.group(1), (
        f"lakebase env pins databricks-sdk=={tf_pin} but pyproject's extra pins =={m.group(1)} — keep them in lockstep"
    )


def test_dependabot_only_dev_deps_grouped() -> None:
    """Only dev/tooling deps may be batched into a Dependabot VERSION group.

    Grouping PROD deps perturbs the conflict-fork resolution: an mlflow bump drags
    env-pinned mlflow-skinny, and a batch even downgraded dbt in the default fork,
    tripping :func:`test_terraform_env_dep_parity` (the closed group #476). Env-pinned
    deps must get INDIVIDUAL PRs so each can be completed with the ADR-046 TF
    lockstep (#447). ``numba`` is dev-classified but env-pinned, so it must be
    excluded even from the dev group. Guards ``.github/dependabot.yml`` against a
    future broad/prod version group re-introducing the drift.
    """
    import yaml

    dependabot = yaml.safe_load((_REPO / ".github" / "dependabot.yml").read_text(encoding="utf-8"))
    uv_update = next(u for u in dependabot["updates"] if u["package-ecosystem"] == "uv")

    env_pins = {pkg for env in _parse_tf_env_deps().values() for pkg in env}
    assert "numba" in env_pins, "expected numba to be a serverless-env pin in main.tf"

    version_groups = {
        name: g
        for name, g in uv_update.get("groups", {}).items()
        if g.get("applies-to", "version-updates") == "version-updates"
    }
    assert version_groups, "expected at least one version-update group in dependabot.yml"

    for name, g in version_groups.items():
        assert g.get("dependency-type") == "development", (
            f"Dependabot version group '{name}' is not restricted to "
            "dependency-type: development — grouping prod deps perturbs the ADR-046 "
            "env-pin resolution (see the closed group #476). Prod deps must stay "
            "individual so each gets the #447 lockstep."
        )
        excluded = {p.lower() for p in g.get("exclude-patterns", [])}
        assert "numba" in excluded, f"env-pinned dev dep 'numba' must be in group '{name}' exclude-patterns"
