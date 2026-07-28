"""dbt versions used by CI must equal ``uv.lock`` (ADR-046 lockstep, 2026-07-27).

The dbt-live-ci outage this guards: the runner produced ``manifest_main.json`` with
uv.lock's dbt-core (1.11.12) while the Databricks job pip-installed a *range*
(``>=1.10.0,<1.12.0``) that resolved 1.11.8. dbt refused the newer manifest --
``Field "macros" of type Mapping[str, Macro] in WritableManifest has invalid value`` --
and exited 2, every night, from the day the lock was bumped.

``run_dbt_in_databricks.py``'s runtime ``pip install`` is now deleted: the job declares its
dependencies on the serverless environment instead (``trigger_dbt_job.py``). What remains
pinned here is that declaration plus the ``uvx --from`` invocations that materialise dbt
packages on the runner.
"""

from __future__ import annotations

import re
from pathlib import Path

import pytest

from scripts._tf_env_pins import parse_lock_versions

_REPO = Path(__file__).resolve().parents[2]
_UVX_WORKFLOWS = (
    "data-quality-ci.yml",
    "lakebase-grants.yml",
    "python-ci.yml",
    "synced-table-heal-e2e.yml",
)
_UVX_RE = re.compile(r'uvx --from "(?P<spec>dbt-core[^"]*)"')
_SUBMIT_DEP_RE = re.compile(r'"(?P<pkg>dbt-core|dbt-databricks)==(?P<version>[\w.]+)"')
_PYPROJECT_FLOOR_RE = re.compile(r'"(?P<pkg>dbt-core|dbt-databricks)>=(?P<version>[\w.]+)"')


def _lock_version(pkg: str) -> str:
    versions = parse_lock_versions((_REPO / "uv.lock").read_text(encoding="utf-8")).get(pkg, set())
    assert len(versions) == 1, f"expected exactly one {pkg} version in uv.lock, got {sorted(versions)}"
    return next(iter(versions))


def test_lock_pins_are_discoverable() -> None:
    """Non-vacuity: if uv.lock parsing silently yielded nothing, every check below would
    pass on an empty set forever."""
    assert _lock_version("dbt-core")
    assert _lock_version("dbt-databricks")


@pytest.mark.parametrize("workflow", _UVX_WORKFLOWS)
def test_uvx_dbt_invocations_are_pinned_to_the_lock(workflow: str) -> None:
    """``uvx --from "dbt-core>=..."`` re-resolves on every run — the same drift class."""
    text = (_REPO / ".github" / "workflows" / workflow).read_text(encoding="utf-8")
    specs = _UVX_RE.findall(text)
    assert specs, f"{workflow}: no `uvx --from dbt-core` found — the scanner is looking in the wrong place"
    want = f"dbt-core=={_lock_version('dbt-core')}"
    assert all(s == want for s in specs), f"{workflow}: expected {want!r}, found {specs}"


def test_the_submitted_job_declares_locked_dbt_dependencies() -> None:
    """The job's serverless environment must declare exactly uv.lock's versions.

    This is what replaced the runtime ``pip install`` — see the module docstring.
    """
    text = (_REPO / "scripts" / "trigger_dbt_job.py").read_text(encoding="utf-8")
    found = {m.group("pkg"): m.group("version") for m in _SUBMIT_DEP_RE.finditer(text)}
    assert found == {
        "dbt-core": _lock_version("dbt-core"),
        "dbt-databricks": _lock_version("dbt-databricks"),
    }, f"trigger_dbt_job.py declares {found}"


def test_the_runtime_pip_install_is_gone() -> None:
    """``_DBT_PIN``/``install_dbt()`` are deleted, not merely pinned (spec D6).

    A pinned runtime install still needs hand-syncing; a declared environment does not.
    """
    text = (_REPO / "scripts" / "ci" / "run_dbt_in_databricks.py").read_text(encoding="utf-8")
    for token in ("_DBT_PIN", "_DBT_DATABRICKS_PIN", "def install_dbt"):
        assert token not in text, f"{token} still present — the runtime install was not removed"


def test_pyproject_floors_do_not_exceed_the_lock() -> None:
    """Consistency only — pyproject is NEVER rewritten (spec D7).

    pyproject is the *input* to uv.lock; making it a sync target would create
    pyproject -> lock -> pyproject. ADR-046 inverts this for ``databricks-sdk`` alone
    (``ExemptStrategy.SDK_EXTRA``) because the dbt extra forks that package in the lock;
    dbt has no equivalent fork, so the normal direction holds.
    """
    from packaging.version import Version

    text = (_REPO / "pyproject.toml").read_text(encoding="utf-8")
    floors = {m.group("pkg"): m.group("version") for m in _PYPROJECT_FLOOR_RE.finditer(text)}
    assert floors, "no dbt floors found in pyproject.toml — the scanner is looking in the wrong place"
    for pkg, floor in floors.items():
        assert Version(floor) <= Version(_lock_version(pkg)), (
            f"pyproject floor {pkg}>={floor} exceeds uv.lock {_lock_version(pkg)}; "
            f"run `uv lock` rather than editing the derived pins"
        )
