"""Tests for scripts/bump_wheel.py — consumer discovery, sync, and check logic."""

from __future__ import annotations

import sys
import textwrap
from pathlib import Path

import pytest

# ---------------------------------------------------------------------------
# Import bump_wheel.py from scripts/ (not a package, so use spec_from_file_location)
# ---------------------------------------------------------------------------

_SCRIPTS_DIR = Path(__file__).resolve().parent.parent.parent / "scripts"
_BUMP_WHEEL_PATH = _SCRIPTS_DIR / "bump_wheel.py"


@pytest.fixture(autouse=True)
def _ensure_bump_wheel_importable() -> None:
    """Add scripts/ to sys.path so ``import bump_wheel`` works in tests."""
    scripts_str = str(_SCRIPTS_DIR)
    if scripts_str not in sys.path:
        sys.path.insert(0, scripts_str)


def _setup_project(tmp_path: Path, *, version: str = "0.3.0", wheel_version: str = "0.3.0") -> Path:
    """Create a minimal project structure for bump_wheel.py testing."""
    # pyproject.toml
    (tmp_path / "pyproject.toml").write_text(f'[project]\nname = "luxury-lakehouse"\nversion = "{version}"\n')

    # src/shared/wheel.py
    shared = tmp_path / "src" / "shared"
    shared.mkdir(parents=True)
    (shared / "wheel.py").write_text(f'WHEEL_VERSION = "{wheel_version}"\n')

    # scripts/ with a PEP 723 consumer and a non-consumer
    scripts = tmp_path / "scripts"
    scripts.mkdir()
    (scripts / "train_hf.py").write_text(
        textwrap.dedent("""\
        # /// script
        # dependencies = [
        #     "luxury-lakehouse @ https://hf.co/luxury_lakehouse-0.1.0-py3-none-any.whl",
        # ]
        # ///
    """)
    )
    (scripts / "unrelated.py").write_text("# no wheel reference\n")
    (scripts / "bump_wheel.py").write_text("# self — should be excluded\nluxury_lakehouse-0.1.0-py3-none-any.whl\n")

    # terraform/
    tf = tmp_path / "terraform" / "modules"
    tf.mkdir(parents=True)
    (tf / "main.tf").write_text('wheel_path = "luxury_lakehouse-0.1.0-py3-none-any.whl"\n')

    # deploy.sh
    (scripts / "deploy.sh").write_text('WHEEL_NAME="luxury_lakehouse-0.1.0-py3-none-any.whl"\n')

    return tmp_path


class TestDiscoverConsumers:
    """Consumer discovery functions find files with wheel URL references."""

    @staticmethod
    def _all_consumers(project: Path) -> list[Path]:
        """Combine hash + version-only consumers into one list."""
        from bump_wheel import _discover_hash_consumers, _discover_version_only_consumers

        return _discover_hash_consumers(project) + _discover_version_only_consumers(project)

    def test_finds_matching_scripts(self, tmp_path: Path) -> None:
        project = _setup_project(tmp_path)
        names = [p.name for p in self._all_consumers(project)]

        assert "train_hf.py" in names
        assert "deploy.sh" in names
        assert "main.tf" in names

    def test_excludes_self(self, tmp_path: Path) -> None:
        project = _setup_project(tmp_path)
        names = [p.name for p in self._all_consumers(project)]

        assert "bump_wheel.py" not in names

    def test_excludes_non_matching(self, tmp_path: Path) -> None:
        project = _setup_project(tmp_path)
        names = [p.name for p in self._all_consumers(project)]

        assert "unrelated.py" not in names

    def test_terraform_in_version_only_group(self, tmp_path: Path) -> None:
        """Terraform consumers must be in the version-only group, not the hash group."""
        from bump_wheel import _discover_hash_consumers, _discover_version_only_consumers

        project = _setup_project(tmp_path)
        hash_names = [p.name for p in _discover_hash_consumers(project)]
        version_only_names = [p.name for p in _discover_version_only_consumers(project)]

        assert "main.tf" in version_only_names
        assert "main.tf" not in hash_names
        assert "train_hf.py" in hash_names
        assert "train_hf.py" not in version_only_names


class TestSync:
    """_sync propagates version to all consumers."""

    def test_dry_run_does_not_modify(self, tmp_path: Path) -> None:
        from bump_wheel import _sync

        project = _setup_project(tmp_path, version="0.3.0")
        original = (project / "scripts" / "train_hf.py").read_text()

        changed = _sync(project, dry_run=True)

        assert changed > 0
        # File should NOT be modified in dry-run
        assert (project / "scripts" / "train_hf.py").read_text() == original

    def test_sync_updates_consumers(self, tmp_path: Path) -> None:
        from bump_wheel import _sync

        project = _setup_project(tmp_path, version="0.3.0")

        changed = _sync(project)

        assert changed > 0
        assert "luxury_lakehouse-0.3.0-py3-none-any.whl" in (project / "scripts" / "train_hf.py").read_text()
        assert "luxury_lakehouse-0.3.0-py3-none-any.whl" in (project / "scripts" / "deploy.sh").read_text()
        assert "luxury_lakehouse-0.3.0-py3-none-any.whl" in (project / "terraform" / "modules" / "main.tf").read_text()

    def test_sync_with_hash(self, tmp_path: Path) -> None:
        from bump_wheel import _sync

        project = _setup_project(tmp_path, version="0.3.0")
        test_hash = "a" * 64

        _sync(project, sha256=test_hash)

        content = (project / "scripts" / "train_hf.py").read_text()
        assert f"luxury_lakehouse-0.3.0-py3-none-any.whl#sha256={'a' * 64}" in content

    def test_sync_returns_zero_when_current(self, tmp_path: Path) -> None:
        from bump_wheel import _sync

        project = _setup_project(tmp_path, version="0.3.0")
        _sync(project)  # First sync brings everything to 0.3.0

        changed = _sync(project)  # Second sync should find nothing to do
        assert changed == 0


class TestCheck:
    """_check verifies version consistency."""

    def test_returns_one_when_stale(self, tmp_path: Path) -> None:
        from bump_wheel import _check

        project = _setup_project(tmp_path, version="0.3.0")
        # train_hf.py has 0.1.0 — should be stale
        assert _check(project) == 1

    def test_returns_zero_when_consistent(self, tmp_path: Path) -> None:
        from bump_wheel import _check, _sync

        project = _setup_project(tmp_path, version="0.3.0")
        _sync(project)  # Bring everything to 0.3.0

        assert _check(project) == 0
