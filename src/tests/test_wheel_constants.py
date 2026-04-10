"""Tests for wheel version constants and rewrite functions."""

from __future__ import annotations

import importlib.metadata
from pathlib import Path

import pytest


class TestWheelConstants:
    """WHEEL_VERSION must stay in sync with pyproject.toml."""

    def test_version_matches_pyproject(self) -> None:
        from shared.wheel import WHEEL_VERSION

        expected = importlib.metadata.version("luxury-lakehouse")
        assert WHEEL_VERSION == expected, (
            f"WHEEL_VERSION={WHEEL_VERSION!r} != pyproject.toml version={expected!r}. "
            "Run: uv run python scripts/bump_wheel.py"
        )

    def test_filename_format(self) -> None:
        from shared.wheel import WHEEL_FILENAME, WHEEL_VERSION

        assert WHEEL_FILENAME == f"luxury_lakehouse-{WHEEL_VERSION}-py3-none-any.whl"

    def test_base_url_format(self) -> None:
        from shared.wheel import WHEEL_BASE_URL, WHEEL_FILENAME

        assert WHEEL_BASE_URL.startswith("https://huggingface.co/")
        assert WHEEL_BASE_URL.endswith(WHEEL_FILENAME)
        assert "/resolve/main/" in WHEEL_BASE_URL

    def test_repo_constant(self) -> None:
        from shared.wheel import WHEEL_REPO

        assert WHEEL_REPO == "luxury-lakehouse/build-artifacts"


class TestRewriteWheelUrl:
    """rewrite_wheel_url replaces wheel filename references."""

    def test_basic_replacement(self) -> None:
        from shared.wheel import rewrite_wheel_url

        text = (
            '#     "luxury-lakehouse @ https://huggingface.co/'
            "luxury-lakehouse/build-artifacts/resolve/main/"
            'luxury_lakehouse-0.1.0-py3-none-any.whl",'
        )
        result = rewrite_wheel_url(text, "0.3.0")
        assert "luxury_lakehouse-0.3.0-py3-none-any.whl" in result
        assert "luxury_lakehouse-0.1.0-py3-none-any.whl" not in result

    def test_preserves_surrounding_text(self) -> None:
        from shared.wheel import rewrite_wheel_url

        text = (
            '#     "luxury-lakehouse[analytics,training] @ '
            'https://example.com/luxury_lakehouse-0.1.0-py3-none-any.whl",'
        )
        result = rewrite_wheel_url(text, "0.3.0")
        assert "[analytics,training]" in result
        assert result.startswith('#     "luxury-lakehouse[analytics,training]')
        assert result.endswith('.whl",')

    def test_adds_sha256_fragment(self) -> None:
        from shared.wheel import rewrite_wheel_url

        text = "luxury_lakehouse-0.1.0-py3-none-any.whl"
        test_hash = "ab" * 32  # pragma: allowlist secret
        result = rewrite_wheel_url(text, "0.3.0", sha256=test_hash)
        assert f"luxury_lakehouse-0.3.0-py3-none-any.whl#sha256={test_hash}" in result

    def test_replaces_existing_hash(self) -> None:
        from shared.wheel import rewrite_wheel_url

        old_hash = "a" * 64
        new_hash = "b" * 64
        text = f"luxury_lakehouse-0.2.0-py3-none-any.whl#sha256={old_hash}"
        result = rewrite_wheel_url(text, "0.3.0", sha256=new_hash)
        assert f"luxury_lakehouse-0.3.0-py3-none-any.whl#sha256={new_hash}" in result
        assert old_hash not in result

    def test_terraform_path(self) -> None:
        from shared.wheel import rewrite_wheel_url

        text = 'wheel_path = "${module.catalog.libs_volume_path}/luxury_lakehouse-0.3.0-py3-none-any.whl"'
        result = rewrite_wheel_url(text, "0.4.0")
        assert "luxury_lakehouse-0.4.0-py3-none-any.whl" in result

    def test_deploy_sh(self) -> None:
        from shared.wheel import rewrite_wheel_url

        text = 'WHEEL_NAME="luxury_lakehouse-0.1.0-py3-none-any.whl"'
        result = rewrite_wheel_url(text, "0.3.0")
        assert "luxury_lakehouse-0.3.0-py3-none-any.whl" in result

    def test_no_match_returns_unchanged(self) -> None:
        from shared.wheel import rewrite_wheel_url

        text = "no wheel reference here"
        assert rewrite_wheel_url(text, "0.3.0") == text

    def test_multiple_occurrences(self) -> None:
        from shared.wheel import rewrite_wheel_url

        text = "luxury_lakehouse-0.1.0-py3-none-any.whl\nluxury_lakehouse-0.2.0-py3-none-any.whl"
        result = rewrite_wheel_url(text, "0.3.0")
        assert result.count("luxury_lakehouse-0.3.0-py3-none-any.whl") == 2


class TestRewriteWheelVersionConstant:
    """rewrite_wheel_version_constant updates the WHEEL_VERSION line."""

    def test_basic(self) -> None:
        from shared.wheel import rewrite_wheel_version_constant

        text = 'WHEEL_VERSION = "0.2.0"\n'
        result = rewrite_wheel_version_constant(text, "0.3.0")
        assert 'WHEEL_VERSION = "0.3.0"' in result
        assert "0.2.0" not in result

    def test_preserves_surrounding(self) -> None:
        from shared.wheel import rewrite_wheel_version_constant

        text = '"""Docstring."""\n\nWHEEL_VERSION = "0.1.0"\n\nWHEEL_REPO = "foo"\n'
        result = rewrite_wheel_version_constant(text, "0.3.0")
        assert '"""Docstring."""' in result
        assert 'WHEEL_REPO = "foo"' in result
        assert 'WHEEL_VERSION = "0.3.0"' in result


class TestReadPyprojectVersion:
    """read_pyproject_version extracts version from pyproject.toml."""

    def test_reads_version(self, tmp_path: Path) -> None:
        from shared.wheel import read_pyproject_version

        (tmp_path / "pyproject.toml").write_text('[project]\nname = "luxury-lakehouse"\nversion = "1.2.3"\n')
        assert read_pyproject_version(tmp_path) == "1.2.3"

    def test_raises_on_missing(self, tmp_path: Path) -> None:
        from shared.wheel import read_pyproject_version

        (tmp_path / "pyproject.toml").write_text("[project]\nname = 'test'\n")
        with pytest.raises(ValueError, match="Could not find version"):
            read_pyproject_version(tmp_path)
