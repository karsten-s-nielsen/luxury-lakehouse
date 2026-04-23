"""Unit tests for scripts.ci.run_dbt_in_databricks (PR 4a shim).

The shim runs inside a Databricks Job cluster. It reads a tarball of
dbt_project/ + manifest-main.json from UC Volume, extracts, installs
dbt, runs `dbt build --select <arg>`, and uploads run_results.json
back to UC Volume.

All subprocess and file-system interactions are mocked here; integration
testing happens in Phase 5 E2E.
"""

from __future__ import annotations

import json
import subprocess
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from scripts.ci import run_dbt_in_databricks as shim


class TestParseArgs:
    def test_happy_path_required_args(self) -> None:
        args = shim.parse_args(
            [
                "--tarball-path",
                "/Volumes/x/y/z/proj.tar.gz",
                "--manifest-path",
                "/Volumes/x/y/z/manifest_main.json",
                "--select-arg",
                "state:modified+",
                "--output-path",
                "/Volumes/x/y/z/run_results.json",
            ]
        )
        assert args.tarball_path == "/Volumes/x/y/z/proj.tar.gz"
        assert args.select_arg == "state:modified+"

    def test_missing_required_raises(self) -> None:
        with pytest.raises(SystemExit):
            shim.parse_args(["--tarball-path", "/a"])


class TestDownloadFromVolume:
    @patch("scripts.ci.run_dbt_in_databricks._workspace_client")
    def test_download_writes_bytes_to_tmp(self, mock_ws: MagicMock, tmp_path: Path) -> None:
        mock_files = MagicMock()
        mock_ws.return_value.files = mock_files
        mock_files.download.return_value = MagicMock(contents=b"fake-tarball-bytes")
        out = tmp_path / "proj.tar.gz"

        shim.download_from_volume("/Volumes/x/y/z/proj.tar.gz", out)

        assert out.read_bytes() == b"fake-tarball-bytes"
        mock_files.download.assert_called_once_with("/Volumes/x/y/z/proj.tar.gz")


class TestExtractTarball:
    def test_extract_creates_dbt_project_directory(self, tmp_path: Path) -> None:
        # Build a real tarball fixture: a tar with a dbt_project/ dir containing profiles.yml.
        import tarfile

        src_dir = tmp_path / "src"
        (src_dir / "dbt_project").mkdir(parents=True)
        (src_dir / "dbt_project" / "profiles.yml").write_text("hello")

        tarball = tmp_path / "proj.tar.gz"
        with tarfile.open(tarball, "w:gz") as tf:
            tf.add(src_dir / "dbt_project", arcname="dbt_project")

        extract_dir = tmp_path / "extract"
        shim.extract_tarball(tarball, extract_dir)

        assert (extract_dir / "dbt_project" / "profiles.yml").read_text() == "hello"


class TestRunDbt:
    @patch("scripts.ci.run_dbt_in_databricks.subprocess.run")
    def test_dbt_deps_called_first(self, mock_run: MagicMock, tmp_path: Path) -> None:
        proj_dir = tmp_path / "dbt_project"
        proj_dir.mkdir()
        mock_run.return_value = subprocess.CompletedProcess(args=[], returncode=0)

        shim.run_dbt(
            project_dir=proj_dir,
            select_arg="state:modified+",
            manifest_main_dir=tmp_path / "manifest",
        )

        calls = mock_run.call_args_list
        assert len(calls) == 2
        # First call: dbt deps
        assert calls[0][0][0][:2] == ["dbt", "deps"]
        # Second call: dbt build --select ...
        assert "--select" in calls[1][0][0]
        assert "state:modified+" in calls[1][0][0]

    @patch("scripts.ci.run_dbt_in_databricks.subprocess.run")
    def test_dbt_build_failure_propagates(self, mock_run: MagicMock, tmp_path: Path) -> None:
        proj_dir = tmp_path / "dbt_project"
        proj_dir.mkdir()
        mock_run.side_effect = [
            subprocess.CompletedProcess(args=[], returncode=0),  # dbt deps OK
            subprocess.CompletedProcess(args=[], returncode=2),  # dbt build failed
        ]

        rc = shim.run_dbt(
            project_dir=proj_dir,
            select_arg="state:modified+",
            manifest_main_dir=tmp_path / "manifest",
        )
        assert rc == 2


class TestUploadOutput:
    @patch("scripts.ci.run_dbt_in_databricks._workspace_client")
    def test_uploads_run_results_json(self, mock_ws: MagicMock, tmp_path: Path) -> None:
        results = tmp_path / "run_results.json"
        results.write_text(json.dumps({"results": []}))
        mock_files = MagicMock()
        mock_ws.return_value.files = mock_files

        shim.upload_output(results, "/Volumes/x/y/z/run_results.json")

        mock_files.upload.assert_called_once()
        args, kwargs = mock_files.upload.call_args
        assert args[0] == "/Volumes/x/y/z/run_results.json"
        assert kwargs.get("overwrite", False) is True


class TestMainEndToEnd:
    @patch("scripts.ci.run_dbt_in_databricks.upload_output")
    @patch("scripts.ci.run_dbt_in_databricks.run_dbt")
    @patch("scripts.ci.run_dbt_in_databricks.install_dbt")
    @patch("scripts.ci.run_dbt_in_databricks.stage_dbt_workspace")
    @patch("scripts.ci.run_dbt_in_databricks.extract_tarball")
    @patch("scripts.ci.run_dbt_in_databricks.download_from_volume")
    def test_main_returns_dbt_exit_code(
        self,
        mock_dl: MagicMock,
        mock_ex: MagicMock,
        mock_stage: MagicMock,
        mock_install: MagicMock,
        mock_run: MagicMock,
        mock_up: MagicMock,
    ) -> None:
        # stage_dbt_workspace returns (project_dir, target_main_dir). Downstream
        # `main` passes those into run_dbt (mocked) and checks for target/run_results.json
        # relative to project_dir — mock with /tmp paths so the missing-results fallback fires.
        mock_stage.return_value = (Path("/tmp/mock_proj"), Path("/tmp/mock_proj/target-main"))  # noqa: S108 — test mock
        mock_run.return_value = 2  # dbt failed
        rc = shim.main(
            [
                "--tarball-path",
                "/Volumes/a/b/c/proj.tar.gz",
                "--manifest-path",
                "/Volumes/a/b/c/manifest_main.json",
                "--select-arg",
                "state:modified+",
                "--output-path",
                "/Volumes/a/b/c/run_results.json",
            ]
        )
        assert rc == 2
        mock_up.assert_called_once()  # Output uploaded even on dbt failure (empty placeholder when no run_results.json)
