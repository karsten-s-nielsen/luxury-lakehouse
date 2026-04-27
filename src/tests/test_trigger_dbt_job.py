"""Unit tests for scripts.trigger_dbt_job (PR 4a GH-side trigger)."""

from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest
import requests

from scripts import trigger_dbt_job as trigger


class TestBuildRunsSubmitPayload:
    def test_payload_structure(self) -> None:
        payload = trigger.build_runs_submit_payload(
            pr_number=42,
            commit_sha="abc1234",
            tarball_volume_path="/Volumes/x/y/z/p.tar.gz",
            manifest_volume_path="/Volumes/x/y/z/m.json",
            select_arg="state:modified+",
            output_volume_path="/Volumes/x/y/z/out.json",
        )
        assert payload["run_name"] == "dbt-live-ci (PR #42, abc1234)"
        assert len(payload["tasks"]) == 1
        task = payload["tasks"][0]
        assert task["task_key"] == "dbt_build"
        assert task["environment_key"] == "Default"
        assert task["performance_target"] == "PERFORMANCE_OPTIMIZED"
        assert task["spark_python_task"]["python_file"] == (
            "/Workspace/Shared/luxury-lakehouse-ci/run_dbt_in_databricks.py"
        )
        params = task["spark_python_task"]["parameters"]
        assert "--select-arg" in params
        assert "state:modified+" in params
        assert "--tarball-path" in params
        assert "/Volumes/x/y/z/p.tar.gz" in params
        # Serverless: environments[] block present, no new_cluster.
        assert "new_cluster" not in task
        assert len(payload["environments"]) == 1
        assert payload["environments"][0]["environment_key"] == "Default"


class TestSubmitRun:
    @patch("scripts.trigger_dbt_job.requests.post")
    def test_submit_returns_run_id(self, mock_post: MagicMock) -> None:
        mock_post.return_value.status_code = 200
        mock_post.return_value.json.return_value = {"run_id": 12345}
        mock_post.return_value.raise_for_status = MagicMock()

        run_id = trigger.submit_run(
            host="https://workspace.databricks.com",
            token="tok",  # noqa: S106 — test fixture, not a real credential
            payload={"run_name": "t"},
        )
        assert run_id == 12345

    @patch("scripts.trigger_dbt_job.requests.post")
    def test_submit_propagates_http_error(self, mock_post: MagicMock) -> None:
        err = requests.HTTPError("429 Too Many Requests")
        mock_post.return_value.status_code = 429
        mock_post.return_value.text = "Rate limited"
        mock_post.return_value.raise_for_status = MagicMock(side_effect=err)
        with pytest.raises(requests.HTTPError):
            trigger.submit_run(
                host="https://workspace.databricks.com",
                token="tok",  # noqa: S106 — test fixture, not a real credential
                payload={"run_name": "t"},
            )


def _sdk_run_mock(life_cycle: str, result_state: str | None, run_page_url: str = "u") -> MagicMock:
    """Build a mock SDK Run object matching the shape poll_run reads.

    `state.life_cycle_state` and `state.result_state` are SDK enums; poll_run
    accesses `.value` on each. Returning a MagicMock with `.value` set on the
    enum mocks satisfies that access pattern. Pass `result_state=None` for an
    in-flight run (poll_run's branch returns the literal None in that case).
    """
    state = MagicMock()
    state.life_cycle_state = MagicMock(value=life_cycle)
    state.result_state = MagicMock(value=result_state) if result_state is not None else None
    return MagicMock(state=state, run_page_url=run_page_url)


class TestPollRun:
    """Tests for the WorkspaceClient.jobs.get_run-based poll loop.

    Pre-PR-6-followup these tests patched `scripts.trigger_dbt_job.requests.get`
    because the implementation polled via raw HTTP and 403'd after ~5 min when
    the GitHub OIDC token expired. PR-6-followup replaced the raw HTTP path
    with the SDK's auto-token-refreshing client, so the patches moved to
    `databricks.sdk.WorkspaceClient`.
    """

    @patch("scripts.trigger_dbt_job.time.sleep", new=MagicMock())
    @patch("databricks.sdk.WorkspaceClient")
    def test_poll_returns_on_terminal_success(self, mock_ws_class: MagicMock) -> None:
        """Poll returns RunResult once the SDK reports a terminal SUCCESS."""
        mock_ws_class.return_value.jobs.get_run.side_effect = [
            _sdk_run_mock(life_cycle="RUNNING", result_state=None),
            _sdk_run_mock(life_cycle="TERMINATED", result_state="SUCCESS"),
        ]

        result = trigger.poll_run(
            host="h",
            token="t",  # noqa: S106 — test fixture, not a real credential
            run_id=1,
            max_attempts=10,
        )
        assert result.life_cycle_state == "TERMINATED"
        assert result.result_state == "SUCCESS"
        assert result.run_page_url == "u"

    @patch("scripts.trigger_dbt_job.time.sleep", new=MagicMock())
    @patch("databricks.sdk.WorkspaceClient")
    def test_poll_returns_on_terminal_failure(self, mock_ws_class: MagicMock) -> None:
        mock_ws_class.return_value.jobs.get_run.return_value = _sdk_run_mock(
            life_cycle="TERMINATED",
            result_state="FAILED",
        )
        result = trigger.poll_run(
            host="h",
            token="t",  # noqa: S106 — test fixture, not a real credential
            run_id=1,
            max_attempts=10,
        )
        assert result.result_state == "FAILED"

    @patch("scripts.trigger_dbt_job.time.sleep", new=MagicMock())
    @patch("databricks.sdk.WorkspaceClient")
    def test_poll_timeout_raises(self, mock_ws_class: MagicMock) -> None:
        mock_ws_class.return_value.jobs.get_run.return_value = _sdk_run_mock(
            life_cycle="RUNNING",
            result_state=None,
        )
        with pytest.raises(TimeoutError):
            trigger.poll_run(
                host="h",
                token="t",  # noqa: S106 — test fixture, not a real credential
                run_id=1,
                max_attempts=3,
            )


class TestUploadTarball:
    @patch("scripts.trigger_dbt_job._workspace_client")
    def test_upload_calls_files_upload(self, mock_ws: MagicMock, tmp_path: Path) -> None:
        p = tmp_path / "proj.tar.gz"
        p.write_bytes(b"fake")
        mock_files = MagicMock()
        mock_ws.return_value.files = mock_files

        trigger.upload_tarball(p, "/Volumes/x/y/z/proj.tar.gz")

        mock_files.upload.assert_called_once()
        args, kwargs = mock_files.upload.call_args
        assert args[0] == "/Volumes/x/y/z/proj.tar.gz"
        assert kwargs["overwrite"] is True


class TestMainCLI:
    @patch("scripts.trigger_dbt_job.poll_run")
    @patch("scripts.trigger_dbt_job.submit_run")
    @patch("scripts.trigger_dbt_job.upload_tarball")
    def test_main_returns_zero_on_success(
        self,
        mock_up: MagicMock,
        mock_sub: MagicMock,
        mock_poll: MagicMock,
        tmp_path: Path,
        capsys: pytest.CaptureFixture[str],
    ) -> None:
        tarball = tmp_path / "p.tar.gz"
        tarball.write_bytes(b"x")
        manifest = tmp_path / "m.json"
        manifest.write_text("{}")
        mock_sub.return_value = 123
        mock_poll.return_value = trigger.RunResult(
            life_cycle_state="TERMINATED",
            result_state="SUCCESS",
            run_page_url="https://x",
        )
        rc = trigger.main(
            [
                "--pr-number",
                "42",
                "--commit-sha",
                "abc1234",
                "--tarball",
                str(tarball),
                "--manifest",
                str(manifest),
                "--select-arg",
                "state:modified+",
                "--host",
                "https://workspace.databricks.com",
                "--token",
                "tok",
                "--volume-prefix",
                "/Volumes/soccer_analytics/dev_gold/ci_dbt/42-abc1234",
            ]
        )
        assert rc == 0
        out = capsys.readouterr().out
        assert '"result_state": "SUCCESS"' in out

    @patch("scripts.trigger_dbt_job.poll_run")
    @patch("scripts.trigger_dbt_job.submit_run")
    @patch("scripts.trigger_dbt_job.upload_tarball")
    def test_main_returns_nonzero_on_failure(
        self,
        mock_up: MagicMock,
        mock_sub: MagicMock,
        mock_poll: MagicMock,
        tmp_path: Path,
    ) -> None:
        tarball = tmp_path / "p.tar.gz"
        tarball.write_bytes(b"x")
        manifest = tmp_path / "m.json"
        manifest.write_text("{}")
        mock_sub.return_value = 123
        mock_poll.return_value = trigger.RunResult(
            life_cycle_state="TERMINATED",
            result_state="FAILED",
            run_page_url="https://x",
        )
        rc = trigger.main(
            [
                "--pr-number",
                "42",
                "--commit-sha",
                "abc1234",
                "--tarball",
                str(tarball),
                "--manifest",
                str(manifest),
                "--select-arg",
                "state:modified+",
                "--host",
                "https://workspace.databricks.com",
                "--token",
                "tok",
                "--volume-prefix",
                "/Volumes/soccer_analytics/dev_gold/ci_dbt/42-abc1234",
            ]
        )
        assert rc != 0
