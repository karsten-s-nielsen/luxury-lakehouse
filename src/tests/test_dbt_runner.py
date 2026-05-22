"""D59: dbt_runner programmatic invocation via dbtRunner inside a Databricks job.

Unit tests cover argument assembly, success/failure handling, the
wheel-bundled dbt_project path resolution, and warehouse-ID extraction
from DATABRICKS_HTTP_PATH. Real dbt execution is covered by the D59 manual
smoke test and the daily-job E2E run.
"""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest


class TestExtractWarehouseIdFromHttpPath:
    """Defensive parsing of DATABRICKS_HTTP_PATH — reject malformed values early
    instead of forwarding ``xyz`` to ``WorkspaceClient.warehouses.get(id='xyz')``
    and waiting for a cryptic 404 from the SDK.
    """

    def test_accepts_canonical_single_slash_form(self) -> None:
        from ingestion.dbt_runner import _extract_warehouse_id_from_http_path

        assert _extract_warehouse_id_from_http_path("/sql/1.0/warehouses/6c3b36ca64d183fe") == "6c3b36ca64d183fe"

    def test_accepts_msys_double_slash_form(self) -> None:
        """CLAUDE.md requires the ``//sql/...`` prefix to survive Git Bash path
        mangling on Windows. ``rsplit`` extracts the same trailing segment
        either way.
        """
        from ingestion.dbt_runner import _extract_warehouse_id_from_http_path

        assert _extract_warehouse_id_from_http_path("//sql/1.0/warehouses/6c3b36ca64d183fe") == "6c3b36ca64d183fe"

    def test_rejects_cluster_path(self) -> None:
        """A mis-pasted cluster path like ``/sql/protocolv1/o/123/xyz`` must
        fail fast with a clear error instead of passing ``xyz`` to the SDK.
        """
        from ingestion.dbt_runner import _extract_warehouse_id_from_http_path

        with pytest.raises(RuntimeError, match="Cannot extract warehouse ID"):
            _extract_warehouse_id_from_http_path("/sql/protocolv1/o/123/xyz")

    def test_rejects_empty_trailing_segment(self) -> None:
        from ingestion.dbt_runner import _extract_warehouse_id_from_http_path

        with pytest.raises(RuntimeError, match="Cannot extract warehouse ID"):
            _extract_warehouse_id_from_http_path("/sql/1.0/warehouses/")

    def test_rejects_non_hex_id(self) -> None:
        from ingestion.dbt_runner import _extract_warehouse_id_from_http_path

        with pytest.raises(RuntimeError, match="Cannot extract warehouse ID"):
            _extract_warehouse_id_from_http_path("/sql/1.0/warehouses/not-a-hex-id")


def _make_runner_result(success: bool, node_count: int = 0) -> MagicMock:
    """Build a mocked dbtRunnerResult matching dbt-core 1.10+ API."""
    mock_result = MagicMock()
    mock_result.success = success
    if success:
        mock_result.result = MagicMock()
        mock_result.result.results = [MagicMock() for _ in range(node_count)]
        mock_result.exception = None
    else:
        mock_result.result = None
        mock_result.exception = RuntimeError("dbt build failed")
    return mock_result


@patch("ingestion.dbt_runner._ensure_databricks_env_vars")
@patch("ingestion.dbt_runner.dbtRunner")
def test_run_pipeline_invokes_dbt_build_with_serverless_target(
    mock_runner_cls: MagicMock, _mock_ensure: MagicMock
) -> None:
    """run_pipeline must call dbtRunner.invoke(['build', '--project-dir', ...,
    '--profiles-dir', ..., '--target', 'serverless'])
    """
    mock_runner = MagicMock()
    mock_runner.invoke.return_value = _make_runner_result(success=True, node_count=33)
    mock_runner_cls.return_value = mock_runner

    from ingestion.dbt_runner import run_pipeline

    result = run_pipeline()
    assert result == 33

    mock_runner.invoke.assert_called_once()
    args = mock_runner.invoke.call_args.args[0]
    assert args[0] == "build"
    assert "--project-dir" in args
    assert "--profiles-dir" in args
    assert "--target" in args
    assert "serverless" in args


@patch("ingestion.dbt_runner._ensure_databricks_env_vars")
@patch("ingestion.dbt_runner.dbtRunner")
def test_run_pipeline_resolves_bundled_dbt_project_path(mock_runner_cls: MagicMock, _mock_ensure: MagicMock) -> None:
    """The project path must resolve via importlib.resources to the
    luxury_lakehouse_dbt_project package (wheel-bundled per Hatch force-include).
    """
    mock_runner = MagicMock()
    mock_runner.invoke.return_value = _make_runner_result(success=True, node_count=0)
    mock_runner_cls.return_value = mock_runner

    from ingestion.dbt_runner import run_pipeline

    run_pipeline()

    args = mock_runner.invoke.call_args.args[0]
    project_dir_idx = args.index("--project-dir")
    project_path = args[project_dir_idx + 1]
    # The path should point at the bundled package, not an absolute repo path
    assert "luxury_lakehouse_dbt_project" in project_path or "dbt_project" in project_path, (
        f"project_dir should resolve to the bundled dbt_project location. Got {project_path!r}"
    )


@patch("ingestion.dbt_runner._ensure_databricks_env_vars")
@patch("ingestion.dbt_runner.dbtRunner")
def test_run_pipeline_raises_on_dbt_failure(mock_runner_cls: MagicMock, _mock_ensure: MagicMock) -> None:
    """When dbtRunnerResult.success is False, run_pipeline must raise RuntimeError."""
    mock_runner = MagicMock()
    mock_runner.invoke.return_value = _make_runner_result(success=False)
    mock_runner_cls.return_value = mock_runner

    from ingestion.dbt_runner import run_pipeline

    with pytest.raises(RuntimeError, match="dbt build failed"):
        run_pipeline()


@patch("ingestion.dbt_runner._ensure_databricks_env_vars")
@patch("ingestion.dbt_runner.dbtRunner")
def test_main_returns_zero_on_success(mock_runner_cls: MagicMock, _mock_ensure: MagicMock) -> None:
    mock_runner = MagicMock()
    mock_runner.invoke.return_value = _make_runner_result(success=True, node_count=33)
    mock_runner_cls.return_value = mock_runner

    from ingestion.dbt_runner import main

    assert main() == 0


@patch("ingestion.dbt_runner._ensure_databricks_env_vars")
@patch("ingestion.dbt_runner.dbtRunner")
def test_main_raises_on_failure(mock_runner_cls: MagicMock, _mock_ensure: MagicMock) -> None:
    """main() must let RuntimeError propagate so Databricks marks the task failed.

    Returning a non-zero int from a python_wheel_task entry point is silently
    treated as SUCCESS by Databricks — only an uncaught exception fails the task.
    """
    mock_runner = MagicMock()
    mock_runner.invoke.return_value = _make_runner_result(success=False)
    mock_runner_cls.return_value = mock_runner

    from ingestion.dbt_runner import main

    with pytest.raises(RuntimeError, match="dbt build failed"):
        main()


@patch("ingestion.dbt_runner._ensure_databricks_env_vars")
@patch("ingestion.dbt_runner.dbtRunner")
def test_no_watermark_bypasses_guard_and_strips_flag(mock_runner_cls: MagicMock, _mock_ensure: MagicMock) -> None:
    """--no-watermark must bypass the watermark guard and NOT be forwarded to dbt."""
    mock_runner = MagicMock()
    mock_runner.invoke.return_value = _make_runner_result(success=True, node_count=5)
    mock_runner_cls.return_value = mock_runner

    from ingestion.dbt_runner import main

    with patch("sys.argv", ["dbt_build", "--select", "tag:output_mart", "--no-watermark"]):
        with patch("ingestion.dbt_runner.get_spark_session") as mock_spark:
            # Mock the watermark recording path (Spark is lazy-initialized for recording)
            mock_spark.return_value = MagicMock()
            with patch("ingestion.dbt_runner.resolve_upstream_tables_from_card", return_value=[]):
                with patch("ingestion.dbt_runner.record_watermarks"):
                    result = main()

    assert result == 0
    # dbt was actually invoked (not skipped by watermark guard)
    mock_runner.invoke.assert_called_once()
    # --no-watermark must NOT appear in the args forwarded to dbt
    dbt_args = mock_runner.invoke.call_args.args[0]
    assert "--no-watermark" not in dbt_args
    # --select tag:output_mart must still be forwarded
    assert "--select" in dbt_args
    assert "tag:output_mart" in dbt_args


@patch("ingestion.dbt_runner._ensure_databricks_env_vars")
@patch("ingestion.dbt_runner.dbtRunner")
def test_dbt_full_refresh_true_injects_flag(mock_runner_cls: MagicMock, _mock_ensure: MagicMock) -> None:
    """--dbt-full-refresh true must inject --full-refresh into dbt args."""
    mock_runner = MagicMock()
    mock_runner.invoke.return_value = _make_runner_result(success=True, node_count=5)
    mock_runner_cls.return_value = mock_runner

    from ingestion.dbt_runner import main

    argv = [
        "dbt_build", "--select", "+tag:input_mart", "+tag:dimension",
        "--dbt-full-refresh", "true",
    ]
    with patch("sys.argv", argv):
        with patch("ingestion.dbt_runner.get_spark_session") as mock_spark:
            mock_spark.return_value = MagicMock()
            with patch("ingestion.dbt_runner.timed_check") as mock_timed:
                mock_timed.return_value = MagicMock(count=1)
                with patch("ingestion.dbt_runner.resolve_upstream_tables_from_card", return_value=[]):
                    with patch("ingestion.dbt_runner.record_watermarks"):
                        result = main()

    assert result == 0
    dbt_args = mock_runner.invoke.call_args.args[0]
    # --full-refresh must be present in args forwarded to dbt
    assert "--full-refresh" in dbt_args
    # --dbt-full-refresh and its value must NOT be forwarded
    assert "--dbt-full-refresh" not in dbt_args
    assert "true" not in dbt_args
    # --select args must still be present
    assert "+tag:input_mart" in dbt_args


@patch("ingestion.dbt_runner._ensure_databricks_env_vars")
@patch("ingestion.dbt_runner.dbtRunner")
def test_dbt_full_refresh_false_does_not_inject_flag(mock_runner_cls: MagicMock, _mock_ensure: MagicMock) -> None:
    """--dbt-full-refresh false (the default) must NOT inject --full-refresh."""
    mock_runner = MagicMock()
    mock_runner.invoke.return_value = _make_runner_result(success=True, node_count=5)
    mock_runner_cls.return_value = mock_runner

    from ingestion.dbt_runner import main

    argv = [
        "dbt_build", "--select", "+tag:input_mart", "+tag:dimension",
        "--dbt-full-refresh", "false",
    ]
    with patch("sys.argv", argv):
        with patch("ingestion.dbt_runner.get_spark_session") as mock_spark:
            mock_spark.return_value = MagicMock()
            with patch("ingestion.dbt_runner.timed_check") as mock_timed:
                mock_timed.return_value = MagicMock(count=1)
                with patch("ingestion.dbt_runner.resolve_upstream_tables_from_card", return_value=[]):
                    with patch("ingestion.dbt_runner.record_watermarks"):
                        result = main()

    assert result == 0
    dbt_args = mock_runner.invoke.call_args.args[0]
    # --full-refresh must NOT be present
    assert "--full-refresh" not in dbt_args
    # --dbt-full-refresh and its value must NOT be forwarded
    assert "--dbt-full-refresh" not in dbt_args


@patch("ingestion.dbt_runner._ensure_databricks_env_vars")
@patch("ingestion.dbt_runner.dbtRunner")
def test_dbt_full_refresh_combined_with_no_watermark(mock_runner_cls: MagicMock, _mock_ensure: MagicMock) -> None:
    """--dbt-full-refresh true + --no-watermark must both work together."""
    mock_runner = MagicMock()
    mock_runner.invoke.return_value = _make_runner_result(success=True, node_count=5)
    mock_runner_cls.return_value = mock_runner

    from ingestion.dbt_runner import main

    with patch(
        "sys.argv",
        ["dbt_build", "--select", "tag:output_mart", "--no-watermark", "--dbt-full-refresh", "true"],
    ):
        with patch("ingestion.dbt_runner.get_spark_session") as mock_spark:
            mock_spark.return_value = MagicMock()
            with patch("ingestion.dbt_runner.resolve_upstream_tables_from_card", return_value=[]):
                with patch("ingestion.dbt_runner.record_watermarks"):
                    result = main()

    assert result == 0
    dbt_args = mock_runner.invoke.call_args.args[0]
    assert "--full-refresh" in dbt_args
    assert "--no-watermark" not in dbt_args
    assert "--dbt-full-refresh" not in dbt_args
    assert "tag:output_mart" in dbt_args
