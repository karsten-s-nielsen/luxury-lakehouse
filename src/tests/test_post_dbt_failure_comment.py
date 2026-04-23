"""Unit tests for scripts.post_dbt_failure_comment (PR 4a)."""

from __future__ import annotations

import json
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from scripts import post_dbt_failure_comment as post

_FIXTURE_DIR = Path(__file__).parent / "fixtures"


class TestParseRunResults:
    def test_extracts_failing_models_and_tests(self) -> None:
        data = json.loads((_FIXTURE_DIR / "run_results_mixed.json").read_text())
        failures = post.parse_failures(data)
        assert len(failures) == 2
        names = {f.unique_id for f in failures}
        assert "model.luxury_lakehouse.fct_action_values" in names
        assert "test.luxury_lakehouse.not_null_fct_action_values_match_key.abcd1234" in names

    def test_all_pass_returns_empty(self) -> None:
        data = json.loads((_FIXTURE_DIR / "run_results_all_pass.json").read_text())
        assert post.parse_failures(data) == []

    def test_truncates_long_error_message(self) -> None:
        data = {
            "results": [
                {
                    "status": "error",
                    "unique_id": "m.x",
                    "message": "\n".join(f"line {i}" for i in range(50)),
                }
            ]
        }
        failures = post.parse_failures(data)
        assert failures[0].error_excerpt.count("\n") <= 14  # 15 lines max
        assert "... (truncated" in failures[0].error_excerpt


class TestFormatComment:
    def test_happy_path_formatting(self) -> None:
        failures = [
            post.Failure(
                unique_id="model.luxury_lakehouse.fct_foo",
                status="error",
                error_excerpt="TABLE_OR_VIEW_NOT_FOUND\n  line 2",
                failures_count=None,
            ),
            post.Failure(
                unique_id="test.luxury_lakehouse.not_null_bar.123",
                status="fail",
                error_excerpt="Got 15 results",
                failures_count=15,
            ),
        ]
        comment = post.format_comment(
            failures=failures,
            run_page_url="https://workspace.databricks.com/#job/run/1",
        )
        assert "❌ dbt-live-ci failed" in comment
        assert "fct_foo" in comment
        assert "not_null_bar" in comment
        assert "15 failing rows" in comment
        assert "https://workspace.databricks.com/#job/run/1" in comment


class TestFetchRunResultsFromVolume:
    @patch("scripts.post_dbt_failure_comment._workspace_client")
    def test_fetches_and_parses(self, mock_ws: MagicMock) -> None:
        mock_files = MagicMock()
        mock_ws.return_value.files = mock_files
        mock_files.download.return_value = MagicMock(
            contents=b'{"results": [{"status": "success", "unique_id": "m.x", "message": "OK"}]}'
        )
        data = post.fetch_run_results("/Volumes/a/b/c/rr.json")
        assert "results" in data


class TestPostComment:
    @patch("scripts.post_dbt_failure_comment.requests.post")
    def test_posts_with_github_token(self, mock_post: MagicMock) -> None:
        mock_post.return_value.status_code = 201
        mock_post.return_value.raise_for_status = MagicMock()
        post.post_comment_to_pr(
            repo="owner/repo",
            pr_number=42,
            comment_body="body",
            github_token="gh_tok",  # noqa: S106 — test fixture, not a real token
        )
        mock_post.assert_called_once()
        args, kwargs = mock_post.call_args
        assert "repos/owner/repo/issues/42/comments" in args[0]
        assert kwargs["headers"]["Authorization"] == "token gh_tok"

    @patch("scripts.post_dbt_failure_comment.requests.post")
    def test_fork_pr_scope_failure_skips_silently(
        self,
        mock_post: MagicMock,
        caplog: pytest.LogCaptureFixture,
    ) -> None:
        mock_post.return_value.status_code = 403
        mock_post.return_value.text = "Resource not accessible by integration"

        import requests as req

        err = req.HTTPError("403")
        err.response = mock_post.return_value
        mock_post.return_value.raise_for_status = MagicMock(side_effect=err)

        # Should NOT raise — fork-PR scope limitation is soft.
        with caplog.at_level("WARNING"):
            post.post_comment_to_pr(
                repo="owner/repo",
                pr_number=42,
                comment_body="b",
                github_token="gh",  # noqa: S106 — test fixture, not a real token
            )
        assert any("fork" in rec.getMessage().lower() or "403" in rec.getMessage() for rec in caplog.records)


class TestMainCLI:
    @patch("scripts.post_dbt_failure_comment.post_comment_to_pr")
    @patch("scripts.post_dbt_failure_comment.fetch_run_results")
    def test_main_returns_zero_after_post(
        self,
        mock_fetch: MagicMock,
        mock_post_fn: MagicMock,
        tmp_path: Path,
    ) -> None:
        mock_fetch.return_value = json.loads((_FIXTURE_DIR / "run_results_mixed.json").read_text())
        rc = post.main(
            [
                "--repo",
                "owner/repo",
                "--pr-number",
                "42",
                "--run-page-url",
                "https://x",
                "--run-output-volume-path",
                "/Volumes/a/b/c/rr.json",
                "--github-token",
                "gh",
            ]
        )
        assert rc == 0
        mock_post_fn.assert_called_once()

    @patch("scripts.post_dbt_failure_comment.post_comment_to_pr")
    @patch("scripts.post_dbt_failure_comment.fetch_run_results")
    def test_main_skips_comment_on_all_pass(
        self,
        mock_fetch: MagicMock,
        mock_post_fn: MagicMock,
    ) -> None:
        mock_fetch.return_value = json.loads((_FIXTURE_DIR / "run_results_all_pass.json").read_text())
        rc = post.main(
            [
                "--repo",
                "owner/repo",
                "--pr-number",
                "42",
                "--run-page-url",
                "https://x",
                "--run-output-volume-path",
                "/Volumes/a/b/c/rr.json",
                "--github-token",
                "gh",
            ]
        )
        assert rc == 0
        mock_post_fn.assert_not_called()
