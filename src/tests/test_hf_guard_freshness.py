"""Tests for HF Hub dataset freshness guard and SHA write-back.

Verifies:
- ``check_hf_dataset_freshness`` returns correct FilterResult based on
  stored vs. current HF Hub commit SHA.
- ``record_import_sha`` issues a MERGE INTO statement or no-ops on None.
- Each promoted guard calls ``check_hf_dataset_freshness`` with the
  correct HF repo constant.
"""

from __future__ import annotations

from unittest.mock import MagicMock, patch

from ingestion.guards import FilterResult

# ---------------------------------------------------------------------------
# Fake HfApi repo_info result
# ---------------------------------------------------------------------------


class _FakeRepoInfo:
    """Minimal stand-in for ``huggingface_hub.hf_api.RepoInfo``."""

    def __init__(self, sha: str) -> None:
        self.sha = sha


# ---------------------------------------------------------------------------
# check_hf_dataset_freshness tests
# ---------------------------------------------------------------------------


class TestCheckHfDatasetFreshness:
    """Unit tests for ``check_hf_dataset_freshness``."""

    @patch("huggingface_hub.HfApi")
    def test_returns_count_1_when_no_stored_sha(self, mock_hf_cls: MagicMock) -> None:
        """First run: no stored SHA means new work (count=1) with commit_sha in metadata."""
        mock_hf_cls.return_value.repo_info.return_value = _FakeRepoInfo(sha="abc123")

        spark = MagicMock()
        # ensure_table is a no-op mock
        # SELECT returns empty (no stored SHA)
        spark.sql.return_value.collect.return_value = []

        from ingestion.guards import check_hf_dataset_freshness

        result = check_hf_dataset_freshness(spark, "cat", "wf-test", "org/repo")

        assert isinstance(result, FilterResult)
        assert result.count == 1
        assert result.workflow_id == "wf-test"
        assert result.metadata["commit_sha"] == "abc123"

    @patch("huggingface_hub.HfApi")
    def test_returns_count_0_when_sha_matches(self, mock_hf_cls: MagicMock) -> None:
        """Stored SHA matches current HF Hub SHA: no new work (count=0)."""
        mock_hf_cls.return_value.repo_info.return_value = _FakeRepoInfo(sha="abc123")

        spark = MagicMock()
        # SELECT returns a row with matching SHA (dict — mirrors Spark Row __getitem__)
        spark.sql.return_value.collect.return_value = [{"last_imported_sha": "abc123"}]

        from ingestion.guards import check_hf_dataset_freshness

        result = check_hf_dataset_freshness(spark, "cat", "wf-test", "org/repo")

        assert result.count == 0
        assert result.workflow_id == "wf-test"

    @patch("huggingface_hub.HfApi")
    def test_returns_count_1_when_sha_differs(self, mock_hf_cls: MagicMock) -> None:
        """Stored SHA differs from current HF Hub SHA: new work (count=1) with new SHA."""
        mock_hf_cls.return_value.repo_info.return_value = _FakeRepoInfo(sha="new456")

        spark = MagicMock()
        spark.sql.return_value.collect.return_value = [{"last_imported_sha": "old123"}]

        from ingestion.guards import check_hf_dataset_freshness

        result = check_hf_dataset_freshness(spark, "cat", "wf-test", "org/repo")

        assert result.count == 1
        assert result.metadata["commit_sha"] == "new456"

    @patch("huggingface_hub.HfApi")
    def test_fail_open_on_hf_unreachable(self, mock_hf_cls: MagicMock) -> None:
        """HF Hub unreachable: fail open (count=1) so the pipeline still runs."""
        mock_hf_cls.return_value.repo_info.side_effect = ConnectionError("HF Hub unreachable")

        spark = MagicMock()

        from ingestion.guards import check_hf_dataset_freshness

        result = check_hf_dataset_freshness(spark, "cat", "wf-test", "org/repo")

        assert result.count == 1
        assert result.workflow_id == "wf-test"
        # No commit_sha in metadata when HF Hub is unreachable
        assert "commit_sha" not in result.metadata


# ---------------------------------------------------------------------------
# record_import_sha tests
# ---------------------------------------------------------------------------


class TestRecordImportSha:
    """Unit tests for ``record_import_sha``."""

    def test_issues_merge_statement(self) -> None:
        """Calling with a commit_sha issues a MERGE INTO SQL statement."""
        spark = MagicMock()

        from ingestion.guards import record_import_sha

        record_import_sha(spark, "cat", "wf-test", "org/repo", "sha123")

        # Should have called spark.sql at least once for the MERGE
        merge_calls = [c for c in spark.sql.call_args_list if "MERGE INTO" in str(c)]
        assert len(merge_calls) >= 1, f"Expected a MERGE INTO call, got: {spark.sql.call_args_list}"

    def test_noop_on_none_sha(self) -> None:
        """Calling with commit_sha=None is a no-op (no SQL issued)."""
        spark = MagicMock()

        from ingestion.guards import record_import_sha

        record_import_sha(spark, "cat", "wf-test", "org/repo", None)

        spark.sql.assert_not_called()


# ---------------------------------------------------------------------------
# Guard repo constant tests — verify each promoted guard calls
# check_hf_dataset_freshness with the correct HF repo.
# ---------------------------------------------------------------------------


class TestPromotedGuardRepoConstants:
    """Each promoted guard must call ``check_hf_dataset_freshness`` with the correct HF repo."""

    @patch("ingestion.import_obso_results.check_hf_dataset_freshness")
    def test_import_obso_guard(self, mock_check: MagicMock) -> None:
        """import_obso_results.skip_guard calls check with obso-pausa-values repo."""
        mock_check.return_value = FilterResult(workflow_id="wf-import-obso", count=1)
        spark = MagicMock()

        from ingestion.import_obso_results import skip_guard

        skip_guard.check(spark, "cat", "schema")

        mock_check.assert_called_once_with(spark, "cat", "wf-import-obso", "luxury-lakehouse/obso-pausa-values")

    @patch("ingestion.import_psxg_predictions.check_hf_dataset_freshness")
    def test_import_psxg_guard(self, mock_check: MagicMock) -> None:
        """import_psxg_predictions.skip_guard calls check with psxg-predictions repo."""
        mock_check.return_value = FilterResult(workflow_id="wf-import-psxg", count=1)
        spark = MagicMock()

        from ingestion.import_psxg_predictions import skip_guard

        skip_guard.check(spark, "cat", "schema")

        mock_check.assert_called_once_with(spark, "cat", "wf-import-psxg", "luxury-lakehouse/psxg-predictions")

    @patch("ingestion.import_space_creation.check_hf_dataset_freshness")
    def test_import_space_creation_guard(self, mock_check: MagicMock) -> None:
        """import_space_creation.skip_guard calls check with space-creation-values repo."""
        mock_check.return_value = FilterResult(workflow_id="wf-import-space-creation", count=1)
        spark = MagicMock()

        from ingestion.import_space_creation import skip_guard

        skip_guard.check(spark, "cat", "schema")

        mock_check.assert_called_once_with(
            spark, "cat", "wf-import-space-creation", "luxury-lakehouse/space-creation-values"
        )

    @patch("ingestion.player_embeddings_v2.check_hf_dataset_freshness")
    def test_football2vec_v2_guard(self, mock_check: MagicMock) -> None:
        """player_embeddings_v2.skip_guard calls check with v2 dataset repo."""
        mock_check.return_value = FilterResult(workflow_id="wf-football2vec-v2", count=1)
        spark = MagicMock()

        from ingestion.player_embeddings_v2 import skip_guard

        skip_guard.check(spark, "cat", "schema")

        mock_check.assert_called_once_with(
            spark, "cat", "wf-football2vec-v2", "luxury-lakehouse/football2vec-statsbomb-wyscout"
        )

    @patch("ingestion.player_embeddings_v2.check_hf_dataset_freshness")
    def test_football2vec_360_guard(self, mock_check: MagicMock) -> None:
        """player_embeddings_v2._football2vec_360_guard calls check with 360 dataset repo."""
        mock_check.return_value = FilterResult(workflow_id="wf-football2vec-360", count=1)
        spark = MagicMock()

        from ingestion.player_embeddings_v2 import _football2vec_360_guard

        _football2vec_360_guard.check(spark, "cat", "schema")

        mock_check.assert_called_once_with(
            spark,
            "cat",
            "wf-football2vec-360",
            "luxury-lakehouse/football2vec-360-embeddings",  # pragma: allowlist secret
        )
