"""SEC2: SHA-256 verification helper for model artifacts loaded from MLflow / UC Volume.

Defense-in-depth: closes SEC-AUDIT-v1.12.0 ML-02 (CWE-345).
"""

from __future__ import annotations

import hashlib
import logging

import pytest

from ingestion.utils import ArtifactHashMismatchError, verify_artifact_hash


def _sha256(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


class TestVerifyArtifactHash:
    def test_passes_with_correct_sha256(self) -> None:
        data = b"hello world"
        expected = _sha256(data)  # b94d27b9934d3e08a52e52d7da7dabfac484efe37a5380ee9088f7ace2efcde9
        verify_artifact_hash(
            data=data,
            expected_sha256=expected,
            artifact_label="test_artifact",
            logger=logging.getLogger("test"),
        )
        # No exception raised — pass

    def test_raises_on_mismatch(self) -> None:
        data = b"hello world"
        wrong = "0" * 64
        with pytest.raises(ArtifactHashMismatchError) as exc_info:
            verify_artifact_hash(
                data=data,
                expected_sha256=wrong,
                artifact_label="test_artifact",
                logger=logging.getLogger("test"),
            )
        msg = str(exc_info.value)
        assert "test_artifact" in msg, "Error must include the artifact label for diagnosis"
        assert wrong in msg, "Error must include the expected hash"
        actual = _sha256(data)
        assert actual in msg, "Error must include the actual hash so the user can inspect"

    def test_warns_on_missing_hash(self, caplog: pytest.LogCaptureFixture) -> None:
        """When expected_sha256 is None, helper logs a WARNING and returns without raising.

        This is the fail-open path that lets the cycle ship without a complete bootstrap
        of historical hashes — verification activates lazily as hashes get recorded.
        """
        with caplog.at_level(logging.WARNING):
            verify_artifact_hash(
                data=b"any bytes",
                expected_sha256=None,
                artifact_label="unrecorded_artifact",
                logger=logging.getLogger("test"),
            )
        warnings = [r for r in caplog.records if r.levelname == "WARNING"]
        assert any("unrecorded_artifact" in r.message for r in warnings), (
            "Helper must log a WARNING mentioning the artifact label when no hash is recorded."
        )

    def test_rejects_malformed_hash_too_short(self) -> None:
        with pytest.raises(ValueError, match="64 hex"):
            verify_artifact_hash(
                data=b"x",
                expected_sha256="abc",
                artifact_label="test",
                logger=logging.getLogger("test"),
            )

    def test_rejects_malformed_hash_invalid_chars(self) -> None:
        with pytest.raises(ValueError, match="64 hex"):
            verify_artifact_hash(
                data=b"x",
                expected_sha256="g" * 64,  # 'g' not valid hex
                artifact_label="test",
                logger=logging.getLogger("test"),
            )

    def test_handles_empty_bytes(self) -> None:
        empty_sha = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"  # pragma: allowlist secret
        verify_artifact_hash(
            data=b"",
            expected_sha256=empty_sha,
            artifact_label="empty",
            logger=logging.getLogger("test"),
        )

    def test_case_insensitive_hash_comparison(self) -> None:
        """SHA-256 hex strings are case-insensitive — both should pass."""
        data = b"hello world"
        upper = _sha256(data).upper()
        verify_artifact_hash(
            data=data,
            expected_sha256=upper,
            artifact_label="test",
            logger=logging.getLogger("test"),
        )
