"""Tests for HF Bucket provisioning script."""

from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock


class TestCreateDemoBucket:
    """Test bucket creation logic."""

    def test_creates_bucket_when_not_exists(self) -> None:
        from scripts.setup_hf_buckets import create_demo_bucket

        api = MagicMock()
        create_demo_bucket(api)
        api.create_bucket.assert_called_once_with(
            "luxury-lakehouse/demo-data",
            private=False,
            exist_ok=True,
        )

    def test_exist_ok_prevents_error_on_duplicate(self) -> None:
        from scripts.setup_hf_buckets import create_demo_bucket

        api = MagicMock()
        create_demo_bucket(api)
        _, kwargs = api.create_bucket.call_args
        assert kwargs.get("exist_ok") is True or api.create_bucket.call_args[1].get("exist_ok") is True


class TestUploadDemoData:
    """Test parquet upload logic."""

    def test_uploads_all_six_parquet_files(self, tmp_path: Path) -> None:
        from scripts.setup_hf_buckets import DEMO_FILES, upload_demo_data

        data_dir = tmp_path / "data"
        data_dir.mkdir()
        for name in DEMO_FILES:
            (data_dir / name).write_bytes(b"fake-parquet")

        api = MagicMock()
        upload_demo_data(api, data_dir)

        api.batch_bucket_files.assert_called_once()
        call_args = api.batch_bucket_files.call_args
        assert call_args[1]["bucket_id"] == "luxury-lakehouse/demo-data"
        add_list = call_args[1]["add"]
        assert len(add_list) == 6
        remote_paths = {item[1] for item in add_list}
        assert remote_paths == set(DEMO_FILES)

    def test_skips_missing_files(self, tmp_path: Path) -> None:
        from scripts.setup_hf_buckets import upload_demo_data

        data_dir = tmp_path / "data"
        data_dir.mkdir()

        api = MagicMock()
        upload_demo_data(api, data_dir)

        api.batch_bucket_files.assert_not_called()
