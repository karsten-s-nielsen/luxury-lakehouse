"""Unit tests for ingestion.player_embeddings_v2 360 import path (D62).

The 360 path is structurally near-identical to the v2 path but:
- Uses a different HF Hub dataset (luxury-lakehouse/football2vec-360-embeddings)
- Produces 144-dim behavioral vectors (v2 is 128-dim)
- Labels rows with data_source='football2vec_360' so downstream dbt models
  can isolate them from v2's statsbomb/wyscout partitions
"""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pandas as pd
import pytest


def test_hf_360_dataset_constant_exists() -> None:
    """The module must export a _HF_360_DATASET constant."""
    from ingestion import player_embeddings_v2 as mod

    assert hasattr(mod, "_HF_360_DATASET")
    assert mod._HF_360_DATASET == "luxury-lakehouse/football2vec-360-embeddings"  # pragma: allowlist secret


def test_import_embeddings_360_function_exists() -> None:
    """The module must expose _import_embeddings_360, run_pipeline_360, main_360."""
    from ingestion import player_embeddings_v2 as mod

    assert callable(getattr(mod, "_import_embeddings_360", None))
    assert callable(getattr(mod, "run_pipeline_360", None))
    assert callable(getattr(mod, "main_360", None))


def test_run_pipeline_360_writes_football2vec_360_data_source() -> None:
    """run_pipeline_360 must label all rows with data_source='football2vec_360'
    regardless of what the source parquet contains."""
    from ingestion import player_embeddings_v2 as mod

    fake_parquet = pd.DataFrame(
        {
            "canonical_player_id": ["p1", "p2", "p3"],
            "match_id": ["m1", "m2", "m3"],
            "behavioral_vector": [
                [0.1] * 144,
                [0.2] * 144,
                [0.3] * 144,
            ],
        }
    )

    captured_writes: list[dict[str, object]] = []

    def fake_write(*args: object, **kwargs: object) -> None:
        captured_writes.append({"args": args, "kwargs": kwargs})

    spark = MagicMock()
    spark.createDataFrame = MagicMock(return_value=MagicMock())
    logger = MagicMock()

    with (
        patch("ingestion.player_embeddings_v2.hf_hub_download", return_value="/nonexistent/path/fake.parquet"),
        patch("ingestion.player_embeddings_v2.repo_exists", return_value=True),
        patch("ingestion.player_embeddings_v2.pd.read_parquet", return_value=fake_parquet),
        patch("ingestion.player_embeddings_v2.write_delta_table", side_effect=fake_write),
        patch("ingestion.player_embeddings_v2.validate_dataframe", return_value=3),
        patch("ingestion.player_embeddings_v2._compute_stat_vectors", return_value=(pd.DataFrame(), {})),
        patch("ingestion.player_embeddings_v2._merge_vectors", return_value={}),
        patch("ingestion.player_embeddings_v2._save_norm_params"),
    ):
        result = mod._import_embeddings_360(spark, "soccer_analytics", "bronze", logger)

    assert result is True
    # Must have called write_delta_table exactly once with
    # replace_where="data_source = 'football2vec_360'"
    assert len(captured_writes) == 1, (
        f"Expected one write_delta_table call for the 360 partition, got {len(captured_writes)}"
    )
    kwargs = captured_writes[0]["kwargs"]
    assert isinstance(kwargs, dict)
    rw = kwargs["replace_where"]
    assert rw == "data_source = 'football2vec_360'", f"replace_where must isolate the 360 partition, got: {rw}"


def test_run_pipeline_360_rejects_wrong_dimension() -> None:
    """If the HF parquet has vectors with length != 144, the import must raise
    (not silently pass through the wrong dimension)."""
    from ingestion import player_embeddings_v2 as mod

    fake_parquet = pd.DataFrame(
        {
            "canonical_player_id": ["p1"],
            "match_id": ["m1"],
            "behavioral_vector": [[0.1] * 128],  # WRONG: 128 instead of 144
        }
    )

    spark = MagicMock()
    logger = MagicMock()

    with (
        patch("ingestion.player_embeddings_v2.hf_hub_download", return_value="/nonexistent/path/fake.parquet"),
        patch("ingestion.player_embeddings_v2.repo_exists", return_value=True),
        patch("ingestion.player_embeddings_v2.pd.read_parquet", return_value=fake_parquet),
    ):
        with pytest.raises((ValueError, RuntimeError), match="144"):
            mod._import_embeddings_360(spark, "soccer_analytics", "bronze", logger)
