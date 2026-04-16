"""Unit tests for ingestion.football2vec_v2_training module.

TDD guard: this module was migrated from scripts/train_football2vec_v2_helpers.py
into the wheel package so HF Jobs scripts and src/tests/test_benchmarks.py can
import it without sys.path manipulation. This test verifies the public API is
stable at the new location.
"""

from __future__ import annotations

import pytest


def test_module_imports_from_wheel_location() -> None:
    """The training-helpers module must live at ingestion.football2vec_v2_training."""
    import ingestion.football2vec_v2_training as mod

    # Constants
    assert mod.VOCAB_SIZE == 23
    assert mod.MASK_TOKEN_ID == 23
    assert mod.PAD_TOKEN_ID == 24
    assert mod.MAX_SEQ_LEN == 512
    assert mod.WEIGHT_DECAY == 0.01
    assert mod.WARMUP_FRACTION == 0.10
    assert mod.RANDOM_STATE == 42
    assert mod.ADVERSARIAL_LAMBDA_MAX == 0.2
    assert mod.ADVERSARIAL_WARMUP_EPOCHS == 5
    assert mod.DEFAULT_MASK_PROB == 0.15


def test_football2vec_dataset_public_name() -> None:
    """Football2VecDataset must be importable at the new path."""
    pytest.importorskip("torch")
    from ingestion.football2vec_v2_training import Football2VecDataset

    ds = Football2VecDataset(
        action_ids=[[1, 2, 3]],
        x_coords=[[10.0, 20.0, 30.0]],
        y_coords=[[5.0, 15.0, 25.0]],
        max_seq_len=10,
        mlm=False,
    )
    assert len(ds) == 1
    item = ds[0]
    assert "action_ids" in item
    assert "x_coords" in item
    assert "y_coords" in item
    assert "attention_mask" in item


def test_public_helper_functions_import() -> None:
    """The public helpers must be importable for the HF Jobs training script."""
    pytest.importorskip("torch")
    pytest.importorskip("sklearn")
    from ingestion.football2vec_v2_training import (
        get_cosine_schedule_with_warmup,
        load_training_data,
        parse_actions,
        stratified_split,
    )

    assert callable(load_training_data)
    assert callable(parse_actions)
    assert callable(stratified_split)
    assert callable(get_cosine_schedule_with_warmup)


def test_old_script_path_no_longer_imported_by_benchmark_fixture() -> None:
    """Regression guard: test_benchmarks.py must not use sys.path hacks for v2 helpers."""
    from pathlib import Path

    repo_root = Path(__file__).resolve().parents[2]
    bench_file = repo_root / "src" / "tests" / "test_benchmarks.py"
    text = bench_file.read_text(encoding="utf-8")

    # The old helpers module must not be referenced anywhere in the file.
    assert "train_football2vec_v2_helpers" not in text, (
        "test_benchmarks.py should import Football2VecDataset from "
        "ingestion.football2vec_v2_training, not via the old helpers script."
    )
    # Positive assertion: the correct import is present
    assert "from ingestion.football2vec_v2_training import" in text, (
        "test_benchmarks.py should import from ingestion.football2vec_v2_training"
    )
