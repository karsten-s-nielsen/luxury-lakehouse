"""Input-dataset upstream sentinel — catches trainer regression to HF dataset consumption.

Verifies that gamma trainers (f2v_v2, f2v_360, scoutgpt) reference
``load_training_data_sql`` or ``query_databricks_sql`` (gold-SQL path),
and that evolve consumers reference the pinned HF dataset repo string.
"""

from __future__ import annotations

from pathlib import Path

import pytest

_REPO_ROOT = Path(__file__).resolve().parents[2]

# Trainer -> (expected source pattern, file path)
# "gold_sql" trainers must reference load_training_data_sql or query_databricks_sql
# "hf_dataset" trainers must reference their dataset repo string
TRAINER_INPUT_DATASETS: list[tuple[str, str, Path]] = [
    # gamma trainers — gold SQL path
    ("train_football2vec_v2", "gold_sql", _REPO_ROOT / "scripts" / "train_football2vec_v2.py"),
    ("train_football2vec_360", "gold_sql", _REPO_ROOT / "scripts" / "train_football2vec_360.py"),
    ("train_scoutgpt_hf", "gold_sql", _REPO_ROOT / "scripts" / "train_scoutgpt_hf.py"),
    # wheel training modules — gold SQL path
    ("football2vec_v2_training", "gold_sql", _REPO_ROOT / "src" / "ingestion" / "football2vec_v2_training.py"),
    ("scoutgpt_training", "gold_sql", _REPO_ROOT / "src" / "analytics" / "scoutgpt_training.py"),
]


@pytest.mark.parametrize(
    "name,source_type,path",
    TRAINER_INPUT_DATASETS,
    ids=[t[0] for t in TRAINER_INPUT_DATASETS],
)
def test_trainer_uses_expected_upstream(name: str, source_type: str, path: Path) -> None:
    content = path.read_text(encoding="utf-8")

    if source_type == "gold_sql":
        assert "load_training_data_sql" in content or "query_databricks_sql" in content, (
            f"{name} ({path.name}) does not reference load_training_data_sql or "
            f"query_databricks_sql — may have regressed to HF dataset consumption"
        )
    elif source_type == "hf_dataset":
        assert "luxury-lakehouse/" in content, (
            f"{name} ({path.name}) does not reference a luxury-lakehouse/ HF dataset repo"
        )
