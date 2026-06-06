"""Regression tests for scripts/train_vaep_model_hf.py.

Locks the 2026-06-06 fix: the stratified train/test split must use the Kimball
surrogate ``competition_key`` (non-NULL for ALL providers), NOT the legacy
numeric ``competition_id`` (NULL for non-numeric provider IDs — idsse / metrica
/ skillcorner). The legacy column put ``pd.NA`` into the stratify array and
crashed ``train_test_split`` with "boolean value of NA is ambiguous", which
blocked the silly-kicks 4.13 own-goal VAEP retrain.

Source-level inspection (no execution): the script is a PEP 723 single-file
whose runtime deps (silly-kicks, xgboost, mlflow) are not installed in the
unit-test env.
"""

from __future__ import annotations

from pathlib import Path

_TRAINER = Path(__file__).resolve().parents[2] / "scripts" / "train_vaep_model_hf.py"


def test_stratifies_on_competition_key() -> None:
    src = _TRAINER.read_text(encoding="utf-8")
    assert "competition_key" in src, (
        "VAEP trainer must stratify the train/test split on the Kimball surrogate "
        "competition_key (ADR-011) — it is non-NULL for every provider, unlike the "
        "legacy competition_id."
    )


def test_no_legacy_competition_id_stratify_mapping() -> None:
    """The buggy pattern — building the stratify mapping straight off the legacy
    competition_id — must not reappear (it crashes on idsse/metrica NULLs)."""
    src = _TRAINER.read_text(encoding="utf-8")
    assert '["game_id", "competition_id"]' not in src, (
        "VAEP trainer builds its stratify mapping from the legacy competition_id, "
        "which is NULL for idsse/metrica/skillcorner and crashes train_test_split. "
        "Use competition_key (ADR-011)."
    )
