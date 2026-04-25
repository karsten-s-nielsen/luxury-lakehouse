"""Unit test for Metrica ingestion's is_anonymized flag contract (PR 5a)."""

from __future__ import annotations

import inspect

from ingestion import metrica, metrica_tracking


def test_ingestion_module_docstring_documents_is_anonymized() -> None:
    doc = inspect.getdoc(metrica)
    assert doc is not None
    assert "is_anonymized" in doc, "module docstring must document the is_anonymized contract"
    lowered = doc.lower()
    assert "sample" in lowered and "subscription" in lowered, (
        "docstring must distinguish sample vs subscription ingestion paths"
    )


def test_tracking_bronze_cols_include_is_anonymized() -> None:
    assert "is_anonymized" in metrica_tracking._METRICA_TRACKING_BRONZE_COLS


def test_tracking_dtype_overrides_declare_is_anonymized_boolean() -> None:
    assert metrica_tracking._METRICA_TRACKING_DTYPE_OVERRIDES.get("is_anonymized") == "boolean"


def test_tracking_ingestion_source_sets_flag_true() -> None:
    """Sample-path ingestion must assign is_anonymized = True pre-finalize."""
    src = inspect.getsource(metrica_tracking)
    assert 'tracking_df["is_anonymized"] = True' in src or "tracking_df['is_anonymized'] = True" in src, (
        "Sample path must literal-assign True to is_anonymized before finalize_bronze_df"
    )
