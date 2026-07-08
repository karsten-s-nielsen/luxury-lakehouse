"""Unit tests for scripts.publish_xg_shot_data_v3_hf — SQL/column/contract regression guards.

Pre-Shot xG v3 delivery, Task 1.2 (spec §A3). This publisher sources shot rows from
gold ``fct_action_values`` and publishes them to the ``xg-shot-data-v3`` HF dataset
(public) + its ``-restricted`` private companion, following the ADR-049/064 access_tier
split and the ADR-054 flat per-provider layout.

The generic ADR-049 split mechanics (imports of the shared helpers, no-provider-filter,
split-on-access_tier + leak-guard, delete_patterns=['**'], restricted-card-exists,
uploads-restricted-card) are enforced across ALL split publishers by
``test_hf_publish_parity.py`` / ``test_gradientsports_hf_exclusion.py`` once this publisher
is registered. This module pins the SQL/column/contract specifics unique to xg-shot-data-v3.
"""

from __future__ import annotations

import ast
from pathlib import Path

_SCRIPT = Path(__file__).resolve().parent.parent.parent / "scripts" / "publish_xg_shot_data_v3_hf.py"

# The authoritative published contract (spec §7, after access_tier is dropped).
_CONTRACT_COLUMNS: frozenset[str] = frozenset(
    {
        "match_key",
        "action_id",
        "action_type",
        "action_result",
        "start_x",
        "start_y",
        "data_source",
    }
)

# The columns the SQL must SELECT (contract columns + access_tier for the split).
_SQL_SELECT_COLUMNS: frozenset[str] = _CONTRACT_COLUMNS | {"access_tier"}


def _source() -> str:
    return _SCRIPT.read_text(encoding="utf-8")


class TestSourceSql:
    """Regression guards on the extraction SQL (the test is the source of truth for columns)."""

    def test_script_exists(self) -> None:
        assert _SCRIPT.is_file(), f"expected publisher script at {_SCRIPT}"

    def test_sql_selects_exactly_the_contract_columns_plus_access_tier(self) -> None:
        content = _source().lower()
        for col in _SQL_SELECT_COLUMNS:
            assert col in content, f"SQL must SELECT {col!r} (contract column / split key)"

    def test_sql_reads_fct_action_values(self) -> None:
        content = _source().lower()
        assert "soccer_analytics.dev_gold.fct_action_values" in content, (
            "SQL must read from soccer_analytics.dev_gold.fct_action_values (spec §A3)."
        )

    def test_sql_filters_the_shot_family(self) -> None:
        content = _source().lower()
        # action_type IN (...) restricting to the shot family — penalties INCLUDED so the
        # downstream scorer's penalty path has rows (the trainer filters them out separately).
        assert "action_type in" in content, "SQL must filter action_type via an IN (...) clause."
        for shot_type in ("'shot'", "'shot_freekick'", "'shot_penalty'"):
            assert shot_type in content, f"action_type IN filter must include {shot_type}."

    def test_sql_has_no_data_source_filter(self) -> None:
        # The license gate lives ONLY at split_restricted (ADR-049); a SQL-side provider
        # filter silently shrinks the restricted repo (and any training corpus).
        content = _source().lower()
        assert "data_source !=" not in content and "data_source <>" not in content, (
            "publish_xg_shot_data_v3_hf.py must NOT filter providers in SQL — the license gate is "
            "split_restricted(column='access_tier'), never a WHERE data_source clause (ADR-049)."
        )
        assert "data_source in" not in content, (
            "SQL must not restrict WHO is published by data_source — that gate is the access_tier split."
        )


class TestSplitAndPublishFlow:
    """The ADR-049/064 split + ADR-054 flat-layout wiring specific to this publisher."""

    def test_splits_on_access_tier(self) -> None:
        assert 'column="access_tier"' in _source(), (
            'publisher must call split_restricted(df, column="access_tier") (per-match boundary, spec §6.5).'
        )

    def test_calls_leak_guard(self) -> None:
        assert "assert_no_private_leak(" in _source(), (
            "publisher must call assert_no_private_leak(public_df, publisher=...) on the PUBLIC frame before upload."
        )

    def test_drops_access_tier_before_upload(self) -> None:
        content = _source()
        assert 'drop(columns=["access_tier"]' in content, (
            "publisher must drop access_tier from BOTH frames after split+guard, before upload (spec R2)."
        )

    def test_delete_patterns_sweep_whole_path_in_repo(self) -> None:
        assert 'delete_patterns=["**"]' in _source(), (
            "upload_folder delete_patterns must be ['**'] — patterns match RELATIVE to path_in_repo (ADR-049)."
        )

    def test_uploads_readme_with_config_providers_for_both_repos(self) -> None:
        content = _source()
        assert "config_providers=" in content, (
            "publisher must pass config_providers= to upload_hf_readme for the ADR-054 dynamic configs injection."
        )
        # Both cards ride with the publish (ADR-014): public + restricted card filenames referenced.
        assert "xg-shot-data-v3.md" in content, "publisher must upload the public card xg-shot-data-v3.md."
        assert "xg-shot-data-v3-restricted.md" in content, (
            "publisher must upload the restricted card xg-shot-data-v3-restricted.md."
        )


class TestRepoIdentity:
    """The dataset repo naming (public + restricted companion derived via the shared helper)."""

    def test_dataset_repo_is_xg_shot_data_v3(self) -> None:
        module = _import_publisher_module()
        assert module.DATASET_REPO.endswith("/xg-shot-data-v3"), (
            f"DATASET_REPO must end with /xg-shot-data-v3, got {module.DATASET_REPO!r}."
        )
        assert module.HF_ORG == "luxury-lakehouse"

    def test_restricted_repo_derives_via_shared_helper(self) -> None:
        # The restricted companion must derive from restricted_repo_id, not a hand-written string.
        content = _source()
        assert "restricted_repo_id(DATASET_REPO)" in content, (
            "RESTRICTED_DATASET_REPO must derive via restricted_repo_id(DATASET_REPO) (ADR-049 single source of truth)."
        )
        module = _import_publisher_module()
        assert module.RESTRICTED_DATASET_REPO == f"{module.DATASET_REPO}-restricted"


class TestDtypeNormalization:
    """The dtype contract (spec §7): int64 keys, float64 coords, str categoricals, nullable access_tier."""

    def test_normalize_dtypes_enforces_the_contract(self) -> None:
        import pandas as pd

        module = _import_publisher_module()
        raw = pd.DataFrame(
            {
                "match_key": ["1001", "1002", "1003"],
                "action_id": ["7", "8", "9"],
                "action_type": ["shot", "shot_freekick", "shot_penalty"],
                "action_result": ["success", "fail", "success"],
                "start_x": ["100.5", "88.0", "94.0"],
                "start_y": ["34.0", "20.0", "34.0"],
                "data_source": ["statsbomb", "skillcorner", "gradientsports"],
                # Include a NULL access_tier — must survive as <NA>, NOT become the string "nan".
                "access_tier": ["public", None, "restricted"],
            }
        )
        out = module.normalize_dtypes(raw.copy())

        assert str(out["match_key"].dtype) == "Int64"
        assert str(out["action_id"].dtype) == "Int64"
        assert str(out["start_x"].dtype) == "float64"
        assert str(out["start_y"].dtype) == "float64"
        for col in ("action_type", "action_result", "data_source"):
            assert out[col].dtype == object, f"{col} must be str/object"
            assert isinstance(out[col].iloc[0], str)

        # access_tier is nullable "string" — a NULL must remain <NA> so the fail-safe split works.
        assert str(out["access_tier"].dtype) == "string"
        assert pd.isna(out["access_tier"].iloc[1]), "NULL access_tier must survive as <NA>, not the literal 'nan'."
        assert out["access_tier"].iloc[0] == "public"


def _import_publisher_module():
    """Import the PEP-723 publisher module by path (it lives in scripts/, not on the package path)."""
    import importlib.util

    spec = importlib.util.spec_from_file_location("publish_xg_shot_data_v3_hf", _SCRIPT)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_contract_columns_are_documented_in_the_public_card() -> None:
    """The public dataset card documents the published contract columns (spec §7)."""
    card = _SCRIPT.parent.parent / "docs" / "huggingface" / "dataset-cards" / "xg-shot-data-v3.md"
    assert card.is_file(), f"public dataset card missing at {card}"
    text = card.read_text(encoding="utf-8").lower()
    for col in _CONTRACT_COLUMNS:
        assert col in text, f"public card must document contract column {col!r}."


def test_module_parses_and_has_no_syntax_errors() -> None:
    ast.parse(_source())
