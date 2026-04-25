"""Unit tests for generate_entity_xref.py fuzzy-match + ordering (PR 5a)."""

from __future__ import annotations

import importlib.util
import pathlib

import pytest

SCRIPT = pathlib.Path("scripts/generate_entity_xref.py")


def _load_module():
    spec = importlib.util.spec_from_file_location("generate_entity_xref", SCRIPT)
    assert spec is not None and spec.loader is not None
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


def test_script_exists() -> None:
    assert SCRIPT.exists()


def test_fuzzy_match_identical_names_scores_100() -> None:
    try:
        mod = _load_module()
    except ImportError:
        pytest.skip("rapidfuzz not installed; run with `uv run --with rapidfuzz ...`")
    assert mod.fuzzy_match_score("Cristiano Ronaldo", "Cristiano Ronaldo") == 100


def test_fuzzy_match_word_order_variant_scores_above_90() -> None:
    try:
        mod = _load_module()
    except ImportError:
        pytest.skip("rapidfuzz not installed")
    assert mod.fuzzy_match_score("Ronaldo, Cristiano", "Cristiano Ronaldo") >= 90


def test_fuzzy_match_threshold_70_filters_weak_pairs() -> None:
    try:
        mod = _load_module()
    except ImportError:
        pytest.skip("rapidfuzz not installed")
    assert mod.fuzzy_match_score("Lionel Messi", "Cristiano Ronaldo") < 70


def test_provider_ordering_player_rows() -> None:
    try:
        mod = _load_module()
    except ImportError:
        pytest.skip("rapidfuzz not installed")
    row = mod.emit_pair_ordered("wyscout", "100", "statsbomb", "200", 85, 1, "player_id")
    assert row["source_a"] == "statsbomb"
    assert row["source_b"] == "wyscout"
    assert row["player_id_a"] == "200"
    assert row["player_id_b"] == "100"


def test_provider_ordering_team_rows() -> None:
    try:
        mod = _load_module()
    except ImportError:
        pytest.skip("rapidfuzz not installed")
    row = mod.emit_pair_ordered("idsse", "DFL-X", "statsbomb", "42", 90, 1, "team_id")
    assert row["source_a"] == "idsse"
    assert row["source_b"] == "statsbomb"
    assert row["team_id_a"] == "DFL-X"
    assert row["team_id_b"] == "42"


def test_emit_pair_preserves_identical_order() -> None:
    try:
        mod = _load_module()
    except ImportError:
        pytest.skip("rapidfuzz not installed")
    row = mod.emit_pair_ordered("idsse", "A", "statsbomb", "B", 80, 1, "player_id")
    # idsse < statsbomb — already ordered
    assert row["source_a"] == "idsse"
    assert row["source_b"] == "statsbomb"
