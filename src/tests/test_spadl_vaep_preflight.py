"""Tests for SPADL/VAEP preflight chunking and --match-ids parsing."""

from __future__ import annotations

import pytest


def test_parse_vaep_match_ids_arg_none() -> None:
    """None input returns None (no filter)."""
    from ingestion.spadl_vaep import _parse_vaep_match_ids_arg

    assert _parse_vaep_match_ids_arg(None) is None


def test_parse_vaep_match_ids_arg_empty() -> None:
    """Empty string returns None."""
    from ingestion.spadl_vaep import _parse_vaep_match_ids_arg

    assert _parse_vaep_match_ids_arg("") is None


def test_parse_vaep_match_ids_arg_convert_chunk() -> None:
    """Provider:ids format returns (provider, [ids])."""
    from ingestion.spadl_vaep import _parse_vaep_match_ids_arg

    result = _parse_vaep_match_ids_arg("statsbomb:3754348,3754349,3754350")
    assert result == ("statsbomb", [3754348, 3754349, 3754350])


def test_parse_vaep_match_ids_arg_score_chunk() -> None:
    """score: prefix returns ("score", [ids])."""
    from ingestion.spadl_vaep import _parse_vaep_match_ids_arg

    result = _parse_vaep_match_ids_arg("score:100,200,300")
    assert result == ("score", [100, 200, 300])


def test_parse_vaep_match_ids_arg_single_id() -> None:
    """Single match ID works."""
    from ingestion.spadl_vaep import _parse_vaep_match_ids_arg

    result = _parse_vaep_match_ids_arg("wyscout:12345")
    assert result == ("wyscout", [12345])


def test_parse_vaep_match_ids_arg_all_providers() -> None:
    """All valid providers parse correctly."""
    from ingestion.spadl_vaep import _parse_vaep_match_ids_arg

    for provider in ("statsbomb", "wyscout", "idsse", "metrica", "skillcorner", "score"):
        result = _parse_vaep_match_ids_arg(f"{provider}:999")
        assert result is not None
        assert result[0] == provider


def test_parse_vaep_match_ids_arg_unknown_provider() -> None:
    """Unknown provider raises SystemExit."""
    from ingestion.spadl_vaep import _parse_vaep_match_ids_arg

    with pytest.raises(SystemExit, match="Unknown provider"):
        _parse_vaep_match_ids_arg("opta:12345")


def test_parse_vaep_match_ids_arg_no_colon() -> None:
    """Missing colon raises SystemExit."""
    from ingestion.spadl_vaep import _parse_vaep_match_ids_arg

    with pytest.raises(SystemExit, match="must be"):
        _parse_vaep_match_ids_arg("12345,67890")


def test_parse_vaep_match_ids_arg_non_integer_ids() -> None:
    """Non-integer match IDs raise SystemExit."""
    from ingestion.spadl_vaep import _parse_vaep_match_ids_arg

    with pytest.raises(SystemExit, match="non-integer"):
        _parse_vaep_match_ids_arg("statsbomb:abc,def")


def test_build_chunks_statsbomb_200_per_chunk() -> None:
    """StatsBomb matches chunked at 200."""
    from ingestion.spadl_vaep import _build_chunks

    metadata = {
        "sb_new": [str(i) for i in range(450)],
        "ws_new": [],
        "idsse_new": [],
        "metrica_new": [],
        "sc_new": [],
        "unscored_vaep_match_ids": [],
    }
    chunks = _build_chunks(metadata)
    # 450 / 200 = 3 chunks (200 + 200 + 50)
    assert len(chunks) == 3
    assert all(c.startswith("statsbomb:") for c in chunks)
    # First chunk has 200 IDs
    assert len(chunks[0].split(":")[1].split(",")) == 200
    # Last chunk has 50
    assert len(chunks[2].split(":")[1].split(",")) == 50


def test_build_chunks_multiple_providers() -> None:
    """Multiple providers produce separate chunk lists."""
    from ingestion.spadl_vaep import _build_chunks

    metadata = {
        "sb_new": ["1", "2", "3"],
        "ws_new": ["10", "11"],
        "idsse_new": ["100"],
        "metrica_new": [],
        "sc_new": [],
        "unscored_vaep_match_ids": ["500", "501"],
    }
    chunks = _build_chunks(metadata)
    providers = [c.split(":")[0] for c in chunks]
    assert "statsbomb" in providers
    assert "wyscout" in providers
    assert "idsse" in providers
    assert "score" in providers
    assert len(chunks) == 4  # sb(1) + ws(1) + idsse(1) + score(1)


def test_build_chunks_unscored_uses_score_prefix() -> None:
    """Unscored matches use 'score:' prefix."""
    from ingestion.spadl_vaep import _build_chunks

    metadata = {
        "sb_new": [],
        "ws_new": [],
        "idsse_new": [],
        "metrica_new": [],
        "sc_new": [],
        "unscored_vaep_match_ids": ["1", "2", "3"],
    }
    chunks = _build_chunks(metadata)
    assert len(chunks) == 1
    assert chunks[0] == "score:1,2,3"


def test_build_chunks_empty_metadata() -> None:
    """Empty metadata produces no chunks."""
    from ingestion.spadl_vaep import _build_chunks

    metadata = {
        "sb_new": [],
        "ws_new": [],
        "idsse_new": [],
        "metrica_new": [],
        "sc_new": [],
        "unscored_vaep_match_ids": [],
    }
    chunks = _build_chunks(metadata)
    assert chunks == []


def test_build_chunks_idsse_50_per_chunk() -> None:
    """IDSSE/Metrica/SkillCorner chunked at 50."""
    from ingestion.spadl_vaep import _build_chunks

    metadata = {
        "sb_new": [],
        "ws_new": [],
        "idsse_new": [str(i) for i in range(120)],
        "metrica_new": [],
        "sc_new": [],
        "unscored_vaep_match_ids": [],
    }
    chunks = _build_chunks(metadata)
    assert len(chunks) == 3  # 50 + 50 + 20
    assert all(c.startswith("idsse:") for c in chunks)


def test_converter_match_id_filter_concept() -> None:
    """Verify the match_id_filter intersection logic works correctly.

    This tests the pattern used in all 5 converters:
    new_game_ids = [gid for gid in all_game_ids if gid not in existing_matches]
    if match_id_filter is not None:
        new_game_ids = [gid for gid in new_game_ids if gid in match_id_filter]
    """
    all_game_ids = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]
    existing_matches: set[int] = {1, 2, 3}  # already converted

    # Without filter: converts 4-10 (7 matches)
    new_game_ids = [gid for gid in all_game_ids if gid not in existing_matches]
    assert new_game_ids == [4, 5, 6, 7, 8, 9, 10]

    # With filter: only converts the chunk's matches (4, 5)
    match_id_filter = {4, 5}
    filtered = [gid for gid in new_game_ids if gid in match_id_filter]
    assert filtered == [4, 5]


def test_chunk_encoding_round_trip() -> None:
    """Chunk string round-trips through parse."""
    from ingestion.spadl_vaep import _parse_vaep_match_ids_arg

    chunk_str = "idsse:111111,222222,333333"
    result = _parse_vaep_match_ids_arg(chunk_str)
    assert result is not None
    provider, ids = result
    reconstructed = f"{provider}:{','.join(str(i) for i in ids)}"
    assert reconstructed == chunk_str


def test_model_cache_round_trip() -> None:
    """Serialize XGBoost model -> write to path -> read -> predict -> same output."""
    import os
    import tempfile

    import numpy as np
    from xgboost import XGBClassifier

    rng = np.random.default_rng(42)
    features = rng.random((100, 5))
    labels = (features[:, 0] > 0.5).astype(int)
    model = XGBClassifier(n_estimators=3, max_depth=2, use_label_encoder=False, eval_metric="logloss")
    model.fit(features, labels)

    # Serialize (same as preflight does)
    raw_bytes = bytes(model.get_booster().save_raw("json"))

    # Write to temp path (simulates UC Volume write)
    with tempfile.TemporaryDirectory() as tmpdir:
        model_path = os.path.join(tmpdir, "test_model.xgb")
        with open(model_path, "wb") as f:
            f.write(raw_bytes)

        # Read back (simulates iteration read)
        with open(model_path, "rb") as f:
            loaded_bytes = f.read()

    # Deserialize (same as scoring UDF does)
    loaded_model = XGBClassifier()
    loaded_model.load_model(bytearray(loaded_bytes))

    # Predict on known input — must match original
    test_features = rng.random((10, 5))
    original_preds = model.predict_proba(test_features)
    loaded_preds = loaded_model.predict_proba(test_features)

    np.testing.assert_array_almost_equal(original_preds, loaded_preds)


def test_main_without_match_ids_calls_run_pipeline(monkeypatch) -> None:
    """main() without --match-ids dispatches to run_pipeline (monolithic mode)."""
    import sys
    from unittest.mock import MagicMock, patch

    monkeypatch.setattr(
        sys,
        "argv",
        ["compute_spadl_vaep", "--catalog", "soccer_analytics", "--schema", "bronze"],
    )
    with (
        patch("ingestion.spadl_vaep.get_spark_session") as mock_spark,
        patch("ingestion.spadl_vaep.timed_check") as mock_check,
        patch("ingestion.spadl_vaep.run_pipeline") as mock_run,
        patch("ingestion.spadl_vaep._run_chunk") as mock_chunk,
        patch("ingestion.spadl_vaep.configure_logging") as mock_log,
        patch("ingestion.bootstrap.bootstrap_hooks"),
    ):
        mock_spark.return_value = MagicMock()
        mock_log.return_value = MagicMock()
        mock_check.return_value = MagicMock(count=0, metadata={})

        from ingestion.spadl_vaep import main

        main()

        # Monolithic path: run_pipeline called, NOT _run_chunk
        mock_run.assert_called_once()
        mock_chunk.assert_not_called()


def test_main_with_match_ids_calls_run_chunk(monkeypatch) -> None:
    """main() with --match-ids dispatches to _run_chunk (chunk mode)."""
    import sys
    from unittest.mock import MagicMock, patch

    monkeypatch.setattr(
        sys,
        "argv",
        [
            "compute_spadl_vaep",
            "--catalog",
            "soccer_analytics",
            "--schema",
            "bronze",
            "--match-ids",
            "statsbomb:100,200,300",
        ],
    )
    with (
        patch("ingestion.spadl_vaep.get_spark_session") as mock_spark,
        patch("ingestion.spadl_vaep._run_chunk") as mock_chunk,
        patch("ingestion.spadl_vaep.run_pipeline") as mock_run,
        patch("ingestion.spadl_vaep.configure_logging") as mock_log,
        patch("ingestion.bootstrap.bootstrap_hooks"),
    ):
        mock_spark.return_value = MagicMock()
        mock_log.return_value = MagicMock()

        from ingestion.spadl_vaep import main

        main()

        # Chunk mode: _run_chunk called with parsed provider + ids
        mock_chunk.assert_called_once_with(
            mock_spark.return_value,
            "soccer_analytics",
            "bronze",
            mock_log.return_value,
            "statsbomb",
            [100, 200, 300],
        )
        mock_run.assert_not_called()
