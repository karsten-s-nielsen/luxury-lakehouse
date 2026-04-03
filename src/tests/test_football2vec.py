"""Tests for Football2Vec tokenizer, training, and inference."""

from __future__ import annotations

import tempfile
from pathlib import Path

import pandas as pd
import pytest
from gensim.models.doc2vec import Doc2Vec

from analytics.football2vec import (
    SPADL_ACTION_TYPES,
    TokenizerConfig,
    TrainingConfig,
    infer_vectors,
    tokenize_event,
    tokenize_match_events,
    train_model,
)

# ---------------------------------------------------------------------------
# TokenizerConfig
# ---------------------------------------------------------------------------


class TestTokenizerConfig:
    """Frozen dataclass defaults and immutability."""

    def test_default_grid_dimensions(self) -> None:
        cfg = TokenizerConfig()
        assert cfg.grid_cols == 12
        assert cfg.grid_rows == 8

    def test_default_pitch_dimensions(self) -> None:
        cfg = TokenizerConfig()
        assert cfg.pitch_length == 105.0
        assert cfg.pitch_width == 68.0

    def test_frozen(self) -> None:
        cfg = TokenizerConfig()
        with pytest.raises(AttributeError):
            cfg.grid_cols = 16  # type: ignore[misc]

    def test_custom_grid(self) -> None:
        cfg = TokenizerConfig(grid_cols=16, grid_rows=12)
        assert cfg.grid_cols == 16
        assert cfg.grid_rows == 12


# ---------------------------------------------------------------------------
# SPADL_ACTION_TYPES
# ---------------------------------------------------------------------------


class TestSPADLActionTypes:
    """Validate the SPADL 23-type vocabulary constant."""

    def test_count(self) -> None:
        assert len(SPADL_ACTION_TYPES) == 23

    def test_is_frozenset(self) -> None:
        assert isinstance(SPADL_ACTION_TYPES, frozenset)

    def test_contains_core_types(self) -> None:
        for action in ["pass", "shot", "cross", "tackle", "interception", "clearance"]:
            assert action in SPADL_ACTION_TYPES

    def test_contains_keeper_types(self) -> None:
        for action in ["keeper_save", "keeper_claim", "keeper_punch", "keeper_pick_up"]:
            assert action in SPADL_ACTION_TYPES

    def test_contains_set_piece_types(self) -> None:
        for action in [
            "throw_in",
            "freekick_crossed",
            "freekick_short",
            "corner_crossed",
            "corner_short",
            "goalkick",
        ]:
            assert action in SPADL_ACTION_TYPES

    def test_contains_shot_variants(self) -> None:
        for action in ["shot", "shot_penalty", "shot_freekick"]:
            assert action in SPADL_ACTION_TYPES

    def test_contains_other_types(self) -> None:
        for action in ["take_on", "foul", "dribble", "bad_touch", "non_action"]:
            assert action in SPADL_ACTION_TYPES


# ---------------------------------------------------------------------------
# tokenize_event — SPADL actions
# ---------------------------------------------------------------------------


class TestTokenizeEvent:
    """SPADL event tokenization with grid mapping on 105x68 pitch."""

    # Cell width = 105 / 12 = 8.75, cell height = 68 / 8 = 8.5

    def test_pass_center(self) -> None:
        """pass at (52.5, 34.0) -> grid (6, 4) — center of SPADL pitch."""
        event = {"action_type": "pass", "start_x": 52.5, "start_y": 34.0}
        result = tokenize_event(event)
        assert result == "pass_6_4"

    def test_shot_near_goal(self) -> None:
        """shot at (100.0, 34.0) -> grid (11, 4)."""
        event = {"action_type": "shot", "start_x": 100.0, "start_y": 34.0}
        result = tokenize_event(event)
        assert result == "shot_11_4"

    def test_cross(self) -> None:
        event = {"action_type": "cross", "start_x": 87.5, "start_y": 8.5}
        result = tokenize_event(event)
        assert result == "cross_10_1"

    def test_tackle(self) -> None:
        event = {"action_type": "tackle", "start_x": 26.25, "start_y": 17.0}
        result = tokenize_event(event)
        assert result == "tackle_3_2"

    def test_interception(self) -> None:
        event = {"action_type": "interception", "start_x": 35.0, "start_y": 51.0}
        result = tokenize_event(event)
        assert result == "interception_4_6"

    def test_foul(self) -> None:
        event = {"action_type": "foul", "start_x": 26.25, "start_y": 8.5}
        result = tokenize_event(event)
        assert result == "foul_3_1"

    def test_clearance(self) -> None:
        event = {"action_type": "clearance", "start_x": 8.75, "start_y": 4.0}
        result = tokenize_event(event)
        assert result == "clearance_1_0"

    def test_take_on(self) -> None:
        event = {"action_type": "take_on", "start_x": 70.0, "start_y": 25.5}
        result = tokenize_event(event)
        assert result == "take_on_8_3"

    def test_dribble(self) -> None:
        event = {"action_type": "dribble", "start_x": 43.75, "start_y": 17.0}
        result = tokenize_event(event)
        assert result == "dribble_5_2"

    def test_shot_penalty(self) -> None:
        event = {"action_type": "shot_penalty", "start_x": 96.0, "start_y": 34.0}
        result = tokenize_event(event)
        assert result == "shot_penalty_10_4"

    def test_shot_freekick(self) -> None:
        event = {"action_type": "shot_freekick", "start_x": 78.75, "start_y": 34.0}
        result = tokenize_event(event)
        assert result == "shot_freekick_9_4"

    def test_keeper_save(self) -> None:
        event = {"action_type": "keeper_save", "start_x": 4.0, "start_y": 34.0}
        result = tokenize_event(event)
        assert result == "keeper_save_0_4"

    def test_keeper_claim(self) -> None:
        event = {"action_type": "keeper_claim", "start_x": 8.75, "start_y": 34.0}
        result = tokenize_event(event)
        assert result == "keeper_claim_1_4"

    def test_keeper_punch(self) -> None:
        event = {"action_type": "keeper_punch", "start_x": 8.75, "start_y": 38.0}
        result = tokenize_event(event)
        assert result == "keeper_punch_1_4"

    def test_keeper_pick_up(self) -> None:
        event = {"action_type": "keeper_pick_up", "start_x": 4.0, "start_y": 30.0}
        result = tokenize_event(event)
        assert result == "keeper_pick_up_0_3"

    def test_throw_in(self) -> None:
        event = {"action_type": "throw_in", "start_x": 26.25, "start_y": 0.0}
        result = tokenize_event(event)
        assert result == "throw_in_3_0"

    def test_freekick_crossed(self) -> None:
        event = {"action_type": "freekick_crossed", "start_x": 70.0, "start_y": 8.5}
        result = tokenize_event(event)
        assert result == "freekick_crossed_8_1"

    def test_freekick_short(self) -> None:
        event = {"action_type": "freekick_short", "start_x": 52.5, "start_y": 34.0}
        result = tokenize_event(event)
        assert result == "freekick_short_6_4"

    def test_corner_crossed(self) -> None:
        event = {"action_type": "corner_crossed", "start_x": 105.0, "start_y": 0.0}
        result = tokenize_event(event)
        assert result == "corner_crossed_11_0"

    def test_corner_short(self) -> None:
        event = {"action_type": "corner_short", "start_x": 105.0, "start_y": 68.0}
        result = tokenize_event(event)
        assert result == "corner_short_11_7"

    def test_goalkick(self) -> None:
        event = {"action_type": "goalkick", "start_x": 8.75, "start_y": 34.0}
        result = tokenize_event(event)
        assert result == "goalkick_1_4"

    def test_bad_touch(self) -> None:
        event = {"action_type": "bad_touch", "start_x": 52.5, "start_y": 34.0}
        result = tokenize_event(event)
        assert result == "bad_touch_6_4"

    def test_non_action(self) -> None:
        event = {"action_type": "non_action", "start_x": 0.0, "start_y": 0.0}
        result = tokenize_event(event)
        assert result == "non_action_0_0"

    def test_missing_action_type_defaults_to_non_action(self) -> None:
        event = {"start_x": 52.5, "start_y": 34.0}
        result = tokenize_event(event)
        assert result == "non_action_6_4"

    def test_all_23_spadl_types_produce_valid_tokens(self) -> None:
        """Every SPADL action type should produce a valid token at pitch center."""
        for action in sorted(SPADL_ACTION_TYPES):
            event = {"action_type": action, "start_x": 52.5, "start_y": 34.0}
            result = tokenize_event(event)
            assert result is not None, f"{action} produced None"
            assert result.startswith(f"{action}_"), f"{action} token: {result}"

    def test_default_config_when_none(self) -> None:
        """Passing config=None should use SPADL 105x68 defaults."""
        event = {"action_type": "pass", "start_x": 52.5, "start_y": 34.0}
        result = tokenize_event(event, config=None)
        assert result == "pass_6_4"

    def test_explicit_config(self) -> None:
        """Custom config overrides defaults."""
        cfg = TokenizerConfig(grid_cols=10, grid_rows=10, pitch_length=100.0, pitch_width=100.0)
        event = {"action_type": "pass", "start_x": 50.0, "start_y": 50.0}
        result = tokenize_event(event, config=cfg)
        assert result == "pass_5_5"


# ---------------------------------------------------------------------------
# tokenize_event — Grid boundary cases (105x68)
# ---------------------------------------------------------------------------


class TestTokenizeEventGrid:
    """Grid coordinate mapping edge cases on SPADL 105x68 pitch."""

    def test_origin(self) -> None:
        event = {"action_type": "pass", "start_x": 0.0, "start_y": 0.0}
        result = tokenize_event(event)
        assert result == "pass_0_0"

    def test_max_coordinates_clamped(self) -> None:
        """start_x=105, start_y=68 should clamp to grid_cols-1, grid_rows-1."""
        event = {"action_type": "pass", "start_x": 105.0, "start_y": 68.0}
        result = tokenize_event(event)
        assert result == "pass_11_7"

    def test_just_below_first_boundary(self) -> None:
        """start_x=8.74 -> grid_x=0 (8.74/8.75=0.999 -> int=0)."""
        event = {"action_type": "pass", "start_x": 8.74, "start_y": 8.49}
        result = tokenize_event(event)
        assert result == "pass_0_0"

    def test_at_first_boundary(self) -> None:
        """start_x=8.75 -> grid_x=1, start_y=8.5 -> grid_y=1."""
        event = {"action_type": "pass", "start_x": 8.75, "start_y": 8.5}
        result = tokenize_event(event)
        assert result == "pass_1_1"

    def test_null_start_x_returns_none(self) -> None:
        event = {"action_type": "pass", "start_x": None, "start_y": 34.0}
        assert tokenize_event(event) is None

    def test_null_start_y_returns_none(self) -> None:
        event = {"action_type": "pass", "start_x": 52.5, "start_y": None}
        assert tokenize_event(event) is None

    def test_both_null_returns_none(self) -> None:
        event = {"action_type": "pass", "start_x": None, "start_y": None}
        assert tokenize_event(event) is None

    def test_nan_start_x_returns_none(self) -> None:
        event = {"action_type": "pass", "start_x": float("nan"), "start_y": 34.0}
        assert tokenize_event(event) is None

    def test_nan_start_y_returns_none(self) -> None:
        event = {"action_type": "pass", "start_x": 52.5, "start_y": float("nan")}
        assert tokenize_event(event) is None

    def test_missing_start_x_key_returns_none(self) -> None:
        event = {"action_type": "pass", "start_y": 34.0}
        assert tokenize_event(event) is None

    def test_missing_start_y_key_returns_none(self) -> None:
        event = {"action_type": "pass", "start_x": 52.5}
        assert tokenize_event(event) is None


# ---------------------------------------------------------------------------
# tokenize_match_events — DataFrame → dict
# ---------------------------------------------------------------------------


class TestTokenizeMatchEvents:
    """Batch tokenization from DataFrame to player-match token sequences."""

    @staticmethod
    def _make_df(rows: list[dict[str, object]]) -> pd.DataFrame:
        cols = [
            "canonical_player_id",
            "match_id",
            "action_type",
            "start_x",
            "start_y",
            "event_index",
        ]
        df = pd.DataFrame(rows)
        for col in cols:
            if col not in df.columns:
                df[col] = None
        return pd.DataFrame(df[cols])

    def test_single_player_single_match(self) -> None:
        rows = [
            {
                "canonical_player_id": "p1",
                "match_id": "m1",
                "action_type": "pass",
                "start_x": 52.5,
                "start_y": 34.0,
                "event_index": 1,
            },
            {
                "canonical_player_id": "p1",
                "match_id": "m1",
                "action_type": "shot",
                "start_x": 100.0,
                "start_y": 34.0,
                "event_index": 2,
            },
        ]
        result = tokenize_match_events(self._make_df(rows))
        assert ("p1", "m1") in result
        assert result[("p1", "m1")] == ["pass_6_4", "shot_11_4"]

    def test_ordering_by_event_index(self) -> None:
        rows = [
            {
                "canonical_player_id": "p1",
                "match_id": "m1",
                "action_type": "shot",
                "start_x": 100.0,
                "start_y": 34.0,
                "event_index": 2,
            },
            {
                "canonical_player_id": "p1",
                "match_id": "m1",
                "action_type": "pass",
                "start_x": 52.5,
                "start_y": 34.0,
                "event_index": 1,
            },
        ]
        result = tokenize_match_events(self._make_df(rows))
        assert result[("p1", "m1")] == ["pass_6_4", "shot_11_4"]

    def test_multiple_players(self) -> None:
        rows = [
            {
                "canonical_player_id": "p1",
                "match_id": "m1",
                "action_type": "pass",
                "start_x": 52.5,
                "start_y": 34.0,
                "event_index": 1,
            },
            {
                "canonical_player_id": "p2",
                "match_id": "m1",
                "action_type": "shot",
                "start_x": 100.0,
                "start_y": 34.0,
                "event_index": 2,
            },
        ]
        result = tokenize_match_events(self._make_df(rows))
        assert ("p1", "m1") in result
        assert ("p2", "m1") in result
        assert result[("p1", "m1")] == ["pass_6_4"]
        assert result[("p2", "m1")] == ["shot_11_4"]

    def test_null_coords_skipped(self) -> None:
        rows = [
            {
                "canonical_player_id": "p1",
                "match_id": "m1",
                "action_type": "pass",
                "start_x": None,
                "start_y": 34.0,
                "event_index": 1,
            },
            {
                "canonical_player_id": "p1",
                "match_id": "m1",
                "action_type": "shot",
                "start_x": 100.0,
                "start_y": 34.0,
                "event_index": 2,
            },
        ]
        result = tokenize_match_events(self._make_df(rows))
        assert result[("p1", "m1")] == ["shot_11_4"]

    def test_empty_dataframe(self) -> None:
        rows: list[dict[str, object]] = []
        result = tokenize_match_events(self._make_df(rows))
        assert result == {}

    def test_all_null_coords_excluded(self) -> None:
        """Player-match with all null coords should not appear in output."""
        rows = [
            {
                "canonical_player_id": "p1",
                "match_id": "m1",
                "action_type": "pass",
                "start_x": None,
                "start_y": None,
                "event_index": 1,
            },
        ]
        result = tokenize_match_events(self._make_df(rows))
        assert ("p1", "m1") not in result

    def test_default_config_when_none(self) -> None:
        """Passing config=None should use defaults."""
        rows = [
            {
                "canonical_player_id": "p1",
                "match_id": "m1",
                "action_type": "pass",
                "start_x": 52.5,
                "start_y": 34.0,
                "event_index": 1,
            },
        ]
        result = tokenize_match_events(self._make_df(rows), config=None)
        assert result[("p1", "m1")] == ["pass_6_4"]

    def test_spadl_action_types_in_dataframe(self) -> None:
        """Various SPADL action types pass through correctly in batch mode."""
        rows = [
            {
                "canonical_player_id": "p1",
                "match_id": "m1",
                "action_type": "tackle",
                "start_x": 26.25,
                "start_y": 17.0,
                "event_index": 1,
            },
            {
                "canonical_player_id": "p1",
                "match_id": "m1",
                "action_type": "keeper_save",
                "start_x": 4.0,
                "start_y": 34.0,
                "event_index": 2,
            },
            {
                "canonical_player_id": "p1",
                "match_id": "m1",
                "action_type": "shot_penalty",
                "start_x": 96.0,
                "start_y": 34.0,
                "event_index": 3,
            },
        ]
        result = tokenize_match_events(self._make_df(rows))
        assert result[("p1", "m1")] == [
            "tackle_3_2",
            "keeper_save_0_4",
            "shot_penalty_10_4",
        ]


# ---------------------------------------------------------------------------
# TrainingConfig
# ---------------------------------------------------------------------------


class TestTrainingConfig:
    """Frozen Doc2Vec hyperparameter config."""

    def test_defaults(self) -> None:
        cfg = TrainingConfig()
        assert cfg.vector_size == 32
        assert cfg.window == 5
        assert cfg.min_count == 2
        assert cfg.epochs == 20
        assert cfg.dm == 1

    def test_frozen(self) -> None:
        cfg = TrainingConfig()
        with pytest.raises(AttributeError):
            cfg.vector_size = 64  # type: ignore[misc]


# ---------------------------------------------------------------------------
# train_model
# ---------------------------------------------------------------------------


class TestTrainModel:
    """Doc2Vec model training from token sequences."""

    @staticmethod
    def _sample_sequences() -> dict[tuple[str, str], list[str]]:
        """Generate enough token sequences to meet min_count=2."""
        return {
            ("p1", "m1"): ["pass_6_4", "shot_11_4", "dribble_5_2", "pass_6_4"],
            ("p1", "m2"): ["pass_6_4", "tackle_3_2", "interception_4_6", "pass_6_4"],
            ("p2", "m1"): ["shot_11_4", "dribble_5_2", "pass_6_4", "shot_11_4"],
            ("p2", "m2"): ["tackle_3_2", "pass_6_4", "shot_11_4", "tackle_3_2"],
            ("p3", "m1"): ["pass_6_4", "dribble_5_2", "foul_3_1", "pass_6_4"],
        }

    def test_returns_doc2vec_model(self) -> None:
        seqs = self._sample_sequences()
        model = train_model(seqs, TrainingConfig())
        assert isinstance(model, Doc2Vec)

    def test_model_has_vocabulary(self) -> None:
        seqs = self._sample_sequences()
        model = train_model(seqs, TrainingConfig())
        # pass_6_4 appears in all sequences; should be in vocab
        assert "pass_6_4" in model.wv

    def test_vector_size_matches_config(self) -> None:
        cfg = TrainingConfig(vector_size=16)
        seqs = self._sample_sequences()
        model = train_model(seqs, cfg)
        assert model.wv.vector_size == 16

    def test_model_save_and_load(self) -> None:
        seqs = self._sample_sequences()
        model = train_model(seqs, TrainingConfig())
        with tempfile.TemporaryDirectory() as tmpdir:
            path = str(Path(tmpdir) / "test.model")
            model.save(path)
            loaded = Doc2Vec.load(path)
            assert loaded.wv.vector_size == model.wv.vector_size

    def test_min_count_filters_rare_tokens(self) -> None:
        """foul_3_1 only appears once — should be filtered with min_count=2."""
        seqs = self._sample_sequences()
        cfg = TrainingConfig(min_count=2)
        model = train_model(seqs, cfg)
        assert "foul_3_1" not in model.wv


# ---------------------------------------------------------------------------
# infer_vectors
# ---------------------------------------------------------------------------


class TestInferVectors:
    """Embedding inference from trained model."""

    @staticmethod
    def _trained_model() -> tuple[Doc2Vec, dict[tuple[str, str], list[str]]]:
        seqs = {
            ("p1", "m1"): ["pass_6_4", "shot_11_4", "dribble_5_2", "pass_6_4"],
            ("p1", "m2"): ["pass_6_4", "tackle_3_2", "interception_4_6", "pass_6_4"],
            ("p2", "m1"): ["shot_11_4", "dribble_5_2", "pass_6_4", "shot_11_4"],
            ("p2", "m2"): ["tackle_3_2", "pass_6_4", "shot_11_4", "tackle_3_2"],
            ("p3", "m1"): ["pass_6_4", "dribble_5_2", "pass_6_4", "dribble_5_2"],
        }
        model = train_model(seqs, TrainingConfig())
        return model, seqs

    def test_returns_dict_of_vectors(self) -> None:
        model, seqs = self._trained_model()
        vectors = infer_vectors(model, seqs)
        assert isinstance(vectors, dict)
        assert len(vectors) == len(seqs)

    def test_vector_length_matches_config(self) -> None:
        model, seqs = self._trained_model()
        vectors = infer_vectors(model, seqs)
        for _key, vec in vectors.items():
            assert len(vec) == 32  # default vector_size

    def test_vector_values_are_floats(self) -> None:
        model, seqs = self._trained_model()
        vectors = infer_vectors(model, seqs)
        for _key, vec in vectors.items():
            assert all(isinstance(v, float) for v in vec)

    def test_keys_preserved(self) -> None:
        model, seqs = self._trained_model()
        vectors = infer_vectors(model, seqs)
        assert set(vectors.keys()) == set(seqs.keys())

    def test_custom_epochs(self) -> None:
        model, seqs = self._trained_model()
        vectors = infer_vectors(model, seqs, epochs=5)
        assert len(vectors) == len(seqs)


# ---------------------------------------------------------------------------
# Football2VecModel (MLflow pyfunc) — mocked since mlflow not installed locally
# ---------------------------------------------------------------------------


class TestFootball2VecModel:
    """MLflow pyfunc wrapper — constructable with in-memory data (no filesystem)."""

    @staticmethod
    def _trained_model() -> Doc2Vec:
        seqs = {
            ("p1", "m1"): ["pass_6_4", "shot_11_4", "dribble_5_2", "pass_6_4"],
            ("p1", "m2"): ["pass_6_4", "tackle_3_2", "pass_6_4", "tackle_3_2"],
            ("p2", "m1"): ["shot_11_4", "dribble_5_2", "pass_6_4", "shot_11_4"],
        }
        return train_model(seqs, TrainingConfig())

    def test_construct_with_model_and_config(self) -> None:
        """Direct construction with pre-loaded model — no filesystem needed."""
        from analytics.football2vec import Football2VecModel

        model = self._trained_model()
        wrapper = Football2VecModel(model=model, tokenizer_config=TokenizerConfig())
        assert wrapper.model is model
        assert wrapper.tokenizer_config == TokenizerConfig()

    def test_construct_empty_for_mlflow(self) -> None:
        """Empty construction (MLflow pattern) — model is None until load_context."""
        from analytics.football2vec import Football2VecModel

        wrapper = Football2VecModel()
        assert wrapper.model is None
        assert wrapper.tokenizer_config == TokenizerConfig()

    def test_predict_returns_dataframe_with_vector_column(self) -> None:
        """Verify predict() shape without requiring mlflow runtime."""
        from analytics.football2vec import Football2VecModel

        wrapper = Football2VecModel(model=self._trained_model(), tokenizer_config=TokenizerConfig())

        input_df = pd.DataFrame(
            {
                "tokens": [["pass_6_4", "shot_11_4"], ["tackle_3_2", "pass_6_4"]],
            }
        )

        # Call predict with context=None (not used in predict body beyond type sig)
        result = wrapper.predict(None, input_df)  # type: ignore[arg-type]
        assert isinstance(result, pd.DataFrame)
        assert "vector" in result.columns
        assert len(result) == 2
        assert len(result.iloc[0]["vector"]) == 32
