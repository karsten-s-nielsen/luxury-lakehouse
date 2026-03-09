"""Tests for Football2Vec tokenizer, training, and inference."""

from __future__ import annotations

import tempfile
from pathlib import Path

import pandas as pd
import pytest
from gensim.models.doc2vec import Doc2Vec

from analytics.football2vec import (
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
        assert cfg.pitch_length == 120.0
        assert cfg.pitch_width == 80.0

    def test_frozen(self) -> None:
        cfg = TokenizerConfig()
        with pytest.raises(AttributeError):
            cfg.grid_cols = 16  # type: ignore[misc]

    def test_custom_grid(self) -> None:
        cfg = TokenizerConfig(grid_cols=16, grid_rows=12)
        assert cfg.grid_cols == 16
        assert cfg.grid_rows == 12


# ---------------------------------------------------------------------------
# tokenize_event — StatsBomb
# ---------------------------------------------------------------------------


class TestTokenizeEventStatsBomb:
    """StatsBomb event tokenization with grid mapping."""

    def test_pass_basic(self) -> None:
        event = {
            "event_type": "Pass",
            "x": 60.0,
            "y": 40.0,
            "data_source": "statsbomb",
        }
        result = tokenize_event(event, TokenizerConfig())
        assert result == "pass_6_4"

    def test_pass_cross(self) -> None:
        event = {
            "event_type": "Pass",
            "x": 60.0,
            "y": 40.0,
            "data_source": "statsbomb",
            "pass_cross": True,
        }
        result = tokenize_event(event, TokenizerConfig())
        assert result == "cross_6_4"

    def test_pass_corner(self) -> None:
        event = {
            "event_type": "Pass",
            "x": 0.0,
            "y": 0.0,
            "data_source": "statsbomb",
            "play_pattern": "From Corner",
        }
        result = tokenize_event(event, TokenizerConfig())
        assert result == "corner_0_0"

    def test_pass_throw_in(self) -> None:
        event = {
            "event_type": "Pass",
            "x": 30.0,
            "y": 0.0,
            "data_source": "statsbomb",
            "play_pattern": "From Throw In",
        }
        result = tokenize_event(event, TokenizerConfig())
        assert result == "throw_in_3_0"

    def test_pass_cross_takes_precedence_over_play_pattern(self) -> None:
        """pass_cross=True should override play_pattern."""
        event = {
            "event_type": "Pass",
            "x": 60.0,
            "y": 40.0,
            "data_source": "statsbomb",
            "pass_cross": True,
            "play_pattern": "From Corner",
        }
        result = tokenize_event(event, TokenizerConfig())
        assert result == "cross_6_4"

    def test_shot(self) -> None:
        event = {
            "event_type": "Shot",
            "x": 110.0,
            "y": 40.0,
            "data_source": "statsbomb",
        }
        result = tokenize_event(event, TokenizerConfig())
        assert result == "shot_11_4"

    def test_carry(self) -> None:
        event = {
            "event_type": "Carry",
            "x": 50.0,
            "y": 20.0,
            "data_source": "statsbomb",
        }
        result = tokenize_event(event, TokenizerConfig())
        assert result == "carry_5_2"

    def test_duel(self) -> None:
        event = {
            "event_type": "Duel",
            "x": 70.0,
            "y": 50.0,
            "data_source": "statsbomb",
        }
        result = tokenize_event(event, TokenizerConfig())
        assert result == "duel_7_5"

    def test_interception(self) -> None:
        event = {
            "event_type": "Interception",
            "x": 40.0,
            "y": 60.0,
            "data_source": "statsbomb",
        }
        result = tokenize_event(event, TokenizerConfig())
        assert result == "interception_4_6"

    def test_foul_committed(self) -> None:
        event = {
            "event_type": "Foul Committed",
            "x": 30.0,
            "y": 10.0,
            "data_source": "statsbomb",
        }
        result = tokenize_event(event, TokenizerConfig())
        assert result == "foul_3_1"

    def test_clearance(self) -> None:
        event = {
            "event_type": "Clearance",
            "x": 10.0,
            "y": 5.0,
            "data_source": "statsbomb",
        }
        result = tokenize_event(event, TokenizerConfig())
        assert result == "clearance_1_0"

    def test_dribble_maps_to_take_on(self) -> None:
        event = {
            "event_type": "Dribble",
            "x": 80.0,
            "y": 30.0,
            "data_source": "statsbomb",
        }
        result = tokenize_event(event, TokenizerConfig())
        assert result == "take_on_8_3"

    def test_goalkeeper(self) -> None:
        event = {
            "event_type": "Goalkeeper",
            "x": 5.0,
            "y": 40.0,
            "data_source": "statsbomb",
        }
        result = tokenize_event(event, TokenizerConfig())
        assert result == "goalkeeper_0_4"

    def test_unknown_event_maps_to_other(self) -> None:
        event = {
            "event_type": "Ball Receipt*",
            "x": 50.0,
            "y": 40.0,
            "data_source": "statsbomb",
        }
        result = tokenize_event(event, TokenizerConfig())
        assert result == "other_5_4"


# ---------------------------------------------------------------------------
# tokenize_event — Wyscout
# ---------------------------------------------------------------------------


class TestTokenizeEventWyscout:
    """Wyscout event tokenization."""

    def test_pass_basic(self) -> None:
        event = {
            "event_type": "Pass",
            "x": 60.0,
            "y": 40.0,
            "data_source": "wyscout",
        }
        result = tokenize_event(event, TokenizerConfig())
        assert result == "pass_6_4"

    def test_pass_cross(self) -> None:
        event = {
            "event_type": "Pass",
            "x": 60.0,
            "y": 40.0,
            "data_source": "wyscout",
            "sub_event_type": "Cross",
        }
        result = tokenize_event(event, TokenizerConfig())
        assert result == "cross_6_4"

    def test_shot(self) -> None:
        event = {
            "event_type": "Shot",
            "x": 100.0,
            "y": 35.0,
            "data_source": "wyscout",
        }
        result = tokenize_event(event, TokenizerConfig())
        assert result == "shot_10_3"

    def test_duel(self) -> None:
        event = {
            "event_type": "Duel",
            "x": 50.0,
            "y": 20.0,
            "data_source": "wyscout",
        }
        result = tokenize_event(event, TokenizerConfig())
        assert result == "duel_5_2"

    def test_foul(self) -> None:
        event = {
            "event_type": "Foul",
            "x": 30.0,
            "y": 10.0,
            "data_source": "wyscout",
        }
        result = tokenize_event(event, TokenizerConfig())
        assert result == "foul_3_1"

    def test_free_kick_clearance(self) -> None:
        event = {
            "event_type": "Free Kick",
            "x": 20.0,
            "y": 40.0,
            "data_source": "wyscout",
            "sub_event_type": "Goal kick clearance",
        }
        result = tokenize_event(event, TokenizerConfig())
        assert result == "clearance_2_4"

    def test_free_kick_other(self) -> None:
        event = {
            "event_type": "Free Kick",
            "x": 20.0,
            "y": 40.0,
            "data_source": "wyscout",
            "sub_event_type": "Free kick shot",
        }
        result = tokenize_event(event, TokenizerConfig())
        assert result == "free_kick_2_4"

    def test_others_interception(self) -> None:
        event = {
            "event_type": "Others",
            "x": 40.0,
            "y": 60.0,
            "data_source": "wyscout",
            "sub_event_type": "Interception",
        }
        result = tokenize_event(event, TokenizerConfig())
        assert result == "interception_4_6"

    def test_others_acceleration(self) -> None:
        event = {
            "event_type": "Others",
            "x": 80.0,
            "y": 30.0,
            "data_source": "wyscout",
            "sub_event_type": "Acceleration",
        }
        result = tokenize_event(event, TokenizerConfig())
        assert result == "take_on_8_3"

    def test_others_touch(self) -> None:
        event = {
            "event_type": "Others",
            "x": 30.0,
            "y": 0.0,
            "data_source": "wyscout",
            "sub_event_type": "Touch",
        }
        result = tokenize_event(event, TokenizerConfig())
        assert result == "throw_in_3_0"

    def test_goalkeeper_leaving_line(self) -> None:
        event = {
            "event_type": "Goalkeeper leaving line",
            "x": 5.0,
            "y": 40.0,
            "data_source": "wyscout",
        }
        result = tokenize_event(event, TokenizerConfig())
        assert result == "goalkeeper_0_4"

    def test_unknown_wyscout_event(self) -> None:
        event = {
            "event_type": "Save attempt",
            "x": 10.0,
            "y": 40.0,
            "data_source": "wyscout",
        }
        result = tokenize_event(event, TokenizerConfig())
        assert result == "other_1_4"


# ---------------------------------------------------------------------------
# tokenize_event — Grid boundary cases
# ---------------------------------------------------------------------------


class TestTokenizeEventGrid:
    """Grid coordinate mapping edge cases."""

    def test_origin(self) -> None:
        event = {"event_type": "Pass", "x": 0.0, "y": 0.0, "data_source": "statsbomb"}
        result = tokenize_event(event, TokenizerConfig())
        assert result == "pass_0_0"

    def test_max_coordinates_clamped(self) -> None:
        """x=120, y=80 should clamp to grid_cols-1, grid_rows-1."""
        event = {"event_type": "Pass", "x": 120.0, "y": 80.0, "data_source": "statsbomb"}
        result = tokenize_event(event, TokenizerConfig())
        assert result == "pass_11_7"

    def test_just_below_boundary(self) -> None:
        """x=9.99 → grid_x=0 (10/10=1.0 → int(0.999)=0)."""
        event = {"event_type": "Pass", "x": 9.99, "y": 9.99, "data_source": "statsbomb"}
        result = tokenize_event(event, TokenizerConfig())
        assert result == "pass_0_0"

    def test_at_boundary(self) -> None:
        """x=10.0 → grid_x=1."""
        event = {"event_type": "Pass", "x": 10.0, "y": 10.0, "data_source": "statsbomb"}
        result = tokenize_event(event, TokenizerConfig())
        assert result == "pass_1_1"

    def test_null_x_returns_none(self) -> None:
        event = {"event_type": "Pass", "x": None, "y": 40.0, "data_source": "statsbomb"}
        assert tokenize_event(event, TokenizerConfig()) is None

    def test_null_y_returns_none(self) -> None:
        event = {"event_type": "Pass", "x": 60.0, "y": None, "data_source": "statsbomb"}
        assert tokenize_event(event, TokenizerConfig()) is None

    def test_both_null_returns_none(self) -> None:
        event = {"event_type": "Pass", "x": None, "y": None, "data_source": "statsbomb"}
        assert tokenize_event(event, TokenizerConfig()) is None

    def test_nan_x_returns_none(self) -> None:
        event = {"event_type": "Pass", "x": float("nan"), "y": 40.0, "data_source": "statsbomb"}
        assert tokenize_event(event, TokenizerConfig()) is None

    def test_nan_y_returns_none(self) -> None:
        event = {"event_type": "Pass", "x": 60.0, "y": float("nan"), "data_source": "statsbomb"}
        assert tokenize_event(event, TokenizerConfig()) is None


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
            "event_type",
            "x",
            "y",
            "event_index",
            "data_source",
            "play_pattern",
            "pass_cross",
            "sub_event_type",
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
                "event_type": "Pass",
                "x": 60.0,
                "y": 40.0,
                "event_index": 1,
                "data_source": "statsbomb",
            },
            {
                "canonical_player_id": "p1",
                "match_id": "m1",
                "event_type": "Shot",
                "x": 110.0,
                "y": 40.0,
                "event_index": 2,
                "data_source": "statsbomb",
            },
        ]
        result = tokenize_match_events(self._make_df(rows), TokenizerConfig())
        assert ("p1", "m1") in result
        assert result[("p1", "m1")] == ["pass_6_4", "shot_11_4"]

    def test_ordering_by_event_index(self) -> None:
        rows = [
            {
                "canonical_player_id": "p1",
                "match_id": "m1",
                "event_type": "Shot",
                "x": 110.0,
                "y": 40.0,
                "event_index": 2,
                "data_source": "statsbomb",
            },
            {
                "canonical_player_id": "p1",
                "match_id": "m1",
                "event_type": "Pass",
                "x": 60.0,
                "y": 40.0,
                "event_index": 1,
                "data_source": "statsbomb",
            },
        ]
        result = tokenize_match_events(self._make_df(rows), TokenizerConfig())
        assert result[("p1", "m1")] == ["pass_6_4", "shot_11_4"]

    def test_multiple_players(self) -> None:
        rows = [
            {
                "canonical_player_id": "p1",
                "match_id": "m1",
                "event_type": "Pass",
                "x": 60.0,
                "y": 40.0,
                "event_index": 1,
                "data_source": "statsbomb",
            },
            {
                "canonical_player_id": "p2",
                "match_id": "m1",
                "event_type": "Shot",
                "x": 110.0,
                "y": 40.0,
                "event_index": 2,
                "data_source": "statsbomb",
            },
        ]
        result = tokenize_match_events(self._make_df(rows), TokenizerConfig())
        assert ("p1", "m1") in result
        assert ("p2", "m1") in result
        assert result[("p1", "m1")] == ["pass_6_4"]
        assert result[("p2", "m1")] == ["shot_11_4"]

    def test_null_coords_skipped(self) -> None:
        rows = [
            {
                "canonical_player_id": "p1",
                "match_id": "m1",
                "event_type": "Pass",
                "x": None,
                "y": 40.0,
                "event_index": 1,
                "data_source": "statsbomb",
            },
            {
                "canonical_player_id": "p1",
                "match_id": "m1",
                "event_type": "Shot",
                "x": 110.0,
                "y": 40.0,
                "event_index": 2,
                "data_source": "statsbomb",
            },
        ]
        result = tokenize_match_events(self._make_df(rows), TokenizerConfig())
        assert result[("p1", "m1")] == ["shot_11_4"]

    def test_empty_dataframe(self) -> None:
        rows: list[dict[str, object]] = []
        result = tokenize_match_events(self._make_df(rows), TokenizerConfig())
        assert result == {}

    def test_all_null_coords_excluded(self) -> None:
        """Player-match with all null coords should not appear in output."""
        rows = [
            {
                "canonical_player_id": "p1",
                "match_id": "m1",
                "event_type": "Pass",
                "x": None,
                "y": None,
                "event_index": 1,
                "data_source": "statsbomb",
            },
        ]
        result = tokenize_match_events(self._make_df(rows), TokenizerConfig())
        assert ("p1", "m1") not in result


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
            ("p1", "m1"): ["pass_6_4", "shot_11_4", "carry_5_2", "pass_6_4"],
            ("p1", "m2"): ["pass_6_4", "duel_7_5", "interception_4_6", "pass_6_4"],
            ("p2", "m1"): ["shot_11_4", "carry_5_2", "pass_6_4", "shot_11_4"],
            ("p2", "m2"): ["duel_7_5", "pass_6_4", "shot_11_4", "duel_7_5"],
            ("p3", "m1"): ["pass_6_4", "carry_5_2", "foul_3_1", "pass_6_4"],
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
            ("p1", "m1"): ["pass_6_4", "shot_11_4", "carry_5_2", "pass_6_4"],
            ("p1", "m2"): ["pass_6_4", "duel_7_5", "interception_4_6", "pass_6_4"],
            ("p2", "m1"): ["shot_11_4", "carry_5_2", "pass_6_4", "shot_11_4"],
            ("p2", "m2"): ["duel_7_5", "pass_6_4", "shot_11_4", "duel_7_5"],
            ("p3", "m1"): ["pass_6_4", "carry_5_2", "pass_6_4", "carry_5_2"],
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
    """MLflow pyfunc wrapper (tested with mocked mlflow)."""

    def test_predict_returns_dataframe_with_vector_column(self) -> None:
        """Verify predict() shape without requiring mlflow runtime."""
        # Build a real model
        seqs = {
            ("p1", "m1"): ["pass_6_4", "shot_11_4", "carry_5_2", "pass_6_4"],
            ("p1", "m2"): ["pass_6_4", "duel_7_5", "pass_6_4", "duel_7_5"],
            ("p2", "m1"): ["shot_11_4", "carry_5_2", "pass_6_4", "shot_11_4"],
        }
        model = train_model(seqs, TrainingConfig())

        # Import the class — it only references mlflow types in method signatures
        from analytics.football2vec import Football2VecModel

        wrapper = Football2VecModel()
        wrapper.model = model  # type: ignore[attr-defined]
        wrapper.tokenizer_config = TokenizerConfig()

        input_df = pd.DataFrame(
            {
                "tokens": [["pass_6_4", "shot_11_4"], ["duel_7_5", "pass_6_4"]],
            }
        )

        # Call predict with context=None (not used in predict body beyond type sig)
        result = wrapper.predict(None, input_df)  # type: ignore[arg-type]
        assert isinstance(result, pd.DataFrame)
        assert "vector" in result.columns
        assert len(result) == 2
        assert len(result.iloc[0]["vector"]) == 32
