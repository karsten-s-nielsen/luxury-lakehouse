"""Tests for cross-source player entity resolution."""

from __future__ import annotations

import pandas as pd

from analytics.entity_resolution import (
    encode_dob,
    generate_candidates,
    match_dob,
    normalize_name,
    resolve_players,
    score_candidate,
    validate_bidirectional,
)

# ---------------------------------------------------------------------------
# Name normalization
# ---------------------------------------------------------------------------


class TestNormalizeName:
    """Name cleaning via unidecode + punctuation strip."""

    def test_accented_characters(self) -> None:
        assert normalize_name("Bruno Guimarães") == "bruno guimaraes"

    def test_diacritics(self) -> None:
        assert normalize_name("Sørloth") == "sorloth"

    def test_punctuation_stripped(self) -> None:
        # Apostrophe and hyphen replaced by space, then collapsed
        assert normalize_name("O'Brien-Smith") == "o brien smith"

    def test_extra_whitespace(self) -> None:
        assert normalize_name("  Harry   Kane  ") == "harry kane"

    def test_empty_string(self) -> None:
        assert normalize_name("") == ""

    def test_none_returns_empty(self) -> None:
        assert normalize_name(None) == ""

    def test_turkish_characters(self) -> None:
        assert normalize_name("Çalhanoğlu") == "calhanoglu"

    def test_arabic_transliteration(self) -> None:
        # Mohamed Salah — common variant spellings
        assert normalize_name("Mohamed Ṣalāḥ") == "mohamed salah"

    def test_hyphenated_name(self) -> None:
        assert normalize_name("Pierre-Emerick Aubameyang") == "pierre emerick aubameyang"


# ---------------------------------------------------------------------------
# DOB encoding
# ---------------------------------------------------------------------------


class TestEncodeDob:
    """DOB encoding for TF-IDF soft signal."""

    def test_valid_date(self) -> None:
        assert encode_dob("1993-12-28") == "28L93"

    def test_january(self) -> None:
        assert encode_dob("1990-01-15") == "15A90"

    def test_none_returns_empty(self) -> None:
        assert encode_dob(None) == ""

    def test_invalid_format(self) -> None:
        assert encode_dob("not-a-date") == ""

    def test_february(self) -> None:
        assert encode_dob("1987-02-05") == "05B87"

    def test_single_digit_day(self) -> None:
        # Day should be zero-padded
        assert encode_dob("1990-06-03") == "03F90"


# ---------------------------------------------------------------------------
# DOB matching with swap detection
# ---------------------------------------------------------------------------


class TestDobMatch:
    """DOB comparison with month/day swap detection (glass_onion insight)."""

    def test_exact_match(self) -> None:
        assert match_dob("1993-07-28", "1993-07-28") == 100.0

    def test_off_by_one_day(self) -> None:
        score = match_dob("1993-07-28", "1993-07-29")
        assert score == 90.0

    def test_off_by_one_day_earlier(self) -> None:
        score = match_dob("1993-07-28", "1993-07-27")
        assert score == 90.0

    def test_month_day_swap(self) -> None:
        # MM-DD vs DD-MM format confusion
        score = match_dob("1993-03-07", "1993-07-03")
        assert score == 80.0

    def test_completely_different(self) -> None:
        assert match_dob("1993-07-28", "2003-07-28") == 0.0

    def test_missing_dob_a(self) -> None:
        assert match_dob(None, "1993-07-28") is None

    def test_missing_dob_b(self) -> None:
        assert match_dob("1993-07-28", None) is None

    def test_both_missing(self) -> None:
        assert match_dob(None, None) is None

    def test_empty_string(self) -> None:
        assert match_dob("", "1993-07-28") is None

    def test_different_month_same_day(self) -> None:
        # Not a swap — genuinely different
        assert match_dob("1993-03-28", "1993-07-28") == 0.0


# ---------------------------------------------------------------------------
# TF-IDF candidate generation
# ---------------------------------------------------------------------------


class TestTfidfCandidateGeneration:
    """TF-IDF + sparse_dot_topn candidate generation."""

    def test_exact_match_returns_high_similarity(self) -> None:
        source_a = pd.DataFrame(
            {
                "player_id": [1],
                "searchable_name": ["harry kane 28G97"],
            }
        )
        source_b = pd.DataFrame(
            {
                "player_id": [101],
                "searchable_name": ["harry kane 28G97"],
            }
        )
        candidates = generate_candidates(source_a, source_b, top_n=1, threshold=0.5)
        assert len(candidates) == 1
        assert candidates.iloc[0]["player_id_a"] == 1
        assert candidates.iloc[0]["player_id_b"] == 101
        assert candidates.iloc[0]["tfidf_score"] > 0.9

    def test_no_match_below_threshold(self) -> None:
        source_a = pd.DataFrame(
            {
                "player_id": [1],
                "searchable_name": ["harry kane 28G97"],
            }
        )
        source_b = pd.DataFrame(
            {
                "player_id": [101],
                "searchable_name": ["completely different name 01A80"],
            }
        )
        candidates = generate_candidates(source_a, source_b, top_n=1, threshold=0.5)
        assert len(candidates) == 0

    def test_returns_top_n_candidates(self) -> None:
        source_a = pd.DataFrame(
            {
                "player_id": [1],
                "searchable_name": ["harry kane"],
            }
        )
        source_b = pd.DataFrame(
            {
                "player_id": [101, 102, 103],
                "searchable_name": ["harry kane", "harry kean", "john smith"],
            }
        )
        candidates = generate_candidates(source_a, source_b, top_n=2, threshold=0.3)
        assert len(candidates) <= 2
        assert all(candidates["player_id_a"] == 1)

    def test_multiple_source_a_players(self) -> None:
        source_a = pd.DataFrame(
            {
                "player_id": [1, 2],
                "searchable_name": ["harry kane", "lionel messi"],
            }
        )
        source_b = pd.DataFrame(
            {
                "player_id": [101, 102],
                "searchable_name": ["harry kane", "lionel messi"],
            }
        )
        candidates = generate_candidates(source_a, source_b, top_n=1, threshold=0.5)
        assert len(candidates) == 2


# ---------------------------------------------------------------------------
# Multi-attribute scoring
# ---------------------------------------------------------------------------


class TestMultiAttributeScoring:
    """Multi-attribute scoring: name + DOB + position + jersey + team."""

    def test_exact_name_high_score(self) -> None:
        score = score_candidate(
            name_a="harry kane",
            name_b="harry kane",
            dob_a="1993-07-28",
            dob_b="1993-07-28",
            position_a="Forward",
            position_b="Forward",
        )
        assert score > 95

    def test_dob_mismatch_penalizes(self) -> None:
        score_match = score_candidate(
            name_a="harry kane",
            name_b="harry kane",
            dob_a="1993-07-28",
            dob_b="1993-07-28",
            position_a="Forward",
            position_b="Forward",
        )
        score_mismatch = score_candidate(
            name_a="harry kane",
            name_b="harry kane",
            dob_a="1993-07-28",
            dob_b="2003-07-28",
            position_a="Forward",
            position_b="Forward",
        )
        assert score_match > score_mismatch

    def test_dob_month_day_swap_partial_credit(self) -> None:
        score_exact = score_candidate(
            name_a="harry kane",
            name_b="harry kane",
            dob_a="1993-07-28",
            dob_b="1993-07-28",
            position_a="Forward",
            position_b="Forward",
        )
        score_swap = score_candidate(
            name_a="harry kane",
            name_b="harry kane",
            dob_a="1993-03-07",
            dob_b="1993-07-03",
            position_a="Forward",
            position_b="Forward",
        )
        score_mismatch = score_candidate(
            name_a="harry kane",
            name_b="harry kane",
            dob_a="1993-07-28",
            dob_b="2003-07-28",
            position_a="Forward",
            position_b="Forward",
        )
        assert score_exact > score_swap > score_mismatch

    def test_missing_dob_no_penalty(self) -> None:
        score = score_candidate(
            name_a="harry kane",
            name_b="harry kane",
            dob_a="1993-07-28",
            dob_b=None,
            position_a="Forward",
            position_b="Forward",
        )
        assert score > 70

    def test_jersey_number_exact_boost(self) -> None:
        score_with = score_candidate(
            name_a="harry kane",
            name_b="harry kane",
            jersey_a="10",
            jersey_b="10",
        )
        score_without = score_candidate(
            name_a="harry kane",
            name_b="harry kane",
        )
        assert score_with >= score_without

    def test_jersey_number_mismatch_no_penalty(self) -> None:
        score = score_candidate(
            name_a="harry kane",
            name_b="harry kane",
            jersey_a="10",
            jersey_b="7",
        )
        assert score > 70

    def test_short_name_boosts_abbreviated_match(self) -> None:
        """When short_name is available, abbreviated names score higher."""
        score_without_short = score_candidate(
            name_a="marco verratti",
            name_b="m verratti",
        )
        score_with_short = score_candidate(
            name_a="marco verratti",
            name_b="marco verratti",
            short_name_b="m verratti",
        )
        assert score_with_short > score_without_short

    def test_cross_source_position_codes(self) -> None:
        """Wyscout 'FW' should match StatsBomb 'Forward'."""
        score = score_candidate(
            name_a="harry kane",
            name_b="harry kane",
            position_a="Forward",
            position_b="FW",
        )
        assert score > 90

    def test_position_mismatch_reduces_score(self) -> None:
        score_match = score_candidate(
            name_a="harry kane",
            name_b="harry kane",
            position_a="Forward",
            position_b="Forward",
        )
        score_mismatch = score_candidate(
            name_a="harry kane",
            name_b="harry kane",
            position_a="Forward",
            position_b="Goalkeeper",
        )
        assert score_match > score_mismatch


# ---------------------------------------------------------------------------
# Bidirectional validation
# ---------------------------------------------------------------------------


class TestBidirectionalValidation:
    """Bidirectional mutual best-match filtering."""

    def test_mutual_match_kept(self) -> None:
        forward = pd.DataFrame(
            {
                "player_id_a": [1, 2],
                "player_id_b": [101, 102],
                "score": [95.0, 90.0],
            }
        )
        reverse = pd.DataFrame(
            {
                "player_id_a": [101, 102],
                "player_id_b": [1, 2],
                "score": [95.0, 90.0],
            }
        )
        result = validate_bidirectional(forward, reverse)
        assert len(result) == 2

    def test_non_mutual_match_rejected(self) -> None:
        forward = pd.DataFrame(
            {
                "player_id_a": [1],
                "player_id_b": [101],
                "score": [80.0],
            }
        )
        reverse = pd.DataFrame(
            {
                "player_id_a": [101],
                "player_id_b": [2],
                "score": [85.0],
            }
        )
        result = validate_bidirectional(forward, reverse)
        assert len(result) == 0


# ---------------------------------------------------------------------------
# Three-layer progressive pipeline (end-to-end)
# ---------------------------------------------------------------------------


class TestResolvePlayers:
    """End-to-end three-layer entity resolution pipeline."""

    def test_resolves_matching_players(self) -> None:
        statsbomb = pd.DataFrame(
            {
                "player_id": [1, 2, 3],
                "player_name": ["Harry Kane", "Lionel Messi", "Unique Player"],
                "birth_date": ["1993-07-28", "1987-06-24", "2000-01-01"],
                "position": ["Forward", "Forward", "Midfielder"],
            }
        )
        wyscout = pd.DataFrame(
            {
                "player_id": [101, 102, 103],
                "player_name": ["H. Kane", "L. Messi", "Different Person"],
                "birth_date": ["1993-07-28", "1987-06-24", "1995-05-05"],
                "position": ["FW", "FW", "DF"],
            }
        )
        result = resolve_players(statsbomb, wyscout, confidence_threshold=50.0)
        assert len(result) >= 2
        kane_match = result[result["player_id_a"] == 1]
        assert len(kane_match) == 1
        assert kane_match.iloc[0]["player_id_b"] == 101
        assert kane_match.iloc[0]["confidence"] > 50

    def test_layer1_strict_with_team_context(self) -> None:
        # Real data: Wyscout provides shortName alongside full name
        statsbomb = pd.DataFrame(
            {
                "player_id": [1],
                "player_name": ["Harry Kane"],
                "birth_date": ["1993-07-28"],
                "position": ["Forward"],
                "jersey_number": ["10"],
                "team_name": ["Tottenham Hotspur"],
            }
        )
        wyscout = pd.DataFrame(
            {
                "player_id": [101],
                "player_name": ["Harry Kane"],
                "short_name": ["H. Kane"],
                "birth_date": ["1993-07-28"],
                "position": ["FW"],
                "jersey_number": ["10"],
                "team_name": ["Tottenham Hotspur"],
            }
        )
        result = resolve_players(statsbomb, wyscout, confidence_threshold=50.0)
        assert len(result) == 1
        assert result.iloc[0]["confidence"] > 90
        assert result.iloc[0]["match_layer"] == 1

    def test_layer2_name_dob_without_team(self) -> None:
        # Real data: Wyscout has full name + short_name, no team overlap
        statsbomb = pd.DataFrame(
            {
                "player_id": [1],
                "player_name": ["Lionel Messi"],
                "birth_date": ["1987-06-24"],
                "position": ["Forward"],
            }
        )
        wyscout = pd.DataFrame(
            {
                "player_id": [101],
                "player_name": ["Lionel Messi"],
                "short_name": ["L. Messi"],
                "birth_date": ["1987-06-24"],
                "position": ["FW"],
            }
        )
        result = resolve_players(statsbomb, wyscout, confidence_threshold=50.0)
        assert len(result) == 1
        assert result.iloc[0]["match_layer"] == 2

    def test_layer3_name_only_relaxed(self) -> None:
        # Layer 3: no DOB available, name similarity between L2 (80%) and L3 (75%)
        # Use a name variant that's close but not close enough for Layer 2's 80% threshold
        # but passes Layer 3's 75% with position match.
        # Layer 2 uses DOB — with no DOB, name-only score must exceed 80%.
        # "Marco Verratti" vs "Marco Verratti" would match at L2 (100% name).
        # Instead, test that Layer 3 catches players where L1+L2 fail.
        from analytics.entity_resolution import LayerConfig, ResolutionConfig

        config = ResolutionConfig(
            confidence_threshold=50.0,
            layers=[
                # Skip L1 and L2 entirely — only run L3
                LayerConfig(threshold=75.0, use_dob=False, use_jersey=False, use_team=False, tfidf_threshold=0.25),
            ],
        )
        statsbomb = pd.DataFrame(
            {
                "player_id": [1],
                "player_name": ["Marco Verratti"],
                "position": ["Midfielder"],
            }
        )
        wyscout = pd.DataFrame(
            {
                "player_id": [101],
                "player_name": ["Marco Verratti"],
                "short_name": ["M. Verratti"],
                "position": ["MF"],
            }
        )
        result = resolve_players(statsbomb, wyscout, config=config)
        assert len(result) == 1
        assert result.iloc[0]["match_layer"] == 1  # First (only) layer in this config

    def test_layer3_degrades_without_shortname(self) -> None:
        # Without short_name, abbreviated names may still match via token overlap
        statsbomb = pd.DataFrame(
            {
                "player_id": [1],
                "player_name": ["Marco Verratti"],
                "position": ["Midfielder"],
            }
        )
        wyscout = pd.DataFrame(
            {
                "player_id": [101],
                "player_name": ["M. Verratti"],
                "position": ["MF"],
            }
        )
        # Without shortname, abbreviated "M." vs "Marco" has lower similarity
        # but should still generate a candidate (TF-IDF catches "verratti")
        result = resolve_players(statsbomb, wyscout, confidence_threshold=50.0)
        # May or may not match depending on score — just verify no crash
        assert isinstance(result, pd.DataFrame)

    def test_matched_players_removed_between_layers(self) -> None:
        statsbomb = pd.DataFrame(
            {
                "player_id": [1, 2],
                "player_name": ["Harry Kane", "Harry Kean"],
                "birth_date": ["1993-07-28", "1993-07-28"],
                "position": ["Forward", "Forward"],
                "jersey_number": ["10", "9"],
                "team_name": ["Tottenham Hotspur", "Tottenham Hotspur"],
            }
        )
        wyscout = pd.DataFrame(
            {
                "player_id": [101],
                "player_name": ["H. Kane"],
                "birth_date": ["1993-07-28"],
                "position": ["FW"],
                "jersey_number": ["10"],
                "team_name": ["Tottenham Hotspur"],
            }
        )
        result = resolve_players(statsbomb, wyscout, confidence_threshold=50.0)
        # Only player 1 should match (jersey 10), not player 2
        assert len(result) == 1
        assert result.iloc[0]["player_id_a"] == 1

    def test_empty_sources_return_empty(self) -> None:
        empty = pd.DataFrame({"player_id": pd.Series(dtype="int"), "player_name": pd.Series(dtype="str")})
        nonempty = pd.DataFrame(
            {
                "player_id": [1],
                "player_name": ["Test Player"],
            }
        )
        result = resolve_players(empty, nonempty, confidence_threshold=50.0)
        assert len(result) == 0

    def test_no_matches_returns_empty(self) -> None:
        source_a = pd.DataFrame(
            {
                "player_id": [1],
                "player_name": ["Aaaa Bbbb"],
                "position": ["Forward"],
            }
        )
        source_b = pd.DataFrame(
            {
                "player_id": [101],
                "player_name": ["Xxxx Yyyy"],
                "position": ["Goalkeeper"],
            }
        )
        result = resolve_players(source_a, source_b, confidence_threshold=90.0)
        assert len(result) == 0

    def test_result_has_expected_columns(self) -> None:
        statsbomb = pd.DataFrame(
            {
                "player_id": [1],
                "player_name": ["Harry Kane"],
                "birth_date": ["1993-07-28"],
                "position": ["Forward"],
            }
        )
        wyscout = pd.DataFrame(
            {
                "player_id": [101],
                "player_name": ["H. Kane"],
                "birth_date": ["1993-07-28"],
                "position": ["FW"],
            }
        )
        result = resolve_players(statsbomb, wyscout, confidence_threshold=50.0)
        expected_cols = {"player_id_a", "player_id_b", "confidence", "match_method", "match_layer"}
        assert expected_cols == set(result.columns)
