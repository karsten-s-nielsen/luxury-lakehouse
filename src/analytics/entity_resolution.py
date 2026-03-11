"""Cross-source player entity resolution.

Three-layer progressive pipeline inspired by US Soccer's glass_onion (BSD 3-Clause):
  Layer 1 (strict):   Name + DOB + jersey + team (threshold 90%)
  Layer 2 (standard): Name + DOB with month/day swap detection (threshold 80%)
  Layer 3 (relaxed):  Name + position (threshold 75%)

Matched players are removed between layers to prevent false positives from
contaminating lower-confidence layers.

Each layer uses TF-IDF character n-gram candidate generation via sparse_dot_topn,
followed by rapidfuzz multi-attribute scoring with bidirectional validation.

References:
  - USSoccerFederation/glass_onion (BSD 3-Clause) — multi-layer progressive
    strategy, jersey number as constraint, team-scoped matching, DOB swap detection
  - parmacalcio1913/players-matcher (Apache-2.0) — bidirectional validation pattern
  - Pappalardo et al. (2019) Wyscout dataset, StatsBomb open data
"""

from __future__ import annotations

import logging
import re
from dataclasses import dataclass, field, replace
from typing import Any, cast

import numpy as np
import pandas as pd
from rapidfuzz import fuzz
from sklearn.feature_extraction.text import TfidfVectorizer
from sparse_dot_topn import sp_matmul_topn
from unidecode import unidecode

logger = logging.getLogger(__name__)

# Month letters: Jan=A, Feb=B, ..., Dec=L
_MONTH_LETTERS = "ABCDEFGHIJKL"
_CLEAN_RE = re.compile(r"[^\w\s]", re.UNICODE)
_WHITESPACE_RE = re.compile(r"\s+")


# ---------------------------------------------------------------------------
# Name cleaning and DOB encoding
# ---------------------------------------------------------------------------


def normalize_name(name: str | None) -> str:
    """Normalize a player name for matching.

    Applies: unidecode -> lowercase -> strip punctuation -> collapse whitespace.
    """
    if not name:
        return ""
    text = unidecode(str(name))
    text = text.lower()
    text = _CLEAN_RE.sub(" ", text)
    text = _WHITESPACE_RE.sub(" ", text).strip()
    return text


def encode_dob(dob: str | None) -> str:
    """Encode date of birth as a compact 5-char token for TF-IDF.

    Format: DDXYY where X is a letter A-L for month (Jan=A, Dec=L).
    Example: 1993-12-28 -> "28L93"
    """
    if not dob:
        return ""
    try:
        parts = str(dob).split("-")
        if len(parts) != 3:
            return ""
        year, month, day = parts
        month_idx = int(month) - 1
        if not (0 <= month_idx < 12):
            return ""
        return f"{int(day):02d}{_MONTH_LETTERS[month_idx]}{year[-2:]}"
    except (ValueError, IndexError):
        return ""


# ---------------------------------------------------------------------------
# TF-IDF candidate generation
# ---------------------------------------------------------------------------


def _build_ngrams(text: str, n: int = 3) -> list[str]:
    """Generate character n-grams from text (whitespace removed)."""
    text = text.replace(" ", "")
    return [text[i : i + n] for i in range(max(len(text) - n + 1, 0))]


def generate_candidates(
    source_a: pd.DataFrame,
    source_b: pd.DataFrame,
    top_n: int = 3,
    threshold: float = 0.5,
    ngram_size: int = 3,
) -> pd.DataFrame:
    """Generate match candidates using TF-IDF character n-gram cosine similarity.

    Args:
        source_a: DataFrame with ``player_id`` and ``searchable_name`` columns.
        source_b: DataFrame with ``player_id`` and ``searchable_name`` columns.
        top_n: Maximum candidates per source_a player.
        threshold: Minimum cosine similarity to consider a candidate.
        ngram_size: Character n-gram size for TF-IDF.

    Returns:
        DataFrame with columns: player_id_a, player_id_b, tfidf_score.
    """
    all_names = pd.concat(
        [source_a["searchable_name"], source_b["searchable_name"]],
        ignore_index=True,
    )

    vectorizer: TfidfVectorizer = TfidfVectorizer(
        analyzer=lambda text: _build_ngrams(text, ngram_size),  # type: ignore[arg-type]
        dtype=np.float32,  # type: ignore[arg-type]
    )
    vectorizer.fit(all_names)

    matrix_a = cast(Any, vectorizer.transform(source_a["searchable_name"]))
    matrix_b = cast(Any, vectorizer.transform(source_b["searchable_name"]))

    # Sparse top-n cosine similarity
    similarity = sp_matmul_topn(
        matrix_a,
        matrix_b.T,
        top_n=top_n,
        threshold=threshold,
    )

    # Extract non-zero entries as candidate pairs
    coo = similarity.tocoo()
    if len(coo.row) == 0:
        return pd.DataFrame(
            {
                "player_id_a": pd.Series(dtype="int64"),
                "player_id_b": pd.Series(dtype="int64"),
                "tfidf_score": pd.Series(dtype="float64"),
            }
        )

    return pd.DataFrame(
        {
            "player_id_a": source_a["player_id"].iloc[coo.row].values,
            "player_id_b": source_b["player_id"].iloc[coo.col].values,
            "tfidf_score": coo.data,
        }
    )


# ---------------------------------------------------------------------------
# Position group mapping for cross-source compatibility
# ---------------------------------------------------------------------------

_POSITION_GROUP: dict[str, str] = {
    # Wyscout role codes
    "GK": "Goalkeeper",
    "Goalkeeper": "Goalkeeper",
    "DF": "Defender",
    "Defender": "Defender",
    "MF": "Midfielder",
    "Midfielder": "Midfielder",
    "FW": "Forward",
    "Forward": "Forward",
    # StatsBomb position_name keywords
    "Back": "Defender",
    "Wing Back": "Defender",
    "Midfield": "Midfielder",
    "Wing": "Forward",
    "Striker": "Forward",
}


def _map_position_group(position: str | None) -> str:
    """Map a position string to a broad group (Goalkeeper/Defender/Midfielder/Forward)."""
    if not position:
        return ""
    for key, group in _POSITION_GROUP.items():
        if key.lower() in position.lower():
            return group
    return ""


# ---------------------------------------------------------------------------
# Multi-attribute scoring with DOB swap detection
# ---------------------------------------------------------------------------


def match_dob(
    dob_a: str | None,
    dob_b: str | None,
) -> float | None:
    """Compare two dates of birth with tolerance for common data quality issues.

    Inspired by glass_onion's DOB matching layers.

    Returns:
        100.0 for exact match, 90.0 for +/-1 day, 80.0 for month/day swap,
        0.0 for complete mismatch, None if either DOB is missing.
    """
    if not dob_a or not dob_b:
        return None
    a, b = str(dob_a).strip(), str(dob_b).strip()
    if a == b:
        return 100.0
    try:
        ya, ma, da = a.split("-")
        yb, mb, db = b.split("-")
    except ValueError:
        return 0.0
    # Same year — check +/-1 day tolerance (timezone/recording differences)
    if ya == yb and ma == mb:
        try:
            if abs(int(da) - int(db)) <= 1:
                return 90.0
        except ValueError:
            pass
    # Month/day swap detection (MM-DD vs DD-MM format confusion)
    if ya == yb and ma == db and da == mb:
        return 80.0
    return 0.0


def score_candidate(
    name_a: str,
    name_b: str,
    dob_a: str | None = None,
    dob_b: str | None = None,
    position_a: str | None = None,
    position_b: str | None = None,
    jersey_a: str | None = None,
    jersey_b: str | None = None,
    short_name_a: str | None = None,
    short_name_b: str | None = None,
) -> float:
    """Score a candidate match using multiple attributes.

    Weights: name similarity 60%, DOB match 25%, position match 15%.
    Missing attributes are excluded and weights redistributed.
    Jersey number match is a bonus (not penalized on mismatch, since
    players change numbers across seasons/teams).

    When short_name is available, the best of (full vs full, full vs short,
    short vs full) is used for name similarity. This handles the common
    real-data pattern where one source has "Marco Verratti" and another has
    "M. Verratti".

    Returns:
        Score 0-100.
    """
    # Name similarity — best of available name combinations
    name_scores = [fuzz.token_sort_ratio(name_a, name_b)]
    if short_name_a:
        name_scores.append(fuzz.token_sort_ratio(short_name_a, name_b))
        if short_name_b:
            name_scores.append(fuzz.token_sort_ratio(short_name_a, short_name_b))
    if short_name_b:
        name_scores.append(fuzz.token_sort_ratio(name_a, short_name_b))
    name_score = max(name_scores)

    weights: list[tuple[float, float]] = [(0.60, name_score)]

    # DOB match with swap detection
    dob_result = match_dob(dob_a, dob_b)
    if dob_result is not None:
        weights.append((0.25, dob_result))

    # Position group match
    group_a = _map_position_group(position_a)
    group_b = _map_position_group(position_b)
    if group_a and group_b:
        pos_score = 100.0 if group_a == group_b else 25.0
        weights.append((0.15, pos_score))

    # Redistribute weights
    total_weight = sum(w for w, _ in weights)
    base_score = sum((w / total_weight) * s for w, s in weights)

    # Jersey number: bonus only (no penalty for mismatch — numbers change)
    if jersey_a and jersey_b and str(jersey_a) == str(jersey_b):
        base_score = min(100.0, base_score + 3.0)

    return base_score


# ---------------------------------------------------------------------------
# Bidirectional validation
# ---------------------------------------------------------------------------


def validate_bidirectional(
    forward: pd.DataFrame,
    reverse: pd.DataFrame,
) -> pd.DataFrame:
    """Keep only mutual best matches (bidirectional validation).

    Args:
        forward: A->B matches with columns player_id_a, player_id_b, score.
        reverse: B->A matches with columns player_id_a, player_id_b, score.

    Returns:
        DataFrame of mutual matches (player_id_a from forward perspective).
    """
    # Best match per player in forward direction
    fwd_best = forward.loc[forward.groupby("player_id_a")["score"].idxmax()]
    # Best match per player in reverse direction
    rev_best = reverse.loc[reverse.groupby("player_id_a")["score"].idxmax()]

    # Build lookup: in reverse, player_id_a is the B-side player
    rev_lookup: dict[Any, Any] = dict(zip(rev_best["player_id_a"], rev_best["player_id_b"], strict=False))

    # Keep forward match (a->b) only if reverse best match for b is a
    mutual_mask = fwd_best.apply(
        lambda row: rev_lookup.get(row["player_id_b"]) == row["player_id_a"],
        axis=1,
    )
    return cast(pd.DataFrame, fwd_best[mutual_mask].reset_index(drop=True))


# ---------------------------------------------------------------------------
# Three-layer progressive pipeline
# ---------------------------------------------------------------------------


@dataclass
class LayerConfig:
    """Configuration for a single matching layer."""

    threshold: float
    use_dob: bool = True
    use_jersey: bool = False
    use_team: bool = False
    use_position: bool = True
    tfidf_threshold: float = 0.3


@dataclass
class ResolutionConfig:
    """Configuration for the three-layer entity resolution pipeline."""

    tfidf_top_n: int = 5
    ngram_size: int = 3
    confidence_threshold: float = 70.0
    layers: list[LayerConfig] = field(
        default_factory=lambda: [
            LayerConfig(threshold=90.0, use_dob=True, use_jersey=True, use_team=True, tfidf_threshold=0.4),
            LayerConfig(threshold=80.0, use_dob=True, use_jersey=False, use_team=False, tfidf_threshold=0.3),
            LayerConfig(threshold=75.0, use_dob=False, use_jersey=False, use_team=False, tfidf_threshold=0.25),
        ]
    )


def _prepare_source(
    df: pd.DataFrame,
    name_col: str = "player_name",
    short_name_col: str = "short_name",
    dob_col: str = "birth_date",
) -> pd.DataFrame:
    """Prepare a source DataFrame for matching.

    Adds ``searchable_name`` column (normalized name + optional short_name
    + encoded DOB). When ``short_name`` is available it is appended so
    TF-IDF can match abbreviated variants (e.g. "M. Verratti") against
    full names. Gracefully degrades when short_name is absent.
    """
    result = df.copy()
    result["_normalized_name"] = result[name_col].apply(normalize_name)

    # Short name provides an abbreviated variant for better TF-IDF recall
    if short_name_col in result.columns:
        result["_normalized_short"] = result[short_name_col].apply(normalize_name)
    else:
        result["_normalized_short"] = ""

    if dob_col in result.columns:
        result["_encoded_dob"] = result[dob_col].apply(encode_dob)
    else:
        result["_encoded_dob"] = ""

    # Combine: "full_name short_name dob_token"
    result["searchable_name"] = (
        result["_normalized_name"] + " " + result["_normalized_short"] + " " + result["_encoded_dob"]
    ).str.strip()

    # Ensure optional columns exist for uniform access
    for col in ("birth_date", "position", "jersey_number", "team_name", "short_name"):
        if col not in result.columns:
            result[col] = None
    return result


def _run_layer(
    prep_a: pd.DataFrame,
    prep_b: pd.DataFrame,
    layer: LayerConfig,
    tfidf_top_n: int,
    ngram_size: int,
) -> pd.DataFrame:
    """Run a single matching layer: TF-IDF -> score -> bidirectional validate.

    Returns DataFrame with player_id_a, player_id_b, confidence.
    """
    _empty = pd.DataFrame(
        {
            "player_id_a": pd.Series(dtype="int64"),
            "player_id_b": pd.Series(dtype="int64"),
            "confidence": pd.Series(dtype="float64"),
        }
    )

    # Filter by team context if layer requires it
    if layer.use_team:
        a_teams = prep_a["team_name"].apply(lambda t: normalize_name(t) if pd.notna(t) else "")
        b_teams = prep_b["team_name"].apply(lambda t: normalize_name(t) if pd.notna(t) else "")
        common_teams = list(set(a_teams[a_teams != ""]) & set(b_teams[b_teams != ""]))
        if not common_teams:
            return _empty
        layer_a = cast(pd.DataFrame, prep_a[a_teams.isin(common_teams)])
        layer_b = cast(pd.DataFrame, prep_b[b_teams.isin(common_teams)])
    else:
        layer_a = prep_a
        layer_b = prep_b

    if layer_a.empty or layer_b.empty:
        return _empty

    # TF-IDF candidate generation
    _cols = ["player_id", "searchable_name"]
    candidates = generate_candidates(
        cast(pd.DataFrame, layer_a[_cols]),
        cast(pd.DataFrame, layer_b[_cols]),
        top_n=tfidf_top_n,
        threshold=layer.tfidf_threshold,
        ngram_size=ngram_size,
    )
    if candidates.empty:
        return _empty

    a_lookup = layer_a.set_index("player_id")
    b_lookup = layer_b.set_index("player_id")

    def _score_pair(pid_a: Any, pid_b: Any) -> float:
        rec_a = a_lookup.loc[pid_a]
        rec_b = b_lookup.loc[pid_b]
        short_a = rec_a.get("_normalized_short", "")
        short_b = rec_b.get("_normalized_short", "")
        return score_candidate(
            name_a=rec_a.get("_normalized_name", ""),
            name_b=rec_b.get("_normalized_name", ""),
            dob_a=rec_a.get("birth_date") if layer.use_dob else None,
            dob_b=rec_b.get("birth_date") if layer.use_dob else None,
            position_a=rec_a.get("position") if layer.use_position else None,
            position_b=rec_b.get("position") if layer.use_position else None,
            jersey_a=rec_a.get("jersey_number") if layer.use_jersey else None,
            jersey_b=rec_b.get("jersey_number") if layer.use_jersey else None,
            short_name_a=short_a if short_a else None,
            short_name_b=short_b if short_b else None,
        )

    # Forward scoring (A->B)
    scores_fwd = [
        {
            "player_id_a": pid_a,
            "player_id_b": pid_b,
            "score": _score_pair(pid_a, pid_b),
        }
        for pid_a, pid_b in zip(candidates["player_id_a"], candidates["player_id_b"], strict=True)
    ]
    forward = pd.DataFrame(scores_fwd)

    # Reverse scoring (B->A)
    candidates_rev = generate_candidates(
        cast(pd.DataFrame, layer_b[_cols]),
        cast(pd.DataFrame, layer_a[_cols]),
        top_n=tfidf_top_n,
        threshold=layer.tfidf_threshold,
        ngram_size=ngram_size,
    )
    if candidates_rev.empty:
        return _empty

    scores_rev = [
        {
            "player_id_a": pid_a,
            "player_id_b": pid_b,
            "score": _score_pair(pid_b, pid_a),
        }
        for pid_a, pid_b in zip(candidates_rev["player_id_a"], candidates_rev["player_id_b"], strict=True)
    ]
    reverse = pd.DataFrame(scores_rev)

    # Bidirectional validation + threshold
    mutual = validate_bidirectional(forward, reverse)
    result = cast(pd.DataFrame, mutual[mutual["score"] >= layer.threshold].copy())
    return cast(pd.DataFrame, result.rename(columns={"score": "confidence"}))


def resolve_players(
    source_a: pd.DataFrame,
    source_b: pd.DataFrame,
    config: ResolutionConfig | None = None,
    confidence_threshold: float | None = None,
) -> pd.DataFrame:
    """Run the three-layer progressive entity resolution pipeline.

    Layers run strict->permissive. Matched players are removed before each
    subsequent layer to prevent false positives from contaminating
    lower-confidence layers. Inspired by glass_onion's multi-layer strategy.

    Args:
        source_a: DataFrame with player_id, player_name, and optional:
            birth_date, position, jersey_number, team_name.
        source_b: Same schema as source_a.
        config: Pipeline configuration.
        confidence_threshold: Shorthand to override config.confidence_threshold.

    Returns:
        DataFrame with columns: player_id_a, player_id_b, confidence,
        match_method, match_layer.
    """
    if config is None:
        config = ResolutionConfig()
    if confidence_threshold is not None:
        config = replace(config, confidence_threshold=confidence_threshold)

    prep_a = _prepare_source(source_a)
    prep_b = _prepare_source(source_b)

    _result_cols = ["player_id_a", "player_id_b", "confidence", "match_method", "match_layer"]
    all_matches: list[pd.DataFrame] = []
    matched_a: list[int] = []
    matched_b: list[int] = []

    for layer_num, layer in enumerate(config.layers, start=1):
        # Remove already-matched players
        remaining_a = cast(pd.DataFrame, prep_a[~prep_a["player_id"].isin(matched_a)])
        remaining_b = cast(pd.DataFrame, prep_b[~prep_b["player_id"].isin(matched_b)])

        if remaining_a.empty or remaining_b.empty:
            break

        layer_result = _run_layer(
            remaining_a,
            remaining_b,
            layer,
            tfidf_top_n=config.tfidf_top_n,
            ngram_size=config.ngram_size,
        )

        if not layer_result.empty:
            layer_result["match_layer"] = layer_num
            layer_result["match_method"] = f"layer{layer_num}_tfidf_rapidfuzz_bidirectional"
            all_matches.append(layer_result)
            matched_a.extend(layer_result["player_id_a"].tolist())
            matched_b.extend(layer_result["player_id_b"].tolist())

        logger.info(
            "Layer %d: %d matches (threshold=%.0f, team=%s, jersey=%s, dob=%s)",
            layer_num,
            len(layer_result),
            layer.threshold,
            layer.use_team,
            layer.use_jersey,
            layer.use_dob,
        )

    if not all_matches:
        logger.info("No cross-source matches found across all layers")
        _col_dtypes = {
            "player_id_a": "int64",
            "player_id_b": "int64",
            "confidence": "float64",
            "match_method": "object",
            "match_layer": "int64",
        }
        return pd.DataFrame({c: pd.Series(dtype=_col_dtypes.get(c, "object")) for c in _result_cols})

    result = cast(pd.DataFrame, pd.concat(all_matches, ignore_index=True))

    # Apply global confidence threshold (may filter some Layer 3 matches)
    result = cast(pd.DataFrame, result[result["confidence"] >= config.confidence_threshold])

    logger.info(
        "Entity resolution complete: %d total matches (L1=%d, L2=%d, L3=%d)",
        len(result),
        len(result[result["match_layer"] == 1]),
        len(result[result["match_layer"] == 2]),
        len(result[result["match_layer"] == 3]),
    )

    return cast(pd.DataFrame, result[_result_cols])
