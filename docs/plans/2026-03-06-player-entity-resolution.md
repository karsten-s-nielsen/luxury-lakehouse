# Phase 14: Cross-Source Player Entity Resolution — Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Resolve player identity across StatsBomb and Wyscout (10,803 + 3,603 players) so `dim_players` has one canonical row per real-world player, enabling cross-source stat aggregation and Phase 15 embeddings.

**Architecture:** Three-layer progressive entity resolution pipeline inspired by US Soccer's [glass_onion](https://github.com/USSoccerFederation/glass_onion) (BSD 3-Clause). Each layer uses TF-IDF character n-gram candidate generation via `sparse_dot_topn` (scales to 500K+), followed by `rapidfuzz` multi-attribute scoring with bidirectional validation. Layers run strict→permissive: Layer 1 (name + DOB + jersey + team, 90% threshold), Layer 2 (name + DOB with month/day swap detection, 80%), Layer 3 (name + position, 75%). Matched players are removed before each subsequent layer. Wyscout player metadata ingestion is a prerequisite (not currently downloaded). The matching runs as a Python analytics + ingestion module; results flow through dbt `int_player_xref` into a refactored `dim_players` with `canonical_player_id`.

**Tech Stack:** `rapidfuzz`, `unidecode`, `sparse-dot-topn`, `scikit-learn` (already installed), dbt (existing), pytest (existing)

**Key references:**
- [glass_onion](https://github.com/USSoccerFederation/glass_onion) (BSD 3-Clause) — multi-layer progressive strategy, jersey number as constraint, team-scoped matching, DOB month/day swap detection
- [players-matcher](https://github.com/parmacalcio1913/players-matcher) (Apache-2.0) — bidirectional mutual best-match validation pattern

---

## Task 1: Add Dependencies

**Files:**
- Modify: `pyproject.toml:17-30` (analytics optional-dependencies)

**Step 1: Add rapidfuzz, unidecode, sparse-dot-topn to analytics dependencies**

In `pyproject.toml`, add to the `[project.optional-dependencies] analytics` list:

```toml
analytics = [
    "pandas>=2.1.0",
    "numpy>=1.26.0",
    "mplsoccer>=1.1.3",
    "matplotlib>=3.8.0",
    "scipy>=1.11.0",
    "scikit-learn>=1.3.0",
    "socceraction==1.5.3",
    "xgboost==3.2.0",
    "multimethod==1.12",
    "rapidfuzz>=3.6.0",
    "unidecode>=1.3.0",
    "sparse-dot-topn>=1.1.0",
]
```

**Step 2: Install and verify**

Run: `uv sync --extra analytics --extra dev`
Expected: Clean install, no conflicts.

**Step 3: Verify imports**

Run: `python -c "import rapidfuzz; import unidecode; import sparse_dot_topn; print('OK')"`
Expected: `OK`

**Step 4: Commit**

```bash
git add pyproject.toml uv.lock
git commit -m "chore(deps): add rapidfuzz, unidecode, sparse-dot-topn for entity resolution"
```

---

## Task 2: Extend Wyscout Ingestion — Download players.json

**Files:**
- Modify: `src/ingestion/wyscout.py`
- Test: `src/tests/test_wyscout.py`

**Context:** The Figshare Wyscout dataset includes `players.json` (3,603 players with `firstName`, `lastName`, `shortName`, `birthDate`, `role`, `wyId`, `passportArea`). Currently only `events.json` and `matches.json` are downloaded. The `players.json` is a flat JSON array (not ZIP-wrapped), downloaded from `https://ndownloader.figshare.com/files/15073721`.

**Step 1: Write the failing test**

In `src/tests/test_wyscout.py`, add:

```python
class TestIngestPlayers:
    """Tests for Wyscout player metadata ingestion."""

    def test_loads_players_from_local_json(self, tmp_path: pathlib.Path) -> None:
        """Local players.json loads correctly."""
        players = [
            {
                "wyId": 32777,
                "firstName": "Harun",
                "middleName": "",
                "lastName": "Tekin",
                "shortName": "H. Tekin",
                "birthDate": "1989-06-17",
                "role": {"code2": "GK", "code3": "GKP", "name": "Goalkeeper"},
                "currentTeamId": 4502,
                "foot": "right",
                "height": 187,
                "weight": 78,
                "passportArea": {"name": "Turkey", "id": "792", "alpha3code": "TUR", "alpha2code": "TR"},
                "birthArea": {"name": "Turkey", "id": "792", "alpha3code": "TUR", "alpha2code": "TR"},
                "currentNationalTeamId": 4687,
            }
        ]
        players_path = tmp_path / "players.json"
        players_path.write_text(json.dumps(players))

        from ingestion.wyscout import _load_players

        df = _load_players(tmp_path, logging.getLogger("test"))
        assert len(df) == 1
        assert df["wyId"].iloc[0] == 32777
        assert df["firstName"].iloc[0] == "Harun"
        assert df["lastName"].iloc[0] == "Tekin"
        assert df["birthDate"].iloc[0] == "1989-06-17"
        # role should be serialized to JSON string
        assert isinstance(df["role"].iloc[0], str)

    def test_loads_players_validates_required_columns(self, tmp_path: pathlib.Path) -> None:
        """Players with missing wyId are rejected."""
        players = [{"firstName": "No", "lastName": "Id"}]
        players_path = tmp_path / "players.json"
        players_path.write_text(json.dumps(players))

        from ingestion.wyscout import _load_players

        with pytest.raises(KeyError):
            _load_players(tmp_path, logging.getLogger("test"))
```

**Step 2: Run test to verify it fails**

Run: `uv run pytest src/tests/test_wyscout.py::TestIngestPlayers -v`
Expected: FAIL with `ImportError` (function not yet defined)

**Step 3: Implement `_load_players` and `ingest_players` in wyscout.py**

Add after line 48 (URL constants):

```python
_PLAYERS_URL = "https://ndownloader.figshare.com/files/15073721"
```

Add new function after `_load_all_competitions` (~line 167):

```python
def _load_players(
    data_dir: pathlib.Path | None,
    logger: logging.Logger,
) -> pd.DataFrame:
    """Load Wyscout player metadata from local file or Figshare download.

    Returns a DataFrame with one row per player. JSON columns (role,
    passportArea, birthArea) are serialized to strings for Spark/Delta.
    """
    # Try local first
    if data_dir is not None:
        local_path = data_dir / "players.json"
        if local_path.exists():
            logger.info("Loading local players file: %s", local_path)
            df = pd.read_json(local_path)
            df = serialize_json_columns(df, ["role", "passportArea", "birthArea"])
            return df

    # Download from Figshare
    logger.info("Downloading players.json from Figshare")
    resp = fetch_url(_PLAYERS_URL, timeout=(10, 60))
    data = resp.json()
    df = pd.DataFrame(data)
    df = serialize_json_columns(df, ["role", "passportArea", "birthArea"])
    logger.info("Loaded %d players from Figshare", len(df))
    return df


def ingest_players(
    spark: "SparkSession",
    catalog: str,
    schema: str,
    data_dir: pathlib.Path | None,
    logger: logging.Logger,
) -> None:
    """Load and write Wyscout player metadata."""
    pdf = _load_players(data_dir, logger)
    pdf = _normalize_mixed_types(pdf)

    sdf = spark.createDataFrame(pdf)
    row_count = validate_dataframe(
        sdf,
        ["wyId", "firstName", "lastName", "shortName", "birthDate"],
        "wyscout_players",
        logger,
    )
    write_delta_table(
        sdf, catalog, schema, "wyscout_players",
        mode="overwrite", logger=logger, row_count=row_count,
    )
```

Update `main()` to call `ingest_players`:

```python
def main() -> None:
    """CLI entry point for Wyscout ingestion."""
    args = parse_ingestion_args(
        "Ingest Wyscout data into the bronze layer",
        extra_args=[("--data-dir", {"default": None, "help": "Optional local directory with Wyscout JSON files"})],
    )

    logger = configure_logging("wyscout")
    spark = get_spark_session()

    data_dir = pathlib.Path(args.data_dir) if args.data_dir else None

    logger.info("Starting Wyscout ingestion into %s.%s", args.catalog, args.schema)

    ingest_events(spark, args.catalog, args.schema, data_dir, logger)
    ingest_matches(spark, args.catalog, args.schema, data_dir, logger)
    ingest_players(spark, args.catalog, args.schema, data_dir, logger)

    logger.info("Wyscout ingestion complete")
```

**Step 4: Run tests**

Run: `uv run pytest src/tests/test_wyscout.py -v`
Expected: All tests PASS (existing + new)

**Step 5: Run linters**

Run: `uv run ruff check src/ingestion/wyscout.py && uv run pyright src/ingestion/wyscout.py`
Expected: Zero violations

**Step 6: Commit**

```bash
git add src/ingestion/wyscout.py src/tests/test_wyscout.py
git commit -m "feat: extend Wyscout ingestion to download players.json from Figshare"
```

---

## Task 3: Create Wyscout Players Staging Model

**Files:**
- Create: `dbt_project/models/staging/wyscout/stg_wyscout__players.sql`
- Modify: `dbt_project/models/staging/wyscout/_wyscout__sources.yml` (add source table)
- Modify: `dbt_project/models/staging/wyscout/_wyscout__models.yml` (add model schema)

**Step 1: Add source definition**

In `_wyscout__sources.yml`, add `wyscout_players` to the tables list under the `wyscout` source:

```yaml
      - name: wyscout_players
        description: "Wyscout player metadata from Figshare (3,603 players across 7 competitions)"
        columns:
          - name: wyId
            description: "Wyscout player identifier"
            tests:
              - not_null
              - unique
```

**Step 2: Create staging model**

Create `stg_wyscout__players.sql`:

```sql
-- stg_wyscout__players.sql
-- Wyscout player metadata from Figshare (Pappalardo et al. 2019).
--
-- Source: https://figshare.com/collections/Soccer_match_event_dataset/4415000
-- 3,603 players across 7 competitions (2017/18 season).
--
-- JSON columns (role, passportArea, birthArea) are pre-serialized to strings
-- by the ingestion layer.

with source as (

    select * from {{ source('wyscout', 'wyscout_players') }}

),

final as (

    select
        cast(wyId as int)                                    as player_id,
        firstName                                            as first_name,
        lastName                                             as last_name,
        shortName                                            as short_name,
        -- Full name for fuzzy matching
        concat_ws(' ', firstName, lastName)                  as player_name,
        birthDate                                            as birth_date,
        -- Parse role JSON for position info
        role:name::string                                    as position_name,
        role:code2::string                                   as position_code,
        cast(currentTeamId as int)                           as current_team_id,
        foot,
        cast(height as int)                                  as height_cm,
        cast(weight as int)                                  as weight_kg,
        passportArea:name::string                            as nationality,
        passportArea:alpha3code::string                      as nationality_code,
        'wyscout'                                            as data_source

    from source
    where wyId is not null

)

select * from final
```

**Step 3: Add model schema**

In `_wyscout__models.yml`, add the model definition with column descriptions and tests (uniqueness on `player_id`, not_null on `player_name`).

**Step 4: Test dbt compilation**

Run (from project root, using the Windows dbt invocation pattern):
```bash
cd dbt_project && MSYS_NO_PATHCONV=1 python -c "import dbt.cli.main; dbt.cli.main.dbtRunner().invoke(['compile', '--select', 'stg_wyscout__players'])"
```
Expected: Compiles without errors.

**Step 5: Commit**

```bash
git add dbt_project/models/staging/wyscout/
git commit -m "feat: add stg_wyscout__players staging model for Figshare player metadata"
```

---

## Task 4: Investigate IDSSE Player Names in DFL XML

**Files:**
- Read-only investigation, no changes unless names are found.

**Context:** The `floodlight` library's DFL parser extracts `Shortname` from `<Player>` elements in matchinformation XML. Our `ingest_idsse.py` currently only extracts `PersonId`. Check if the IDSSE dataset's XML includes player names.

**Step 1: Inspect a sample IDSSE matchinformation XML**

Check if the DFL XML files in the IDSSE dataset contain `<Player>` elements with `ShortName`, `Name`, or similar attributes. Two paths:
- Read a local XML file if available in the data directory
- Check what `kloppy.sportec.load()` returns for player metadata

**Step 2: Document findings**

If player names ARE available:
- Note the XML attribute names and extraction path
- Create a follow-up task to extend `ingest_idsse.py` to extract player metadata
- This becomes Task 4b (not blocking for the main entity resolution pipeline)

If player names are NOT available:
- Document in the design doc that IDSSE matching requires external data
- Skip IDSSE columns in `dim_players` refactor for now

**Step 3: Repeat for SkillCorner**

Check if `kloppy`'s `TrackingDataset` Player objects have `.name` attributes populated for SkillCorner open data.

**Step 4: Commit findings**

```bash
git commit --allow-empty -m "docs: IDSSE/SkillCorner player metadata investigation results"
```

> **Note:** This task may produce zero code changes. That's fine — the investigation informs design decisions for later tasks.

---

## Task 5: Entity Resolution Analytics Module — Name Cleaning

**Files:**
- Create: `src/analytics/entity_resolution.py`
- Create: `src/tests/test_entity_resolution.py`

**Step 1: Write failing tests for name normalization**

```python
"""Tests for cross-source player entity resolution."""

from __future__ import annotations

import pandas as pd
import pytest

from analytics.entity_resolution import normalize_name, encode_dob


class TestNormalizeName:
    """Name cleaning via unidecode + punctuation strip."""

    def test_accented_characters(self) -> None:
        assert normalize_name("Bruno Guimarães") == "bruno guimaraes"

    def test_diacritics(self) -> None:
        assert normalize_name("Sørloth") == "sorloth"

    def test_punctuation_stripped(self) -> None:
        assert normalize_name("O'Brien-Smith") == "obrien smith"

    def test_extra_whitespace(self) -> None:
        assert normalize_name("  Harry   Kane  ") == "harry kane"

    def test_empty_string(self) -> None:
        assert normalize_name("") == ""

    def test_none_returns_empty(self) -> None:
        assert normalize_name(None) == ""


class TestEncodeDob:
    """DOB encoding for TF-IDF soft signal."""

    def test_valid_date(self) -> None:
        # 1993-12-28 → "28L93"
        assert encode_dob("1993-12-28") == "28L93"

    def test_january(self) -> None:
        assert encode_dob("1990-01-15") == "15A90"

    def test_none_returns_empty(self) -> None:
        assert encode_dob(None) == ""

    def test_invalid_format(self) -> None:
        assert encode_dob("not-a-date") == ""
```

**Step 2: Run test to verify it fails**

Run: `uv run pytest src/tests/test_entity_resolution.py::TestNormalizeName -v`
Expected: FAIL with `ModuleNotFoundError`

**Step 3: Implement name cleaning**

Create `src/analytics/entity_resolution.py`:

```python
"""Cross-source player entity resolution.

Hybrid pipeline: TF-IDF candidate generation (sparse_dot_topn) followed by
multi-attribute scoring (rapidfuzz) with bidirectional validation.

References:
  - parmacalcio1913/players-matcher (Apache-2.0) — bidirectional pattern
  - Pappalardo et al. (2019) Wyscout dataset, StatsBomb open data
"""

from __future__ import annotations

import logging
import re
from dataclasses import dataclass, field

import numpy as np
import pandas as pd
from rapidfuzz import fuzz
from scipy.sparse import csr_matrix
from sklearn.feature_extraction.text import TfidfVectorizer
from sparse_dot_topn import sp_matmul_topn
from unidecode import unidecode

logger = logging.getLogger(__name__)

# Month letters: Jan=A, Feb=B, ..., Dec=L
_MONTH_LETTERS = "ABCDEFGHIJKL"
_CLEAN_RE = re.compile(r"[^\w\s]", re.UNICODE)
_WHITESPACE_RE = re.compile(r"\s+")


def normalize_name(name: str | None) -> str:
    """Normalize a player name for matching.

    Applies: unidecode → lowercase → strip punctuation → collapse whitespace.
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
    Example: 1993-12-28 → "28L93"
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
```

**Step 4: Run tests**

Run: `uv run pytest src/tests/test_entity_resolution.py -v`
Expected: All PASS

**Step 5: Run linters**

Run: `uv run ruff check src/analytics/entity_resolution.py && uv run pyright src/analytics/entity_resolution.py`
Expected: Zero violations

**Step 6: Commit**

```bash
git add src/analytics/entity_resolution.py src/tests/test_entity_resolution.py
git commit -m "feat: entity resolution name cleaning and DOB encoding"
```

---

## Task 6: Entity Resolution — TF-IDF Candidate Generation

**Files:**
- Modify: `src/analytics/entity_resolution.py`
- Modify: `src/tests/test_entity_resolution.py`

**Step 1: Write failing tests for candidate generation**

```python
class TestTfidfCandidateGeneration:
    """TF-IDF + sparse_dot_topn candidate generation."""

    def test_exact_match_returns_high_similarity(self) -> None:
        from analytics.entity_resolution import generate_candidates

        source_a = pd.DataFrame({
            "player_id": [1],
            "searchable_name": ["harry kane 28G97"],
        })
        source_b = pd.DataFrame({
            "player_id": [101],
            "searchable_name": ["harry kane 28G97"],
        })
        candidates = generate_candidates(source_a, source_b, top_n=1, threshold=0.5)
        assert len(candidates) == 1
        assert candidates.iloc[0]["player_id_a"] == 1
        assert candidates.iloc[0]["player_id_b"] == 101
        assert candidates.iloc[0]["tfidf_score"] > 0.9

    def test_no_match_below_threshold(self) -> None:
        from analytics.entity_resolution import generate_candidates

        source_a = pd.DataFrame({
            "player_id": [1],
            "searchable_name": ["harry kane 28G97"],
        })
        source_b = pd.DataFrame({
            "player_id": [101],
            "searchable_name": ["completely different name 01A80"],
        })
        candidates = generate_candidates(source_a, source_b, top_n=1, threshold=0.5)
        assert len(candidates) == 0

    def test_returns_top_n_candidates(self) -> None:
        from analytics.entity_resolution import generate_candidates

        source_a = pd.DataFrame({
            "player_id": [1],
            "searchable_name": ["harry kane"],
        })
        source_b = pd.DataFrame({
            "player_id": [101, 102, 103],
            "searchable_name": ["harry kane", "harry kean", "john smith"],
        })
        candidates = generate_candidates(source_a, source_b, top_n=2, threshold=0.3)
        # Should return up to 2 candidates for player 1
        assert len(candidates) <= 2
        assert all(candidates["player_id_a"] == 1)
```

**Step 2: Run test to verify it fails**

Run: `uv run pytest src/tests/test_entity_resolution.py::TestTfidfCandidateGeneration -v`
Expected: FAIL with `ImportError`

**Step 3: Implement `generate_candidates`**

Add to `src/analytics/entity_resolution.py`:

```python
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

    vectorizer = TfidfVectorizer(
        analyzer=lambda text: _build_ngrams(text, ngram_size),
        dtype=np.float32,
    )
    vectorizer.fit(all_names)

    matrix_a = vectorizer.transform(source_a["searchable_name"])
    matrix_b = vectorizer.transform(source_b["searchable_name"])

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
        return pd.DataFrame(columns=["player_id_a", "player_id_b", "tfidf_score"])

    return pd.DataFrame({
        "player_id_a": source_a["player_id"].iloc[coo.row].values,
        "player_id_b": source_b["player_id"].iloc[coo.col].values,
        "tfidf_score": coo.data,
    })
```

**Step 4: Run tests**

Run: `uv run pytest src/tests/test_entity_resolution.py -v`
Expected: All PASS

**Step 5: Commit**

```bash
git add src/analytics/entity_resolution.py src/tests/test_entity_resolution.py
git commit -m "feat: TF-IDF candidate generation with sparse_dot_topn"
```

---

## Task 7: Entity Resolution — Multi-Attribute Scoring + Bidirectional Validation

**Files:**
- Modify: `src/analytics/entity_resolution.py`
- Modify: `src/tests/test_entity_resolution.py`

**Step 1: Write failing tests for scoring and bidirectional validation**

```python
class TestDobMatch:
    """DOB comparison with month/day swap detection (glass_onion insight)."""

    def test_exact_match(self) -> None:
        from analytics.entity_resolution import match_dob

        assert match_dob("1993-07-28", "1993-07-28") == 100.0

    def test_off_by_one_day(self) -> None:
        from analytics.entity_resolution import match_dob

        # ±1 day tolerance (timezone/recording differences)
        score = match_dob("1993-07-28", "1993-07-29")
        assert score == 90.0

    def test_month_day_swap(self) -> None:
        from analytics.entity_resolution import match_dob

        # MM-DD vs DD-MM format confusion (e.g., 03-07 vs 07-03)
        score = match_dob("1993-03-07", "1993-07-03")
        assert score == 80.0

    def test_completely_different(self) -> None:
        from analytics.entity_resolution import match_dob

        assert match_dob("1993-07-28", "2003-07-28") == 0.0

    def test_missing_dob(self) -> None:
        from analytics.entity_resolution import match_dob

        # None means "no data" — not a mismatch
        assert match_dob("1993-07-28", None) is None
        assert match_dob(None, "1993-07-28") is None
        assert match_dob(None, None) is None


class TestMultiAttributeScoring:
    """Multi-attribute scoring: name + DOB + position + jersey + team."""

    def test_exact_name_high_score(self) -> None:
        from analytics.entity_resolution import score_candidate

        score = score_candidate(
            name_a="harry kane", name_b="harry kane",
            dob_a="1993-07-28", dob_b="1993-07-28",
            position_a="Forward", position_b="Forward",
        )
        assert score > 95

    def test_dob_mismatch_penalizes(self) -> None:
        from analytics.entity_resolution import score_candidate

        score_match = score_candidate(
            name_a="harry kane", name_b="harry kane",
            dob_a="1993-07-28", dob_b="1993-07-28",
            position_a="Forward", position_b="Forward",
        )
        score_mismatch = score_candidate(
            name_a="harry kane", name_b="harry kane",
            dob_a="1993-07-28", dob_b="2003-07-28",
            position_a="Forward", position_b="Forward",
        )
        assert score_match > score_mismatch

    def test_dob_month_day_swap_partial_credit(self) -> None:
        from analytics.entity_resolution import score_candidate

        score_exact = score_candidate(
            name_a="harry kane", name_b="harry kane",
            dob_a="1993-07-28", dob_b="1993-07-28",
            position_a="Forward", position_b="Forward",
        )
        score_swap = score_candidate(
            name_a="harry kane", name_b="harry kane",
            dob_a="1993-03-07", dob_b="1993-07-03",
            position_a="Forward", position_b="Forward",
        )
        # Swap should score higher than total mismatch, lower than exact
        score_mismatch = score_candidate(
            name_a="harry kane", name_b="harry kane",
            dob_a="1993-07-28", dob_b="2003-07-28",
            position_a="Forward", position_b="Forward",
        )
        assert score_exact > score_swap > score_mismatch

    def test_missing_dob_no_penalty(self) -> None:
        from analytics.entity_resolution import score_candidate

        score = score_candidate(
            name_a="harry kane", name_b="harry kane",
            dob_a="1993-07-28", dob_b=None,
            position_a="Forward", position_b="Forward",
        )
        # Should still score well on name alone
        assert score > 70

    def test_jersey_number_exact_boost(self) -> None:
        from analytics.entity_resolution import score_candidate

        score_with = score_candidate(
            name_a="harry kane", name_b="harry kane",
            jersey_a="10", jersey_b="10",
        )
        score_without = score_candidate(
            name_a="harry kane", name_b="harry kane",
        )
        assert score_with >= score_without

    def test_jersey_number_mismatch_no_penalty(self) -> None:
        from analytics.entity_resolution import score_candidate

        # Different jersey numbers shouldn't penalize — players change numbers
        score = score_candidate(
            name_a="harry kane", name_b="harry kane",
            jersey_a="10", jersey_b="7",
        )
        # Should still score well on name alone
        assert score > 70


class TestBidirectionalValidation:
    """Bidirectional mutual best-match filtering."""

    def test_mutual_match_kept(self) -> None:
        from analytics.entity_resolution import validate_bidirectional

        forward = pd.DataFrame({
            "player_id_a": [1, 2],
            "player_id_b": [101, 102],
            "score": [95.0, 90.0],
        })
        reverse = pd.DataFrame({
            "player_id_a": [101, 102],
            "player_id_b": [1, 2],
            "score": [95.0, 90.0],
        })
        result = validate_bidirectional(forward, reverse)
        assert len(result) == 2

    def test_non_mutual_match_rejected(self) -> None:
        from analytics.entity_resolution import validate_bidirectional

        # A→B: player 1 best match is 101
        forward = pd.DataFrame({
            "player_id_a": [1],
            "player_id_b": [101],
            "score": [80.0],
        })
        # B→A: player 101 best match is player 2 (not 1)
        reverse = pd.DataFrame({
            "player_id_a": [101],
            "player_id_b": [2],
            "score": [85.0],
        })
        result = validate_bidirectional(forward, reverse)
        assert len(result) == 0
```

**Step 2: Run tests to verify they fail**

Run: `uv run pytest src/tests/test_entity_resolution.py::TestMultiAttributeScoring -v`
Expected: FAIL

**Step 3: Implement scoring and validation**

Add to `src/analytics/entity_resolution.py`:

```python
# ---------------------------------------------------------------------------
# Position group mapping for cross-source compatibility
# ---------------------------------------------------------------------------

_POSITION_GROUP: dict[str, str] = {
    # Wyscout role codes
    "GK": "Goalkeeper", "Goalkeeper": "Goalkeeper",
    "DF": "Defender", "Defender": "Defender",
    "MF": "Midfielder", "Midfielder": "Midfielder",
    "FW": "Forward", "Forward": "Forward",
    # StatsBomb position_name keywords
    "Back": "Defender", "Wing Back": "Defender",
    "Midfield": "Midfielder", "Wing": "Forward",
    "Forward": "Forward", "Striker": "Forward",
}


def _map_position_group(position: str | None) -> str:
    """Map a position string to a broad group (Goalkeeper/Defender/Midfielder/Forward)."""
    if not position:
        return ""
    for key, group in _POSITION_GROUP.items():
        if key.lower() in position.lower():
            return group
    return ""


def match_dob(
    dob_a: str | None,
    dob_b: str | None,
) -> float | None:
    """Compare two dates of birth with tolerance for common data quality issues.

    Inspired by glass_onion's DOB matching layers.

    Returns:
        100.0 for exact match, 90.0 for ±1 day, 80.0 for month/day swap,
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
    # Same year — check ±1 day tolerance (timezone/recording differences)
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
) -> float:
    """Score a candidate match using multiple attributes.

    Weights: name similarity 60%, DOB match 25%, position match 15%.
    Missing attributes are excluded and weights redistributed.
    Jersey number match is a bonus (not penalized on mismatch, since
    players change numbers across seasons/teams).

    Returns:
        Score 0-100.
    """
    # Name similarity (rapidfuzz token_sort_ratio)
    name_score = fuzz.token_sort_ratio(name_a, name_b)

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


def validate_bidirectional(
    forward: pd.DataFrame,
    reverse: pd.DataFrame,
) -> pd.DataFrame:
    """Keep only mutual best matches (bidirectional validation).

    Args:
        forward: A→B matches with columns player_id_a, player_id_b, score.
        reverse: B→A matches with columns player_id_a, player_id_b, score.

    Returns:
        DataFrame of mutual matches (player_id_a from forward perspective).
    """
    # Best match per player in forward direction
    fwd_best = forward.loc[forward.groupby("player_id_a")["score"].idxmax()]
    # Best match per player in reverse direction
    rev_best = reverse.loc[reverse.groupby("player_id_a")["score"].idxmax()]

    # Build lookup: in reverse, player_id_a is the B-side player
    rev_lookup = dict(zip(rev_best["player_id_a"], rev_best["player_id_b"]))

    # Keep forward match (a→b) only if reverse best match for b is a
    mutual_mask = fwd_best.apply(
        lambda row: rev_lookup.get(row["player_id_b"]) == row["player_id_a"],
        axis=1,
    )
    return fwd_best[mutual_mask].reset_index(drop=True)
```

**Step 4: Run all tests**

Run: `uv run pytest src/tests/test_entity_resolution.py -v`
Expected: All PASS

**Step 5: Run linters**

Run: `uv run ruff check src/analytics/entity_resolution.py && uv run pyright src/analytics/entity_resolution.py`
Expected: Zero violations

**Step 6: Commit**

```bash
git add src/analytics/entity_resolution.py src/tests/test_entity_resolution.py
git commit -m "feat: multi-attribute scoring and bidirectional validation"
```

---

## Task 8: Entity Resolution — Three-Layer Progressive Pipeline

**Files:**
- Modify: `src/analytics/entity_resolution.py`
- Modify: `src/tests/test_entity_resolution.py`

**Context:** Inspired by glass_onion's five-layer escalating strategy. Each layer catches matches at a different confidence level. Matched players are removed before the next layer runs, preventing false positives from contaminating lower-confidence layers.

| Layer | Strategy | Constraints | Threshold |
|-------|----------|-------------|-----------|
| 1 (strict) | Name + DOB + jersey + team | Same team context | 90% |
| 2 (standard) | Name + DOB (with month/day swap) | Any overlap | 80% |
| 3 (relaxed) | Name + position | No constraints | 75% |

**Step 1: Write failing test for the full pipeline**

```python
class TestResolvePlayers:
    """End-to-end three-layer entity resolution pipeline."""

    def test_resolves_matching_players(self) -> None:
        from analytics.entity_resolution import resolve_players

        statsbomb = pd.DataFrame({
            "player_id": [1, 2, 3],
            "player_name": ["Harry Kane", "Lionel Messi", "Unique Player"],
            "birth_date": ["1993-07-28", "1987-06-24", "2000-01-01"],
            "position": ["Forward", "Forward", "Midfielder"],
        })
        wyscout = pd.DataFrame({
            "player_id": [101, 102, 103],
            "player_name": ["H. Kane", "L. Messi", "Different Person"],
            "birth_date": ["1993-07-28", "1987-06-24", "1995-05-05"],
            "position": ["FW", "FW", "DF"],
        })
        result = resolve_players(
            statsbomb, wyscout,
            confidence_threshold=50.0,
        )
        # Kane and Messi should match; Unique Player and Different Person should not
        assert len(result) >= 2
        kane_match = result[result["player_id_a"] == 1]
        assert len(kane_match) == 1
        assert kane_match.iloc[0]["player_id_b"] == 101
        assert kane_match.iloc[0]["confidence"] > 50

    def test_layer1_strict_with_team_context(self) -> None:
        from analytics.entity_resolution import resolve_players

        # Layer 1: Same name + DOB + jersey + team → very high confidence
        statsbomb = pd.DataFrame({
            "player_id": [1],
            "player_name": ["Harry Kane"],
            "birth_date": ["1993-07-28"],
            "position": ["Forward"],
            "jersey_number": ["10"],
            "team_name": ["Tottenham Hotspur"],
        })
        wyscout = pd.DataFrame({
            "player_id": [101],
            "player_name": ["H. Kane"],
            "birth_date": ["1993-07-28"],
            "position": ["FW"],
            "jersey_number": ["10"],
            "team_name": ["Tottenham Hotspur"],
        })
        result = resolve_players(statsbomb, wyscout, confidence_threshold=50.0)
        assert len(result) == 1
        assert result.iloc[0]["confidence"] > 90
        assert result.iloc[0]["match_layer"] == 1

    def test_layer2_name_dob_without_team(self) -> None:
        from analytics.entity_resolution import resolve_players

        # Layer 2: Name + DOB match without team context
        statsbomb = pd.DataFrame({
            "player_id": [1],
            "player_name": ["Lionel Messi"],
            "birth_date": ["1987-06-24"],
            "position": ["Forward"],
        })
        wyscout = pd.DataFrame({
            "player_id": [101],
            "player_name": ["L. Messi"],
            "birth_date": ["1987-06-24"],
            "position": ["FW"],
        })
        result = resolve_players(statsbomb, wyscout, confidence_threshold=50.0)
        assert len(result) == 1
        assert result.iloc[0]["match_layer"] == 2

    def test_layer3_name_only_relaxed(self) -> None:
        from analytics.entity_resolution import resolve_players

        # Layer 3: Name + position only (no DOB available)
        statsbomb = pd.DataFrame({
            "player_id": [1],
            "player_name": ["Marco Verratti"],
            "position": ["Midfielder"],
        })
        wyscout = pd.DataFrame({
            "player_id": [101],
            "player_name": ["M. Verratti"],
            "position": ["MF"],
        })
        result = resolve_players(statsbomb, wyscout, confidence_threshold=50.0)
        assert len(result) == 1
        assert result.iloc[0]["match_layer"] == 3

    def test_matched_players_removed_between_layers(self) -> None:
        from analytics.entity_resolution import resolve_players

        # Player matched in Layer 1 should not appear in Layer 2/3
        statsbomb = pd.DataFrame({
            "player_id": [1, 2],
            "player_name": ["Harry Kane", "Harry Kean"],
            "birth_date": ["1993-07-28", "1993-07-28"],
            "position": ["Forward", "Forward"],
            "jersey_number": ["10", "9"],
            "team_name": ["Tottenham Hotspur", "Tottenham Hotspur"],
        })
        wyscout = pd.DataFrame({
            "player_id": [101],
            "player_name": ["H. Kane"],
            "birth_date": ["1993-07-28"],
            "position": ["FW"],
            "jersey_number": ["10"],
            "team_name": ["Tottenham Hotspur"],
        })
        result = resolve_players(statsbomb, wyscout, confidence_threshold=50.0)
        # Only player 1 should match (jersey 10), not player 2
        assert len(result) == 1
        assert result.iloc[0]["player_id_a"] == 1
```

**Step 2: Run test to verify it fails**

**Step 3: Implement three-layer `resolve_players` orchestrator**

```python
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
    layers: list[LayerConfig] = field(default_factory=lambda: [
        LayerConfig(threshold=90.0, use_dob=True, use_jersey=True, use_team=True, tfidf_threshold=0.4),
        LayerConfig(threshold=80.0, use_dob=True, use_jersey=False, use_team=False, tfidf_threshold=0.3),
        LayerConfig(threshold=75.0, use_dob=False, use_jersey=False, use_team=False, tfidf_threshold=0.25),
    ])


def _prepare_source(
    df: pd.DataFrame,
    name_col: str = "player_name",
    dob_col: str = "birth_date",
) -> pd.DataFrame:
    """Prepare a source DataFrame for matching.

    Adds ``searchable_name`` column (normalized name + encoded DOB).
    """
    result = df.copy()
    result["_normalized_name"] = result[name_col].apply(normalize_name)
    if dob_col in result.columns:
        result["_encoded_dob"] = result[dob_col].apply(encode_dob)
    else:
        result["_encoded_dob"] = ""
    result["searchable_name"] = (
        result["_normalized_name"] + " " + result["_encoded_dob"]
    ).str.strip()
    # Ensure optional columns exist for uniform access
    for col in ("birth_date", "position", "jersey_number", "team_name"):
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
    """Run a single matching layer: TF-IDF → score → bidirectional validate.

    Returns DataFrame with player_id_a, player_id_b, confidence.
    """
    # Filter by team context if Layer requires it
    if layer.use_team:
        # Only compare players on the same team (normalized)
        a_teams = prep_a["team_name"].apply(
            lambda t: normalize_name(t) if pd.notna(t) else ""
        )
        b_teams = prep_b["team_name"].apply(
            lambda t: normalize_name(t) if pd.notna(t) else ""
        )
        common_teams = set(a_teams[a_teams != ""]) & set(b_teams[b_teams != ""])
        if not common_teams:
            return pd.DataFrame(columns=["player_id_a", "player_id_b", "confidence"])
        mask_a = a_teams.isin(common_teams)
        mask_b = b_teams.isin(common_teams)
        layer_a = prep_a[mask_a]
        layer_b = prep_b[mask_b]
    else:
        layer_a = prep_a
        layer_b = prep_b

    if layer_a.empty or layer_b.empty:
        return pd.DataFrame(columns=["player_id_a", "player_id_b", "confidence"])

    # TF-IDF candidate generation
    candidates = generate_candidates(
        layer_a[["player_id", "searchable_name"]],
        layer_b[["player_id", "searchable_name"]],
        top_n=tfidf_top_n,
        threshold=layer.tfidf_threshold,
        ngram_size=ngram_size,
    )
    if candidates.empty:
        return pd.DataFrame(columns=["player_id_a", "player_id_b", "confidence"])

    a_lookup = layer_a.set_index("player_id")
    b_lookup = layer_b.set_index("player_id")

    def _score_pair(pid_a: int, pid_b: int) -> float:
        rec_a = a_lookup.loc[pid_a]
        rec_b = b_lookup.loc[pid_b]
        return score_candidate(
            name_a=rec_a.get("_normalized_name", ""),
            name_b=rec_b.get("_normalized_name", ""),
            dob_a=rec_a.get("birth_date") if layer.use_dob else None,
            dob_b=rec_b.get("birth_date") if layer.use_dob else None,
            position_a=rec_a.get("position") if layer.use_position else None,
            position_b=rec_b.get("position") if layer.use_position else None,
            jersey_a=rec_a.get("jersey_number") if layer.use_jersey else None,
            jersey_b=rec_b.get("jersey_number") if layer.use_jersey else None,
        )

    # Forward scoring (A→B)
    scores_fwd = [
        {"player_id_a": r["player_id_a"], "player_id_b": r["player_id_b"],
         "score": _score_pair(r["player_id_a"], r["player_id_b"])}
        for _, r in candidates.iterrows()
    ]
    forward = pd.DataFrame(scores_fwd)

    # Reverse scoring (B→A)
    candidates_rev = generate_candidates(
        layer_b[["player_id", "searchable_name"]],
        layer_a[["player_id", "searchable_name"]],
        top_n=tfidf_top_n,
        threshold=layer.tfidf_threshold,
        ngram_size=ngram_size,
    )
    if candidates_rev.empty:
        return pd.DataFrame(columns=["player_id_a", "player_id_b", "confidence"])

    scores_rev = [
        {"player_id_a": r["player_id_a"], "player_id_b": r["player_id_b"],
         "score": _score_pair(r["player_id_b"], r["player_id_a"])}
        for _, r in candidates_rev.iterrows()
    ]
    reverse = pd.DataFrame(scores_rev)

    # Bidirectional validation + threshold
    mutual = validate_bidirectional(forward, reverse)
    result = mutual[mutual["score"] >= layer.threshold].copy()
    return result.rename(columns={"score": "confidence"})


def resolve_players(
    source_a: pd.DataFrame,
    source_b: pd.DataFrame,
    config: ResolutionConfig | None = None,
    confidence_threshold: float | None = None,
) -> pd.DataFrame:
    """Run the three-layer progressive entity resolution pipeline.

    Layers run strict→permissive. Matched players are removed before each
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
        config.confidence_threshold = confidence_threshold

    prep_a = _prepare_source(source_a)
    prep_b = _prepare_source(source_b)

    all_matches: list[pd.DataFrame] = []
    matched_a: set[int] = set()
    matched_b: set[int] = set()

    for layer_num, layer in enumerate(config.layers, start=1):
        # Remove already-matched players
        remaining_a = prep_a[~prep_a["player_id"].isin(matched_a)]
        remaining_b = prep_b[~prep_b["player_id"].isin(matched_b)]

        if remaining_a.empty or remaining_b.empty:
            break

        layer_result = _run_layer(
            remaining_a, remaining_b, layer,
            tfidf_top_n=config.tfidf_top_n,
            ngram_size=config.ngram_size,
        )

        if not layer_result.empty:
            layer_result["match_layer"] = layer_num
            layer_result["match_method"] = f"layer{layer_num}_tfidf_rapidfuzz_bidirectional"
            all_matches.append(layer_result)
            matched_a.update(layer_result["player_id_a"].tolist())
            matched_b.update(layer_result["player_id_b"].tolist())

        logger.info(
            "Layer %d: %d matches (threshold=%.0f, team=%s, jersey=%s, dob=%s)",
            layer_num, len(layer_result), layer.threshold,
            layer.use_team, layer.use_jersey, layer.use_dob,
        )

    if not all_matches:
        logger.info("No cross-source matches found across all layers")
        return pd.DataFrame(
            columns=["player_id_a", "player_id_b", "confidence", "match_method", "match_layer"]
        )

    result = pd.concat(all_matches, ignore_index=True)

    # Apply global confidence threshold (may filter some Layer 3 matches)
    result = result[result["confidence"] >= config.confidence_threshold]

    logger.info(
        "Entity resolution complete: %d total matches (L1=%d, L2=%d, L3=%d)",
        len(result),
        len(result[result["match_layer"] == 1]),
        len(result[result["match_layer"] == 2]),
        len(result[result["match_layer"] == 3]),
    )

    return result[["player_id_a", "player_id_b", "confidence", "match_method", "match_layer"]]
```

**Step 4: Run all tests**

Run: `uv run pytest src/tests/test_entity_resolution.py -v`
Expected: All PASS

**Step 5: Run full test suite + linters**

Run: `uv run ruff check src/ && uv run pyright src/ && uv run pytest src/tests/ -v`
Expected: Zero violations, all tests PASS

**Step 6: Commit**

```bash
git add src/analytics/entity_resolution.py src/tests/test_entity_resolution.py
git commit -m "feat: full entity resolution pipeline with TF-IDF + rapidfuzz + bidirectional"
```

---

## Task 9: Entity Resolution Ingestion Module

**Files:**
- Create: `src/ingestion/entity_resolution.py`
- Modify: `pyproject.toml:59-70` (add entry point)

**Context:** This module reads player metadata from bronze/staging tables (StatsBomb lineups, Wyscout players), runs the resolution pipeline, and writes `player_xref_raw` to bronze. Follows the same CLI pattern as other ingestion modules.

**Step 1: Implement the ingestion module**

Create `src/ingestion/entity_resolution.py`:

```python
"""Cross-source player entity resolution batch pipeline.

Reads player metadata from StatsBomb lineups and Wyscout players bronze
tables, runs the hybrid matching pipeline (TF-IDF + rapidfuzz + bidirectional),
and writes results to ``player_xref_raw`` bronze table.

Bronze table produced:
  - player_xref_raw
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING

import pandas as pd

from analytics.entity_resolution import ResolutionConfig, resolve_players
from ingestion.utils import (
    add_audit_columns,
    configure_logging,
    get_spark_session,
    parse_ingestion_args,
    validate_dataframe,
    write_delta_table,
)

if TYPE_CHECKING:
    from pyspark.sql import SparkSession


def _load_statsbomb_players(spark: SparkSession, catalog: str, schema: str) -> pd.DataFrame:
    """Load StatsBomb player metadata from lineups bronze table.

    Extracts jersey_number and team_name for Layer 1 team-scoped matching.
    Deduplicates by player_id, keeping the most recent match's metadata.
    """
    df = spark.sql(f"""
        WITH ranked AS (
            SELECT
                CAST(player_id AS INT) AS player_id,
                player_name,
                player_nickname,
                CAST(jersey_number AS STRING) AS jersey_number,
                team_name,
                get(
                    from_json(positions, 'ARRAY<STRUCT<position:STRING>>'),
                    0
                ).position AS position,
                ROW_NUMBER() OVER (
                    PARTITION BY player_id ORDER BY match_id DESC
                ) AS rn
            FROM {catalog}.{schema}.statsbomb_lineups
            WHERE player_id IS NOT NULL
        )
        SELECT player_id, player_name, player_nickname,
               jersey_number, team_name, position
        FROM ranked WHERE rn = 1
    """).toPandas()
    # Use nickname as alternate name signal
    df["player_name"] = df["player_name"].fillna(df["player_nickname"])
    return df[["player_id", "player_name", "position", "jersey_number", "team_name"]]


def _load_wyscout_players(spark: SparkSession, catalog: str, schema: str) -> pd.DataFrame:
    """Load Wyscout player metadata from players bronze table.

    Includes currentTeamId for potential team-scoped matching in Layer 1.
    """
    df = spark.sql(f"""
        SELECT
            CAST(wyId AS INT) AS player_id,
            CONCAT_WS(' ', firstName, lastName) AS player_name,
            shortName AS short_name,
            birthDate AS birth_date,
            role:name::STRING AS position,
            CAST(currentTeamId AS STRING) AS current_team_id
        FROM {catalog}.{schema}.wyscout_players
        WHERE wyId IS NOT NULL
    """).toPandas()
    return df


def main() -> None:
    """CLI entry point for entity resolution."""
    args = parse_ingestion_args("Run cross-source player entity resolution")
    logger = configure_logging("entity_resolution")
    spark = get_spark_session()

    logger.info("Starting entity resolution for %s.%s", args.catalog, args.schema)

    # Load player metadata from each source
    sb_players = _load_statsbomb_players(spark, args.catalog, args.schema)
    ws_players = _load_wyscout_players(spark, args.catalog, args.schema)

    logger.info("Loaded %d StatsBomb players, %d Wyscout players", len(sb_players), len(ws_players))

    # Run three-layer resolution
    config = ResolutionConfig(confidence_threshold=70.0)
    xref = resolve_players(sb_players, ws_players, config=config)

    if xref.empty:
        logger.warning("No cross-source matches found")
        return

    # Add source labels
    xref["source_a"] = "statsbomb"
    xref["source_b"] = "wyscout"

    # Write to bronze
    sdf = spark.createDataFrame(xref)
    row_count = validate_dataframe(
        sdf,
        ["player_id_a", "player_id_b", "confidence", "match_method", "match_layer", "source_a", "source_b"],
        "player_xref_raw",
        logger,
    )
    write_delta_table(
        sdf, args.catalog, args.schema, "player_xref_raw",
        mode="overwrite", logger=logger, row_count=row_count,
    )

    logger.info("Entity resolution complete: %d cross-source matches written", row_count)
```

**Step 2: Add entry point to pyproject.toml**

```toml
resolve_players = "ingestion.entity_resolution:main"
```

**Step 3: Run linters**

Run: `uv run ruff check src/ingestion/entity_resolution.py && uv run pyright src/ingestion/entity_resolution.py`
Expected: Zero violations

**Step 4: Commit**

```bash
git add src/ingestion/entity_resolution.py pyproject.toml
git commit -m "feat: entity resolution ingestion pipeline writing player_xref_raw to bronze"
```

---

## Task 10: dbt Layer — Player Xref Override Seed + Intermediate Model

**Files:**
- Create: `dbt_project/seeds/player_xref_overrides.csv`
- Modify: `dbt_project/seeds/_seeds__schema.yml`
- Create: `dbt_project/models/intermediate/int_player_xref.sql`
- Modify: `dbt_project/models/intermediate/_intermediate__models.yml`

**Step 1: Create overrides seed**

Create `dbt_project/seeds/player_xref_overrides.csv`:

```csv
statsbomb_player_id,wyscout_player_id,action
```

This starts empty. Rows can be added for manual corrections:
- `action=force_match`: Override the algorithm and force a match
- `action=force_reject`: Override the algorithm and reject a false positive

**Step 2: Add seed schema**

In `_seeds__schema.yml`, add:

```yaml
  - name: player_xref_overrides
    description: "Manual overrides for entity resolution (force_match/force_reject)"
    columns:
      - name: statsbomb_player_id
        description: "StatsBomb player_id"
      - name: wyscout_player_id
        description: "Wyscout wyId"
      - name: action
        description: "force_match or force_reject"
```

**Step 3: Create `int_player_xref.sql`**

```sql
-- int_player_xref.sql
-- Cross-source player identity mapping.
--
-- Combines automated resolution results (player_xref_raw bronze table)
-- with manual overrides (player_xref_overrides seed).
--
-- Grain: one row per cross-source match (statsbomb_player_id ↔ wyscout_player_id).

{{ config(materialized='ephemeral') }}

with automated_matches as (

    select
        cast(player_id_a as int)    as statsbomb_player_id,
        cast(player_id_b as int)    as wyscout_player_id,
        confidence,
        match_method,
        match_layer

    from {{ source('entity_resolution', 'player_xref_raw') }}
    where confidence >= 70.0

),

overrides as (

    select
        cast(statsbomb_player_id as int)    as statsbomb_player_id,
        cast(wyscout_player_id as int)      as wyscout_player_id,
        action

    from {{ ref('player_xref_overrides') }}

),

-- Remove automated matches that have a force_reject override
filtered as (

    select
        a.statsbomb_player_id,
        a.wyscout_player_id,
        a.confidence,
        a.match_layer,
        'automated' as resolution_type

    from automated_matches a
    left join overrides o
        on  a.statsbomb_player_id = o.statsbomb_player_id
        and a.wyscout_player_id = o.wyscout_player_id
        and o.action = 'force_reject'
    where o.statsbomb_player_id is null

),

-- Add force_match overrides that weren't in automated results
forced as (

    select
        o.statsbomb_player_id,
        o.wyscout_player_id,
        100.0 as confidence,
        0 as match_layer,
        'manual_override' as resolution_type

    from overrides o
    where o.action = 'force_match'

),

combined as (

    select * from filtered
    union all
    select * from forced

)

select * from combined
```

**Step 4: Add source definition for player_xref_raw**

Create or update the entity_resolution source definition:

```yaml
# In a new file or existing sources.yml
sources:
  - name: entity_resolution
    schema: "{{ var('bronze_schema', 'bronze') }}"
    tables:
      - name: player_xref_raw
        description: "Automated cross-source player matches from entity resolution pipeline"
```

**Step 5: Add intermediate model schema**

In `_intermediate__models.yml`, add `int_player_xref` with column descriptions.

**Step 6: Commit**

```bash
git add dbt_project/seeds/ dbt_project/models/intermediate/ dbt_project/models/staging/
git commit -m "feat: int_player_xref intermediate model with manual override support"
```

---

## Task 11: Refactor dim_players with canonical_player_id

**Files:**
- Modify: `dbt_project/models/marts/dim_players.sql`
- Modify: `dbt_project/models/marts/_marts__models.yml`

**Step 1: Rewrite dim_players.sql**

```sql
-- dim_players.sql
-- Player dimension table combining StatsBomb and Wyscout data.
--
-- Cross-source entity resolution (Phase 14) maps players across sources.
-- Players matched across sources share a canonical_player_id; unmatched
-- players retain their source-native ID.
--
-- Grain: one row per unique canonical_player_id.

with statsbomb_players as (

    select
        player_id,
        player_name,
        player_nickname,
        position_name                                       as primary_position,
        'statsbomb'                                         as data_source,
        row_number() over (
            partition by player_id
            order by match_id desc
        )                                                   as rn

    from {{ ref('stg_statsbomb__lineups') }}
    where player_id is not null

),

sb_deduped as (

    select * from statsbomb_players where rn = 1

),

wyscout_players as (

    select
        player_id,
        player_name,
        short_name,
        position_name                                       as primary_position,
        birth_date,
        nationality,
        'wyscout'                                           as data_source

    from {{ ref('stg_wyscout__players') }}

),

xref as (

    select * from {{ ref('int_player_xref') }}

),

-- StatsBomb players enriched with Wyscout cross-reference
sb_enriched as (

    select
        -- Canonical ID: StatsBomb player_id is the canonical anchor
        {{ dbt_utils.generate_surrogate_key(['sb.player_id', "'statsbomb'"]) }}
                                                            as canonical_player_id,
        sb.player_id                                        as player_id,
        sb.player_name,
        coalesce(sb.player_nickname, sb.player_name)        as player_display_name,
        sb.primary_position,
        sb.data_source,
        -- Cross-source IDs
        sb.player_id                                        as statsbomb_player_id,
        xref.wyscout_player_id,
        xref.confidence                                     as match_confidence,
        xref.match_layer,
        -- Enrich from Wyscout if matched
        ws.birth_date,
        ws.nationality

    from sb_deduped sb
    left join xref
        on sb.player_id = xref.statsbomb_player_id
    left join wyscout_players ws
        on xref.wyscout_player_id = ws.player_id

),

-- Wyscout-only players (no StatsBomb match)
ws_unmatched as (

    select
        {{ dbt_utils.generate_surrogate_key(['ws.player_id', "'wyscout'"]) }}
                                                            as canonical_player_id,
        ws.player_id,
        ws.player_name,
        coalesce(ws.short_name, ws.player_name)             as player_display_name,
        ws.primary_position,
        ws.data_source,
        -- Cross-source IDs
        null                                                as statsbomb_player_id,
        ws.player_id                                        as wyscout_player_id,
        null                                                as match_confidence,
        null                                                as match_layer,
        ws.birth_date,
        ws.nationality

    from wyscout_players ws
    left join xref
        on ws.player_id = xref.wyscout_player_id
    where xref.wyscout_player_id is null

),

combined as (

    select * from sb_enriched
    union all
    select * from ws_unmatched

),

final as (

    select
        canonical_player_id,
        player_id,
        player_name,
        player_display_name,
        primary_position,
        pm.position_group,
        statsbomb_player_id,
        wyscout_player_id,
        match_confidence,
        match_layer,
        birth_date,
        nationality,
        case
            when statsbomb_player_id is not null and wyscout_player_id is not null
                then 'statsbomb,wyscout'
            when statsbomb_player_id is not null then 'statsbomb'
            else 'wyscout'
        end                                                 as data_sources

    from combined c
    left join {{ ref('position_mapping') }} pm
        on c.primary_position = pm.position_name

)

select * from final
```

**Step 2: Update _marts__models.yml**

Add new columns to the `dim_players` model definition:
- `canonical_player_id` (surrogate key, unique)
- `statsbomb_player_id` (nullable)
- `wyscout_player_id` (nullable)
- `match_confidence` (nullable, 0-100)
- `match_layer` (nullable, 0-3; 0=manual, 1=strict, 2=standard, 3=relaxed)
- `birth_date`, `nationality`, `data_sources`

Add dbt tests:
- `unique` on `canonical_player_id`
- `not_null` on `canonical_player_id`, `player_name`
- `accepted_values` on `data_sources` values

**Step 3: Test dbt compilation**

Run: `cd dbt_project && MSYS_NO_PATHCONV=1 python -c "import dbt.cli.main; dbt.cli.main.dbtRunner().invoke(['compile', '--select', 'dim_players'])"`
Expected: Compiles without errors.

**Step 4: Commit**

```bash
git add dbt_project/models/marts/dim_players.sql dbt_project/models/marts/_marts__models.yml
git commit -m "feat: refactor dim_players with canonical_player_id and Wyscout integration"
```

---

## Task 12: dbt Feature Toggle + Conditional Compilation

**Files:**
- Modify: `dbt_project/dbt_project.yml` (add `entity_resolution_enabled` var)
- Modify: `dbt_project/models/marts/dim_players.sql` (wrap xref in conditional)

**Context:** The `int_player_xref` depends on `player_xref_raw` bronze table, which only exists after running the entity resolution pipeline. Similar to `off_ball_xt_enabled` and `defcon_enabled`, add a toggle so `dbt build` doesn't fail before the bronze table exists.

**Step 1: Add variable to dbt_project.yml**

```yaml
vars:
  entity_resolution_enabled: false
```

**Step 2: Wrap the xref reference in dim_players.sql**

```sql
{% if var('entity_resolution_enabled', false) %}
xref as (
    select * from {{ ref('int_player_xref') }}
),
{% else %}
xref as (
    -- Entity resolution not yet run — empty xref
    select
        cast(null as int) as statsbomb_player_id,
        cast(null as int) as wyscout_player_id,
        cast(null as double) as confidence,
        cast(null as int) as match_layer,
        cast(null as string) as resolution_type
    where 1 = 0
),
{% endif %}
```

Apply same pattern to `int_player_xref.sql` source reference.

**Step 3: Verify dbt builds with toggle off**

Run: `cd dbt_project && MSYS_NO_PATHCONV=1 python -c "import dbt.cli.main; dbt.cli.main.dbtRunner().invoke(['build', '--select', 'dim_players', '--vars', '{entity_resolution_enabled: false}'])"`
Expected: Build succeeds with empty xref (StatsBomb-only dim_players, identical to current behavior).

**Step 4: Commit**

```bash
git add dbt_project/dbt_project.yml dbt_project/models/
git commit -m "feat: add entity_resolution_enabled toggle for graceful dbt build"
```

---

## Task 13: Update NOTICE, PLAN.md, TODO.md

**Files:**
- Modify: `NOTICE`
- Modify: `PLAN.md`
- Modify: `TODO.md`

**Step 1: Add players-matcher attribution to NOTICE**

Add under Third-Party Libraries:

```
The player entity resolution module (src/analytics/entity_resolution.py) uses
the three-layer progressive matching strategy inspired by:

  glass_onion — US Soccer Federation (2026). "A package for synchronizing
  soccer data object identifiers." BSD 3-Clause License.
  See: https://github.com/USSoccerFederation/glass_onion
  Concepts: multi-layer progressive strategy, team-scoped matching,
  jersey number as constraint, DOB month/day swap detection.

  players-matcher — Parma Calcio 1913 (2024). Apache License, Version 2.0.
  See: https://github.com/parmacalcio1913/players-matcher
  Concept: bidirectional mutual best-match validation pattern.

The implementation is independent, using rapidfuzz and sparse_dot_topn.
```

**Step 2: Update PLAN.md**

Mark Phase 14 as complete in the status line and phase table.

**Step 3: Update TODO.md**

Check off Phase 14 tasks.

**Step 4: Commit**

```bash
git add NOTICE PLAN.md TODO.md
git commit -m "docs: update NOTICE, PLAN, TODO for Phase 14 completion"
```

---

## Task 14: Synced Table Recreation for dim_players

**Context:** `dim_players_synced` schema changes (new columns). Must follow the established recreation procedure. This task is operational — requires Databricks UI access.

**Step 1: Delete synced table via Terraform**

```bash
cd terraform/environments/dev
AWS_PROFILE=devops-agent terraform destroy \
  -target='module.synced_tables.databricks_database_synced_database_table.dim_players' \
  -auto-approve
```

**Step 2: Drop PG ghost table**

Via psycopg2 with OAuth credentials:
```sql
DROP TABLE IF EXISTS dev_gold.dim_players_synced CASCADE;
```

**Step 3: Recreate in Databricks UI**

Catalog → gold → dim_players → Create synced table:
- Project: `soccer-analytics-dev`
- Branch: `production`
- Logical DB: `databricks_postgres`
- Scheduling: SNAPSHOT

**Step 4: Import into Terraform**

```bash
AWS_PROFILE=devops-agent terraform import \
  'module.synced_tables.databricks_database_synced_database_table.dim_players' \
  'soccer_analytics.dev_gold.dim_players_synced'
```

**Step 5: Restore PG grants**

Run grant SQL via psycopg2 for SP `be66af99-5296-4fd9-887a-c081bce38bfa`.

**Step 6: Verify**

Run: `.venv/Scripts/python.exe scripts/create_indexes.py --verify`
Expected: All existing indexes verified. No new indexes needed for dim_players (under 50K rows).

---

## Task 15: Final Verification

**Step 1: Run full test suite**

Run: `uv run ruff check src/ && uv run ruff format --check src/ && uv run pyright src/ && uv run pytest src/tests/ -v`
Expected: Zero violations, all tests PASS.

**Step 2: Run dbt build**

Run: `cd dbt_project && MSYS_NO_PATHCONV=1 python -c "import dbt.cli.main; dbt.cli.main.dbtRunner().invoke(['build', '--vars', '{entity_resolution_enabled: true, defcon_enabled: true, off_ball_xt_enabled: true}'])"`
Expected: All models build, all tests pass.

**Step 3: Verify Streamlit app**

Check that the Player Radar page still works correctly — existing `player_id` FK joins should be unaffected.

**Step 4: Commit any final fixes**

```bash
git add -A
git commit -m "chore: final verification fixes for Phase 14"
```
