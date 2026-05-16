"""Single source of truth for native identifier format generators across all
5 SPADL data sources (StatsBomb, Wyscout, IDSSE, Metrica, SkillCorner).

ADR-018 — cross-table format-contract testing — requires that every value
flowing into a ``(provider, native_id)`` JOIN key has a single canonical
generator. Bronze writers + dim staging + applyInPandas UDFs all reach
into this module; format errors fail at construction time, not at
full-refresh dbt-build time.

Pure stdlib (re, typing). No Spark/dbt/pandas imports — runs in unit
tests, in bronze-writer Python paths, and in dbt analyses (via macro
parity tests). Adding a function here triggers ADR-018 maintenance:
every new function needs a corresponding format-contract test in
``src/tests/test_format_contract.py``.
"""

from __future__ import annotations

import re
from typing import Literal, NamedTuple

# ---------------------------------------------------------------------------
# IDSSE (DFL Bundesliga / Sportec)
# ---------------------------------------------------------------------------

_IDSSE_MATCH_ID_PATTERN = re.compile(r"^[A-Z0-9]+$")
_IDSSE_COMPETITION_ID_PATTERN = re.compile(r"^DFL-COM-[A-Z0-9]+$")


def idsse_native_match_id(raw_dfl_match_id: str) -> str:
    """Canonical IDSSE native match id — bare DFL MatchId (e.g. ``J03WMX``).

    Source of truth for the format that lands in:

    - ``bronze.idsse_events.match_id``
    - ``bronze.idsse_tracking.match_id``
    - ``bronze.spadl_actions.match_id_native`` (for IDSSE rows)
    - ``dim_matches.native_match_id`` (for IDSSE rows)

    Pre-2026-04-29 PR-LL2-Path-B-close-out, ``idsse.py`` erroneously
    prefixed this with ``idsse_`` (e.g. ``idsse_J03WMX``). The ADR-018
    format contract enforces the bare form — bronze writer must use
    this generator, and the dim staging side strips any residual prefix
    via ``regexp_replace(..., '^idsse_', '')`` for resilience.
    """
    if not _IDSSE_MATCH_ID_PATTERN.match(raw_dfl_match_id):
        raise ValueError(f"invalid IDSSE match id: {raw_dfl_match_id!r} (expected bare DFL MatchId like 'J03WMX')")
    return raw_dfl_match_id


def idsse_native_competition_id(raw_dfl_competition_id: str) -> str:
    """Canonical IDSSE native competition id — ``DFL-COM-XXXXXX``."""
    if not _IDSSE_COMPETITION_ID_PATTERN.match(raw_dfl_competition_id):
        raise ValueError(f"invalid IDSSE competition id: {raw_dfl_competition_id!r} (expected 'DFL-COM-XXXXXX' format)")
    return raw_dfl_competition_id


# ---------------------------------------------------------------------------
# Metrica (anonymised open-data sample)
# ---------------------------------------------------------------------------

_METRICA_MATCH_ID_PATTERN = re.compile(r"^Sample_Game_[0-9]+$")


def metrica_native_match_id(raw_metrica_match_id: str) -> str:
    """Canonical Metrica native match id — ``Sample_Game_N``."""
    if not _METRICA_MATCH_ID_PATTERN.match(raw_metrica_match_id):
        raise ValueError(f"invalid Metrica match id: {raw_metrica_match_id!r} (expected 'Sample_Game_N' format)")
    return raw_metrica_match_id


def metrica_native_team_id(match_id: str, side: Literal["home", "away"]) -> str:
    """Canonical Metrica native team id — ``metrica_<match>_<home|away>``.

    Source of truth for the format that lands in:

    - ``bronze.metrica_events.{home,away}_team_id_native``, ``team_id_native``
    - ``bronze.spadl_actions.{home_team_id_native, team_id_native}`` (Metrica rows)
    - ``dim_teams.native_team_id`` (Metrica rows; via ``stg_metrica__team_players``'s
      ``concat('metrica_', match_id, '_', side)`` pattern — same convention)

    Pre-2026-04-29 PR-LL2-Path-B-close-out, ``metrica_events.py`` emitted
    ``f'{match_id}-{side.title()}'`` (capital-Home, hyphen) which did not
    match dim_teams's lowercase-prefix-underscore convention. ADR-018-
    driven format contract enforces alignment.
    """
    if side not in ("home", "away"):
        raise ValueError(f"side must be 'home' or 'away', got {side!r}")
    metrica_native_match_id(match_id)  # validate match_id format too
    return f"metrica_{match_id}_{side}"


def metrica_native_competition_id() -> str:
    """Canonical Metrica native competition id — sample-data sentinel.

    Per ``stg_metrica__matches.sql:26`` + ``dim_competitions.sql`` metrica CTE
    (PR 5a, ADR-011) — single value across all Metrica rows.
    """
    return "metrica-sample"


def metrica_native_season_id() -> str:
    """Canonical Metrica native season id — sample-data sentinel."""
    return "metrica-open-2017"


# ---------------------------------------------------------------------------
# StatsBomb / Wyscout — numeric BIGINT natives, stringified at SPADL boundary
# ---------------------------------------------------------------------------
# These are added for completeness so the format-contract test parametrization
# is uniform across sources. The functions are simple cast-to-str wrappers
# but raise on non-positive or non-int input.


def statsbomb_native_match_id(raw_match_id: int) -> str:
    """Canonical StatsBomb native match id — stringified positive BIGINT."""
    if not isinstance(raw_match_id, int) or raw_match_id <= 0:
        raise ValueError(f"invalid StatsBomb match id: {raw_match_id!r} (expected positive int)")
    return str(raw_match_id)


def wyscout_native_match_id(raw_match_id: int) -> str:
    """Canonical Wyscout native match id — stringified positive BIGINT."""
    if not isinstance(raw_match_id, int) or raw_match_id <= 0:
        raise ValueError(f"invalid Wyscout match id: {raw_match_id!r} (expected positive int)")
    return str(raw_match_id)


# ---------------------------------------------------------------------------
# Player ID generators - PR-LL3 (S2, ADR-018)
# ---------------------------------------------------------------------------


def statsbomb_native_player_id(raw_player_id: int) -> str:
    """Canonical StatsBomb native player id - stringified positive BIGINT."""
    if not isinstance(raw_player_id, int) or raw_player_id <= 0:
        raise ValueError(f"invalid StatsBomb player id: {raw_player_id!r} (expected positive int)")
    return str(raw_player_id)


def wyscout_native_player_id(raw_player_id: int) -> str:
    """Canonical Wyscout native player id - stringified positive BIGINT."""
    if not isinstance(raw_player_id, int) or raw_player_id <= 0:
        raise ValueError(f"invalid Wyscout player id: {raw_player_id!r} (expected positive int)")
    return str(raw_player_id)


_IDSSE_PLAYER_ID_PATTERN = re.compile(r"^DFL-OBJ-[A-Z0-9]+$")


def idsse_native_player_id(raw_dfl_player_id: str) -> str:
    """Canonical IDSSE native player id - ``DFL-OBJ-XXXXXX``."""
    if not _IDSSE_PLAYER_ID_PATTERN.match(raw_dfl_player_id):
        raise ValueError(f"invalid IDSSE player id: {raw_dfl_player_id!r} (expected 'DFL-OBJ-XXXXXX' format)")
    return raw_dfl_player_id


_METRICA_PLAYER_ID_PATTERN = re.compile(r"^Player[0-9]+$")


def metrica_native_player_id(raw_metrica_player_id: str) -> str:
    """Canonical Metrica native player id - ``PlayerN`` (anonymous)."""
    if not _METRICA_PLAYER_ID_PATTERN.match(raw_metrica_player_id):
        raise ValueError(f"invalid Metrica player id: {raw_metrica_player_id!r} (expected 'PlayerN' format)")
    return raw_metrica_player_id


# ---------------------------------------------------------------------------
# Team ID generators - PR-LL3 (S7, ADR-018)
# ---------------------------------------------------------------------------
# SB/WS team IDs are stringified positive BIGINTs (same shape as match/player).
# IDSSE team IDs are DFL CLU strings: DFL-CLU-XXXXXX.
# Metrica team IDs already have a generator (metrica_native_team_id above).


def statsbomb_native_team_id(raw_team_id: int) -> str:
    """Canonical StatsBomb native team id - stringified positive BIGINT."""
    if not isinstance(raw_team_id, int) or raw_team_id <= 0:
        raise ValueError(f"invalid StatsBomb team id: {raw_team_id!r} (expected positive int)")
    return str(raw_team_id)


def wyscout_native_team_id(raw_team_id: int) -> str:
    """Canonical Wyscout native team id - stringified positive BIGINT."""
    if not isinstance(raw_team_id, int) or raw_team_id <= 0:
        raise ValueError(f"invalid Wyscout team id: {raw_team_id!r} (expected positive int)")
    return str(raw_team_id)


_IDSSE_TEAM_ID_PATTERN = re.compile(r"^DFL-CLU-[A-Z0-9]+$")


def idsse_native_team_id(raw_dfl_team_id: str) -> str:
    """Canonical IDSSE native team id - ``DFL-CLU-XXXXXX``."""
    if not _IDSSE_TEAM_ID_PATTERN.match(raw_dfl_team_id):
        raise ValueError(f"invalid IDSSE team id: {raw_dfl_team_id!r} (expected 'DFL-CLU-XXXXXX' format)")
    return raw_dfl_team_id


# ---------------------------------------------------------------------------
# SkillCorner (A-League broadcast tracking)
# ---------------------------------------------------------------------------

_SKILLCORNER_NUMERIC_ID_PATTERN = re.compile(r"^[0-9]+$")


def skillcorner_native_match_id(raw_match_id: str | int) -> str:
    """Canonical SkillCorner native match id -- stringified positive integer."""
    s = str(raw_match_id)
    if not _SKILLCORNER_NUMERIC_ID_PATTERN.match(s):
        raise ValueError(f"invalid SkillCorner match id: {raw_match_id!r} (expected numeric string)")
    return s


def skillcorner_native_player_id(raw_player_id: str | int) -> str:
    """Canonical SkillCorner native player id -- stringified positive integer."""
    s = str(raw_player_id)
    if not _SKILLCORNER_NUMERIC_ID_PATTERN.match(s):
        raise ValueError(f"invalid SkillCorner player id: {raw_player_id!r} (expected numeric string)")
    return s


def skillcorner_native_team_id(raw_team_id: str | int) -> str:
    """Canonical SkillCorner native team id -- stringified positive integer."""
    s = str(raw_team_id)
    if not _SKILLCORNER_NUMERIC_ID_PATTERN.match(s):
        raise ValueError(f"invalid SkillCorner team id: {raw_team_id!r} (expected numeric string)")
    return s


# ---------------------------------------------------------------------------
# Type-safe identifier wrappers — PR-LL3 S7
# ---------------------------------------------------------------------------
# Non-breaking additions — existing bare-string functions remain. These
# wrappers carry (provider, value) together for downstream type safety.
# No Pydantic — src/shared/ is stdlib-only per CLAUDE.md.


class NativeMatchId(NamedTuple):
    """Type-safe wrapper for a native match identifier."""

    provider: str
    value: str

    @classmethod
    def statsbomb(cls, raw: int) -> NativeMatchId:
        return cls(provider="statsbomb", value=statsbomb_native_match_id(raw))

    @classmethod
    def wyscout(cls, raw: int) -> NativeMatchId:
        return cls(provider="wyscout", value=wyscout_native_match_id(raw))

    @classmethod
    def idsse(cls, raw: str) -> NativeMatchId:
        return cls(provider="idsse", value=idsse_native_match_id(raw))

    @classmethod
    def metrica(cls, raw: str) -> NativeMatchId:
        return cls(provider="metrica", value=metrica_native_match_id(raw))

    @classmethod
    def skillcorner(cls, raw: str | int) -> NativeMatchId:
        return cls(provider="skillcorner", value=skillcorner_native_match_id(raw))


class NativePlayerId(NamedTuple):
    """Type-safe wrapper for a native player identifier."""

    provider: str
    value: str

    @classmethod
    def statsbomb(cls, raw: int) -> NativePlayerId:
        return cls(provider="statsbomb", value=statsbomb_native_player_id(raw))

    @classmethod
    def wyscout(cls, raw: int) -> NativePlayerId:
        return cls(provider="wyscout", value=wyscout_native_player_id(raw))

    @classmethod
    def idsse(cls, raw: str) -> NativePlayerId:
        return cls(provider="idsse", value=idsse_native_player_id(raw))

    @classmethod
    def metrica(cls, raw: str) -> NativePlayerId:
        return cls(provider="metrica", value=metrica_native_player_id(raw))

    @classmethod
    def skillcorner(cls, raw: str | int) -> NativePlayerId:
        return cls(provider="skillcorner", value=skillcorner_native_player_id(raw))


class NativeTeamId(NamedTuple):
    """Type-safe wrapper for a native team identifier."""

    provider: str
    value: str

    @classmethod
    def statsbomb(cls, raw: int) -> NativeTeamId:
        return cls(provider="statsbomb", value=statsbomb_native_team_id(raw))

    @classmethod
    def wyscout(cls, raw: int) -> NativeTeamId:
        return cls(provider="wyscout", value=wyscout_native_team_id(raw))

    @classmethod
    def idsse(cls, raw: str) -> NativeTeamId:
        return cls(provider="idsse", value=idsse_native_team_id(raw))

    @classmethod
    def metrica(cls, match_id: str, side: Literal["home", "away"]) -> NativeTeamId:
        return cls(provider="metrica", value=metrica_native_team_id(match_id, side))

    @classmethod
    def skillcorner(cls, raw: str | int) -> NativeTeamId:
        return cls(provider="skillcorner", value=skillcorner_native_team_id(raw))
