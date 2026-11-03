"""Native identifier formats: the single source of truth for every (provider, native_id) key (spec §14; v1 ADR-018).

Copied from v1 `src/shared/identifiers.py` with its docstrings. Two changes from v1: the Metrica generators are not
carried over (Metrica is not a v2 provider), and v2 adds competition-id generators for StatsBomb, Wyscout and
SkillCorner, plus `native_season_id` (spec §14.4). Every function here has a format-contract test in
`tests/contract/test_format_contract.py`.
"""

from __future__ import annotations

import re
from typing import Final, Literal, NamedTuple

Provider = Literal["statsbomb", "wyscout", "idsse", "skillcorner", "gradientsports"]
Channel = Literal["public", "pining"]

# Rank for canonical pointers, best first (spec §14.3).
PROVIDER_RANK: Final[tuple[Provider, ...]] = ("statsbomb", "wyscout", "idsse", "skillcorner", "gradientsports")
PROVIDERS: Final[frozenset[str]] = frozenset(PROVIDER_RANK)

# ---------------------------------------------------------------------------
# IDSSE (DFL Bundesliga / Sportec)
# ---------------------------------------------------------------------------

_IDSSE_MATCH_ID_PATTERN = re.compile(r"^[A-Z0-9]+$")
_IDSSE_COMPETITION_ID_PATTERN = re.compile(r"^DFL-COM-[A-Z0-9]+$")


def idsse_native_match_id(raw_dfl_match_id: str) -> str:
    """Canonical IDSSE native match id — bare DFL MatchId (e.g. ``J03WMX``).

    v1 history: before 2026-04-29 the v1 writer prefixed this with ``idsse_`` (e.g. ``idsse_J03WMX``). The format
    contract enforces the bare form.
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
# StatsBomb / Wyscout — numeric BIGINT natives, stringified at the key boundary
# ---------------------------------------------------------------------------
# Simple cast-to-str wrappers that raise on non-positive or non-int input.


def _positive_int(raw: int, label: str) -> str:
    # bool is an int subclass; a True/False id is always a bug.
    # Ids arrive from parsed source JSON, so the type is checked at run time too.
    if not isinstance(raw, int) or isinstance(raw, bool) or raw <= 0:  # pyright: ignore[reportUnnecessaryIsInstance]
        raise ValueError(f"invalid {label}: {raw!r} (expected positive int)")
    return str(raw)


def statsbomb_native_match_id(raw_match_id: int) -> str:
    """Canonical StatsBomb native match id — stringified positive BIGINT."""
    return _positive_int(raw_match_id, "StatsBomb match id")


def wyscout_native_match_id(raw_match_id: int) -> str:
    """Canonical Wyscout native match id — stringified positive BIGINT."""
    return _positive_int(raw_match_id, "Wyscout match id")


def statsbomb_native_player_id(raw_player_id: int) -> str:
    """Canonical StatsBomb native player id - stringified positive BIGINT."""
    return _positive_int(raw_player_id, "StatsBomb player id")


def wyscout_native_player_id(raw_player_id: int) -> str:
    """Canonical Wyscout native player id - stringified positive BIGINT."""
    return _positive_int(raw_player_id, "Wyscout player id")


def statsbomb_native_team_id(raw_team_id: int) -> str:
    """Canonical StatsBomb native team id - stringified positive BIGINT."""
    return _positive_int(raw_team_id, "StatsBomb team id")


def wyscout_native_team_id(raw_team_id: int) -> str:
    """Canonical Wyscout native team id - stringified positive BIGINT."""
    return _positive_int(raw_team_id, "Wyscout team id")


def statsbomb_native_competition_id(raw_competition_id: int) -> str:
    """Canonical StatsBomb native competition id - stringified positive BIGINT (v2 addition)."""
    return _positive_int(raw_competition_id, "StatsBomb competition id")


def wyscout_native_competition_id(raw_competition_id: int) -> str:
    """Canonical Wyscout native competition id - stringified positive BIGINT (v2 addition)."""
    return _positive_int(raw_competition_id, "Wyscout competition id")


_IDSSE_PLAYER_ID_PATTERN = re.compile(r"^DFL-OBJ-[A-Z0-9]+$")


def idsse_native_player_id(raw_dfl_player_id: str) -> str:
    """Canonical IDSSE native player id - ``DFL-OBJ-XXXXXX``."""
    if not _IDSSE_PLAYER_ID_PATTERN.match(raw_dfl_player_id):
        raise ValueError(f"invalid IDSSE player id: {raw_dfl_player_id!r} (expected 'DFL-OBJ-XXXXXX' format)")
    return raw_dfl_player_id


_IDSSE_TEAM_ID_PATTERN = re.compile(r"^DFL-CLU-[A-Z0-9]+$")


def idsse_native_team_id(raw_dfl_team_id: str) -> str:
    """Canonical IDSSE native team id - ``DFL-CLU-XXXXXX``."""
    if not _IDSSE_TEAM_ID_PATTERN.match(raw_dfl_team_id):
        raise ValueError(f"invalid IDSSE team id: {raw_dfl_team_id!r} (expected 'DFL-CLU-XXXXXX' format)")
    return raw_dfl_team_id


# ---------------------------------------------------------------------------
# SkillCorner and Gradient Sports — numeric strings
# ---------------------------------------------------------------------------

_NUMERIC_ID_PATTERN = re.compile(r"^[0-9]+$")


def _numeric_string(raw: str | int, label: str) -> str:
    if isinstance(raw, bool):
        raise ValueError(f"invalid {label}: {raw!r} (expected numeric string)")
    text = str(raw)
    if not _NUMERIC_ID_PATTERN.match(text):
        raise ValueError(f"invalid {label}: {raw!r} (expected numeric string)")
    return text


def skillcorner_native_match_id(raw_match_id: str | int) -> str:
    """Canonical SkillCorner native match id -- stringified positive integer."""
    return _numeric_string(raw_match_id, "SkillCorner match id")


def skillcorner_native_player_id(raw_player_id: str | int) -> str:
    """Canonical SkillCorner native player id -- stringified positive integer."""
    return _numeric_string(raw_player_id, "SkillCorner player id")


def skillcorner_native_team_id(raw_team_id: str | int) -> str:
    """Canonical SkillCorner native team id -- stringified positive integer."""
    return _numeric_string(raw_team_id, "SkillCorner team id")


def skillcorner_native_competition_id(raw_competition_id: str | int) -> str:
    """Canonical SkillCorner native competition id -- stringified positive integer (v2 addition)."""
    return _numeric_string(raw_competition_id, "SkillCorner competition id")


def gradientsports_native_match_id(raw_match_id: str | int) -> str:
    """Canonical Gradient Sports native match id -- stringified positive integer."""
    return _numeric_string(raw_match_id, "Gradient Sports match id")


def gradientsports_native_player_id(raw_player_id: str | int) -> str:
    """Canonical Gradient Sports native player id -- stringified positive integer."""
    return _numeric_string(raw_player_id, "Gradient Sports player id")


def gradientsports_native_team_id(raw_team_id: str | int) -> str:
    """Canonical Gradient Sports native team id -- stringified positive integer."""
    return _numeric_string(raw_team_id, "Gradient Sports team id")


def gradientsports_native_competition_id(raw_competition_id: str | int) -> str:
    """Canonical Gradient Sports native competition id -- stringified positive integer."""
    return _numeric_string(raw_competition_id, "Gradient Sports competition id")


# ---------------------------------------------------------------------------
# Seasons (spec §14.4): competition-season grain
# ---------------------------------------------------------------------------


def native_season_id(provider: Provider, native_competition_id: str, season: str) -> str:
    """Native season id at competition-season grain: ``<competition>:<season>``.

    StatsBomb reuses one season id across competitions (season 281 is 2023/2024 for five competitions), so the season
    alone is not a key. Neither part may hold ``|`` (the key delimiter) or ``:`` (this separator).
    """
    if provider not in PROVIDERS:
        raise ValueError(f"unknown provider {provider!r}")
    for part, label in ((native_competition_id, "competition"), (season, "season")):
        if not part or "|" in part or ":" in part:
            raise ValueError(f"invalid {label} part for a season id: {part!r} (empty, or holds '|' or ':')")
    return f"{native_competition_id}:{season}"


# ---------------------------------------------------------------------------
# Type-safe identifier wrappers (provider, value)
# ---------------------------------------------------------------------------


class NativeMatchId(NamedTuple):
    """Type-safe wrapper for a native match identifier."""

    provider: Provider
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
    def skillcorner(cls, raw: str | int) -> NativeMatchId:
        return cls(provider="skillcorner", value=skillcorner_native_match_id(raw))

    @classmethod
    def gradientsports(cls, raw: str | int) -> NativeMatchId:
        return cls(provider="gradientsports", value=gradientsports_native_match_id(raw))


class NativePlayerId(NamedTuple):
    """Type-safe wrapper for a native player identifier."""

    provider: Provider
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
    def skillcorner(cls, raw: str | int) -> NativePlayerId:
        return cls(provider="skillcorner", value=skillcorner_native_player_id(raw))

    @classmethod
    def gradientsports(cls, raw: str | int) -> NativePlayerId:
        return cls(provider="gradientsports", value=gradientsports_native_player_id(raw))


class NativeTeamId(NamedTuple):
    """Type-safe wrapper for a native team identifier."""

    provider: Provider
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
    def skillcorner(cls, raw: str | int) -> NativeTeamId:
        return cls(provider="skillcorner", value=skillcorner_native_team_id(raw))

    @classmethod
    def gradientsports(cls, raw: str | int) -> NativeTeamId:
        return cls(provider="gradientsports", value=gradientsports_native_team_id(raw))


class NativeCompetitionId(NamedTuple):
    """Type-safe wrapper for a native competition identifier."""

    provider: Provider
    value: str

    @classmethod
    def statsbomb(cls, raw: int) -> NativeCompetitionId:
        return cls(provider="statsbomb", value=statsbomb_native_competition_id(raw))

    @classmethod
    def wyscout(cls, raw: int) -> NativeCompetitionId:
        return cls(provider="wyscout", value=wyscout_native_competition_id(raw))

    @classmethod
    def idsse(cls, raw: str) -> NativeCompetitionId:
        return cls(provider="idsse", value=idsse_native_competition_id(raw))

    @classmethod
    def skillcorner(cls, raw: str | int) -> NativeCompetitionId:
        return cls(provider="skillcorner", value=skillcorner_native_competition_id(raw))

    @classmethod
    def gradientsports(cls, raw: str | int) -> NativeCompetitionId:
        return cls(provider="gradientsports", value=gradientsports_native_competition_id(raw))
