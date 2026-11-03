"""Kimball surrogate keys (spec §14.1, v1 ADR-011).

`xxhash64(concat_ws('|', provider, cast(native_id as string)))` with seed 42, as a signed 64-bit integer: the formula of
v1's dbt macros `generate_*_key`. A NULL native id gives a NULL key, as in v1. One Python generator computes every key
at bronze write time; dbt never hashes. Check value: `match_key("idsse", "J03WMX") == 4203055498971744044`.
"""

from __future__ import annotations

from typing import Final

import xxhash

from lakehouse.ids.native import PROVIDERS, Provider

_SEED: Final = 42
_TWO_63: Final = 1 << 63
_TWO_64: Final = 1 << 64


def surrogate_key(provider: Provider, native_id: str | None) -> int | None:
    """The signed 64-bit key of (provider, native_id); None when native_id is None."""
    if provider not in PROVIDERS:
        raise ValueError(f"unknown provider {provider!r}; expected one of {sorted(PROVIDERS)}")
    if native_id is None:
        return None
    unsigned = xxhash.xxh64_intdigest(f"{provider}|{native_id}".encode(), seed=_SEED)
    return unsigned - _TWO_64 if unsigned >= _TWO_63 else unsigned


def match_key(provider: Provider, native_match_id: str) -> int:
    """The match key. A match always has a native id, so an empty one is an error."""
    if not native_match_id:
        raise ValueError(f"empty native match id for provider {provider!r}")
    key = surrogate_key(provider, native_match_id)
    if key is None:  # unreachable: native_match_id is a non-empty string
        raise ValueError("match key is None")
    return key


def player_key(provider: Provider, native_player_id: str | None) -> int | None:
    return surrogate_key(provider, native_player_id)


def team_key(provider: Provider, native_team_id: str | None) -> int | None:
    return surrogate_key(provider, native_team_id)


def competition_key(provider: Provider, native_competition_id: str | None) -> int | None:
    return surrogate_key(provider, native_competition_id)


def season_key(provider: Provider, native_season_id: str | None) -> int | None:
    """The season key at competition-season grain; build the id with `native.native_season_id`."""
    return surrogate_key(provider, native_season_id)
