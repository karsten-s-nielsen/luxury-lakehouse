"""Channel id mapping (spec §7.3): every channel's id form maps to one canonical native id before any key is computed.

The inventory behind these rules is `tests/fixtures/channels/inventory.json`. The same real match gets the same
native id, and so the same key, whatever channel delivered it.
"""

from __future__ import annotations

import re
from typing import Final

from lakehouse.ids.native import (
    Channel,
    Provider,
    gradientsports_native_match_id,
    idsse_native_match_id,
    skillcorner_native_match_id,
    statsbomb_native_match_id,
    wyscout_native_match_id,
)

_PINING_IDSSE_PREFIX: Final = "DFL-MAT-"
_DIGITS: Final = re.compile(r"^[0-9]+$")


def _int_from(raw_id: str | int, label: str) -> int:
    if isinstance(raw_id, int) and not isinstance(raw_id, bool):
        return raw_id
    if isinstance(raw_id, str) and _DIGITS.match(raw_id):
        return int(raw_id)
    raise ValueError(f"invalid {label}: {raw_id!r} (expected an integer or a string of digits)")


def canonical_native_match_id(provider: Provider, channel: Channel, raw_id: str | int) -> str:
    """The canonical native match id for an id as one channel delivers it.

    - ``idsse``: pining sends ``DFL-MAT-<MatchId>``, the public dataset the bare ``<MatchId>``; canonical is bare.
    - ``statsbomb``: open data sends an integer, pining a string of the same digits; canonical is the decimal string.
    - ``skillcorner`` and ``gradientsports``: both channels send the same numeric string (identity).
    - ``wyscout``: public channel only.
    """
    match provider:
        case "idsse":
            if channel == "pining":
                if not isinstance(raw_id, str) or not raw_id.startswith(_PINING_IDSSE_PREFIX):
                    raise ValueError(f"invalid pining IDSSE match id: {raw_id!r} (expected 'DFL-MAT-<MatchId>')")
                return idsse_native_match_id(raw_id.removeprefix(_PINING_IDSSE_PREFIX))
            if not isinstance(raw_id, str):
                raise ValueError(f"invalid IDSSE match id: {raw_id!r} (expected a string)")
            return idsse_native_match_id(raw_id)
        case "statsbomb":
            return statsbomb_native_match_id(_int_from(raw_id, "StatsBomb match id"))
        case "skillcorner":
            return skillcorner_native_match_id(raw_id)
        case "gradientsports":
            return gradientsports_native_match_id(raw_id)
        case "wyscout":
            if channel != "public":
                raise ValueError(f"wyscout has no {channel!r} channel")
            return wyscout_native_match_id(_int_from(raw_id, "Wyscout match id"))
