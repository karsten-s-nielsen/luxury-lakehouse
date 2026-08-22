"""Vectorized StatsBomb-360 ``visible_area`` polygon parser for AC visibility coverage.

Builds the ``action_id -> polygon`` frame that ``silly_kicks.tracking.add_visible_area_coverage`` and
``add_action_context(visible_area=...)`` consume (silly-kicks 4.87.0; spec §7.1 / §7.5). SB360-only: the
source is the per-event ``visible_area`` polygon shipped in ``bronze.statsbomb_360`` — a JSON/Python-repr
array-of-vertices STRING, flat ``[x1, y1, x2, y2, ...]`` in RAW StatsBomb pitch coordinates.

Pure pandas/numpy + silly-kicks (NO pyspark): the production cogroup UDF (``ingestion.action_context``)
and any local path share this one impl, mirroring ``sb360_snapshots.build_sb360_snapshots`` and the
silly-kicks reference ``providers.statsbomb.shape_snapshots`` (which emits ONE row per ACTION — a polygon
is a per-action quantity — not one per player).

Coordinate system: the flat polygon is converted to SPADL vertices via the canonical
``polygon_to_spadl`` (StatsBomb 0-120x0-80 -> SPADL 0-105x0-68 with the cell-centre correction, NOT
clipped to the pitch). This makes the polygon coordinate-consistent with the SPADL actions the
visibility aggregators compare it against (goal fixed at x=105, pitch clip applied inside
``add_visible_area_coverage``). Default ``fidelity_version=1`` matches ``polygon_to_spadl`` /
``shape_snapshots``; SB360 AC is currently held/empty (ADR-058), so this affects no live data yet.

Join key (ADR-019): the emitted ``action_id`` matches ``actions_df.action_id``; the consumers
canonicalize BOTH sides via ``canonical_id`` before joining, so a raw-dtype dict miss (which reports
all-``no_polygon`` SILENTLY) cannot occur. A published-but-unusable polygon is emitted as an empty
``(0, 2)`` array so it reads as ``degenerate_polygon`` downstream — distinct from an absent one
(``no_polygon``), the distinction ``silly_kicks.tracking._visibility`` exists to keep (ADR-055).
"""

from __future__ import annotations

import json

import numpy as np
import pandas as pd
from silly_kicks.providers.statsbomb import polygon_to_spadl

_VISIBLE_AREA_COLUMNS = ["action_id", "polygon"]

# Bronze STRING sentinels that decode to "nothing published". A None/NaN cell, an empty string, or an
# empty/None/nan literal from a str()-serialized null all mean the same absence.
_EMPTY_TOKENS = frozenset({"", "[]", "none", "nan", "null"})


def _parse_flat_polygon(raw: object) -> list[float]:
    """Parse one bronze ``visible_area`` cell to a flat ``[x1, y1, ...]`` list; ``[]`` when unusable.

    Bronze stores the StatsBomb 360 ``visible_area`` as a JSON/Python-repr array-of-vertices STRING
    (e.g. ``"[41.2, 33.6, 50.1, 20.2]"``). Missing / malformed / non-list values yield ``[]`` — which
    ``polygon_to_spadl`` maps to an empty ``(0, 2)`` array (``degenerate_polygon`` downstream), never a
    raise, so one bad row cannot kill a multi-hour distributed pass. An already-parsed list/array
    (defensive: a caller that skipped the STRING serialization) is flattened too.
    """
    if raw is None or (isinstance(raw, float) and raw != raw):  # None or NaN
        return []
    if isinstance(raw, (list, tuple, np.ndarray)):
        try:
            return [float(v) for v in np.asarray(raw, dtype=float).ravel()]
        except (ValueError, TypeError):
            return []
    s = str(raw).strip()
    if s.lower() in _EMPTY_TOKENS:
        return []
    try:
        parsed = json.loads(s)
    except (ValueError, TypeError):
        return []
    if not isinstance(parsed, (list, tuple)):
        return []
    try:
        return [float(v) for v in np.asarray(parsed, dtype=float).ravel()]
    except (ValueError, TypeError):
        return []


def build_visible_area(
    actions_df: pd.DataFrame,
    sb360_raw_df: pd.DataFrame,
    *,
    fidelity_version: int = 1,
) -> pd.DataFrame:
    """Raw ``bronze.statsbomb_360`` rows -> ``action_id -> polygon`` frame (one row per action).

    ``sb360_raw_df`` carries ``id`` (the 360 ``event_uuid``) + ``visible_area`` (STRING), replicated
    across the freeze-frame's per-player rows — so it is de-duped to one polygon per event. Each event
    maps to ``action_id`` via ``actions_df.original_event_id`` (the LAST action wins on a dup, matching
    ``build_sb360_snapshots``'s ``dict(zip(...))`` keep-last semantics). Unmapped events are dropped.

    A row is emitted whenever something WAS published (``len(poly) or flat``) so a published-but-unusable
    polygon reads as ``degenerate_polygon``, distinct from an absent one (``no_polygon``). ``polygon`` is
    the ``(N, 2)`` SPADL-vertex ndarray (unclipped) that the visibility aggregators consume.

    Returns an empty ``["action_id", "polygon"]`` frame when either input is empty or the raw 360 df
    lacks the ``id`` / ``visible_area`` columns — callers pass ``visible_area=None`` in that case so the
    8 visibility columns fill NaN/None via ``build_output``.
    """
    if (
        sb360_raw_df is None
        or sb360_raw_df.empty
        or actions_df.empty
        or "visible_area" not in sb360_raw_df.columns
        or "id" not in sb360_raw_df.columns
        or "original_event_id" not in actions_df.columns
    ):
        return pd.DataFrame(columns=_VISIBLE_AREA_COLUMNS)

    ev = actions_df["original_event_id"].dropna()
    if ev.empty:
        return pd.DataFrame(columns=_VISIBLE_AREA_COLUMNS)
    ev_to_action = pd.DataFrame(
        {
            "id": ev.astype(str).to_numpy(),
            "action_id": actions_df.loc[ev.index, "action_id"].to_numpy(),
        }
    ).drop_duplicates("id", keep="last")  # MATCH build_sb360_snapshots: dict(zip(...)) keeps the LAST.

    # One polygon per event: ``visible_area`` is an event-level field replicated across the
    # freeze-frame's per-player bronze rows. Prefer a non-null value on the (rare) mixed row via a
    # STABLE sort, then keep one row per id.
    df = sb360_raw_df[["id", "visible_area"]].copy()
    df["id"] = df["id"].astype(str)
    df = df.assign(_has=df["visible_area"].notna()).sort_values("_has", ascending=False, kind="stable")
    df = df.drop_duplicates("id", keep="first").drop(columns="_has")

    df = df.merge(ev_to_action, on="id", how="inner")  # drops unmapped events
    if df.empty:
        return pd.DataFrame(columns=_VISIBLE_AREA_COLUMNS)

    rows: list[dict] = []
    for action_id, raw in zip(df["action_id"].to_numpy(), df["visible_area"].to_numpy(), strict=True):
        flat = _parse_flat_polygon(raw)
        poly = polygon_to_spadl(flat, fidelity_version=fidelity_version)
        if len(poly) or flat:  # emit whenever something WAS published (ADR-055 degenerate vs absent)
            rows.append({"action_id": action_id, "polygon": poly})

    return pd.DataFrame(rows, columns=_VISIBLE_AREA_COLUMNS)
