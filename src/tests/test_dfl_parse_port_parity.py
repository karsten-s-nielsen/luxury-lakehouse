"""Live cross-repo contract: the silly-kicks DFL parse port reproduces a frozen golden.

ADR-031 T3 / Gate B. silly-kicks 4.30.0 ships ``silly_kicks.providers.sportec`` — a verbatim lift
of the lakehouse's IDSSE/Sportec DFL parser+shaper, pinned at lakehouse commit ``0efac60``.

Under **delete-and-depend** the lakehouse copies of that parser+shaper are now DELETED
(``ingestion.idsse`` parser functions, ``ingestion.spadl_adapter.adapt_idsse_events_for_silly_kicks``
+ the two ``derive_idsse_home_team_start_left*`` helpers, and
``analytics.action_context.convert._bronze_idsse_to_sportec_input``). So the original
differential cross-comparison (lakehouse-parser == port) no longer has a lakehouse side to compare
against — there is nothing left in this repo that re-parses DFL XML.

This test replaces that cross-comparison with a **committed port-output golden**: the five seams the
parity test used to compare (match_info fields, tracking bronze df, tracking native df, events bronze
df, and the derive bool) are captured to disk and committed. The committed test LOADS those goldens
and asserts the port still reproduces them, feeding the SAME synthetic DFL XML (reused from
``tests.test_idsse``) that the lift was pinned against. If silly-kicks ever regresses the port at the
lakehouse's pinned ``0efac60`` shape, this goes RED — it is the live cross-repo guard.

Regenerate the goldens (only when the port output legitimately changes — e.g. an intentional,
reviewed silly-kicks port revision) with::

    CAPTURE_DFL_GOLDEN=1 uv run --inexact --extra spadl --extra analytics python -m pytest \\
        src/tests/test_dfl_parse_port_parity.py -q

That writes the parquet/JSON artifacts under ``fixtures/dfl_parse_port_golden/`` and skips the
assertions; commit the regenerated artifacts. The default (no env var) run LOADS and asserts.

NOTE on smoothing: the port emits RAW bronze; production ``ingest_idsse`` applies
``_smooth_tracking`` AFTER the parser and BEFORE persisting. The golden captures the port's PRE-smooth
bronze — smoothing is a consumer-side stage, out of the contract (ADR-031 §4.5).
"""

from __future__ import annotations

import json
import os
import tempfile
from pathlib import Path

import pandas as pd
import pytest
import silly_kicks.providers.sportec as port

from tests.test_idsse import _EVENTS_XML, _MATCH_INFO_XML, _POSITIONS_XML

_MID = "J03WMX"
_GOLDEN_DIR = Path(__file__).parent / "fixtures" / "dfl_parse_port_golden"
_TRACKING_BRONZE_PARQUET = _GOLDEN_DIR / "tracking_bronze.parquet"
_TRACKING_NATIVE_PARQUET = _GOLDEN_DIR / "tracking_native.parquet"
_EVENTS_BRONZE_PARQUET = _GOLDEN_DIR / "events_bronze.parquet"
_SCALARS_JSON = _GOLDEN_DIR / "match_info_and_derive.json"

_CAPTURE = bool(os.environ.get("CAPTURE_DFL_GOLDEN"))


def _xml(content: str) -> str:
    p = Path(tempfile.mkdtemp()) / "dfl.xml"
    p.write_text(content, encoding="utf-8")
    return str(p)


@pytest.fixture(scope="module")
def paths() -> dict[str, str]:
    return {"info": _xml(_MATCH_INFO_XML), "pos": _xml(_POSITIONS_XML), "events": _xml(_EVENTS_XML)}


def _port_match_info(paths: dict[str, str]):  # type: ignore[no-untyped-def]
    return port.parse_dfl_match_info(paths["info"])


def _port_tracking_bronze(paths: dict[str, str]) -> pd.DataFrame:
    mi = _port_match_info(paths)
    return pd.DataFrame(port.parse_dfl_tracking(paths["pos"], match_info=mi, match_id=_MID))


def _port_tracking_native(paths: dict[str, str]) -> pd.DataFrame:
    mi = _port_match_info(paths)
    bronze = port.parse_dfl_tracking(paths["pos"], match_info=mi, match_id=_MID)
    return pd.DataFrame(port.shape_tracking_to_native(bronze))


def _port_events_bronze(paths: dict[str, str]) -> pd.DataFrame:
    mi = _port_match_info(paths)
    return pd.DataFrame(port.parse_dfl_events(paths["events"], match_info=mi, match_id=_MID))


def _port_scalars(paths: dict[str, str]) -> dict[str, object]:
    mi = _port_match_info(paths)
    events = _port_events_bronze(paths)
    return {
        "home_team_id": mi.home_team_id,
        "away_team_id": mi.away_team_id,
        "competition_id": mi.competition_id,
        "season_id": mi.season_id,
        "player_team_map": dict(mi.player_team_map),
        "gk_player_ids": sorted(mi.gk_player_ids),
        "derive_home_team_start_left": bool(port.derive_idsse_home_team_start_left(events, mi.home_team_id)),
    }


def _assert_frame_golden(actual: pd.DataFrame, golden_path: Path, sort_keys: list[str]) -> None:
    """Semantic equality vs the committed golden: identical column SET, then row/column-sorted value
    equality (dtype-relaxed, 1e-9 float tolerance — the port lift is verbatim so values are
    byte-identical, but parquet round-trip + pandas-version dtype nuances on all-None object columns
    shouldn't make the contract brittle)."""
    assert golden_path.exists(), (
        f"missing golden {golden_path.name} — regenerate with CAPTURE_DFL_GOLDEN=1 (see module docstring)"
    )
    golden = pd.read_parquet(golden_path)
    assert sorted(actual.columns) == sorted(golden.columns), (
        f"column-set drift vs golden — only-port={sorted(set(actual.columns) - set(golden.columns))}, "
        f"only-golden={sorted(set(golden.columns) - set(actual.columns))}"
    )
    keys = [c for c in sort_keys if c in golden.columns]

    def _norm(df: pd.DataFrame) -> pd.DataFrame:
        out = pd.DataFrame(df).reindex(sorted(df.columns), axis=1)
        out = out.sort_values(keys, kind="stable").reset_index(drop=True)
        # Unify null-likes (np.nan vs None) on object columns: the parquet
        # round-trip can swap one for the other on all-None columns, and pandas
        # >= a future version will treat nan != None in assert_frame_equal. Map
        # every null to a single sentinel so the contract stays null-rep-agnostic.
        obj_cols = out.select_dtypes(include="object").columns
        for col in obj_cols:
            out[col] = out[col].where(out[col].notna(), other="<NULL>")
        return out

    pd.testing.assert_frame_equal(_norm(actual), _norm(golden), check_dtype=False, check_exact=False, atol=1e-9)


def _maybe_capture(paths: dict[str, str]) -> None:
    """One-shot golden capture, guarded by CAPTURE_DFL_GOLDEN. No-op on the committed test path."""
    _GOLDEN_DIR.mkdir(parents=True, exist_ok=True)
    _port_tracking_bronze(paths).to_parquet(_TRACKING_BRONZE_PARQUET, index=False)
    _port_tracking_native(paths).to_parquet(_TRACKING_NATIVE_PARQUET, index=False)
    _port_events_bronze(paths).to_parquet(_EVENTS_BRONZE_PARQUET, index=False)
    _SCALARS_JSON.write_text(json.dumps(_port_scalars(paths), indent=2, sort_keys=True), encoding="utf-8")


@pytest.mark.skipif(not _CAPTURE, reason="capture-only; set CAPTURE_DFL_GOLDEN=1 to regenerate goldens")
def test_capture_dfl_port_golden(paths: dict[str, str]) -> None:
    """Regenerate the committed goldens from live port output. Skipped unless CAPTURE_DFL_GOLDEN=1."""
    _maybe_capture(paths)


def test_match_info_and_derive_golden(paths: dict[str, str]) -> None:
    if _CAPTURE:
        pytest.skip("capture run — assertions skipped")
    assert _SCALARS_JSON.exists(), "missing match_info/derive golden — regenerate with CAPTURE_DFL_GOLDEN=1"
    golden = json.loads(_SCALARS_JSON.read_text(encoding="utf-8"))
    assert _port_scalars(paths) == golden


def test_tracking_bronze_golden(paths: dict[str, str]) -> None:
    if _CAPTURE:
        pytest.skip("capture run — assertions skipped")
    _assert_frame_golden(
        _port_tracking_bronze(paths),
        _TRACKING_BRONZE_PARQUET,
        ["period", "frame", "is_goalkeeper", "team", "player_id"],
    )


def test_tracking_native_golden(paths: dict[str, str]) -> None:
    if _CAPTURE:
        pytest.skip("capture run — assertions skipped")
    _assert_frame_golden(
        _port_tracking_native(paths),
        _TRACKING_NATIVE_PARQUET,
        ["period_id", "frame_id", "is_ball", "team_id", "player_id"],
    )


def test_events_bronze_golden(paths: dict[str, str]) -> None:
    if _CAPTURE:
        pytest.skip("capture run — assertions skipped")
    _assert_frame_golden(
        _port_events_bronze(paths),
        _EVENTS_BRONZE_PARQUET,
        ["period", "event_id", "team_id_native", "player_id"],
    )
