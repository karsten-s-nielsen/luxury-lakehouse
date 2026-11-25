"""The frames and ball key reads push the match filter down (perf spec §7.4a; plan Amendment 2026-11-23 (b) Task 8c).

Real dbt views over seeded bronze. The lake holds three gradientsports match records: 101 (no pointer), and 202 + 303,
which both point to canonical 202 (a canonical match fed by two record keys). An idsse record 505 has ball rows with
a possession team, so the ball view's team lookup is exercised too.

- The pushed reads (`canonical_match_key = ? AND list_contains(?::BIGINT[], match_key)`) return exactly the rows of
  the canonical-only reads (§7.4a item 5).
- The pushed gradientsports frames read filters the bronze table scan on `match_key`, below the dedup window, so a
  rewrite to a form that does not push down fails here and not only in the Mac bench.
- A duplicate pointer fails the reads that meet it with DuckDB's multiple-rows error, and an unrelated match's pushed
  reads still work (owner ruling 2026-11-23: fail loud + writer guard).
"""

from __future__ import annotations

import json
from collections.abc import Iterator
from datetime import UTC, datetime
from typing import Any, cast

import duckdb
import pytest
from lakekit import LakeKit

from lakehouse.catalog.working import WorkingCatalog
from lakehouse.compute import input_key as ik
from lakehouse.compute.frames import FRAME_INPUT_READS
from lakehouse.config.model import ResourceCaps, load_defaults
from lakehouse.resolve.links import run_resolution
from lakehouse.runs.dbt import run_dbt

CAPS = ResourceCaps(threads=1, memory_limit="1GB", arrow_cpus=1)
GS = "gradientsports"
LONE, CANONICAL, FEEDER, IDSSE = 101, 202, 303, 505
HOME_TEAM = 900
FRAMES_READ, BALL_READ = FRAME_INPUT_READS[0], FRAME_INPUT_READS[1]
_AT = datetime(2026, 11, 23, tzinfo=UTC)
_MULTIPLE_ROWS = "More than one row returned by a subquery"


def _seed_gradientsports_matches(conn: duckdb.DuckDBPyConnection) -> None:
    """Only the columns the tracking views read. Seeded after resolution, which reads more of this table (the
    competition columns) to build match records; these tests need the views, not those records."""
    conn.execute(
        "CREATE TABLE lake.main.gradientsports_matches (match_key BIGINT, home_team_key BIGINT, away_team_key BIGINT, "
        "home_team_start_left BOOLEAN, home_team_start_left_extra_time BOOLEAN, fps DOUBLE)"
    )
    conn.executemany(
        "INSERT INTO lake.main.gradientsports_matches VALUES (?, ?, ?, ?, NULL, 29.97)",
        [[LONE, 800, 801, True], [CANONICAL, HOME_TEAM, 901, True], [FEEDER, HOME_TEAM, 901, False]],
    )


def _seed_bronze(conn: duckdb.DuckDBPyConnection) -> None:
    conn.execute(
        "CREATE TABLE lake.main.gradientsports_tracking_players (match_key BIGINT, period BIGINT, frame BIGINT, "
        "record_index BIGINT, player_key BIGINT, team_side VARCHAR, x_mm BIGINT, y_mm BIGINT, is_visible BOOLEAN, "
        "smoothed BOOLEAN)"
    )
    players = {LONE: (1, 2), CANONICAL: (3, 4), FEEDER: (5, 6)}
    values: list[str] = []
    for mk, (home, away) in players.items():
        for frame in (100, 101):
            for record_index, offset in ((frame * 10, 0), (frame * 10 + 1, 7)):  # a later copy of the frame
                for player, side in ((home, "home"), (away, "away")):
                    x = mk + frame + player + offset
                    values.append(f"({mk}, 1, {frame}, {record_index}, {player}, '{side}', {x}, {-x}, true, false)")
                    values.append(f"({mk}, 1, {frame}, {record_index}, {player}, '{side}', {x + 1}, {-x}, true, true)")
    conn.execute(f"INSERT INTO lake.main.gradientsports_tracking_players VALUES {', '.join(values)}")  # noqa: S608 - test constants, no outside input
    conn.execute(
        "CREATE TABLE lake.main.gradientsports_tracking_frames (match_key BIGINT, period BIGINT, frame BIGINT, "
        "record_index BIGINT, ball_x_mm BIGINT, ball_y_mm BIGINT, ball_detected BOOLEAN, timestamp_us BIGINT)"
    )
    balls = [
        f"({mk}, 1, {frame}, {frame * 10 + copy}, {mk + frame + copy}, 0, true, {frame * 33366})"
        for mk in players
        for frame in (100, 101)
        for copy in (0, 1)
    ]
    conn.execute(f"INSERT INTO lake.main.gradientsports_tracking_frames VALUES {', '.join(balls)}")  # noqa: S608 - test constants, no outside input
    conn.execute(
        "CREATE TABLE lake.main.idsse_match_information (match_key BIGINT, home_team_key BIGINT, "
        "guest_team_key BIGINT, native_match_id VARCHAR, general_kickoff_time TIMESTAMPTZ, "
        "general_home_team_id VARCHAR, general_home_team_name VARCHAR, general_guest_team_id VARCHAR, "
        "general_guest_team_name VARCHAR, general_competition_id VARCHAR, general_competition_name VARCHAR, "
        "general_season_id VARCHAR, general_season VARCHAR, environment_country VARCHAR)"
    )
    conn.execute(
        f"INSERT INTO lake.main.idsse_match_information VALUES ({IDSSE}, {HOME_TEAM}, 902, 'J03WMX', "  # noqa: S608 - test constants, no outside input
        "'2026-11-01 15:30:00+00', 'DFL-CLU-00000G', 'Home', 'DFL-CLU-000017', 'Guest', 'DFL-COM-000001', 'BL', "
        "'DFL-SEA-0001K6', '2026', 'DE')"
    )
    conn.execute(
        "CREATE TABLE lake.main.idsse_tracking_frames (match_key BIGINT, period BIGINT, frame BIGINT, "
        "ball_x_mm BIGINT, ball_y_mm BIGINT, ball_detected BOOLEAN, ball_possession BIGINT, timestamp_us BIGINT)"
    )
    conn.execute(
        f"INSERT INTO lake.main.idsse_tracking_frames VALUES ({IDSSE}, 1, 100, 500, -300, true, 1, 4000000), "  # noqa: S608 - test constants, no outside input
        f"({IDSSE}, 1, 101, 510, -310, true, 2, 4040000)"
    )


def _pointer(entity: str, record_key: int, canonical_key: int) -> tuple[Any, ...]:
    return (entity, GS, str(record_key), record_key, GS, str(canonical_key), canonical_key, "run-pd", _AT)


def _set_pointers(conn: duckdb.DuckDBPyConnection, pointers: list[tuple[Any, ...]]) -> None:
    conn.execute("DELETE FROM lake.main.ops_canonical_pointers")
    conn.executemany("INSERT INTO lake.main.ops_canonical_pointers VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)", pointers)


BASE_POINTERS = [
    _pointer("match", CANONICAL, CANONICAL),
    _pointer("match", FEEDER, CANONICAL),
    _pointer("player", 3, 3),
    _pointer("team", HOME_TEAM, HOME_TEAM),
]


@pytest.fixture
def working(kit: LakeKit) -> Iterator[WorkingCatalog]:
    cat = kit.catalog(run_id="run-pd")
    conn = cat.conn
    _seed_bronze(conn)
    run_resolution(conn, storage=kit.storage, settings=load_defaults("laptop"), run_id="run-pd")
    _seed_gradientsports_matches(conn)
    assert run_dbt(cat, run_id="run-pd", caps=CAPS).success
    _set_pointers(conn, BASE_POINTERS)  # the views are live: they see these pointers on the next read
    yield cat
    cat.close()


def _pushed(conn: duckdb.DuckDBPyConnection, read: Any, match_key: int, provider: str) -> list[tuple[Any, ...]]:
    params = read.params(match_key, provider, ik.match_record_keys(conn, match_key))
    return sorted(conn.execute(read.sql, params).fetchall(), key=repr)


def _canonical_only(conn: duckdb.DuckDBPyConnection, view: str, match_key: int, provider: str | None) -> Any:
    where = "canonical_match_key = ?" + (" AND provider = ?" if provider else "")
    params: list[object] = [match_key, provider] if provider else [match_key]
    sql = f"SELECT * EXCLUDE (match_key) FROM lake.main.{view} WHERE {where}"  # noqa: S608 - constant view names
    return sorted(conn.execute(sql, params).fetchall(), key=repr)


@pytest.mark.parametrize("match_key", [LONE, CANONICAL], ids=["no-pointer", "two-record-keys"])
def test_pushed_frame_reads_return_the_canonical_rows(working: WorkingCatalog, match_key: int) -> None:
    conn = working.conn
    frames = _pushed(conn, FRAMES_READ, match_key, GS)
    ball = _pushed(conn, BALL_READ, match_key, GS)
    assert frames and frames == _canonical_only(conn, "fct_tracking_frames", match_key, GS)
    assert ball and ball == _canonical_only(conn, "fct_tracking_ball", match_key, None)
    # one kept record per (period, frame) per record key: 2 frames x 2 players, and 2 ball rows, per record key
    records = len(ik.match_record_keys(conn, match_key))
    assert (len(frames), len(ball)) == (4 * records, 2 * records)


def _scan_filters(plan: object, table: str) -> list[str]:
    """The `Filters` text of every scan node of `table` in a DuckDB JSON plan."""
    found: list[str] = []
    if isinstance(plan, dict):
        node = cast("dict[str, object]", plan)
        info = node.get("extra_info")
        if isinstance(info, dict):
            extra = cast("dict[str, object]", info)
            if table in json.dumps(extra.get("Table", "")):
                found.append(json.dumps(extra.get("Filters", "")))
        for value in node.values():
            found.extend(_scan_filters(value, table))
    elif isinstance(plan, list):
        for item in cast("list[object]", plan):
            found.extend(_scan_filters(item, table))
    return found


@pytest.mark.parametrize("match_key", [LONE, CANONICAL], ids=["one-key", "two-keys"])
def test_pushed_gradientsports_frames_read_filters_the_bronze_scan(working: WorkingCatalog, match_key: int) -> None:
    conn = working.conn
    params = FRAMES_READ.params(match_key, GS, ik.match_record_keys(conn, match_key))
    rows = conn.execute(f"EXPLAIN (FORMAT json) {FRAMES_READ.sql}", params).fetchall()
    plan = json.loads(rows[0][1])
    filters = _scan_filters(plan, "gradientsports_tracking_players")
    assert filters, "no scan of gradientsports_tracking_players in the plan"
    assert all("match_key" in text for text in filters), filters


@pytest.mark.parametrize(
    ("duplicate", "frames_match", "ball_match"),
    [
        (_pointer("match", FEEDER, 909), CANONICAL, CANONICAL),
        (_pointer("player", 3, 33), CANONICAL, None),  # the ball view looks up no player
        (_pointer("team", HOME_TEAM, 990), CANONICAL, IDSSE),  # frames: team_key; idsse ball: the possession team
    ],
    ids=["match", "player", "team"],
)
def test_a_duplicate_pointer_fails_the_reads_that_meet_it(
    working: WorkingCatalog, duplicate: tuple[Any, ...], frames_match: int, ball_match: int | None
) -> None:
    conn = working.conn
    _set_pointers(conn, [*BASE_POINTERS, duplicate])
    with pytest.raises(duckdb.InvalidInputException, match=_MULTIPLE_ROWS):
        _pushed(conn, FRAMES_READ, frames_match, GS)
    with pytest.raises(duckdb.InvalidInputException, match=_MULTIPLE_ROWS):
        _canonical_only(conn, "fct_tracking_frames", frames_match, GS)
    if ball_match is not None:
        provider = GS if ball_match != IDSSE else "idsse"
        with pytest.raises(duckdb.InvalidInputException, match=_MULTIPLE_ROWS):
            _pushed(conn, BALL_READ, ball_match, provider)
        with pytest.raises(duckdb.InvalidInputException, match=_MULTIPLE_ROWS):
            _canonical_only(conn, "fct_tracking_ball", ball_match, None)
    # an unrelated match's pushed reads still return their rows
    assert _pushed(conn, FRAMES_READ, LONE, GS)
    assert _pushed(conn, BALL_READ, LONE, GS)
