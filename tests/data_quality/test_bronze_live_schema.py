"""Live-Delta bronze schema coverage test.

Complements the existing parser-level bronze-coverage tests
(``test_<provider>_bronze_coverage.py``) by verifying that every column the
parser emits **actually lands in the live Delta table**.

Root-cause of the coverage gap this test closes (2026-04-21):

  The parser-level tests run a synthetic XML through the bronze parser and
  check that the parser's output row dicts contain every expected key. They
  do NOT query the live Delta table. The gap: pandas → Arrow → Spark
  conversion silently infers all-None columns as ``NullType``. Delta rejects
  NullType columns during write, so they get dropped. Per-match
  ``replaceWhere`` writes amplify this — each match exercises only a subset
  of event types, so non-applicable prefix columns become all-None and
  vanish. Result: wide parser → narrow Delta table. The parser-level test
  stays green (parser emits the columns in isolation) while bronze is thin.

  This test queries ``DESCRIBE <table>`` directly and asserts every expected
  parser column appears in the live schema.

  Fix pattern: each bronze parser must either (a) cast every emitted column
  to an explicit pandas dtype before DataFrame creation, OR (b) pass an
  explicit Spark ``StructType`` to ``createDataFrame`` so Arrow can't drop
  all-None cols.

Test scope — bronze tables for 5 providers:

  - ``bronze.idsse_events``       (IDSSE DFL event XML)
  - ``bronze.idsse_tracking``       (IDSSE DFL position XML)
  - ``bronze.metrica_events``       (Metrica sample-data CSV + EPTS)
  - ``bronze.metrica_tracking``     (Metrica sample-data CSV + EPTS)
  - ``bronze.wyscout_events``       (Wyscout Figshare JSON)
  - ``bronze.skillcorner_tracking`` (SkillCorner broadcast-tracking JSON)
  - ``bronze.statsbomb_competitions`` / ``.statsbomb_matches`` /
    ``.statsbomb_events``           (StatsBomb open-data via statsbombpy)

  StatsBomb + Wyscout were previously excluded because their bulk-per-
  competition ingestion is unlikely to trigger the all-None NullType drop
  pattern — every competition typically exercises every event field at
  least once. G1 of the PR #173 drop-safety sweep still wires
  finalize_bronze_df into both writers so a future per-competition
  regression would not silently reintroduce the gap.

Requires live Databricks SQL warehouse. Skipped when the
``DATABRICKS_{HOST,HTTP_PATH,TOKEN}`` env vars are unset.
"""

from __future__ import annotations

import logging
import os
import sys
from collections.abc import Iterator
from pathlib import Path

import pytest

from ingestion.databricks_auth import bearer_token, has_databricks_auth

databricks_sql = pytest.importorskip("databricks.sql")

requires_databricks = pytest.mark.skipif(
    not (has_databricks_auth() and os.environ.get("DATABRICKS_HTTP_PATH")),
    reason="Databricks SQL env vars not set",
)

_LOGGER = logging.getLogger("test_bronze_live_schema")


@pytest.fixture(scope="module")
def conn() -> Iterator[object]:
    host = os.environ["DATABRICKS_HOST"].replace("https://", "").rstrip("/")
    c = databricks_sql.connect(
        server_hostname=host,
        http_path=os.environ["DATABRICKS_HTTP_PATH"],
        access_token=bearer_token(),
    )
    try:
        yield c
    finally:
        c.close()


def _live_bronze_cols(conn: object, table: str) -> set[str]:
    """Return the column set of the live Delta table via DESCRIBE."""
    cur = conn.cursor()  # type: ignore[attr-defined]
    cur.execute(f"DESCRIBE soccer_analytics.bronze.{table}")
    rows = cur.fetchall()
    cols: set[str] = set()
    for row in rows:
        name = row[0]
        # DESCRIBE appends partitioning/clustering metadata rows that start
        # with '#' or are blank — skip them.
        if not name or name.startswith("#"):
            continue
        cols.add(name)
    return cols


# ---------------------------------------------------------------------------
# IDSSE events
# ---------------------------------------------------------------------------


def _idsse_events_expected_cols() -> set[str]:
    """The IDSSE events parser's full bronze column set.

    Since the DFL parser was lifted to the silly-kicks parse port (ADR-055,
    delete-and-depend), the lakehouse-side source of truth for the expected
    events-bronze columns is the ``_IDSSE_EVENTS_BRONZE_COLS`` constant that
    ``finalize_bronze_df`` enforces on every write — mirrors the tracking test
    below (which uses ``_IDSSE_TRACKING_BRONZE_COLS``).
    """
    sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "src"))
    from ingestion.idsse import _IDSSE_EVENTS_BRONZE_COLS

    return set(_IDSSE_EVENTS_BRONZE_COLS)


# Audit columns added post-parser by write_delta_table.add_audit_columns.
_AUDIT_COLS: set[str] = {"_ingested_at"}


@requires_databricks
def test_idsse_events_live_schema_covers_parser(conn: object) -> None:
    """Every column the parser emits must exist in live bronze.idsse_events."""
    expected = _idsse_events_expected_cols()
    actual = _live_bronze_cols(conn, "idsse_events") - _AUDIT_COLS
    missing = expected - actual
    if missing:
        msg = (
            f"\n[IDSSE events] Live bronze.idsse_events is missing {len(missing)} "
            f"column(s) the parser emits.\n"
            f"Parser emitted: {len(expected)} cols\n"
            f"Live bronze has: {len(actual)} cols\n"
            f"Missing (first 40):\n  {sorted(missing)[:40]}\n"
            "Fix: parser output is being dropped during pandas→Arrow→Spark "
            "conversion (all-None columns infer as NullType and Delta rejects them). "
            "Apply explicit dtype preservation via the shared _finalize_bronze_df helper."
        )
        raise AssertionError(msg)


# ---------------------------------------------------------------------------
# Placeholder tests for providers whose expected-col set needs enumeration
# (fills in as Tasks 17-21 extend each parser).
# ---------------------------------------------------------------------------


@requires_databricks
def test_idsse_tracking_live_schema_covers_parser(conn: object) -> None:
    """Every column the IDSSE tracking parser emits must exist in live Delta."""
    sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "src"))
    from ingestion.idsse import _IDSSE_TRACKING_BRONZE_COLS

    expected = set(_IDSSE_TRACKING_BRONZE_COLS)
    actual = _live_bronze_cols(conn, "idsse_tracking") - _AUDIT_COLS
    missing = expected - actual
    if missing:
        msg = (
            f"\n[IDSSE tracking] Live bronze.idsse_tracking is missing "
            f"{len(missing)} column(s) the parser emits.\n"
            f"Parser constant: {len(expected)} cols\n"
            f"Live bronze has: {len(actual)} cols\n"
            f"Missing: {sorted(missing)}\n"
            "Fix: re-ingest bronze.idsse_tracking with wheel containing the "
            "extended _parse_positions_xml + finalize_bronze_df guard."
        )
        raise AssertionError(msg)


@requires_databricks
def test_metrica_tracking_live_schema_covers_parser(conn: object) -> None:
    sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "src"))
    from ingestion.metrica_tracking import _METRICA_TRACKING_BRONZE_COLS

    expected = set(_METRICA_TRACKING_BRONZE_COLS)
    actual = _live_bronze_cols(conn, "metrica_tracking") - _AUDIT_COLS
    missing = expected - actual
    if missing:
        msg = (
            f"\n[Metrica tracking] Live bronze.metrica_tracking is missing "
            f"{len(missing)} column(s): {sorted(missing)}\n"
            f"Fix: re-ingest with wheel containing finalize_bronze_df guard."
        )
        raise AssertionError(msg)


@requires_databricks
def test_tracking_player_metadata_has_is_anonymized(conn: object) -> None:
    """PR 5a (ADR-011 §4): is_anonymized forward-compat flag present on
    bronze.tracking_player_metadata. Set False for IDSSE + SkillCorner
    (both carry real identity); schema unified with bronze.metrica_tracking
    so downstream can branch on a single flag contract."""
    cols = _live_bronze_cols(conn, "tracking_player_metadata")
    assert "is_anonymized" in cols, (
        f"is_anonymized absent from bronze.tracking_player_metadata. "
        f"Cols present: {sorted(cols)}. "
        f"Fix: run scripts/migrations/2026-04-24-add-metrica-is-anonymized.sql "
        f"via scripts/migrations/_runner.py"
    )


@requires_databricks
def test_team_xref_raw_live_schema(conn: object) -> None:
    """PR 5a (ADR-011): bronze.team_xref_raw created as the team analogue
    of player_xref_raw. Populated by scripts/generate_entity_xref.py with
    cross-provider team identity pairs (SB↔WS↔IDSSE) at confidence ≥ 70."""
    expected = {
        "source_a",
        "team_id_a",
        "source_b",
        "team_id_b",
        "confidence",
        "match_layer",
        "resolution_type",
        "_ingested_at",
    }
    cols = _live_bronze_cols(conn, "team_xref_raw")
    missing = expected - cols
    assert not missing, (
        f"bronze.team_xref_raw missing {len(missing)} expected col(s): "
        f"{sorted(missing)}. Cols present: {sorted(cols)}. "
        f"Fix: run scripts/migrations/2026-04-24-create-team-xref-raw.sql "
        f"via scripts/migrations/_runner.py"
    )


@requires_databricks
def test_wyscout_teams_live_schema(conn: object) -> None:
    """PR 5a (ADR-011): bronze.wyscout_teams landed via ingest_teams()
    closing the pre-existing Figshare teams.json ingestion gap. 142 teams
    across 7 competitions (verified against wyscout_matches.teamsData).
    Schema source: Figshare article 7765310 teams.json columns serialised
    through the G1 finalize_bronze_df guard."""
    expected = {"wyId", "officialName", "name", "city", "area", "type"}
    cols = _live_bronze_cols(conn, "wyscout_teams")
    missing = expected - cols
    assert not missing, (
        f"bronze.wyscout_teams missing {len(missing)} expected col(s): "
        f"{sorted(missing)}. Cols present: {sorted(cols)}. "
        f"Fix: trigger the ingest_wyscout Databricks Job to populate the "
        f"table via src/ingestion/wyscout.py::ingest_teams."
    )


@requires_databricks
def test_metrica_events_live_schema_covers_parser(conn: object) -> None:
    sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "src"))
    from ingestion.metrica_events import _METRICA_EVENTS_BRONZE_COLS

    expected = set(_METRICA_EVENTS_BRONZE_COLS)
    actual = _live_bronze_cols(conn, "metrica_events") - _AUDIT_COLS
    missing = expected - actual
    if missing:
        msg = (
            f"\n[Metrica events] Live bronze.metrica_events is missing "
            f"{len(missing)} column(s): {sorted(missing)}\n"
            f"Fix: re-ingest with wheel containing finalize_bronze_df guard."
        )
        raise AssertionError(msg)


@requires_databricks
def test_wyscout_events_live_schema_covers_parser(conn: object) -> None:
    """Every column the Wyscout events parser emits must exist in live Delta.

    Un-skipped by G1 of the PR #173 drop-safety sweep — now that
    ``finalize_bronze_df`` is wired into ``wyscout._write_events_competition``,
    the expected-cols contract is machine-enforceable against live bronze.
    """
    sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "src"))
    from ingestion.wyscout import _WYSCOUT_EVENTS_EXPECTED_COLS

    expected = set(_WYSCOUT_EVENTS_EXPECTED_COLS)
    actual = _live_bronze_cols(conn, "wyscout_events") - _AUDIT_COLS
    missing = expected - actual
    if missing:
        msg = (
            f"\n[Wyscout events] Live bronze.wyscout_events is missing "
            f"{len(missing)} column(s): {sorted(missing)}\n"
            f"Fix: re-ingest with wheel containing finalize_bronze_df guard."
        )
        raise AssertionError(msg)


@requires_databricks
def test_statsbomb_competitions_live_schema_covers_parser(conn: object) -> None:
    """Every StatsBomb competitions parser column must land in live Delta."""
    sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "src"))
    from ingestion.statsbomb import _STATSBOMB_COMPETITIONS_EXPECTED_COLS

    expected = set(_STATSBOMB_COMPETITIONS_EXPECTED_COLS)
    actual = _live_bronze_cols(conn, "statsbomb_competitions") - _AUDIT_COLS
    missing = expected - actual
    if missing:
        msg = (
            f"\n[StatsBomb competitions] Live bronze.statsbomb_competitions is "
            f"missing {len(missing)} column(s): {sorted(missing)}\n"
            f"Fix: re-ingest with wheel containing finalize_bronze_df guard."
        )
        raise AssertionError(msg)


@requires_databricks
def test_statsbomb_matches_live_schema_covers_parser(conn: object) -> None:
    """Every StatsBomb matches parser column must land in live Delta."""
    sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "src"))
    from ingestion.statsbomb import _STATSBOMB_MATCHES_EXPECTED_COLS

    expected = set(_STATSBOMB_MATCHES_EXPECTED_COLS)
    actual = _live_bronze_cols(conn, "statsbomb_matches") - _AUDIT_COLS
    missing = expected - actual
    if missing:
        msg = (
            f"\n[StatsBomb matches] Live bronze.statsbomb_matches is missing "
            f"{len(missing)} column(s): {sorted(missing)}\n"
            f"Fix: re-ingest with wheel containing finalize_bronze_df guard."
        )
        raise AssertionError(msg)


@requires_databricks
def test_statsbomb_events_live_schema_covers_parser(conn: object) -> None:
    """Every StatsBomb events parser column must land in live Delta (126 cols)."""
    sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "src"))
    from ingestion.statsbomb import _STATSBOMB_EVENTS_EXPECTED_COLS

    expected = set(_STATSBOMB_EVENTS_EXPECTED_COLS)
    actual = _live_bronze_cols(conn, "statsbomb_events") - _AUDIT_COLS
    missing = expected - actual
    if missing:
        msg = (
            f"\n[StatsBomb events] Live bronze.statsbomb_events is missing "
            f"{len(missing)} column(s): {sorted(missing)}\n"
            f"Fix: re-ingest with wheel containing finalize_bronze_df guard."
        )
        raise AssertionError(msg)


@requires_databricks
def test_skillcorner_tracking_live_schema_covers_parser(conn: object) -> None:
    sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "src"))
    from ingestion.skillcorner_tracking import _TRACKING_DTYPE_OVERRIDES

    expected = set(_TRACKING_DTYPE_OVERRIDES.keys()) | {"match_id"}
    actual = _live_bronze_cols(conn, "skillcorner_tracking") - _AUDIT_COLS
    missing = expected - actual
    if missing:
        msg = (
            f"\n[SkillCorner tracking] Live bronze.skillcorner_tracking is "
            f"missing {len(missing)} column(s): {sorted(missing)}\n"
            f"Fix: re-ingest with wheel containing finalize_bronze_df guard."
        )
        raise AssertionError(msg)


# ---------------------------------------------------------------------------
# Pitch control values — promoted to first-class in PR 6 (ADR-011)
# ---------------------------------------------------------------------------


@requires_databricks
def test_pitch_control_values_live_schema_covers_writer(conn: object) -> None:
    """Every column the pitch_control_batch writer emits must exist in live
    bronze.pitch_control_values.

    PR 6 (ADR-011): closed a previously-uncovered bronze table; the
    staging promotion (data_source + match_key) is upstream-derived only,
    so this test guards against the writer's bronze contract drifting
    from the live Delta schema.
    """
    sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "src"))
    from ingestion.pitch_control_batch import _PITCH_CONTROL_BRONZE_COLS

    expected = set(_PITCH_CONTROL_BRONZE_COLS) - _AUDIT_COLS
    actual = _live_bronze_cols(conn, "pitch_control_values") - _AUDIT_COLS
    missing = expected - actual
    if missing:
        msg = (
            f"\n[Pitch Control] Live bronze.pitch_control_values is missing "
            f"{len(missing)} column(s) the writer emits: {sorted(missing)}\n"
            f"Expected: {sorted(set(_PITCH_CONTROL_BRONZE_COLS))}\n"
            f"(per ingestion.pitch_control_batch._PITCH_CONTROL_BRONZE_COLS)\n"
            f"Actual (live, minus audit cols): {sorted(actual)}\n"
            "Fix: re-run compute_pitch_control with the current writer code."
        )
        raise AssertionError(msg)


# The v2 xG writer (ingestion.xg_model_v2 → bronze.xg_predictions_v2) was retired
# 2026-07-10 (ADR-066); its live-schema-covers-writer test was removed with the module.
# The v3 pre-shot xG writer's bronze (bronze.xg_shot_predictions) is guarded by its own
# schema-drift test in src/tests/test_xg_shot_scorer.py.


# ---------------------------------------------------------------------------
# PAUSA values — added in PR 7 (ADR-013 second application)
# ---------------------------------------------------------------------------


_PAUSA_BRONZE_COLS: tuple[str, ...] = (
    "pass_id",
    "match_id",
    "player_id",
    "team",
    "period",
    "timestamp_seconds",
    "frame_id",
    "temporal_judgment",
    "spatial_selection",
    "pausa_score",
    "actual_obso",
    "peak_obso",
    "optimal_obso",
    "receiver_x",
    "receiver_y",
)


@requires_databricks
def test_pausa_values_live_schema_covers_writer(conn: object) -> None:
    """Every column the pausa writer emits must exist in live bronze.pausa_values.

    PR 7 (ADR-013 second application): src/ingestion/pausa.py retargets from
    direct gold-write to bronze.pausa_values; the dbt-built mart fct_pausa_values
    inherits Kimball FKs via INNER JOIN to fct_passes on pass_id.
    """
    expected = set(_PAUSA_BRONZE_COLS)
    actual = _live_bronze_cols(conn, "pausa_values") - _AUDIT_COLS
    missing = expected - actual
    if missing:
        msg = (
            f"\n[PAUSA] Live bronze.pausa_values is missing "
            f"{len(missing)} column(s) the writer emits: {sorted(missing)}\n"
            f"Expected (per src/ingestion/pausa.py _RESULTS_SCHEMA): {sorted(expected)}\n"
            f"Actual (live, minus audit cols): {sorted(actual)}\n"
            "Fix: drop dev_gold.fct_pausa_values + re-trigger wf-obso-pausa "
            "(post-PR-7 deploy step). The writer now targets bronze.pausa_values."
        )
        raise AssertionError(msg)
