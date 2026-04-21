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
  - ``bronze.idsse_tracking``     (IDSSE DFL position XML)
  - ``bronze.metrica_events``     (Metrica sample-data CSV + EPTS)
  - ``bronze.metrica_tracking``   (Metrica sample-data CSV + EPTS)
  - ``bronze.wyscout_events``     (Wyscout Figshare JSON)
  - ``bronze.skillcorner_tracking`` (SkillCorner broadcast-tracking JSON)

  StatsBomb is deliberately excluded — its bronze is already wide (126 cols)
  because its bulk-per-competition ingestion avoids the per-match all-None
  drop pattern.

Requires live Databricks SQL warehouse. Skipped when the
``DATABRICKS_{HOST,HTTP_PATH,TOKEN}`` env vars are unset.
"""

from __future__ import annotations

import logging
import os
import sys
import tempfile
from collections.abc import Iterator
from pathlib import Path

import pytest

databricks_sql = pytest.importorskip("databricks.sql")

requires_databricks = pytest.mark.skipif(
    not all(os.environ.get(var) for var in ("DATABRICKS_HOST", "DATABRICKS_HTTP_PATH", "DATABRICKS_TOKEN")),
    reason="Databricks SQL env vars not set",
)

_LOGGER = logging.getLogger("test_bronze_live_schema")


@pytest.fixture(scope="module")
def conn() -> Iterator[object]:
    host = os.environ["DATABRICKS_HOST"].replace("https://", "").rstrip("/")
    c = databricks_sql.connect(
        server_hostname=host,
        http_path=os.environ["DATABRICKS_HTTP_PATH"],
        access_token=os.environ["DATABRICKS_TOKEN"],
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
    """Run the IDSSE events parser on the fixture synthetic XML and collect keys.

    Matches the existing ``test_idsse_bronze_coverage.py`` pattern — single
    source of truth for what the parser emits.
    """
    import pandas as pd

    # Make src importable when running via `uv run pytest`
    sys.path.insert(0, str(Path(__file__).parent.parent.resolve()))
    from ingestion.idsse import _parse_events_xml

    # Reuse the coverage test's synthetic-XML generator
    sys.path.insert(0, str(Path(__file__).parent.resolve()))
    from coverage_utils import load_attr_enumeration

    try:
        from test_idsse_bronze_coverage import _generate_synthetic_xml
    except ImportError:
        from tests.test_idsse_bronze_coverage import _generate_synthetic_xml  # type: ignore

    fixture = Path(__file__).parent / "fixtures" / "idsse_dfl_event_attr_enumeration.json"
    enum = load_attr_enumeration(fixture)
    xml_text = _generate_synthetic_xml(enum)

    fd, path = tempfile.mkstemp(suffix=".xml")
    os.close(fd)
    try:
        with open(path, "w", encoding="utf-8") as f:
            f.write(xml_text)
        rows = _parse_events_xml(path, {}, "TEST", _LOGGER)
    finally:
        os.unlink(path)

    df = pd.DataFrame(rows)
    return set(df.columns)


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
    sys.path.insert(0, str(Path(__file__).parent.parent.resolve()))
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
    sys.path.insert(0, str(Path(__file__).parent.parent.resolve()))
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
def test_metrica_events_live_schema_covers_parser(conn: object) -> None:
    sys.path.insert(0, str(Path(__file__).parent.parent.resolve()))
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
@pytest.mark.skip(
    reason=(
        "Wyscout events bronze is already source-complete per the existing "
        "test_wyscout_bronze_coverage snapshot-vs-sources.yml test. "
        "Per-competition writes don't exhibit the NullType drop pattern "
        "because every competition exercises every event field at least once."
    )
)
def test_wyscout_events_live_schema_covers_parser(conn: object) -> None:
    pass


@requires_databricks
def test_skillcorner_tracking_live_schema_covers_parser(conn: object) -> None:
    sys.path.insert(0, str(Path(__file__).parent.parent.resolve()))
    from ingestion.skillcorner import _SKILLCORNER_TRACKING_BRONZE_COLS

    expected = set(_SKILLCORNER_TRACKING_BRONZE_COLS)
    actual = _live_bronze_cols(conn, "skillcorner_tracking") - _AUDIT_COLS
    missing = expected - actual
    if missing:
        msg = (
            f"\n[SkillCorner tracking] Live bronze.skillcorner_tracking is "
            f"missing {len(missing)} column(s): {sorted(missing)}\n"
            f"Fix: re-ingest with wheel containing finalize_bronze_df guard."
        )
        raise AssertionError(msg)
