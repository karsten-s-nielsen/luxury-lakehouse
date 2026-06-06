"""Per-table strand-state for recurrence-RED detection (spec H3 / review P1, R1a, R3).

The daily detect task (SP) records a ``stranded`` event each time it sees a table checkpoint-broken;
the privileged heal pass records a ``healed`` event on a successful recreate. ``was_stranded_unhealed``
is the recurrence signal: a table is "still broken since we last saw it" iff its most recent strand
is newer than its most recent heal. Combined with classify checking this *before* recording the
current strand (Task 8), this gives: first detection of a (new) incident -> green-with-warning;
still-broken-on-the-next-run -> RED. A heal clears it, so a strand-heal-restrand weeks later is a new
incident (green), not a recurrence.

Append-only event model (review R3): two identities write the same table, and the SP is not in the
maintenance concurrency group, so writes can collide. Appends (not upserts) let ``write_delta_table``
(ADR-038 concurrent-commit retry) handle that with no MERGE conflict; reads take ``max`` per event
type. Reads are fail-open (review R1a): a missing/unreadable state table -> "no prior strand", never
a crash and never a false-RED — survives the first run before the migration is applied.

Spark/Delta IO sits behind an injected ``StrandStateBackend`` so the store logic is unit-tested
offline; ``SparkStrandStateBackend`` is the production implementation.
"""

from __future__ import annotations

import logging
from datetime import datetime
from typing import TYPE_CHECKING, Protocol

if TYPE_CHECKING:
    from pyspark.sql import SparkSession

logger = logging.getLogger(__name__)

_SCHEMA = "observability"
_TABLE = "synced_table_strand_state"
STRANDED = "stranded"
HEALED = "healed"


class StrandStateBackend(Protocol):
    def read_latest(self, table_name: str) -> tuple[datetime | None, datetime | None]:
        """Return ``(last_stranded_at, last_healed_at)``; ``(None, None)`` if absent (fail-open)."""
        ...

    def append_event(self, table_name: str, event_type: str, event_at: datetime) -> None: ...


class StrandStateStore:
    """Pure recurrence logic over a ``StrandStateBackend`` (offline-testable)."""

    def __init__(self, backend: StrandStateBackend) -> None:
        self._backend = backend

    def mark_stranded(self, table_name: str, event_at: datetime) -> None:
        self._backend.append_event(table_name, STRANDED, event_at)

    def mark_healed(self, table_name: str, event_at: datetime) -> None:
        self._backend.append_event(table_name, HEALED, event_at)

    def was_stranded_unhealed(self, table_name: str) -> bool:
        """True iff the most recent strand is newer than the most recent heal (or never healed)."""
        last_stranded, last_healed = self._backend.read_latest(table_name)
        return last_stranded is not None and (last_healed is None or last_healed < last_stranded)


class SparkStrandStateBackend:
    """Production backend over ``{catalog}.observability.synced_table_strand_state`` (Delta)."""

    def __init__(self, spark: SparkSession, catalog: str) -> None:
        self._spark = spark
        self._catalog = catalog
        self._fqn = f"{catalog}.{_SCHEMA}.{_TABLE}"

    def read_latest(self, table_name: str) -> tuple[datetime | None, datetime | None]:
        # Fail-open: a missing/unreadable state table -> (None, None) -> "no prior strand" (R1a).
        # table_name is a validated synced-table identifier from SYNCED_TABLES (no injection).
        from ingestion.utils import tolerate_missing_table

        result: dict[str, datetime | None] = {STRANDED: None, HEALED: None}
        with tolerate_missing_table(logger, f"strand-state read for {table_name}"):
            rows = self._spark.sql(
                f"SELECT event_type, MAX(event_at) AS ts FROM {self._fqn} "  # noqa: S608 -- internal FQN + validated name
                f"WHERE table_name = '{table_name}' GROUP BY event_type"
            ).collect()
            for r in rows:
                if r["event_type"] in result:
                    result[r["event_type"]] = r["ts"]
        return result[STRANDED], result[HEALED]

    def append_event(self, table_name: str, event_type: str, event_at: datetime) -> None:
        from pyspark.sql import types as T  # noqa: N812

        from ingestion.utils import write_delta_table

        schema = T.StructType(
            [
                T.StructField("table_name", T.StringType(), False),
                T.StructField("event_type", T.StringType(), False),
                T.StructField("event_at", T.TimestampType(), False),
            ]
        )
        sdf = self._spark.createDataFrame([(table_name, event_type, event_at)], schema=schema)
        write_delta_table(sdf, self._catalog, _SCHEMA, _TABLE, mode="append", row_count=1)
