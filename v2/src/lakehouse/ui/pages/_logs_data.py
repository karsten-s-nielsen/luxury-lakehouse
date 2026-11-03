"""Taipy-free log rendering helpers (page-binding addendum §3): the level/text filter and the native-table columns
the Runs page's run-detail log view uses. Pure functions over `observability` LogRecord; no Taipy, no socket.

The log view moved from a top-level Logs page into the Runs run-detail drill-down (audit F6): one run's steps, spend,
dependencies and logs sit together, keyed by the selected run, instead of a separate page pinned to the latest run.
"""

from __future__ import annotations

from lakehouse.observability import LogRecord
from lakehouse.ui.template import table_columns

__all__ = ["LOG_HEADERS", "MESSAGE_CLIP", "filtered_records", "log_table_cols"]

MESSAGE_CLIP = 160  # clip a log message to one table cell (spec §9)
LOG_HEADERS = ("Level", "Time", "Logger", "Message")


def _clip(text: str) -> str:
    one_line = text.replace("\n", " ").replace("|", "¦")  # keep the table cell intact
    return one_line if len(one_line) <= MESSAGE_CLIP else one_line[: MESSAGE_CLIP - 1] + "…"


def _log_dict(record: LogRecord) -> dict[str, str]:
    return {"Level": record.level, "Time": record.ts, "Logger": record.logger, "Message": _clip(record.msg)}


def log_table_cols(records: list[LogRecord]) -> dict[str, list[str]]:
    """The records as Taipy's native-table column format."""
    return table_columns([_log_dict(record) for record in records], LOG_HEADERS)


def filtered_records(records: list[LogRecord], level: str, text: str) -> list[LogRecord]:
    """Match read_logs: level is an exact match; text is a substring over the message and logger name."""
    needle = text.lower() if text else None
    out: list[LogRecord] = []
    for record in records:
        if level and record.level != level:
            continue
        if needle is not None and needle not in f"{record.msg} {record.logger}".lower():
            continue
        out.append(record)
    return out
