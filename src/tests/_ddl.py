"""Shared CREATE TABLE column parser for migration↔schema parity tests (W5).

Extracted from ``test_work_queue_schema_parity.py`` so BOTH parity tests (the work queue and the
D9 unit-event log) use ONE parser — importing it *from another test module* is fragile under
pytest collection and creates a test→test dependency.

The guard it powers compares ORDERED ``(name, type)`` tuples, never substrings (V4): a substring
check passes on a wrong type, a wrong order, or a column that appears only in a comment.
"""

from __future__ import annotations


def ddl_columns(sql: str) -> list[tuple[str, str]]:
    """Ordered ``(column_name, lowercase_sql_type)`` of the FIRST ``CREATE TABLE`` in ``sql``.

    The closing paren is found by a BALANCED scan from the opening one — not ``rindex(")")`` —
    because a trailing ``PARTITIONED BY (event_date)`` has parens of its own and would otherwise
    swallow the whole statement into the "column list".
    """
    # Drop comment lines FIRST so a "(" inside a comment (e.g. "(ADR-037)") can't fool the paren scan.
    sql = "\n".join(line for line in sql.splitlines() if not line.strip().startswith("--"))
    start = sql.index("(")
    depth = 0
    end = -1
    for i, ch in enumerate(sql[start:], start):
        if ch == "(":
            depth += 1
        elif ch == ")":
            depth -= 1
            if depth == 0:
                end = i
                break
    if end < 0:
        raise ValueError("unbalanced parentheses in CREATE TABLE column list")
    cols: list[tuple[str, str]] = []
    for raw in sql[start + 1 : end].splitlines():
        line = raw.strip().rstrip(",")
        if not line:
            continue
        name, sql_type = line.split(None, 1)
        cols.append((name, sql_type.lower()))
    return cols
