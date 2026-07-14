"""Shared AST guard for §0d — ``write_delta_table`` DEFAULTS to ``mode="overwrite"``.

```python
def write_delta_table(df, catalog, schema, table_name,
                      mode: str = "overwrite",      # <-- utils.py. THE DEFAULT.
                      replace_where: str | None = None, ...)
```

Every other caller in this repo relies on that default (they overwrite a partition via
``replace_where``). So the natural way to write an APPEND-ONLY log —
``write_delta_table(sdf, catalog, schema, table)`` — **silently OVERWRITES the whole log on every
write.** Measured in the Task-2 spike: 392 default-mode "appends" left **1 row** in the table. The
consequence is not a slow gate but an actively LYING one — it would find no terminal event for any
unit and accuse a healthy drain on every run.

Extracted here (the ``_ddl.py`` / W5 pattern) so BOTH the sink's guard and the sb360 producer's
guard use ONE parser rather than importing from each other's test module.
"""

from __future__ import annotations

import ast


def write_delta_table_calls(src: str) -> list[ast.Call]:
    """Every ``write_delta_table(...)`` call in ``src``, however it is spelled.

    Matches the bare name AND the attribute form (``utils.write_delta_table(...)``) — the guard
    forbids a SHAPE, not a spelling.
    """
    return [
        n
        for n in ast.walk(ast.parse(src))
        if isinstance(n, ast.Call)
        and (n.func.attr if isinstance(n.func, ast.Attribute) else getattr(n.func, "id", None)) == "write_delta_table"
    ]


def writes_without_append(src: str) -> list[str]:
    """Every ``write_delta_table(...)`` in ``src`` that does not explicitly pass ``mode="append"``."""
    bad: list[str] = []
    for call in write_delta_table_calls(src):
        mode = next((kw.value for kw in call.keywords if kw.arg == "mode"), None)
        if not (isinstance(mode, ast.Constant) and mode.value == "append"):
            bad.append(f"line {call.lineno}: write_delta_table without mode='append'")
    return bad
