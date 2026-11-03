"""S1 Task 11: a standing gate forbidding DuckDB's key-introspection SQL functions in `src/` (plan T11, B-14).

On an encrypted-local lake the DuckLake secret holds the encryption key, so `duckdb_secrets().options` and the
`duckdb_databases()` attach string would expose it. Those SQL functions have no legitimate use anywhere in `src/`,
so the gate bans the call form outright. It is a blanket ban rather than a keyed-connection-only one because
whether a connection holds the key is not decidable from the source text; banning the call form everywhere is the
safe realization. The Python method `Storage.duckdb_secrets()` (which renders S3/HF `CREATE SECRET` statements) is
an attribute call, not a SQL string, so it is unaffected.
"""

from __future__ import annotations

import ast
import re
from pathlib import Path

_SRC = Path(__file__).resolve().parents[2] / "src" / "lakehouse"
_FORBIDDEN = re.compile(r"duckdb_(?:secrets|databases)\s*\(")


def _sql_function_hits() -> list[str]:
    hits: list[str] = []
    for path in sorted(_SRC.rglob("*.py")):
        tree = ast.parse(path.read_text(encoding="utf-8"))
        for node in ast.walk(tree):
            if isinstance(node, ast.Constant) and isinstance(node.value, str) and _FORBIDDEN.search(node.value):
                hits.append(f"{path.relative_to(_SRC.parent.parent)}:{node.lineno}")
    return hits


def test_src_never_calls_duckdb_secrets_or_databases_in_sql() -> None:
    hits = _sql_function_hits()
    assert hits == [], (
        "a key-introspection SQL function (duckdb_secrets()/duckdb_databases()) appears in src SQL; on an "
        f"encrypted-local lake it would expose the encryption key: {hits}"
    )
