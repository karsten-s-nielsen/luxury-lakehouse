"""SQL-shape and security tests for filters.search_* functions (mocked DB).

These run in CI without a Lakebase connection. Real-data behaviour is verified
end-to-end via the local Taipy server + Puppeteer; this file guards the SQL
construction logic (LIKE escape, ORDER BY, LIMIT placement, scope predicates).
"""

from __future__ import annotations

from typing import Any
from unittest.mock import patch

import pandas as pd
from filters import _escape_like, search_players


def _mock_df(rows: list[tuple[str, int]]) -> pd.DataFrame:
    return pd.DataFrame([{"name": n, "id": i} for n, i in rows])


# ---------------------------------------------------------------------------
# _escape_like: protect LIKE wildcards before they reach SQL
# ---------------------------------------------------------------------------


def test_escape_like_passes_plain_text_through() -> None:
    assert _escape_like("messi") == "messi"
    assert _escape_like("van der Berg") == "van der Berg"


def test_escape_like_escapes_pct_underscore_backslash() -> None:
    # Order matters: backslash first (so the escapes for % and _ don't get re-escaped)
    assert _escape_like("%") == "\\%"
    assert _escape_like("_") == "\\_"
    assert _escape_like("\\") == "\\\\"
    assert _escape_like("a%b_c\\d") == "a\\%b\\_c\\\\d"


def test_escape_like_prevents_injection_pattern() -> None:
    # User typing pure wildcards must produce a literal-match pattern, not a wildcard
    escaped = _escape_like("%")
    assert "%" not in escaped.replace("\\%", "")  # only %s are the escaped pair


# ---------------------------------------------------------------------------
# search_players: SQL shape + parameter binding
# ---------------------------------------------------------------------------


def test_search_players_non_empty_query_includes_substring_predicate() -> None:
    captured: dict[str, Any] = {}

    def fake_execute(sql: str, params: tuple[Any, ...]) -> pd.DataFrame:
        captured["sql"] = sql
        captured["params"] = params
        return _mock_df([("Mikel Merino", 3042)])

    with patch("filters.execute_query", side_effect=fake_execute):
        result = search_players("mik", competition_id=11)

    assert result == [("Mikel Merino", 3042)]
    assert "LIKE LOWER(%s) ESCAPE '\\'" in captured["sql"]
    assert captured["params"][-2] == "%mik%"  # escaped query
    assert captured["params"][-1] == 500  # default limit


def test_search_players_empty_query_uses_top_n_when_empty() -> None:
    captured: dict[str, Any] = {}

    def fake_execute(sql: str, params: tuple[Any, ...]) -> pd.DataFrame:
        captured["sql"] = sql
        captured["params"] = params
        return _mock_df([])

    with patch("filters.execute_query", side_effect=fake_execute):
        search_players("", competition_id=11, top_n_when_empty=25)

    assert "LIKE" not in captured["sql"]  # no substring predicate when query empty
    assert captured["params"][-1] == 25  # top_n_when_empty applied


def test_search_players_team_scope_uses_cte_union() -> None:
    captured: dict[str, Any] = {}

    def fake_execute(sql: str, params: tuple[Any, ...]) -> pd.DataFrame:
        captured["sql"] = sql
        return _mock_df([])

    with patch("filters.execute_query", side_effect=fake_execute):
        search_players("x", competition_id=11, team_id=552)

    # Team-scoped search uses the team_players CTE (matches fetch_players semantics)
    assert "WITH team_players AS" in captured["sql"]
    assert "fct_shots_synced" in captured["sql"] or "fct_shots" in captured["sql"]
    assert "fct_passes_synced" in captured["sql"] or "fct_passes" in captured["sql"]


def test_search_players_cross_competition_skips_comp_predicate() -> None:
    captured: dict[str, Any] = {}

    def fake_execute(sql: str, params: tuple[Any, ...]) -> pd.DataFrame:
        captured["sql"] = sql
        captured["params"] = params
        return _mock_df([])

    with patch("filters.execute_query", side_effect=fake_execute):
        search_players("messi", competition_id=None)

    # Cross-comp uses WHERE 1=1 + only the substring predicate
    assert "WHERE 1=1" in captured["sql"]
    assert "competition_id" not in captured["sql"]


def test_search_players_escapes_wildcards_in_query() -> None:
    captured: dict[str, Any] = {}

    def fake_execute(sql: str, params: tuple[Any, ...]) -> pd.DataFrame:
        captured["sql"] = sql
        captured["params"] = params
        return _mock_df([])

    with patch("filters.execute_query", side_effect=fake_execute):
        search_players("100%", competition_id=11)

    # The % in user input must be escaped before reaching the LIKE pattern
    assert captured["params"][-2] == "%100\\%%"


def test_search_players_strips_whitespace() -> None:
    captured: dict[str, Any] = {}

    def fake_execute(sql: str, params: tuple[Any, ...]) -> pd.DataFrame:
        captured["sql"] = sql
        captured["params"] = params
        return _mock_df([])

    with patch("filters.execute_query", side_effect=fake_execute):
        search_players("   ", competition_id=11)  # whitespace-only -> empty path

    assert "LIKE" not in captured["sql"]
