"""Per-period disjoint-write invariant: replaceWhere is period-scoped iff a period is set (pure)."""

from ingestion.action_context import _period_replace_where


def test_period_scoped_predicate_includes_period_id():
    pred = _period_replace_where("J03WMX", 2)
    assert "match_id = 'J03WMX'" in pred
    assert "period_id = 2" in pred


def test_whole_match_predicate_omits_period_id():
    pred = _period_replace_where("J03WMX", None)
    assert pred == "match_id = 'J03WMX'"
    assert "period_id" not in pred
