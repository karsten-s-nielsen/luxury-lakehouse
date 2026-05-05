"""Smoke test for the query_databricks_sql helper — module-level import only."""

from ingestion.databricks_sql_fetch import query_databricks_sql


def test_query_databricks_sql_is_callable() -> None:
    assert callable(query_databricks_sql)
