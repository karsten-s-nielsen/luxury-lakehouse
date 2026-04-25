"""Shape test — Metrica pseudo-competition present in dim_competitions (PR 5a)."""

from pathlib import Path

MODEL = Path("dbt_project/models/marts/dim_competitions.sql")


def test_metrica_cte_present() -> None:
    src = MODEL.read_text()
    assert "metrica_competitions" in src


def test_metrica_sample_literal() -> None:
    src = MODEL.read_text()
    assert "'metrica-sample'" in src


def test_metrica_display_name() -> None:
    src = MODEL.read_text()
    assert "Metrica Sample Dataset" in src


def test_union_includes_metrica() -> None:
    src = MODEL.read_text()
    assert "select * from metrica_competitions" in src
