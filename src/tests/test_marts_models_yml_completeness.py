"""Every mart model must have a `_marts__models.yml` entry, and vice versa.

WHAT THIS GUARDS
----------------
`_marts__models.yml` is one file carrying the schema, contract and meta block for all 43
mart models. A model's `contract: enforced: true` lives ONLY here — dbt reads it from this
file, not from the `.sql`. So an entry silently going missing does not fail a parse: it
removes that model's enforced contract, and the build keeps going.

The hazard is editing by line range rather than by the block's own anchors. The blocks are
long and visually similar, so a range chosen to delete one model overshoots into its
neighbours — and the neighbours' loss is invisible, because nothing counted them. That is
why this file gets a completeness test while `.sql`-only changes do not.

WHY COUNT *AND* COMPARE NAMES
-----------------------------
A bare `count(- name:) == count(marts/*.sql)` passes when one model is dropped and an
unrelated one duplicated, or when a rename lands in only one of the two places. Both
totals stay equal while the mapping is wrong, so this asserts on the SET of names in each
direction and reports which side each discrepancy is on.
"""

from __future__ import annotations

from pathlib import Path

import yaml

_REPO = Path(__file__).resolve().parents[2]
_MARTS = _REPO / "dbt_project" / "models" / "marts"
_MODELS_YML = _MARTS / "_marts__models.yml"


def _sql_models() -> set[str]:
    return {p.stem for p in _MARTS.glob("*.sql")}


def _yml_models() -> list[str]:
    """Model names as declared, as a LIST so duplicates survive for the check below."""
    doc = yaml.safe_load(_MODELS_YML.read_text(encoding="utf-8")) or {}
    return [m["name"] for m in (doc.get("models") or []) if "name" in m]


def test_the_scanner_finds_something() -> None:
    """Non-vacuity. If either side parsed to an empty set, every assertion below would hold
    forever while checking nothing — the failure mode this repo keeps paying for."""
    assert len(_sql_models()) >= 40, f"only {len(_sql_models())} mart .sql files found — wrong directory?"
    assert len(_yml_models()) >= 40, f"only {len(_yml_models())} yml entries parsed — wrong shape?"


def test_every_mart_model_has_a_yml_entry() -> None:
    """A missing entry silently drops that model's enforced contract."""
    missing = sorted(_sql_models() - set(_yml_models()))
    assert not missing, (
        f"mart model(s) with no _marts__models.yml entry: {missing}. Their `contract: enforced: true` is NOT in effect."
    )


def test_every_yml_entry_has_a_mart_model() -> None:
    """An orphan entry means a model was deleted or renamed and its schema block left behind,
    which dbt reports as a warning that is easy to miss in a long build log."""
    orphans = sorted(set(_yml_models()) - _sql_models())
    assert not orphans, f"_marts__models.yml entries with no matching .sql: {orphans}"


def test_no_duplicate_yml_entries() -> None:
    """Two blocks for one model: the second silently wins, so a contract edit to the first
    has no effect. This also keeps the count-based reading of the two sets honest."""
    names = _yml_models()
    dupes = sorted({n for n in names if names.count(n) > 1})
    assert not dupes, f"duplicate _marts__models.yml entries: {dupes}"


def test_the_counts_agree() -> None:
    """The blunt check, kept deliberately: it is the one that fails loudly on a range-delete
    that takes several neighbouring blocks at once."""
    sql, yml = _sql_models(), _yml_models()
    assert len(yml) == len(sql), (
        f"{len(yml)} yml entries vs {len(sql)} mart .sql files — "
        "a block was likely removed by line range instead of by its own anchors"
    )
