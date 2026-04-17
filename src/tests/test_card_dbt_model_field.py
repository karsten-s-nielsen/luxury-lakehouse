"""Workflow card <-> dbt model parity via the `dbt_model` field on TableRef.

Rules:
  1. Every `outputs.tables[*].dbt_model` on every card MUST reference a real
     SQL file at `dbt_project/models/**/<dbt_model>.sql`. Typos silently
     break the governance trail — enforce.
  2. `wf-dbt-build.yaml` is the authoritative owner of every dbt mart.
     Every .sql file under `dbt_project/models/marts/` MUST appear as a
     TableRef with `dbt_model` set inside that card. New marts added
     without updating the card will fail this test.

The `dbt_model` field was introduced as Option A of the PR #128 follow-up
cycle — "close wf-goalkeeper dbt-derived-output gap". It makes dbt-derived
outputs machine-enforceable rather than prose-only.
"""

from __future__ import annotations

import re
from pathlib import Path

import pytest
import yaml

_REPO = Path(__file__).resolve().parents[2]
_CARDS_DIR = _REPO / "workflow-cards"
_DBT_MARTS_DIR = _REPO / "dbt_project" / "models" / "marts"
_DBT_MODELS_ROOT = _REPO / "dbt_project" / "models"

_FRONTMATTER = re.compile(r"^---\s*\n(.*?\n)---\s*\n", re.DOTALL)


def _load_card(path: Path) -> dict:
    text = path.read_text(encoding="utf-8")
    m = _FRONTMATTER.match(text)
    if not m:
        pytest.fail(f"Card {path.name} has no YAML frontmatter")
    return yaml.safe_load(m.group(1))


def _known_dbt_model_names() -> set[str]:
    """Return the set of dbt model names discoverable via .sql stem."""
    return {p.stem for p in _DBT_MODELS_ROOT.rglob("*.sql")}


def _iter_dbt_model_refs() -> list[tuple[str, str]]:
    """Yield (card_filename, dbt_model_value) for every TableRef that
    declares `dbt_model:` across all cards."""
    refs: list[tuple[str, str]] = []
    for path in sorted(_CARDS_DIR.glob("wf-*.yaml")):
        card = _load_card(path)
        outputs = card.get("outputs") or {}
        tables = outputs.get("tables") or []
        for entry in tables:
            if isinstance(entry, dict) and entry.get("dbt_model"):
                refs.append((path.name, entry["dbt_model"]))
    return refs


def test_every_dbt_model_ref_points_at_real_sql_file() -> None:
    """Every `dbt_model:` value on a TableRef must match a .sql file stem
    under `dbt_project/models/`. Catches typos and stale references."""
    known = _known_dbt_model_names()
    errors: list[str] = []
    for card_file, dbt_model in _iter_dbt_model_refs():
        if dbt_model not in known:
            errors.append(f"{card_file}: dbt_model={dbt_model!r} has no matching .sql file under dbt_project/models/")
    assert not errors, "\n".join(errors)


def test_wf_dbt_build_enumerates_every_mart_model() -> None:
    """wf-dbt-build.yaml is the execution owner of every model built by
    `dbt build`. Every .sql file under dbt_project/models/marts/ must
    appear as a TableRef with dbt_model set on that card."""
    card_path = _CARDS_DIR / "wf-dbt-build.yaml"
    card = _load_card(card_path)
    tables = (card.get("outputs") or {}).get("tables") or []
    declared: set[str] = {
        entry["dbt_model"] for entry in tables if isinstance(entry, dict) and isinstance(entry.get("dbt_model"), str)
    }

    on_disk = {p.stem for p in _DBT_MARTS_DIR.glob("*.sql")}

    missing_from_card = on_disk - declared
    extra_on_card = declared - on_disk

    assert not missing_from_card, (
        f"wf-dbt-build.yaml is missing TableRef entries for these mart "
        f"models on disk: {sorted(missing_from_card)}. "
        f"Add them with dbt_model set."
    )
    assert not extra_on_card, (
        f"wf-dbt-build.yaml references mart models that no longer exist "
        f"on disk: {sorted(extra_on_card)}. Remove the stale entries."
    )
