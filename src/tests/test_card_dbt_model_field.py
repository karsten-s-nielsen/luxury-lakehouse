"""Workflow card <-> dbt model parity via the `dbt_model` field on TableRef.

Rules:
  1. Every `outputs.tables[*].dbt_model` on every card MUST reference a real
     SQL file at `dbt_project/models/**/<dbt_model>.sql`. Typos silently
     break the governance trail — enforce.
  2. The three `wf-dbt-build-*-marts.yaml` cards (input/intermediate/output —
     PR-Cycle-C PR-β, ADR-019) collectively own every dbt mart. The UNION
     of their `dbt_model` TableRef declarations MUST equal the set of
     `.sql` files under `dbt_project/models/marts/`. New marts added
     without updating the matching stage card will fail this test.

The `dbt_model` field was introduced as Option A of the PR #128 follow-up
cycle — "close wf-goalkeeper dbt-derived-output gap". It makes dbt-derived
outputs machine-enforceable rather than prose-only.

PR-Cycle-C PR-β (2026-05-02) split the single `wf-dbt-build.yaml` into the
three stage cards above; this test enumerates the union to keep the
"every mart has a card" invariant stage-aware.
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


_THREE_STAGE_CARDS = (
    "wf-dbt-build-input-marts.yaml",
    "wf-dbt-build-intermediate-marts.yaml",
    "wf-dbt-build-output-marts.yaml",
)


def test_three_stage_dbt_cards_collectively_enumerate_every_mart_model() -> None:
    """The UNION of `dbt_model` TableRefs across the three stage cards
    (`wf-dbt-build-input-marts`, `wf-dbt-build-intermediate-marts`,
    `wf-dbt-build-output-marts`) must equal the set of `.sql` files under
    `dbt_project/models/marts/`. Stage-card split was introduced by
    PR-Cycle-C PR-β (ADR-019) — see `docs/superpowers/adrs/ADR-019-three-stage-dbt-build.md`."""
    declared: set[str] = set()
    for card_filename in _THREE_STAGE_CARDS:
        card_path = _CARDS_DIR / card_filename
        assert card_path.is_file(), (
            f"Stage card {card_filename!r} is missing — every dbt build stage must have its own card."
        )
        card = _load_card(card_path)
        tables = (card.get("outputs") or {}).get("tables") or []
        declared |= {
            entry["dbt_model"]
            for entry in tables
            if isinstance(entry, dict) and isinstance(entry.get("dbt_model"), str)
        }

    on_disk = {p.stem for p in _DBT_MARTS_DIR.glob("*.sql")}

    missing_from_cards = on_disk - declared
    extra_on_cards = declared - on_disk

    assert not missing_from_cards, (
        f"Stage cards collectively missing TableRef entries for these mart "
        f"models on disk: {sorted(missing_from_cards)}. "
        f"Add each to its appropriate stage card "
        f"(input_mart/dimension → wf-dbt-build-input-marts.yaml; "
        f"intermediate_mart → wf-dbt-build-intermediate-marts.yaml; "
        f"output_mart → wf-dbt-build-output-marts.yaml)."
    )
    assert not extra_on_cards, (
        f"Stage cards reference mart models that no longer exist on disk: "
        f"{sorted(extra_on_cards)}. Remove the stale entries."
    )


def test_three_stage_dbt_cards_no_mart_in_two_stages() -> None:
    """Belt-and-suspenders: a mart appearing on two stage cards is a
    classification error (ADR-019 mandates exactly-one stage per mart).
    `test_dbt_mart_classification` (PR-alpha) enforces this on the SQL side
    via tags; this test enforces it on the workflow-card side."""
    seen: dict[str, str] = {}
    duplicates: list[str] = []
    for card_filename in _THREE_STAGE_CARDS:
        card_path = _CARDS_DIR / card_filename
        if not card_path.is_file():
            continue
        card = _load_card(card_path)
        tables = (card.get("outputs") or {}).get("tables") or []
        for entry in tables:
            if not (isinstance(entry, dict) and isinstance(entry.get("dbt_model"), str)):
                continue
            model = entry["dbt_model"]
            if model in seen:
                duplicates.append(f"{model!r}: appears in BOTH {seen[model]} and {card_filename}")
            else:
                seen[model] = card_filename
    assert not duplicates, "ADR-019 requires exactly one stage card per mart. Duplicates:\n  " + "\n  ".join(duplicates)
