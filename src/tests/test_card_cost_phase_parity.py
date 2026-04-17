"""Workflow card cost-phase / execution-phase parity.

Each card that declares `cost.<phase>` MUST declare the same `<phase>` under
`execution:`. Without this rule, authors can put a cost block under the wrong
phase key (e.g. `cost.inference` on a card whose execution phase is
`orchestration`), which renders as the wrong heading in the Taipy AI/ML
Workflows detail panel and silently misleads operators reading the cost page.

Prior incident: `wf-hf-sync.yaml` declared `execution.orchestration` but
carried `cost.inference` as a workaround for the Taipy renderer's hardcoded
`("training", "inference")` iteration. The render loop has since been updated
to iterate every phase; this test enforces the YAML contract so the two
sides stay in sync.
"""

from __future__ import annotations

import re
from pathlib import Path

import pytest
import yaml

_REPO = Path(__file__).resolve().parents[2]
_CARDS_DIR = _REPO / "workflow-cards"
_FRONTMATTER = re.compile(r"^---\s*\n(.*?\n)---\s*\n", re.DOTALL)


def _load_card(path: Path) -> dict:
    text = path.read_text(encoding="utf-8")
    m = _FRONTMATTER.match(text)
    if not m:
        pytest.fail(f"Card {path.name} has no YAML frontmatter")
    return yaml.safe_load(m.group(1))


def test_every_cost_phase_has_matching_execution_phase() -> None:
    """For every card, every key under `cost:` must also be a key under
    `execution:`. The converse is not required — a card may declare an
    execution phase without a cost estimate."""
    errors: list[str] = []
    for path in sorted(_CARDS_DIR.glob("wf-*.yaml")):
        card = _load_card(path)
        exec_cfg = card.get("execution") or {}
        cost_cfg = card.get("cost") or {}
        if not isinstance(exec_cfg, dict) or not isinstance(cost_cfg, dict):
            continue
        exec_phases = {k for k, v in exec_cfg.items() if isinstance(v, dict)}
        cost_phases = {k for k, v in cost_cfg.items() if isinstance(v, dict)}
        missing_exec = cost_phases - exec_phases
        if missing_exec:
            errors.append(
                f"{path.name}: cost declares phase(s) {sorted(missing_exec)} "
                f"that have no matching execution phase "
                f"(execution phases: {sorted(exec_phases)})"
            )
    assert not errors, "\n".join(errors)
