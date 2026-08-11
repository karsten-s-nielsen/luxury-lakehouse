"""Every C4 container renders in some diagram — not just in the model.

WHY THIS EXISTS
---------------
On 2026-08-11 `deployPipeline` declared **eight** containers and **no container view**, so every
operator tool in it (`manage_space`, `deploy_wheel`, `bump_wheel`, `dbt_build_and_refresh`,
`sync_tf_env_pins`, `sync_bronze_sources_yml`, `check_cve_blockers`, `audit_resolutions`)
existed in the DSL and rendered in no diagram at all. Six of them had been invisible for months.

Two mechanisms let that survive:

* **The c4 skill's coverage lint is one level too low.** It flags a CONTAINER that declares
  components but has no `component` view; it does not flag a SOFTWARE SYSTEM that declares
  containers but has no `container` view. An entire invisible subsystem trips nothing.
* **The obvious manual check is unsound.** Grepping the assembled HTML for a container's name
  proves nothing: PlantUML splits label text across `<text>` nodes, so ``CVE Review`` appears as
  ``>CVE<`` + ``>Review<`` and ``Data Quality CI`` looks absent while being present. Worse, the
  page embeds the DSL source in a panel, so a name "found" in the HTML may only be the model
  text — exactly the false pass that let the original claim through.

This test replaces the unsound check with a structural one against the DSL alone: no HTML
parsing, no SVG text matching, nothing that label wrapping can defeat.

It is the same shape as the rest of this cycle's gates (ADR-075): a PARTITION, where nothing
lands silently in neither bucket.
"""

from __future__ import annotations

import re
from pathlib import Path

_DSL = Path(__file__).resolve().parents[2] / "docs" / "c4" / "architecture.dsl"

#: `<identifier> = softwareSystem "..."` — model declarations only.
_SYSTEM_RE = re.compile(r"^\s*(\w+)\s*=\s*softwareSystem\b")

#: `<identifier> = container "..."` — the `=` is what separates a model DECLARATION from a
#: `container <systemId> "Key"` VIEW declaration, which has none.
_CONTAINER_RE = re.compile(r"^\s*(\w+)\s*=\s*container\b")

#: `container <systemId> "Key" {` inside the views block.
_CONTAINER_VIEW_RE = re.compile(r'^\s*container\s+(\w+)\s+"')


def _parse(dsl: str) -> tuple[dict[str, list[str]], set[str]]:
    """Return ({system identifier: [container identifiers]}, {systems with a container view}).

    Brace-depth tracking rather than indentation: indentation is a formatting choice and would
    make this test fail on a re-indent that changed nothing real.
    """
    systems: dict[str, list[str]] = {}
    views: set[str] = set()
    open_systems: list[tuple[int, str]] = []
    depth = 0

    for raw in dsl.splitlines():
        line = raw.split("#", 1)[0]

        system = _SYSTEM_RE.match(line)
        container = _CONTAINER_RE.match(line)
        view = _CONTAINER_VIEW_RE.match(line)

        if system:
            systems.setdefault(system.group(1), [])
            open_systems.append((depth, system.group(1)))
        elif container and open_systems:
            systems[open_systems[-1][1]].append(container.group(1))
        elif view:
            views.add(view.group(1))

        depth += line.count("{") - line.count("}")
        while open_systems and depth <= open_systems[-1][0]:
            open_systems.pop()

    return systems, views


def test_every_system_with_containers_has_a_container_view() -> None:
    """A system whose containers render nowhere is authoring effort with no output.

    This is the mirror of the c4 skill's own component-level lint, one level up — the case it
    does not cover, and the one that actually bit.
    """
    systems, views = _parse(_DSL.read_text(encoding="utf-8"))
    invisible = {system: containers for system, containers in systems.items() if containers and system not in views}
    assert not invisible, (
        'software system(s) declare containers but have no `container <systemId> "..."` view, '
        f"so those containers render in NO diagram: "
        f"{ {s: len(c) for s, c in invisible.items()} }. "
        "Add a container view for each, then regenerate architecture.html."
    )


def test_the_parse_is_not_vacuous() -> None:
    """A parser that finds nothing passes the assertion above while checking nothing.

    Both sides are pinned: real systems WITH containers, and real container views. If a DSL
    reformat breaks either regex, this fails instead of the gate going quietly green.
    """
    systems, views = _parse(_DSL.read_text(encoding="utf-8"))
    with_containers = {s: c for s, c in systems.items() if c}
    assert len(with_containers) >= 5, f"parsed only {len(with_containers)} systems with containers"
    assert len(views) >= 5, f"parsed only {len(views)} container views"
    assert "deployPipeline" in with_containers, "deployPipeline lost its containers — regex drift?"


def test_container_views_reference_real_systems() -> None:
    """A view scoped to a system that no longer exists renders an empty panel.

    The opposite drift from the one above, and equally silent.
    """
    systems, views = _parse(_DSL.read_text(encoding="utf-8"))
    dangling = sorted(v for v in views if v not in systems)
    assert not dangling, f"container view(s) scoped to unknown software system(s): {dangling}"
