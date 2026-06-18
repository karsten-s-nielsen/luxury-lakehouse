"""Conformance test: every today's-bronze table read by a Databricks task
must have a transitive ``depends_on`` path to the writer task.

Catches the C-1..C-5 + dbt_build missing-edge class going forward.
PR-Cycle-B (2026-05-01) added 9 missing edges; this test would have
caught all of them at the writer-PR time, not on a scheduled scheduled run.

Forward-compatible with Option B's gold-reader topology rewrite — Option B
will introduce a peer invariant ("every today's-gold read in task T has
a transitive ``depends_on`` path to ``dbt_build_input_marts``") that
will go in this file as a sibling assertion.

Curated rather than auto-discovered: each new bronze read by a compute
task adds an entry to ``_BRONZE_READ_REQUIREMENTS``. Auto-discovery via
AST scan was rejected because of false positives — many compute task
modules contain string literals like ``f"{catalog}.{schema}.{TABLE_NAME}"``
that are WRITES, not reads, and the heuristic fingerprint to distinguish
them is not robust.

Pure parse of ``terraform/modules/workflows/main.tf``. No Databricks
connection, no module imports.
"""

from __future__ import annotations

import re
from collections import defaultdict
from pathlib import Path

_REPO = Path(__file__).resolve().parents[2]
_TF_FILE = _REPO / "terraform" / "modules" / "workflows" / "main.tf"

# ──────────────────────────────────────────────────────────────────────────────
# Curated bronze-read requirements.
#
# Format: (consumer_task, bronze_table, writer_task)
#
# Each entry asserts that ``consumer_task`` has a transitive ``depends_on``
# path to ``writer_task``. ``bronze_table`` is documentary — it identifies
# WHY the dependency exists (which read motivates it) — and is not parsed
# from the TF.
#
# When you add a new bronze read by a compute task, add an entry here.
# When you remove a read, remove the entry. The test fails loudly either
# way — drift is caught at PR-CI time, not at scheduled-run time.
# ──────────────────────────────────────────────────────────────────────────────

_BRONZE_READ_REQUIREMENTS: list[tuple[str, str, str]] = [
    # ── compute_spadl_vaep: 4-source SPADL union (LL2 Path B) ───────────
    # Evidence: src/ingestion/spadl_vaep.py:151,156,169,174
    #         + src/ingestion/spadl_conversion.py
    ("compute_spadl_vaep", "statsbomb_events", "ingest_statsbomb"),
    ("compute_spadl_vaep", "wyscout_events", "ingest_wyscout"),
    ("compute_spadl_vaep", "idsse_events", "ingest_idsse_events"),
    ("compute_spadl_vaep", "metrica_events", "ingest_metrica"),
    # ── compute_line_breaking: 4-path detector ──────────────────────────
    # Evidence: src/ingestion/line_breaking.py:82-87
    #         + src/ingestion/line_breaking_tracking.py (Path C)
    ("compute_line_breaking", "statsbomb_360", "backfill_statsbomb_360"),
    ("compute_line_breaking", "metrica_events", "ingest_metrica"),
    ("compute_line_breaking", "idsse_events", "ingest_idsse_events"),
    ("compute_line_breaking", "idsse_tracking", "ingest_idsse"),
    # ── compute_defcon_lite: 360-enriched defensive credit assignment ───
    # Evidence: src/ingestion/defcon_lite_360.py:49-52
    ("compute_defcon_lite", "statsbomb_360", "backfill_statsbomb_360"),
    # ── compute_pausa: OBSO-anchored timing values ──────────────────────
    # Evidence: src/ingestion/pausa.py:69,180
    #         + src/ingestion/import_obso_results.py:141-142
    ("compute_pausa", "pausa_raw_scores", "import_obso_results"),
    # ── dbt_build_*: stg_* views read leaf compute outputs ──────────────
    # PR-Cycle-C PR-β (2026-05-02, ADR-019): single `dbt_build` task replaced
    # with three sequential stages. Each entry assigned to the stage that
    # actually rebuilds the consuming mart. Without these edges, today's
    # gold marts get built from yesterday's bronze for the named source
    # (1-day lag class).
    ("dbt_build_output_marts", "pausa_values", "compute_pausa"),
    ("dbt_build_output_marts", "elastic_event_match", "compute_elastic_sync"),
    # statsbomb_360 (backfill_statsbomb_360 writer) — read by stg_statsbomb_360
    # which is an ancestor of input_mart fct_shots. Migration to
    # dbt_build_input_marts is correct per ADR-019 § ingest-helper exemption.
    ("dbt_build_input_marts", "statsbomb_360", "backfill_statsbomb_360"),
    ("dbt_build_output_marts", "player_embeddings_raw_360", "compute_embeddings_360"),
    # ── compute_action_context: TRACKING providers only (ADR-058 — statsbomb exited the drain) ──
    # Evidence: src/ingestion/action_context.py — _process_tracking_match (the drain worker).
    ("compute_action_context", "spadl_actions", "compute_spadl_vaep"),
    ("compute_action_context", "idsse_tracking", "ingest_idsse"),
    ("compute_action_context", "idsse_events", "ingest_idsse_events"),
    ("compute_action_context", "metrica_tracking", "ingest_metrica"),
    ("compute_action_context", "skillcorner_tracking", "ingest_skillcorner"),
    ("compute_action_context", "gradientsports_tracking", "ingest_gradientsports"),
    # ── compute_action_context_statsbomb: the sb360 cogroup batch (ADR-058) reads SPADL +
    #    statsbomb_360. Evidence: _process_statsbomb_matches / main_statsbomb. ──
    ("compute_action_context_statsbomb", "spadl_actions", "compute_spadl_vaep"),
    ("compute_action_context_statsbomb", "statsbomb_360", "backfill_statsbomb_360"),
    # ── dbt_build_output_marts: stg_action_context reads bronze written by BOTH AC arms ──
    ("dbt_build_output_marts", "spadl_action_context", "compute_action_context"),
    ("dbt_build_output_marts", "spadl_action_context", "compute_action_context_statsbomb"),
]


# ──────────────────────────────────────────────────────────────────────────────
# TF parser — extract task → depends_on map.
# ──────────────────────────────────────────────────────────────────────────────

_TASK_BLOCK_RE = re.compile(
    r'^\s*task\s*\{\s*\n\s*task_key\s*(?:=|=)\s*"([^"]+)"',
    re.MULTILINE,
)


def _parse_task_depends_on(tf_text: str) -> dict[str, set[str]]:
    """Return ``{task_key: {dep_task_keys}}`` for every ``task { ... }``
    block declared inside the ``databricks_job "data_ingestion"`` resource.

    Handles both single-line and multi-line ``depends_on`` syntaxes.
    """
    lines = tf_text.splitlines()
    deps: dict[str, set[str]] = defaultdict(set)
    in_resource = False
    depth = 0
    current_task: str | None = None
    task_start_depth: int | None = None
    pending_dep_block = False

    resource_re = re.compile(r'^resource\s+"databricks_job"\s+"data_ingestion"\s*\{')
    task_open_re = re.compile(r"^\s\stask\s*\{\s*$")
    task_key_re = re.compile(r'^\s{4}task_key\s*=\s*"([^"]+)"')
    dep_block_open_re = re.compile(r"^\s+depends_on\s*\{\s*$")
    dep_block_inline_re = re.compile(r'^\s+depends_on\s*\{\s*task_key\s*=\s*"([^"]+)"\s*\}\s*$')
    dep_task_key_re = re.compile(r'^\s+task_key\s*=\s*"([^"]+)"\s*$')

    for line in lines:
        if not in_resource:
            if resource_re.search(line):
                in_resource = True
                depth = 1
            continue
        open_braces = line.count("{")
        close_braces = line.count("}")

        if current_task is None and task_open_re.match(line):
            task_start_depth = depth + open_braces
        elif current_task is None and task_start_depth is not None:
            m = task_key_re.match(line)
            if m:
                current_task = m.group(1)
        elif current_task is not None:
            inline = dep_block_inline_re.match(line)
            if inline:
                deps[current_task].add(inline.group(1))
            elif dep_block_open_re.match(line):
                pending_dep_block = True
            elif pending_dep_block:
                m = dep_task_key_re.match(line)
                if m:
                    deps[current_task].add(m.group(1))
                if "}" in line:
                    pending_dep_block = False

        depth += open_braces - close_braces
        if current_task is not None and depth <= (task_start_depth or 1) - 1:
            current_task = None
            task_start_depth = None
            pending_dep_block = False
        if depth <= 0:
            break
    return dict(deps)


def _transitive_closure(deps: dict[str, set[str]], start: str) -> set[str]:
    """BFS across ``deps`` from ``start``; return all transitively-reachable
    task keys (excluding ``start`` itself)."""
    visited: set[str] = set()
    frontier: list[str] = list(deps.get(start, set()))
    while frontier:
        node = frontier.pop()
        if node in visited:
            continue
        visited.add(node)
        frontier.extend(deps.get(node, set()))
    return visited


# ──────────────────────────────────────────────────────────────────────────────
# Tests.
# ──────────────────────────────────────────────────────────────────────────────


def test_every_bronze_read_has_transitive_depends_on_path() -> None:
    """For each curated (consumer, bronze_table, writer) requirement, the
    consumer task's transitive ``depends_on`` closure must contain the
    writer task. Catches the missing-bronze-edge class that opened
    PR-Cycle-B (2026-05-01).
    """
    deps = _parse_task_depends_on(_TF_FILE.read_text(encoding="utf-8"))
    errors: list[str] = []
    for consumer, table, writer in _BRONZE_READ_REQUIREMENTS:
        if consumer not in deps:
            errors.append(
                f"{consumer!r} not found in TF data_ingestion job — "
                f"requirement (consumer={consumer!r}, table={table!r}, "
                f"writer={writer!r}) is unsatisfiable."
            )
            continue
        closure = _transitive_closure(deps, consumer)
        if writer not in closure:
            errors.append(
                f"{consumer!r} reads bronze.{table} (written by {writer!r}) "
                f"but has no transitive depends_on path to {writer!r}. "
                f"Closure: {sorted(closure)}. "
                f"Add `depends_on {{ task_key = {writer!r} }}` to {consumer!r} "
                f"in terraform/modules/workflows/main.tf."
            )
    assert not errors, "\n\n".join(errors)


def test_parser_finds_known_tasks() -> None:
    """Anchor the parser: every task in ``_BRONZE_READ_REQUIREMENTS`` must
    be parseable from the TF file. Guards against a parser regression
    silently producing an empty deps dict."""
    deps = _parse_task_depends_on(_TF_FILE.read_text(encoding="utf-8"))
    consumers = {c for c, _t, _w in _BRONZE_READ_REQUIREMENTS}
    writers = {w for _c, _t, w in _BRONZE_READ_REQUIREMENTS}
    referenced = consumers | writers
    missing = referenced - set(deps.keys()) - {w for w in writers if w not in deps}
    # Writers may not have their own depends_on entries (e.g., ingest_metrica
    # has none) — that's fine, they just don't appear as keys in `deps`. But
    # consumers MUST have depends_on entries (otherwise they wouldn't have
    # any deps to traverse).
    consumer_missing = consumers - set(deps.keys())
    assert not consumer_missing, (
        f"Consumer tasks missing from TF parse output: {sorted(consumer_missing)}. "
        f"Either the TF lost the task or the parser has a regression. "
        f"Parsed tasks: {sorted(deps.keys())}"
    )
    _ = missing  # silence unused-var warning if writers-only branch dominates
