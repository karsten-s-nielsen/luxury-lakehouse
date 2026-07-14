"""Workflow card <-> Terraform daily-job parity.

Rules:
  1. Every direct task in the daily job has a card with trigger=scheduled
     (or the super-task card for hf_sync) and a matching entry_point.
  2. Every card with trigger=scheduled or trigger=orchestrated has its
     entry_point present in pyproject.toml [project.scripts].
  3. Every module in src/ingestion/hf_sync.py:_SUB_OPERATIONS maps to exactly
     one card with trigger=orchestrated + orchestrated_by=wf-hf-sync.
  4. Every card declaring orchestrated_by=<id> has its id listed in card
     <id>'s execution.orchestration.sub_operations.
  5. Every sub_operations entry in the super-task card corresponds to a real
     sub-operation card whose orchestrated_by points back.
"""

from __future__ import annotations

import ast
import re
from pathlib import Path

import pytest
import tomli
import yaml

_REPO = Path(__file__).resolve().parents[2]
_CARDS_DIR = _REPO / "workflow-cards"
_MAIN_TF = _REPO / "terraform" / "modules" / "workflows" / "main.tf"
_HF_SYNC = _REPO / "src" / "ingestion" / "hf_sync.py"
_PYPROJECT = _REPO / "pyproject.toml"

_FRONTMATTER = re.compile(r"^---\s*\n(.*?\n)---\s*\n", re.DOTALL)


def _load_card(path: Path) -> dict:
    text = path.read_text(encoding="utf-8")
    m = _FRONTMATTER.match(text)
    if not m:
        pytest.fail(f"Card {path.name} has no YAML frontmatter")
    return yaml.safe_load(m.group(1))


def _card_phases(card: dict) -> dict[str, dict]:
    return {k: v for k, v in (card.get("execution") or {}).items() if isinstance(v, dict)}


def _parse_tf_task_entry_points() -> dict[str, str]:
    """Return {task_key: entry_point} for every top-level task in the daily job."""
    text = _MAIN_TF.read_text(encoding="utf-8")
    lines = text.splitlines()
    depth = 0
    in_resource = False
    result: dict[str, str] = {}
    current_task: dict[str, str | None] = {"task_key": None, "entry_point": None}
    in_task_depth: int | None = None
    resource_re = re.compile(r'^resource\s+"databricks_job"\s+"data_ingestion"\s*\{')
    task_re = re.compile(r"^\s*task\s*\{")
    task_key_re = re.compile(r'^\s*task_key\s*=\s*"([^"]+)"')
    entry_point_re = re.compile(r'^\s*entry_point\s*=\s*"([^"]+)"')

    for line in lines:
        if not in_resource:
            if resource_re.search(line):
                in_resource = True
                depth = 1
            continue
        opens = line.count("{")
        closes = line.count("}")
        if in_task_depth is None and depth == 1 and task_re.match(line):
            in_task_depth = depth + opens
            current_task = {"task_key": None, "entry_point": None}
        if in_task_depth is not None:
            if current_task["task_key"] is None:
                m = task_key_re.match(line)
                if m:
                    current_task["task_key"] = m.group(1)
            if current_task["entry_point"] is None:
                m = entry_point_re.match(line)
                if m:
                    current_task["entry_point"] = m.group(1)
        depth += opens - closes
        if in_task_depth is not None and depth < in_task_depth:
            if current_task["task_key"] and current_task["entry_point"]:
                result[current_task["task_key"]] = current_task["entry_point"]
            in_task_depth = None
        if depth <= 0:
            break
    return result


def _parse_tf_task_timeouts() -> dict[str, tuple[int, str | None]]:
    """Return ``{task_key: (timeout_seconds, entry_point)}`` for every top-level task.

    TWO PAIRING RULES, BOTH LOAD-BEARING — each one was a bug in the first cut of this parser:

    1. **The timeout is the LAST one seen before the entry_point**, i.e. the innermost enclosing
       task's. A ``for_each_task`` nests the real task inside a wrapper that carries its OWN
       ``timeout_seconds``: ``compute_spadl_vaep``'s wrapper says ``0`` while the iteration that
       actually runs the wheel says ``1800``. Taking the FIRST one reads the timeout from the
       wrapper and the entry_point from the inner task — two different blocks — and reports ``0s``
       for a task with a 30-minute budget.

    2. **Key on ``task_key``, NOT ``entry_point``.** For a for_each the two DIFFER: task_key
       ``compute_action_context`` runs entry_point ``compute_action_context_drain_worker``, while
       the card declares ``entry_point: compute_action_context`` (the task_key). An entry_point-keyed
       join therefore silently SKIPS this card — which is how the original ``7200s`` vs ``28800s``
       drift survived, and how the first version of this very guard managed to be vacuous for the
       one card that motivated it. Both names are returned so callers can match on either.
    """
    text = _MAIN_TF.read_text(encoding="utf-8")
    depth = 0
    in_resource = False
    result: dict[str, tuple[int, str | None]] = {}
    task_key: str | None = None
    timeout: int | None = None
    entry_point: str | None = None
    in_task_depth: int | None = None
    resource_re = re.compile(r'^resource\s+"databricks_job"\s+"data_ingestion"\s*\{')
    task_re = re.compile(r"^\s*task\s*\{")
    task_key_re = re.compile(r'^\s*task_key\s*=\s*"([^"]+)"')
    entry_point_re = re.compile(r'^\s*entry_point\s*=\s*"([^"]+)"')
    timeout_re = re.compile(r"^\s*timeout_seconds\s*=\s*(\d+)")

    for line in text.splitlines():
        if not in_resource:
            if resource_re.search(line):
                in_resource = True
                depth = 1
            continue
        opens = line.count("{")
        closes = line.count("}")
        if in_task_depth is None and depth == 1 and task_re.match(line):
            in_task_depth = depth + opens
            task_key, timeout, entry_point = None, None, None
        if in_task_depth is not None:
            m = task_key_re.match(line)
            if m and task_key is None:
                task_key = m.group(1)  # FIRST -> the TOP-LEVEL task's key (not the for_each child)
            m = timeout_re.match(line)
            if m:
                timeout = int(m.group(1))  # LAST -> the INNERMOST enclosing task (rule 1)
            m = entry_point_re.match(line)
            if m and entry_point is None:
                entry_point = m.group(1)
        depth += opens - closes
        if in_task_depth is not None and depth < in_task_depth:
            if task_key and timeout is not None:
                result[task_key] = (timeout, entry_point)
            in_task_depth = None
        if depth <= 0:
            break
    return result


def test_card_phase_timeouts_match_terraform() -> None:
    """A card's declared phase timeout must equal its Terraform task's `timeout_seconds`.

    TERRAFORM IS THE RUNTIME TRUTH; the card is documentation ABOUT it. A card is what an operator
    reads to answer "how long may this run before it is considered hung" — and a card that lies
    about that is worse than a card with no timeout at all.

    THIS TEST EXISTS BECAUSE THE DRIFT WAS TOTAL. When it was first written, **21 of 42** cards
    misstated their timeout — `wf-defcon` advertised 7200s for a 1500s task, `wf-pitch-control`
    7200s for 1200s, `wf-action-context` 7200s for the 28800s ADR-037 worker-drain exception. Not
    one was caught, because the parity suite checked task_key / entry_point / trigger and never
    once looked at the timeout. Card timeouts were decorative for the entire life of the repo.
    """
    tf = _parse_tf_task_timeouts()
    assert tf, "parsed ZERO task timeouts from main.tf — the parser is broken, not the cards"

    cards = {p.stem: _load_card(p) for p in _CARDS_DIR.glob("wf-*.yaml")}
    mismatches: list[str] = []
    unjoinable: list[str] = []
    checked = 0

    for task_key, (tf_timeout, tf_entry_point) in sorted(tf.items()):
        card_id = _DIRECT_TASK_ENTRY_POINT_TO_CARD.get(task_key)
        if card_id is None:
            continue  # pure orchestration (e.g. preflight_action_context) — intentionally no card
        # Match the phase on EITHER name: for a for_each the card names the task_key while
        # Terraform's python_wheel_task names a different entry_point (see rule 2 in the parser).
        phases = _card_phases(cards[card_id])
        hits = [(n, p) for n, p in phases.items() if p.get("entry_point") in (task_key, tf_entry_point)]
        if not hits:
            unjoinable.append(f"{card_id}: no phase matches task_key={task_key} or entry_point={tf_entry_point}")
            continue
        phase_name, phase = hits[0]
        declared = phase.get("timeout")
        if not declared:
            continue
        actual = int(str(declared).rstrip("s"))
        checked += 1
        if actual != tf_timeout:
            mismatches.append(
                f"{card_id}.yaml [{phase_name}] (task {task_key}): card says {actual}s, terraform says {tf_timeout}s"
            )

    # ANTI-VACUITY. A join that pairs nothing passes silently — which is EXACTLY how the original
    # drift survived, and how this guard's own first draft managed to skip the one card that
    # motivated it. Both floors must hold, or the guard is decorative in the same way the cards were.
    assert not unjoinable, "card/terraform join is broken:\n  " + "\n  ".join(unjoinable)
    assert checked >= 30, f"only {checked} card phases paired with a TF task — the join is broken"
    assert not mismatches, "card/terraform timeout drift:\n  " + "\n  ".join(mismatches)


def _parse_hf_sync_sub_operations() -> list[str]:
    """Return the module paths in hf_sync.py:_SUB_OPERATIONS via AST."""
    tree = ast.parse(_HF_SYNC.read_text(encoding="utf-8"))
    for node in tree.body:
        if not (isinstance(node, ast.AnnAssign) and isinstance(node.target, ast.Name)):
            continue
        if node.target.id != "_SUB_OPERATIONS":
            continue
        assert isinstance(node.value, ast.List), "_SUB_OPERATIONS must be a list literal"
        modules: list[str] = []
        for item in node.value.elts:
            assert isinstance(item, ast.Tuple) and len(item.elts) == 2
            first = item.elts[0]
            assert isinstance(first, ast.Constant) and isinstance(first.value, str)
            modules.append(first.value)
        return modules
    pytest.fail("_SUB_OPERATIONS not found in hf_sync.py")


def _parse_pyproject_entry_points() -> dict[str, str]:
    data = tomli.loads(_PYPROJECT.read_text(encoding="utf-8"))
    return dict((data.get("project") or {}).get("scripts") or {})


# ---------------------------------------------------------------------------
# Rule 1: direct TF tasks -> matching scheduled card (or hf_sync super-task)
# ---------------------------------------------------------------------------

# TF task_key -> expected card id. `None` = intentionally no owning card
# (documented governance gap). Every `None` must carry a justification
# comment explaining why CLAUDE.md:253 ("AI/ML workflows ... academic
# provenance, cost estimates, monitoring thresholds") does not apply to
# this entry point. Keep the right-hand side stable; update when TF
# tasks are renamed.
_DIRECT_TASK_ENTRY_POINT_TO_CARD: dict[str, str | None] = {
    "ingest_statsbomb": "wf-statsbomb",
    "ingest_metrica": "wf-metrica",
    "ingest_wyscout": "wf-wyscout",
    "ingest_idsse": "wf-idsse",
    "ingest_gradientsports": "wf-gradientsports",
    "ingest_skillcorner": "wf-skillcorner",
    # Separate XML schema (DFL_03_02 events) subordinate to the IDSSE source
    # bundle already governed by wf-idsse. Pure data-relay helper with no
    # ML methodology and no per-player evaluation — falls outside CLAUDE.md:253.
    "ingest_idsse_events": None,
    # PR-Cycle-A (2026-04-30): Runtime chunk-discovery preflight feeding the
    # `ingest_idsse` for_each_task fan-out. Pure orchestration helper —
    # writes a Databricks task value (`idsse_match_chunks`) and exits.
    # Subordinate to wf-idsse; no independent methodology, no per-player
    # output.
    "preflight_idsse": None,
    # TC-1 memory fix: Runtime chunk-discovery preflight feeding the
    # `compute_tracking_context` for_each_task fan-out. Pure orchestration
    # helper — writes a Databricks task value and exits.
    # Subordinate to wf-tracking-context; no independent methodology.
    "preflight_tracking_context": None,
    # Idempotent repair helper that refills _raw_extra_json where upstream
    # ingest left it NULL. Subordinate to wf-statsbomb; no independent
    # methodology or output.
    "backfill_statsbomb_extra": None,
    # Idempotent repair helper for StatsBomb 360 freeze-frame catchup after
    # the 360 code path was added. Subordinate to wf-statsbomb; no
    # independent methodology.
    "backfill_statsbomb_360": None,
    # SPADL/VAEP chunked execution: Runtime chunk-discovery preflight feeding
    # the `compute_spadl_vaep` for_each_task fan-out. Pure orchestration
    # helper — writes Databricks task values and exits.
    # Subordinate to wf-vaep; no independent methodology.
    "preflight_spadl_vaep": None,
    # Gradient Sports fan-out: Runtime match-discovery preflight feeding
    # the `ingest_gradientsports` for_each_task fan-out. Pure orchestration
    # helper — writes a Databricks task value (`gradientsports_matches`)
    # and exits. Subordinate to wf-gradientsports; no independent methodology.
    "preflight_gradientsports": None,
    "compute_spadl_vaep": "wf-vaep",
    "compute_expected_threat": "wf-xt-grids",
    # compute_xg_model (v1) retired SK3-MIG-B 2026-05-03 (XG1-RETIRE per ADR-023);
    # compute_xg_model_v2 retired 2026-07-10 (v2 producer chain — ADR-066).
    # Canonical-SPADL pre-shot xG (Task 1.9, 2026-07-08): scores fct_action_values with
    # xg_model_v3@Champion -> bronze.xg_shot_predictions (two-mode gate + per-provider calibration).
    "compute_xg_shot_scores": "wf-shot-xg-scorer",
    "compute_off_ball_xt": "wf-off-ball-xt",
    "compute_pitch_control": "wf-pitch-control",
    "compute_formations_efpi": "wf-formations",
    "compute_formations_shape_graph": "wf-shape-graphs",
    "compute_line_breaking": "wf-line-breaking",
    "compute_defcon_lite": "wf-defcon",
    # Canonical-SPADL pre-shot xG (Task 0.5, 2026-07-07): populates bronze.shot_freeze_frames.
    "compute_shot_freeze_frames": "wf-shot-freeze-frames",
    "resolve_players": "wf-entity-resolution",
    "compute_embeddings_v2": "wf-football2vec-v2",
    # compute_embeddings_v1 removed — v1 Doc2Vec deprecated (2026-05-07).
    # wf-football2vec.yaml stays on disk (status: deprecated) but is no
    # longer wired to any TF task or pyproject entry point.
    "compute_embeddings_360": "wf-football2vec-360",
    "compute_elastic_sync": "wf-elastic-sync",
    "compute_pausa": "wf-obso-pausa",
    "compute_tracking_context": "wf-tracking-context",
    "compute_action_context": "wf-action-context",
    # ADR-058: statsbomb sb360 arm of action-context — a single distributed cogroup.applyInPandas
    # job (statsbomb exits the per-match drain). Same methodology + card as compute_action_context.
    "compute_action_context_statsbomb": "wf-action-context",
    # AC-1 fan-out: Runtime chunk-discovery preflight feeding the
    # `compute_action_context` for_each_task fan-out. Pure orchestration
    # helper — writes Databricks task values and exits.
    # Subordinate to wf-action-context; no independent methodology.
    "preflight_action_context": None,
    # D8 (2026-07-13): the drain-completeness gate — the fan-in over BOTH action-context arms
    # (the 8-way drain for_each + the sb360 cogroup task). Same methodology + card as
    # compute_action_context; it asserts the enrichment finished rather than producing new
    # features, so it carries no independent methodology of its own.
    "verify_action_context_drain": "wf-action-context",
    # PR-Cycle-B (2026-05-01): split out of wf-hf-sync into its own scheduled
    # Databricks task so wf-obso-pausa can declare an explicit dependency.
    "import_obso_results": "wf-import-obso",
    "run_model_validation": "wf-model-validation",
    # ADR-063 H4: cross-cutting derived-artifact staleness monitor (detect-and-alert).
    "run_staleness_monitor": "wf-staleness-monitor",
    # Metadata extraction plumbing — reads tracking player/team names from
    # IDSSE DFL match info XMLs into a bronze Delta table for dbt resolution.
    # No ML, no methodology.
    "extract_tracking_metadata": None,
    "hf_sync": "wf-hf-sync",
    # PR-Cycle-C PR-β (2026-05-02, ADR-019): single `dbt_build` task replaced
    # with three sequential dbt invocations driven by mart classification tags.
    # All three share the same `dbt_build` wheel entry point — differentiated
    # by the `--select` parameter passed in TF. Cards are split 1→3 to match.
    "dbt_build_input_marts": "wf-dbt-build-input-marts",
    "dbt_build_intermediate_marts": "wf-dbt-build-intermediate-marts",
    "dbt_build_output_marts": "wf-dbt-build-output-marts",
    # Infrastructure plumbing — triggers Lakebase SNAPSHOT refresh via the
    # Databricks REST API. Not an AI/ML workflow under CLAUDE.md:253.
    "refresh_synced_tables": None,
}


# ---------------------------------------------------------------------------
# HF Jobs publish scripts (not TF wheel tasks, not tracked by rules 1/3)
# ---------------------------------------------------------------------------

# Relative path under scripts/ -> expected card id. `None` = intentionally
# no owning card. Justification required. HF Jobs scripts are invoked on
# demand via `hf jobs uv run scripts/<name>` — they are not part of the
# daily Databricks job, so they do not appear in TF task mappings.
_HF_JOBS_SCRIPT_TO_CARD: dict[str, str | None] = {
    "publish_xg_shots_hf.py": "wf-publish-xg-shots",
    "publish_spadl_vaep_hf.py": "wf-publish-spadl-vaep",
    "publish_shots_on_target_hf.py": "wf-publish-shots-on-target",
    # Freeze-frame positions are supporting context for the xG v2 Deep Sets
    # encoder but do not themselves constitute a published methodology or a
    # per-player evaluative output. The upstream source (StatsBomb freeze
    # frames) is governed by wf-statsbomb; the downstream consumer model
    # (xG v2) is governed by wf-xg-v2. No independent governance needed.
    "publish_freeze_frame_hf.py": None,
    # SK3-MIG-B 2026-05-03 (HF4 migration) — added 4 publishers replacing the
    # notebook publishers in notebooks/publish_datasets.py + publish_obso_data.py.
    # Each publisher's dataset card is governed by ADR-014; these scripts live
    # in scripts/ and are invoked on-demand by the SK3-MIG-B orchestrator.
    "publish_obso_pausa_inputs_hf.py": None,
    "publish_football2vec_embeddings_hf.py": None,
    "publish_line_breaking_passes_hf.py": None,
    "publish_pitch_control_tracking_hf.py": None,
    "publish_tracking_context_hf.py": None,
    "publish_action_context_hf.py": None,
    # Operator-run (PEP 723), like publish_action_context_hf.py: publishes the gold
    # shot-grain PSxG fact (fct_shot_psxg) to psxg-shots + the ADR-049 private
    # companion. Dataset cards governed by ADR-014; the PSxG model's per-player
    # governance lives in wf-goalkeeper. No independent owning card.
    "publish_psxg_shots_hf.py": None,
    # Pre-Shot xG v3 delivery Task 1.2 (spec §A3, 2026-07-07): operator-run (PEP 723), like
    # publish_action_context_hf.py — publishes the tabular shot corpus (xg-shot-data-v3 + the
    # ADR-049 private companion) that trains xg_model_v3. Dataset cards governed by ADR-014;
    # the pre-shot xG model's governance lives under wf-xg-v2. No independent owning card.
    "publish_xg_shot_data_v3_hf.py": None,
    # Pre-Shot xG v3 delivery Task 1.3 (spec §A4, 2026-07-07): operator-run (PEP 723), like
    # publish_xg_shot_data_v3_hf.py — publishes the freeze-frame / player-set (context) corpus
    # (xg-shot-freeze-frames + the ADR-049 private companion) that trains xg_model_v3. Dataset
    # cards governed by ADR-014; the pre-shot xG model's governance lives under wf-xg-v2. No
    # independent owning card.
    "publish_shot_freeze_frames_hf.py": None,
}


def _known_hf_jobs_publish_scripts() -> set[str]:
    """Return the set of `publish_*_hf.py` scripts present on disk."""
    return {p.name for p in (_REPO / "scripts").glob("publish_*_hf.py")}


def test_hf_jobs_script_mapping_matches_disk() -> None:
    """The _HF_JOBS_SCRIPT_TO_CARD mapping must stay in sync with the
    publish_*_hf.py scripts on disk. A new publish script must be either
    added with its owning card id, or added with None plus a justification
    comment — silent omission is not allowed."""
    on_disk = _known_hf_jobs_publish_scripts()
    mapped = set(_HF_JOBS_SCRIPT_TO_CARD)
    unexpected = on_disk - mapped
    missing = mapped - on_disk
    assert not unexpected, (
        f"HF Jobs publish script(s) present on disk but not in "
        f"_HF_JOBS_SCRIPT_TO_CARD: {sorted(unexpected)}. Add them with a "
        f"card id, or None + justification comment."
    )
    assert not missing, (
        f"_HF_JOBS_SCRIPT_TO_CARD references scripts that no longer exist: "
        f"{sorted(missing)}. Remove the entry or restore the script."
    )


def test_every_mapped_hf_jobs_script_has_card() -> None:
    """Scripts mapped to a card id must have the card file on disk and
    the card must declare the script in an execution phase."""
    cards = {p.stem: _load_card(p) for p in _CARDS_DIR.glob("wf-*.yaml")}
    errors: list[str] = []
    for script_name, card_id in _HF_JOBS_SCRIPT_TO_CARD.items():
        if card_id is None:
            continue
        if card_id not in cards:
            errors.append(f"Mapping {script_name!r} -> {card_id!r} but card file is missing")
            continue
        phases = _card_phases(cards[card_id])
        script_rel = f"scripts/{script_name}"
        found = any(phase.get("script") == script_rel for phase in phases.values())
        if not found:
            errors.append(
                f"{card_id}: no execution phase declares script={script_rel!r}; "
                f"mapping expects the card to own this script"
            )
    assert not errors, "\n".join(errors)


def test_mapping_matches_tf_task_list() -> None:
    """The mapping at the top of this file must stay in sync with TF reality."""
    tf_tasks = _parse_tf_task_entry_points()
    unexpected = set(tf_tasks) - set(_DIRECT_TASK_ENTRY_POINT_TO_CARD)
    missing = set(_DIRECT_TASK_ENTRY_POINT_TO_CARD) - set(tf_tasks)
    assert not unexpected, (
        f"TF has task(s) not classified: {sorted(unexpected)}. "
        "Add them to _DIRECT_TASK_ENTRY_POINT_TO_CARD (with None for intentional gaps)."
    )
    assert not missing, (
        f"Mapping references TF tasks that no longer exist: {sorted(missing)}. Remove the entry or restore the TF task."
    )


def test_every_direct_tf_task_has_scheduled_card() -> None:
    """Every TF task in the mapping must have its expected card with a
    phase whose entry_point matches and whose trigger is 'scheduled'.
    Exception: the hf_sync super-task card uses an 'orchestration' phase
    with trigger=scheduled."""
    tf_tasks = _parse_tf_task_entry_points()
    cards = {p.stem: _load_card(p) for p in _CARDS_DIR.glob("wf-*.yaml")}
    errors: list[str] = []
    for entry_point, card_id in _DIRECT_TASK_ENTRY_POINT_TO_CARD.items():
        if card_id is None:
            continue
        if entry_point not in tf_tasks.values():
            continue  # covered by test_mapping_matches_tf_task_list
        if card_id not in cards:
            errors.append(f"Mapping points at missing card {card_id!r}")
            continue
        phases = _card_phases(cards[card_id])
        matched = False
        for phase_name, phase in phases.items():
            if phase.get("entry_point") == entry_point:
                trig = phase.get("trigger")
                # hf_sync's orchestration phase uses trigger=scheduled
                expected = "scheduled"
                if trig != expected:
                    errors.append(
                        f"{card_id} phase {phase_name!r} entry_point={entry_point!r} "
                        f"but trigger={trig!r} (expected {expected!r} — it is a direct TF task)"
                    )
                matched = True
                break
        if not matched:
            errors.append(f"{card_id} has no phase with entry_point={entry_point!r}")
    assert not errors, "\n".join(errors)


# ---------------------------------------------------------------------------
# Rule 3/4/5: hf_sync super-task bidirectional parity
# ---------------------------------------------------------------------------

# hf_sync.py module path -> expected card id. Kept explicit because module
# paths and card ids don't share a single mechanical transformation.
# PR-Cycle-B (2026-05-01): ingestion.import_obso_results split out of
# hf_sync._SUB_OPERATIONS into its own Databricks task; mapping moved to
# _DIRECT_TASK_ENTRY_POINT_TO_CARD above.
_MODULE_TO_CARD: dict[str, str] = {
    "ingestion.import_space_creation": "wf-import-space-creation",
    "ingestion.import_psxg_predictions": "wf-import-psxg",
    "ingestion.export_embeddings_training_data": "wf-football2vec-v2-export",
    "ingestion.export_shots_on_target": "wf-export-shots",
    "ingestion.prepare_360_training_data": "wf-prepare-360-data",
    "ingestion.export_scoutgpt_training_data": "wf-scoutgpt-export",
    "ingestion.publish_spadl_vaep_hf": "wf-publish-spadl-vaep",
    "ingestion.publish_xg_shots_hf": "wf-publish-xg-shots",
    "ingestion.publish_freeze_frame_hf": "wf-publish-freeze-frames",
    "ingestion.sync_hf_costs": "wf-sync-hf-costs",
}


def test_module_to_card_mapping_matches_hf_sync() -> None:
    modules = _parse_hf_sync_sub_operations()
    unexpected = set(modules) - set(_MODULE_TO_CARD)
    missing = set(_MODULE_TO_CARD) - set(modules)
    assert not unexpected, f"hf_sync.py:_SUB_OPERATIONS has module(s) not in _MODULE_TO_CARD: {sorted(unexpected)}"
    assert not missing, f"_MODULE_TO_CARD references modules not in hf_sync.py:_SUB_OPERATIONS: {sorted(missing)}"


def test_hf_sync_super_task_declares_all_sub_operations_in_order() -> None:
    modules = _parse_hf_sync_sub_operations()
    expected = [_MODULE_TO_CARD[m] for m in modules]
    cards = {p.stem: _load_card(p) for p in _CARDS_DIR.glob("wf-*.yaml")}
    assert "wf-hf-sync" in cards, "wf-hf-sync.yaml must exist as the super-task card"
    orch = _card_phases(cards["wf-hf-sync"]).get("orchestration")
    assert orch, "wf-hf-sync must declare an execution.orchestration phase"
    declared = list(orch.get("sub_operations") or [])
    assert declared == expected, (
        f"wf-hf-sync.execution.orchestration.sub_operations must match "
        f"hf_sync.py:_SUB_OPERATIONS order.\n  declared: {declared}\n  expected: {expected}"
    )


def test_each_sub_operation_card_points_back() -> None:
    modules = _parse_hf_sync_sub_operations()
    cards = {p.stem: _load_card(p) for p in _CARDS_DIR.glob("wf-*.yaml")}
    errors: list[str] = []
    for module in modules:
        card_id = _MODULE_TO_CARD[module]
        assert card_id in cards, f"Missing sub-operation card {card_id!r}"
        phases = _card_phases(cards[card_id])
        orchestrated_phases = [
            (name, phase) for name, phase in phases.items() if phase.get("trigger") == "orchestrated"
        ]
        if not orchestrated_phases:
            errors.append(f"{card_id} must have a phase with trigger='orchestrated'")
            continue
        if len(orchestrated_phases) > 1:
            errors.append(
                f"{card_id} has multiple orchestrated phases — only one expected "
                f"(found {[n for n, _ in orchestrated_phases]})"
            )
            continue
        phase_name, phase = orchestrated_phases[0]
        if phase.get("orchestrated_by") != "wf-hf-sync":
            errors.append(
                f"{card_id} phase {phase_name!r} must declare orchestrated_by='wf-hf-sync' "
                f"(got {phase.get('orchestrated_by')!r})"
            )
    assert not errors, "\n".join(errors)


# ---------------------------------------------------------------------------
# Rule 2: scheduled / orchestrated cards must have entry_point in pyproject.toml
# ---------------------------------------------------------------------------


def test_scheduled_and_orchestrated_cards_have_entry_points_in_pyproject() -> None:
    scripts = _parse_pyproject_entry_points()
    cards = {p.stem: _load_card(p) for p in _CARDS_DIR.glob("wf-*.yaml")}
    errors: list[str] = []
    for card_id, card in cards.items():
        for phase_name, phase in _card_phases(card).items():
            trig = phase.get("trigger")
            if trig not in ("scheduled", "orchestrated"):
                continue
            entry = phase.get("entry_point")
            if entry and entry not in scripts:
                errors.append(
                    f"{card_id} phase {phase_name!r} entry_point={entry!r} "
                    f"(trigger={trig!r}) is absent from pyproject.toml [project.scripts]"
                )
    assert not errors, "\n".join(errors)
