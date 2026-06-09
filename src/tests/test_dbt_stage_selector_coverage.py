"""Meta-test: the union of the three-stage dbt_build ``--select`` selectors
(terraform/modules/workflows/main.tf) must build EVERY dbt model.

Why this exists (the gap it closes): dbt staging models are materialized as
**views** — a view's column list is frozen at creation, so an additive schema
change to a staging model's SQL only takes effect when the view is rebuilt. The
ADR-019 three-stage flow selects marts by tag (stages 1/2 with ``+`` ancestors,
stage 3 ``tag:output_mart`` with NO ``+``). A staging view that feeds ONLY an
output mart is therefore never selected by any stage → never rebuilt → goes
permanently schema-stale, and any build of that output mart fails with
``UNRESOLVED_COLUMN``. This is exactly what happened to
``stg_action_context__values`` (fed only ``fct_action_context``) after the
PR-#337 GK-zone columns were added.

This test resolves each stage selector against the dbt ref-graph and asserts
the union covers all models, so an orphaned model can never recur. Pure regex
scan of the .sql files + the TF parameters; no dbt manifest / warehouse needed.

References: ADR-019 (three-stage dbt_build, amended for full model coverage).
"""

from __future__ import annotations

import re
from pathlib import Path

_REPO = Path(__file__).resolve().parents[2]
_MODELS = _REPO / "dbt_project" / "models"
_WORKFLOWS_TF = _REPO / "terraform" / "modules" / "workflows" / "main.tf"

_THREE_STAGE_TASKS = ("dbt_build_input_marts", "dbt_build_intermediate_marts", "dbt_build_output_marts")

# Locates the `{{ config(...) }}` opening; the close is found by _config_body
# (quote/paren-aware) so a macro pre_hook value containing `) }}` (ADR-043) does
# not prematurely truncate the body before `tags=[...]`.
_CONFIG_OPEN_RE = re.compile(r"\{\{\s*config\s*\(")
_TAGS_KWARG_RE = re.compile(r"\btags\s*=\s*\[([^\]]*)\]", re.DOTALL)
_TAG_LITERAL_RE = re.compile(r"['\"]([^'\"]+)['\"]")
_REF_RE = re.compile(r"ref\s*\(\s*['\"]([^'\"]+)['\"]")


def _all_models() -> dict[str, Path]:
    """model_name -> .sql path for every model under models/ (staging/intermediate/marts)."""
    return {p.stem: p for p in _MODELS.rglob("*.sql")}


def _config_body(text: str) -> str | None:
    """Kwargs body of `{{ config(...) }}`, via a quote/paren-aware scan (see
    test_dbt_mart_classification._config_body). Robust to kwarg values that
    contain `)`/`) }}` (macro pre_hooks, contract=(...))."""
    m = _CONFIG_OPEN_RE.search(text)
    if not m:
        return None
    depth = 1
    quote: str | None = None
    out: list[str] = []
    for ch in text[m.end() :]:
        if quote is not None:
            if ch == quote:
                quote = None
            out.append(ch)
            continue
        if ch in ("'", '"'):
            quote = ch
            out.append(ch)
            continue
        if ch == "(":
            depth += 1
        elif ch == ")":
            depth -= 1
            if depth == 0:
                return "".join(out)
        out.append(ch)
    return "".join(out)


def _tags(path: Path) -> set[str]:
    body = _config_body(path.read_text(encoding="utf-8"))
    if body is None:
        return set()
    t = _TAGS_KWARG_RE.search(body)
    return set(_TAG_LITERAL_RE.findall(t.group(1))) if t else set()


def _direct_refs(path: Path) -> set[str]:
    return set(_REF_RE.findall(path.read_text(encoding="utf-8")))


def _build_graph() -> tuple[dict[str, set[str]], dict[str, set[str]]]:
    """Return (tags_by_model, ancestors_by_model). Ancestors = transitive ref() parents."""
    models = _all_models()
    tags_by = {name: _tags(p) for name, p in models.items()}
    direct = {name: (_direct_refs(p) & models.keys()) for name, p in models.items()}

    def ancestors(name: str) -> set[str]:
        seen: set[str] = set()
        stack = list(direct.get(name, set()))
        while stack:
            cur = stack.pop()
            if cur in seen:
                continue
            seen.add(cur)
            stack.extend(direct.get(cur, set()))
        return seen

    return tags_by, {name: ancestors(name) for name in models}


def _resolve_selector(sel: str, models: set[str], tags_by: dict[str, set[str]], anc: dict[str, set[str]]) -> set[str]:
    """Resolve ONE dbt selector token to a model set. Supports tag:/+tag:/path:/bare-name."""
    with_anc = sel.startswith("+")
    body = sel[1:] if with_anc else sel
    if body.startswith("tag:"):
        tag = body[len("tag:") :]
        base = {m for m in models if tag in tags_by.get(m, set())}
    elif body.startswith("path:"):
        rel = body[len("path:") :]  # dbt path: is relative to the dbt project root (dbt_project/)
        paths = _all_models()
        base = {
            m for m in models if str(paths[m].relative_to(_REPO / "dbt_project")).replace("\\", "/").startswith(rel)
        }
    else:  # bare model name
        base = {body} & models
    if with_anc:
        base = base | {a for m in base for a in anc.get(m, set())}
    return base


def _stage_selectors() -> dict[str, dict[str, list[str]]]:
    """Parse the `parameters = [...]` of each dbt_build task -> {select:[...], exclude:[...]}."""
    src = _WORKFLOWS_TF.read_text(encoding="utf-8")
    out: dict[str, dict[str, list[str]]] = {}
    for task in _THREE_STAGE_TASKS:
        idx = src.find(f'task_key        = "{task}"')
        assert idx != -1, f"{task} not found in {_WORKFLOWS_TF}"
        window = src[idx : idx + 2500]
        params_m = re.search(r"parameters\s*=\s*\[(.*?)\]", window, re.DOTALL)
        assert params_m, f"{task} has no parameters list"
        toks = _TAG_LITERAL_RE.findall(params_m.group(1))
        sel: list[str] = []
        exc: list[str] = []
        mode = None
        for tok in toks:
            if tok == "--select":
                mode = "sel"
            elif tok == "--exclude":
                mode = "exc"
            elif tok.startswith("--"):
                mode = None
            elif mode == "sel":
                sel.append(tok)
            elif mode == "exc":
                exc.append(tok)
        out[task] = {"select": sel, "exclude": exc}
    return out


def test_three_stage_selectors_cover_every_model() -> None:
    """The union of the three stages' --select (minus --exclude) must equal the
    full model inventory. A model left uncovered is an orphan that will go stale."""
    tags_by, anc = _build_graph()
    all_models = set(tags_by.keys())
    stages = _stage_selectors()

    covered: set[str] = set()
    for spec in stages.values():
        built = set()
        for s in spec["select"]:
            built |= _resolve_selector(s, all_models, tags_by, anc)
        for s in spec["exclude"]:
            built -= _resolve_selector(s, all_models, tags_by, anc)
        covered |= built

    orphans = sorted(all_models - covered)
    assert not orphans, (
        "These dbt models are NOT built by any of the three dbt_build stages "
        "(they will go schema-stale — see test docstring):\n  " + "\n  ".join(orphans)
    )
