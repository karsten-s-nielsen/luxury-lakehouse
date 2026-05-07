# Bundle Workflow Cards in Wheel -- Design Spec

**Goal:** Make `resolve_upstream_tables_from_card` resolve cards from the wheel install tree so the deployed wheel is self-contained. Eliminates the runtime dependency on `/Workspace/Repos/luxury-lakehouse/`.

**Architecture:** Force-include `workflow-cards/` in the wheel (same pattern as `docs/huggingface/` cards). Change the resolver to find cards relative to the package install path first, fall back to source-tree for local dev. Protect post-run `record_watermarks` calls from crashing the task if card resolution fails.

**Tech Stack:** hatchling force-include, Python pathlib, existing `guards.py` + `pyproject.toml`

---

## Context

PR #261 introduced watermark-based skip guards for 10 downstream tasks. Five HF publisher sub-operations and three dbt stages resolve their upstream table lists by reading workflow card YAML files via `resolve_upstream_tables_from_card()`.

This function defaults to `/Workspace/Repos/luxury-lakehouse/workflow-cards/` -- a Databricks Workspace Repos path that is:

1. **Not part of the automated deployment** -- CI builds and deploys the wheel; the Workspace Repo requires separate manual sync.
2. **Not forkable** -- any fork would need a Workspace Repo at that exact path.
3. **Version-decoupled** -- code in the wheel can reference cards that don't exist at that path if the Repo isn't synced.

The codebase already solved this exact problem for HuggingFace cards: `docs/huggingface/` is force-included in the wheel, and `get_hf_card_path()` uses dual-mode resolution (wheel path first, source-tree fallback).

## Root Cause (observed failure)

On the first daily job run after PR #261 merged (run 576801510890618), all 5 watermark-guarded `hf_sync` sub-operations failed with:

```
FileNotFoundError: [Errno 2] No such file or directory:
  '/Workspace/Repos/luxury-lakehouse/workflow-cards/wf-export-shots.yaml'
```

These errors were swallowed by `_run_sub_workflow`'s `except Exception` handler, so `hf_sync` reported SUCCESS despite all watermark sub-ops failing. The `workflow_watermarks` table was never created.

The 3 `dbt_build_*` tasks have the same latent bug: the guard wraps `resolve_upstream_tables_from_card` in `try/except FileNotFoundError` (fail open), but the post-build `record_watermarks` call at `dbt_runner.py:362` is bare -- it will crash the task after a successful dbt build.

## Affected Call Sites

| Caller | Guard (fail-open) | Post-run record |
|--------|-------------------|-----------------|
| `dbt_runner.py` (3 stages) | line 74: try/except FileNotFoundError | line 362: **bare -- crashes** |
| `hf_sync.py` (5 publishers) | line 109/129: inside `_run_sub_workflow` swallower | line 116/137: inside same swallower |
| `model_validation.py` | line 56: try/except FileNotFoundError | line 451: **bare -- crashes** |
| `refresh_synced_tables.py` | N/A (uses `_derive_upstream_tables`) | N/A (same) |

## Design

### 1. Wheel Packaging

Add to `pyproject.toml` `[tool.hatch.build.targets.wheel.force-include]`:

```toml
"workflow-cards" = "workflow_cards"
```

This places card YAML files at `<site-packages>/workflow_cards/` alongside `ingestion/`, `luxury_lakehouse_dbt_project/`, etc. The hyphen-to-underscore rename follows Python packaging convention (hyphens are not valid in importable package names).

### 2. Dual-Mode Resolver

Change `resolve_upstream_tables_from_card` default resolution (when `cards_dir is None`):

1. **Wheel path:** `Path(__file__).resolve().parent.parent / "workflow_cards"` -- `guards.py` lives at `<site-packages>/ingestion/guards.py`, so `parent.parent` reaches the site-packages root where `workflow_cards/` is force-included.
2. **Source-tree fallback:** `_repo_cards_dir()` (already exists) -- `Path(__file__).resolve().parent.parent.parent / "workflow-cards"` -- for local dev and pytest runs where the wheel is not installed.
3. **Explicit `cards_dir` parameter:** Retained for tests that pass a temp directory.

The hardcoded `/Workspace/Repos/luxury-lakehouse/workflow-cards` default is removed entirely.

### 3. Error Handling for Post-Run `record_watermarks`

Wrap the bare `resolve_upstream_tables_from_card` + `record_watermarks` calls in `dbt_runner.py:362` and `model_validation.py:451` with try/except that logs at ERROR level. The task succeeds; watermarks get recorded on the next run.

This is defense-in-depth: once cards are in the wheel, this path cannot fail under normal operation. But if someone runs `dbt_runner` locally without building the wheel and outside the source tree, the task should not crash after a successful dbt build.

### 4. Tests

- **Unit test:** `resolve_upstream_tables_from_card` resolves from wheel-install path when it exists.
- **Unit test:** Falls back to source-tree path when wheel path does not exist.
- Existing conformance tests in `test_guard_conformance.py` continue to pass -- they already use the source-tree path.

### 5. Not in Scope

- No changes to `refresh_synced_tables.py` (derives upstream from `SYNCED_TABLES`, not cards).
- No changes to workflow card YAML content.
- No ADR -- this is a bug fix applying an established codebase pattern (`get_hf_card_path`), not a new architectural decision.
