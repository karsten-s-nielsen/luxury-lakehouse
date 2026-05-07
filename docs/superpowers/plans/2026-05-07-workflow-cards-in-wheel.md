# Workflow Cards in Wheel Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Base:** d32 worktree at commit `17fc375` (main, post-PR #261 watermark guards merge).

**Goal:** Bundle `workflow-cards/` in the wheel and fix `resolve_upstream_tables_from_card` to resolve cards from the install tree, eliminating the runtime dependency on `/Workspace/Repos/luxury-lakehouse/`.

**Architecture:** Add `workflow-cards/` to hatchling force-include (ships as `workflow_cards/` in the wheel). Change the resolver to check wheel-install path first, fall back to source-tree for local dev. Narrow-catch `FileNotFoundError` on bare `record_watermarks` post-run calls. Follow the `get_hf_card_path` dual-mode pattern already established in `ingestion.hf_publish` (package-level anchor via `ingestion.__file__`).

**Tech Stack:** hatchling (pyproject.toml), Python pathlib, pytest

---

## File Structure

| File | Action | Responsibility |
|------|--------|---------------|
| `pyproject.toml` | Modify (line ~307) | Add `workflow-cards` force-include entry |
| `src/ingestion/guards.py` | Modify (lines 514-561) | Dual-mode resolver + package-level anchor |
| `src/ingestion/dbt_runner.py` | Modify (lines 360-363) | Narrow-catch FileNotFoundError on post-build `record_watermarks` |
| `src/ingestion/model_validation.py` | Modify (lines 448-452) | Narrow-catch FileNotFoundError on post-validation `record_watermarks` |
| `src/ingestion/hf_sync.py` | Modify (lines 43-44, 150) | Replace hardcoded `_DEFAULT_CARDS_DIR` with `guards._default_cards_dir()` |
| `src/tests/test_watermark_freshness.py` | Modify (add tests) | Dual-mode resolver tests |

**Not touched:** `src/ingestion/refresh_synced_tables.py` — derives upstream tables from in-code `SYNCED_TABLES` list via `_derive_upstream_tables()`, never calls `resolve_upstream_tables_from_card`. Zero dependency on workflow card YAML files.

---

### Task 1: Add `workflow-cards` to wheel force-include

**Files:**
- Modify: `pyproject.toml:307`

- [ ] **Step 1: Add the force-include entry**

Insert after line 307 (`"docs/huggingface/org-card.md" = "docs/huggingface/org-card.md"`) in `pyproject.toml`:

```toml
# Workflow card YAML manifests so ``ingestion.guards.resolve_upstream_tables_from_card``
# resolves from the wheel install tree at runtime. Eliminates the runtime
# dependency on ``/Workspace/Repos/luxury-lakehouse/workflow-cards``.
# Hyphen-to-underscore rename follows Python packaging convention.
"workflow-cards" = "workflow_cards"
```

- [ ] **Step 2: Verify wheel builds and contains workflow cards**

Run: `uv build --wheel 2>&1 | tail -5`

Then verify cards are in the wheel:

Run: `python -c "import zipfile, glob; whl = glob.glob('dist/*.whl')[-1]; names = zipfile.ZipFile(whl).namelist(); cards = [n for n in names if n.startswith('workflow_cards/')]; print(f'{len(cards)} workflow card files in wheel'); assert len(cards) > 30, f'Expected 30+ cards, got {len(cards)}'"`

Expected: `3X workflow card files in wheel` (35+ YAML files)

---

### Task 2: Dual-mode resolver in `resolve_upstream_tables_from_card`

**Files:**
- Modify: `src/ingestion/guards.py:16` (add import) and `src/ingestion/guards.py:514-561` (replace resolver block)

- [ ] **Step 1: Write the failing test for wheel-path resolution**

Add to `src/tests/test_watermark_freshness.py` inside class `TestResolveUpstreamTablesFromCard`:

```python
def test_resolves_from_wheel_install_path(self, tmp_path: Path) -> None:
    """When wheel-install path exists, resolver uses it (not source-tree)."""
    from ingestion.guards import resolve_upstream_tables_from_card

    # Create a fake wheel-install layout: <site-packages>/workflow_cards/wf-test.yaml
    fake_site_packages = tmp_path / "site_packages"
    cards_in_wheel = fake_site_packages / "workflow_cards"
    cards_in_wheel.mkdir(parents=True)
    card = cards_in_wheel / "wf-test.yaml"
    card.write_text(
        "inputs:\n  datasets:\n    - id: '{catalog}.{schema}.my_table'\n      source: delta-table\n",
        encoding="utf-8",
    )

    # Monkeypatch the package-level anchor so the resolver thinks
    # ingestion/__init__.py lives at <site-packages>/ingestion/__init__.py.
    # This matches the hf_publish.py test pattern (_WHEEL_INGESTION_FILE).
    import ingestion.guards as guards_mod

    original = guards_mod._WHEEL_INGESTION_FILE
    try:
        guards_mod._WHEEL_INGESTION_FILE = fake_site_packages / "ingestion" / "__init__.py"
        result = resolve_upstream_tables_from_card("wf-test", "cat", "sch")
    finally:
        guards_mod._WHEEL_INGESTION_FILE = original

    assert result == ["cat.sch.my_table"]
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest src/tests/test_watermark_freshness.py::TestResolveUpstreamTablesFromCard::test_resolves_from_wheel_install_path -v`

Expected: FAIL with `AttributeError: module 'ingestion.guards' has no attribute '_WHEEL_INGESTION_FILE'`

- [ ] **Step 3: Write the failing test for source-tree fallback**

Add to `src/tests/test_watermark_freshness.py` inside class `TestResolveUpstreamTablesFromCard`:

```python
def test_falls_back_to_source_tree(self, tmp_path: Path) -> None:
    """When wheel-install path does not exist, resolver falls back to source-tree."""
    from ingestion.guards import resolve_upstream_tables_from_card

    # Point _WHEEL_INGESTION_FILE to a nonexistent location so wheel path misses
    import ingestion.guards as guards_mod

    original = guards_mod._WHEEL_INGESTION_FILE
    try:
        guards_mod._WHEEL_INGESTION_FILE = tmp_path / "nonexistent" / "ingestion" / "__init__.py"
        # Source-tree fallback should find the real workflow-cards/ at repo root
        result = resolve_upstream_tables_from_card(
            "wf-publish-spadl-vaep", "soccer_analytics", "dev_gold"
        )
    finally:
        guards_mod._WHEEL_INGESTION_FILE = original

    assert len(result) > 0
    assert all("soccer_analytics" in t for t in result)
```

- [ ] **Step 4: Run test to verify it fails**

Run: `uv run pytest src/tests/test_watermark_freshness.py::TestResolveUpstreamTablesFromCard::test_falls_back_to_source_tree -v`

Expected: FAIL (same `AttributeError`)

- [ ] **Step 5a: Add the package-level import at the top of guards.py**

Add `import ingestion as _ingestion` to the import block at the top of `src/ingestion/guards.py`, after line 16 (after `from typing import TYPE_CHECKING, Any, Protocol`). This must be at the top of the file to satisfy E402 ("module level import not at top of file").

```python
import ingestion as _ingestion
```

This matches the `hf_publish.py` pattern exactly (line 36: `import ingestion as _ingestion`). Underscore prefix convention signals "internal use only" — no `del` needed.

- [ ] **Step 5b: Implement the dual-mode resolver**

Replace lines 514-561 of `src/ingestion/guards.py` with:

```python
# Reference the installed ``ingestion`` package to anchor wheel-path
# resolution.  Uses ``_ingestion.__file__`` (package ``__init__.py``), NOT
# ``__file__`` (this module), so the anchor is resilient to guards.py
# moving within the package tree.  Same pattern as ``_WHEEL_INGESTION_FILE``
# in ``ingestion.hf_publish``.  Exposed as a module-level attribute so tests
# can monkeypatch it to simulate a site-packages layout.
# The ``import ingestion as _ingestion`` statement is at the top of this file.
_WHEEL_INGESTION_FILE: Path = Path(_ingestion.__file__).resolve()


def _default_cards_dir() -> Path:
    """Resolve workflow-cards using dual-mode resolution.

    Dual-mode so the same code works at runtime inside both a wheel install
    (Databricks workflow task) and a source-tree checkout (local dev, tests):

      1. **Wheel install**: the wheel force-includes ``workflow-cards/`` as
         ``workflow_cards/`` (sibling of the ``ingestion`` package).  Resolves
         via ``Path(ingestion.__file__).parent.parent / "workflow_cards"``.

      2. **Source-tree fallback**: when the wheel-side candidate does not
         exist, walks up from this module to the repo root and descends into
         ``workflow-cards/``.

    Follows the ``get_hf_card_path`` precedent in ``ingestion.hf_publish``.
    """
    # Wheel-first: site-packages layout where workflow_cards/ is a sibling of ingestion/.
    wheel_candidate = _WHEEL_INGESTION_FILE.parent.parent / "workflow_cards"
    if wheel_candidate.is_dir():
        return wheel_candidate

    # Dev fallback: walk up from this module to repo root.
    # src/ingestion/guards.py -> parents[2] = repo root.
    return Path(__file__).resolve().parents[2] / "workflow-cards"


def resolve_upstream_tables_from_card(
    workflow_id: str,
    catalog: str,
    schema: str,
    cards_dir: Path | None = None,
) -> list[str]:
    """Load upstream Delta table FQNs from a workflow card's inputs section.

    Reads ``inputs.tables`` and ``inputs.datasets`` entries where
    ``source == "delta-table"``, substitutes ``{catalog}`` and ``{schema}``
    placeholders in the ``id`` field, and returns the resolved list.

    ``cards_dir`` defaults to dual-mode resolution: wheel-install path first
    (``workflow_cards/`` force-included in the wheel), then source-tree
    fallback (``workflow-cards/`` at repo root).  Tests can pass an explicit
    ``cards_dir`` to override.
    """
    if cards_dir is None:
        cards_dir = _default_cards_dir()

    card_path = cards_dir / f"{workflow_id}.yaml"
    with open(card_path, encoding="utf-8") as f:
        import yaml

        # Workflow cards have YAML front matter delimited by ---
        content = f.read()
        # Split on --- and take the first YAML document
        parts = content.split("---")
        if len(parts) >= 3:
            card = yaml.safe_load(parts[1])
        else:
            card = yaml.safe_load(content)

    tables: list[str] = []
    inputs = card.get("inputs", {})
    for section in ("tables", "datasets"):
        for entry in inputs.get(section, []):
            if entry.get("source") == "delta-table":
                fqn = entry["id"].replace("{catalog}", catalog).replace("{schema}", schema)
                tables.append(fqn)
    return tables


def _repo_cards_dir() -> Path:
    """Resolve workflow-cards/ from the repo root (for local/test use)."""
    return Path(__file__).resolve().parent.parent.parent / "workflow-cards"
```

Note: `import ingestion as _ingestion` is at the top of the file (E402 compliant). The `_WHEEL_INGESTION_FILE` assignment at line ~514 references `_ingestion.__file__`. This is safe because `guards.py` is inside the `ingestion` package, so the self-import resolves without circularity. Same pattern as `hf_publish.py` lines 36 and 62.

- [ ] **Step 6: Run both new tests to verify they pass**

Run: `uv run pytest src/tests/test_watermark_freshness.py::TestResolveUpstreamTablesFromCard -v`

Expected: all tests PASS (including the 2 pre-existing ones)

- [ ] **Step 7: Run full test suite to verify nothing breaks**

Run: `uv run pytest src/tests/test_watermark_freshness.py src/tests/test_guard_conformance.py -v`

Expected: all PASS

---

### Task 3: Protect bare `record_watermarks` calls + fix `_DEFAULT_CARDS_DIR`

**Files:**
- Modify: `src/ingestion/dbt_runner.py:360-363`
- Modify: `src/ingestion/model_validation.py:448-452`
- Modify: `src/ingestion/hf_sync.py:43-44, 150`

**Design note on exception handling (ADR-002 compliance):**

The catch is narrowed to `FileNotFoundError` only — the specific failure mode where the card YAML isn't found (e.g. running dbt_runner locally outside both wheel and source tree). All other exceptions (Spark failures, Delta write errors in `record_watermarks`) propagate normally. This is ADR-002 compliant: specific exception class, not a broad catch.

The blast radius of a missed watermark is one redundant full run of downstream tasks (they re-process already-processed data because no stored watermark exists). This is the same behavior as the first-ever run — wasteful but not incorrect. With cards bundled in the wheel, this `FileNotFoundError` path is unreachable under normal operation; the catch exists only for the edge case of local CLI invocation outside the source tree.

- [ ] **Step 1: Wrap dbt_runner post-build record_watermarks**

Replace lines 360-363 of `src/ingestion/dbt_runner.py`:

```python
    # Record watermarks after successful dbt build
    if card_id is not None and spark is not None:
        upstream = resolve_upstream_tables_from_card(card_id, "soccer_analytics", "dev_gold")
        record_watermarks(spark, "soccer_analytics", card_id, upstream)
```

with:

```python
    # Record watermarks after successful dbt build.
    # FileNotFoundError catch: if card resolution fails (e.g. local CLI run
    # outside wheel and source tree), the dbt build already succeeded and
    # should not be marked as failed.  Next run re-processes (same as first
    # run).  All other exceptions (Spark, Delta) propagate — ADR-002.
    if card_id is not None and spark is not None:
        try:
            upstream = resolve_upstream_tables_from_card(card_id, "soccer_analytics", "dev_gold")
            record_watermarks(spark, "soccer_analytics", card_id, upstream)
        except FileNotFoundError:
            logger.error("Failed to record watermarks for %s — card file not found", card_id, exc_info=True)
```

- [ ] **Step 2: Wrap model_validation post-run record_watermarks**

Replace lines 448-452 of `src/ingestion/model_validation.py`:

```python
    # Record watermarks after successful validation
    from ingestion.guards import record_watermarks, resolve_upstream_tables_from_card

    upstream = resolve_upstream_tables_from_card(skip_guard.workflow_id, args.catalog, args.schema)
    record_watermarks(spark, args.catalog, skip_guard.workflow_id, upstream)
```

with:

```python
    # Record watermarks after successful validation.
    # FileNotFoundError catch: if card resolution fails (e.g. local CLI run
    # outside wheel and source tree), validation already succeeded and should
    # not be marked as failed.  Next run re-processes (same as first run).
    # All other exceptions (Spark, Delta) propagate — ADR-002.
    try:
        from ingestion.guards import record_watermarks, resolve_upstream_tables_from_card

        upstream = resolve_upstream_tables_from_card(skip_guard.workflow_id, args.catalog, args.schema)
        record_watermarks(spark, args.catalog, skip_guard.workflow_id, upstream)
    except FileNotFoundError:
        logger.error(
            "Failed to record watermarks for %s — card file not found",
            skip_guard.workflow_id,
            exc_info=True,
        )
```

- [ ] **Step 3: Replace `_DEFAULT_CARDS_DIR` in hf_sync.py**

`hf_sync.py:44` has `_DEFAULT_CARDS_DIR = Path("/Workspace/Repos/luxury-lakehouse/workflow-cards")` — the last hardcoded Workspace Repos reference. The only consumer is `_make_sync_costs_op` at line 150.

Delete lines 43-44 of `src/ingestion/hf_sync.py`:

```python
# Default workflow cards directory (matches the Databricks task parameter)
_DEFAULT_CARDS_DIR = Path("/Workspace/Repos/luxury-lakehouse/workflow-cards")
```

Delete both lines entirely — no tombstone comment. Git history has the provenance.

Then update `_make_sync_costs_op` at line 150. Replace:

```python
        run_pipeline(catalog, _DEFAULT_CARDS_DIR, filter_result=filter_result)
```

with:

```python
        from ingestion.guards import _default_cards_dir

        run_pipeline(catalog, _default_cards_dir(), filter_result=filter_result)
```

The import is inside the closure because `_make_sync_costs_op` returns a callable — the import happens at call time, not at module load time. `sync_hf_costs.run_pipeline` takes `cards_dir: Path` and only uses `cards_dir.glob("wf-*.yaml")` — no path-specific assumptions beyond "directory containing YAML files."

- [ ] **Step 4: Remove unused `Path` import if now orphaned in hf_sync.py**

Check whether `Path` is still used elsewhere in hf_sync.py. If `_DEFAULT_CARDS_DIR` was the only consumer, the `from pathlib import Path` import at the top of hf_sync.py may now be unused.

Run: `uv run ruff check src/ingestion/hf_sync.py`

If ruff reports F811 or F401 for `Path`, remove the unused import.

- [ ] **Step 5: Run lint to verify all changes**

Run: `uv run ruff check src/ingestion/dbt_runner.py src/ingestion/model_validation.py src/ingestion/hf_sync.py`

Expected: 0 violations

---

### Task 4: Final verification

**Files:**
- None (verification only)

- [ ] **Step 1: Run full lint + type check**

Run: `uv run ruff check src/ scripts/ && uv run ruff format --check src/ scripts/ && uv run pyright src/`

Expected: 0 violations

- [ ] **Step 2: Run full unit test suite**

Run: `uv run pytest src/tests/ -v --timeout=120`

Expected: all PASS

- [ ] **Step 3: Build wheel and verify cards are included**

Run: `uv build --wheel 2>&1 | tail -3`

Then: `python -c "import zipfile, glob; whl = glob.glob('dist/*.whl')[-1]; names = zipfile.ZipFile(whl).namelist(); cards = [n for n in names if n.startswith('workflow_cards/')]; print(f'{len(cards)} workflow card files'); assert len(cards) > 30"`

Expected: `3X workflow card files` (35+ YAML files in the wheel)
