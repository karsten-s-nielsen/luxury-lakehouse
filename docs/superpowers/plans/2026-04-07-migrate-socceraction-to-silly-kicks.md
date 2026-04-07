# Migrate socceraction to silly-kicks — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace the unmaintained `socceraction==1.5.3` dependency with `silly-kicks>=0.1.0,<1.0` (the actively-maintained successor), deleting workarounds that silly-kicks makes unnecessary.

**Architecture:** Import-path swap (`socceraction.*` → `silly_kicks.*`) plus three structural changes: (1) converters now return `(DataFrame, ConversionReport)` tuples, (2) `add_names()` preserves extra columns (deleting `match_id` re-injection workarounds), (3) guaranteed output dtypes (deleting `_clean_spadl_for_spark`). `_TYPE_KEY_OVERRIDES` in `statsbomb.py` is **kept** — it fixes raw JSON key extraction, not a library bug (silly-kicks' StatsBomb converter still reads `extra["goalkeeper"]`).

**Tech Stack:** silly-kicks 0.1.0 (pip install from git tag v0.1.0), pandas, numpy, xgboost, PySpark `applyInPandas`

**Install:** `pip install git+https://github.com/karsten-s-nielsen/silly-kicks.git@v0.1.0`

---

## File Map

| File | Action | Responsibility |
|------|--------|----------------|
| `pyproject.toml` | Modify | Swap dependency, remove `multimethod` pin |
| `src/ingestion/spadl_conversion.py` | Modify | Import swap, tuple return, delete `_clean_spadl_for_spark`, simplify UDFs |
| `src/ingestion/spadl_vaep.py` | Modify | Import swap, delete `match_id` re-injection workaround |
| `src/ingestion/vaep_training.py` | Modify | Import swap |
| `src/ingestion/statsbomb.py` | Modify | Update `_TYPE_KEY_OVERRIDES` comment (keep function) |
| `src/ingestion/spadl_adapter.py` | Modify | Update docstrings |
| `scripts/train_vaep_model_hf.py` | Modify | Import swap, PEP 723 dep swap |
| `terraform/modules/workflows/main.tf` | Modify | Swap analytics env dep |
| `src/tests/test_spadl_vaep.py` | Modify | Delete `TestCleanSpadlForSpark`, update imports |
| `src/tests/test_spadl_adapter.py` | Modify | Update docstring |
| `ARCHITECTURE.md` | Modify | Text updates (2 lines) |
| `NOTICE` | Modify | Update third-party library entry |
| `hf_taipy_app/src/pages/action_values.py` | Modify | Update description + citation |
| `TODO.md` | Modify | Replace D44 |
| `docs/` (multiple) | Modify | Text references |

---

### Task 1: Dependency Swap in pyproject.toml

**Files:**
- Modify: `pyproject.toml:22-38`

- [ ] **Step 1: Swap socceraction for silly-kicks and remove multimethod**

In `pyproject.toml`, replace the analytics dependency block:

```python
# BEFORE (lines 29-34):
    # DEPENDENCY CHAIN: socceraction -> pandera 0.17.2 -> multimethod <2.0
    # multimethod 2.0 removed the 'overload' API that pandera uses.
    # Safe to relax when pandera >= 0.18 drops multimethod dependency.
    "socceraction==1.5.3",
    "xgboost==3.2.0",
    "multimethod==1.12",

# AFTER:
    "silly-kicks>=0.1.0,<1.0",
    "xgboost==3.2.0",
```

The `multimethod` pin existed solely for socceraction's transitive `pandera` dependency. silly-kicks has neither.

- [ ] **Step 2: Regenerate lock file**

```bash
uv lock
```

Expected: resolves `silly-kicks` from PyPI/git, drops `socceraction`, `pandera`, `multimethod` from the lock.

- [ ] **Step 3: Sync environment**

```bash
uv sync --extra analytics
```

Expected: installs `silly-kicks`, uninstalls `socceraction` + `pandera` + `multimethod`.

- [ ] **Step 4: Verify silly-kicks is importable**

```bash
uv run python -c "import silly_kicks; print(silly_kicks.__version__)"
```

Expected: `0.1.0`

---

### Task 2: SPADL Conversion — Import Swap + Workaround Deletion

**Files:**
- Modify: `src/ingestion/spadl_conversion.py`

- [ ] **Step 1: Update module docstring**

```python
# BEFORE (line 1):
"""SPADL conversion from bronze event tables.

Reads events from existing bronze Delta tables (``statsbomb_events``,
``wyscout_events``) and converts them into SPADL unified format via
socceraction.  Each data source has a dedicated UDF factory (for

# AFTER:
"""SPADL conversion from bronze event tables.

Reads events from existing bronze Delta tables (``statsbomb_events``,
``wyscout_events``) and converts them into SPADL unified format via
silly-kicks.  Each data source has a dedicated UDF factory (for
```

- [ ] **Step 2: Delete `_clean_spadl_for_spark` function**

Delete lines 44–92 entirely (the function definition and its docstring). silly-kicks guarantees output dtypes via `_finalize_output()`. This function was never called in the pipeline — only in tests.

- [ ] **Step 3: Rewrite `_make_sb_spadl_udf` to use silly-kicks**

Replace the entire UDF closure (lines 122–193). Key changes:
1. Import `silly_kicks.spadl.statsbomb` instead of `socceraction.spadl.statsbomb`
2. Handle `(actions, _report)` tuple return from `convert_to_actions`
3. Remove `_spadl_cols` allowlist — silly-kicks guarantees output columns
4. Remove column guards and string coercion — silly-kicks guarantees dtypes
5. Keep metadata injection (match_id, competition_id, season_id, data_source)

```python
def _make_sb_spadl_udf() -> object:
    """Build the ``applyInPandas`` UDF closure for StatsBomb SPADL conversion.

    All library imports happen inside the closure so they are available
    on Spark executors without requiring module-level serialisation.

    Returns:
        A callable ``(pd.DataFrame) -> pd.DataFrame`` suitable for
        ``applyInPandas``.
    """

    def _udf(pdf: pd.DataFrame) -> pd.DataFrame:
        """Convert one game's StatsBomb events to SPADL actions."""
        import pandas as _pd

        from ingestion.spadl_adapter import adapt_statsbomb_events as _adapt

        _output_cols = _pd.Index(
            [
                "game_id",
                "match_id",
                "original_event_id",
                "period_id",
                "time_seconds",
                "team_id",
                "player_id",
                "start_x",
                "start_y",
                "end_x",
                "end_y",
                "type_id",
                "result_id",
                "bodypart_id",
                "competition_id",
                "season_id",
                "data_source",
            ]
        )

        if pdf.empty:
            return _pd.DataFrame(columns=_output_cols)

        import silly_kicks.spadl.statsbomb as _spadl_sb

        home_team_id = int(pdf["home_team_id"].iloc[0])
        match_id = int(pdf["match_id"].iloc[0])
        competition_id = int(pdf["competition_id"].iloc[0])
        season_id = int(pdf["season_id"].iloc[0])

        try:
            adapted = _adapt(pdf, home_team_id)
            actions, _report = _spadl_sb.convert_to_actions(adapted, home_team_id)
        except Exception:
            return _pd.DataFrame(columns=_output_cols)

        actions["match_id"] = match_id
        actions["competition_id"] = competition_id
        actions["season_id"] = season_id
        actions["data_source"] = "statsbomb"

        # Project to output schema (silly-kicks guarantees SPADL column dtypes)
        for col in _output_cols:
            if col not in actions.columns:
                actions[col] = "" if col == "data_source" else 0
        return _pd.DataFrame(actions[_output_cols])

    return _udf
```

- [ ] **Step 4: Rewrite `_make_ws_spadl_udf` to use silly-kicks**

Same pattern as StatsBomb. Replace lines 319–391:

```python
def _make_ws_spadl_udf() -> object:
    """Build the ``applyInPandas`` UDF closure for Wyscout SPADL conversion.

    All library imports happen inside the closure so they are available
    on Spark executors without requiring module-level serialisation.

    Returns:
        A callable ``(pd.DataFrame) -> pd.DataFrame`` suitable for
        ``applyInPandas``.
    """

    def _udf(pdf: pd.DataFrame) -> pd.DataFrame:
        """Convert one game's Wyscout events to SPADL actions."""
        import pandas as _pd

        from ingestion.spadl_adapter import adapt_wyscout_events as _adapt

        _output_cols = _pd.Index(
            [
                "game_id",
                "match_id",
                "original_event_id",
                "period_id",
                "time_seconds",
                "team_id",
                "player_id",
                "start_x",
                "start_y",
                "end_x",
                "end_y",
                "type_id",
                "result_id",
                "bodypart_id",
                "competition_id",
                "season_id",
                "data_source",
            ]
        )

        if pdf.empty:
            return _pd.DataFrame(columns=_output_cols)

        import silly_kicks.spadl.wyscout as _spadl_ws

        home_team_id = int(pdf["home_team_id"].iloc[0])
        # Wyscout uses matchId or match_id depending on ingestion format
        match_id = int(pdf["matchId"].iloc[0]) if "matchId" in pdf.columns else int(pdf["match_id"].iloc[0])
        competition_id = int(pdf["competition_id"].iloc[0])
        season_id = int(pdf["season_id"].iloc[0])

        try:
            adapted = _adapt(pdf)
            actions, _report = _spadl_ws.convert_to_actions(adapted, home_team_id)
        except Exception:
            return _pd.DataFrame(columns=_output_cols)

        actions["match_id"] = match_id
        actions["competition_id"] = competition_id
        actions["season_id"] = season_id
        actions["data_source"] = "wyscout"

        # Project to output schema (silly-kicks guarantees SPADL column dtypes)
        for col in _output_cols:
            if col not in actions.columns:
                actions[col] = "" if col == "data_source" else 0
        return _pd.DataFrame(actions[_output_cols])

    return _udf
```

- [ ] **Step 5: Run tests**

```bash
uv run pytest src/tests/test_spadl_adapter.py src/tests/test_spadl_vaep.py -v
```

Expected: `TestCleanSpadlForSpark` fails (function deleted — will fix in Task 8). All other tests pass.

---

### Task 3: VAEP Scoring — Import Swap + Delete match_id Workaround

**Files:**
- Modify: `src/ingestion/spadl_vaep.py`

- [ ] **Step 1: Swap module-level import**

```python
# BEFORE (line 26):
import socceraction.vaep.features as fs

# AFTER:
import silly_kicks.vaep.features as fs
```

- [ ] **Step 2: Swap imports inside `_make_scoring_udf` closure**

```python
# BEFORE (lines 187-189):
        import socceraction.spadl as _spadl
        import socceraction.vaep.features as _fs
        import socceraction.vaep.formula as _vaepformula

# AFTER:
        import silly_kicks.spadl as _spadl
        import silly_kicks.vaep.features as _fs
        import silly_kicks.vaep.formula as _vaepformula
```

- [ ] **Step 3: Delete `match_id` re-injection workaround**

Delete lines 277-279 from the scoring UDF:

```python
# DELETE THIS:
                # match_id may not survive add_names(); fall back to pdf
                if "match_id" not in game_out.columns:
                    game_out["match_id"] = pdf["match_id"].iloc[0]
```

silly-kicks' `add_names()` preserves all extra columns including `match_id`.

- [ ] **Step 4: Run tests**

```bash
uv run pytest src/tests/test_spadl_vaep.py -v -k "not TestCleanSpadlForSpark"
```

Expected: all non-deleted tests pass.

---

### Task 4: VAEP Training — Import Swap

**Files:**
- Modify: `src/ingestion/vaep_training.py`

- [ ] **Step 1: Swap imports**

```python
# BEFORE (lines 22-24):
import socceraction.spadl as spadl
import socceraction.vaep.features as fs
import socceraction.vaep.labels as labels

# AFTER:
import silly_kicks.spadl as spadl
import silly_kicks.vaep.features as fs
import silly_kicks.vaep.labels as labels
```

- [ ] **Step 2: Update docstring reference**

```python
# BEFORE (line 6):
        the standard socceraction columns.

# AFTER:
        the standard SPADL columns.
```

(This is in the `extract_features_for_games` docstring, line 48.)

---

### Task 5: StatsBomb — Update Comments (Keep _TYPE_KEY_OVERRIDES)

**Files:**
- Modify: `src/ingestion/statsbomb.py:48-67`

- [ ] **Step 1: Update `_TYPE_KEY_OVERRIDES` comment**

```python
# BEFORE (lines 49-54):
_TYPE_KEY_OVERRIDES: dict[str, str] = {
    # StatsBomb JSON uses "goalkeeper" for Goal Keeper events, not "goal_keeper".
    # Without this override, the goalkeeper sub-dict (containing claim/punch/save
    # type info) is silently dropped, and keeper_claim actions are never generated.
    "goal_keeper": "goalkeeper",
}

# AFTER:
_TYPE_KEY_OVERRIDES: dict[str, str] = {
    # StatsBomb JSON nests Goal Keeper payloads under "goalkeeper", but our
    # snake_case normalisation produces "goal_keeper".  This mapping ensures
    # the extra dict uses the key the SPADL converter actually reads.
    "goal_keeper": "goalkeeper",
}
```

- [ ] **Step 2: Update `_build_raw_extra_json` docstring**

```python
# BEFORE (line 58-67):
    """Fetch raw StatsBomb JSON and extract type-specific 'extra' dicts.

    socceraction's SPADL converter needs an ``extra`` dict containing

# AFTER:
    """Fetch raw StatsBomb JSON and extract type-specific 'extra' dicts.

    The SPADL converter needs an ``extra`` dict containing
```

---

### Task 6: SPADL Adapter — Update Docstrings

**Files:**
- Modify: `src/ingestion/spadl_adapter.py`

- [ ] **Step 1: Update module docstring**

```python
# BEFORE (line 1):
"""Adapters to transform bronze event tables into socceraction-compatible DataFrames.

# AFTER:
"""Adapters to transform bronze event tables into SPADL-converter-compatible DataFrames.
```

- [ ] **Step 2: Update `adapt_statsbomb_events` docstring**

```python
# BEFORE (line 47):
        Adapted DataFrame ready for ``socceraction.spadl.statsbomb.convert_to_actions``.

# AFTER:
        Adapted DataFrame ready for ``silly_kicks.spadl.statsbomb.convert_to_actions``.
```

- [ ] **Step 3: Update `adapt_wyscout_events` docstring**

```python
# BEFORE (line 129):
        Adapted DataFrame ready for ``socceraction.spadl.wyscout.convert_to_actions``.

# AFTER:
        Adapted DataFrame ready for ``silly_kicks.spadl.wyscout.convert_to_actions``.
```

---

### Task 7: HF Jobs Training Script — Import + Dep Swap

**Files:**
- Modify: `scripts/train_vaep_model_hf.py`

- [ ] **Step 1: Swap PEP 723 dependencies**

```python
# BEFORE (lines 10-11):
#     "socceraction==1.5.3",
#     "multimethod==1.12",

# AFTER:
#     "silly-kicks>=0.1.0,<1.0",
```

- [ ] **Step 2: Update module docstring**

```python
# BEFORE (line 18-19):
Downloads SPADL action data from HF Hub, extracts features via
socceraction, trains two XGBClassifier models (P(scoring) and

# AFTER:
Downloads SPADL action data from HF Hub, extracts features via
silly-kicks, trains two XGBClassifier models (P(scoring) and
```

- [ ] **Step 3: Swap imports**

```python
# BEFORE (lines 49-52):
import socceraction.spadl as spadl
import socceraction.spadl.config as spadlcfg
import socceraction.vaep.features as fs
import socceraction.vaep.labels as labels

# AFTER:
import silly_kicks.spadl as spadl
import silly_kicks.spadl.config as spadlcfg
import silly_kicks.vaep.features as fs
import silly_kicks.vaep.labels as labels
```

- [ ] **Step 4: Update `_convert_hf_to_spadl` docstring**

```python
# BEFORE (line 109):
    """Convert HF dataset columns to socceraction SPADL format.

    The HF dataset has string-typed columns (action_type, action_result,
    bodypart) and uses different column names (match_id, period) than
    socceraction expects (game_id, period_id, type_id, result_id,

# AFTER:
    """Convert HF dataset columns to SPADL format.

    The HF dataset has string-typed columns (action_type, action_result,
    bodypart) and uses different column names (match_id, period) than
    SPADL expects (game_id, period_id, type_id, result_id,
```

- [ ] **Step 5: Update column mapping comment**

```python
# BEFORE (line 99):
# Column mapping: HF dataset -> socceraction SPADL format

# AFTER:
# Column mapping: HF dataset -> SPADL format
```

---

### Task 8: Tests — Delete Obsolete Tests + Update Docstrings

**Files:**
- Modify: `src/tests/test_spadl_vaep.py`
- Modify: `src/tests/test_spadl_adapter.py`

- [ ] **Step 1: Update test_spadl_vaep.py imports**

```python
# BEFORE (line 12):
from ingestion.spadl_conversion import _clean_spadl_for_spark, _read_existing_match_ids

# AFTER:
from ingestion.spadl_conversion import _read_existing_match_ids
```

- [ ] **Step 2: Delete `TestCleanSpadlForSpark` class**

Delete lines 19–82 entirely (the class and all 5 test methods). The function it tests no longer exists.

- [ ] **Step 3: Update test_spadl_adapter.py docstring**

```python
# BEFORE (line 1):
"""Tests for ingestion.spadl_adapter — bronze-to-socceraction format mapping."""

# AFTER:
"""Tests for ingestion.spadl_adapter — bronze-to-SPADL-converter format mapping."""
```

- [ ] **Step 4: Run full test suite**

```bash
uv run pytest src/tests/test_spadl_adapter.py src/tests/test_spadl_vaep.py -v
```

Expected: all tests pass.

- [ ] **Step 5: Run linting and type checks**

```bash
uv run ruff check src/ scripts/
uv run ruff format --check src/ scripts/
uv run pyright src/
```

Expected: zero violations.

---

### Task 9: Terraform — Swap Analytics Environment Dependency

**Files:**
- Modify: `terraform/modules/workflows/main.tf:884-906`

- [ ] **Step 1: Swap dependency in analytics environment**

```hcl
# BEFORE (lines 892-896):
      dependencies = [
        var.wheel_path,
        "socceraction==1.5.3",
        "xgboost==3.2.0",
        "multimethod==1.12",

# AFTER:
      dependencies = [
        var.wheel_path,
        "silly-kicks>=0.1.0,<1.0",
        "xgboost==3.2.0",
```

- [ ] **Step 2: Update comment about socceraction on line 164**

```hcl
# BEFORE (lines 163-165):
  # Ensures the goalkeeper sub-dict (and other type-specific extras) are
  # present in statsbomb_events._raw_extra_json. Without this, socceraction
  # cannot distinguish keeper_claim/keeper_punch/keeper_save sub-types.

# AFTER:
  # Ensures the goalkeeper sub-dict (and other type-specific extras) are
  # present in statsbomb_events._raw_extra_json. Without this, the SPADL
  # converter cannot distinguish keeper_claim/keeper_punch/keeper_save sub-types.
```

- [ ] **Step 3: Validate Terraform**

```bash
cd terraform/environments/dev && terraform validate
```

Expected: `Success! The configuration is valid.`

---

### Task 10: Documentation — Update All References

**Files:**
- Modify: `ARCHITECTURE.md` (2 lines)
- Modify: `NOTICE` (3 lines)
- Modify: `hf_taipy_app/src/pages/action_values.py` (2 lines)
- Modify: `docs/huggingface/model-cards/vaep-model.md` (multiple)
- Modify: `docs/huggingface/dataset-cards/spadl-vaep.md` (multiple)

- [ ] **Step 1: Update ARCHITECTURE.md**

Line 265:
```
# BEFORE:
│   Technology: Python + statsbombpy + requests + socceraction
# AFTER:
│   Technology: Python + statsbombpy + requests + silly-kicks
```

Line 472:
```
# BEFORE:
│   │   ├── spadl_adapter.py          # Bronze-to-socceraction format adapters
# AFTER:
│   │   ├── spadl_adapter.py          # Bronze-to-SPADL-converter format adapters
```

- [ ] **Step 2: Update NOTICE**

```
# BEFORE (lines 41-43):
socceraction — SPADL conversion and VAEP action valuation (MIT License).
Copyright (c) DTAI Research KU Leuven.
See: https://github.com/ML-KULeuven/socceraction

# AFTER:
silly-kicks — SPADL conversion and VAEP action valuation (MIT License).
Successor to socceraction. Copyright (c) Karsten S. Nielsen.
Original socceraction: Copyright (c) DTAI Research KU Leuven.
See: https://github.com/karsten-s-nielsen/silly-kicks
```

Line 62-63:
```
# BEFORE:
socceraction library (see Third-Party Libraries above).

# AFTER:
silly-kicks library (see Third-Party Libraries above).
```

- [ ] **Step 3: Update action_values.py**

```python
# BEFORE (line 21):
        "Valuing Actions by Estimating Probabilities (VAEP) — Decroos et al. (2019). Implemented via socceraction."

# AFTER:
        "Valuing Actions by Estimating Probabilities (VAEP) — Decroos et al. (2019). Implemented via silly-kicks."
```

```python
# BEFORE (line 26):
        Citation("socceraction", "https://github.com/ML-KULeuven/socceraction"),

# AFTER:
        Citation("silly-kicks", "https://github.com/karsten-s-nielsen/silly-kicks"),
```

- [ ] **Step 4: Update HF model card (vaep-model.md)**

Replace all `socceraction` import examples with `silly_kicks` equivalents. Update the dependency tag from `socceraction` to `silly-kicks`. Update the BibTeX citation block to include both the original paper and silly-kicks.

Key changes:
- Line 14: `- socceraction` → `- silly-kicks`
- Line 62: `[socceraction](https://pypi.org/project/socceraction/) library (v1.5.3)` → `[silly-kicks](https://github.com/karsten-s-nielsen/silly-kicks) library`
- Lines 158-160: swap all `import socceraction.*` → `import silly_kicks.*`
- Lines 225-229: update citation URL

- [ ] **Step 5: Update HF dataset card (spadl-vaep.md)**

- Line 16: `[socceraction](https://pypi.org/project/socceraction/)` → `[silly-kicks](https://github.com/karsten-s-nielsen/silly-kicks)`
- Lines 113-117: update citation URL

- [ ] **Step 6: Update remaining docs (best-effort text search)**

Update references in these files (comments/descriptions only, no code changes):
- `docs/decisions/pep723-hf-jobs.md`
- `docs/research/adversarial-training.md`
- `docs/superpowers/plans/` (historical plans — update only active references)

Note: Historical plan files that describe past work can keep their original text. Only update files that serve as active documentation.

---

### Task 11: TODO.md — Replace D44

**Files:**
- Modify: `TODO.md:29`

- [ ] **Step 1: Replace D44 entry**

```
# BEFORE:
| D44 | socceraction PR — Wyscout `keeper_claim` mapping | Dunkin' | GK data quality investigation (2026-04-05) | Upstream fix: ...

# AFTER:
Delete D44 entirely — the task is resolved by migrating to silly-kicks, which fixes keeper_claim across all providers.
```

---

### Task 12: Final Verification

- [ ] **Step 1: Run full lint + type check + test suite**

```bash
uv run ruff check src/ scripts/
uv run ruff format --check src/ scripts/
uv run pyright src/
uv run pytest src/tests/ -v
```

Expected: zero violations, all tests pass.

- [ ] **Step 2: Verify no remaining socceraction references in code**

```bash
grep -r "socceraction" src/ scripts/ --include="*.py" | grep -v "# " | grep -v '"""' | grep -v "docstring"
```

Expected: zero hits in import statements or function calls. Comment/docstring references in historical files are acceptable.

- [ ] **Step 3: Verify no remaining socceraction imports**

```bash
grep -rn "import socceraction" src/ scripts/
```

Expected: zero hits.
