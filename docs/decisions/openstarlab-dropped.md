# Decision: OpenSTARLab Event Prediction Dropped (D5, D15)

**Date:** 2026-03-13
**Status:** Final
**Decision:** Drop all OpenSTARLab integration (LEM_3, Seq2Event, FMS)

## Context

D5 planned to use the `openstarlab-event` PyPI package (Apache 2.0) to train and run LEM_3 (Large Events Model) on our StatsBomb + Wyscout event data. D15 would have extended this to Seq2Event and FMS models.

## Problem

The `openstarlab-event` package has a hardcoded internal data format called **UEID** that is fundamentally incompatible with our multi-league SPADL data:

1. **22 required columns** — The package's `UEID_preprocessing` function expects exactly 22 columns including `team`, `goal`, `seconds`, `deltaX`, `deltaY`, `distance`, `dist2goal`, `angle2goal`, `home_score`, `away_score`, and more. These are not optional.

2. **Hardcoded La Liga team dictionary** — Team names are mapped via a literal Python dictionary of La Liga club names. Non-La Liga teams (which are all of our data) have no mapping path.

3. **Only 8 action types** — The UEID vocabulary supports only 8 fixed action types vs. our unified 33-type vocabulary across StatsBomb + Wyscout.

4. **No public data format documentation** — The UEID format is undocumented. We discovered these requirements only by reading the package source code and debugging runtime errors (`KeyError: 'team'`, `KeyError: 'eps'`).

5. **No adapter/plugin architecture** — The package provides no hooks, callbacks, or configuration to override the data preprocessing pipeline. The only path would be forking the entire package.

## Alternatives Considered

| Option | Assessment |
|--------|------------|
| Map our data to UEID format | Impossible — hardcoded La Liga team dict, 8-action vocabulary too restrictive |
| Fork `openstarlab-event` | High maintenance burden for a research package with sparse docs |
| Write our own LEM from scratch | Out of scope — we'd be reimplementing the paper, not using the library |
| Use a different event prediction library | No mature alternatives exist with Apache 2.0 licensing |

## What Was Built and Removed

- `src/analytics/openstarlab.py` — preprocessing + inference wrapper
- `src/ingestion/openstarlab.py` — Databricks batch pipeline
- `src/tests/test_openstarlab.py` — 21 unit tests
- `src/streamlit_app/pages/event_prediction.py` — Streamlit page
- `scripts/train_openstarlab_hf.py` — HF Jobs training script
- `scripts/inference_openstarlab_hf.py` — HF Jobs inference script
- `notebooks/train_openstarlab.py` — Databricks training notebook
- `notebooks/export_training_data.py` — HF dataset export notebook
- `docs/huggingface/openstarlab-model-card.md` — HF Hub model card
- `dbt_project/models/marts/fct_event_predictions.sql` — gold mart table
- `dbt_project/models/staging/openstarlab/` — staging view + source
- Terraform task + environment in `workflows/main.tf`
- `openstarlab` optional dependency group in `pyproject.toml`
- `openstarlab_enabled` feature toggle in `dbt_project.yml`
- `compute_openstarlab` entry point in `pyproject.toml`
- HF Hub dataset `luxury-lakehouse/openstarlab-training-events` (deleted)

## Impact

- Streamlit app drops from 12 pages to 11 (Event Prediction page removed)
- No downstream dependencies existed — the feature toggle was never flipped to `true`
- No bronze/silver/gold tables were ever populated
- The HF Hub model repo `luxury-lakehouse/openstarlab-lem3-statsbomb-wyscout` was never created

## Future Event Prediction

If event prediction is revisited, consider:
- A custom transformer-based model trained directly on our SPADL data (no external package dependency)
- Libraries that accept configurable action vocabularies and coordinate systems
- Verifying data format compatibility **before** building infrastructure
