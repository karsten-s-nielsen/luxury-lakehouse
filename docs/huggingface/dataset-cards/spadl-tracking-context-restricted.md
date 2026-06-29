---
language: [en]
license: other
license_name: provider-restricted
license_details: Per-match license terms; not redistributable. See Access below.
task_categories:
  - tabular-classification
  - tabular-regression
tags:
  - football
  - soccer
  - tracking
  - spadl
  - action-context
  - restricted
size_categories:
  - 10K<n<100K
configs:
  - config_name: default
    data_files:
      - split: train
        path: "data/**/*.parquet"
---

# SPADL Tracking Context — Restricted Partitions

The **private companion** to [`luxury-lakehouse/spadl-tracking-context`](https://huggingface.co/datasets/luxury-lakehouse/spadl-tracking-context): identical schema and per-provider Hive layout (`data/data_source=<provider>/data.parquet`), carrying only the **per-match** rows whose license does **not** permit public redistribution.

```python
from datasets import load_dataset  # org-members only (private repo)

ds = load_dataset("luxury-lakehouse/spadl-tracking-context-restricted")
df = ds["train"].to_pandas()
```

## Access

**Organization members only** (private repo). The data here is computed internally by the (Right! Luxury!) Lakehouse pipeline and held back from the public dataset because the relevant per-match license does not permit publication. Do not mirror, re-export, or quote row-level data from this repo outside the organization.

## Why this repo exists

The redistribution boundary is **per-match**, driven by the ingestion-time `access_tier` (`public` | `restricted`) classified from each match's `visibility` signal. Public-tier rows go to the public dataset; restricted-tier rows (e.g. GradientSports, or any per-match-restricted provider) are published here. This migrates the publisher off the legacy `WHERE data_source != 'gradientsports'` SQL gate onto the uniform per-match split (spec D6). Both repos are published on every run and are permanent infrastructure — this repo may legitimately be empty.

The split is governed by `shared.access_tier` / `ingestion.hf_publish.split_restricted` (single source of truth for publishers and trainers; see ADR-049 in the lakehouse repo) and enforced on the public artifact by the fail-closed `ingestion.hf_leak_guard.assert_no_private_leak`.

## Schema

Identical to the public dataset — see [its card](https://huggingface.co/datasets/luxury-lakehouse/spadl-tracking-context) for the full column reference. The internal `access_tier` column is dropped before upload (it is constant per repo).
