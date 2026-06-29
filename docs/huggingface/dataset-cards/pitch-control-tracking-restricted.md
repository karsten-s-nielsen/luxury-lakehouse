---
language: [en]
license: other
license_name: provider-restricted
license_details: Per-match license terms; not redistributable. See Access below.
task_categories: [tabular-regression]
tags: [sports-analytics, soccer, football, pitch-control, tracking-data, restricted]
size_categories: [10M-100M]
configs:
  - config_name: default
    data_files:
      - split: train
        path: "data/**/*.parquet"
---

# Pitch Control Tracking Data — Restricted Partitions

The **private companion** to [`luxury-lakehouse/pitch-control-tracking`](https://huggingface.co/datasets/luxury-lakehouse/pitch-control-tracking): identical schema and per-provider Hive layout (`data/source_provider=<provider>/data.parquet`), carrying only the **per-match** rows whose license does **not** permit public redistribution.

```python
from datasets import load_dataset  # org-members only (private repo)

ds = load_dataset("luxury-lakehouse/pitch-control-tracking-restricted")
df = ds["train"].to_pandas()
```

## Access

**Organization members only** (private repo). The data here is computed internally by the (Right! Luxury!) Lakehouse pipeline and held back from the public dataset because the relevant per-match license does not permit publication. Do not mirror, re-export, or quote row-level data from this repo outside the organization.

## Why this repo exists

The redistribution boundary is **per-match**, driven by the ingestion-time `access_tier` (`public` | `restricted`) classified from each match's `visibility` signal. Public-tier frames go to the public dataset; restricted-tier frames are published here. Both repos are published on every run and are permanent infrastructure — this repo may legitimately be empty (e.g. when no restricted tracking matches have been ingested yet).

The split is governed by `shared.access_tier` / `ingestion.hf_publish.split_restricted` (single source of truth for publishers and trainers; see ADR-049 in the lakehouse repo) and enforced on the public artifact by the fail-closed `ingestion.hf_leak_guard.assert_no_private_leak`.

## Schema

Identical to the public dataset — see [its card](https://huggingface.co/datasets/luxury-lakehouse/pitch-control-tracking) for the full column reference. The internal `access_tier` column is dropped before upload (it is constant per repo).
