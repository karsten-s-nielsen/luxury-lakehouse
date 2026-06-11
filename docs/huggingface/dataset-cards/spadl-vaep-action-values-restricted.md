---
language: [en]
license: other
license_name: provider-restricted
license_details: Per-provider license terms; not redistributable. See Access below.
task_categories: [tabular-classification, tabular-regression]
tags: [sports-analytics, soccer, football, spadl, vaep, action-valuation, restricted]
configs:
  - config_name: default
    data_files:
      - split: train
        path: "data/*/data.parquet"
---

# SPADL/VAEP Action Values — Restricted Partitions

The **private companion** to [`luxury-lakehouse/spadl-vaep-action-values`](https://huggingface.co/datasets/luxury-lakehouse/spadl-vaep-action-values): identical schema and partition layout (`data/data_source=<provider>/data.parquet`), carrying only the partitions whose provider licenses do **not** permit public redistribution.

## Access

**Organization members only** (private repo). The data here is computed internally by the (Right! Luxury!) Lakehouse pipeline and held back from the public dataset until the relevant provider licenses permit publication. Do not mirror, re-export, or quote row-level data from this repo outside the organization.

## Why this repo exists

Model training (e.g. the VAEP Champion) uses the **full** corpus — public *and* restricted partitions — with per-repo commit hashes recorded in MLflow for lineage. Publishing the restricted slice here (rather than side-loading it from the warehouse) keeps the restricted data on the same publish → version → train path as everything else.

The split is governed by `RESTRICTED_HF_PROVIDERS` in the lakehouse's `ingestion.hf_publish` module (single source of truth for publishers and trainers; see ADR-049 in the lakehouse repo). When a provider grants full permission, removing it from that set migrates its partition to the public dataset on the next publish and sweeps it from this repo — the repo itself is permanent infrastructure and may legitimately be empty.

## Current contents

| Partition | Reason restricted |
|---|---|
| `data_source=gradientsports` | License for public redistribution not yet secured (policy 2026-06-10) |

## Schema

Identical to the public dataset — see its card for the full column reference, including the Kimball dual-column window (legacy `match_id`/`competition_id` sunset 2026-07-22).
