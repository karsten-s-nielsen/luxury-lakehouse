---
language: [en]
license: other
license_name: provider-restricted
license_details: Per-provider license terms; not redistributable. See Access below.
task_categories: [tabular-classification, tabular-regression]
tags: [sports-analytics, soccer, football, spadl, expected-goals, xg, shots, freeze-frames, tracking, restricted]
# NOTE: the per-provider `configs:` block is injected at publish time from the providers
# actually present (ingestion.hf_publish.build_provider_configs / inject_frontmatter_configs).
---

# Pre-Shot xG v3 — Shot Freeze Frames (Restricted Partitions)

The **private companion** to [`luxury-lakehouse/xg-shot-freeze-frames`](https://huggingface.co/datasets/luxury-lakehouse/xg-shot-freeze-frames): identical schema and per-provider layout (`data/<provider>.parquet`, one HF config per provider), carrying only the providers whose licenses do **not** permit public redistribution. Each row keeps its `data_source` column.

```python
from datasets import load_dataset  # org-members only (private repo)

gs = load_dataset("luxury-lakehouse/xg-shot-freeze-frames-restricted", "gradientsports", split="train").to_pandas()
```

## Access

**Organization members only** (private repo). The partitions here are governed by per-provider license terms (currently **RM SkillCorner** and **GradientSports**) that forbid public redistribution. The schema and column semantics are identical to the public [`xg-shot-freeze-frames`](https://huggingface.co/datasets/luxury-lakehouse/xg-shot-freeze-frames) dataset — see that card for the full column reference (`match_key`, `action_id`, `data_source`, `player_id`, `x`, `y`, `is_keeper`, `is_teammate`, `set_cardinality`, `shooter_attacks_high_x`, `team_attacking_direction`; canonical SPADL 105×68, home-LTR; one row per (shot, player); shot identity `(match_key, action_id)`). Do not mirror, re-export, or quote row-level data from this repo outside the organization.

## Why this repo exists

The `xg_model_v3` training corpus reads **both** repos — public *and* restricted partitions — with per-repo commit hashes recorded for lineage. Publishing the restricted slice here (rather than side-loading it from the warehouse) keeps the restricted data on the same publish → version → consume path as everything else. This is the freeze-frame / player-set (context) half of the corpus; the tabular half lives in [`xg-shot-data-v3`](https://huggingface.co/datasets/luxury-lakehouse/xg-shot-data-v3) (public) + `xg-shot-data-v3-restricted`, joined on `(match_key, action_id)`.

The split is per-row (per-match `access_tier`), governed by `shared.access_tier.classify_access_tier` + `ingestion.hf_publish.split_restricted` (single source of truth for publishers and trainers; see [ADR-049](https://github.com/karsten-s-nielsen/luxury-lakehouse/blob/main/docs/superpowers/adrs/ADR-049-restricted-hf-dataset-companion-repos.md) / [ADR-064](https://github.com/karsten-s-nielsen/luxury-lakehouse/blob/main/docs/superpowers/adrs/ADR-064-per-match-access-tier.md)). When a match's license permits public redistribution, its rows migrate to the public dataset on the next publish and are swept from this repo — the repo itself is permanent infrastructure and may legitimately be empty.
