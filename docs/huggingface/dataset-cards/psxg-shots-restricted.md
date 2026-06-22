---
language: [en]
license: other
license_name: provider-restricted
license_details: Per-provider license terms; not redistributable. See Access below.
task_categories: [tabular-classification, tabular-regression]
tags: [sports-analytics, soccer, football, psxg, xgot, tracking, goalkeeping, restricted]
# NOTE: the per-provider `configs:` block is injected at publish time from the providers
# actually present (ingestion.hf_publish.build_provider_configs / inject_frontmatter_configs).
---

# Post-Shot Expected Goals (PSxG) — Restricted Partitions

The **private companion** to [`luxury-lakehouse/psxg-shots`](https://huggingface.co/datasets/luxury-lakehouse/psxg-shots): identical schema and per-provider layout (`data/<provider>.parquet`, one HF config per provider), carrying only the providers whose licenses do **not** permit public redistribution. Each row keeps its `data_source` column.

```python
from datasets import load_dataset  # org-members only (private repo)

gs = load_dataset("luxury-lakehouse/psxg-shots-restricted", "gradientsports", split="train")
```

## Access

This repo is private to `luxury-lakehouse` org members. The partitions here are governed by per-provider license terms (currently **GradientSports**) that forbid public redistribution. The schema, model, and column semantics are identical to the public [`psxg-shots`](https://huggingface.co/datasets/luxury-lakehouse/psxg-shots) dataset — see that card for the full column reference and the PSxG model description (ADR-060). A partition migrates from here to the public repo automatically once its license permits redistribution (lakehouse [ADR-049](https://github.com/karsten-s-nielsen/luxury-lakehouse/blob/main/docs/superpowers/adrs/ADR-049-restricted-hf-dataset-companion-repos.md)).
