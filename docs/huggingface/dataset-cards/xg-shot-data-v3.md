---
license: cc-by-nc-4.0
task_categories:
  - tabular-classification
  - tabular-regression
tags:
  - soccer
  - football
  - analytics
  - spadl
  - expected-goals
  - xg
  - shots
size_categories:
  - 100K<n<1M
# NOTE: the per-provider `configs:` block is injected at publish time from the providers
# actually present (ingestion.hf_publish.build_provider_configs / inject_frontmatter_configs),
# so it never drifts from the data. See the Quick Start for the resulting configs.
---

# Pre-Shot xG v3 — Shot Data (Tabular Corpus)

The tabular half of the training corpus for `xg_model_v3`, the canonical-SPADL-native **pre-shot expected goals** model from the [luxury-lakehouse](https://github.com/karsten-s-nielsen/luxury-lakehouse) analytics platform. **One row per shot, all providers** — the provider is the `data_source` column, not a separate file format. Sourced from the gold `fct_action_values` fact.

Each shot is joinable to its freeze-frame player set (dataset [`xg-shot-freeze-frames`](https://huggingface.co/datasets/luxury-lakehouse/xg-shot-freeze-frames)) and to the full action-level corpus (`spadl-vaep-action-values`) on the shot identity **`(match_key, action_id)`** — `action_id` is per-match, NOT globally unique, so both keys are always required.

## Shot family

Rows cover `action_type ∈ {shot, shot_freekick, shot_penalty}`. Penalties are **included** here so the downstream xG scorer's constant-penalty path has rows; the `xg_model_v3` trainer excludes `shot_penalty` from the model itself and assigns it a constant penalty-xG at scoring time. The goal label is `action_result == 'success'`.

## Columns / contract

| Column | Type | Meaning |
|--------|------|---------|
| `match_key` | BIGINT | Kimball match surrogate — half of the shot identity |
| `action_id` | BIGINT | Per-match SPADL action id — the other half of the shot identity |
| `action_type` | STRING | `shot`, `shot_freekick`, or `shot_penalty` |
| `action_result` | STRING | Outcome; `success` = goal (the training label) |
| `start_x` | DOUBLE | Shot origin x, canonical SPADL 105×68 (goal at x=105) |
| `start_y` | DOUBLE | Shot origin y, canonical SPADL 105×68 |
| `data_source` | STRING | Provider (`statsbomb`, `wyscout`, `skillcorner`, `idsse`, `metrica`, ...) |

Coordinates are **canonical SPADL 105×68, home-LTR** — no provider is bent to StatsBomb units. (`access_tier` is used internally for the public/restricted split and is dropped before upload.)

## Public / restricted split

RM SkillCorner and GradientSports partitions are license-restricted: they publish to a private org-members-only companion repo ([`xg-shot-data-v3-restricted`](https://huggingface.co/datasets/luxury-lakehouse/xg-shot-data-v3-restricted)) rather than this public dataset, per lakehouse [ADR-049](https://github.com/karsten-s-nielsen/luxury-lakehouse/blob/main/docs/superpowers/adrs/ADR-049-restricted-hf-dataset-companion-repos.md) / [ADR-064](https://github.com/karsten-s-nielsen/luxury-lakehouse/blob/main/docs/superpowers/adrs/ADR-064-per-match-access-tier.md). The split is per-row (per-match `access_tier`), so a public-licensed SkillCorner match publishes here while a restricted one goes to the companion. A partition migrates here automatically once its license permits public redistribution. **The `xg_model_v3` trainer reads BOTH repos.**

## Quick Start

Every row carries a `data_source` column. The dataset is split into one config per provider, so you can pull a single provider **without downloading the rest**:

```python
from datasets import load_dataset

# All public providers at once (config "all" — the default):
ds = load_dataset("luxury-lakehouse/xg-shot-data-v3", "all", split="train")
df = ds.to_pandas()
print(df["data_source"].value_counts())

# Just one provider (downloads only that provider's parquet):
sb = load_dataset("luxury-lakehouse/xg-shot-data-v3", "statsbomb", split="train").to_pandas()
```

## Related artifacts

- [`xg-shot-freeze-frames`](https://huggingface.co/datasets/luxury-lakehouse/xg-shot-freeze-frames) — the per-shot freeze-frame player set (the context half of the corpus), joined on `(match_key, action_id)`
- [`spadl-vaep-action-values`](https://huggingface.co/datasets/luxury-lakehouse/spadl-vaep-action-values) — the full action-level value corpus

## Citation

```bibtex
@software{luxury_lakehouse,
  title  = {Luxury Lakehouse — Serverless Soccer Analytics Platform},
  url    = {https://github.com/karsten-s-nielsen/luxury-lakehouse}
}
```

## License

CC-BY-NC-4.0 — see repository for details.
