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
  - freeze-frames
  - tracking
size_categories:
  - 1M<n<10M
# NOTE: the per-provider `configs:` block is injected at publish time from the providers
# actually present (ingestion.hf_publish.build_provider_configs / inject_frontmatter_configs),
# so it never drifts from the data. See the Quick Start for the resulting configs.
---

# Pre-Shot xG v3 — Shot Freeze Frames (Context Corpus)

The **context half** of the training corpus for `xg_model_v3`, the canonical-SPADL-native **pre-shot expected goals** model from the [luxury-lakehouse](https://github.com/karsten-s-nielsen/luxury-lakehouse) analytics platform. **One row per (shot, player)** — every player present in the shot's freeze frame is a row. The provider is the `data_source` column, not a separate file format. Sourced from `bronze.shot_freeze_frames`.

Each shot's freeze-frame player set is joinable to its tabular shot record (dataset [`xg-shot-data-v3`](https://huggingface.co/datasets/luxury-lakehouse/xg-shot-data-v3)) on the shot identity **`(match_key, action_id)`** — `action_id` is per-match, NOT globally unique, so both keys are always required. The set encoder in `xg_model_v3` **sum-aggregates** this player set into the shot's context vector.

## Columns / contract

| Column | Type | Meaning |
|--------|------|---------|
| `match_key` | BIGINT | Kimball match surrogate — half of the shot identity |
| `action_id` | BIGINT | Per-match SPADL action id — the other half of the shot identity |
| `data_source` | STRING | Provider (`statsbomb`, `skillcorner`, `gradientsports`, ...) |
| `player_id` | STRING | Player present in this shot's freeze frame |
| `x` | DOUBLE | Player x, canonical SPADL 105×68, home-LTR (goal at x=105) |
| `y` | DOUBLE | Player y, canonical SPADL 105×68 |
| `is_keeper` | INT | 1 if this player is a goalkeeper, else 0 |
| `is_teammate` | INT | 1 if this player is on the shooting team, else 0 |
| `set_cardinality` | INT | Number of players in this shot's freeze-frame set |
| `shooter_attacks_high_x` | BOOLEAN | Whether the shooting team attacks the HIGH-x goal in the canonical home-LTR frame (per-shot orientation; may be NULL when it could not be derived) |
| `team_attacking_direction` | STRING | Provenance string the `shooter_attacks_high_x` flag is derived from |

Coordinates are **canonical SPADL 105×68, home-LTR** — no provider is bent to StatsBomb units. StatsBomb-360 freeze frames (raw 120×80) are converted at compute time. One row per (shot, player); the ball row is dropped and the shooter is always included (the sum-aggregation requires actor-inclusion consistency across sources). (`access_tier` is used internally for the public/restricted split and is dropped before upload.)

## Public / restricted split

RM SkillCorner and GradientSports partitions are license-restricted: they publish to a private org-members-only companion repo ([`xg-shot-freeze-frames-restricted`](https://huggingface.co/datasets/luxury-lakehouse/xg-shot-freeze-frames-restricted)) rather than this public dataset, per lakehouse [ADR-049](https://github.com/karsten-s-nielsen/luxury-lakehouse/blob/main/docs/superpowers/adrs/ADR-049-restricted-hf-dataset-companion-repos.md) / [ADR-064](https://github.com/karsten-s-nielsen/luxury-lakehouse/blob/main/docs/superpowers/adrs/ADR-064-per-match-access-tier.md). StatsBomb-360 freeze frames are **public**. The split is per-row (per-match `access_tier`), so a public-licensed SkillCorner match publishes here while a restricted one goes to the companion. A partition migrates here automatically once its license permits public redistribution. **The `xg_model_v3` trainer reads BOTH repos.**

## Quick Start

Every row carries a `data_source` column. The dataset is split into one config per provider, so you can pull a single provider **without downloading the rest**:

```python
from datasets import load_dataset

# All public providers at once (config "all" — the default):
ds = load_dataset("luxury-lakehouse/xg-shot-freeze-frames", "all", split="train")
df = ds.to_pandas()
print(df["data_source"].value_counts())

# Just one provider (downloads only that provider's parquet):
sb = load_dataset("luxury-lakehouse/xg-shot-freeze-frames", "statsbomb", split="train").to_pandas()

# Reassemble a single shot's freeze-frame set:
shot = sb[(sb["match_key"] == 12345) & (sb["action_id"] == 678)]
```

## Related artifacts

- [`xg-shot-data-v3`](https://huggingface.co/datasets/luxury-lakehouse/xg-shot-data-v3) — the tabular shot record (the other half of the corpus), joined on `(match_key, action_id)`
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
