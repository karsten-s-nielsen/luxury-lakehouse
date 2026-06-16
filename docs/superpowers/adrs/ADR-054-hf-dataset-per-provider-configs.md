# ADR-054: Per-provider HF dataset configs via flat layout + dynamic card injection

| Field | Value |
|---|---|
| **Date** | 2026-06-15 |
| **Status** | Accepted |
| **Deciders** | Karsten Nielsen |

## Context

The multi-provider HF datasets (action-context, and the siblings spadl-vaep, tracking-context,
xg-shots, line-breaking) were published Hive-partitioned: `data/data_source=<provider>/data.parquet`,
with the `data_source` column dropped from the parquet and recovered from the path. With that layout
and **no `configs:` block in the dataset card**, HF auto-detects a single default config and exposes
`data_source` only as a recovered column. Consequence: all providers **collapse into one set** — the
viewer shows one undifferentiated dataset, and there is no documented way to pull a single provider
(e.g. SkillCorner) without downloading the entire corpus and filtering client-side. The
`spadl-action-context-restricted` card had a single hand-written `default` config globbing
`data/*/data.parquet`, which made the problem explicit (one set, no per-provider split).

A static per-provider `configs:` list in each card would work but **drifts**: the published providers
are data-dependent (a run may add statsbomb/wyscout AC, or a provider may be empty), and a config
pointing at a missing file breaks the viewer. The layout (Hive paths + dropped column) also makes
explicit `data_files` configs awkward — HF does **not** apply Hive `key=value` path-key recovery to
explicitly-listed `data_files`, so an "all" config over Hive paths would silently lose `data_source`.

## Decision

1. **Flat per-provider files, `data_source` retained.** Publishers write `data/<provider>.parquet`
   (one flat file per provider) and KEEP the `data_source` column. Every config — including the
   default "all" — then carries `data_source` from a real column, with zero reliance on Hive
   path-key recovery.
2. **Per-provider HF configs, generated dynamically.** Each card gets a `configs:` block: a default
   `all` config (`data/*.parquet`) plus one config per provider (`data/<provider>.parquet`). The
   block is **injected at publish time from the providers actually present** — never a static list in
   the card — so it cannot drift from the data and auto-handles new providers (statsbomb/wyscout when
   their AC lands).
3. **Reusable mechanism in `ingestion.hf_publish`** (single source of truth, ADR-014 peer):
   `build_provider_configs(providers)` builds the config list; `inject_frontmatter_configs(card, cfgs)`
   splices it into the card's YAML frontmatter (preserving all other keys + body);
   `upload_hf_readme(..., config_providers=…)` applies both before upload. `config_providers=None`/empty
   keeps the upload byte-identical (existing callers and non-dataset repos unaffected).

First applied to `publish_action_context_hf.py` (public: idsse/metrica/skillcorner; restricted:
gradientsports). The 4 sibling publishers adopt it by switching to flat files + passing
`config_providers` — tracked as a follow-up.

## Alternatives considered

| Option | Pros | Cons | Why rejected |
|---|---|---|---|
| A. Static per-provider `configs:` in each card | Card-only; no publisher change | Drifts from the data; a config pointing at an absent provider breaks the viewer; must hand-edit on every provider change | Fragile, high-maintenance |
| B. Keep Hive layout + add configs over Hive globs | No re-layout | HF doesn't recover Hive keys for explicit `data_files` → "all" config silently loses `data_source` (regression); column-vs-path double-count risk | Unreliable; data-loss-shaped |
| C. **Flat files + dynamic injection (chosen)** | Zero drift; `data_source` always present; reusable across all 5 datasets; viewer subset selector + `load_dataset(repo, "<provider>")` | Changes the published file layout (Hyrum's-law break for hardcoded paths); needs a re-publish | — |

## Consequences

### Positive
- Viewer shows a per-provider subset selector; consumers pull one provider via
  `load_dataset(repo, "skillcorner")` without downloading the rest.
- Configs are data-driven → no static list to maintain; new providers appear automatically.
- `data_source` is an explicit column in every config (no Hive-recovery fragility).
- The mechanism is shared, so the 4 sibling datasets get it for ~free.

### Negative
- **Breaking layout change**: `data/data_source=<provider>/data.parquet` → `data/<provider>.parquet`.
  External consumers with hardcoded partition paths must move to the configs API (the new documented
  contract). `upload_folder(delete_patterns=["**"])` sweeps the old Hive dirs on the next publish.
- Takes effect only on a **re-publish** (re-lays-out files + uploads injected cards).
- The on-disk card's frontmatter no longer shows the configs (injected at publish); a `# NOTE`
  comment points maintainers at the mechanism, and the comment is stripped from the uploaded copy
  (YAML round-trip) — by design.

### Neutral
- Private datasets still have no HF viewer regardless (PRO/Enterprise-gated) — orthogonal to this ADR.
- Non-dataset README uploads (models, org Space) are unaffected (`config_providers` rejected for
  non-dataset repo types).

## Related
- **ADRs:** peer to `ADR-014` (HF card upload via `upload_hf_readme`, the parity contract) and
  `ADR-049` (restricted companion repos / `split_restricted`).
- **Code:** `src/ingestion/hf_publish.py` (`build_provider_configs`, `inject_frontmatter_configs`,
  `upload_hf_readme`); `scripts/publish_action_context_hf.py`.
- **Tests:** `src/tests/test_hf_publish.py::TestProviderConfigInjection`.
- **Follow-up:** roll out to `publish_spadl_vaep_hf.py`, `publish_tracking_context_hf.py`,
  `publish_xg_shots_hf.py`, `publish_line_breaking_passes_hf.py`.
