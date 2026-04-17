# ADR-003: Machine-enforce dbt-derived workflow outputs via a `dbt_model` TableRef field

| Field | Value |
|---|---|
| **Date** | 2026-04-16 |
| **Status** | Accepted |
| **Deciders** | Karsten Nielsen |

## Context

Workflow cards in `workflow-cards/wf-*.yaml` describe the authoritative governance surface for every AI/ML and data-movement workflow on the platform — inputs, outputs, cost, academic provenance, monitoring thresholds. `outputs.tables` is a list of `TableRef` entries, one per Delta table the workflow produces.

A structural ambiguity surfaced during the PR #128 follow-up cycle. Some cards produce their tables via a Python execution phase on the same card (e.g. `wf-vaep` produces `bronze.vaep_action_values` via `ingestion.spadl_vaep`). Other cards declare an output table that is actually produced by the nightly `dbt build` task on `wf-dbt-build` — the card lists the table for discoverability, but there is no corresponding `execution:` phase on the card that produces it. `wf-goalkeeper.yaml` was the motivating case: it declares `fct_goalkeeper_stats` as an output, but PR #128 correctly removed the misleading `inference: {entry_point: n/a}` phase, leaving the output entry with no explicit execution owner. Several other cards (VAEP, DEFCON, off-ball-xT) have the opposite issue — they do not declare their downstream dbt-derived gold tables at all, creating an asymmetric governance picture.

CLAUDE.md line 253 says cards "describe inputs, outputs, dependencies, execution config, academic provenance, cost estimates, and monitoring thresholds." It does not prescribe how a dbt-derived output should be linked to the authoritative dbt model definition. Without a machine-enforceable convention, a future card author can rename the dbt model, move it to a different mart file, or add a new mart without any signal reaching the card layer.

## Decision

Add a `dbt_model: str | None` optional field to the `TableRef` Pydantic model. When a `TableRef` is produced by a dbt model rather than by a Python execution phase on the same card, `dbt_model` MUST be set to the name of the owning `.sql` file under `dbt_project/models/**/` (stem, without extension). A new test, `test_card_dbt_model_field`, enforces two parity rules: (a) every `dbt_model:` value on every card resolves to a real `.sql` file on disk, and (b) every `.sql` file under `dbt_project/models/marts/` appears as a `TableRef` with `dbt_model:` set on `wf-dbt-build.yaml`.

## Alternatives considered

| Option | Pros | Cons | Why rejected |
|---|---|---|---|
| A. Re-introduce an inference phase with a `entry_point: dbt:<model_name>` sentinel on cards like `wf-goalkeeper` | Keeps the pattern "execution phase owns the output" visually consistent with every other card. | Requires extending `InferenceExecution` and `RuntimeLiteral` to accept a `dbt:` prefix — a novel convention with no precedent. PR #128's own commit at `workflow-cards/wf-goalkeeper.yaml:63` explicitly rejected "declaring a fake Python entry_point here would misrepresent the actual execution path." | Misrepresents execution; adds schema noise without an enforcement win. |
| B. Accept the gap; document a CLAUDE.md policy that dbt-derived gold tables are governed in `_marts__models.yml` only | Zero code change; no schema churn. | Test does not enforce anything; a future rename in `dbt_project/` would not break any CI signal. `wf-dbt-build.yaml`'s historical under-declaration (1 table listed, 33 built) would remain misleading. | Documentation-only rules drift; this one already drifted once inside PR #128. |
| C. Add `dbt_model: str \| None` to `TableRef` and enumerate every mart on `wf-dbt-build` | Machine-enforceable traceability. Mirrors the existing optional `mart:` field on `TableRef`. Typos break CI at build time. Reverse parity (every `.sql` file must appear on the card) makes the card the authoritative inventory. | One-time cost: enumerate 33 gold mart tables on `wf-dbt-build.yaml`. Ongoing cost: any new dbt mart requires a new `TableRef` entry. | — |

## Consequences

### Positive

- Every `dbt_model:` value in every workflow card is now verified against real `.sql` files in CI. Typos fail fast.
- `wf-dbt-build.yaml` is the authoritative inventory of gold-layer mart tables. A new dbt model without a matching card entry fails `test_wf_dbt_build_enumerates_every_mart_model`.
- `wf-goalkeeper.yaml` now declares its dbt-derived output explicitly via `dbt_model: fct_goalkeeper_stats`, resolving the post-PR-#128 governance gap.
- Future cards declaring dbt-derived outputs have a canonical pattern to follow, enforced by CI.

### Negative

- Adding a new dbt mart now requires a one-line `TableRef` entry on `wf-dbt-build.yaml`. The alternative (silent addition) is no longer possible — this is a feature, not a bug, but it is a new maintenance burden on the dbt author.
- The `dbt_model:` field duplicates information already present in `_marts__models.yml` column contracts. The duplication is intentional (the card is the governance surface; the dbt YAML is the column schema surface) but authors need to keep them aligned when adding a new mart.

### Neutral

- The cost-phase / execution-phase parity rule (enforced by `test_card_cost_phase_parity`) is a sibling enforcement rule landing in the same cycle but governed by a separate test.

## Related

- **Commits:** PR #128 (`d692561`) removed the misleading inference phase on `wf-goalkeeper`; this ADR's enforcement cycle follows it.
- **Tests:** `src/tests/test_card_dbt_model_field.py`
- **CLAUDE.md:** Amended to require `dbt_model:` on dbt-derived `TableRef` entries (see the "dbt-derived outputs in workflow cards" bullet under Project Conventions).
- **Cards amended:** `workflow-cards/wf-goalkeeper.yaml`, `workflow-cards/wf-dbt-build.yaml` (33-model enumeration).

## Notes

The `TableRef` class already carried an optional `mart: str | None` field that has never been enforced. This ADR intentionally adds a sibling field (`dbt_model`) rather than re-purposing `mart` because the two concepts are distinct: `mart` documents which logical mart a table belongs to (cross-cutting grouping), while `dbt_model` names the specific `.sql` file that produces it (1:1 identity). Rolling them together would lose the distinction and make downstream tooling ambiguous.
