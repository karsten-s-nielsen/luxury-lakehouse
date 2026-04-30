# ADR-018: Cross-Table Format-Contract Testing

| Field | Value |
|---|---|
| **Date** | 2026-04-29 |
| **Status** | Accepted |
| **Deciders** | Karsten S. Nielsen |

## Context

Three PR-LL waves (PR-LL1 through PR-LL2 + close-out) over 36 hours
shipped six bugs that all share a single systemic root: code is the
source of truth for cross-file conventions that no test enforces. Every
existing test asserts properties of a single file in isolation:

- `test_spadl_vaep_writer_parity.py` — DDL ↔ StructType parity (ADR-002),
  one writer
- `test_idsse_bronze_coverage.py` — bronze column presence vs DFL XML
  attribute set, one writer
- `validate_pr_ll2_post_deploy.py` — non-NULL counts per source per
  column on bronze, one bronze table

No test asserts that **values flowing out of bronze.X** can join to
**values flowing into dim.Y**. The mart `not_null` test on `match_key`
is the first thing that catches a `(bronze_native_id, dim_native_id)`
format drift — and only at full-refresh `dbt build` against
production-scale data. Slim CI runs against a small fixture slice so
the drift only surfaces against the full real dim at scale.

PR-LL2 Path B close-out's six bugs map to seven recurring patterns
(see the close-out spec "The seven recurring patterns"). Five of seven
require this ADR's testing layer to catch at PR time.

## Decision

Every cross-file convention that produces a value used as a JOIN key
MUST be enforced by a test that runs at PR-time, not at
full-refresh-build time. Concretely:

1. **Format generators centralised.** Every native ID format used at
   a JOIN key has a single canonical generator function in
   `src/shared/identifiers.py`. Bronze writers, dim staging
   reflections, and applyInPandas UDFs reach into this module —
   format errors raise `ValueError` at construction time.

2. **Format-contract Python tests.** `src/tests/test_format_contract.py`
   parametrizes over (source, entity) and asserts:
   (a) bronze writer's emitted format matches the dim staging's
   regex/format expectation; (b) the canonical generator function in
   `shared.identifiers` produces the same string a test fixture's
   bronze writer would. Runs in slim CI.

3. **dbt singular JOIN-coverage tests.** Every
   `(bronze.X.native_id_col, dim.Y.native_id_col)` pair used by a
   JOIN in `fct_*` marts has an
   `assert_<source>_<entity>_native_join_resolves.sql` singular
   test. Returns rows ⇒ failure. Tagged `slim_ci`.

4. **Third-party API boundary tests.** Every external library producing
   values that flow into bronze gets a boundary test at OUR repo
   asserting the contract we depend on. Runs in slim CI.

5. **Adding a new bronze writer / dim staging touchpoint requires
   adding the corresponding format-contract test in the same PR.**
   Enforced by code review.

## Alternatives considered

| Option | Pros | Cons | Why rejected |
|---|---|---|---|
| A. Documentation only | minimal change | failed empirically — PR-LL1 + PR-LL2 + close-out shipped six bugs despite ADR-016 documenting the rules | failed empirically |
| B. Pydantic NamedTuple identifier wrappers | strong type safety throughout pipeline | doesn't help dbt SQL boundary; significant refactor across all bronze writers; deferred to PR-LL3 (S7) | scope; F1 functions catch the same errors more cheaply |
| C. Static schema parity only (extension of ADR-002) | builds on existing pattern | ADR-002 is single-table writer/DDL; cross-table is a different problem | needed but not sufficient — covers schema, not values |
| D (chosen). F1+F2+F3+F4+F5 layered enforcement | catches all 6 bug-pattern classes; runnable in slim CI; extension via per-source parametrize | slightly more LOC than C alone | — |

## Consequences

### Positive

- Six bug classes catchable at PR time (slim CI), not at production-scale full-refresh.
- Single source of truth for format strings closes the surface for P1
  (writer↔dim drift).
- silly-kicks API drift catchable at OUR boundary even when upstream
  doesn't break us obviously.
- Pattern locks for future converter additions (4-source SPADL, future
  SkillCorner / Opta / Bundesliga additions).

### Negative

- 12 dbt singular tests added (4 sources × 3 entities). Each scans a
  bounded mart slice; aggregate slim-CI cost ~10s per `dbt build`.
- 16 silly-kicks boundary tests added. Each loads a small parquet
  fixture (1 match) and runs a converter; aggregate ~5s.
- Future converters / sources require parametrization addition + 3
  new dbt tests + boundary fixture. Estimated 2 hr per new source.

### Neutral

- ADR-002 (writer/DDL parity) unchanged; this ADR extends the discipline
  to cross-table joins.
- ADR-016 SPADL Path B naming rule reinforced — every native ID format
  declared in ADR-016 now has a canonical generator function.
- PR-LL3 scope tracker S5/S6/S7 deferred items remain valid follow-ups.

## CLAUDE.md Amendment

Adds one bullet to the "Architectural Decision Records (ADRs)" section
under `## When to write an ADR`:

> - Introduces a cross-table value-format contract or referential-integrity
>   invariant (e.g., `bronze.X.native_id` ⊆ `dim.Y.native_id` per
>   provider). See ADR-018 + the per-(source, entity) singular tests
>   under `dbt_project/tests/`.

And one rule under `## Code Quality`:

> **Cross-table format contracts** ([ADR-018](docs/superpowers/adrs/ADR-018-cross-table-format-contract-testing.md)):
> Every native ID format used as a JOIN key has its canonical generator
> in `src/shared/identifiers.py`. Bronze writers + applyInPandas UDFs
> import from this module; dbt singular tests
> (`assert_<source>_<entity>_native_join_resolves.sql`) assert
> JOIN-coverage from `bronze.spadl_actions` to `dim_*`. Adding a new
> bronze writer / dim staging touchpoint REQUIRES adding the
> corresponding format-contract test in the same PR.

## Related

- **Spec:** `docs/superpowers/specs/2026-04-29-pr-ll2-path-b-close-out-design.md`
- **Plan:** `docs/superpowers/plans/2026-04-29-pr-ll2-path-b-close-out.md`
- **PRs:** PR #224 (PR-LL2), PR-LL2 close-out (this PR — TBD)
- **ADRs:**
  - ADR-002 (writer/DDL parity, single-table) — this ADR extends
  - ADR-011 (Kimball surrogate keys) — JOIN keys this ADR protects
  - ADR-016 (SPADL canonical/native naming) — names enforced
- **External references:** silly-kicks 2.0.0 ADR-001 (caller's identifier
  conventions are sacred) — peer pattern at upstream library boundary.
