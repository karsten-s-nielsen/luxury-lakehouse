# ADR-003: The Kimball id layer carried over from v1

## Status

Accepted (2026-10-27). Owner decision: carry the v1 id layer over completely (spec §14). Keys built in plan Tasks 2
and 3, consumed by resolution in plan Task 22. Carries over v1 ADR-011 (surrogate keys) and v1 ADR-018 (cross-table
format contracts).

## Context

- The owner said the v1 id layer "eliminated any confusion about IDs overlapping between providers" (spec §14).
- Five providers deliver ids in their own formats: StatsBomb and Wyscout integers, IDSSE `DFL-...` strings,
  SkillCorner and Gradient Sports numeric strings. The same number can name different things at two providers.
- v1 and v2 run side by side for a while. Their dimensions must be comparable row by row.
- A bronze key is permanent: a key that changes later would orphan every fact and link that holds it.

## Decision

**Keys.** `match_key`, `player_key`, `team_key`, `competition_key` and the new `season_key` are
`xxhash64(concat_ws('|', provider, cast(native_id as string)))` with seed 42, as a signed 64-bit BIGINT — the formula
of the v1 dbt macros. Python computes them with `xxhash.xxh64_intdigest(f"{provider}|{native}".encode("utf-8"),
seed=42)`, converted to signed (`lakehouse.ids.keys`).

- The provider names in the hash are the v1 names `statsbomb`, `wyscout`, `idsse`, `skillcorner`,
  `gradientsports`. The channel (public or pining) never enters the hash: a match has one key whatever channel
  delivered it.
- A NULL native id gives a NULL player, team, competition or season key, as in v1. A match needs a non-null native
  id; v2 fails loudly without one (the v1 match macro had no NULL guard).
- `season_key` identifies a competition-season: the native season id is `<native_competition_id>:<native_season_id>`
  (spec §14.4), because StatsBomb shares one season id across competitions.

**One generator.** Every key is computed once, in Python, at bronze write time. dbt reads key columns and never
hashes. Each native id format has one generator in `lakehouse.ids.native`, with a format-contract test. Resolution
(`lakehouse.resolve.records`) builds its records with the same generators.

**Natural keys stay.** Every dimension keeps `(provider, native_*_id)` as attributes. Facts and bronze hold only the
per-provider keys, which never change. Cross-provider identity is a separate layer: the link tables and the
canonical pointer on the dimensions (ADR-004).

**Parity.** `tests/unit/test_canonical_keys.py` holds known v1 values per entity, read from the v1 Lakebase
dimensions at Gate G-A (plan Task 2), including NULL native ids. Check value: `match_key("idsse", "J03WMX") ==
4203055498971744044`.

## Consequences

- v1 and v2 dimensions join on the same keys during the side-by-side period.
- A provider rename or a change to a native id format is a key break. It needs a new ADR and a migration plan.
- Resolution never rewrites a key. A merge of records across providers changes only the links and the canonical
  pointers (ADR-004), so a wrong link is undone without touching bronze.
- The legacy `canonical_player_id` string of v1 is dropped (spec §14.3, guess 50).
