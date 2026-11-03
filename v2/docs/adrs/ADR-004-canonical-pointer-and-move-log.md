# ADR-004: The canonical pointer and its move log

## Status

Accepted (2026-10-27). Owner decision: every pointer move is logged (spec §14.3; replaces guess 41). Built in plan
Task 22; the dimensions read the pointers in plan Task 23.

## Context

- Each provider's record of a real player, team, competition or match has its own key (ADR-003). Analysts need one
  id per real entity across providers.
- v1 put a canonical pointer on the dimension (`dim_players.sql:230-262`). It recomputed the pointer on every build
  and kept no record of a change, so nobody could say why a pointer moved.
- Clusters change: a new match can add a record, a hand decision can join or split a cluster, and an undo can remove
  a batch of links (spec §15.3, §15.6).

## Decision

**The pointer.** `canonical_<entity>_key` lives on the dimension only. Facts and bronze never hold it. It names the
member of the cluster from the highest-ranked provider, then the smallest native id. The rank is the v1 order without
Metrica: `statsbomb` > `wyscout` > `idsse` > `skillcorner` > `gradientsports`. The IRIS resolver put SkillCorner
before IDSSE; v2 follows v1.

**Clusters.** Each resolution pass clusters the links of every provider pair with union-find
(`lakehouse.resolve.nway`, ported from IRIS). A same-source guard splits a component that holds two ids of one
provider: the provider with the most ids gives the anchors, and each other record joins the anchor it links to with
the highest confidence among the anchors that do not yet hold its provider. No cluster ever holds two ids of one
provider.

**Storage.** The pass writes one row per record to `ops_canonical_pointers` (entity, provider, native id, record key,
canonical provider, native id and key, run id). dbt joins it into the dimensions. A table name under `ops_` keeps
it in the lake-root allowlist `{dim_*, *_links, ops_*, lineage_*}`.

**The move log.** When a record's canonical key differs from its row in the previous pass, the pass writes one row
to `ops_pointer_moves`: entity, record, old canonical key, new canonical key, run id, time and reason. The reasons:

- `a higher-ranked record joined the cluster` — the old canonical member is still in the cluster;
- `the canonical record left the cluster` — the old canonical member is in another cluster now, or gone;
- `the cluster changed` — any other change.

A record's first pointer is not a move. A pass that changes nothing writes no row. Guard:
`tests/unit/test_resolve_links.py::test_pointer_move_logged`.

## Consequences

- Every pointer change can be explained from the log, with the run that made it.
- A consumer that stores a canonical key must expect it to move. The log tells it from which value to which.
- The pointer table is rewritten on every pass, so it always matches the current links. The move log only grows.
