# Decision: Wyscout Anti-Corruption Layer Deferred to dbt Staging

**Status:** Accepted
**Date:** 2026-04-02

## Context

The platform ingests from five data providers: StatsBomb, Metrica Sports, Wyscout, IDSSE (Bundesliga tracking), and SkillCorner (A-League tracking). StatsBomb, Metrica, and IDSSE all have Python-level anti-corruption layers (ACLs) that normalize field names at ingestion time — raw provider fields are mapped to snake_case platform conventions before being written to Delta bronze. This ensures the bronze layer presents a consistent surface regardless of source.

Wyscout uses camelCase provider field names throughout (`eventId`, `matchId`, `wyId`, `subEventName`, `positions`). A Python-level ACL would normalize these before bronze, but Wyscout data is ingested infrequently (it is a static open dataset, not a live feed), and the dbt staging layer already handles normalization for all sources via SQL `AS` aliases.

## Decision

The Wyscout ACL is intentionally deferred to dbt staging models. Python ingestion writes raw Wyscout JSON to bronze without field-name normalization. The staging model (`stg_wyscout_events`, `stg_wyscout_matches`, etc.) performs the camelCase → snake_case mapping alongside all other provider normalizations.

## Alternatives Considered

| Option | Assessment |
|--------|------------|
| Python-level ACL (consistent with Metrica/IDSSE) | Adds a mapping layer to the ingestor for a dataset that never changes; the benefit of bronze-layer consistency is outweighed by the complexity of maintaining two ACL mechanisms (Python + SQL) for the same dataset |
| Skip ACL entirely | Would break dbt staging consistency — downstream models assume snake_case column names from staging |
| Unified Python ACL for all providers | Desirable long-term but requires refactoring StatsBomb/Metrica ingestors; out of scope for current cycle |

## Consequences

**Positive:**
- Simpler Wyscout Python ingestor — no field mapping logic, just JSON extraction and Delta write.
- All field normalization for Wyscout is handled in a single place (dbt staging), keeping the transformation layer consistent with how dbt handles other normalization concerns.

**Negative:**
- Bronze schema is inconsistent across providers: Wyscout bronze uses camelCase (`eventId`, `wyId`), while Metrica and IDSSE bronze use snake_case. This is a documented exception, not a convention.
- The ACL boundary for Wyscout crosses the Python/SQL boundary — normalization logic is split across two languages depending on which provider is being processed. Any engineer reading the Wyscout ingestor must know to look in dbt staging for the field mapping.
