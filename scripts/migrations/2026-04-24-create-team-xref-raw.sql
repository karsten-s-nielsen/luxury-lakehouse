-- Kimball PR 5a — new bronze table for cross-provider team identity pairs.
-- Mirror of player_xref_raw shape (source_a / source_b provider-label columns,
-- not provider_a / provider_b — matches the existing bronze schema convention).
-- Populated by scripts/generate_entity_xref.py.
-- Ref: docs/superpowers/specs/2026-04-24-kimball-pr5-design.md §2 + §3.4

CREATE TABLE IF NOT EXISTS soccer_analytics.bronze.team_xref_raw (
    source_a         STRING,
    team_id_a        STRING,
    source_b         STRING,
    team_id_b        STRING,
    confidence       DOUBLE,
    match_layer      INT,
    resolution_type  STRING,
    _ingested_at     TIMESTAMP
)
USING DELTA
TBLPROPERTIES (
    'delta.columnMapping.mode' = 'name',
    'delta.minReaderVersion' = '2',
    'delta.minWriterVersion' = '5',
    'delta.autoOptimize.optimizeWrite' = 'true'
);
