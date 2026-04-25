-- Kimball PR 5a — (superseded) provider_a/provider_b on bronze.player_xref_raw.
--
-- During execution we discovered the bronze table already had `source_a` and
-- `source_b` columns pre-populated with 'statsbomb' / 'wyscout' for all 2,780
-- existing rows — written by the legacy Python matcher but never consumed by
-- any dbt model. Adding provider_a/provider_b would have been redundant.
--
-- Resolution: adopt the existing `source_a` / `source_b` as the canonical
-- Kimball provider-label columns. No schema change required on
-- bronze.player_xref_raw. This file is retained for rollout history; running
-- it is a no-op today.
--
-- Enable name-mapping mode so future DROP / RENAME operations remain safe.

ALTER TABLE soccer_analytics.bronze.player_xref_raw
  SET TBLPROPERTIES ('delta.columnMapping.mode' = 'name',
                     'delta.minReaderVersion' = '2',
                     'delta.minWriterVersion' = '5');
