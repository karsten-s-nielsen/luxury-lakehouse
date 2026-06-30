-- Encode existing SkillCorner match-info as EXPLICIT public (H1.4 + review P1/P3).
--
-- After the P1 allowlist flip, `classify_access_tier(skillcorner, None)` fails safe to RESTRICTED, and the publish
-- guard requires an EXPLICIT `visibility='public'` for non-allowlisted providers reaching public. The existing
-- SkillCorner rows are the public A-League (no private Real Madrid match ingested yet), so stamp their `visibility`
-- explicitly. `access_tier` is already 'public' from the 2026-06-30 backfill; this adds the matching `visibility`.
--
-- A privacy-stamp MUST verify its own premise: a wrong-public baked into the source of truth is unrecoverable.
-- Mapping verified 2026-06-30: competition_id 61 <-> competition_name 'A-League' for all 360 rows. The assertion
-- aborts the migration if ANY row is not the confirmed-public A-League (guarding on the human-meaningful name, not
-- just the magic id). Run-once, idempotent (the UPDATE is gated on `visibility IS NULL`).

SELECT assert_true(
  (SELECT COUNT(*) FROM soccer_analytics.bronze.skillcorner_matches
     WHERE NOT (competition_id = 61 AND competition_name = 'A-League')) = 0,
  'ABORT: non-A-League SkillCorner match present in skillcorner_matches -- do NOT mass-stamp visibility=public'
);

UPDATE soccer_analytics.bronze.skillcorner_matches
  SET visibility = 'public'
  WHERE visibility IS NULL AND competition_id = 61 AND competition_name = 'A-League';
