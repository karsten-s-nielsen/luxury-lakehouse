-- Backfill access_tier on pre-H1-ingested bronze tracking tables.
--
-- All raw tracking tables were populated BEFORE H1 (per-match access_tier, ADR-064) added
-- tier stamping, so every row carries access_tier = NULL. split_restricted() fail-safes
-- NULL -> restricted, which WRONGLY WITHHOLDS public (open-data / MIT-licensed) tracking
-- from the public HF datasets. Public data must never be blocked as private.
--
-- Tier is set to mirror classify_access_tier(provider, visibility):
--   * skillcorner — per-match `visibility` feed (pining): public -> 'public',
--     private (RM) -> 'restricted'. (RM rows ingested post-H1 are already 'restricted';
--     no-op for them.)
--   * idsse, metrica — PUBLIC_BY_LICENSE_PROVIDERS (Bundesliga DFL open-data / Metrica
--     sample data): every row is 'public'.
--   * gradientsports — a RESTRICTED provider. NULL already fail-safes to 'restricted' in
--     split_restricted (the correct/intended outcome), so it is intentionally LEFT as-is:
--     an explicit stamp of ~270M rows buys no correctness for the public-data concern.
--
-- Idempotent by construction (only touches access_tier IS NULL rows). Re-runnable.

-- ── skillcorner: derive from the authoritative per-match visibility ──────────────────
UPDATE soccer_analytics.bronze.skillcorner_tracking AS t
SET access_tier = 'public'
WHERE t.access_tier IS NULL
  AND t.match_id IN (
    SELECT match_id FROM soccer_analytics.bronze.skillcorner_matches WHERE visibility = 'public'
  );

UPDATE soccer_analytics.bronze.skillcorner_tracking AS t
SET access_tier = 'restricted'
WHERE t.access_tier IS NULL
  AND t.match_id IN (
    SELECT match_id FROM soccer_analytics.bronze.skillcorner_matches WHERE visibility = 'private'
  );

-- ── idsse / metrica: public by license (all rows) ───────────────────────────────────
UPDATE soccer_analytics.bronze.idsse_tracking
SET access_tier = 'public'
WHERE access_tier IS NULL;

UPDATE soccer_analytics.bronze.metrica_tracking
SET access_tier = 'public'
WHERE access_tier IS NULL;
