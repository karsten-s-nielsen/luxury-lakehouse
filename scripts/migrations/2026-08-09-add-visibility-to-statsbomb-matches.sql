-- PR-2a (spec 2026-08-06 statsbomb-commercial-360-containment) — visibility plumbing.
--
-- SEMANTICALLY INERT. StatsBomb is in PUBLIC_BY_LICENSE_PROVIDERS, so every existing row
-- already resolves to access_tier='public'. This makes that implicit default EXPLICIT and
-- per-row, so the PR-2b flip has real data to flip against instead of an absence.
--
-- OPERATOR-APPLIED, WITH THE MERGE — never after. dbt-live-ci.yml is a daily scheduled live
-- build and PR-2a's dim_matches.sql selects these columns.
--
--   uv run --extra sdk python scripts/migrations/_runner.py \
--     scripts/migrations/2026-08-09-add-visibility-to-statsbomb-matches.sql
--
-- The ALTERs are idempotent (_runner.py DESCRIBE skip-if-exists).
--
-- THE UPDATES ARE DELIBERATELY *NOT* PERMANENTLY RE-RUNNABLE (review B5). A bare
-- `WHERE visibility IS NULL` is idempotent by design and therefore re-runnable forever —
-- so after PR-4 ingests commercial data, any row that reached bronze without a visibility
-- (the exact omission R-16 exists to prevent) would be stamped PUBLIC by a re-run of this
-- file. R-19's precondition guards the Python backfill, which is a different artifact.
-- Bounding on _ingested_at pins the statement to the row set whose OQ-1 premise was
-- verified, and makes a later re-run a no-op instead of a fail-open.
--
-- OPERATOR: replace <CUTOFF> with the UTC timestamp at which you verified OQ-1 — that this
-- table holds zero commercially-licensed rows — using THIS statement:
--
--   SELECT count(*) FROM soccer_analytics.bronze.statsbomb_matches
--    WHERE visibility IS NULL OR visibility <> 'public';   -- expect 0 (pre-migration: all NULL,
--                                                          -- so run it AFTER the ALTERs and
--                                                          -- BEFORE the UPDATEs)
--
-- Record the count and the timestamp in the PR description (OQ-1 evidence).
--
-- The predicate is `IS NULL OR <> 'public'`, NOT `= 'private'` (review A2). classify_access_tier
-- fail-safes on ANY non-'public' value — NULL, 'private', or an unrecognised string — so a
-- `= 'private'` check would report clean on a row the classifier would restrict.
--
-- This statement lives HERE, in the migration, rather than in access_tier_backfill.py's
-- _PRECONDITIONS (review D4). Registering it there would require a `statsbomb` entry in
-- _EXISTING_CONFIRMED_PUBLIC, and default_tier_for_provider returns an override INSTEAD of
-- consulting the classifier — so after the PR-2b flip a no-signal StatsBomb row would still
-- resolve 'public' from a hardcoded override, defeating the fail-safe PR-2b installs.

ALTER TABLE soccer_analytics.bronze.statsbomb_matches ADD COLUMNS (visibility STRING);

ALTER TABLE soccer_analytics.bronze.statsbomb_matches ADD COLUMNS (access_tier STRING);

UPDATE soccer_analytics.bronze.statsbomb_matches
   SET visibility = 'public'
 WHERE visibility IS NULL
   AND _ingested_at < TIMESTAMP '<CUTOFF>';

UPDATE soccer_analytics.bronze.statsbomb_matches
   SET access_tier = 'public'
 WHERE access_tier IS NULL
   AND _ingested_at < TIMESTAMP '<CUTOFF>';
