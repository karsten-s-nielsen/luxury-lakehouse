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
-- OPERATOR — two DIFFERENT checks. Earlier revisions of this comment conflated them into one
-- impossible instruction ("run the visibility count after the ALTERs, before the UPDATEs, expect
-- 0"): at that moment the column exists but every row is NULL, so it returns the FULL row count,
-- never 0. Corrected 2026-08-10 after actually executing it.
--
--  (1) PRECONDITION (OQ-1) — "no commercially-licensed rows exist, so stamping every existing
--      row 'public' is safe". Pre-migration there is NO visibility column, so this CANNOT be a
--      column query. It is a PROVENANCE question, answered by the competition inventory:
--
--        SELECT c.competition_name, c.season_name, count(*) AS n
--          FROM soccer_analytics.bronze.statsbomb_matches m
--          JOIN soccer_analytics.bronze.statsbomb_competitions c
--            ON m.competition_id = c.competition_id AND m.season_id = c.season_id
--         GROUP BY 1, 2 ORDER BY 3 DESC;
--
--      Every row must belong to a StatsBomb FREE/OPEN release. A club-subscription competition
--      appearing here means the premise is broken — STOP, do not apply.
--
--  (2) POSTCONDITION — run AFTER the UPDATEs; proves the stamp reached every row:
--
--        SELECT count(*) FROM soccer_analytics.bronze.statsbomb_matches
--         WHERE visibility IS NULL OR visibility <> 'public';   -- expect 0
--
--      The predicate is `IS NULL OR <> 'public'`, NOT `= 'private'` (review A2):
--      classify_access_tier fail-safes on ANY non-'public' value — NULL, 'private', or an
--      unrecognised string — so a `= 'private'` check would report clean on a row the
--      classifier would restrict.
--
-- Set <CUTOFF> to the UTC timestamp at which (1) was verified. Record both in the PR description.
--
-- Run with an explicit profile if ~/.databrickscfg has more than one entry for this host:
--   DATABRICKS_CONFIG_PROFILE=OAUTH uv run --extra sdk python scripts/migrations/_runner.py <file>
--
-- APPLIED 2026-08-10 11:06:56 UTC (cutoff). Precondition: 3,464 rows / 21 competitions, all
-- StatsBomb open-data releases (2015/16 big-five, WC 2018+2022, WWC 2019+2023, Euro 2020+2024,
-- AFCON 2023, FAWSL, NWSL, Messi-era La Liga, PL 2003/04); no club-subscription competition.
-- Postcondition: 0 rows non-public on both columns; 3,464 rows visibility='public'.
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
