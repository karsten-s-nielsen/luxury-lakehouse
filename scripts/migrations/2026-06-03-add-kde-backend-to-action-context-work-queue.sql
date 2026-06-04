-- scripts/migrations/2026-06-03-add-kde-backend-to-action-context-work-queue.sql
--
-- Adds the ghost-GK kde_backend policy/provenance column to the AC-1 work queue (ADR-035 amendment).
-- The backend is resolved at preflight and stamped onto each WorkUnit (domain policy on the work spec);
-- the drain worker reads it per-unit. See the 2026-06-03-ac1-ghost-gk-backend-and-period-units design.
--
-- Idempotent: ALTER ... ADD COLUMNS is skip-if-exists handled by scripts/migrations/_runner.py.
-- Auto-applied by .github/workflows/dbt-live-ci.yml "Apply pending bronze migrations" step.

ALTER TABLE soccer_analytics.observability.action_context_work_queue ADD COLUMNS (
  kde_backend STRING
);
