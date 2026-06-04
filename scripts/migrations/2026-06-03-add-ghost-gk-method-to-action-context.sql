-- Adds the ghost_gk_method backend-provenance column to bronze.spadl_action_context (ADR-035 amendment).
-- Records which ghost-GK KDE backend produced each row's ghost_gk_x/y/spread (scopes to ghost_gk_* only;
-- orthogonal to pitch_control_method). NULL on event-only rows.
--
-- Idempotent: ALTER ... ADD COLUMNS is skip-if-exists handled by scripts/migrations/_runner.py.
-- Auto-applied by .github/workflows/dbt-live-ci.yml "Apply pending bronze migrations" step (new files
-- added in a PR run via --diff-filter=A BEFORE dbt build). Already applied to LIVE manually this PR via the
-- Databricks SDK so the gold mart contract column ghost_gk_method resolves on the next live build.
ALTER TABLE soccer_analytics.bronze.spadl_action_context ADD COLUMNS (
  ghost_gk_method STRING
);
