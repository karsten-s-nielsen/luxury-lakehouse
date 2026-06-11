#!/usr/bin/env bash
# ──────────────────────────────────────────────────────────────────────────────
# deploy.sh — Build, upload wheel, apply Terraform, and trigger ingestion job
# ──────────────────────────────────────────────────────────────────────────────
# Usage:
#   TF_VAR_databricks_token="$DATABRICKS_TOKEN" AWS_PROFILE=devops-agent ./scripts/deploy.sh
#
# Prerequisites:
#   1. Databricks CLI configured:
#        databricks configure --host "$DATABRICKS_HOST"
#      (generates ~/.databrickscfg with a personal access token)
#
#   2. Databricks token exported as an environment variable:
#        bash:        export TF_VAR_databricks_token=<your-token>
#        PowerShell:  $env:TF_VAR_databricks_token = "<your-token>"
#      (Terraform reads TF_VAR_* automatically — never store tokens in .tfvars)
#
#   3. AWS credentials available (AWS_PROFILE=devops-agent or environment vars)
# ──────────────────────────────────────────────────────────────────────────────

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
WHEEL_NAME="luxury_lakehouse-0.5.34-py3-none-any.whl"
WHEEL_PATH="$PROJECT_ROOT/dist/$WHEEL_NAME"
VOLUME_PATH="/Volumes/soccer_analytics/bronze/libs"
JOB_ID="${DATABRICKS_JOB_ID:?Set DATABRICKS_JOB_ID to the ingestion job ID}"

echo "=== Phase 2 Deployment: Data Ingestion ==="
echo ""

# ── Step 1: Build the wheel ──────────────────────────────────────────────────

echo "Step 1: Building wheel..."
cd "$PROJECT_ROOT"
# D59: use --wheel (not plain `uv build`) because the wheel's force-include
# bundles dbt_project/dbt_packages/ which is gitignored. Plain `uv build`
# would build an sdist that omits dbt_packages then fail the wheel step.
uv build --wheel
echo "  ✓ Built $WHEEL_NAME"
echo ""

# ── Step 2: Upload wheel to UC Volume ────────────────────────────────────────

echo "Step 2: Uploading wheel to Databricks UC Volume..."
echo "  Target: $VOLUME_PATH/$WHEEL_NAME"

# Remove old version if present, then upload new one
databricks fs rm "$VOLUME_PATH/$WHEEL_NAME" 2>/dev/null || true
databricks fs cp "$WHEEL_PATH" "$VOLUME_PATH/$WHEEL_NAME"
echo "  ✓ Uploaded to $VOLUME_PATH/$WHEEL_NAME"
echo ""

# ── Step 3: Terraform apply (creates Volume + updates job) ───────────────────

echo "Step 3: Running terraform apply..."
cd "$PROJECT_ROOT/terraform/environments/dev"
terraform init
terraform apply
echo "  ✓ Terraform applied"
echo ""

# ── Step 4: Trigger the ingestion job ────────────────────────────────────────

echo "Step 4: Triggering ingestion job ($JOB_ID)..."
RUN_ID=$(databricks jobs run-now --job-id "$JOB_ID" --output json | python -c "import sys,json; print(json.load(sys.stdin)['run_id'])")
echo "  ✓ Job triggered — Run ID: $RUN_ID"
echo ""

# ── Step 5: Monitor ──────────────────────────────────────────────────────────

echo "Step 5: Monitoring job run..."
echo "  Databricks UI: ${DATABRICKS_HOST}/#job/$JOB_ID/run/$RUN_ID"
echo ""
echo "  Waiting for completion (Ctrl+C to stop monitoring)..."
databricks runs get --run-id "$RUN_ID" --output json | python -c "
import sys, json, time
run = json.load(sys.stdin)
state = run.get('state', {})
print(f\"  Status: {state.get('life_cycle_state', 'UNKNOWN')} / {state.get('result_state', 'PENDING')}\")
"

echo ""
echo "=== Deployment script complete ==="
echo ""
echo "Next steps:"
echo "  1. Wait for all 3 tasks (statsbomb, metrica, wyscout) to complete"
echo "  2. Verify bronze tables:"
echo "     databricks sql exec --warehouse-id \$SQL_WAREHOUSE_ID \\"
echo "       --sql \"SELECT 'statsbomb_events' AS tbl, COUNT(*) AS n FROM soccer_analytics.bronze.statsbomb_events"
echo "              UNION ALL SELECT 'statsbomb_matches', COUNT(*) FROM soccer_analytics.bronze.statsbomb_matches"
echo "              UNION ALL SELECT 'statsbomb_competitions', COUNT(*) FROM soccer_analytics.bronze.statsbomb_competitions"
echo "              UNION ALL SELECT 'statsbomb_lineups', COUNT(*) FROM soccer_analytics.bronze.statsbomb_lineups"
echo "              UNION ALL SELECT 'statsbomb_360', COUNT(*) FROM soccer_analytics.bronze.statsbomb_360"
echo "              UNION ALL SELECT 'metrica_tracking', COUNT(*) FROM soccer_analytics.bronze.metrica_tracking"
echo "              UNION ALL SELECT 'metrica_events', COUNT(*) FROM soccer_analytics.bronze.metrica_events"
echo "              UNION ALL SELECT 'wyscout_events', COUNT(*) FROM soccer_analytics.bronze.wyscout_events"
echo "              UNION ALL SELECT 'wyscout_matches', COUNT(*) FROM soccer_analytics.bronze.wyscout_matches\""
