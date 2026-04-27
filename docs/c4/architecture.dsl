workspace "Luxury Lakehouse" "Serverless soccer analytics platform: 41 AI/ML workflows, three-tier cost tracking, 16-page Taipy dashboard on HF Spaces, Databricks Lakebase. Daily Databricks job (28 tasks): 26 ingestion + compute → dbt_build (python_wheel_task materializes 39 gold marts) → refresh_synced_tables (Lakebase propagation). Evolve Engine (AlphaEvolve-style LLM-guided architecture search via OpenEvolve). Four-pillar GK evaluation (PSxG, distribution xT, collection, sweeper). Football2vec 360-enriched embeddings (144d transformer + Deep Sets). ScoutGPT decoder (256d GPT-style, player-conditioned, Hong et al. 2025). Shape graph formation detection (Sotudeh 2026), Football2vec v2 transformer with adversarial debiasing (128d). SHA-256 artifact integrity verification on model loads (SEC-AUDIT ML-02). EU AI Act gap analysis in AI_GOVERNANCE.md covering 13 per-player evaluative ML systems (SEC-AUDIT REG-01), enforced by test_ai_governance_md.py; every HuggingFace model card carries an Intended-Use / Non-Use stanza and every workflow card carries a governance: YAML block. Workflow-card governance extended 2026-04-16 (ADR-003): dbt-derived outputs declare `dbt_model:` on TableRef, enforced by test_card_dbt_model_field; cost.<phase> keys must match execution.<phase> keys, enforced by test_card_cost_phase_parity; databricks_job block ordering generalised across every job in terraform/, enforced by test_workflows_tf_ordering. Semgrep SAST, ruff S (bandit), and import-linter boundary enforcement in CI." {

    model {
        analyst = person "Soccer Analyst" "Coaches, scouts, and analysts exploring match and player data"
        developer = person "Developer" "Deploys application updates and triggers pipeline runs"

        taipyApp = softwareSystem "Taipy Dashboard" "Interactive soccer analytics application with 16 pages: 15 analytics + AI/ML Workflows operations dashboard. Conversion Funnel page built but disabled pending performance validation (D57/D58). Authenticated admin API (POST /api/cache/clear) for forced cache invalidation + on-demand synced table refresh, mounted on the underlying Flask layer via Gui(flask=...)" {
            guiLayer = container "Taipy GUI" "Root template with sidebar navigation, glossary panels, conditional footer (show_site_footer), and page routing" "Python, Taipy 4.1"
            adminApi = container "Admin API" "Flask blueprint mounted on the Taipy GUI's underlying Flask app via Gui(flask=...). Endpoint: POST /api/cache/clear (with optional ?refresh_synced=1). Auth: validates HF user access token against huggingface.co/api/whoami-v2 per request, requires luxury-lakehouse org membership with admin/write role, never stores or logs the token. Optionally spawns isolated background subprocess to run python -m ingestion.refresh_synced_tables --wait. Used for forced cache invalidation during incident response and manual synced-table refresh from outside the daily Databricks job" "Python, Flask Blueprint, requests"
            templateEngine = container "Template Engine" "Three layout builders (standard, sub-view, dashboard) dispatched by build_page(). Dashboard layout: ll-page-scope accent bar + StatCard stats bar + ll-dashboard-scroll viewport container (scope renders ABOVE the tiles so it captions them — scope → stats → content). Typed dataclasses: PageConfig (scope_dims + scope_vars), SubView, ContentBlock (table_cell_class_name for per-cell CSS, alt_var for scope-aware image alt text), ContentRow, SidebarWidget (kinds: dropdown, dropdown_multi, combobox, slider, toggle; required: bool auto-appends ' (optional)' to label; filterable enables Taipy client-side filter; kind='combobox' renders a <|{var}|ll_ext.combobox|…|> fragment backed by the ll_ext GUI extension — ADR-009 — for WAI-ARIA APG server-driven autocomplete), Metric, Citation, StatCard (detail_html for content-provider iframes), ScopeDim (label + value_var for canonical scope line — ADR-008 Tier A canon). Scope rendering: per-dimension Taipy state vars rendered as accent-bar panel (orange left border, uppercase orange labels, bold white values, dot separators); the same span vocabulary is cloned by lightbox JS into the overlay figcaption" "Python, frozen dataclasses"
            sidebarWidgets = container "Sidebar Widgets" "Centralized filter cascade with progressive disclosure, view-dependent visibility, change_delay debounce, and absolute-positioned help tooltips" "Python, Taipy Markdown"
            stateModules = container "State Modules" "Per-page state variables, callbacks, chart rendering (16 modules, SQL-free). Delegates all data fetching to Query Layer. Static charts via mplsoccer PNG. Interactive charts via Plotly. Conversion Funnel: horizontal mirror bars (Plotly), game state filter (winning/losing/drawing). Workflows split: workflows.py orchestrator + workflows_dag.py (Cytoscape.js DAG) + workflows_stats.py (card loading via WorkflowCard from wheel, cost computation). 2-min auto-refresh timer, WCAG shape markers, DISABLED task filtering" "Python, pandas, mplsoccer, Plotly, Cytoscape.js"
            queryLayer = container "Query Layer" "Centralized SQL: 12 modules (shots, passes, tracking, match, players, team_shape, defensive, goalkeepers, tactical_positions, workflows, common). All parameterized queries with TTL cache. State modules are SQL-free after extraction. Column name constants serve as read-side contract documentation" "Python, psycopg2, src/queries/"
            filterLayer = container "Filter Layer" "Shared filter queries with TTL cache, scope labels, data freshness, and embedding player search. Uses recursive CTE for DISTINCT alternatives. Canonical UI helpers (ADR-008): build_scope_label_plain() for image alt text + screen-reader contexts, build_warning(domain, suggestions) for canonical no-data messages. Server-driven autocomplete (ADR-009): _execute_search_query helper + per-entity search_players / search_goalkeepers / search_pausa_players / search_embedding_players; LIKE-escape via _escape_like + SQL ESCAPE; NO_MATCHES_SENTINEL '(no matches)' forces dropdown re-render when the match list is empty (Taipy state diff treats lov=[] as no-op). Consumed by the ll_ext.combobox extension's on_search callback" "Python, psycopg2"
            dbLayer = container "DB Layer" "OAuth token management, connection pooling, parameterized query execution, /health endpoint with background DB connectivity check" "Python, psycopg2, Databricks SDK"
            renderEngine = container "Render Engine" "Matplotlib/mplsoccer figure-to-PNG with cache-busting paths for static pitch diagrams" "Python, matplotlib, mplsoccer"
            pitchControl = container "Pitch Control Engine" "Physics-based (Spearman 2017) and Voronoi pitch control surface computation" "Python, NumPy, SciPy"
            configLayer = container "Config" "Pydantic settings from environment variables and .env file with identifier validation" "Python, pydantic-settings"
            guiExtensions = container "GUI Extensions (ll_ext)" "In-repo Taipy GUI ElementLibrary at hf_taipy_app/src/extensions/ll_ext/ (ADR-009). Current elements: combobox (WAI-ARIA APG combobox-with-list-autocomplete wrapping MUI Autocomplete — listbox hidden when input empty/unfocused, opens on type/ArrowDown, closes on Escape/selection/blur; controlled inputValue, Escape restores selection, NO_MATCHES_SENTINEL rendered as disabled row). Webpack-built UMD bundle checked in at front-end/dist/library.js and served via the Taipy Flask blueprint at /taipy-extension/ll_ext/. MUI/React/emotion externalised via DllReferencePlugin against Taipy's taipy-gui-deps-manifest.json; taipy-gui SDK types installed with --no-save to keep absolute paths out of package.json. Lightbox overlay (main.py _LIGHTBOX_SCRIPT) provides gallery nav via ArrowLeft/Right + on-screen prev/next buttons and clones .ll-page-scope innerHTML into a prominent top figcaption — scope vocabulary shared with the main-page accent bar" "TypeScript, React 18, MUI 6, webpack"
        }

        deployPipeline = softwareSystem "Deploy Pipeline" "Deployment scripts for Taipy app and analytics wheel" {
            deployScript = container "manage_space.py" "CLI tool: full Space lifecycle -- create, deploy, status, rebuild, teardown. Pre-flight checks, upload_folder with ignore/delete patterns, secret management, polling." "Python, huggingface_hub"
            deployWheel = container "deploy_wheel.py" "Downloads wheel from HF Hub build-artifacts (WHEEL_FILENAME from shared.wheel), uploads to UC Volume /Volumes/{catalog}/bronze/libs/, post-upload size verification" "Python, huggingface_hub, databricks-sdk"
            bumpWheel = container "bump_wheel.py" "Syncs wheel version from pyproject.toml to all static consumers (PEP 723 scripts, deploy.sh, Terraform). Modes: --check (CI), --dry-run (preview), --pin-hash (SEC3 SHA-256). Discovers consumers via glob + regex matching" "Python, shared.wheel"
            dbtBuildAndRefresh = container "dbt_build_and_refresh.py" "Canonical local dev flow: chains dbt build with python -m ingestion.refresh_synced_tables --wait. Fail-fast on dbt error (refresh skipped if dbt fails). Forwards extra args to dbt build" "Python, subprocess"
        }

        pipelinePlatform = softwareSystem "AI/ML Pipeline Platform" "41 workflow-card-registered workflows (34 @workflow-decorated Databricks pipelines plus 7 manual/data-movement cards including 3 HF Jobs publish scripts), centralized bootstrap hooks, lifecycle tracking, three-tier cost tracking, YAML manifests with dbt_model and cost/execution phase parity enforcement (ADR-003), Evolve Engine for LLM-guided architecture search, and import-linter boundary enforcement" {
            workflowFramework = container "Workflow Framework" "Registry, @workflow decorator, WorkflowContext, lifecycle runner with on_start/on_complete/on_skip/on_error dispatch. Circular dependency broken via _set_runner injection" "Python, src/workflows/"
            workflowCards = container "Workflow Cards" "41 YAML manifests defining inputs, outputs, deps, execution config, cost estimates, academic provenance. TableRef supports dbt_model: for dbt-derived outputs (ADR-003); wf-dbt-build enumerates all 39 gold marts. cost.<phase> keys must mirror execution.<phase> keys (enforced)" "YAML, workflow-cards/" "Database"
            costEstimateHook = container "CostEstimateHook" "Lifecycle hook writing run state, entity_count (input entities from guard), row_count (output rows), guard_duration_seconds (from timed_check), and cost estimates to workflow_cost_live Delta table via MERGE. Enables three-way decomposition of total task time: env init + guard + pipeline work. Centralized registration via bootstrap_hooks()" "Python, PySpark, Delta, src/ingestion/cost_hook.py"
            hfCostRecorder = container "HFJobsCostRecorder" "Standalone cost recorder for HF Jobs scripts. Writes _workflow_cost.json (live status) and _cost_history/{job_id}.json (per-run history) to HF Hub repos. 90-day auto-pruning" "Python, huggingface_hub, src/ingestion/hf_jobs_cost.py"
            guardRegistry = container "Guard Registry" "SkipGuard protocol + FilterResult dataclass (with guard_duration_seconds field) + find_new_ids() (Spark LEFT ANTI JOIN) + timed_check() wrapper + check_hf_dataset_freshness() (SHA-based skip guard for 5 HF Hub import pipelines: compares HfApi.repo_info().sha against stored SHA in observability.workflow_import_checksums, fails open on network errors) + record_import_sha() (MERGE write-back after successful import). Guard-as-wrapper: each pipeline's main() calls timed_check(skip_guard, ...) which records wall-clock duration via time.monotonic() and returns FilterResult. Mandatory injection: filter_result is a required parameter on all run_pipeline() functions (no default). Conformance tests enforce import isolation, mandatory params, no inline guards, direct guard call, early exit structure/behavior, exception propagation, count/ID consistency, cost/time capture, workflow ID consistency" "Python, src/ingestion/guards.py"
            artifactDeploy = container "Artifact Deploy" "Training-to-production delivery contract (ADR-012). Three helpers used by every training script that targets the Databricks inference path: require_mlflow_env() pre-flight check raises RuntimeError listing missing env vars from (MLFLOW_TRACKING_URI, DATABRICKS_HOST, DATABRICKS_TOKEN); upload_weights_to_uc_volume(*, catalog, schema, model_name, filename, weights_bytes) writes the artifact + a .sha256 sidecar to /Volumes/{catalog}/{schema}/model_weights/{model_name}/{filename} via WorkspaceClient.files.upload(..., overwrite=True); set_and_verify_mlflow_champion(client, *, mlflow_fqn, run_id) wraps set_registered_model_alias with a round-trip get_model_version_by_alias check and raises on zombie-alias state. Closes the producer-side counterpart to ADR-002's consumer-side silent-swallow elimination. Imported by scripts/train_xg_model_hf.py (v1) and scripts/train_xg_v2_hf.py (v2)" "Python, src/ingestion/artifact_deploy.py"
            hfPublish = container "HF Publish Helper" "Producer-side documentation delivery (ADR-014). Peer of Artifact Deploy: artifact_deploy handles *weights*, hf_publish handles *READMEs*. Two public functions: upload_hf_readme(repo_id, readme_path, hf_token, *, repo_type=Literal['dataset','model','space']) validates inputs, LF-normalizes the markdown, calls HfApi.upload_file(path_in_repo='README.md', ...), returns {commit_url, sha256}; get_hf_card_path(name, *, kind=Literal['dataset','model']) wheel-aware resolver — returns site-packages/docs/huggingface/<kind>-cards/<name> if present (wheel install path, force-included in pyproject.toml) else falls back to the repo-root docs/ path. Filename == HF repo basename invariant enforced by src/tests/test_hf_publish_parity.py, which queries HfApi.list_datasets/list_models and diffs against the in-repo card directories. Called by every publisher as the final step after data/weight upload: 3 PEP 723 dataset publishers (publish_spadl_vaep_hf, publish_xg_shots_hf, publish_freeze_frame_hf), 4 workflow-task producers (export_shots_on_target, export_scoutgpt_training_data, export_embeddings_training_data, prepare_360_training_data), 4 PEP 723 compute scripts (compute_obso_hf, compute_epv_transition_hf, compute_xt_grid_hf, compute_space_creation_hf — inline README strings removed as drift sources), and 7 training scripts (train_xg_model_hf, train_xg_v2_hf, train_vaep_model_hf, train_psxg_hf, train_football2vec_v2, train_football2vec_360, train_scoutgpt_hf). Orphan push path scripts/publish_hf_cards.py handles the org Space (docs/huggingface/org-card.md → luxury-lakehouse/README) and 6 method/research model cards that have no payload publisher. AD002: propagates HfHubHTTPError (no silent swallow)" "Python, src/ingestion/hf_publish.py"
            ingestionPipelines = container "Compute Pipelines" "30 @workflow-decorated Databricks pipelines (all instrumented): 5 raw ingestors (StatsBomb, Metrica, Wyscout, IDSSE, SkillCorner), 14 compute (xG, VAEP, DEFCON 360+tracking, pitch control, xT, OBSO/PAUSA, entity resolution, line-breaking 360+tracking, formations EFPI+shape graph, embeddings v1/v2, model validation), 1 HF sync (consolidates 7 former tasks: 3 imports + 3 exports + cost sync), elastic sync. Each runs its own SkipGuard at startup (guard-as-wrapper, D52). Centralized hook registration via bootstrap.py" "Python, PySpark, src/ingestion/"
            refreshSyncedTables = container "Synced Table Refresh" "Operational module that triggers SNAPSHOT refresh on all 37 Lakebase synced tables via Databricks REST API. Uses WorkspaceClient for env-agnostic credentials (PAT/OAuth M2M/CLI profile/runtime context). --catalog and --schema CLI flags validated against IDENTIFIER_RE per CLAUDE.md security rule. Invoked from three contexts: (1) final task in daily Databricks job — depends only on dbt_build after D59 collapsed the 9-way fan-in to a single edge, (2) scripts/dbt_build_and_refresh.py wrapper, (3) background subprocess from Admin API. NOT @workflow-decorated (operational, not analytics)" "Python, requests, databricks-sdk, src/ingestion/refresh_synced_tables.py"
            dbtRunner = container "dbt Runner" "D59 python_wheel_task entry point for the daily Databricks job. Resolves the wheel-bundled dbt_project via importlib.resources (namespace package luxury_lakehouse_dbt_project/), exchanges runtime SP identity for an OAuth M2M bearer token via WorkspaceClient.config.authenticate() (profiles.yml's serverless target consumes via env_var('DATABRICKS_TOKEN')), verifies the project SQL warehouse is RUNNING via warehouses.start_and_wait, and invokes dbtRunner().invoke(['build', ...]). Depends on 9 leaf compute tasks; the downstream refresh_synced_tables task now depends only on dbt_build. NOT @workflow-decorated (dbt has its own lineage/tests). Artifact hash verification helper: verify_artifact_hash() in src/ingestion/utils.py — SHA-256 byte-compare against MLflow tag or UC Volume sidecar, fail-open when absent" "Python, dbt-core, dbt-databricks, src/ingestion/dbt_runner.py"
            evolveEngine = container "Evolve Engine" "LLM-guided evolutionary architecture search (Level 1: config-only, Level 2: code evolution). CLI runner with --resume/--code-evolution, AST allowlist validator (ValidationProfile), evaluator bridge returning EvaluationResult with error artifacts (tracebacks fed back to LLM prompt), PriorityQueue-based BackendPool. Level 2: LLM generates custom_embed()/custom_layers() PyTorch functions, AST-validated then exec'd with restricted globals (__builtins__={}). Defense-in-depth per ADR-001. Pluggable backends: local CUDA, remote SSH, HF Jobs L40S" "Python, OpenEvolve, src/evolve/"
            analyticsLibrary = container "Analytics Library" "Pure-Python domain models (zero I/O). Pitch control (Spearman 2017), xG, xT, VAEP, OBSO, line-breaking, DEFCON, entity resolution, shape graph (construction + inference split), football2vec v2/360, ScoutGPT decoder (256d GPT-style causal, Hong et al. 2025), goalkeeper, coordinates. Split modules all under 800 lines" "Python, NumPy, SciPy, PyTorch, scikit-learn, src/analytics/"
            sharedLibrary = container "Shared Library" "Cross-package constants: IDENTIFIER_RE, DEFAULT_GOLD_SCHEMA, mlflow_model_uri(). Wheel version management: WHEEL_VERSION, WHEEL_FILENAME, WHEEL_BASE_URL, rewrite utilities. Zero external deps. Imported by analytics, ingestion, evolve, deploy scripts, and Taipy Docker image" "Python, src/shared/"
        }

        dbtProject = softwareSystem "dbt Project" "Medallion transformation: 82 models (36 staging, 7 intermediate incl. int_running_score for per-event game state, 39 marts with game_state/possession columns + Kimball-conformed match_key/team_key/player_key FKs per ADR-011), normalize_coordinates / generate_match_key / generate_team_key / generate_player_key macros, data classification meta tags, model contracts, liquid clustering. dbt-owners-dev group owns dev_silver/dev_gold schemas + a +post-hook transfers per-object ownership back to the group on each build, allowing both developer user and ingestion SP to REPLACE objects without lockout" {
            fctWorkflowCosts = container "fct_workflow_costs" "Gold-layer cost attribution. Tasks-driven (lakeflow) with LEFT JOIN billing (~1 day lag). effective_cost_usd = COALESCE(actual, estimated). cold_start_seconds (total pre-pipeline = env + guard), guard_duration_seconds (guard only), entity_count, row_count from warm-tier via workflow_id + temporal window. UI derives environment_setup = cold_start - guard. 90-day rolling window. D59 Option I: reads system.billing + system.lakeflow via definer's-rights views in soccer_analytics.observability (system_billing_usage, system_billing_list_prices, system_lakeflow_job_task_run_timeline) — the system catalog is metastore-managed and cannot be granted via the standard UC grant API, so the views interpose filtered projections owned by an account admin who inherits SELECT via account-users membership" "SQL, dbt" "Database"
            goldModels = container "Gold Models" "35 fact tables + 4 dimension tables (39 total) with enforced contracts, liquid clustering, auto-compaction. Includes 3 pre-aggregated base-case marts (fct_heatmap_agg, fct_vaep_breakdown_agg, fct_gk_actions_detail) added 2026-04-17 to replace comp-only Parallel Seq Scans on >1M-row fact tables with sub-100ms index scans. Kimball-conformed dimensions: dim_matches / dim_competitions / dim_players / dim_teams (ADR-011); fact tables progressively migrated to match_key / team_key / player_key BIGINT FKs across PRs 1-6" "SQL, dbt" "Database"
        }

        # Data stores
        unityCatalog = softwareSystem "Unity Catalog" "Governed Delta Lake storage: bronze (raw), gold (analytics), observability (platform metadata)" "External" {
            bronzeSchema = container "Bronze Schema" "Raw ingested data: events, tracking, SPADL actions, VAEP scores, compute results" "Delta Lake" "Database"
            goldSchema = container "Gold Schema" "Analytics-ready facts and dimensions: 35 fact tables, 4 dim tables, fct_workflow_costs" "Delta Lake" "Database"
            observabilitySchema = container "Observability Schema" "Platform operational metadata: workflow_cost_live (state, duration_seconds, guard_duration_seconds, cost, entity_count, row_count), workflow_import_checksums (HF Hub commit SHA tracking for 5 import guards). Column-mapped Delta table — supports DROP COLUMN for dead-code cleanup. Also hosts 3 definer's-rights views (system_billing_usage, system_billing_list_prices, system_lakeflow_job_task_run_timeline) owned by an account admin, exposing filtered system.* data to fct_workflow_costs via GRANT SELECT to dbt-owners-dev" "Delta Lake" "Database"
        }

        lakebase = softwareSystem "Databricks Lakebase" "PostgreSQL-compatible endpoint syncing 38 Delta Lake tables from Unity Catalog (67 btree/HNSW indexes: 61 btree + 6 HNSW vector: 4x128d + 2x144d)" "External"
        databricksApi = softwareSystem "Databricks REST API" "Workspace REST endpoints: OAuth credential issuance for Lakebase auth, synced table metadata (/api/2.0/database/synced_tables), pipeline update triggers (/api/2.0/pipelines/{id}/updates), and pipeline state polling" "External"
        databricksWorkflows = softwareSystem "Databricks Workflows" "Scheduled DAG orchestration: 28 tasks total: 5 ingest as DAG roots, 2 backfill, 14 compute, 1 HF sync, 2 360 pipeline, 1 entity resolution, 1 tracking metadata, dbt_build python_wheel_task (materializes 39 gold marts, depends on 9 leaves), and final refresh_synced_tables task (depends only on dbt_build — D59 collapsed 9-way fan-in to single edge). Each pipeline task runs its own skip guard at startup, performance-optimized mode (1-4s cold starts), daily 06:00 UTC" "External"
        hfIdentity = softwareSystem "HuggingFace Identity API" "User token validation via /api/whoami-v2. Returns user identity + org memberships with per-org roles (admin/write/read). Used by Admin API for per-request authorization — server-authoritative validation, immediate revocation by token owner, no shared secret persistence" "External"
        hfSpaces = softwareSystem "HuggingFace Spaces" "Docker SDK hosting. Builds from Dockerfile, serves on port 7860" "External"
        hfHub = softwareSystem "HuggingFace Hub" "Hosts 17 models (7 trained: xG v1/v2, VAEP, PSxG, Football2vec v1/v2/360, ScoutGPT; 4 research artefacts: scoutgpt-variant-rope/learnable, scoutgpt-l2-harvest, football2vec-l2-harvest; 5 doc-only method cards: Pitch Control, DEFCON, Off-Ball xT, OBSO+PAUSA, Space Creation; plus build-artifacts wheel host), 19 datasets (incl. training data, ScoutGPT episodes, 360 embeddings, pining-for-the-data SkillCorner mirror), and _workflow_cost.json cost artifacts. Every artifact's README is in-repo at docs/huggingface/{dataset-cards,model-cards}/ and auto-pushed by ingestion.hf_publish (ADR-014); filename == HF repo basename invariant enforced by test_hf_publish_parity.py. Every model card carries an EU AI Act Intended-Use / Non-Use stanza per AI_GOVERNANCE.md (SEC1)" "External"
        hfJobs = softwareSystem "HuggingFace Jobs" "L40S GPU / cpu-basic compute: PEP 723 UV scripts for training (xG v1/v2, VAEP, PSxG, Football2vec v2/360), batch analytics (xT, EPV, OBSO, Space Creation), dataset publishing (freeze frames, xG shots, SPADL/VAEP action values — governed by wf-publish-xg-shots, wf-publish-spadl-vaep), and Evolve Engine candidate evaluation" "External"
        openRouter = softwareSystem "OpenRouter" "LLM API gateway: Claude Sonnet 4 (80%) and Haiku 4.5 (20%) for evolutionary code mutation via OpenAI-compatible endpoint" "External"

        githubActions = softwareSystem "GitHub Actions CI/CD" "Automation surface for platform state, code validation, security scanning, and self-healing. Auth via AWS OIDC federation (for Terraform) and admin PAT stored in GitHub Secrets (for Databricks/Lakebase operations). Each workflow runs in ubuntu-latest with pinned-SHA action versions" {
            terraformApply = container "Terraform Apply" "Auto-apply on push to main when terraform/ files change. Assumes the AWS OIDC role (vars.AWS_OIDC_ROLE_ARN) and uses DATABRICKS_AUTH_TYPE=github-oidc federation for Databricks provider auth. Concurrency-gated to prevent state-lock races" ".github/workflows/terraform-apply.yml"
            terraformPlan = container "Terraform Plan" "Runs on pull_request touching terraform/. Same OIDC federation. Posts plan diff to the PR; human reviews before merge triggers Apply" ".github/workflows/terraform-plan.yml"
            pythonCi = container "Python CI" "Runs on push/PR touching src/, scripts/, workflow-cards/, pyproject.toml, uv.lock, hf_taipy_app/ (added 2026-04-17 to catch requirements-compile drift). Stages: uvx dbt deps (materialize dbt_packages for hatch force-include), uv sync, ruff check, ruff format --check, pyright, pytest, detect-secrets, pip-audit. On main pushes: deploys the wheel to UC Volume via databricks-sdk Files API" ".github/workflows/python-ci.yml"
            dbtCi = container "dbt CI" "Runs on push/PR touching dbt_project/. Uses uv sync --no-install-project + dbt deps. Validates dbt parse + slim CI (state:modified+ --empty) for contract verification without data movement" ".github/workflows/dbt-ci.yml"
            semgrepCi = container "Semgrep SAST" "Runs on every push/PR. Third-party static analysis for common security anti-patterns (OWASP-aligned rulesets). nosemgrep inline exemptions require justification comment" ".github/workflows/semgrep.yml"
            lakebaseGrantsWorkflow = container "Lakebase Grants" "Self-healing SELECT grants for the Taipy SP on Lakebase synced tables (ADR-005). Triggers: schedule cron 07:00 UTC daily (post-Databricks-daily-job), workflow_run chained after Terraform Apply on main, workflow_dispatch for incidents. Applies + verifies grants via scripts/run_lakebase_grants.py. Required config: secrets.DATABRICKS_TOKEN (admin PAT), vars.HF_APP_SP_APPLICATION_ID (from terraform output)" ".github/workflows/lakebase-grants.yml"
            bronzeLiveSchemaCi = container "Bronze Live Schema + Parity" "Runs schedule cron 08:00 UTC daily (post-Lakebase-Grants) + push to main on src/ingestion/** / dbt_project/models/staging/** / sources/** / test_bronze_live_schema.py / test_staging_rowcount_vs_bronze.py / coverage_utils.py / this workflow + workflow_dispatch. Installs databricks-sql-connector ad-hoc (intentionally NOT in pyproject extras — test-only dep). Injects DATABRICKS_HOST/HTTP_PATH (repo vars) + DATABRICKS_TOKEN (secret). Executes test_bronze_live_schema.py (DESCRIBE-based parser-vs-live schema parity for all 5 providers — Mode 1 writer drop detection) + test_staging_rowcount_vs_bronze.py (count(staging) + count(bronze WHERE filter-null) == count(bronze) across 4 WHERE IS NOT NULL filter sites — Mode 4 filter drop detection). G6 of the PR #173 drop-safety sweep: the main python-ci.yml was silently skipping these tests for TWO reasons (pytest.importorskip('databricks.sql') + DATABRICKS_* env vars scoped to dbt deps step only); this workflow fixes both" ".github/workflows/bronze-live-schema.yml"
        }

        # Relationships - users
        analyst -> guiLayer "Browses pages, selects filters, views interactive and static charts" "HTTPS"
        developer -> deployScript "Runs manage_space.py {create|deploy|status|rebuild|teardown} staging" "CLI"
        developer -> deployWheel "Runs deploy_wheel.py to push wheel to UC Volume" "CLI"
        developer -> bumpWheel "Runs bump_wheel.py to sync version after pyproject.toml bump" "CLI"
        developer -> dbtBuildAndRefresh "Runs dbt_build_and_refresh.py to rebuild gold + propagate to Lakebase atomically" "CLI"
        developer -> adminApi "POST /api/cache/clear with HF user token for forced cache invalidation (incident response)" "HTTPS/Bearer"

        # Relationships - Taipy internal
        guiLayer -> templateEngine "Calls build_page() and build_nav() to generate Taipy Markdown" ""
        templateEngine -> sidebarWidgets "Generates filter sections from SidebarWidget data lists" ""
        guiLayer -> stateModules "Binds state variables (including go.Figure) and triggers callbacks" ""
        templateEngine -> stateModules "References state variables in generated content blocks" ""
        stateModules -> queryLayer "Calls typed query functions for all page data" ""
        stateModules -> filterLayer "Fetches filter options, scope labels, and data freshness" ""
        queryLayer -> dbLayer "Executes parameterized SQL via connection pool" "SQL"
        filterLayer -> dbLayer "Queries dimension and fact tables" "SQL"
        stateModules -> renderEngine "Generates static pitch diagrams (Shot Map, Pass Map, Heat Map, Pitch Control)" ""
        stateModules -> pitchControl "Computes pitch control surfaces for tracking data" ""
        renderEngine -> guiLayer "Returns image file paths for template binding" ""
        dbLayer -> configLayer "Reads Lakebase host and endpoint settings" ""

        # Relationships - external (Taipy)
        dbLayer -> lakebase "Queries 38 synced tables via parameterized SQL" "PostgreSQL/SSL"
        dbLayer -> databricksApi "Fetches OAuth tokens for Lakebase auth" "HTTPS/REST"
        stateModules -> workflowCards "Reads YAML manifests on first page load (cached in _cards module variable)" ""
        stateModules -> hfHub "Loads embeddings for similarity search; reads _workflow_cost.json (RUNNING detection) + _cost_history/ (30-day cost aggregation) via 60s TTL" "HTTPS"

        # Relationships - Admin API
        adminApi -> hfIdentity "Validates HF user token per request via /api/whoami-v2" "HTTPS/Bearer"
        adminApi -> refreshSyncedTables "Spawns isolated background subprocess on ?refresh_synced=1 (subprocess, not in-process import)" "subprocess"

        # Relationships - pipeline platform
        ingestionPipelines -> workflowFramework "Decorated with @workflow, lifecycle hooks fire on start/complete/skip/error" ""
        workflowFramework -> workflowCards "Loads YAML cards, attaches metadata to registry entries" ""
        workflowFramework -> costEstimateHook "Dispatches on_start/on_complete/on_skip/on_error to registered hooks" ""
        ingestionPipelines -> analyticsLibrary "Imports domain logic (xG, xT, pitch control, OBSO)" ""
        ingestionPipelines -> sharedLibrary "Imports constants (catalog, schema, MLflow URI builder)" ""
        analyticsLibrary -> sharedLibrary "Imports IDENTIFIER_RE for array_utils" ""
        costEstimateHook -> sharedLibrary "Imports COST_TABLE_NAME and schema constants" ""
        ingestionPipelines -> bronzeSchema "Writes compute results to Delta tables" "PySpark/Delta"
        costEstimateHook -> observabilitySchema "MERGE run state + cost estimates to workflow_cost_live" "PySpark/Delta"
        databricksWorkflows -> ingestionPipelines "Executes 26 pipeline tasks (5 ingest as DAG roots)" "Databricks Jobs API"
        databricksWorkflows -> dbtRunner "Invokes dbt_build python_wheel_task after 9 leaf computes" "Databricks Jobs API / python_wheel_task"
        databricksWorkflows -> refreshSyncedTables "Final task: single-edge dependency on dbt_build" "Databricks Jobs API / python_wheel_task"
        dbtRunner -> dbtProject "Invokes dbtRunner().invoke() with build args against wheel-bundled dbt_project/ (serverless target)" "dbt-core"
        dbtRunner -> databricksApi "Resolves warehouse, starts if STOPPED, exchanges runtime SP identity for OAuth M2M token" "HTTPS/REST"
        dbtRunner -> sharedLibrary "Imports IDENTIFIER_RE and module path conventions" ""
        ingestionPipelines -> analyticsLibrary "Also imports verify_artifact_hash() via ingestion.utils for SHA-256 integrity check of model loads (SEC2)" ""
        ingestionPipelines -> guardRegistry "Each pipeline calls timed_check(skip_guard, ...) at startup (guard-as-wrapper)" ""
        guardRegistry -> hfHub "check_hf_dataset_freshness() fetches commit SHA via HfApi.repo_info() for 5 import guards" "HTTPS/HF API"
        guardRegistry -> observabilitySchema "Reads/writes workflow_import_checksums (SHA comparison + MERGE write-back)" "PySpark/Delta"
        refreshSyncedTables -> databricksApi "Triggers SNAPSHOT pipeline updates and polls pipeline state for 38 synced tables" "HTTPS/REST"
        refreshSyncedTables -> sharedLibrary "Imports IDENTIFIER_RE for catalog/schema validation" ""
        dbtBuildAndRefresh -> refreshSyncedTables "Subprocess invocation after successful dbt build: python -m ingestion.refresh_synced_tables --wait" "subprocess"

        # Relationships - Evolve Engine
        developer -> evolveEngine "Runs 'uv run evolve --target scoutgpt'" "CLI"
        evolveEngine -> analyticsLibrary "Imports ScoutGPT decoder for candidate training" ""
        evolveEngine -> openRouter "Sends LLM mutation prompts via OpenAI-compatible API" "HTTPS/REST"
        evolveEngine -> hfHub "Downloads ScoutGPT training data" "HTTPS/HF API"
        evolveEngine -> workflowCards "Registered as wf-evolve-scoutgpt" ""
        evolveEngine -> sharedLibrary "Imports WHEEL_BASE_URL for worker script PEP 723 header" ""
        evolveEngine -> hfJobs "Submits candidate training jobs via run_uv_job" "HTTPS/HF API"

        # Relationships - HF Jobs
        hfJobs -> analyticsLibrary "Imports from wheel (luxury-lakehouse/build-artifacts)" "pip/HTTPS"
        hfJobs -> hfCostRecorder "Records cost via start()/complete()/fail()/skip()" ""
        hfCostRecorder -> hfHub "Writes _workflow_cost.json (live status) + _cost_history/{job_id}.json (per-run history)" "HTTPS/HF API"
        hfJobs -> hfHub "Publishes trained models and computed grids" "HTTPS/HF API"
        hfJobs -> hfPublish "Imports upload_hf_readme + get_hf_card_path from wheel; every publisher pushes its README as the final step" "pip/HTTPS"

        # Relationships - HF Publish helper (ADR-014 producer-side documentation delivery)
        ingestionPipelines -> hfPublish "Workflow-task publishers (export_shots_on_target, export_scoutgpt_training_data, export_embeddings_training_data, prepare_360_training_data) call upload_hf_readme after the data upload" ""
        hfPublish -> hfHub "HfApi.upload_file(path_in_repo='README.md', repo_type=<dataset|model|space>)" "HTTPS/HF API"
        developer -> hfPublish "scripts/publish_hf_cards.py --org | --orphans | --name --kind for manual pushes (org Space + 6 payload-less method/research cards)" "CLI"

        # Relationships - dbt
        dbtProject -> unityCatalog "Reads bronze, writes gold via Databricks SQL" "Delta Lake"
        fctWorkflowCosts -> observabilitySchema "Post-hook: cleans up redundant warm-tier rows from workflow_cost_live" "SQL DELETE"

        # Relationships - deploy
        deployScript -> hfSpaces "upload_folder() with ignore_patterns + delete_patterns for full sync" "HTTPS/HF API"
        bumpWheel -> sharedLibrary "Imports rewrite_wheel_url, read_pyproject_version" ""
        deployWheel -> sharedLibrary "Imports WHEEL_FILENAME, WHEEL_REPO" ""
        deployWheel -> hfHub "Downloads wheel from build-artifacts" "HTTPS/HF API"
        deployWheel -> bronzeSchema "Uploads wheel to /Volumes/{catalog}/bronze/libs/" "Databricks SDK"
        taipyApp -> analyticsLibrary "Installs luxury-lakehouse wheel at Docker build time (analytics, shared packages)" "pip/wheel"
        hfSpaces -> taipyApp "Builds Docker image, runs Taipy GUI on port 7860" "Docker"
        analyst -> hfSpaces "Accesses luxury-lakehouse/soccer-analytics-app" "HTTPS"

        # Relationships - GitHub Actions CI/CD
        terraformApply -> databricksApi "Applies Databricks provider resources (workspace, catalog, workflows, synced_tables, SPs)" "HTTPS/github-oidc"
        terraformPlan -> databricksApi "Reads state for diff rendering" "HTTPS/github-oidc"
        pythonCi -> bronzeSchema "Deploys wheel to /Volumes/{catalog}/bronze/libs/ on main pushes (databricks-sdk Files API)" "HTTPS"
        bronzeLiveSchemaCi -> bronzeSchema "DESCRIBE + count(*) parity tests against live Delta bronze tables across 5 providers (Mode 1 + Mode 4 drop detection)" "Databricks SQL/Thrift"
        bronzeLiveSchemaCi -> databricksApi "Warehouse auto-resume + Thrift query execution via databricks-sql-connector" "HTTPS/Thrift"
        lakebaseGrantsWorkflow -> databricksApi "Obtains Lakebase PG credential via /api/2.0/postgres/credentials using admin PAT" "HTTPS/REST"
        lakebaseGrantsWorkflow -> lakebase "GRANT SELECT on synced tables for Taipy SP; verifies coverage against SYNCED_TABLES inventory" "PostgreSQL/SSL"
        terraformApply -> lakebaseGrantsWorkflow "workflow_run trigger: TF changes can recreate synced tables, grants workflow re-applies" "GitHub Actions event"
        developer -> pythonCi "Opens PR / pushes to main" "git push"
        developer -> dbtCi "Opens PR / pushes to main with dbt_project/** changes" "git push"
        developer -> terraformPlan "Opens PR with terraform/** changes (plan posted to PR)" "git push"
        developer -> terraformApply "Merges PR to main (auto-apply)" "git push"

        # Deployment environment
        production = deploymentEnvironment "Production" {
            deploymentNode "HuggingFace Infrastructure" "Managed container hosting" "Docker SDK" {
                deploymentNode "cpu-basic" "Free tier, sleep after 48h" "2 vCPU, 16 GB RAM" {
                    appInstance = containerInstance guiLayer
                    healthEndpoint = infrastructureNode "Health Endpoint" "/health route, background DB connectivity check every 60s"
                }
            }
            deploymentNode "Databricks Cloud" "US East 1" "AWS" {
                deploymentNode "Lakebase Autoscaling" "0.5-4 CU, scale-to-zero" "PostgreSQL 17" {
                    lakebaseNode = infrastructureNode "Lakebase Endpoint" "PostgreSQL-compatible"
                }
                deploymentNode "Serverless Compute" "Auto-scaling, 16 GB driver" "Python 3.10" {
                    pipelineInstance = containerInstance ingestionPipelines
                }
                deploymentNode "Unity Catalog" "Managed Delta Lake" "Delta Lake 3.x" {
                    bronzeInstance = containerInstance bronzeSchema
                    goldInstance = containerInstance goldSchema
                    obsInstance = containerInstance observabilitySchema
                }
            }
            deploymentNode "HuggingFace Jobs" "Ephemeral GPU/CPU containers" "Docker" {
                deploymentNode "cpu-basic ($0.01/hr)" "16 GB RAM" "Python 3.10, UV" {
                    cpuJobInstance = infrastructureNode "xT, EPV, xG v1, VAEP training"
                }
                deploymentNode "l40sx1 ($1.80/hr)" "62 GB RAM, L40S 48 GB VRAM" "Python 3.10, UV" {
                    gpuJobInstance = infrastructureNode "PSxG, xG v2, Football2vec v2/360, OBSO, Space Creation, Evolve candidates"
                }
            }
        }
    }

    views {
        systemContext taipyApp "SystemContext" {
            include *
            include deployPipeline
            include pipelinePlatform
            include dbtProject
            include unityCatalog
            include databricksWorkflows
            include hfJobs
            include openRouter
            include hfIdentity
            include githubActions
            autoLayout
        }

        container githubActions "CIContainers" {
            include *
            include databricksApi
            include lakebase
            include bronzeSchema
            include developer
            autoLayout
        }

        container pipelinePlatform "PipelineContainers" {
            include *
            include databricksWorkflows
            include hfJobs
            include hfHub
            include observabilitySchema
            include bronzeSchema
            include openRouter
            include developer
            autoLayout
        }

        container taipyApp "TaipyContainers" {
            include *
            include lakebase
            include databricksApi
            include hfHub
            include hfIdentity
            include refreshSyncedTables
            include analyticsLibrary
            include sharedLibrary
            autoLayout
        }

        container unityCatalog "DataStores" {
            include *
            include ingestionPipelines
            include costEstimateHook
            include fctWorkflowCosts
            include dbtProject
            autoLayout
        }

        dynamic pipelinePlatform "CostTracking" {
            ingestionPipelines -> workflowFramework "Pipeline decorated with @workflow starts"
            workflowFramework -> costEstimateHook "Dispatches on_start hook"
            costEstimateHook -> observabilitySchema "MERGE: RUNNING state, cost=0"
            ingestionPipelines -> workflowFramework "Pipeline completes"
            workflowFramework -> costEstimateHook "Dispatches on_complete hook"
            costEstimateHook -> observabilitySchema "MERGE: COMPLETED state, duration × rate"
            autoLayout
        }

        dynamic taipyApp "FilterCascade" {
            analyst -> guiLayer "Selects competition from sidebar dropdown"
            guiLayer -> stateModules "Fires on_competition_change callback"
            stateModules -> filterLayer "Fetches teams, matches, players for competition"
            filterLayer -> dbLayer "Queries dim tables with recursive CTE"
            dbLayer -> lakebase "SELECT from dim_competitions_synced"
            stateModules -> guiLayer "Updates team_lov, match_lov, player_lov state"
            stateModules -> renderEngine "Re-renders chart with new data scope"
            renderEngine -> guiLayer "Returns new image path (cache-busted)"
            autoLayout
        }

        dynamic pipelinePlatform "GuardAsWrapper" {
            databricksWorkflows -> ingestionPipelines "Job starts, 5 ingest tasks launch as DAG roots (no gate)"
            ingestionPipelines -> guardRegistry "Each main() calls timed_check(skip_guard, ...) at startup"
            ingestionPipelines -> workflowFramework "WorkflowSkippedError caught by @workflow decorator"
            workflowFramework -> costEstimateHook "Dispatches on_skip to CostEstimateHook"
            costEstimateHook -> observabilitySchema "MERGE SKIPPED state + entity_count to workflow_cost_live"
            autoLayout
        }

        dynamic pipelinePlatform "EvolveLevel2" {
            developer -> evolveEngine "Launch --code-evolution run"
            evolveEngine -> openRouter "Generate candidate (config + custom_embed)"
            evolveEngine -> analyticsLibrary "AST-validate, then exec with restricted globals, monkey-patch _embed"
            evolveEngine -> hfJobs "Dispatch to L40S for training"
            autoLayout
        }

        dynamic pipelinePlatform "DailyJobHardening" {
            databricksWorkflows -> ingestionPipelines "9 leaf compute tasks run (guard-as-wrapper, @workflow-decorated)"
            databricksWorkflows -> dbtRunner "dbt_build python_wheel_task launches after 9 leaves complete"
            dbtRunner -> databricksApi "WorkspaceClient.config.authenticate() exchanges SP identity for OAuth M2M bearer"
            dbtRunner -> dbtProject "dbtRunner().invoke(['build', ...]) — 39 marts rebuilt, 600+ tests run"
            dbtProject -> observabilitySchema "fct_workflow_costs reads system.billing + system.lakeflow via definer's-rights views"
            databricksWorkflows -> refreshSyncedTables "Final task runs with single-edge dependency on dbt_build"
            refreshSyncedTables -> databricksApi "Triggers SNAPSHOT on 37 Lakebase synced tables; polls until COMPLETE"
            autoLayout
        }

        deployment taipyApp "Production" "Deployment" {
            include *
            autoLayout
        }

        styles {
            element "Person" {
                shape Person
                background #08427B
                color #ffffff
            }
            element "Software System" {
                background #1168BD
                color #ffffff
            }
            element "External" {
                background #999999
                color #ffffff
            }
            element "Container" {
                background #438DD5
                color #ffffff
            }
            element "Database" {
                shape Cylinder
            }
            element "Component" {
                background #85BBF0
                color #000000
            }
        }
    }

}
