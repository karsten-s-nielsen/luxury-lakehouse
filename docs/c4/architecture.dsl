workspace "Luxury Lakehouse" "Serverless soccer analytics platform: 35 AI/ML workflows, three-tier cost tracking, 16-page Taipy dashboard on HF Spaces, Databricks Lakebase. Evolve Engine (AlphaEvolve-style LLM-guided architecture search via OpenEvolve). Four-pillar GK evaluation (PSxG, distribution xT, collection, sweeper). Football2vec 360-enriched embeddings (144d transformer + Deep Sets). ScoutGPT decoder (256d GPT-style, player-conditioned, Hong et al. 2025). Shape graph formation detection (Sotudeh 2026), Football2vec v2 transformer with adversarial debiasing (128d). Semgrep SAST, ruff S (bandit), and import-linter boundary enforcement in CI." {

    model {
        analyst = person "Soccer Analyst" "Coaches, scouts, and analysts exploring match and player data"
        developer = person "Developer" "Deploys application updates and triggers pipeline runs"

        taipyApp = softwareSystem "Taipy Dashboard" "Interactive soccer analytics application with 16 pages: 15 analytics + AI/ML Workflows operations dashboard" {
            guiLayer = container "Taipy GUI" "Root template with sidebar navigation, glossary panels, conditional footer (show_site_footer), and page routing" "Python, Taipy 4.1"
            templateEngine = container "Template Engine" "Three layout builders (standard, sub-view, dashboard) dispatched by build_page(). Dashboard layout: StatCard stats bar + ll-dashboard-scroll viewport container. Typed dataclasses: PageConfig, SubView, ContentBlock (table_cell_class_name for per-cell CSS), ContentRow, SidebarWidget, Metric, Citation, StatCard (detail_html for content-provider iframes)" "Python, frozen dataclasses"
            sidebarWidgets = container "Sidebar Widgets" "Centralized filter cascade with progressive disclosure, view-dependent visibility, change_delay debounce, and absolute-positioned help tooltips" "Python, Taipy Markdown"
            stateModules = container "State Modules" "Per-page state variables, callbacks, chart rendering (15 modules, SQL-free). Delegates all data fetching to Query Layer. Static charts via mplsoccer PNG. Interactive charts via Plotly. Workflows split: workflows.py orchestrator + workflows_dag.py (Cytoscape.js DAG) + workflows_stats.py (card loading via WorkflowCard from wheel, cost computation). 2-min auto-refresh timer, WCAG shape markers, DISABLED task filtering" "Python, pandas, mplsoccer, Plotly, Cytoscape.js"
            queryLayer = container "Query Layer" "Centralized SQL: 12 modules (shots, passes, tracking, match, players, team_shape, defensive, goalkeepers, tactical_positions, workflows, common). All parameterized queries with TTL cache. State modules are SQL-free after extraction. Column name constants serve as read-side contract documentation" "Python, psycopg2, src/queries/"
            filterLayer = container "Filter Layer" "Shared filter queries with TTL cache, scope labels, data freshness, and embedding player search. Uses recursive CTE for DISTINCT alternatives" "Python, psycopg2"
            dbLayer = container "DB Layer" "OAuth token management, connection pooling, parameterized query execution, /health endpoint with background DB connectivity check" "Python, psycopg2, Databricks SDK"
            renderEngine = container "Render Engine" "Matplotlib/mplsoccer figure-to-PNG with cache-busting paths for static pitch diagrams" "Python, matplotlib, mplsoccer"
            pitchControl = container "Pitch Control Engine" "Physics-based (Spearman 2017) and Voronoi pitch control surface computation" "Python, NumPy, SciPy"
            configLayer = container "Config" "Pydantic settings from environment variables and .env file with identifier validation" "Python, pydantic-settings"
        }

        deployPipeline = softwareSystem "Deploy Pipeline" "Deployment scripts for Taipy app and analytics wheel" {
            deployScript = container "manage_space.py" "CLI tool: full Space lifecycle -- create, deploy, status, rebuild, teardown. Pre-flight checks, upload_folder with ignore/delete patterns, secret management, polling." "Python, huggingface_hub"
            deployWheel = container "deploy_wheel.py" "Downloads wheel from HF Hub build-artifacts, uploads to UC Volume /Volumes/{catalog}/bronze/libs/, post-upload size verification" "Python, huggingface_hub, databricks-sdk"
        }

        pipelinePlatform = softwareSystem "AI/ML Pipeline Platform" "35 workflow-card-registered compute pipelines with @workflow decorators, centralized bootstrap hooks, lifecycle tracking, three-tier cost tracking, YAML manifests, Evolve Engine for LLM-guided architecture search, and import-linter boundary enforcement" {
            workflowFramework = container "Workflow Framework" "Registry, @workflow decorator, WorkflowContext, lifecycle runner with on_start/on_complete/on_skip/on_error dispatch. Circular dependency broken via _set_runner injection" "Python, src/workflows/"
            workflowCards = container "Workflow Cards" "35 YAML manifests defining inputs, outputs, deps, execution config, cost estimates, academic provenance" "YAML, workflow-cards/" "Database"
            costEstimateHook = container "CostEstimateHook" "Lifecycle hook writing run state + cost estimates to workflow_cost_live Delta table via MERGE. Centralized registration via bootstrap_hooks()" "Python, PySpark, Delta, src/ingestion/cost_hook.py"
            hfCostRecorder = container "HFJobsCostRecorder" "Standalone cost recorder for HF Jobs scripts. Writes _workflow_cost.json (live status) and _cost_history/{job_id}.json (per-run history) to HF Hub repos. 90-day auto-pruning" "Python, huggingface_hub, src/ingestion/hf_jobs_cost.py"
            ingestionPipelines = container "Compute Pipelines" "30 @workflow-decorated Databricks pipelines (all instrumented): 5 raw ingestors (StatsBomb, Metrica, Wyscout, IDSSE, SkillCorner), 14 compute (xG, VAEP, DEFCON 360+tracking, pitch control, xT, OBSO/PAUSA, entity resolution, line-breaking 360+tracking, formations EFPI+shape graph, embeddings v1/v2, model validation), 5 HF export/import (shots, OBSO, PSxG, space creation, 360 training data), elastic sync, cost sync. Centralized hook registration via bootstrap.py" "Python, PySpark, src/ingestion/"
            evolveEngine = container "Evolve Engine" "LLM-guided evolutionary architecture search (Level 1: config-only, Level 2: code evolution). CLI runner with --resume/--code-evolution, AST allowlist validator (ValidationProfile), self-contained evaluator bridge, PriorityQueue-based BackendPool. Level 2: LLM generates custom_embed()/custom_layers() PyTorch functions, AST-validated then exec'd with restricted globals (__builtins__={}). Defense-in-depth per ADR-001. Pluggable backends: local CUDA, remote SSH, HF Jobs L40S" "Python, OpenEvolve, src/evolve/"
            analyticsLibrary = container "Analytics Library" "Pure-Python domain models (zero I/O). Pitch control (Spearman 2017), xG, xT, VAEP, OBSO, line-breaking, DEFCON, entity resolution, shape graph (construction + inference split), football2vec v2/360, ScoutGPT decoder (256d GPT-style causal, Hong et al. 2025), goalkeeper, coordinates. Split modules all under 800 lines" "Python, NumPy, SciPy, PyTorch, scikit-learn, src/analytics/"
            sharedLibrary = container "Shared Library" "Cross-package constants: IDENTIFIER_RE, DEFAULT_GOLD_SCHEMA, mlflow_model_uri(). Zero external deps. Imported by analytics, ingestion, and Taipy Docker image" "Python, src/shared/"
        }

        dbtProject = softwareSystem "dbt Project" "Medallion transformation: 65 models (27 staging, 5 intermediate, 33 marts), normalize_coordinates macro, data classification meta tags, model contracts, liquid clustering" {
            fctWorkflowCosts = container "fct_workflow_costs" "Gold-layer cost attribution from system.billing.usage × list_prices, proportional per-task by execution_duration. 90-day rolling window. Post-hook cleanup of warm-tier rows" "SQL, dbt" "Database"
            goldModels = container "Gold Models" "29 fact tables + 4 dimension tables with enforced contracts, liquid clustering, auto-compaction" "SQL, dbt" "Database"
        }

        # Data stores
        unityCatalog = softwareSystem "Unity Catalog" "Governed Delta Lake storage: bronze (raw), gold (analytics), observability (platform metadata)" "External" {
            bronzeSchema = container "Bronze Schema" "Raw ingested data: events, tracking, SPADL actions, VAEP scores, compute results" "Delta Lake" "Database"
            goldSchema = container "Gold Schema" "Analytics-ready facts and dimensions: 29 fact tables, 4 dim tables, fct_workflow_costs" "Delta Lake" "Database"
            observabilitySchema = container "Observability Schema" "Platform operational metadata: workflow_cost_live (warm/hot cost tracking)" "Delta Lake" "Database"
        }

        lakebase = softwareSystem "Databricks Lakebase" "PostgreSQL-compatible endpoint syncing 36 Delta Lake tables from Unity Catalog (56 btree/HNSW indexes: 50 btree + 6 HNSW vector: 4x128d + 2x144d)" "External"
        databricksApi = softwareSystem "Databricks REST API" "OAuth credential endpoint for Lakebase authentication" "External"
        databricksWorkflows = softwareSystem "Databricks Workflows" "Scheduled DAG orchestration: 32 tasks (6 ingest, 1 backfill, 14 compute, 5 HF export/import, 2 360 pipeline, 1 entity resolution, 1 validation, 1 cost sync, 1 tracking metadata), daily 06:00 UTC" "External"
        hfSpaces = softwareSystem "HuggingFace Spaces" "Docker SDK hosting. Builds from Dockerfile, serves on port 7860" "External"
        hfHub = softwareSystem "HuggingFace Hub" "Hosts 7 models (incl. football2vec-v2, football2vec-360, PSxG), 18 datasets (incl. training data, ScoutGPT episodes, 360 embeddings), build-artifacts wheel, and _workflow_cost.json cost artifacts" "External"
        hfJobs = softwareSystem "HuggingFace Jobs" "L40S GPU compute: 12 PEP 723 UV scripts for training (xG v1/v2, VAEP, PSxG, Football2vec v2/360), batch analytics (xT, EPV, OBSO, Space Creation), dataset publishing (freeze frames, xG shots), and Evolve Engine candidate evaluation" "External"
        openRouter = softwareSystem "OpenRouter" "LLM API gateway: Claude Sonnet 4 (80%) and Haiku 4.5 (20%) for evolutionary code mutation via OpenAI-compatible endpoint" "External"

        # Relationships - users
        analyst -> guiLayer "Browses pages, selects filters, views interactive and static charts" "HTTPS"
        developer -> deployScript "Runs manage_space.py {create|deploy|status|rebuild|teardown} staging" "CLI"
        developer -> deployWheel "Runs deploy_wheel.py to push wheel to UC Volume" "CLI"

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
        dbLayer -> lakebase "Queries 36 synced tables via parameterized SQL" "PostgreSQL/SSL"
        dbLayer -> databricksApi "Fetches OAuth tokens for Lakebase auth" "HTTPS/REST"
        stateModules -> workflowCards "Reads YAML manifests on first page load (cached in _cards module variable)" ""
        stateModules -> hfHub "Loads embeddings for similarity search; reads _workflow_cost.json (RUNNING detection) + _cost_history/ (30-day cost aggregation) via 60s TTL" "HTTPS"

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
        databricksWorkflows -> ingestionPipelines "Schedules and executes 31 pipeline tasks" "Databricks Jobs API"

        # Relationships - Evolve Engine
        developer -> evolveEngine "Runs 'uv run evolve --target scoutgpt'" "CLI"
        evolveEngine -> analyticsLibrary "Imports ScoutGPT decoder for candidate training" ""
        evolveEngine -> openRouter "Sends LLM mutation prompts via OpenAI-compatible API" "HTTPS/REST"
        evolveEngine -> hfHub "Downloads ScoutGPT training data" "HTTPS/HF API"
        evolveEngine -> workflowCards "Registered as wf-evolve-scoutgpt" ""
        evolveEngine -> hfJobs "Submits candidate training jobs via run_uv_job" "HTTPS/HF API"

        # Relationships - HF Jobs
        hfJobs -> analyticsLibrary "Imports from wheel (luxury-lakehouse/build-artifacts)" "pip/HTTPS"
        hfJobs -> hfCostRecorder "Records cost via start()/complete()/fail()/skip()" ""
        hfCostRecorder -> hfHub "Writes _workflow_cost.json (live status) + _cost_history/{job_id}.json (per-run history)" "HTTPS/HF API"
        hfJobs -> hfHub "Publishes trained models and computed grids" "HTTPS/HF API"

        # Relationships - dbt
        dbtProject -> unityCatalog "Reads bronze, writes gold via Databricks SQL" "Delta Lake"
        fctWorkflowCosts -> observabilitySchema "Post-hook: cleans up redundant warm-tier rows from workflow_cost_live" "SQL DELETE"

        # Relationships - deploy
        deployScript -> hfSpaces "upload_folder() with ignore_patterns + delete_patterns for full sync" "HTTPS/HF API"
        deployWheel -> hfHub "Downloads wheel from build-artifacts" "HTTPS/HF API"
        deployWheel -> bronzeSchema "Uploads wheel to /Volumes/{catalog}/bronze/libs/" "Databricks SDK"
        taipyApp -> analyticsLibrary "Installs luxury-lakehouse wheel at Docker build time (analytics, shared packages)" "pip/wheel"
        hfSpaces -> taipyApp "Builds Docker image, runs Taipy GUI on port 7860" "Docker"
        analyst -> hfSpaces "Accesses luxury-lakehouse/soccer-analytics-app" "HTTPS"

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

        dynamic pipelinePlatform "EvolveLevel2" {
            developer -> evolveEngine "Launch --code-evolution run"
            evolveEngine -> openRouter "Generate candidate (config + custom_embed)"
            evolveEngine -> analyticsLibrary "AST-validate, then exec with restricted globals, monkey-patch _embed"
            evolveEngine -> hfJobs "Dispatch to L40S for training"
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
