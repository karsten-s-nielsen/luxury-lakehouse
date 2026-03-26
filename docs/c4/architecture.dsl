workspace "Luxury Lakehouse" "Serverless soccer analytics platform: 17 AI/ML workflows, three-tier cost tracking, 14-page Taipy dashboard on HF Spaces, Databricks Lakebase." {

    model {
        analyst = person "Soccer Analyst" "Coaches, scouts, and analysts exploring match and player data"
        developer = person "Developer" "Deploys application updates and triggers pipeline runs"

        taipyApp = softwareSystem "Taipy Dashboard" "Interactive soccer analytics application with 14 pages: 13 analytics + AI/ML Workflows operations dashboard" {
            guiLayer = container "Taipy GUI" "Root template with sidebar navigation, glossary panels, conditional footer (show_site_footer), and page routing" "Python, Taipy 4.1"
            templateEngine = container "Template Engine" "Three layout builders (standard, sub-view, dashboard) dispatched by build_page(). Dashboard layout: StatCard stats bar + ll-dashboard-scroll viewport container. Typed dataclasses: PageConfig, SubView, ContentBlock (table_cell_class_name for per-cell CSS), ContentRow, SidebarWidget, Metric, Citation, StatCard (detail_html for content-provider iframes)" "Python, frozen dataclasses"
            sidebarWidgets = container "Sidebar Widgets" "Centralized filter cascade with progressive disclosure, view-dependent visibility, change_delay debounce, and absolute-positioned help tooltips" "Python, Taipy Markdown"
            stateModules = container "State Modules" "Per-page state variables, callbacks, data fetching, chart rendering (13 modules). Static charts via mplsoccer PNG. Interactive charts via Plotly. DAG via Cytoscape.js iframe. Workflows: YAML cards cached on first load (_cards module-level), DAG HTML cached for filter resets (_unfiltered_dag_html), TTL-cached Lakebase queries (3600s cold / 1800s warm+jobs), lazy WorkspaceClient singleton for Jobs API, cell_class_name callbacks with WCAG shape markers, RawHtml stat details via content provider. Detail drilldown deferred (page_md = dashboard only)" "Python, pandas, mplsoccer, Plotly, Cytoscape.js"
            filterLayer = container "Filter Layer" "Shared filter queries with TTL cache, scope labels, data freshness, and embedding player search" "Python, psycopg2"
            dbLayer = container "DB Layer" "OAuth token management, connection pooling, parameterized query execution" "Python, psycopg2, Databricks SDK"
            renderEngine = container "Render Engine" "Matplotlib/mplsoccer figure-to-PNG with cache-busting paths for static pitch diagrams" "Python, matplotlib, mplsoccer"
            pitchControl = container "Pitch Control Engine" "Physics-based (Spearman 2017) and Voronoi pitch control surface computation" "Python, NumPy, SciPy"
            configLayer = container "Config" "Pydantic settings from environment variables and .env file with identifier validation" "Python, pydantic-settings"
        }

        deployPipeline = softwareSystem "Deploy Pipeline" "Deployment scripts for Taipy app and analytics wheel" {
            deployScript = container "deploy_taipy.py" "CLI tool: pre-flight checks, upload_folder with ignore/delete patterns, post-upload verification, dry-run mode" "Python, huggingface_hub"
            deployWheel = container "deploy_wheel.py" "Downloads wheel from HF Hub build-artifacts, uploads to UC Volume /Volumes/{catalog}/bronze/libs/, post-upload size verification" "Python, huggingface_hub, databricks-sdk"
        }

        pipelinePlatform = softwareSystem "AI/ML Pipeline Platform" "17 workflow-card-registered compute pipelines with @workflow decorators, lifecycle hooks, three-tier cost tracking, and YAML manifests" {
            workflowFramework = container "Workflow Framework" "Registry, @workflow decorator, WorkflowContext, lifecycle runner with on_start/on_complete/on_skip/on_error dispatch" "Python, src/workflows/"
            workflowCards = container "Workflow Cards" "16 YAML manifests defining inputs, outputs, deps, execution config, cost estimates, academic provenance" "YAML, workflow-cards/" "Database"
            costEstimateHook = container "CostEstimateHook" "Lifecycle hook writing run state + cost estimates to workflow_cost_live Delta table via MERGE. Configurable rate via DATABRICKS_SERVERLESS_RATE_USD env var" "Python, PySpark, Delta, src/ingestion/cost_hook.py"
            hfCostRecorder = container "HFJobsCostRecorder" "Standalone cost recorder for HF Jobs scripts. Writes _workflow_cost.json to HF Hub repos with RUNNING→COMPLETED state transitions" "Python, huggingface_hub, src/analytics/cost.py"
            ingestionPipelines = container "Compute Pipelines" "12 @workflow-decorated Databricks pipelines: xG, VAEP, DEFCON, pitch control, xT, OBSO/PAUSA, entity resolution, line-breaking, model validation" "Python, PySpark, src/ingestion/"
            analyticsLibrary = container "Analytics Library" "Pure-Python domain models: pitch control (Spearman 2017), xG (calibrated XGBoost), xT (Markov chain), VAEP (socceraction), OBSO (Fernandez & Bornn), line-breaking (Ward clustering), DEFCON (Kim et al. 2025), entity resolution (TF-IDF + rapidfuzz), augmentation (TacticAI)" "Python, NumPy, SciPy, src/analytics/"
        }

        dbtProject = softwareSystem "dbt Project" "Medallion transformation: 36 models (staging→intermediate→marts), 464 data tests, model contracts, liquid clustering" {
            fctWorkflowCosts = container "fct_workflow_costs" "Gold-layer cost attribution from system.billing.usage × list_prices, proportional per-task by execution_duration. 90-day rolling window. Post-hook cleanup of warm-tier rows" "SQL, dbt" "Database"
            goldModels = container "Gold Models" "21 fact tables + 3 dimension tables with enforced contracts, liquid clustering, auto-compaction" "SQL, dbt" "Database"
        }

        # Data stores
        unityCatalog = softwareSystem "Unity Catalog" "Governed Delta Lake storage: bronze (raw), gold (analytics), observability (platform metadata)" "External" {
            bronzeSchema = container "Bronze Schema" "Raw ingested data: events, tracking, SPADL actions, VAEP scores, compute results" "Delta Lake" "Database"
            goldSchema = container "Gold Schema" "Analytics-ready facts and dimensions: 21 fact tables, 3 dim tables, fct_workflow_costs" "Delta Lake" "Database"
            observabilitySchema = container "Observability Schema" "Platform operational metadata: workflow_cost_live (warm/hot cost tracking)" "Delta Lake" "Database"
        }

        lakebase = softwareSystem "Databricks Lakebase" "PostgreSQL-compatible endpoint syncing 26 Delta Lake tables from Unity Catalog (41 btree + 4 HNSW vector indexes)" "External"
        databricksApi = softwareSystem "Databricks REST API" "OAuth credential endpoint for Lakebase authentication" "External"
        databricksWorkflows = softwareSystem "Databricks Workflows" "Scheduled DAG orchestration: 19 tasks (5 ingest + 13 compute + 1 validation), daily 06:00 UTC" "External"
        hfSpaces = softwareSystem "HuggingFace Spaces" "Docker SDK hosting. Builds from Dockerfile, serves on port 7860" "External"
        hfHub = softwareSystem "HuggingFace Hub" "Hosts 4 models, 7 datasets, build-artifacts wheel, and _workflow_cost.json cost artifacts" "External"
        hfJobs = softwareSystem "HuggingFace Jobs" "GPU/CPU compute: 7 PEP 723 UV scripts for training (xG, VAEP) and batch analytics (xT, EPV, OBSO, Space Creation)" "External"

        # Relationships - users
        analyst -> guiLayer "Browses pages, selects filters, views interactive and static charts" "HTTPS"
        developer -> deployScript "Runs deploy_taipy.py staging [--dry-run]" "CLI"
        developer -> deployWheel "Runs deploy_wheel.py to push wheel to UC Volume" "CLI"

        # Relationships - Taipy internal
        guiLayer -> templateEngine "Calls build_page() and build_nav() to generate Taipy Markdown" ""
        templateEngine -> sidebarWidgets "Generates filter sections from SidebarWidget data lists" ""
        guiLayer -> stateModules "Binds state variables (including go.Figure) and triggers callbacks" ""
        templateEngine -> stateModules "References state variables in generated content blocks" ""
        stateModules -> filterLayer "Fetches filter options, scope labels, and data freshness" ""
        filterLayer -> dbLayer "Queries dimension and fact tables" "SQL"
        stateModules -> dbLayer "Executes parameterized SQL queries for page data" ""
        stateModules -> renderEngine "Generates static pitch diagrams (Shot Map, Pass Map, Heat Map, Pitch Control)" ""
        stateModules -> pitchControl "Computes pitch control surfaces for tracking data" ""
        renderEngine -> guiLayer "Returns image file paths for template binding" ""
        dbLayer -> configLayer "Reads Lakebase host and endpoint settings" ""

        # Relationships - external (Taipy)
        dbLayer -> lakebase "Queries 26 synced tables via parameterized SQL" "PostgreSQL/SSL"
        dbLayer -> databricksApi "Fetches OAuth tokens for Lakebase auth" "HTTPS/REST"
        stateModules -> workflowCards "Reads YAML manifests on first page load (cached in _cards module variable)" ""
        stateModules -> hfHub "Loads embedding vectors for similarity search via pgvector" "HTTPS"

        # Relationships - pipeline platform
        ingestionPipelines -> workflowFramework "Decorated with @workflow, lifecycle hooks fire on start/complete/skip/error" ""
        workflowFramework -> workflowCards "Loads YAML cards, attaches metadata to registry entries" ""
        workflowFramework -> costEstimateHook "Dispatches on_start/on_complete/on_skip/on_error to registered hooks" ""
        ingestionPipelines -> analyticsLibrary "Imports domain logic (xG, xT, pitch control, OBSO)" ""
        ingestionPipelines -> bronzeSchema "Writes compute results to Delta tables" "PySpark/Delta"
        costEstimateHook -> observabilitySchema "MERGE run state + cost estimates to workflow_cost_live" "PySpark/Delta"
        databricksWorkflows -> ingestionPipelines "Schedules and executes 19 compute tasks" "Databricks Jobs API"

        # Relationships - HF Jobs
        hfJobs -> analyticsLibrary "Imports from wheel (luxury-lakehouse/build-artifacts)" "pip/HTTPS"
        hfJobs -> hfCostRecorder "Records cost via start()/complete()/fail()/skip()" ""
        hfCostRecorder -> hfHub "Writes _workflow_cost.json with RUNNING→COMPLETED transitions" "HTTPS/HF API"
        hfJobs -> hfHub "Publishes trained models and computed grids" "HTTPS/HF API"

        # Relationships - dbt
        dbtProject -> unityCatalog "Reads bronze, writes gold via Databricks SQL" "Delta Lake"
        fctWorkflowCosts -> observabilitySchema "Post-hook: cleans up redundant warm-tier rows from workflow_cost_live" "SQL DELETE"

        # Relationships - deploy
        deployScript -> hfSpaces "upload_folder() with ignore_patterns + delete_patterns for full sync" "HTTPS/HF API"
        deployWheel -> hfHub "Downloads wheel from build-artifacts" "HTTPS/HF API"
        deployWheel -> bronzeSchema "Uploads wheel to /Volumes/{catalog}/bronze/libs/" "Databricks SDK"
        hfSpaces -> taipyApp "Builds Docker image, runs Taipy GUI on port 7860" "Docker"
        analyst -> hfSpaces "Accesses luxury-lakehouse/soccer-analytics-app" "HTTPS"

        # Deployment environment
        production = deploymentEnvironment "Production" {
            deploymentNode "HuggingFace Infrastructure" "Managed container hosting" "Docker SDK" {
                deploymentNode "cpu-basic" "Free tier, sleep after 48h" "2 vCPU, 16 GB RAM" {
                    appInstance = containerInstance guiLayer
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
                deploymentNode "a10g-large ($1.50/hr)" "46 GB RAM, A10G GPU" "Python 3.10, UV" {
                    gpuJobInstance = infrastructureNode "OBSO, Space Creation, xG v2 training"
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
            autoLayout
        }

        container pipelinePlatform "PipelineContainers" {
            include *
            include databricksWorkflows
            include hfJobs
            include hfHub
            include observabilitySchema
            include bronzeSchema
            autoLayout
        }

        container taipyApp "TaipyContainers" {
            include *
            include lakebase
            include databricksApi
            include hfHub
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
