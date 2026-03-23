workspace "Luxury Lakehouse" "Serverless soccer analytics platform: 16 AI/ML workflows, Taipy dashboard on HF Spaces, Databricks Lakebase." {

    model {
        analyst = person "Soccer Analyst" "Coaches, scouts, and analysts exploring match and player data"
        developer = person "Developer" "Deploys application updates to HuggingFace Spaces"

        taipyApp = softwareSystem "Taipy Dashboard" "Interactive soccer analytics application with 12 pages covering shots, passes, networks, player comparison, pitch control, and defensive metrics" {
            guiLayer = container "Taipy GUI" "Root template with sidebar navigation, glossary panels, footer links, and page routing" "Python, Taipy 4.1"
            templateEngine = container "Template Engine" "Generates all page layouts from typed dataclasses: PageConfig, SubView, ContentBlock (image/table/text/expandable_table/chart), ContentRow, SidebarWidget, Metric, Citation. Chart blocks render Plotly figures via native figure= binding" "Python, frozen dataclasses"
            sidebarWidgets = container "Sidebar Widgets" "Centralized filter cascade with progressive disclosure, view-dependent visibility, change_delay debounce, and inline help tooltips" "Python, Taipy Markdown"
            stateModules = container "State Modules" "Per-page state variables, callbacks, data fetching, chart rendering (12 modules). Static charts via mplsoccer PNG. Interactive charts via Plotly go.Figure (Pass Network, Pass Timing, Defensive Impact)" "Python, pandas, mplsoccer, Plotly"
            filterLayer = container "Filter Layer" "Shared filter queries with TTL cache, scope labels, data freshness, and embedding player search" "Python, psycopg2"
            dbLayer = container "DB Layer" "OAuth token management, connection pooling, parameterized query execution" "Python, psycopg2, Databricks SDK"
            renderEngine = container "Render Engine" "Matplotlib/mplsoccer figure-to-PNG with cache-busting paths for static pitch diagrams" "Python, matplotlib, mplsoccer"
            pitchControl = container "Pitch Control Engine" "Physics-based (Spearman 2017) and Voronoi pitch control surface computation" "Python, NumPy, SciPy"
            configLayer = container "Config" "Pydantic settings from environment variables with identifier validation" "Python, pydantic-settings"
        }

        deployPipeline = softwareSystem "Deploy Pipeline" "scripts/deploy_taipy.py with pre-flight checks, ignore patterns, full sync with delete_patterns, and post-upload verification" {
            deployScript = container "deploy_taipy.py" "CLI tool: pre-flight checks (README, token, space), upload_folder with ignore/delete patterns, post-upload timestamp verification, dry-run mode" "Python, huggingface_hub"
        }

        pipelinePlatform = softwareSystem "AI/ML Pipeline Platform" "16 workflow-card-registered compute pipelines with @workflow decorators, lifecycle hooks, and YAML manifests" {
            workflowFramework = container "Workflow Framework" "Registry, @workflow decorator, WorkflowContext, lifecycle hooks (LoggingHook)" "Python, src/workflows/"
            workflowCards = container "Workflow Cards" "16 YAML manifests defining inputs, outputs, deps, cost, academic provenance" "YAML, workflow-cards/" "Database"
            ingestionPipelines = container "Compute Pipelines" "12 @workflow-decorated pipelines: xG, VAEP, DEFCON, pitch control, xT, OBSO/PAUSA, entity resolution, etc." "Python, PySpark, src/ingestion/"
            analyticsLibrary = container "Analytics Library" "Pure-Python models: pitch control, xG, xT, VAEP, OBSO, line-breaking, augmentation" "Python, NumPy, SciPy, src/analytics/"
        }

        lakebase = softwareSystem "Databricks Lakebase" "PostgreSQL-compatible endpoint syncing 19 Delta Lake tables from Unity Catalog (38 indexes, 4 HNSW vector)" "External"
        databricksApi = softwareSystem "Databricks REST API" "OAuth credential endpoint for Lakebase authentication" "External"
        databricksWorkflows = softwareSystem "Databricks Workflows" "Scheduled DAG orchestration for compute pipelines (daily 06:00 UTC)" "External"
        hfSpaces = softwareSystem "HuggingFace Spaces" "Docker SDK hosting at luxury-lakehouse/staging. Builds from Dockerfile, serves on port 7860" "External"
        hfHub = softwareSystem "HuggingFace Hub" "Hosts models, datasets, and build-artifacts wheel for HF Jobs scripts" "External"
        hfJobs = softwareSystem "HuggingFace Jobs" "GPU/CPU training and grid computation for xG, VAEP, xT, EPV, OBSO, Space Creation" "External"

        # Relationships - users
        analyst -> guiLayer "Browses pages, selects filters, views interactive and static charts" "HTTPS"
        developer -> deployScript "Runs deploy_taipy.py staging [--dry-run]" "CLI"

        # Relationships - internal
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

        # Relationships - external
        dbLayer -> lakebase "Queries 19 synced tables via parameterized SQL" "PostgreSQL/SSL"
        dbLayer -> databricksApi "Fetches OAuth tokens for Lakebase auth" "HTTPS/REST"
        stateModules -> hfHub "Loads embedding vectors for similarity search via pgvector" "HTTPS"

        # Relationships - pipeline platform
        ingestionPipelines -> workflowFramework "Decorated with @workflow, lifecycle hooks fire on start/complete/error" ""
        workflowFramework -> workflowCards "Loads YAML cards, attaches metadata to registry entries" ""
        ingestionPipelines -> analyticsLibrary "Imports domain logic (xG, xT, pitch control, OBSO)" ""
        ingestionPipelines -> lakebase "Writes Delta tables synced to Lakebase" "PySpark/Delta"
        databricksWorkflows -> ingestionPipelines "Schedules and executes compute tasks" "Databricks Jobs API"
        hfJobs -> analyticsLibrary "Imports from wheel (luxury-lakehouse/build-artifacts)" "pip/HTTPS"
        hfJobs -> hfHub "Publishes trained models and computed grids" "HTTPS/HF API"

        # Deployment relationships
        deployScript -> hfSpaces "upload_folder() with ignore_patterns + delete_patterns for full sync" "HTTPS/HF API"
        hfSpaces -> taipyApp "Builds Docker image, runs Taipy GUI on port 7860" "Docker"
        analyst -> hfSpaces "Accesses luxury-lakehouse-staging.hf.space" "HTTPS"

        # Deployment environment
        production = deploymentEnvironment "HuggingFace Spaces" {
            deploymentNode "HuggingFace Infrastructure" "Managed container hosting" "Docker SDK" {
                deploymentNode "cpu-basic" "Free tier, sleep after 48h" "2 vCPU, 16 GB RAM" {
                    appInstance = containerInstance guiLayer
                }
            }
            deploymentNode "Databricks Cloud" "US East 1" "AWS" {
                deploymentNode "Lakebase Autoscaling" "0.5-4 CU, scale-to-zero" "PostgreSQL 17" {
                    lakebaseNode = infrastructureNode "Lakebase Endpoint" "ep-spring-rain-d2i6lozx" "PostgreSQL-compatible"
                }
            }
        }
    }

    views {
        systemContext taipyApp "SystemContext" {
            include *
            include deployPipeline
            include pipelinePlatform
            include databricksWorkflows
            include hfJobs
            autoLayout
        }

        container pipelinePlatform "PipelineContainers" {
            include *
            autoLayout
        }

        container taipyApp "Containers" {
            include *
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

        deployment taipyApp "HuggingFace Spaces" "Deployment" {
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
