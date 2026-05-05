# Structurizr DSL
workspace "Luxury Lakehouse" "Serverless soccer analytics platform on Databricks. Medallion architecture (bronze/silver/gold), dbt-managed marts, Lakebase Postgres for low-latency UI reads, Taipy HF Space for dashboards." {

    model {
        analyst = person "Soccer Analyst" "Coaches, scouts, analysts exploring match and player data"
        developer = person "Developer" "Deploys updates, triggers pipeline runs, monitors costs"
        operator = person "Platform Operator" "Triggers retrain cycles, reviews job runs, manages synced tables"

        taipyApp = softwareSystem "Taipy Dashboard" "16-page interactive analytics app on HF Spaces. Shot maps, player comparisons, defensive impact, workflow monitoring." {
            guiLayer = container "Taipy GUI" "Root template with sidebar nav, glossary panels, page routing" "Python, Taipy 4.1"
            adminApi = container "Admin API" "Flask blueprint for cache clear and synced table refresh. HF org membership auth." "Python, Flask"
            templateEngine = container "Template Engine" "Three layout builders (standard, sub-view, dashboard). Typed PageConfig dataclasses." "Python"
            sidebarWidgets = container "Sidebar Widgets" "Filter cascade with progressive disclosure, debounce, help tooltips" "Python, Taipy"
            stateModules = container "State Modules" "Per-page state, callbacks, chart rendering. SQL-free; delegates to Query Layer." "Python, Plotly"
            queryLayer = container "Query Layer" "12 SQL modules with TTL cache. All parameterized queries." "Python, psycopg2"
            filterLayer = container "Filter Layer" "Shared filter queries, scope labels, embedding player search" "Python, psycopg2"
            dbLayer = container "DB Layer" "OAuth tokens, connection pooling, /health endpoint" "Python, psycopg2"
            renderEngine = container "Render Engine" "Matplotlib/mplsoccer figure-to-PNG with cache-busting paths" "Python, mplsoccer"
            pitchControl = container "Pitch Control Engine" "Physics-based (Spearman 2017) and Voronoi surface computation" "Python, NumPy"
            configLayer = container "Config" "Pydantic settings from env vars and .env file" "Python, pydantic-settings"
            guiExtensions = container "GUI Extensions (ll_ext)" "WAI-ARIA combobox, lightbox overlay. MUI/React UMD bundle." "TypeScript, React 18"
        }

        deployPipeline = softwareSystem "Deploy Pipeline" "Scripts for Taipy app and wheel deployment" {
            deployScript = container "manage_space.py" "Full Space lifecycle: create, deploy, status, rebuild, teardown" "Python, huggingface_hub"
            deployWheel = container "deploy_wheel.py" "Downloads wheel from HF Hub, uploads to UC Volume" "Python, databricks-sdk"
            bumpWheel = container "bump_wheel.py" "Syncs wheel version to all static consumers (25+ files)" "Python"
            dbtBuildAndRefresh = container "dbt_build_and_refresh.py" "Chains dbt build with synced table refresh" "Python, subprocess"
        }

        pipelinePlatform = softwareSystem "AI/ML Pipeline Platform" "41 workflow-card-registered workflows. Centralized hooks, lifecycle tracking, three-tier cost tracking." {
            workflowFramework = container "Workflow Framework" "Registry, @workflow decorator, lifecycle runner with hook dispatch" "Python"
            workflowCards = container "Workflow Cards" "41 YAML manifests: inputs, outputs, deps, cost estimates, provenance" "YAML" "Database"
            costEstimateHook = container "CostEstimateHook" "Writes run state, entity_count, row_count, cost to Delta via MERGE" "Python, PySpark"
            hfCostRecorder = container "HFJobsCostRecorder" "Cost recorder for HF Jobs. Writes to HF Hub repos. 90-day pruning." "Python"
            guardRegistry = container "Guard Registry" "SkipGuard protocol, FilterResult, find_new_ids(), timed_check() wrapper" "Python"
            artifactDeploy = container "Artifact Deploy" "Training-to-production contract (ADR-012). MLflow + UC Volume helpers." "Python"
            hfPublish = container "HF Publish Helper" "README delivery (ADR-014). upload_hf_readme + get_hf_card_path." "Python"
            ingestionPipelines = container "Compute Pipelines" "30 @workflow-decorated Databricks pipelines across 5 providers" "Python, PySpark"
            refreshSyncedTables = container "Synced Table Refresh" "Triggers SNAPSHOT refresh on 34 Lakebase synced tables" "Python, databricks-sdk"
            dbtRunner = container "dbt Runner" "python_wheel_task entry point. OAuth token exchange, warehouse start." "Python, dbt-core"
            evolveEngine = container "Evolve Engine" "LLM-guided architecture search. AST validation, restricted exec." "Python, OpenEvolve"
            analyticsLibrary = container "Analytics Library" "Pure-Python domain models: xG, xT, VAEP, OBSO, pitch control, embeddings" "Python, PyTorch"
            sharedLibrary = container "Shared Library" "Cross-package constants. Zero external deps." "Python"
        }

        # SK3-MIG-B Retrain Orchestrator (new)
        sk3MigBOrch = softwareSystem "Retrain Orchestrator" "Idempotent retrain cycle: pre-flight gates, input publish, HF Jobs trainers, dbt build, output publish, Lakebase refresh." {
            orchestratorScript = container "sk3_mig_b_retrain.py" "PEP 723 single-file orchestrator. Resumable, cost-capped, walltime-capped." "Python"
            hfInputPublishers = container "Input Dataset Publishers" "3 scripts: SPADL/VAEP, xG shots, freeze frames. Publish to HF Hub pre-training." "Python"
            hfOutputPublishers = container "Output Dataset Publishers" "5 scripts: embeddings, OBSO grids, line-breaking, pitch control, shots-on-target." "Python"
            hfJobsTrainers = container "HF Jobs Trainers" "Cloud-GPU training: VAEP, xG v2, PSxG, Football2Vec v2/360, ScoutGPT." "Python, PyTorch"
            extV2Gates = container "ExT Smoke Gates" "Local NLL validation for Expected Threat Phase 0/1. Baseline thresholds." "pytest"
            ciSentinels = container "CI Invariant Sentinels" "Task mappings, flavor configs, dep versions, Kimball contracts." "pytest"
            telemetryTable = container "Telemetry Table" "Cycle log: items, smoke-gate pass/fail, cost tracking, heartbeat rows." "Delta Lake" "Database"
        }

        dbtProject = softwareSystem "dbt Project" "Medallion transformation: 82 models (36 staging, 7 intermediate, 39 marts). Kimball dimensions, liquid clustering." {
            fctWorkflowCosts = container "fct_workflow_costs" "Gold-layer cost attribution with billing JOIN. 90-day rolling window." "SQL, dbt" "Database"
            goldModels = container "Gold Models" "35 fact + 4 dim tables. Enforced contracts, liquid clustering." "SQL, dbt" "Database"
        }

        # Data stores
        unityCatalog = softwareSystem "Unity Catalog" "Governed Delta Lake: bronze (raw), gold (analytics), observability (metadata)" "External" {
            bronzeSchema = container "Bronze Schema" "Raw events, tracking, SPADL actions, VAEP scores, compute results" "Delta Lake" "Database"
            goldSchema = container "Gold Schema" "35 fact + 4 dim tables. Analytics-ready." "Delta Lake" "Database"
            observabilitySchema = container "Observability Schema" "workflow_cost_live, workflow_import_checksums, definer's-rights views" "Delta Lake" "Database"
        }

        lakebase = softwareSystem "Databricks Lakebase" "PostgreSQL endpoint syncing 34 Delta tables (56 indexes: 50 btree + 6 HNSW)" "External"
        databricksApi = softwareSystem "Databricks REST API" "OAuth, synced table metadata, pipeline triggers, state polling" "External"
        databricksWorkflows = softwareSystem "Databricks Workflows" "32-task daily DAG: 5 ingest, 14 compute, 1 HF sync, dbt_build, refresh" "External"
        hfIdentity = softwareSystem "HuggingFace Identity API" "Token validation via /api/whoami-v2. Org membership check." "External"
        hfSpaces = softwareSystem "HuggingFace Spaces" "Docker SDK hosting. Builds Dockerfile, serves port 7860." "External"
        hfHub = softwareSystem "HuggingFace Hub" "17 models, 19 datasets, build-artifacts wheel. READMEs via ADR-014." "External"
        hfJobs = softwareSystem "HuggingFace Jobs" "L40S GPU / cpu-basic compute for training and batch analytics" "External"
        openRouter = softwareSystem "OpenRouter" "LLM API: Claude Sonnet 4 (80%), Haiku 4.5 (20%) for Evolve mutations" "External"

        githubActions = softwareSystem "GitHub Actions CI/CD" "Platform automation: Terraform, Python CI, dbt CI, Semgrep, Lakebase grants" {
            terraformApply = container "Terraform Apply" "Auto-apply on push to main. AWS OIDC + Databricks federation." ".github/workflows/"
            terraformPlan = container "Terraform Plan" "Plan on PR, posts diff. Human reviews before merge." ".github/workflows/"
            pythonCi = container "Python CI" "ruff, pyright, pytest, detect-secrets, pip-audit. Deploys wheel on main." ".github/workflows/"
            dbtCi = container "dbt CI" "dbt parse + slim CI for contract verification" ".github/workflows/"
            semgrepCi = container "Semgrep SAST" "OWASP-aligned static analysis on every push" ".github/workflows/"
            lakebaseGrantsWorkflow = container "Lakebase Grants" "Self-healing SELECT grants for Taipy SP (ADR-005)" ".github/workflows/"
            bronzeLiveSchemaCi = container "Bronze Live Schema" "DESCRIBE parity + rowcount tests against live bronze" ".github/workflows/"
        }

        # Relationships - users
        analyst -> guiLayer "Browses pages, views charts" "HTTPS"
        developer -> deployScript "Deploys to staging/production" "CLI"
        developer -> deployWheel "Pushes wheel to UC Volume" "CLI"
        developer -> bumpWheel "Syncs version after bump" "CLI"
        developer -> dbtBuildAndRefresh "Rebuilds gold + Lakebase" "CLI"
        developer -> adminApi "Cache clear (incident response)" "HTTPS"
        operator -> orchestratorScript "Triggers retrain cycles" "CLI"
        operator -> databricksWorkflows "Triggers daily job" "Databricks UI"

        # Relationships - Taipy internal
        guiLayer -> templateEngine "Calls build_page()" ""
        templateEngine -> sidebarWidgets "Generates filter sections" ""
        guiLayer -> stateModules "Binds state and callbacks" ""
        templateEngine -> stateModules "References state variables" ""
        stateModules -> queryLayer "Fetches page data" ""
        stateModules -> filterLayer "Fetches filter options" ""
        queryLayer -> dbLayer "Executes SQL" "SQL"
        filterLayer -> dbLayer "Queries dimensions" "SQL"
        stateModules -> renderEngine "Generates pitch diagrams" ""
        stateModules -> pitchControl "Computes surfaces" ""
        renderEngine -> guiLayer "Returns image paths" ""
        dbLayer -> configLayer "Reads Lakebase settings" ""

        # Relationships - external (Taipy)
        dbLayer -> lakebase "Queries synced tables" "PostgreSQL/SSL"
        dbLayer -> databricksApi "Fetches OAuth tokens" "HTTPS"
        stateModules -> workflowCards "Reads YAML manifests" ""
        stateModules -> hfHub "Loads embeddings, cost history" "HTTPS"

        # Relationships - Admin API
        adminApi -> hfIdentity "Validates HF token" "HTTPS"
        adminApi -> refreshSyncedTables "Spawns refresh subprocess" "subprocess"

        # Relationships - pipeline platform
        ingestionPipelines -> workflowFramework "Decorated with @workflow" ""
        workflowFramework -> workflowCards "Loads YAML metadata" ""
        workflowFramework -> costEstimateHook "Dispatches lifecycle hooks" ""
        ingestionPipelines -> analyticsLibrary "Imports domain logic" ""
        ingestionPipelines -> sharedLibrary "Imports constants" ""
        analyticsLibrary -> sharedLibrary "Imports IDENTIFIER_RE" ""
        costEstimateHook -> sharedLibrary "Imports schema constants" ""
        ingestionPipelines -> bronzeSchema "Writes compute results" "PySpark/Delta"
        costEstimateHook -> observabilitySchema "MERGE cost estimates" "PySpark/Delta"
        databricksWorkflows -> ingestionPipelines "Executes pipeline tasks" "Jobs API"
        databricksWorkflows -> dbtRunner "Invokes dbt_build task" "Jobs API"
        databricksWorkflows -> refreshSyncedTables "Final refresh task" "Jobs API"
        dbtRunner -> dbtProject "Invokes dbt build" "dbt-core"
        dbtRunner -> databricksApi "Exchanges SP for OAuth" "HTTPS"
        dbtRunner -> sharedLibrary "Imports conventions" ""
        ingestionPipelines -> guardRegistry "Calls timed_check()" ""
        guardRegistry -> hfHub "Fetches commit SHA" "HTTPS"
        guardRegistry -> observabilitySchema "Reads/writes checksums" "PySpark/Delta"
        refreshSyncedTables -> databricksApi "Triggers SNAPSHOT" "HTTPS"
        refreshSyncedTables -> sharedLibrary "Imports IDENTIFIER_RE" ""
        dbtBuildAndRefresh -> refreshSyncedTables "Subprocess after dbt" "subprocess"

        # Relationships - SK3-MIG-B Orchestrator
        orchestratorScript -> hfInputPublishers "Publishes input datasets" ""
        orchestratorScript -> databricksWorkflows "Triggers hf_sync prereq" "REST API"
        orchestratorScript -> hfJobsTrainers "Dispatches cloud trainers" "HF Jobs API"
        orchestratorScript -> extV2Gates "Runs local smoke gates" "pytest"
        orchestratorScript -> databricksWorkflows "Triggers compute tasks" "REST API"
        orchestratorScript -> hfOutputPublishers "Publishes output datasets" ""
        orchestratorScript -> telemetryTable "Logs cycle progress" "Delta"
        orchestratorScript -> lakebase "Refreshes synced tables" "REST API"
        hfInputPublishers -> goldSchema "Reads gold marts" "Spark SQL"
        hfInputPublishers -> hfHub "Publishes datasets" "HTTPS"
        hfOutputPublishers -> goldSchema "Reads post-retrain marts" "Spark SQL"
        hfOutputPublishers -> hfHub "Publishes datasets" "HTTPS"
        hfJobsTrainers -> hfHub "Reads training data" "HTTPS"
        hfJobsTrainers -> hfHub "Publishes weights + cards" "HTTPS"
        hfJobsTrainers -> artifactDeploy "Registers models" "MLflow"
        extV2Gates -> goldSchema "Fetches action values" "Spark SQL"
        ciSentinels -> orchestratorScript "Validates config" "pytest"
        ciSentinels -> hfJobsTrainers "Validates trainer config" "pytest"

        # Relationships - Evolve Engine
        developer -> evolveEngine "Runs evolve CLI" "CLI"
        evolveEngine -> analyticsLibrary "Imports ScoutGPT decoder" ""
        evolveEngine -> openRouter "Sends mutation prompts" "HTTPS"
        evolveEngine -> hfHub "Downloads training data" "HTTPS"
        evolveEngine -> workflowCards "Registered as wf-evolve-scoutgpt" ""
        evolveEngine -> sharedLibrary "Imports WHEEL_BASE_URL" ""
        evolveEngine -> hfJobs "Submits training jobs" "HTTPS"

        # Relationships - HF Jobs
        hfJobs -> analyticsLibrary "Imports from wheel" "pip"
        hfJobs -> hfCostRecorder "Records cost" ""
        hfCostRecorder -> hfHub "Writes cost JSON" "HTTPS"
        hfJobs -> hfHub "Publishes models/grids" "HTTPS"
        hfJobs -> hfPublish "Pushes README" "HTTPS"

        # Relationships - HF Publish helper
        ingestionPipelines -> hfPublish "Uploads README after data" ""
        hfPublish -> hfHub "HfApi.upload_file" "HTTPS"
        developer -> hfPublish "Manual card pushes" "CLI"

        # Relationships - dbt
        dbtProject -> unityCatalog "Reads bronze, writes gold" "Delta Lake"
        fctWorkflowCosts -> observabilitySchema "Cleans warm-tier rows" "SQL"

        # Relationships - deploy
        deployScript -> hfSpaces "upload_folder()" "HTTPS"
        bumpWheel -> sharedLibrary "Imports version helpers" ""
        deployWheel -> sharedLibrary "Imports WHEEL_FILENAME" ""
        deployWheel -> hfHub "Downloads wheel" "HTTPS"
        deployWheel -> bronzeSchema "Uploads to UC Volume" "SDK"
        taipyApp -> analyticsLibrary "Installs wheel at build" "pip"
        hfSpaces -> taipyApp "Builds Docker, serves 7860" "Docker"
        analyst -> hfSpaces "Accesses app" "HTTPS"

        # Relationships - GitHub Actions
        terraformApply -> databricksApi "Applies TF resources" "OIDC"
        terraformPlan -> databricksApi "Reads state for diff" "OIDC"
        pythonCi -> bronzeSchema "Deploys wheel on main" "HTTPS"
        bronzeLiveSchemaCi -> bronzeSchema "DESCRIBE + count parity" "SQL"
        bronzeLiveSchemaCi -> databricksApi "Warehouse auto-resume" "HTTPS"
        lakebaseGrantsWorkflow -> databricksApi "Gets PG credential" "HTTPS"
        lakebaseGrantsWorkflow -> lakebase "GRANT SELECT" "PostgreSQL"
        terraformApply -> lakebaseGrantsWorkflow "workflow_run trigger" "GH Actions"
        developer -> pythonCi "Opens PR / pushes" "git"
        developer -> dbtCi "PR with dbt changes" "git"
        developer -> terraformPlan "PR with TF changes" "git"
        developer -> terraformApply "Merges to main" "git"

        # Deployment environment
        production = deploymentEnvironment "Production" {
            deploymentNode "HuggingFace Infrastructure" "Managed container hosting" "Docker SDK" {
                deploymentNode "cpu-basic" "Free tier, sleep after 48h" "2 vCPU, 16 GB RAM" {
                    appInstance = containerInstance guiLayer
                    healthEndpoint = infrastructureNode "Health Endpoint" "/health route"
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
                    gpuJobInstance = infrastructureNode "PSxG, xG v2, Football2vec, OBSO, Evolve"
                }
            }
        }
    }

    views {
        systemContext taipyApp "SystemContext" {
            include *
            include deployPipeline
            include pipelinePlatform
            include sk3MigBOrch
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

        container sk3MigBOrch "RetrainOrchestrator" {
            include *
            include databricksWorkflows
            include hfJobs
            include hfHub
            include lakebase
            include goldSchema
            include operator
            autoLayout
        }

        dynamic pipelinePlatform "CostTracking" {
            ingestionPipelines -> workflowFramework "Pipeline starts"
            workflowFramework -> costEstimateHook "on_start hook"
            costEstimateHook -> observabilitySchema "MERGE RUNNING state"
            ingestionPipelines -> workflowFramework "Pipeline completes"
            workflowFramework -> costEstimateHook "on_complete hook"
            costEstimateHook -> observabilitySchema "MERGE COMPLETED + cost"
            autoLayout
        }

        dynamic taipyApp "FilterCascade" {
            analyst -> guiLayer "Selects competition"
            guiLayer -> stateModules "Fires callback"
            stateModules -> filterLayer "Fetches filtered options"
            filterLayer -> dbLayer "Recursive CTE query"
            dbLayer -> lakebase "SELECT from synced table"
            stateModules -> guiLayer "Updates state"
            stateModules -> renderEngine "Re-renders chart"
            renderEngine -> guiLayer "Returns image path"
            autoLayout
        }

        dynamic pipelinePlatform "GuardAsWrapper" {
            databricksWorkflows -> ingestionPipelines "Job starts"
            ingestionPipelines -> guardRegistry "Calls timed_check()"
            ingestionPipelines -> workflowFramework "WorkflowSkippedError"
            workflowFramework -> costEstimateHook "on_skip dispatch"
            costEstimateHook -> observabilitySchema "MERGE SKIPPED"
            autoLayout
        }

        dynamic pipelinePlatform "EvolveLevel2" {
            developer -> evolveEngine "Launch --code-evolution"
            evolveEngine -> openRouter "Generate candidate"
            evolveEngine -> analyticsLibrary "AST-validate + exec"
            evolveEngine -> hfJobs "Dispatch to L40S"
            autoLayout
        }

        dynamic pipelinePlatform "DailyJobHardening" {
            databricksWorkflows -> ingestionPipelines "9 leaf computes run"
            databricksWorkflows -> dbtRunner "dbt_build after leaves"
            dbtRunner -> databricksApi "Exchange SP for OAuth"
            dbtRunner -> dbtProject "Build 39 marts"
            dbtProject -> observabilitySchema "fct_workflow_costs reads"
            databricksWorkflows -> refreshSyncedTables "Final task"
            refreshSyncedTables -> databricksApi "SNAPSHOT 34 tables"
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
