# Structurizr DSL
workspace "Luxury Lakehouse" "Serverless soccer analytics platform on Databricks Lakebase. Medallion architecture (bronze/silver/gold), dbt-managed marts, Lakebase Postgres synced for low-latency UI reads, Taipy HF Space for analyst dashboards. Reflects SK3-MIG-B 2026-05-03 (silly-kicks 3.0.1 + XG1-RETIRE; xG v1 retired entirely). Wheel 0.3.31." {

    model {
        // ── Actors ─────────────────────────────────────────────────────
        operator = person "Platform Operator" "Triggers retrain cycles, reviews mega-job runs, manages Lakebase synced tables."
        analyst = person "Soccer Analyst" "Reads Taipy dashboards: shot maps, player comparisons, defensive impact."

        // ── External systems ───────────────────────────────────────────
        statsbomb = softwareSystem "StatsBomb Open Data" "~3,000 matches (events) + 323 matches (360 freeze frames). CC-BY 4.0." "External"
        wyscout = softwareSystem "Wyscout Public Dataset" "~1,900 matches (events). CC-BY-NC 4.0." "External"
        idsse = softwareSystem "IDSSE / Bundesliga" "DFL XML tracking + matchinformation. Opta/StatsPerform format." "External"
        metrica = softwareSystem "Metrica Sports" "Tracking + events (kloppy format)." "External"
        skillcorner = softwareSystem "SkillCorner" "A-League tracking (kloppy format)." "External"
        hfhub = softwareSystem "HuggingFace Hub" "Hosts datasets, model weights, Spaces. Org: luxury-lakehouse." "External"

        // ── The lakehouse system ───────────────────────────────────────
        lakehouse = softwareSystem "Luxury Lakehouse" "Serverless soccer analytics on Databricks." {

            // Databricks-side containers
            megaJob = container "Mega-Job Orchestrator" "Single Databricks Workflow (job_id 302697362345215, 33 task_keys). Triggers daily; SK3-MIG-B orchestrator triggers full job + relies on per-task skip-guards." "Databricks Workflow"

            bronze = container "Bronze Delta Layer" "Raw provider data: idsse_events, idsse_tracking, metrica_*, sk_events, statsbomb_*, wyscout_*, spadl_actions, xg_predictions_v2, sk3_mig_b_runs (telemetry, ADR-002 §4)." "Delta Lake / Unity Catalog" "Database"

            silverGold = container "dbt Models (silver + gold)" "40 marts, 30 with enforced contracts, 24 with liquid clustering. Three-stage build (input -> intermediate -> output) per ADR-019." "dbt + Databricks SQL"

            mlflow = container "MLflow UC Registry" "Model versioning + @Champion alias. ADR-012 zombie-alias-guarded delivery (set_and_verify_mlflow_champion)." "MLflow + Unity Catalog"

            ucVolume = container "UC Volume Model Weights" "Inference-time fallback artifacts: /Volumes/{cat}/dev_gold/model_weights/{model}/. SHA-256 sidecar per ADR-012." "Unity Catalog Volume"

            lakebase = container "Lakebase Postgres" "34 synced tables, 56 PG indexes (50 btree + 6 HNSW vector). Low-latency reads for the Taipy app. Indexes auto-restored daily via dbt-live-ci.yml." "Lakebase Postgres" "Database"

            telemetryTable = container "Telemetry: sk3_mig_b_runs" "Orchestrator cycle log per spec §5.3. cycle_id + cycle_item + smoke_pass + cost_usd + heartbeat rows. ADR-002 §4 schema-drift guard." "Delta Lake" "Database"

            // SK3-MIG-B orchestrator (PEP 723 single-file)
            sk3MigBOrch = container "SK3-MIG-B Retrain Orchestrator" "PEP 723 single-file script. Runs 11 cycle items + 8 HF republishes + Lakebase synced refresh + XG1-RETIRE runtime. Cost cap $80, walltime cap 8h, halt-resume." "Python (PEP 723)"

            // HF-Jobs side
            hfJobs = container "HF Jobs Trainers" "Cloud-GPU PEP 723 trainers (cpu-basic | l40sx1 | gpu-medium | gpu-large). xG v2, VAEP, F2V v1/v2/360, ScoutGPT." "HuggingFace Jobs"

            hfPublishers = container "HF Publishers" "8 PEP 723 scripts: spadl-vaep, xg-shots, freeze-frame, shots-on-target, obso-pausa-inputs, obso-trained-grids, obso-pausa-values, football2vec-player-embeddings. SDK statement_execution + upload_hf_readme (ADR-014)." "Python (PEP 723)"

            // Taipy app
            taipy = container "Taipy Dashboard App" "16 pages on HF Space (prod + staging). Template-driven via page_template.PageConfig. Reads Lakebase Postgres for low-latency interactions." "Taipy / Python"
        }

        // ── External relationships ─────────────────────────────────────
        operator -> sk3MigBOrch "Triggers retrain cycles" "uv run python"
        operator -> megaJob "Triggers daily / on-demand" "Databricks SDK"
        analyst -> taipy "Browses dashboards" "HTTPS"

        statsbomb -> megaJob "Provides events + 360" "ingest_statsbomb (HTTP/CC-BY)"
        wyscout -> megaJob "Provides events" "ingest_wyscout (HTTP/CC-BY-NC)"
        idsse -> megaJob "Provides DFL XML tracking" "ingest_idsse"
        metrica -> megaJob "Provides tracking + events" "ingest_metrica (kloppy)"
        skillcorner -> megaJob "Provides A-League tracking" "ingest_skillcorner (kloppy)"

        sk3MigBOrch -> megaJob "Triggers full job + waits on task_key" "Databricks SDK run_now"
        sk3MigBOrch -> hfJobs "Dispatches trainers" "huggingface_hub.HfApi.run_jobs"
        sk3MigBOrch -> hfPublishers "Invokes via uv run" "subprocess"
        sk3MigBOrch -> telemetryTable "Appends cycle_item + heartbeat rows" "INSERT INTO (statement_execution)"
        sk3MigBOrch -> lakebase "Refreshes synced tables + restores indexes" "scripts/maintain_synced_tables.py"

        megaJob -> bronze "Writes raw + SPADL actions + telemetry" "Spark Delta writes"
        bronze -> silverGold "Sourced by staging; gold materialised" "dbt models (3-stage build)"
        silverGold -> lakebase "Synced via Databricks Synced Tables" "Snapshot replication"

        hfJobs -> mlflow "Registers model versions + sets @Champion" "set_and_verify_mlflow_champion (ADR-012)"
        hfJobs -> ucVolume "Uploads weight bytes + SHA-256 sidecar" "upload_weights_to_uc_volume (ADR-012)"
        hfJobs -> hfhub "Publishes model weights + README" "upload_hf_readme (ADR-014)"

        hfPublishers -> silverGold "Reads gold marts via SQL" "Databricks Statement Execution"
        hfPublishers -> hfhub "Publishes datasets + cards" "HfApi.upload_folder + upload_hf_readme"

        megaJob -> mlflow "Loads @Champion at inference" "Pyfunc + UC Volume fallback"
        megaJob -> ucVolume "Reads weight bytes (fallback)" "WorkspaceClient.files.download"
        megaJob -> hfhub "SHA freshness probe" "huggingface_hub.HfApi"

        taipy -> lakebase "Low-latency reads" "PG SELECT (psycopg)"
        taipy -> hfhub "Hosted on HF Space" "Docker container"
    }

    views {
        systemContext lakehouse "SystemContext" {
            include *
            autoLayout
        }

        container lakehouse "Containers" {
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
