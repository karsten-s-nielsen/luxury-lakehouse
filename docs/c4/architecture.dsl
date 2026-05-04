# Structurizr DSL
workspace "Luxury Lakehouse" "Serverless soccer analytics platform on Databricks Lakebase. Medallion architecture (bronze/silver/gold), dbt-managed marts, Lakebase Postgres synced for low-latency UI reads, Taipy HF Space for analyst dashboards. Reflects SK3-MIG-B PR-1 (orchestrator hardening, 2026-05-04): Group 0 input-publish gating + hf_sync_prereq + runtime silly-kicks guard (ADR-012 §6). Wheel 0.3.32, silly-kicks 3.0.1." {

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
            megaJob = container "Mega-Job Orchestrator" "Single Databricks Workflow (job_id 302697362345215, 32 task_keys post seed reconciliation). Triggers daily; SK3-MIG-B orchestrator triggers full job + waits on per-task termination." "Databricks Workflow"

            bronze = container "Bronze Delta Layer" "Raw provider data: idsse_events, idsse_tracking, metrica_*, sk_events, statsbomb_*, wyscout_*, spadl_actions, xg_predictions_v2, sk3_mig_b_runs (telemetry, ADR-002 §4)." "Delta Lake / Unity Catalog" "Database"

            silverGold = container "dbt Models (silver + gold)" "40 marts, 30 with enforced contracts, 24 with liquid clustering. Three-stage build (input -> intermediate -> output) per ADR-019. fct_action_values is the canonical SPADL action mart consumed by ext_v2 + γ trainers." "dbt + Databricks SQL"

            mlflow = container "MLflow UC Registry" "Model versioning + @Champion alias. ADR-012 zombie-alias-guarded delivery (set_and_verify_mlflow_champion)." "MLflow + Unity Catalog"

            ucVolume = container "UC Volume Model Weights" "Inference-time fallback artifacts: /Volumes/{cat}/dev_gold/model_weights/{model}/. SHA-256 sidecar per ADR-012." "Unity Catalog Volume"

            lakebase = container "Lakebase Postgres" "34 synced tables, 56 PG indexes (50 btree + 6 HNSW vector). Low-latency reads for the Taipy app. Indexes auto-restored daily via dbt-live-ci.yml." "Lakebase Postgres" "Database"

            telemetryTable = container "Telemetry: sk3_mig_b_runs" "Orchestrator cycle log per spec §5.3. cycle_id + cycle_item + cycle_item_kind (trained_model / compute_only / input_publish / publish / meta_event) + smoke_pass + cost_usd + heartbeat rows. ADR-002 §4 schema-drift guard." "Delta Lake" "Database"

            // SK3-MIG-B orchestrator (PEP 723 single-file)
            sk3MigBOrch = container "SK3-MIG-B Retrain Orchestrator" "PEP 723 single-file. Step 0 pre-flight -> 0a Group 0 input-publish (3 datasets, gates Group 1) -> 0b hf_sync_prereq (refreshes f2v_v2/f2v_360 inputs via daily mega-job) -> Group 1/2 trainers (ext_v2_p0/p1 local-only) -> Group 3 output-publish (5) -> XG1-retire runtime -> HF4 cleanup -> final sweep. Cost cap $80, walltime cap 8h, halt-resume. _FLAVOR_MAP / _TASK_KEY_MAP / _LOCAL_TRAINED_MODELS / _TRAINER_SCRIPT_MAP / _GROUP_0_PUBLISHERS / _GROUP_3_PUBLISHERS module-level constants." "Python (PEP 723)"

            // HF-Jobs side
            hfJobs = container "HF Jobs Trainers" "Cloud-GPU PEP 723 trainers (vaep cpu-large | xg_v2 l40sx1 | f2v_v1 cpu-large | f2v_v2 / f2v_360 / scoutgpt l40sx1). Each declares VALIDATED_HF_FLAVOR + _REQUIRED_SK_MIN module constants; main() asserts silly-kicks >= 3.0.1 at runtime (ADR-012 §6 — uv silent-downgrade defense). vaep adds psutil RSS HWM logging." "HuggingFace Jobs"

            // Group 0 input-publish (vaep + xg_v2 input datasets — gates Group 1)
            hfInputPublishers = container "Group 0 Input Publishers" "3 PEP 723 scripts run synchronously by orchestrator step 0a BEFORE Group 1 trainers: spadl-vaep-action-values, xg-shots, xg-freeze-frame-data. Ensures vaep + xg_v2 retrain on FRESH SK3-MIG-corrected inputs." "Python (PEP 723)"

            // Group 3 output-publish (depend on Group 1/2 retrain results)
            hfOutputPublishers = container "Group 3 Output Publishers" "5 PEP 723 scripts: shots-on-target, obso-pausa-inputs, obso-trained-grids, obso-pausa-values, football2vec-player-embeddings. SDK statement_execution + upload_hf_readme (ADR-014). Run AFTER Group 1/2 trainers + dbt build." "Python (PEP 723)"

            // ext_v2 smoke-gate path — runs on operator host, fetches fct_action_values via chunked Arrow
            extV2Gates = container "ext_v2 Smoke Gates (P0 + P1)" "Local-host pytest gates invoked by orchestrator after ext_v2_p0/p1 dispatch. Fetch fct_action_values (~10M rows) via Databricks Statement Execution + EXTERNAL_LINKS Arrow stream (chunked_sql_to_pandas helper). Phase 0: SinghProducer + run_phase0_harness; Phase 1: KDESmoothedProducer with champion params (gaussian, bandwidth=1.99998, adaptive). Assert NLL <= baseline + 1%." "pytest + analytics.ext_v2"

            // CI sentinels
            ciSentinels = container "CI Invariant Sentinels" "5 importlib-based sentinels in test_sk3_mig_b_orchestrator_invariants.py: _TASK_KEY_MAP-vs-seed, _FLAVOR_MAP-vs-VALIDATED_HF_FLAVOR, seed-vs-live-mega-job (env-gated), no-trainer-pins-silly-kicks, all-trainers-assert-_REQUIRED_SK_MIN. Plus test_marts_kimball_contracts.py + test_marts_live_schema.py (env-gated; PR-1.5 wires CI creds)." "pytest"

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

        // SK3-MIG-B orchestrator step ordering (post PR-1)
        sk3MigBOrch -> hfInputPublishers "Step 0a: dispatch input-publish (gates Group 1)" "subprocess uv run"
        sk3MigBOrch -> megaJob "Step 0b: trigger hf_sync task to refresh f2v_v2 + f2v_360 inputs (Q31)" "WorkspaceClient.jobs.run_now"
        sk3MigBOrch -> hfJobs "Group 1/2: dispatch HF Jobs trainers (5 of 8 — ext_v2 is local)" "huggingface_hub.HfApi.run_uv_job"
        sk3MigBOrch -> extV2Gates "Group 1: dispatch ext_v2_p0/p1 (local) + smoke validation" "uv run python -c (import sentinel) + pytest"
        sk3MigBOrch -> megaJob "Group 1/2: trigger compute_* mega-job tasks + wait on per-task TERMINATED" "WorkspaceClient.jobs.run_now"
        sk3MigBOrch -> hfOutputPublishers "Step 3: dispatch output-publish (depends on retrain results)" "subprocess uv run"
        sk3MigBOrch -> telemetryTable "Appends cycle_item + heartbeat rows + final cost_usd" "INSERT INTO (statement_execution)"
        sk3MigBOrch -> lakebase "Refreshes synced tables + restores indexes per cycle item" "scripts/maintain_synced_tables.py"

        // Data flow
        megaJob -> bronze "Writes raw + SPADL actions + telemetry" "Spark Delta writes"
        bronze -> silverGold "Sourced by staging; gold materialised" "dbt models (3-stage build)"
        silverGold -> lakebase "Synced via Databricks Synced Tables" "Snapshot replication"

        // HF-Jobs trainer artifact delivery (ADR-012 three-destination contract)
        hfJobs -> mlflow "Registers model versions + verifies @Champion (ADR-012 §1)" "set_and_verify_mlflow_champion"
        hfJobs -> ucVolume "Uploads weight bytes + SHA-256 sidecar (ADR-012 §1)" "upload_weights_to_uc_volume"
        hfJobs -> hfhub "Publishes weights + README (ADR-014)" "upload_hf_readme"

        // Trainer input data sources
        hfJobs -> hfhub "Reads training datasets from HF Hub (vaep, xg_v2, f2v_v2, f2v_360, scoutgpt)" "datasets.load_dataset"
        hfJobs -> silverGold "f2v_v1 reads fct_action_values directly via SQL (γ-precursor); PR-2 extends to f2v_v2/f2v_360/scoutgpt" "Statement Execution + Arrow EXTERNAL_LINKS"

        // Group 0 + Group 3 publishers
        hfInputPublishers -> silverGold "Reads gold marts (fct_action_values, fct_shots, fct_shot_freeze_frames)" "Statement Execution"
        hfInputPublishers -> hfhub "Publishes input datasets" "HfApi.upload_folder"
        hfOutputPublishers -> silverGold "Reads gold marts (post-retrain)" "Statement Execution"
        hfOutputPublishers -> hfhub "Publishes output datasets + cards" "HfApi.upload_folder + upload_hf_readme"

        // ext_v2 smoke-gate dataflow
        extV2Gates -> silverGold "Fetches fct_action_values via chunked Arrow stream (~10M rows)" "Statement Execution EXTERNAL_LINKS"

        // Mega-job inference path (consumers of ADR-012 artifacts)
        megaJob -> mlflow "Loads @Champion at inference" "Pyfunc + UC Volume fallback"
        megaJob -> ucVolume "Reads weight bytes (fallback)" "WorkspaceClient.files.download"
        megaJob -> hfhub "SHA freshness probe" "HfApi.dataset_info / model_info"

        // CI gate (currently DB-gated tests skip; PR-1.5 wires creds)
        ciSentinels -> sk3MigBOrch "Imports orchestrator + asserts _TASK_KEY_MAP / _FLAVOR_MAP / _LOCAL_TRAINED_MODELS" "importlib introspection"
        ciSentinels -> hfJobs "Imports each trainer + asserts VALIDATED_HF_FLAVOR + _REQUIRED_SK_MIN" "importlib introspection"

        // Taipy serving
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
