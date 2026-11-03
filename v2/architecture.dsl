workspace "Luxury Lakehouse v2" "Serverless soccer-data lakehouse (DuckDB + DuckLake) with a Hugging Face operator app and a scheduled ingest Job." {

    model {
        operator = person "Operator" "Signs in with Hugging Face OAuth (allow-list) to run ingests, edit filters, review links and manage the schedule."
        viewer = person "Public viewer" "Anonymous visitor; may view only the monitoring page (spend and layer dependencies, no match identity)."

        lakehouse = softwareSystem "Lakehouse v2" "Ingests open soccer data into a local or HF lake, links entities across providers and publishes gold dbt marts." {
            opsSpace = container "Ops Space" "Taipy operator app; a socket app at / behind HF OAuth. Holds the lake-only data token and a confined job-control token (ADR-014)." "Python, Taipy, Flask" {
                shell = component "UI shell + template" "Builds each page from a PageConfig; one by-name control on_change dispatcher (ADR-015); reaches the lake only via services." "Python"
                services = component "Services layer" "Settings, Modules, Filters, Runs, Review, Schedule. The only path from the UI to the lake (import-linter)." "Python"
                schedule = component "HfJobsSchedule" "Reads LAKEHOUSE_HF_JOBS_TOKEN only here; inspects/reschedules/suspends/resumes/triggers the HF scheduled job." "Python, huggingface_hub"
                auth = component "Web auth + sessions" "HF OAuth sign-in (https callback), allow-list, login registry, per-socket principal." "Python, authlib, Flask"
            }
            jobImage = container "Job image / scheduled run" "The only production writer. Holds the S3 gateway key. Runs under the secret mask." "Python, DuckDB, dbt" {
                runEngine = component "Run engine" "Orders the run: lease, list, fetch, parse, resolve, dbt, checkpoint publish, cleanup." "Python"
                adapters = component "Provider adapters + parsers" "Fetch each provider's deliveries and parse them into exact, typed bronze tables." "Python"
                resolver = component "Entity resolution" "Links players, teams, competitions and matches across providers; feeds the review queue." "Python"
                dbtRunner = component "dbt runner" "Builds the silver and gold dbt models and the contract-enforced dimensions." "dbt, DuckDB"
                catalog = component "Catalog + checkpoint" "Registers files, publishes catalog versions under the single-writer lease, prunes and sweeps." "DuckLake"
            }
            lakeBucket = container "Lake bucket" "Published data, the DuckLake catalog, config and run records. Local folder or hf://buckets/<ns>/lake." "DuckLake on Parquet" "Database"
            restrictedBucket = container "Restricted bucket" "Bronze and raw files of private matches only. Never shared or made public." "Parquet" "Database"
            controlRepo = container "Lease ledger" "Single-writer lease on the HF lake: a private dataset repo whose head advances only by a parent-matched commit (T6)." "HF dataset repo" "Database"
        }

        pining = softwareSystem "Pining API" "The owner's private data API; lists and serves private-tier provider entries (needs an owner or list-only token)." "External"
        providers = softwareSystem "Provider sources" "StatsBomb, Wyscout, IDSSE, SkillCorner, Gradient Sports: public GitHub, figshare, HF dataset and Drive sources." "External"
        hfPlatform = softwareSystem "Hugging Face platform" "OAuth identity, Spaces runtime, Storage Buckets, the S3 gateway and the Jobs API." "External"

        operator -> opsSpace "Signs in and operates" "HTTPS, websocket"
        viewer -> opsSpace "Views monitoring only" "HTTPS"
        opsSpace -> hfPlatform "OAuth sign-in; triggers/reschedules the Job" "HTTPS, Jobs API"
        opsSpace -> lakeBucket "Reads the catalog and config; writes config" "hf:// + HF_TOKEN"
        opsSpace -> jobImage "Run now / schedule triggers the Job" "HF Jobs API"
        jobImage -> providers "Downloads deliveries" "HTTPS"
        jobImage -> pining "Lists and fetches private entries" "HTTPS + token"
        jobImage -> lakeBucket "Writes bronze, catalog, gold marts" "S3 gateway + DuckLake"
        jobImage -> restrictedBucket "Writes private-match files" "S3 gateway"
        jobImage -> controlRepo "Acquires the single-writer lease" "HF commit, parent-matched"

        shell -> services "Reads display data; dispatches actions" "Python call"
        services -> schedule "Reads schedule status; applies changes" "Python call"
        schedule -> hfPlatform "Inspect/reschedule/suspend/resume/trigger" "Jobs API"
        services -> lakeBucket "Reads the catalog and config; writes config" "hf:// + HF_TOKEN"
        auth -> hfPlatform "OAuth authorization-code flow" "HTTPS"

        runEngine -> adapters "Fetches and parses each module" "Python call"
        runEngine -> resolver "Links entities after bronze" "Python call"
        runEngine -> dbtRunner "Builds the gold marts" "Python call"
        runEngine -> catalog "Publishes catalog versions under the lease" "Python call"
        adapters -> restrictedBucket "Writes private-match bronze" "S3 gateway"
        catalog -> lakeBucket "Registers files; publishes versions" "DuckLake"
        catalog -> controlRepo "Holds the single-writer lease" "HF commit"

        production = deploymentEnvironment "Hugging Face" {
            deploymentNode "Hugging Face" "The HF platform" "SaaS" {
                deploymentNode "karstenskyt/ops Space" "Docker Space, 1 replica, OAuth" "Docker" {
                    opsInstance = containerInstance opsSpace
                }
                deploymentNode "Scheduled Job (lakehouse-ingest)" "cpu-basic, cron 0 3 * * * UTC, --no-concurrency, --timeout 21600" "HF Jobs" {
                    jobInstance = containerInstance jobImage
                }
                deploymentNode "luxury-lakehouse org storage" "HF Storage Buckets + dataset repo" "AES-256 at rest" {
                    lakeInstance = containerInstance lakeBucket
                    restrictedInstance = containerInstance restrictedBucket
                    controlInstance = containerInstance controlRepo
                }
            }
        }

        local = deploymentEnvironment "Local / Mac" {
            deploymentNode "Operator machine (Windows / macOS / Linux)" "uv-run CLI and local Taipy app; native OS scheduled task" "uv, Python 3.12" {
                localOps = containerInstance opsSpace
                localJob = containerInstance jobImage
                deploymentNode "Local disk" "A lake folder and a sibling restricted folder" "Filesystem" {
                    localLake = containerInstance lakeBucket
                    localRestricted = containerInstance restrictedBucket
                }
            }
        }
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

        component opsSpace "Component_opsSpace" {
            include *
            autoLayout
        }

        component jobImage "Component_jobImage" {
            include *
            autoLayout
        }

        dynamic lakehouse "Dynamic_run" {
            operator -> opsSpace "Clicks Run now (or the schedule fires)"
            opsSpace -> hfPlatform "Triggers the scheduled Job"
            jobImage -> controlRepo "Acquires the single-writer lease"
            jobImage -> providers "Downloads what changed"
            jobImage -> pining "Lists and fetches private entries"
            jobImage -> restrictedBucket "Writes private-match files"
            jobImage -> lakeBucket "Writes bronze, builds dbt marts, publishes a catalog version"
            opsSpace -> lakeBucket "Reads the new catalog for the Runs and Monitoring pages"
            autoLayout
        }

        deployment lakehouse production "Deployment_HuggingFace" {
            include *
            autoLayout
        }

        deployment lakehouse local "Deployment_Local" {
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
            relationship "Relationship" {
                color #707070
            }
        }
    }

}
