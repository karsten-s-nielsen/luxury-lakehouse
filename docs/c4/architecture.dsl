workspace "Luxury Lakehouse" "Serverless soccer analytics platform built on Databricks Lakebase, replacing a traditional 6-service AWS pipeline with a unified lakehouse architecture." {

    model {
        // --- Persons ---
        coach = person "Coach / Match Analyst" "Views match dashboards, shot maps, and pass networks to inform tactical decisions"
        scout = person "Scouting Director" "Compares player stats via radar charts and similarity search to identify transfer targets"
        dataScientist = person "Data Scientist" "Runs notebooks against Delta Lake, builds xG and xT models, trains embeddings"
        platformEngineer = person "Platform Engineer" "Provisions infrastructure via Terraform, maintains pipeline, monitors costs"

        // --- External Systems ---
        statsbomb = softwareSystem "StatsBomb Open Data" "REST API and GitHub JSON providing match events, lineups, and 360-degree tracking context for ~3,000 matches" "External"
        metrica = softwareSystem "Metrica Sports" "GitHub CSV repository providing optical tracking data at 25 frames per second for sample matches" "External"
        wyscout = softwareSystem "Wyscout Public Dataset" "JSON event stream dataset covering the top 5 European leagues (2017-18 season)" "External"
        github = softwareSystem "GitHub" "Source control and CI/CD pipeline via GitHub Actions" "External"
        aws = softwareSystem "AWS" "Underlying cloud infrastructure providing S3 storage, IAM, and networking for the Databricks workspace" "External"

        // --- Main System ---
        platform = softwareSystem "Soccer Analytics Platform" "Serverless soccer analytics platform that ingests open-source match data, transforms it through a medallion architecture, and serves interactive dashboards for coaches and analysts" {

            ingestion = container "Ingestion Workflows" "Fetches raw data from StatsBomb, Metrica Sports, and Wyscout APIs, writing to the Bronze layer in Unity Catalog" "Python, statsbombpy, Databricks Serverless Compute"
            catalog = container "Unity Catalog" "Governed data storage across the medallion architecture with Bronze (raw), Silver (cleaned), and Gold (analytics-ready) schemas" "Delta Lake, Apache Parquet" "Database"
            sqlWarehouse = container "Serverless SQL Warehouse" "Executes dbt transformations and ad-hoc analytical queries using the Photon engine" "Databricks Serverless SQL, Photon"
            dbt = container "dbt Project" "Transforms raw Bronze data through Silver staging to Gold analytics tables, including xG features, pass metrics, and player statistics" "dbt-core, dbt-databricks"
            syncedTables = container "Synced Tables Pipeline" "Synchronizes Gold Delta tables into Lakebase via SNAPSHOT scheduling, eliminating Reverse ETL" "Lakeflow Synced Database Tables" "Queue"
            lakebase = container "Lakebase PostgreSQL 17" "Managed OLTP database (CU_1 capacity) providing sub-10ms query latency for the Streamlit app, with native pgvector support for player similarity search" "PostgreSQL 17, Capacity Units, pgvector" "Database"
            streamlit = container "Streamlit Dashboard" "Interactive analytics UI with shot maps, pass networks, player radars, pitch control visualizations, and pgvector similarity search" "Python, Streamlit, mplsoccer, psycopg2, Databricks Apps"
        }

        // --- Relationships: Persons to System ---
        coach -> platform "Views match dashboards and tactical analysis" "HTTPS"
        scout -> platform "Searches for similar players and compares stats" "HTTPS"
        dataScientist -> platform "Queries Delta Lake tables and builds ML models" "Databricks SQL, Notebooks"
        platformEngineer -> platform "Provisions and maintains infrastructure" "Terraform, Databricks CLI"
        platformEngineer -> github "Pushes code and reviews PRs" "Git, HTTPS"

        // --- Relationships: System to External ---
        platform -> statsbomb "Fetches match events, lineups, and 360 data" "REST API, HTTPS"
        platform -> metrica "Fetches optical tracking CSV data" "HTTPS"
        platform -> wyscout "Fetches event stream JSON data" "HTTPS"
        platform -> aws "Runs on" "Databricks on AWS"
        github -> platform "Deploys infrastructure and code changes" "GitHub Actions, Terraform"

        // --- Relationships: Container level ---
        ingestion -> statsbomb "Fetches competitions, matches, events, lineups, 360 data" "statsbombpy, HTTPS"
        ingestion -> metrica "Fetches sample tracking CSV and event data" "requests, HTTPS"
        ingestion -> wyscout "Fetches public event stream JSON" "requests, HTTPS"
        ingestion -> catalog "Writes raw data to Bronze schema" "Delta Lake API"

        dbt -> catalog "Reads Bronze, writes Silver and Gold tables" "Databricks SQL"
        dbt -> sqlWarehouse "Executes SQL transformations on" "dbt-databricks adapter"

        syncedTables -> catalog "Reads Gold Delta tables" "Spark Streaming"
        syncedTables -> lakebase "Continuously syncs analytics tables" "Lakeflow Pipeline"

        streamlit -> lakebase "Queries analytics data with sub-10ms latency" "psycopg2, OAuth M2M"

        coach -> streamlit "Views shot maps, pass networks, match dashboards" "HTTPS"
        scout -> streamlit "Uses radar charts and player similarity search" "HTTPS"
        dataScientist -> catalog "Runs queries and builds models against Delta tables" "Databricks SQL, Notebooks"
        platformEngineer -> aws "Manages infrastructure" "Terraform, AWS Console"
    }

    views {
        systemContext platform "SystemContext" {
            include *
            autoLayout
        }

        container platform "Containers" {
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
            element "Queue" {
                shape Pipe
            }
            element "Component" {
                background #85BBF0
                color #000000
            }
        }
    }

}
