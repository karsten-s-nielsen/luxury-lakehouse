workspace "(Right! Luxury!) Lakehouse" "Serverless soccer analytics platform built on Databricks Lakebase, replacing a traditional 6-service AWS pipeline with a unified lakehouse architecture." {

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

            ingestion = container "Ingestion Workflows" "Fetches raw data from StatsBomb, Metrica Sports, and Wyscout APIs, writing to the Bronze layer in Unity Catalog" "Python, statsbombpy, Databricks Serverless Compute" {
                utilsComp = component "Shared Utilities" "CLI parsing with SQL injection prevention, HTTPS-only HTTP client with retry, structured JSON logging, JSON column serialization, Delta write helpers with table name validation and audit columns, content validation" "Python, requests, pandas"
                sbComp = component "StatsBomb Ingester" "Hierarchical API traversal: competitions, matches, events, lineups, 360 frames. Incremental loading via partition-level overwrite. JSON column serialization." "Python, statsbombpy"
                metricaComp = component "Metrica Ingester" "Downloads tracking CSVs with 3-row multi-line header parsing. Reshapes wide player coordinates to narrow JSON format. Processes 2 sample games." "Python, pandas"
                wyscoutComp = component "Wyscout Ingester" "Local-first loading with Figshare HTTPS fallback. Processes 7 competitions (2017-18). Serializes positions and tags to JSON strings." "Python, requests"
            }
            catalog = container "Unity Catalog" "Governed data storage across the medallion architecture with Bronze (raw), Silver (cleaned), and Gold (analytics-ready) schemas" "Delta Lake, Apache Parquet" "Database"
            sqlWarehouse = container "Serverless SQL Warehouse" "Executes dbt transformations and ad-hoc analytical queries using the Photon engine" "Databricks Serverless SQL, Photon"
            dbt = container "dbt Project" "Transforms raw Bronze data through Silver staging to Gold analytics tables, including xG features, pass metrics, and player statistics" "dbt-core, dbt-databricks" {
                stagingStatsbomb = component "StatsBomb Staging" "4 views: events, shots, matches, lineups. Flattens nested JSON, extracts coordinates and shot attributes" "SQL Views, Silver Schema"
                stagingMetrica = component "Metrica Staging" "2 views: events, tracking. Scales normalized coordinates to 120x80, generates surrogate keys" "SQL Views, Silver Schema"
                stagingWyscout = component "Wyscout Staging" "1 view: events. Decodes tag IDs, maps periods, scales percentage coordinates to 120x80" "SQL Views, Silver Schema"
                intermediate = component "Intermediate Layer" "3 ephemeral CTEs: unified shots, unified passes, minutes played. Cross-source unification with progressive pass detection" "Ephemeral Models"
                factTables = component "Fact Tables" "6 tables: shots, passes, player stats, match summary, tracking frames, player embeddings. xG features, per-90 rates, velocity metrics" "Delta Tables, Gold Schema"
                dimTables = component "Dimension Tables" "3 tables: players, teams, competitions. Deduplicated master data from all sources" "Delta Tables, Gold Schema"
                macros = component "Custom Macros" "distance_to_goal and shot_angle geometry calculations for xG features" "Jinja SQL Macros"
                testSuite = component "Test Suite" "168 data tests: unique, not_null, accepted_values, composite keys, coordinate bounds, source freshness" "dbt-expectations, dbt-utils"
            }
            syncedTables = container "Synced Tables Pipeline" "8 synced tables (5 fact, 3 dimension) replicate Gold Delta tables into Lakebase via SNAPSHOT scheduling, eliminating Reverse ETL. All tables online with verified row counts." "Lakeflow Synced Database Tables, Terraform" "Queue"
            lakebase = container "Lakebase PostgreSQL 16" "Managed OLTP database (CU_1 capacity, running) providing sub-10ms query latency for the Streamlit app, with native pgvector support. OAuth M2M authentication, SSL enforced." "PostgreSQL 16, Capacity Units, pgvector" "Database"
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

        // --- Relationships: Component level (Ingestion) ---
        sbComp -> statsbomb "Fetches competitions, matches, events, lineups, 360 frames" "statsbombpy API"
        sbComp -> utilsComp "Uses HTTP client, Delta writer, logging, validation" ""
        metricaComp -> metrica "Downloads tracking and event CSVs" "fetch_url (HTTPS)"
        metricaComp -> utilsComp "Uses HTTP client, Delta writer, logging, validation" ""
        wyscoutComp -> wyscout "Downloads event and match JSON" "fetch_url (HTTPS)"
        wyscoutComp -> utilsComp "Uses HTTP client, Delta writer, logging, validation" ""
        utilsComp -> catalog "Writes DataFrames to Bronze Delta tables with audit columns" "PySpark, Delta Lake API"

        // --- Relationships: Component level (dbt) ---
        stagingStatsbomb -> catalog "Reads Bronze statsbomb tables, writes Silver views" "Databricks SQL"
        stagingMetrica -> catalog "Reads Bronze metrica tables, writes Silver views" "Databricks SQL"
        stagingWyscout -> catalog "Reads Bronze wyscout tables, writes Silver views" "Databricks SQL"
        intermediate -> stagingStatsbomb "Unifies shots, passes, minutes from StatsBomb" ""
        intermediate -> stagingWyscout "Unifies shots and passes from Wyscout" ""
        intermediate -> stagingMetrica "References Metrica tracking data" ""
        macros -> stagingStatsbomb "Provides distance_to_goal, shot_angle calculations" ""
        macros -> intermediate "Provides geometry calculations for unified models" ""
        factTables -> intermediate "Builds gold fact tables from unified data" ""
        factTables -> stagingStatsbomb "Builds match summary and tracking from staging" ""
        factTables -> stagingMetrica "Builds tracking frames from Metrica staging" ""
        dimTables -> stagingStatsbomb "Deduplicates players, teams, competitions" ""
        dimTables -> stagingWyscout "Merges Wyscout team data" ""
        testSuite -> factTables "Validates fact table data quality" "dbt-expectations"
        testSuite -> dimTables "Validates dimension table integrity" "dbt-expectations"
        testSuite -> stagingStatsbomb "Validates staging data quality" "dbt-expectations"
        factTables -> catalog "Writes Gold Delta tables" "Databricks SQL"
        dimTables -> catalog "Writes Gold dimension tables" "Databricks SQL"

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

        component ingestion "IngestionComponents" {
            include *
            autoLayout
        }

        component dbt "dbtComponents" {
            include *
            autoLayout
        }

        dynamic platform "IngestionFlow" {
            ingestion -> statsbomb "Fetches competitions, matches, events, lineups, 360 frames"
            ingestion -> metrica "Downloads tracking CSV and event data"
            ingestion -> wyscout "Downloads event and match JSON from UC Volume"
            ingestion -> catalog "Writes 9 bronze Delta tables with audit columns"
            dbt -> catalog "Transforms Bronze to Silver to Gold"
            syncedTables -> catalog "Reads Gold Delta tables"
            syncedTables -> lakebase "Syncs analytics tables via Lakeflow"
            streamlit -> lakebase "Queries analytics data"
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
