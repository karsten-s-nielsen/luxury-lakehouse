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
        idsse = softwareSystem "IDSSE Bundesliga Dataset" "Figshare collection providing DFL position tracking XML at 25fps for 7 Bundesliga matches (CC-BY 4.0)" "External"
        skillcorner = softwareSystem "SkillCorner Open Data" "GitHub JSONL repository providing broadcast tracking data at 10fps for 10 A-League matches (MIT)" "External"
        github = softwareSystem "GitHub" "Source control and CI/CD pipeline via GitHub Actions with OIDC federation for secretless authentication to AWS and Databricks" "External"
        hfHub = softwareSystem "HuggingFace Hub" "Model registry hosting the football2vec Doc2Vec checkpoint and z-score parameters at luxury-lakehouse/football2vec-statsbomb-wyscout" "External"
        aws = softwareSystem "AWS" "Underlying cloud infrastructure providing S3 storage (KMS CMK-encrypted state), IAM OIDC provider, and networking for the Databricks workspace" "External"

        // --- Main System ---
        platform = softwareSystem "Soccer Analytics Platform" "Serverless soccer analytics platform that ingests open-source match data, transforms it through a medallion architecture, and serves interactive dashboards for coaches and analysts" {

            ingestion = container "Ingestion Workflows" "Fetches raw data from StatsBomb, Metrica, Wyscout, IDSSE, and SkillCorner, writing to the Bronze layer in Unity Catalog. Includes SPADL/VAEP pipeline reading from bronze." "Python, statsbombpy, xml.etree, kloppy, Databricks Serverless Compute" {
                utilsComp = component "Shared Utilities" "CLI parsing with SQL injection prevention, HTTPS-only HTTP client with retry and SQLite response caching (requests-cache), structured JSON logging, JSON column serialization, Delta write helpers (replaceWhere partitioned writes + MERGE upsert) with table name validation and audit columns, content validation" "Python, requests, requests-cache, pandas"
                sbComp = component "StatsBomb Ingester" "Hierarchical API traversal: competitions, matches, events, lineups, 360 frames. Incremental loading via partition-level overwrite. JSON column serialization. Enriches events with _raw_extra_json for SPADL." "Python, statsbombpy"
                metricaComp = component "Metrica Ingester" "Downloads tracking CSVs (Games 1-2) and EPTS XML+tracking+JSON (Game 3). Reshapes wide player coordinates to narrow JSON format. Normalizes EPTS center-origin meters to [0,1] convention. 25fps, frame_rate column." "Python, pandas, xml.etree"
                wyscoutComp = component "Wyscout Ingester" "Local-first loading with Figshare HTTPS fallback. Processes 7 competitions (2017-18). Serializes positions and tags to JSON strings." "Python, requests"
                idsseComp = component "IDSSE Ingester" "Parses 7 pre-downloaded Bundesliga DFL position XML files from UC Volume using two-pass iterparse. Converts XY arrays to narrow rows with center-origin meters. Savitzky-Golay smoothing + pitch clamping via analytics.smoothing. 25fps, prefixed match IDs (idsse_*)." "Python, xml.etree.ElementTree (stdlib)"
                skillcornerComp = component "SkillCorner Ingester" "Downloads 10 A-League matches via kloppy load_open_data. Per-match processing to prevent serverless OOM. Iterates TrackingDataset Frame objects to narrow rows with center-origin meters. Savitzky-Golay smoothing + pitch clamping via analytics.smoothing. 10fps, prefixed match IDs (skillcorner_*)." "Python, kloppy"
                spadlAdapter = component "SPADL Adapter" "Transforms bronze event tables into socceraction-compatible DataFrames. Adapters for StatsBomb (column rename, extra dict, home_team_id) and Wyscout (period mapping, milliseconds, JSON parsing)." "Python, pandas"
                spadlVaep = component "SPADL/VAEP Pipeline" "4-phase pipeline: read bronze events, convert to SPADL actions via socceraction, extract features and train XGBoost VAEP models, score all actions. Incremental processing, model persistence to UC Volumes." "Python, socceraction, xgboost"
                lineBreakingComp = component "Line-Breaking Pipeline" "Batch computation: reads 360 freeze frames (Path A) and Metrica tracking+events (Path B), calls detect_line_breaking() per pass, writes line_breaking_results to Bronze via Delta MERGE on event_id for structural deduplication." "Python, analytics.line_breaking"
                offBallXtComp = component "Off-Ball xT Pipeline" "Batch computation: reads gold fct_tracking_frames and xT grid seed, computes pitch_control × xT per player per frame at 1fps sampling, writes off_ball_xt_results to Bronze with replaceWhere." "Python, analytics.off_ball_xt"
                defconLitePipeline = component "DEFCON-lite Pipeline" "Batch computation: reads 360 freeze frames (StatsBomb) and tracking data (Metrica), calls assign_defensive_credits() per action, estimates DEFCON values via XGBoost, writes defcon_results to Bronze with replaceWhere." "Python, analytics.defcon_lite"
                entityResComp = component "Entity Resolution Pipeline" "Cross-source player identity matching: reads StatsBomb and Wyscout player tables from Bronze, runs three-layer progressive resolution (TF-IDF + rapidfuzz), writes player_xref_raw to Bronze with replaceWhere." "Python, analytics.entity_resolution"
                embeddingsPipeline = component "Embeddings Pipeline" "Training notebook tokenizes StatsBomb events, trains Doc2Vec (32-dim behavioral embeddings), and publishes to HuggingFace Hub. Ingestion task loads model from UC Volume, infers embeddings for all players, computes 13-dim z-score stat vectors, writes per-match embeddings to Bronze." "Python, analytics.football2vec, gensim, huggingface_hub"
            }
            analytics = container "Analytics Models" "Pure-Python analytics library providing physics-based pitch control (Spearman 2017), line-breaking pass detection (Ward clustering + straddle test), and off-ball expected threat valuation" "Python, NumPy, SciPy" {
                pitchControlModel = component "Pitch Control Model" "Spearman (2017) physics-based model: kinematic TTI, logistic sigmoid influence, vectorized 50x32 grid computation. PitchControlParams frozen dataclass." "pitch_control.py, NumPy"
                lineBreakingModel = component "Line-Breaking Model" "Ward hierarchical clustering of opponent positions into 3 defensive lines, cross-product straddle test for pass-line intersection. Dual data paths: StatsBomb 360 freeze frames and Metrica tracking." "line_breaking.py, NumPy, SciPy"
                offBallXtModel = component "Off-Ball xT Model" "Weights pitch control by Expected Threat grid (Karun Singh 2018) to quantify off-ball player contributions. 1fps sampling, per-player per-match aggregation." "off_ball_xt.py, NumPy"
                defconLiteModel = component "DEFCON-lite Model" "Heuristic defensive credit assignment (intercept/concede/disturb/deter) based on Kim et al. (2025). XGBoost confidence scoring. Tier 3 tabular approximation of full GNN DEFCON." "defcon_lite.py, NumPy, XGBoost"
                entityResModel = component "Entity Resolution Model" "Three-layer progressive player matching: Layer 1 strict (name+DOB+jersey+team), Layer 2 standard (name+DOB), Layer 3 relaxed (name+position). TF-IDF character n-grams with sparse_dot_topn blocking, rapidfuzz multi-attribute scoring, bidirectional validation." "entity_resolution.py, rapidfuzz, sparse_dot_topn, sklearn"
                football2vecModel = component "Football2Vec Model" "Doc2Vec behavioral embeddings (32-dim) trained on tokenized StatsBomb event sequences per player per match. Tokenizer converts events to action_bodypart_result trigrams. Z-score normalization for 13-dim statistical vectors." "football2vec.py, gensim Doc2Vec, NumPy"
                smoothingModel = component "Position Smoothing" "Savitzky-Golay filter (window=7, polyorder=2) applied per player per period to reduce tracking sensor noise in x,y positions. Downstream velocity and acceleration derivatives are naturally cleaner. Short sequences pass through unmodified." "smoothing.py, SciPy"
            }
            catalog = container "Unity Catalog" "Governed data storage across the medallion architecture with Bronze (raw), Silver (cleaned), and Gold (analytics-ready) schemas" "Delta Lake, Apache Parquet" "Database"
            sqlWarehouse = container "Serverless SQL Warehouse" "Executes dbt transformations and ad-hoc analytical queries using the Photon engine" "Databricks Serverless SQL, Photon"
            dbt = container "dbt Project" "Transforms raw Bronze data through Silver staging to Gold analytics tables, including xG features, pass metrics, player statistics, and multi-source tracking frames" "dbt-core, dbt-databricks" {
                stagingStatsbomb = component "StatsBomb Staging" "5 views: events, shots, matches, lineups, 360 freeze frames. Flattens nested JSON, extracts coordinates, deduplicates bronze data" "SQL Views, Silver Schema"
                stagingMetrica = component "Metrica Staging" "2 views: events, tracking. Scales normalized coordinates to 120x80, adds source_provider and frame_rate columns" "SQL Views, Silver Schema"
                stagingWyscout = component "Wyscout Staging" "1 view: events. Decodes tag IDs, maps periods, scales percentage coordinates to 120x80" "SQL Views, Silver Schema"
                stagingIdsse = component "IDSSE Staging" "1 view: tracking. Transforms center-origin meters to 120x80 coordinate system, adds source_provider (idsse) and frame_rate (25)" "SQL Views, Silver Schema"
                stagingSkillcorner = component "SkillCorner Staging" "1 view: tracking. Transforms center-origin meters to 120x80 coordinate system, adds source_provider (skillcorner) and frame_rate (10)" "SQL Views, Silver Schema"
                stagingSpadl = component "SPADL Staging" "1 view: action values. Deduplicates VAEP scores from bronze, casts types, adds audit columns" "SQL Views, Silver Schema"
                stagingLineBreaking = component "Line-Breaking Staging" "1 view: line-breaking results. Deduplicates on event_id via ROW_NUMBER by _ingested_at DESC" "SQL Views, Silver Schema"
                stagingOffBallXt = component "Off-Ball xT Staging" "1 view: off-ball xT results. Deduplicates on (player_id, match_id) via ROW_NUMBER by _ingested_at DESC" "SQL Views, Silver Schema"
                stagingDefcon = component "DEFCON Staging" "1 view: DEFCON-lite results. Deduplicates on (event_id, defender_player_id, data_source) via ROW_NUMBER by _ingested_at DESC" "SQL Views, Silver Schema"
                stagingEntityRes = component "Entity Resolution Staging" "1 source definition: player_xref_raw. References Bronze entity_resolution schema for cross-source player identity matches." "SQL Source, Bronze Schema"
                stagingEmbeddings = component "Embeddings Staging" "1 view: player embeddings. Deduplicates on (canonical_player_id, match_id) via ROW_NUMBER by _ingested_at DESC, casts vector arrays" "SQL Views, Silver Schema"
                intermediate = component "Intermediate Layer" "4 ephemeral CTEs: unified shots, unified passes, minutes played, player cross-reference (int_player_xref). Cross-source unification with progressive pass detection and entity resolution override merging." "Ephemeral Models"
                factTables = component "Fact Tables" "13 tables: shots, passes (line-breaking), player stats (VAEP, LB, DEFCON per-90), match summary (PPDA), tracking frames (3 sources), player embeddings (per-match, per-season, per-career with dual vectors), action values, physical stats (off-ball xT), defensive values, DEFCON actions, DEFCON pressure (attacker perspective). All tables use liquid clustering, model contracts, auto-compaction, and optimizeWrite." "Delta Tables, Gold Schema"
                dimTables = component "Dimension Tables" "3 tables: players (11,918 unified via canonical_player_id with entity resolution), teams, competitions. Deduplicated master data from all sources with cross-source identity mapping." "Delta Tables, Gold Schema"
                macros = component "Custom Macros" "distance_to_goal and shot_angle geometry calculations for xG features" "Jinja SQL Macros"
                testSuite = component "Test Suite" "381 data tests: unique, not_null, accepted_values, range bounds, composite keys, relationships, source freshness" "dbt-expectations, dbt-utils"
            }
            syncedTables = container "Synced Tables Pipeline" "16 synced tables (13 fact, 3 dimension) replicate Gold Delta tables into Lakebase via SNAPSHOT scheduling. 34 indexes (30 btree + 4 HNSW) across fact tables for sub-100ms queries." "Lakeflow Synced Database Tables, Terraform" "Queue"
            lakebase = container "Lakebase PostgreSQL 17 (Autoscaling)" "Managed OLTP database with autoscaling (0.5–4 CU) and scale-to-zero, providing sub-10ms query latency for the Streamlit app, with native pgvector support for cosine-distance player similarity search. OAuth M2M authentication, SSL enforced." "PostgreSQL 17, Autoscaling, pgvector" "Database"
            streamlit = container "Streamlit Dashboard" "Interactive analytics dashboard deployed as a Databricks App with 11 pages covering event analysis, player comparison, player similarity search, movement analysis, defensive pressure, and multi-source tracking visualization" "Python, Streamlit, mplsoccer, psycopg2, Databricks Apps" {
                appEntry = component "App Entry Point" "st.navigation page routing, dark theme, sidebar branding" "app.py, Streamlit 1.36+"
                configComp = component "Configuration" "Pydantic BaseSettings with env var binding, identifier validation, cached singleton" "config.py, pydantic-settings"
                dbComp = component "Database Layer" "OAuth M2M token management (SDK + REST fallback), JWT UUID validation, ThreadedConnectionPool with 55-min recycle, parameterized queries, table name validation, statement_timeout, sanitized errors" "db.py, psycopg2, databricks-sdk"
                filtersComp = component "Filter Widgets" "5 cascading selectbox/slider widgets backed by Lakebase dimension tables with 10-min cache" "filters.py, st.cache_data"
                pitchComp = component "Pitch Visualizations" "mplsoccer wrappers: shot scatter (sized by xG), pass arrows (progressive + line-breaking highlighting), heat map (bin_statistic density), interactive Plotly pass network with hover tooltips, Voronoi pitch control, physics pitch control (imshow heatmap with RdBu colormap)" "pitch.py, mplsoccer, Plotly, scipy.spatial"
                chartsComp = component "Chart Visualizations" "Radar chart (1-3 players, per-90 metrics), horizontal bar comparison, VAEP action value timeline, and action type breakdown chart" "charts.py, mplsoccer Radar, matplotlib"
                shotMapPage = component "Shot Map Page" "Half-pitch shot visualization with xG sizing, summary stats (goals, conversion rate, xG/shot)" "shot_map.py"
                passMapPage = component "Pass Map Page" "Full-pitch pass arrows with progressive and line-breaking (gold) highlighting, pass completion and line-breaking stats" "pass_map.py"
                radarPage = component "Player Radar Page" "Radar chart comparing 1-3 players across per-90 metrics including VAEP, offensive VAEP, defensive VAEP, and LB Passes/90. Color-coded legend." "player_radar.py"
                matchPage = component "Match Summary Page" "Scorecard header, xG comparison, 8-stat horizontal bar chart" "match_summary.py"
                heatMapPage = component "Heat Map Page" "Action density visualization with competition/team/player/match filters, 3x3 zone stats, supports all-matches aggregation" "heat_map.py"
                passNetworkPage = component "Pass Network Page" "Interactive Plotly passing graph with hover tooltips showing pair counts, min-passes threshold slider, scaled nodes by pass count, edges by pair frequency" "pass_network.py"
                actionValuesPage = component "Action Values Page" "3 views: Player VAEP Rankings table, Action Type Breakdown bar chart, Match Action Timeline. Filters by competition, team, player, match." "action_values.py"
                pitchControlPage = component "Pitch Control Page" "Physics (Spearman 2017) and Voronoi pitch control with model toggle, provider filter (Metrica/IDSSE/SkillCorner), adaptive frame slider step, Home/Away control %, recursive CTE loose index scan" "pitch_control.py"
                movementPage = component "Movement Analysis Page" "3-view page: Physical Performance (distance, HSR, sprints per player), PPDA / Pressing Intensity (bar chart per competition), Off-Ball xT (player ranking per match). Tracking and event data." "movement_analysis.py"
                defPressurePage = component "Defensive Pressure Page" "3-view page: Pressure Rankings (attacker pressure received), Pressure Breakdown (per-match stacked bar), Match Timeline (per-action DEFCON credits). DEFCON-lite attacker perspective." "defensive_valuation.py"
                playerSimilarityPage = component "Player Similarity Page" "pgvector cosine-distance search: type-ahead player dropdown, dual-vector similarity (behavioral + statistical), adjustable minutes threshold, results table with similarity scores, works across StatsBomb and Wyscout sources." "player_similarity.py"
            }
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
        platform -> idsse "Reads pre-downloaded DFL position tracking XML (7 matches)" "xml.etree, UC Volume"
        platform -> skillcorner "Fetches broadcast tracking JSONL (10 matches)" "kloppy, HTTPS"
        platform -> hfHub "Publishes trained football2vec model artifacts for community consumption" "huggingface_hub, HTTPS"
        platform -> aws "Runs on" "Databricks on AWS"
        github -> platform "Deploys infrastructure and code changes" "GitHub Actions OIDC, Terraform"
        github -> aws "Authenticates via OIDC federation" "IAM AssumeRoleWithWebIdentity"

        // --- Relationships: Container level ---
        ingestion -> statsbomb "Fetches competitions, matches, events, lineups, 360 data" "statsbombpy, HTTPS"
        ingestion -> metrica "Fetches sample tracking CSV and event data" "requests, HTTPS"
        ingestion -> wyscout "Fetches public event stream JSON" "requests, HTTPS"
        ingestion -> idsse "Reads pre-downloaded DFL position tracking XML for 7 Bundesliga matches" "xml.etree, UC Volume"
        ingestion -> skillcorner "Fetches broadcast tracking JSONL for 10 A-League matches" "kloppy, HTTPS"
        ingestion -> catalog "Writes raw data to Bronze schema" "Delta Lake API"
        ingestion -> hfHub "Publishes trained football2vec model via training notebook" "huggingface_hub, HTTPS"

        // --- Relationships: Component level (Ingestion) ---
        sbComp -> statsbomb "Fetches competitions, matches, events, lineups, 360 frames" "statsbombpy API"
        sbComp -> utilsComp "Uses HTTP client, Delta writer, logging, validation" ""
        metricaComp -> metrica "Downloads tracking and event CSVs" "fetch_url (HTTPS)"
        metricaComp -> utilsComp "Uses HTTP client, Delta writer, logging, validation" ""
        wyscoutComp -> wyscout "Downloads event and match JSON" "fetch_url (HTTPS)"
        wyscoutComp -> utilsComp "Uses HTTP client, Delta writer, logging, validation" ""
        idsseComp -> idsse "Parses pre-downloaded DFL position XML via two-pass iterparse" "xml.etree.ElementTree"
        idsseComp -> utilsComp "Uses Delta writer, logging, validation" ""
        idsseComp -> analytics "Applies Savitzky-Golay position smoothing + pitch clamping" ""
        skillcornerComp -> skillcorner "Downloads tracking JSONL via kloppy load_open_data" "kloppy, HTTPS"
        skillcornerComp -> utilsComp "Uses Delta writer, logging, validation" ""
        skillcornerComp -> analytics "Applies Savitzky-Golay position smoothing + pitch clamping" ""
        spadlVaep -> spadlAdapter "Transforms bronze events to socceraction format" ""
        spadlVaep -> utilsComp "Uses Delta writer, logging, validation" ""
        spadlAdapter -> catalog "Reads StatsBomb and Wyscout bronze event tables" "PySpark SQL"
        spadlVaep -> catalog "Writes SPADL actions and VAEP scores to Bronze" "PySpark, Delta Lake API"
        lineBreakingComp -> catalog "Reads 360 freeze frames and Metrica tracking+events from Bronze" "PySpark SQL"
        lineBreakingComp -> analytics "Calls detect_line_breaking() per pass" ""
        lineBreakingComp -> utilsComp "Uses Delta writer, logging, validation" ""
        offBallXtComp -> catalog "Reads gold fct_tracking_frames and xT grid seed" "PySpark SQL"
        offBallXtComp -> analytics "Calls compute_off_ball_xt_match() per match" ""
        offBallXtComp -> utilsComp "Uses Delta writer, logging, validation" ""
        defconLitePipeline -> catalog "Reads 360 freeze frames, tracking, and action values from Bronze/Gold" "PySpark SQL"
        defconLitePipeline -> analytics "Calls assign_defensive_credits() and estimate_defcon_values()" ""
        defconLitePipeline -> utilsComp "Uses Delta writer, logging, validation" ""
        entityResComp -> catalog "Reads StatsBomb and Wyscout player tables from Bronze, writes player_xref_raw" "PySpark SQL, Delta Lake API"
        entityResComp -> analytics "Calls resolve_players() with three-layer progressive matching" ""
        entityResComp -> utilsComp "Uses Delta writer, logging, validation" ""
        embeddingsPipeline -> catalog "Reads bronze events and gold fct_player_stats, writes per-match embeddings to Bronze" "PySpark SQL, Delta Lake API"
        embeddingsPipeline -> analytics "Calls football2vec tokenizer, Doc2Vec training, z-score normalization" ""
        embeddingsPipeline -> utilsComp "Uses Delta writer, logging, validation" ""
        embeddingsPipeline -> hfHub "Publishes trained Doc2Vec model and z-score params for community access" "huggingface_hub, HTTPS"
        utilsComp -> catalog "Writes DataFrames to Bronze Delta tables with audit columns" "PySpark, Delta Lake API"

        // --- Relationships: Component level (dbt) ---
        stagingStatsbomb -> catalog "Reads Bronze statsbomb tables, writes Silver views" "Databricks SQL"
        stagingMetrica -> catalog "Reads Bronze metrica tables, writes Silver views" "Databricks SQL"
        stagingWyscout -> catalog "Reads Bronze wyscout tables, writes Silver views" "Databricks SQL"
        stagingIdsse -> catalog "Reads Bronze IDSSE tracking, writes Silver view" "Databricks SQL"
        stagingSkillcorner -> catalog "Reads Bronze SkillCorner tracking, writes Silver view" "Databricks SQL"
        stagingSpadl -> catalog "Reads Bronze VAEP action values, writes Silver view" "Databricks SQL"
        stagingLineBreaking -> catalog "Reads Bronze line-breaking results, writes Silver view" "Databricks SQL"
        stagingOffBallXt -> catalog "Reads Bronze off-ball xT results, writes Silver view" "Databricks SQL"
        stagingDefcon -> catalog "Reads Bronze DEFCON-lite results, writes Silver view" "Databricks SQL"
        stagingEntityRes -> catalog "References Bronze entity_resolution.player_xref_raw" "Databricks SQL"
        stagingEmbeddings -> catalog "Reads Bronze player embeddings, writes Silver view" "Databricks SQL"
        intermediate -> stagingStatsbomb "Unifies shots, passes, minutes from StatsBomb" ""
        intermediate -> stagingWyscout "Unifies shots and passes from Wyscout" ""
        intermediate -> stagingMetrica "References Metrica tracking data" ""
        intermediate -> stagingEntityRes "Merges automated xref matches with manual overrides (int_player_xref)" ""
        macros -> stagingStatsbomb "Provides distance_to_goal, shot_angle calculations" ""
        macros -> intermediate "Provides geometry calculations for unified models" ""
        factTables -> intermediate "Builds gold fact tables from unified data" ""
        factTables -> stagingStatsbomb "Builds match summary and tracking from staging" ""
        factTables -> stagingMetrica "Builds tracking frames from Metrica staging" ""
        factTables -> stagingIdsse "Builds tracking frames from IDSSE staging (UNION ALL)" ""
        factTables -> stagingSkillcorner "Builds tracking frames from SkillCorner staging (UNION ALL)" ""
        factTables -> stagingSpadl "Builds action values fact table from SPADL staging" ""
        factTables -> stagingLineBreaking "LEFT JOINs line-breaking results into fct_passes" ""
        factTables -> stagingOffBallXt "LEFT JOINs off-ball xT results into fct_physical_stats" ""
        factTables -> stagingDefcon "Builds fct_defensive_values, fct_defcon_actions, fct_defcon_pressure from DEFCON staging" ""
        factTables -> stagingEmbeddings "Builds fct_player_embeddings (per-match), fct_player_embeddings_season, fct_player_embeddings_career from embeddings staging" ""
        dimTables -> stagingStatsbomb "Deduplicates players, teams, competitions" ""
        dimTables -> stagingWyscout "Merges Wyscout team and player data" ""
        dimTables -> intermediate "Joins int_player_xref for cross-source identity mapping in dim_players" ""
        testSuite -> factTables "Validates fact table data quality" "dbt-expectations"
        testSuite -> dimTables "Validates dimension table integrity" "dbt-expectations"
        testSuite -> stagingStatsbomb "Validates staging data quality" "dbt-expectations"
        testSuite -> stagingSpadl "Validates SPADL action value data quality" "dbt-expectations"
        testSuite -> stagingLineBreaking "Validates line-breaking result data quality" "dbt-expectations"
        testSuite -> stagingOffBallXt "Validates off-ball xT result data quality" "dbt-expectations"
        testSuite -> stagingDefcon "Validates DEFCON-lite result data quality" "dbt-expectations"
        testSuite -> stagingEntityRes "Validates entity resolution match data quality" "dbt-expectations"
        testSuite -> stagingEmbeddings "Validates embedding vector data quality" "dbt-expectations"
        factTables -> catalog "Writes Gold Delta tables" "Databricks SQL"
        dimTables -> catalog "Writes Gold dimension tables" "Databricks SQL"

        dbt -> catalog "Reads Bronze, writes Silver and Gold tables" "Databricks SQL"
        dbt -> sqlWarehouse "Executes SQL transformations on" "dbt-databricks adapter"

        syncedTables -> catalog "Reads Gold Delta tables" "Spark Streaming"
        syncedTables -> lakebase "Continuously syncs analytics tables" "Lakeflow Pipeline"

        streamlit -> lakebase "Queries analytics data with sub-10ms latency" "psycopg2, OAuth M2M"

        // --- Relationships: Component level (Streamlit) ---
        appEntry -> shotMapPage "Routes to" ""
        appEntry -> passMapPage "Routes to" ""
        appEntry -> radarPage "Routes to" ""
        appEntry -> matchPage "Routes to" ""
        appEntry -> heatMapPage "Routes to" ""
        appEntry -> passNetworkPage "Routes to" ""
        appEntry -> actionValuesPage "Routes to" ""
        appEntry -> pitchControlPage "Routes to" ""
        appEntry -> movementPage "Routes to" ""
        appEntry -> defPressurePage "Routes to" ""
        appEntry -> playerSimilarityPage "Routes to" ""
        appEntry -> configComp "Reads settings" ""
        shotMapPage -> filtersComp "Uses competition, team, player filters" ""
        shotMapPage -> pitchComp "Renders shot scatter" ""
        shotMapPage -> dbComp "Queries fct_shots_synced" ""
        passMapPage -> filtersComp "Uses competition, team, match filters" ""
        passMapPage -> pitchComp "Renders pass arrows" ""
        passMapPage -> dbComp "Queries fct_passes_synced" ""
        radarPage -> filtersComp "Uses competition, team, player multiselect" ""
        radarPage -> chartsComp "Renders radar chart" ""
        radarPage -> dbComp "Queries fct_player_stats_synced" ""
        matchPage -> filtersComp "Uses competition, team, match filters" ""
        matchPage -> chartsComp "Renders bar comparison chart" ""
        matchPage -> dbComp "Queries fct_match_summary_synced" ""
        heatMapPage -> filtersComp "Uses competition, team, player, match filters" ""
        heatMapPage -> pitchComp "Renders action density heat map" ""
        heatMapPage -> dbComp "Queries fct_passes_synced, fct_shots_synced" ""
        passNetworkPage -> filtersComp "Uses competition, team, match filters" ""
        passNetworkPage -> pitchComp "Renders pass network graph" ""
        passNetworkPage -> dbComp "Queries fct_passes_synced, dim_players_synced" ""
        actionValuesPage -> filtersComp "Uses competition, team, player, match filters" ""
        actionValuesPage -> chartsComp "Renders action value timeline and type breakdown" ""
        actionValuesPage -> dbComp "Queries fct_action_values_synced, fct_player_stats_synced" ""
        pitchControlPage -> pitchComp "Renders Voronoi and physics pitch control" ""
        pitchControlPage -> analytics "Computes pitch control surface via Spearman model" ""
        pitchControlPage -> dbComp "Queries fct_tracking_frames_synced" ""
        movementPage -> filtersComp "Uses competition filter for PPDA view" ""
        movementPage -> chartsComp "Renders physical bars and PPDA charts" ""
        movementPage -> dbComp "Queries fct_physical_stats_synced, fct_match_summary_synced" ""
        defPressurePage -> filtersComp "Uses competition and team filters" ""
        defPressurePage -> chartsComp "Renders pressure breakdown bar chart" ""
        defPressurePage -> dbComp "Queries fct_defcon_pressure_synced, fct_defcon_actions_synced" ""
        playerSimilarityPage -> dbComp "Queries fct_player_embeddings_career_synced, fct_player_embeddings_season_synced via pgvector cosine distance" ""
        filtersComp -> dbComp "Queries dimension tables" ""
        dbComp -> configComp "Reads Lakebase connection settings" ""
        dbComp -> lakebase "Connects via OAuth M2M, parameterized SQL" "psycopg2, SSL"

        coach -> streamlit "Views shot maps, pass networks, match dashboards" "HTTPS"
        scout -> streamlit "Uses radar charts and player comparison" "HTTPS"
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

        component analytics "AnalyticsComponents" {
            include *
            autoLayout
        }

        component streamlit "StreamlitComponents" {
            include *
            autoLayout
        }

        dynamic platform "IngestionFlow" {
            ingestion -> statsbomb "Fetches competitions, matches, events, lineups, 360 frames"
            ingestion -> metrica "Downloads tracking CSV and event data"
            ingestion -> wyscout "Downloads event and match JSON from UC Volume"
            ingestion -> idsse "Downloads DFL position XML for 7 Bundesliga matches"
            ingestion -> skillcorner "Downloads broadcast tracking JSONL for 10 A-League matches"
            ingestion -> catalog "Writes 12 bronze Delta tables with audit columns"
            dbt -> catalog "Transforms Bronze to Silver to Gold (381 data tests)"
            syncedTables -> catalog "Reads Gold Delta tables"
            syncedTables -> lakebase "Syncs 16 tables via Lakeflow, 34 indexes (30 btree + 4 HNSW) on partitions"
            streamlit -> lakebase "Queries analytics data with recursive CTE optimization"
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
