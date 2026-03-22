workspace "Taipy Soccer Analytics" "Taipy-based soccer analytics dashboard replacing the Streamlit UI. Connects to Databricks Lakebase (PostgreSQL) for all data." {

    model {
        analyst = person "Soccer Analyst" "Coaches, scouts, and analysts exploring match and player data"

        taipyApp = softwareSystem "Taipy Dashboard" "Interactive soccer analytics application with 12 pages covering shots, passes, networks, player comparison, pitch control, and defensive metrics" {
            guiLayer = container "Taipy GUI" "Root template with sidebar navigation, glossary panels, footer links, and page routing" "Python, Taipy 4.1"
            templateEngine = container "Template Engine" "Generates all page layouts from typed dataclasses: PageConfig, SubView, ContentBlock (image/table/text/expandable_table/chart), ContentRow, SidebarWidget (with help tooltips), Metric (with help_text), Citation. Chart blocks render Plotly figures via native figure= binding. Warning/scope/freshness rendering" "Python, frozen dataclasses"
            sidebarWidgets = container "Sidebar Widgets" "Centralized filter cascade with progressive disclosure, view-dependent visibility, change_delay debounce, and inline help tooltips. Includes metric selectors and search widgets" "Python, Taipy Markdown"
            stateModules = container "State Modules" "Per-page state variables, callbacks, data fetching, chart rendering (12 modules). Static charts via mplsoccer PNG. Interactive charts via Plotly go.Figure (Pass Network, Pass Timing, Defensive Impact Breakdown). Each module manages warning_text, scope_label, data_freshness" "Python, pandas, mplsoccer, Plotly"
            filterLayer = container "Filter Layer" "Shared filter queries with TTL cache, scope labels, data freshness, and embedding player search" "Python, psycopg2"
            dbLayer = container "DB Layer" "OAuth token management, connection pooling, parameterized query execution" "Python, psycopg2, Databricks SDK"
            renderEngine = container "Render Engine" "Matplotlib/mplsoccer figure-to-PNG with cache-busting paths for static pitch diagrams" "Python, matplotlib, mplsoccer"
            pitchControl = container "Pitch Control Engine" "Physics-based (Spearman 2017) and Voronoi pitch control surface computation" "Python, NumPy, SciPy"
            configLayer = container "Config" "Pydantic settings from environment variables" "Python, pydantic-settings"
        }

        lakebase = softwareSystem "Databricks Lakebase" "PostgreSQL-compatible endpoint syncing 19 Delta Lake tables from Unity Catalog" "External"
        databricksApi = softwareSystem "Databricks REST API" "OAuth credential endpoint for Lakebase authentication" "External"
        hfHub = softwareSystem "HuggingFace Hub" "Hosts football2vec embedding models and datasets for player similarity" "External"

        # Relationships - user
        analyst -> guiLayer "Browses pages, selects filters, views interactive and static charts" "WebSocket/HTTP"

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
    }

    views {
        systemContext taipyApp "SystemContext" {
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
