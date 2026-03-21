workspace "Taipy Soccer Analytics" "Taipy-based soccer analytics dashboard replacing the Streamlit UI. Connects to Databricks Lakebase (PostgreSQL) for all data." {

    model {
        analyst = person "Soccer Analyst" "Coaches, scouts, and analysts exploring match and player data"

        taipyApp = softwareSystem "Taipy Dashboard" "Interactive soccer analytics application with 12 pages covering shots, passes, networks, player comparison, pitch control, and defensive metrics" {
            guiLayer = container "Taipy GUI" "Root template with sidebar navigation, filter cascade, and page routing" "Python, Taipy 4.1"
            pageTemplates = container "Page Templates" "12 page layout definitions using build_page() pattern" "Python, Taipy Markdown"
            stateModules = container "State Modules" "Per-page state variables, callbacks, data fetching, and chart rendering" "Python, pandas, mplsoccer"
            filterLayer = container "Filter Layer" "Shared filter queries with TTL cache and scope/freshness helpers" "Python, psycopg2"
            dbLayer = container "DB Layer" "OAuth token management, connection pooling, parameterized query execution" "Python, psycopg2, Databricks SDK"
            renderEngine = container "Render Engine" "Matplotlib figure-to-PNG with cache-busting paths" "Python, matplotlib"
            configLayer = container "Config" "Pydantic settings from environment variables" "Python, pydantic-settings"
        }

        lakebase = softwareSystem "Databricks Lakebase" "PostgreSQL-compatible endpoint syncing Delta Lake tables from Unity Catalog" "External"
        databricksApi = softwareSystem "Databricks REST API" "OAuth credential endpoint for Lakebase authentication" "External"
        hfHub = softwareSystem "HuggingFace Hub" "Hosts football2vec embedding models used for player similarity" "External"

        # Relationships - user
        analyst -> guiLayer "Browses pages, selects filters, views charts" "WebSocket/HTTP"

        # Relationships - internal
        guiLayer -> pageTemplates "Routes to page layouts" ""
        guiLayer -> stateModules "Binds state variables and triggers callbacks" ""
        pageTemplates -> stateModules "References state variables for rendering" ""
        stateModules -> filterLayer "Fetches filter options and scope labels" ""
        filterLayer -> dbLayer "Queries dimension tables" "SQL"
        stateModules -> dbLayer "Executes parameterized SQL queries" ""
        stateModules -> renderEngine "Generates pitch and chart PNGs" ""
        renderEngine -> guiLayer "Returns image file paths for binding" ""
        dbLayer -> configLayer "Reads Lakebase host and endpoint settings" ""

        # Relationships - external
        dbLayer -> lakebase "Queries synced tables via SQL" "PostgreSQL/SSL"
        dbLayer -> databricksApi "Fetches OAuth tokens" "HTTPS/REST"
        stateModules -> hfHub "Loads embedding vectors for similarity search" "HTTPS"
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
            stateModules -> renderEngine "Re-renders chart with new data"
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
