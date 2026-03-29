# Glossary

Domain terminology used throughout the (Right! Luxury!) Lakehouse documentation. Terms are listed alphabetically.

| Term | Definition | Scale / Direction |
|------|-----------|-------------------|
| **applyInPandas** | Spark API that distributes a Python function across groups on executors, avoiding driver-bound processing | — |
| **Bronze / Silver / Gold** | Medallion architecture layers: Bronze = raw ingested data, Silver = cleaned/standardized, Gold = analytics-ready aggregates | — |
| **DEFCON** | Defensive contribution credit assignment — quantifies each defender's impact on preventing goals | Higher = more defensive contribution |
| **Delta Lake** | Open-source storage layer on Databricks providing ACID transactions, schema enforcement, and time travel on Parquet files | — |
| **dbt** | Data Build Tool — SQL-based transformation framework used for the Silver → Gold layer | — |
| **ELASTIC** | Event-Tracking Data Synchronization — aligns event timestamps to tracking frames without annotated event locations (Kim et al. 2025) | Confidence 0–1, higher = better alignment |
| **EPTS** | Electronic Performance and Tracking Systems — stadium-installed cameras that capture player positions at 25fps | — |
| **EPV** | Expected Possession Value — the probability that the current possession ends in a goal, given ball position | 0–1, higher = more threatening possession |
| **HSR** | High-Speed Running — distance covered above ~5.5 m/s (provider-specific threshold) | Meters, higher = more high-intensity running |
| **Lakebase** | Databricks product that syncs Delta tables to a managed PostgreSQL endpoint for low-latency analytical queries | — |
| **Medallion architecture** | Data pipeline pattern with three layers (Bronze → Silver → Gold) providing progressive data quality refinement | — |
| **OBSO** | Off-Ball Scoring Opportunities — the scoring opportunity at a pitch location based on pitch control, transition probability, and EPV (Spearman 2018) | 0–1, higher = better scoring opportunity |
| **PAUSA** | Passing Ability Under Spatiotemporal Awareness — decomposes pass quality into temporal judgment (timing) and spatial selection (target choice) (Lee et al. 2026) | 0–1 for each component, higher = better |
| **Pitch Control** | Physics-based model estimating each team's probability of controlling any point on the pitch at a given instant (Spearman 2018) | 0–1, higher = more control by home team |
| **PPDA** | Passes Per Defensive Action — measures pressing intensity; the number of opponent passes allowed per defensive action | Lower = more intense pressing |
| **SPADL** | Simplified Player Action Description Language — a unified event representation that normalizes actions across data providers (Decroos et al. 2019) | — |
| **Synced table** | A Lakebase table that mirrors a Delta table in near-real-time to PostgreSQL | — |
| **UC Volume** | Unity Catalog Volume — Databricks-managed cloud storage path for files (model weights, exports) | — |
| **VAEP** | Valuing Actions by Estimating Probabilities — scores each action by its impact on the probability of scoring/conceding within the next 10 actions (Decroos et al. 2019) | Offensive: higher = better; Defensive: lower = better |
| **xG** | Expected Goals — the probability that a shot results in a goal, given its characteristics (distance, angle, body part, context) | 0–1, higher = better scoring chance |
| **xT** | Expected Threat — data-driven pitch zone valuation via Markov chain value iteration; the probability that a possession reaching a zone leads to a goal (Singh 2018) | 0–1, higher = more threatening zone |
