# Glossary

Domain terminology used throughout the (Right! Luxury!) Lakehouse documentation. Terms are listed alphabetically.

| Term | Definition | Scale / Direction |
|------|-----------|-------------------|
| **applyInPandas** | Spark API that distributes a Python function across groups on executors, avoiding driver-bound processing | — |
| **Bronze / Silver / Gold** | Medallion architecture layers: Bronze = raw ingested data, Silver = cleaned/standardized, Gold = analytics-ready aggregates | — |
| **DEFCON** | Defensive contribution credit assignment — quantifies each defender's impact on preventing goals | Higher = more defensive contribution |
| **Delta Lake** | Open-source storage layer on Databricks providing ACID transactions, schema enforcement, and time travel on Parquet files | — |
| **dbt** | Data Build Tool — SQL-based transformation framework used for the Silver → Gold layer | — |
| **Convex Hull** | Smallest polygon enclosing all outfield players. Area indicates territorial extent (~1,000 m&sup2; defending, ~1,500 m&sup2; attacking) | m&sup2;, larger = more spread |
| **EFPI** | Elastic Formation and Position Identification — template matching algorithm for automatic formation detection (Bekkers & Dabadghao 2025) | — |
| **ELASTIC** | Event-Tracking Data Synchronization — aligns event timestamps to tracking frames without annotated event locations (Kim et al. 2025) | Confidence 0–1, higher = better alignment |
| **EPTS** | Electronic Performance and Tracking Systems — stadium-installed cameras that capture player positions at 25fps | — |
| **EPV** | Expected Possession Value — the probability that the current possession ends in a goal, given ball position | 0–1, higher = more threatening possession |
| **Football2vec** | Player embedding model that converts match event sequences into fixed-length behavioral vectors. V1: Doc2Vec (32-dim). V2: Transformer encoder (128-dim) with adversarial team debiasing (Ganin GRL) | — |
| **GRL (Gradient Reversal Layer)** | Adversarial training technique (Ganin et al. 2016) that prevents a model from learning a confounding signal (e.g., team identity) by reversing gradients from a discriminator head | — |
| **HSR** | High-Speed Running — distance covered above ~5.5 m/s (provider-specific threshold) | Meters, higher = more high-intensity running |
| **Lakebase** | Databricks product that syncs Delta tables to a managed PostgreSQL endpoint for low-latency analytical queries | — |
| **Line-Breaking Pass** | A pass that penetrates at least one defensive line, detected via Ward clustering on StatsBomb 360 freeze-frame defender positions | — |
| **Medallion architecture** | Data pipeline pattern with three layers (Bronze → Silver → Gold) providing progressive data quality refinement | — |
| **OBSO** | Off-Ball Scoring Opportunities — the scoring opportunity at a pitch location based on pitch control, transition probability, and EPV (Spearman 2018) | 0–1, higher = better scoring opportunity |
| **PAUSA** | Passing Ability Under Spatiotemporal Awareness — decomposes pass quality into temporal judgment (timing) and spatial selection (target choice) (Lee et al. 2026) | 0–1 for each component, higher = better |
| **Pitch Control** | Physics-based model estimating each team's probability of controlling any point on the pitch at a given instant (Spearman 2018) | 0–1, higher = more control by home team |
| **Position Map** | 5x5 time-in-position matrix per player per match, showing which tactical role a player occupied (from shape graph position assignments). Three phase variants: all, in-possession, out-of-possession | Proportion 0–1, higher = more time in that position |
| **PPDA** | Passes Per Defensive Action — measures pressing intensity; the number of opponent passes allowed per defensive action | Lower = more intense pressing |
| **Shape Graph** | Delaunay-based geometric formation detection — builds a stable proximity graph from player positions without formation templates (Sotudeh 2026). Decomposes formation into vertical and horizontal positions | — |
| **SPADL** | Simplified Player Action Description Language — a unified event representation that normalizes actions across data providers (Decroos et al. 2019) | — |
| **Stretch Index** | Mean distance of all outfield players from the team centroid (Bourbousson et al. 2010). Measures team compactness | Meters, lower = more compact |
| **Synced table** | A Lakebase table that mirrors a Delta table in near-real-time to PostgreSQL | — |
| **Team Shape** | Spatial metrics describing how a team is spread across the pitch — length, width, hull area, stretch index, defensive line height, inter-line gaps | — |
| **UC Volume** | Unity Catalog Volume — Databricks-managed cloud storage path for files (model weights, exports) | — |
| **VAEP** | Valuing Actions by Estimating Probabilities — scores each action by its impact on the probability of scoring/conceding within the next 10 actions (Decroos et al. 2019) | Offensive: higher = better; Defensive: lower = better |
| **xG** | Expected Goals — the probability that a shot results in a goal, given its characteristics (distance, angle, body part, context) | 0–1, higher = better scoring chance |
| **xT** | Expected Threat — data-driven pitch zone valuation via Markov chain value iteration; the probability that a possession reaching a zone leads to a goal (Singh 2018) | 0–1, higher = more threatening zone |
