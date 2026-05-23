# Decision: Lakebase as Interactive Read-Serving Layer

**Status:** Accepted
**Date:** 2026-04-02

## Context

The dashboard requires sub-10ms query latency for interactive filter cascades and visualizations. The primary data store is Delta Lake on Databricks. Querying it directly via a Databricks SQL warehouse introduces 2-5 second cold-start latency (warehouse auto-stop after 10 minutes of inactivity) and adds cluster-compute cost per interactive request. Neither is acceptable for a dashboard where users change filters frequently.

The platform also has vector similarity search requirements: player embeddings (192d football2vec, 208d football2vec-360) need approximate nearest-neighbor queries to power the Player Similarity page.

## Decision

Use Lakebase (Databricks-managed PostgreSQL 17) as the read-serving layer. Mart tables are synced from Delta Lake to Lakebase via Databricks sync jobs. The dashboard connects via standard psycopg2 wire protocol. Vector search uses pgvector HNSW indexes (6 indexes: 4×192d + 2×208d). Custom B-tree indexes (66 total) cover all filtered columns on fact tables exceeding 100K rows.

## Alternatives Considered

| Option | Assessment |
|--------|------------|
| Direct Databricks SQL warehouse | Cold-start latency (2-5s) unacceptable for interactive use; cluster cost per request |
| External PostgreSQL (RDS, Cloud SQL) | Requires managing a separate service, credentials, network peering, and sync pipeline |
| DuckDB embedded in app container | No pgvector support; no multi-user concurrency; state lost on container restart |
| Pinecone / Weaviate | Overkill for 6 vector indexes; adds a third data service to operate |

## Consequences

**Positive:**
- Sub-10ms reads on indexed columns, meeting the interactive performance budget.
- pgvector HNSW indexes support cosine-similarity player search without a separate vector store.
- Standard psycopg2 wire protocol — no proprietary SDK in the dashboard.
- Scale-to-zero: Lakebase pauses when idle, reducing cost.

**Negative:**
- Eventual consistency: synced tables lag Delta Lake by seconds to minutes depending on sync frequency. Dashboard data is not real-time.
- Custom PG indexes are dropped when a synced table is recreated. `scripts/create_indexes.py` must be re-run after any synced table rebuild.
- Indexes must be created WITHOUT the `ONLY` keyword — Lakebase synced tables use internal child partitions (`__db_system.partition_*`) and parent-only indexes are invisible to the query planner.
