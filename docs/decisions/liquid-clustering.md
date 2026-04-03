# Decision: Liquid Clustering over Z-Ordering for Mart Tables

**Status:** Accepted
**Date:** 2026-04-02

## Context

Delta Lake tables benefit from data layout optimization: files are organized so that filter predicates skip as many files as possible (data skipping). The platform's mart tables are written by dbt and queried through both Databricks SQL and Lakebase synced tables. As query patterns evolve across development cycles, the optimal clustering columns are not fixed — new pages and metrics introduce new filter predicates.

## Decision

Use liquid clustering (`liquid_clustered_by` in dbt model config) on all mart tables instead of Z-ordering. All mart tables also have `delta.autoOptimize.autoCompact` and `delta.autoOptimize.optimizeWrite` enabled via dbt `tblproperties`. Predictive Optimization is enabled at catalog level (`enable_predictive_optimization = "ENABLE"` in Terraform) and handles auto-VACUUM and auto-OPTIMIZE on Unity Catalog managed tables.

## Alternatives Considered

| Option | Assessment |
|--------|------------|
| Z-ordering | Requires manual `OPTIMIZE ... ZORDER BY` runs after data loads; column selection is static and must be revisited as query patterns evolve; does not adapt incrementally |
| No clustering | Full file scans on large fact tables (e.g., `fct_events` with 3M+ rows); acceptable only for small dimension tables |
| Hive-style partitioning | Effective for a single high-cardinality key (e.g., `match_id`) but degrades for multi-dimensional filters; creates small-file problems on low-cardinality partitions |

## Consequences

**Positive:**
- Incremental and automatic: clustering happens progressively as data is written, without scheduled OPTIMIZE jobs.
- Adapts to evolving query patterns without requiring schema or configuration changes.
- Consistent with Databricks Serverless recommendations for Unity Catalog managed tables.

**Negative:**
- Requires Databricks Runtime 13.3 LTS or later. This is already above the platform's minimum serverless version and is not a practical constraint.
- Liquid clustering is a Databricks proprietary feature; migrating to open-source Delta Lake would require switching to Z-ordering or partitioning.
