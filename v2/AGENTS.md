# AGENTS.md — Lakehouse v2

Class-1 rules for `v2/`. Each rule points to its detail in the design spec, `v2/docs/specs/2026-10-26-v2-ingestion-design.md` (§ numbers below). The v1 Databricks, Lakebase, Spark and serverless rules of the root `AGENTS.md` do not apply in `v2/`. The root rules on commits, security, failure investigation, STE prose and UI conduct still apply.

## Rules

- **Python 3.12 only** (`>=3.12,<3.13`), with exact pins in `pyproject.toml` and `uv.lock`; dependency overrides are listed and re-checked (§5, §19.6, ADR-008). → spec §5
- **DuckDB 2.0.x and DuckLake 1.1** are the storage engine; bronze is v2's own tuned Parquet files, registered with `ducklake_add_data_files`; `auto_compact = false` on those tables. → spec §10.1, §10.2
- **Follow the write protocol**: write and store the file, register it, commit the catalog version, and never let a reader see a catalog that names a missing file. → spec §10.3, §10.4
- **One writer per lake**: the lease is a chain of generation objects created only if absent; versions are created only if absent; `CURRENT` changes only by compare-and-swap. → spec §10.6, §29.9
- **Two storage roots on every install** (`lake`, `restricted`): every bronze file of a private match and every raw file of a private delivery lives only in the restricted root. → spec §9, §10.5, §29.6
- **Key formula is fixed**: `xxhash64(concat_ws('|', provider, cast(native_id as string)))`, seed 42, signed; the channel never enters a key. → spec §14.1, §14.2
- **Bronze is complete and exact**: every source field becomes a typed column, an unmapped field fails loudly, and coordinates are exact integer centimetres. → spec §10.2, §12
- **Moves and restores work per match**, file names carry a write id, and the orphan sweep never deletes a file that DuckLake still tracks. → spec §29.10, §29.11, §29.12
- **Security**: HTTPS only, `verify=True`, timeouts `(10, 30)`, at most 3 retries; no secret in code, config, logs or pages; secrets are `SecretStr` and every HF Job runs under `python -m lakehouse.mask`. → spec §20
- **Logs are JSON lines on stdout** through `lakehouse.logging.get_logger`; no `print` in `src/`; span names come from `lakehouse.telemetry.SPAN_NAMES`. → spec §21
- **The AI-PC runs light jobs only**: production-scale benchmarks and every full tracking-file parse run on the Mac Mini. → spec §5 (C7), §10.9
- **One git commit per cycle**, only after the owner's explicit yes for that commit; no push without a separate yes. → spec §23.5
