"""Local (Parquet fixture) adapters for the action-context ports.

Mirror the Spark/Delta adapters in ``ingestion.action_context`` but read from
committed Parquet fixtures so the real domain (``run_work_unit`` → ``enrich_batch``)
runs locally with zero Databricks dependency.
"""
