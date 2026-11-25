"""The calculation layer's compute phase (spec D1/D2; plan Task 1).

A compute step reads a dbt input view per match, calls a silly-kicks scorer, and writes one bronze table through the
bronze writer. `run_compute` runs the registered steps between the run's two dbt builds (see `lakehouse.runs.run`).
"""
