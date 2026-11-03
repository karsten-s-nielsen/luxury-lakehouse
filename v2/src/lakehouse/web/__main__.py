"""`python -m lakehouse.web`: serve the Ops app (plan Task 29)."""

from __future__ import annotations

from lakehouse.limits import raise_open_file_limit, require_open_file_minimum
from lakehouse.web.main import serve

if __name__ == "__main__":
    require_open_file_minimum(raise_open_file_limit())  # the app reads the catalog (T12 spec D1)
    serve()
