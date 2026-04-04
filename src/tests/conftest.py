"""Shared test configuration."""

import sys

# On Windows, jaxlib native extensions fail to load if pandas/matplotlib
# load OpenBLAS DLLs first (DLL load order conflict). Importing jax early
# — before conftest triggers matplotlib — avoids the clash.
if sys.platform == "win32":
    try:
        import jax  # noqa: F401
    except ImportError:
        pass

import matplotlib

matplotlib.use("Agg")
