"""Pytest configuration for hf_taipy_app/src tests.

test_render.py is the Taipy app entry point (not a pytest test) — exclude it
from collection so pytest does not attempt to import it as a test module.
"""

import os

import pytest

collect_ignore = ["test_render.py"]


@pytest.fixture(autouse=True)
def _dummy_lakebase_settings():
    """SQL-builder tests call db.t(), which reads AppSettings (lakebase_host +
    lakebase_endpoint_name are required). Provide harmless dummies so pure
    builder tests need no real Lakebase config; clear the lru_cache singleton
    so the dummies (or real env, when present) are actually picked up.
    Added for the GK tracking page (ADR-051); benefits all future query tests.
    """
    from config import get_settings

    set_here = []
    for key, value in (("LAKEBASE_HOST", "test-host"), ("LAKEBASE_ENDPOINT_NAME", "test-endpoint")):
        if key not in os.environ:
            os.environ[key] = value
            set_here.append(key)
    get_settings.cache_clear()
    yield
    for key in set_here:
        del os.environ[key]
    get_settings.cache_clear()
