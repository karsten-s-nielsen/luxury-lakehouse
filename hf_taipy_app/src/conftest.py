"""Pytest configuration for hf_taipy_app/src tests.

test_render.py is the Taipy app entry point (not a pytest test) — exclude it
from collection so pytest does not attempt to import it as a test module.
"""

collect_ignore = ["test_render.py"]
