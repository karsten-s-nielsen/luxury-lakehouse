"""Unit test for generate_team_key macro.

Asserts the rendered SQL matches the expected xxhash64(concat_ws('|', ...)) shape
and the null-passthrough behaviour mirrors generate_competition_key.
"""

from __future__ import annotations

import re
from pathlib import Path

MACRO_PATH = Path("dbt_project/macros/generate_team_key.sql")


def test_macro_file_exists() -> None:
    assert MACRO_PATH.exists(), f"{MACRO_PATH} missing"


def test_macro_signature_two_args() -> None:
    src = MACRO_PATH.read_text()
    assert "macro generate_team_key(provider_col, native_team_id_col)" in src


def test_macro_uses_xxhash64_with_delimiter() -> None:
    src = MACRO_PATH.read_text()
    assert "xxhash64" in src
    assert "concat_ws(" in src
    assert "'|'" in src, "delimiter must be the pipe character"


def test_macro_null_safe_branch_for_native_id() -> None:
    src = MACRO_PATH.read_text()
    assert re.search(r"when\s+\{\{\s*native_team_id_col\s*\}\}\s+is\s+null\s+then\s+null", src, re.IGNORECASE)


def test_macro_references_adr_011() -> None:
    src = MACRO_PATH.read_text()
    assert "ADR-011" in src
