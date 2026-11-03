"""Secret masking in the Job's output (spec §20; plan Task 33 step 3).

A real Job secret value, emitted both as a plain log line and inside a forced DuckDB error, must appear masked (`***`)
and never in the clear when the command runs under `python -m lakehouse.mask`. The test runs that wrapper on a child
process and checks the child's combined output, so it proves the wrapper masks pytest's own report, captured output
and C-level prints — the output no in-process filter reaches.
"""

from __future__ import annotations

import os
import subprocess
import sys

import pytest

pytestmark = [pytest.mark.hf, pytest.mark.hf_job]

_SECRET_ENV = "HF_S3_SECRET_ACCESS_KEY"  # noqa: S105 - an environment variable name, not a secret value

_LOG_LINE = "import os, sys; sys.stdout.write('leak ' + os.environ['HF_S3_SECRET_ACCESS_KEY'] + '\\n')"
_DUCKDB_ERROR = (  # fetchall(): duckdb.sql() is lazy in DuckDB 2.0, so the cast error fires only on execution
    "import os, duckdb\n"
    "from lakehouse.catalog.duck import sql_text\n"
    "secret = os.environ['HF_S3_SECRET_ACCESS_KEY']\n"
    "duckdb.sql('SELECT CAST(' + sql_text(secret) + ' AS INTEGER)').fetchall()\n"
)


def _run_under_mask(code: str) -> str:
    """Run `python -m lakehouse.mask -- python -c <code>` and return its combined stdout+stderr."""
    proc = subprocess.run(  # noqa: S603 - a fixed interpreter and our own code, no shell
        [sys.executable, "-m", "lakehouse.mask", "--", sys.executable, "-c", code],
        capture_output=True,
        text=True,
        env=dict(os.environ),
        check=False,
    )
    return proc.stdout + proc.stderr


def test_secret_is_masked_in_a_log_line() -> None:
    secret = os.environ.get(_SECRET_ENV, "")
    assert len(secret) >= 8, f"{_SECRET_ENV} must be set (a real Job secret) for the masking test"
    output = _run_under_mask(_LOG_LINE)
    # booleans first, so a failed assertion never prints the secret operand (pytest rewrites the expression)
    leaked = secret in output
    masked = "***" in output
    assert not leaked, "the secret value leaked past the mask wrapper in a log line"
    assert masked, "the mask wrapper did not emit the mask token"


def test_secret_is_masked_in_a_duckdb_error() -> None:
    secret = os.environ.get(_SECRET_ENV, "")
    assert len(secret) >= 8, f"{_SECRET_ENV} must be set (a real Job secret) for the masking test"
    output = _run_under_mask(_DUCKDB_ERROR)
    leaked = secret in output
    masked = "***" in output
    assert not leaked, "the secret value leaked past the mask wrapper in a DuckDB error"
    assert masked, "the mask wrapper did not emit the mask token"
