"""A process that starts at a soft open-file limit of 256 still scans a large table (T12 spec D1; OS tests, markers
`macos` and `linux`, run with `LAKEHOUSE_OS_TESTS=1` as CI does).

The subprocess scans a DuckLake table of 300 data files through `CatalogReader`, as a run's lineage read does. Without
`raise_open_file_limit` it fails with "Too many open files" (the T11 real-data failure under launchd's 256); with the
raise, as every entry point now calls it first, it reads every row.

This is the end-to-end manifestation. Whether the plain scan actually exhausts the descriptors at a 256 soft limit is
host-dependent — DuckDB streams, so some hosts (seen on the GitHub macOS/Linux runners) open fewer than 256 handles for
300 files and the scan succeeds. When this host does not reproduce the exhaustion, the test skips LOUDLY with that
reason: the raise invariant itself is proven deterministically and cross-platform by `tests/unit/test_limits.py` (the
soft limit is lifted to `OPEN_FILES_TARGET`, capped by the hard limit) and `tests/unit/test_entry_points_raise_limit.py`
(every entry point calls it first), so the skip loses no coverage of the guarantee.
"""

from __future__ import annotations

import os
import subprocess
import sys
from pathlib import Path

import pytest
from lakekit import StubLease, lake_roots

from lakehouse.catalog.working import WorkingCatalog
from lakehouse.config.model import load_defaults
from lakehouse.storage.encryption import ENCRYPTION_KEY_ENV
from lakehouse.storage.local import LocalStorage

KEY = "5" * 64
FILES = 300

_SCRIPT = """
import os, sys
from pathlib import Path
from lakehouse.catalog.reader import CatalogReader
from lakehouse.limits import raise_open_file_limit
from lakehouse.storage.local import LocalStorage
sys.path.insert(0, {support!r})
from lakekit import lake_roots
if sys.argv[1] == "raise":
    raise_open_file_limit()
reader = CatalogReader(LocalStorage(lake_roots(Path({base!r}))), env=dict(os.environ))
with reader.session() as conn:
    # a column read opens the files (a bare count(*) is answered from the file statistics)
    print(conn.execute("SELECT sum(k) FROM lake.main.ops_probe").fetchone()[0])
reader.close()
"""


def _lake(base: Path) -> None:
    for name in ("lake", "lake-restricted"):
        (base / name).mkdir(parents=True, exist_ok=True)
    storage = LocalStorage(lake_roots(base))
    cat = WorkingCatalog.open(
        storage, load_defaults("laptop"), StubLease(), work_dir=base / "w", env={ENCRYPTION_KEY_ENV: KEY}
    )
    try:
        cat.conn.execute("CREATE TABLE lake.main.ops_probe (k BIGINT)")
        for i in range(FILES):  # the encrypted attach inlines nothing: one data file per insert
            cat.conn.execute("INSERT INTO lake.main.ops_probe VALUES (?)", [i])
        cat.commit("run_end")
    finally:
        cat.close()


def _scan(base: Path, mode: str) -> subprocess.CompletedProcess[str]:
    if sys.platform == "win32":  # POSIX only; the type checker then never sees `resource` on Windows
        raise AssertionError("an OS test of the macos and linux markers")
    import resource

    _, hard = resource.getrlimit(resource.RLIMIT_NOFILE)

    def at_256() -> None:
        resource.setrlimit(resource.RLIMIT_NOFILE, (256, hard))

    script = base / "scan.py"
    support = str(Path(__file__).resolve().parents[1] / "support")
    script.write_text(_SCRIPT.format(support=support, base=str(base)), encoding="utf-8")
    return subprocess.run(  # noqa: S603 - this interpreter and a script the test wrote
        [sys.executable, str(script), mode],
        env={**os.environ, ENCRYPTION_KEY_ENV: KEY},
        preexec_fn=at_256,
        capture_output=True,
        text=True,
        check=False,
        timeout=300,
    )


@pytest.mark.parametrize(
    "host", [pytest.param("macos", marks=pytest.mark.macos), pytest.param("linux", marks=pytest.mark.linux)]
)
def test_a_large_scan_works_from_a_low_soft_limit(tmp_path: Path, host: str) -> None:
    _lake(tmp_path)
    plain = _scan(tmp_path, "plain")
    if not (plain.returncode != 0 and "Too many open files" in plain.stderr):
        # the negative control did not reproduce the exhaustion on this host (DuckDB streamed under the 256 soft
        # limit), so the end-to-end manifestation is untestable here; the raise invariant is covered deterministically
        # by tests/unit/test_limits.py and tests/unit/test_entry_points_raise_limit.py
        pytest.skip(f"host did not exhaust fds at soft=256 with {FILES} files (plain returncode={plain.returncode})")
    raised = _scan(tmp_path, "raise")
    assert raised.returncode == 0, raised.stderr[-2000:]
    assert raised.stdout.strip().splitlines()[-1] == str(sum(range(FILES)))  # after the `open-file limit` log line
