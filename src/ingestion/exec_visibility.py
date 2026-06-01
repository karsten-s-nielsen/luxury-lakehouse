"""Executor → driver visibility for Spark serverless ``applyInPandas`` work.

Databricks serverless runs on Spark Connect. Two hard constraints shape every
progress-reporting design here (both verified empirically — see ADR-031 and
[[test-production-driver-entry-point]]):

1. **Executor stdout/stderr never reaches the parent task log.** A
   ``logger.info(...)`` / ``print(...)`` inside an ``applyInPandas`` closure
   goes to the *executor* log stream, which the Jobs UI "Task logs" view (and
   ``jobs.get_run_output``) do NOT surface. Only *driver*-process stdout lands
   in the task log.

2. **Driver-side mid-flight executor signalling is forbidden.**
   ``spark.sparkContext`` (accumulators / broadcast), ``StreamingQueryListener``,
   and ``query.recentProgress`` all raise ``PySparkAttributeError`` on Connect.
   ``DataFrame.observe(...)`` only yields metrics *after* the stage finishes —
   useless while a stage hangs.

The only way to get "an executor sends a statement back to the parent task that
shows up immediately" is therefore a **rendezvous + driver-poller**:

* the executor writes a tiny marker file into a shared UC Volume directory
  (raw FUSE ``open()`` — no token, no internet needed), and
* a driver-side ``threading.Thread`` globs that directory every N seconds and
  reads + ``print()``s the newest marker's content to the task log.

This module provides composable, individually-degrading layers:

* :class:`PhaseHeartbeat` — driver-side thread that prints elapsed + the current
  driver phase + (optionally) a target-table row count and the newest
  rendezvous marker's CONTENT. ALWAYS works (pure SparkSession + stdlib). This
  alone distinguishes a driver-side blocking action (``.toPandas()`` /
  ``.count()``) from an ``applyInPandas`` DAG hang, AND surfaces executor
  markers (env fingerprint, errors) into the task log.
* :func:`executor_env_fingerprint` — one-shot executor-environment snapshot
  written at UDF entry (numba threading layer, fork/spawn, versions, internet
  reachability) to test the leading hang hypotheses.
* :func:`install_executor_faulthandler` — arms a watchdog that dumps every
  thread's stack to executor stderr if a UDF group hangs (read via the Spark UI
  thread dump). Pure stdlib.
* :func:`executor_marker` — best-effort raw-``open()`` write of one marker file
  into the rendezvous directory from inside a UDF. Non-fatal on failure (which
  itself answers the "can an executor write to a UC Volume?" capability
  question — the first run's heartbeat reports whether any markers appear).

The rendezvous **directory must be created by the driver** via
:func:`ingestion.utils.ensure_volume_directory` (the Files API needs the driver
token); executors only ``open()`` files *inside* the pre-created directory.
"""

from __future__ import annotations

import logging
import os
import tempfile
import threading
import time
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from collections.abc import Callable

    from pyspark.sql import SparkSession

logger = logging.getLogger(__name__)


def ensure_numba_cache_dir() -> str:
    """Point ``NUMBA_CACHE_DIR`` at a writable temp dir, idempotently.

    silly-kicks decorates its pitch-control + ball-carrier kernels with
    ``@njit(cache=True)``, which makes numba try to persist compiled code to
    disk **at import time** (the decoration runs on module import). numba tries
    three cache locators in order — UserProvided (needs ``NUMBA_CACHE_DIR``) →
    InTree (needs a writable ``__pycache__`` beside the source) → UserWide
    (needs a writable user cache dir). On Databricks serverless the wheel is
    installed to a read-only ephemeral NFS path, so InTree + UserWide both fail
    and numba raises ``RuntimeError: cannot cache function ... no locator
    available`` — taking down *all* of ``silly_kicks.tracking`` on import.

    Setting ``NUMBA_CACHE_DIR`` activates the UserProvided locator (tried
    first), which only needs the *target* dir writable — it does not care that
    the source is on read-only NFS. ``tempfile.gettempdir()`` resolves to a
    writable path on serverless executors, the driver, local dev, and CI alike.

    MUST be called BEFORE the first ``silly_kicks.tracking`` import — i.e. at
    process bootstrap on the driver, and as the FIRST statement inside an
    ``applyInPandas`` UDF closure on executors (which never run bootstrap).
    ``setdefault`` preserves a deliberate operator override.

    Returns the resolved cache dir (the active value, override-respecting).
    """
    return os.environ.setdefault("NUMBA_CACHE_DIR", os.path.join(tempfile.gettempdir(), "numba_cache"))


# ── Driver-side phase heartbeat ───────────────────────────────────────────


class PhaseHeartbeat:
    """Driver-side background heartbeat printer for long Spark actions.

    Spawns a daemon thread that, every ``interval_s`` seconds, prints a single
    line to **driver stdout** (which IS the parent task log) reporting:

    * seconds elapsed since :meth:`start`,
    * the current phase string (set via :meth:`set_phase`),
    * optionally, ``COUNT(*)`` of the target table filtered by ``count_where``
      (a pure ``spark.sql`` call — Spark-Connect-safe), and
    * optionally, the newest rendezvous marker's filename + CONTENT (executor
      progress / env fingerprint / error traceback, via ``glob`` + ``open``).

    Because the print happens on the driver, it appears in the task log in real
    time even while an ``applyInPandas`` stage is mid-flight or a driver-side
    ``.toPandas()`` is blocking — which is exactly what the bare three-line AC-1
    log could not show.

    The heartbeat NEVER raises into the caller: count/glob/read failures are
    caught and reported inline so a flaky probe can't break the pipeline.
    """

    # Max chars of a marker's content echoed into the driver task log per tick.
    # Generous enough to carry an env-fingerprint JSON or an error traceback,
    # bounded so a runaway marker can't flood the log.
    _MARKER_ECHO_CHARS = 4000

    def __init__(
        self,
        *,
        tag: str = "AC1_HEARTBEAT",
        interval_s: float = 15.0,
        emit: Callable[[str], None] | None = None,
        spark: SparkSession | None = None,
        count_table: str | None = None,
        count_where: str | None = None,
        rendezvous_dir: str | None = None,
    ) -> None:
        self._tag = tag
        self._interval_s = interval_s
        # Default to print() (driver stdout → task log). A logger handler also
        # works, but print is unconditionally flushed to the task log.
        self._emit = emit if emit is not None else lambda m: print(m, flush=True)
        self._spark = spark
        self._count_table = count_table
        self._count_where = count_where
        self._rendezvous_dir = rendezvous_dir

        self._phase = "init"
        self._lock = threading.Lock()
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None
        self._start_monotonic = 0.0
        # (path, mtime) of the last marker whose content we echoed — dedup guard.
        self._last_echo_sig: tuple[str, float] | None = None

    def set_phase(self, phase: str) -> None:
        """Update the current phase label (thread-safe)."""
        with self._lock:
            self._phase = phase
        # Emit an immediate transition line so phase boundaries are precise in
        # the log even between heartbeat ticks.
        self._emit(f"{self._tag} phase->{phase} elapsed={self._elapsed():.0f}s")

    def start(self, phase: str = "init") -> PhaseHeartbeat:
        """Start the heartbeat thread. Returns self for chaining."""
        self._phase = phase
        self._start_monotonic = time.monotonic()
        self._thread = threading.Thread(target=self._loop, name="phase-heartbeat", daemon=True)
        self._thread.start()
        self._emit(f"{self._tag} started phase={phase} interval={self._interval_s:.0f}s")
        return self

    def stop(self) -> None:
        """Signal the heartbeat thread to stop and join briefly."""
        self._stop.set()
        if self._thread is not None:
            self._thread.join(timeout=self._interval_s + 1)
        self._emit(f"{self._tag} stopped phase={self._current_phase()} elapsed={self._elapsed():.0f}s")

    # ── context-manager sugar ──
    def __enter__(self) -> PhaseHeartbeat:
        return self.start()

    def __exit__(self, *_exc: object) -> None:
        self.stop()

    # ── internals ──
    def _elapsed(self) -> float:
        return time.monotonic() - self._start_monotonic if self._start_monotonic else 0.0

    def _current_phase(self) -> str:
        with self._lock:
            return self._phase

    def _row_count(self) -> str:
        if self._spark is None or self._count_table is None:
            return ""
        try:
            where = f" WHERE {self._count_where}" if self._count_where else ""
            sql = f"SELECT COUNT(*) AS n FROM {self._count_table}{where}"  # noqa: S608 — internal table+where, not user input
            n = self._spark.sql(sql).collect()[0]["n"]
            return f" rows={n}"
        except Exception as exc:  # noqa: BLE001 — heartbeat must never raise into the pipeline
            return f" rows=<err:{type(exc).__name__}>"

    def _newest_marker(self) -> str | None:
        import glob as _glob
        import os as _os

        files = _glob.glob(f"{self._rendezvous_dir}/*.txt")
        if not files:
            return None
        return max(files, key=_os.path.getmtime)

    def _marker_count(self) -> str:
        if not self._rendezvous_dir:
            return ""
        try:
            import glob as _glob

            files = _glob.glob(f"{self._rendezvous_dir}/*.txt")
            if not files:
                return " markers=0"
            newest = self._newest_marker()
            tail = f" latest={newest.rsplit('/', 1)[-1]}" if newest else ""
            return f" markers={len(files)}{tail}"
        except Exception as exc:  # noqa: BLE001 — heartbeat must never raise
            return f" markers=<err:{type(exc).__name__}>"

    def _echo_newest_marker(self) -> None:
        """Read the newest marker's content and print it to the task log.

        This is the load-bearing executor->driver channel: executor stderr never
        reaches the parent task log on Spark Connect serverless, but a marker
        file written by the executor (raw FUSE open()) CAN be read by the driver
        and printed here. Carries the env fingerprint and the per-batch error
        traceback when a UDF group fails.
        """
        if not self._rendezvous_dir:
            return
        try:
            import os as _os

            newest = self._newest_marker()
            if newest is None:
                return
            # Only re-echo when the newest marker changed since last tick (avoid
            # spamming the same content every interval).
            sig = (newest, _os.path.getmtime(newest))
            if sig == self._last_echo_sig:
                return
            self._last_echo_sig = sig
            with open(newest) as f:
                content = f.read(self._MARKER_ECHO_CHARS)
            name = newest.rsplit("/", 1)[-1]
            self._emit(f"{self._tag} MARKER {name}:\n{content}")
        except Exception as exc:  # noqa: BLE001 — heartbeat must never raise
            self._emit(f"{self._tag} marker-echo-err={type(exc).__name__}")

    def _loop(self) -> None:
        while not self._stop.wait(self._interval_s):
            line = (
                f"{self._tag} alive elapsed={self._elapsed():.0f}s "
                f"phase={self._current_phase()}{self._row_count()}{self._marker_count()}"
            )
            self._emit(line)
            self._echo_newest_marker()


# ── Executor-side faulthandler ────────────────────────────────────────────


def install_executor_faulthandler(timeout_s: float = 120.0, *, repeat: bool = True) -> None:
    """Arm ``faulthandler`` to dump all thread stacks if a UDF group hangs.

    Call this ONCE at the top of an ``applyInPandas`` / ``mapInPandas`` closure.
    If the group does not finish within ``timeout_s``, ``faulthandler`` writes
    every thread's current stack to ``sys.stderr`` (the executor log), then —
    with ``repeat=True`` — re-arms for another ``timeout_s``. This is the only
    way to capture *where* a silent executor hang is stuck (numba kernel, C
    extension, BLAS, I/O wait) without attaching py-spy.

    The dump lands in the **executor** log (read via the Spark UI "Thread Dump"
    / executor stderr), NOT the task log. Surfacing the *stack* to the task log
    is deliberately not attempted: writing from the watchdog thread to a FUSE
    Volume file while the main thread is wedged is unsafe (it may hold locks the
    write needs). The task-log-visible signal comes from the env fingerprint +
    start/error markers instead. Pure stdlib; safe on serverless.
    """
    import contextlib
    import faulthandler
    import sys

    with contextlib.suppress(Exception):  # nothing armed yet — cancel is a no-op
        faulthandler.cancel_dump_traceback_later()
    faulthandler.dump_traceback_later(timeout_s, repeat=repeat, file=sys.stderr)


def disarm_executor_faulthandler() -> None:
    """Cancel a pending :func:`install_executor_faulthandler` timer (call on success)."""
    import contextlib
    import faulthandler

    with contextlib.suppress(Exception):  # nothing armed — cancel is a no-op
        faulthandler.cancel_dump_traceback_later()


# ── Executor-side rendezvous marker ───────────────────────────────────────


def executor_marker(rendezvous_dir: str | None, *, seq: str, payload: str) -> bool:
    """Best-effort write of one marker file into the rendezvous directory.

    Called from inside a UDF closure to signal progress to the driver poller.
    Uses a **raw ``open()``** on the FUSE-mounted Volume path — no Databricks
    token, no internet, no ``spark`` (none of which an executor has). Returns
    ``True`` if the write succeeded, ``False`` otherwise. NEVER raises — a
    visibility marker must never fail the actual compute.

    The directory MUST already exist (driver creates it via
    ``ensure_volume_directory`` before dispatch). Marker filename is
    ``{seq}.txt`` so the driver's newest-by-mtime echo surfaces the latest.

    Whether this returns True on serverless executors is the open capability
    question; the driver heartbeat's ``markers=N`` field + content echo report
    the answer on the first real run.
    """
    if not rendezvous_dir:
        return False
    import os

    path = f"{rendezvous_dir}/{seq}.txt"
    try:
        with open(path, "w") as f:
            f.write(payload)
            f.flush()
            os.fsync(f.fileno())
        return True
    except Exception:  # noqa: BLE001 — marker is best-effort visibility only
        return False


def executor_env_fingerprint(rendezvous_dir: str | None, *, seq: str) -> bool:
    """Write a one-shot executor-environment fingerprint marker at UDF entry.

    Captures the load-bearing facts for the leading hang hypotheses, so the
    driver heartbeat echoes them into the task log the instant a worker starts:

    - numba threading layer + thread count (numba ``prange`` under fork is the
      prime suspect for the silent ``applyInPandas`` hang),
    - whether numba / accessible_space / scipy / sklearn import at all on the
      executor (a stalled import is itself a hang mode),
    - process / thread / start-method (fork vs spawn changes BLAS+OpenMP safety),
    - silly-kicks / accessible-space / numpy versions (env-drift check),
    - internet reachability (no-internet-in-UDF stalls any model/grid download).

    Dual-written to executor stderr (reliable — always in the executor log for
    the Spark UI) AND a marker file (best-effort — echoed to the task log by the
    driver heartbeat IF executor FUSE-write works, which this marker also
    proves or disproves). Best-effort: never raises. Returns True if the marker
    file was written.
    """
    if not rendezvous_dir:
        return False
    import importlib
    import json
    import multiprocessing
    import os
    import socket
    import sys
    import threading

    fp: dict[str, object] = {
        "event": "executor_env_fingerprint",
        "pid": os.getpid(),
        "thread_count": threading.active_count(),
        "mp_start_method": multiprocessing.get_start_method(allow_none=True),
        "hostname": socket.gethostname(),
        "env_omp_num_threads": os.environ.get("OMP_NUM_THREADS"),
        "env_numba_threading_layer": os.environ.get("NUMBA_THREADING_LAYER"),
        "env_numba_num_threads": os.environ.get("NUMBA_NUM_THREADS"),
    }

    # Module versions + import-ability (a hung import never returns — if a
    # fingerprint is missing a key, that import is the suspect).
    for mod in ("numba", "accessible_space", "silly_kicks", "numpy", "scipy", "sklearn"):
        try:
            m = importlib.import_module(mod)
            fp[f"{mod}_version"] = getattr(m, "__version__", "unknown")
        except Exception as exc:  # noqa: BLE001 — diagnostic; import failure is a finding
            fp[f"{mod}_import_error"] = f"{type(exc).__name__}: {exc}"[:200]

    # numba threading internals (the prime hang suspect).
    try:
        import numba

        fp["numba_threading_layer_cfg"] = str(getattr(numba.config, "THREADING_LAYER", "?"))
        fp["numba_num_threads"] = numba.get_num_threads()
    except Exception as exc:  # noqa: BLE001 — diagnostic
        fp["numba_introspect_error"] = f"{type(exc).__name__}: {exc}"[:200]

    # Internet reachability (no-internet UDF stalls any download).
    try:
        sock = socket.create_connection(("huggingface.co", 443), timeout=3)
        sock.close()
        fp["internet"] = "reachable"
    except Exception as exc:  # noqa: BLE001 — diagnostic; no-internet is the expected serverless state
        fp["internet"] = f"unreachable:{type(exc).__name__}"

    payload = json.dumps(fp, indent=2, sort_keys=True)
    print(f"AC1_ENVFP {payload}", file=sys.stderr, flush=True)
    return executor_marker(rendezvous_dir, seq=seq, payload=payload)
