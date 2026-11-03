"""Local test of the hf_space reader/writer barrier nonce handshake (impl review B HFB-IMPL-01 / HFB-IMPL-04).

The hf tiers run only on live HF, so the nonce logic the reuse fix rests on — `hfkit.armed_for` / `hfkit.done_for` and
the marker protocol — had no local coverage. These drive the REAL helpers against an in-memory marker store, including
a prior campaign's leftover markers pre-seeded on the (reused) bucket, which the reader must ignore.
"""

from __future__ import annotations

import threading
import time
from typing import cast

from hfkit import BARRIER_ARMED, BARRIER_READER_READY, BARRIER_WRITER_DONE, armed_for, done_for

from lakehouse.storage.base import Storage


class _Markers:
    """A minimal thread-safe stand-in for the marker surface `armed_for`/`done_for` use (put/get/exists/delete)."""

    def __init__(self) -> None:
        self._store: dict[tuple[str, str], bytes] = {}
        self._lock = threading.Lock()

    def put(self, root: str, key: str, data: bytes) -> None:
        with self._lock:
            self._store[root, key] = data

    def get(self, root: str, key: str) -> bytes:
        with self._lock:
            return self._store[root, key]

    def exists(self, root: str, key: str) -> bool:
        with self._lock:
            return (root, key) in self._store

    def delete(self, root: str, key: str, *, missing_ok: bool = False) -> None:
        with self._lock:
            self._store.pop((root, key), None)


def test_armed_for_matches_only_this_nonce() -> None:
    markers = _Markers()
    storage = cast("Storage", markers)
    assert not armed_for(storage, "n1")  # absent
    markers.put("lake", BARRIER_ARMED, b"n1")
    assert armed_for(storage, "n1")
    assert not armed_for(storage, "n2")  # a prior campaign's armed (another nonce) is ignored


def test_done_for_parses_only_this_nonce() -> None:
    markers = _Markers()
    storage = cast("Storage", markers)
    assert done_for(storage, "nA") is None  # absent
    markers.put("lake", BARRIER_WRITER_DONE, b"nA:7:5")
    assert done_for(storage, "nA") == "7:5"
    assert done_for(storage, "nB") is None  # another campaign's done is ignored
    markers.put("lake", BARRIER_WRITER_DONE, b"nA:error")
    assert done_for(storage, "nA") == "error"


def _real_writer(markers: _Markers, result: dict[str, str]) -> None:
    """Model the REAL writer's read of `reader-ready`: block until it exists, then a SINGLE get — the real writer has
    no freshness guard (the `!= OLD` guard in the earlier test was test-only, A/B r2). Echo the nonce."""
    deadline = time.monotonic() + 10
    while not markers.exists("lake", BARRIER_READER_READY):
        if time.monotonic() >= deadline:
            result["writer"] = "no-ready"
            return
        time.sleep(0.005)
    nonce = markers.get("lake", BARRIER_READER_READY).decode()
    markers.put("lake", BARRIER_ARMED, nonce.encode())
    markers.put("lake", BARRIER_WRITER_DONE, f"{nonce}:9:5".encode())
    result["writer"] = "done"


def _run_reader(markers: _Markers, storage: Storage, result: dict[str, str], nonce: str) -> None:
    """The real reader's steps: clear armed + writer-done, write its nonce as reader-ready, then wait for THIS
    campaign's armed (releasing early on a writer error) and read its done payload."""
    markers.delete("lake", BARRIER_ARMED, missing_ok=True)
    markers.delete("lake", BARRIER_WRITER_DONE, missing_ok=True)
    markers.put("lake", BARRIER_READER_READY, nonce.encode())
    deadline = time.monotonic() + 10
    while not armed_for(storage, nonce):
        if done_for(storage, nonce) == "error":
            result["reader"] = "writer-error"
            return
        if time.monotonic() >= deadline:
            result["reader"] = "timeout-arm"
            return
        time.sleep(0.005)
    result["reader"] = done_for(storage, nonce) or "no-done"


def test_handshake_reader_first_with_prior_markers() -> None:
    """Reader-first on a reused bucket: a prior campaign's armed/writer-done/reader-ready are present. The reader clears
    armed + writer-done and overwrites reader-ready; the REAL writer (single get) reads this campaign's nonce."""
    markers = _Markers()
    storage = cast("Storage", markers)
    markers.put("lake", BARRIER_READER_READY, b"OLD")
    markers.put("lake", BARRIER_ARMED, b"OLD")
    markers.put("lake", BARRIER_WRITER_DONE, b"OLD:3:5")
    result: dict[str, str] = {}
    writer_may_read = threading.Event()

    def reader() -> None:
        _run_reader(markers, storage, result, "NEW")

    def writer() -> None:
        writer_may_read.wait(10)  # reader-first: the writer reads only after the reader has written its fresh nonce
        _real_writer(markers, result)

    reader_thread, writer_thread = threading.Thread(target=reader), threading.Thread(target=writer)
    reader_thread.start()
    deadline = time.monotonic() + 5  # release the writer once reader-ready carries THIS campaign's nonce
    while markers.exists("lake", BARRIER_READER_READY) and markers.get("lake", BARRIER_READER_READY) == b"OLD":
        if time.monotonic() >= deadline:
            break
        time.sleep(0.005)
    writer_may_read.set()
    writer_thread.start()
    reader_thread.join(12)
    writer_thread.join(12)
    assert result.get("writer") == "done", result
    assert result.get("reader") == "9:5", result  # this campaign's result, not the stale OLD:3:5


def test_handshake_writer_first_with_launcher_clear() -> None:
    """Writer-first, but the launcher cleared all three markers first (the required step, A/B round 2). The writer
    blocks on wait-for-ready until the reader writes its nonce, so it never reads a stale one."""
    markers = _Markers()
    storage = cast("Storage", markers)
    result: dict[str, str] = {}
    writer_up = threading.Event()

    def writer() -> None:
        writer_up.set()
        _real_writer(markers, result)

    def reader() -> None:
        writer_up.wait(10)
        time.sleep(0.03)  # let the writer reach its wait-for-ready loop with no marker present (pre-cleared)
        _run_reader(markers, storage, result, "NEW")

    writer_thread, reader_thread = threading.Thread(target=writer), threading.Thread(target=reader)
    writer_thread.start()
    reader_thread.start()
    writer_thread.join(12)
    reader_thread.join(12)
    assert result.get("writer") == "done", result
    assert result.get("reader") == "9:5", result
