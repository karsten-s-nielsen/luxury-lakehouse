"""Reads of the published catalog for the services (spec §9, §19.5, §20; plan Task 25).

Each call opens a reader on the version that `CURRENT` names, so a page always reads one consistent version. A read
that hits a file that a visibility move removed (`IO Error: Cannot open file`) raises `StaleCatalogError`: the page
shows the error, and its next read opens the new `CURRENT` — it never shows missing data. A pruned version object
(T3 spec D4) gets one retry on the new `CURRENT` first. A lake without `CURRENT` (no run yet) reads as empty.
"""

from __future__ import annotations

import re
from collections.abc import Callable
from typing import Final

import duckdb

from lakehouse.catalog.publish import read_current
from lakehouse.catalog.reader import CatalogReader
from lakehouse.errors import StaleCatalogError
from lakehouse.storage.base import Storage

__all__ = ["read_catalog"]

# The error of an HF session whose version object was deleted mid-session (T3 Task 12, 2026-10-31): "... HTTP GET
# error on '.../catalog/v/<seq>.ducklake' (HTTP 404 Not Found)". Only a 404 on a version object is a pruned version.
_VERSION_GONE: Final = re.compile(r"/catalog/v/\d+\.ducklake'? \(HTTP 404\b")
_REPLACED: Final = "the catalog version was replaced while this page opened it: reload the page"


def _version_gone(exc: duckdb.IOException) -> bool:
    return isinstance(exc, duckdb.HTTPException) and _VERSION_GONE.search(str(exc)) is not None


def read_catalog[T](storage: Storage, read: Callable[[duckdb.DuckDBPyConnection], T], *, empty: T) -> T:
    """`read` on a session of the current version; `empty` before the first run.

    The version object that `CURRENT` named can be pruned between reading `CURRENT` and the attach (T3 spec D4): the
    open then raises `FileNotFoundError`. On HF it can also be pruned during the session: the next metadata read
    raises `duckdb.HTTPException` with HTTP 404 on the version URL. Either way the read retries once on the new
    `CURRENT` (a page read is read-only, so a repeat is safe); a second miss raises `StaleCatalogError`. A
    `FileNotFoundError` from `read` itself is not a pruned version and propagates, as does every other HTTP error."""
    try:
        read_current(storage)
    except FileNotFoundError:
        return empty  # no CURRENT yet: nothing has run (any other missing file still raises)
    for attempt in (1, 2):
        reader = CatalogReader(storage)
        opened = False
        try:
            with reader.session() as conn:
                opened = True
                return read(conn)
        except FileNotFoundError as exc:
            if opened:
                raise
            if attempt == 2:
                raise StaleCatalogError(_REPLACED) from exc
        except duckdb.IOException as exc:
            if _version_gone(exc):
                if attempt == 2:
                    raise StaleCatalogError(_REPLACED) from exc
                continue
            if "Cannot open file" in str(exc):
                raise StaleCatalogError(
                    "the lake changed while this page read it (a match moved between roots): reload the page"
                ) from exc
            raise
        finally:
            reader.close()
    raise AssertionError("unreachable: the second attempt returns or raises")
