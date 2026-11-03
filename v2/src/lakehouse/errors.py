"""Typed errors of lakehouse v2. Every error the package raises on purpose subclasses LakehouseError."""

from __future__ import annotations

from datetime import datetime

__all__ = [
    "ArtifactUnavailableError",
    "ConfigError",
    "ConflictError",
    "GuardStopError",
    "HttpStatusError",
    "InexactValueError",
    "IntegrityError",
    "LakehouseError",
    "LeaseLedgerError",
    "LeaseLostError",
    "LeaseMarkerError",
    "LedgerConflictError",
    "LedgerUnavailableError",
    "MissingRootError",
    "RateLimitedError",
    "RootMismatchError",
    "StaleCatalogError",
    "StaleSaveError",
    "StorageLimitError",
    "UnexpectedPayloadError",
    "UnmappedFieldError",
]


class LakehouseError(Exception):
    """Base class of every error that lakehouse raises on purpose."""


class ConfigError(LakehouseError):
    """A configuration or usage error (for example an invalid encryption key). The CLI maps it to exit code 2,
    alongside `ValueError` and `MissingRootError`."""


class UnmappedFieldError(LakehouseError):
    """A source payload holds a field that no bronze column maps (spec §12)."""

    def __init__(self, artifact: str, field_paths: list[str]) -> None:
        self.artifact = artifact
        self.field_paths = field_paths
        super().__init__(f"unmapped fields in {artifact}: {', '.join(field_paths)}")


class InexactValueError(LakehouseError):
    """A coordinate cannot be stored as exact integer millimetres (spec §10.2)."""


class UnexpectedPayloadError(LakehouseError):
    """A source answered with a payload of the wrong kind, for example an HTML page instead of data."""


class LeaseLostError(LakehouseError):
    """This run no longer holds the single-writer lease, so it must not change the catalog (spec §10.6)."""


class LeaseMarkerError(LakehouseError):
    """A won lease generation could not store its run anchor or its acquisition marker (T3 spec D5). The generation
    is released and the caller writes nothing; the CLI exits 1."""


class ConflictError(LakehouseError):
    """A conditional write found a different object than expected (compare-and-swap or create-if-absent)."""


class MissingRootError(LakehouseError):
    """A configured storage root does not exist, or no root is configured (spec §10.5)."""


class RootMismatchError(LakehouseError):
    """The resolved storage roots differ from the roots that the catalog recorded."""


class StaleCatalogError(LakehouseError):
    """A read of a published catalog version hit a file that a visibility move has removed (spec §9): the page shows
    this error and reads the new `CURRENT` next time, never a page with missing data."""


class StaleSaveError(LakehouseError):
    """A config save was based on an older version than the one stored (spec §8.3)."""


class GuardStopError(LakehouseError):
    """A fail-closed guard stopped an operation before any delete or publish (spec §10.3)."""


class IntegrityError(LakehouseError):
    """Stored or received bytes do not match their recorded size, hash or structure."""


class LedgerConflictError(LakehouseError):
    """A lease-ledger commit failed its parent check: another commit moved the head first (T6 spec D1)."""


class LedgerUnavailableError(LakehouseError):
    """The lease ledger refused a commit for its rate limit (HTTP 429) (T6 spec D1)."""


class LeaseLedgerError(LakehouseError):
    """The lease ledger stopped an acquisition: a rate limit, an outage or an outcome it cannot decide. The run
    writes nothing; the CLI exits 1 (T6 spec D1)."""


class StorageLimitError(LakehouseError):
    """A write is larger than the storage can make conditional: on HF a conditional write is one part only, and the
    gateway takes one part up to a measured limit (T6 spec D2). The CLI exits 1."""


class HttpStatusError(LakehouseError):
    """An HTTP request failed with a status that retries did not fix. The URL is shown without its query string,
    which can hold a presigned signature."""

    def __init__(self, status: int, url: str, detail: str = "") -> None:
        self.status = status
        self.url = url
        super().__init__(f"HTTP {status} from {url}" + (f": {detail}" if detail else ""))


class RateLimitedError(HttpStatusError):
    """A source rate-limits this client until `until`; the run reports it visibly and stops that source."""

    def __init__(self, status: int, url: str, until: datetime | None) -> None:
        self.until = until
        when = until.isoformat() if until is not None else "an unknown time"
        super().__init__(status, url, f"rate limit reached, retry after {when}")


class ArtifactUnavailableError(HttpStatusError):
    """A pining artifact that this token may not read: the uniform 404 of a private entry for the public tier, and of
    every artifact for the list-only tier (spec §7.2, §29.6)."""
