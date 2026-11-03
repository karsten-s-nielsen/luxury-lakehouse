"""S1 Task 6: raw deliveries are AES-256-GCM encrypted at rest on an encrypted-local lake (plan T6)."""

from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path

import pytest
import zstandard

from lakehouse.raw.archive import Delivery, RawArchive
from lakehouse.storage.keys import RootMap, StorageRootInfo
from lakehouse.storage.local import LocalStorage

_KEY = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
_WRONG = "fedcba9876543210"
_MARKER = b"markerzz-raw-secret"


def _storage(base: Path) -> LocalStorage:
    roots = RootMap(
        lake=StorageRootInfo(local_path=base / "lake", hf_bucket=None, s3_bucket_path=None),
        restricted=StorageRootInfo(local_path=base / "lake-restricted", hf_bucket=None, s3_bucket_path=None),
    )
    (base / "lake").mkdir(parents=True, exist_ok=True)
    (base / "lake-restricted").mkdir(parents=True, exist_ok=True)
    return LocalStorage(roots)


def _delivery() -> Delivery:
    raw = b"<data>" + _MARKER * 100 + b"</data>"
    return Delivery.received(
        provider="skillcorner",
        channel="pining",
        native_match_id="m1",
        artifact="tracking",
        raw=raw,
        visibility="public",
        change_signal=None,
        fetched_at=datetime.now(UTC),
    )


def test_raw_encrypted_at_rest_and_reads_back(tmp_path: Path) -> None:
    storage = _storage(tmp_path)
    delivery = _delivery()
    arch = RawArchive(storage, enabled=True, key=_KEY)
    key = arch.store(delivery)
    assert key is not None
    stored = storage.get(key.root, key.path)
    assert _MARKER not in stored
    with pytest.raises(Exception):  # noqa: B017 - encrypted bytes are not a valid zstd frame
        zstandard.ZstdDecompressor().decompress(stored)
    assert arch.read(delivery.content_sha256) == delivery.decoded


def test_raw_unencrypted_is_plain_zstd(tmp_path: Path) -> None:
    storage = _storage(tmp_path)
    delivery = _delivery()
    arch = RawArchive(storage, enabled=True, key=None)
    key = arch.store(delivery)
    assert key is not None
    stored = storage.get(key.root, key.path)
    assert zstandard.ZstdDecompressor().decompress(stored) == delivery.decoded
    assert arch.read(delivery.content_sha256) == delivery.decoded


def test_encrypted_raw_uses_the_single_lake_root(tmp_path: Path) -> None:
    """Encrypted-local is a single root (spec S1 A-prime): a private raw delivery goes to the lake root, protected
    by the encryption, not to a restricted root. Unencrypted-local keeps the two-root split."""
    storage = _storage(tmp_path)
    private = Delivery.received(
        provider="skillcorner",
        channel="pining",
        native_match_id="m2",
        artifact="tracking",
        raw=b"<data>" + _MARKER * 50 + b"</data>",
        visibility="private",
        change_signal=None,
        fetched_at=datetime.now(UTC),
    )
    keyed = RawArchive(storage, enabled=True, key=_KEY).store(private)
    assert keyed is not None and keyed.root == "lake", "encrypted-local raw is single-root (lake)"
    plain = RawArchive(storage, enabled=True, key=None).store(private)
    assert plain is not None and plain.root == "restricted", "unencrypted-local keeps the two-root split"


def test_encrypted_raw_round_trip_preserves_both_classifications(tmp_path: Path) -> None:
    """S1-IMPL-05: a public and a private delivery both round-trip through the keyed single-root archive — each is
    stored (in the lake root), located and read back by its own content hash, so the public/private classification
    is not lost when the two-root split collapses to one encrypted root."""
    storage = _storage(tmp_path)
    archive = RawArchive(storage, enabled=True, key=_KEY)
    deliveries = {
        vis: Delivery.received(
            provider="skillcorner",
            channel="pining",
            native_match_id=f"m-{vis}",
            artifact="tracking",
            raw=f"<data>{vis}-{_MARKER.decode()}</data>".encode(),
            visibility=vis,
            change_signal=None,
            fetched_at=datetime.now(UTC),
        )
        for vis in ("public", "private")
    }
    for vis, delivery in deliveries.items():
        stored = archive.store(delivery)
        assert stored is not None and stored.root == "lake", f"{vis} raw is in the single lake root"
        assert archive.read(delivery.content_sha256) == delivery.decoded, f"{vis} raw reads back"


def test_wrong_key_cannot_read(tmp_path: Path) -> None:
    storage = _storage(tmp_path)
    delivery = _delivery()
    RawArchive(storage, enabled=True, key=_KEY).store(delivery)
    with pytest.raises(Exception):  # noqa: B017 - GCM auth failure on a wrong key
        RawArchive(storage, enabled=True, key=_WRONG).read(delivery.content_sha256)
