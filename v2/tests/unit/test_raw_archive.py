"""The raw archive (spec §13; §23.6 "Raw-file visibility", "Same content, same hash")."""

from __future__ import annotations

import dataclasses
import gzip
from datetime import UTC, datetime

import pytest
from lakekit import LakeKit

from lakehouse.errors import IntegrityError
from lakehouse.ids.native import Channel
from lakehouse.raw.archive import Delivery, RawArchive

PAYLOAD = b'{"frames": [1, 2, 3], "match": "J03WPY"}' * 50


def _delivery(raw: bytes, *, channel: Channel = "public", visibility: str = "public") -> Delivery:
    return Delivery.received(
        provider="idsse",
        channel=channel,
        native_match_id="J03WPY",
        artifact="positions",
        raw=raw,
        visibility="public" if visibility == "public" else "private",
        change_signal="sha-1",
        fetched_at=datetime(2026, 10, 26, tzinfo=UTC),
    )


def test_delivery_hashes_and_wrapper() -> None:
    plain, wrapped = _delivery(PAYLOAD), _delivery(gzip.compress(PAYLOAD))
    assert plain.wrapper == "none"
    assert wrapped.wrapper == "gzip"
    assert plain.content_sha256 == wrapped.content_sha256
    assert plain.delivery_sha256 != wrapped.delivery_sha256
    assert wrapped.decoded == PAYLOAD


def test_same_content_one_raw_object(kit: LakeKit) -> None:
    archive = RawArchive(kit.storage, enabled=True)
    first = archive.store(_delivery(PAYLOAD))
    second = archive.store(_delivery(gzip.compress(PAYLOAD)))
    assert first == second
    assert first is not None
    assert first.path == f"raw/{_delivery(PAYLOAD).content_sha256}.zst"
    assert kit.objects("lake", "raw/") == [first.path]
    assert archive.read(_delivery(PAYLOAD).content_sha256) == PAYLOAD


def test_store_never_overwrites(kit: LakeKit) -> None:
    archive = RawArchive(kit.storage, enabled=True)
    key = archive.store(_delivery(PAYLOAD))
    assert key is not None
    before = kit.storage.stat("lake", key.path)
    archive.store(_delivery(PAYLOAD))
    assert kit.storage.stat("lake", key.path) == before


def test_raw_file_keeps_delivery_flag(kit: LakeKit) -> None:
    """A private delivery stays private even when the public copy of the match wins (§9, §29.2)."""
    archive = RawArchive(kit.storage, enabled=True)
    private = _delivery(PAYLOAD + b"pining extras", channel="pining", visibility="private")
    key = archive.store(private)
    assert key is not None
    assert key.root == "restricted"
    assert key.path.startswith("raw/")
    assert kit.objects("lake", "raw/") == []


def test_archive_off_stores_nothing(kit: LakeKit) -> None:
    archive = RawArchive(kit.storage, enabled=False)
    assert archive.store(_delivery(PAYLOAD)) is None
    assert kit.objects("lake", "raw/") == []
    assert kit.objects("restricted", "raw/") == []


def test_copy_to_without_archived_object(kit: LakeKit) -> None:
    archive = RawArchive(kit.storage, enabled=False)
    archive.store(_delivery(PAYLOAD))
    assert archive.copy_to(_delivery(PAYLOAD).content_sha256, "restricted") is None
    assert kit.objects("restricted", "raw/") == []
    assert archive.locate(_delivery(PAYLOAD).content_sha256) is None
    with pytest.raises(IntegrityError, match="neither root"):
        archive.read(_delivery(PAYLOAD).content_sha256)


def test_locate_prefers_restricted(kit: LakeKit) -> None:
    archive = RawArchive(kit.storage, enabled=True)
    lake_key = archive.store(_delivery(PAYLOAD))
    assert lake_key is not None
    content = _delivery(PAYLOAD).content_sha256
    copied = archive.copy_to(content, "restricted")
    assert copied is not None
    assert copied.root == "restricted"
    assert archive.locate(content) == copied  # the restricted root first, even while the lake copy still exists
    kit.storage.delete("lake", lake_key.path)
    assert archive.locate(content) == copied
    assert archive.read(content) == PAYLOAD
    assert archive.copy_to(content, "restricted") == copied  # an existing copy is not written again


def test_read_detects_altered_object(kit: LakeKit) -> None:
    archive = RawArchive(kit.storage, enabled=True)
    key = archive.store(_delivery(PAYLOAD))
    assert key is not None
    import zstandard

    kit.storage.delete("lake", key.path)
    kit.storage.put("lake", key.path, zstandard.ZstdCompressor().compress(b"other bytes"))
    with pytest.raises(IntegrityError, match="SHA-256"):
        archive.read(_delivery(PAYLOAD).content_sha256)


def test_store_checks_the_content_hash(kit: LakeKit) -> None:
    good = _delivery(PAYLOAD)
    forged = dataclasses.replace(good, content_sha256="0" * 64)
    with pytest.raises(IntegrityError, match="content_sha256"):
        RawArchive(kit.storage, enabled=True).store(forged)
