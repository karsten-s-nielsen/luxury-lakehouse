"""Hash identity and transport wrappers (spec §13.1, §17.3, §23.6 "Same content, same hash")."""

from __future__ import annotations

import bz2
import gzip
import hashlib

import pytest

from lakehouse.errors import IntegrityError
from lakehouse.hashing import content_sha256, decode_transport, delivery_sha256

PAYLOAD = b'{"frame": 1, "x": 12.34}\n' * 2000


def test_same_content_same_hash() -> None:
    wrapped = gzip.compress(PAYLOAD)
    assert content_sha256(PAYLOAD) == content_sha256(wrapped)
    assert delivery_sha256(PAYLOAD) != delivery_sha256(wrapped)
    assert content_sha256(PAYLOAD) == hashlib.sha256(PAYLOAD).hexdigest()
    assert delivery_sha256(wrapped) == hashlib.sha256(wrapped).hexdigest()


def test_gzip_detected() -> None:
    decoded, wrapper = decode_transport(gzip.compress(PAYLOAD))
    assert (decoded, wrapper) == (PAYLOAD, "gzip")


def test_bz2_detected() -> None:
    decoded, wrapper = decode_transport(bz2.compress(PAYLOAD))
    assert (decoded, wrapper) == (PAYLOAD, "bz2")


def test_plain_passthrough() -> None:
    assert decode_transport(PAYLOAD) == (PAYLOAD, "none")
    assert decode_transport(b"") == (b"", "none")


def test_multi_member_streams() -> None:
    assert decode_transport(gzip.compress(b"a" * 10) + gzip.compress(b"b" * 10))[0] == b"a" * 10 + b"b" * 10
    assert decode_transport(bz2.compress(b"a" * 10) + bz2.compress(b"b" * 10))[0] == b"a" * 10 + b"b" * 10


def test_truncated_gzip_raises() -> None:
    wrapped = gzip.compress(PAYLOAD)
    with pytest.raises(IntegrityError, match="truncated"):
        decode_transport(wrapped[: int(len(wrapped) * 0.6)])


def test_truncated_bz2_raises() -> None:
    wrapped = bz2.compress(PAYLOAD)
    with pytest.raises(IntegrityError, match="truncated"):
        decode_transport(wrapped[: int(len(wrapped) * 0.6)])


def test_corrupt_gzip_raises() -> None:
    wrapped = bytearray(gzip.compress(PAYLOAD))
    wrapped[20:30] = b"\x00" * 10
    with pytest.raises(IntegrityError):
        decode_transport(bytes(wrapped))


def test_decompression_bomb_refused() -> None:
    bomb = gzip.compress(b"\x00" * 5_000_000)
    with pytest.raises(IntegrityError, match="limit"):
        decode_transport(bomb, max_decoded_bytes=1_000_000)


def test_magic_bytes_only_not_headers() -> None:
    # a text that merely starts like a gzip name is not gzip; only the two magic bytes count
    assert decode_transport(b"gzip-like text")[1] == "none"
