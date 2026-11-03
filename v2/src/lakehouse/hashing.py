"""Hash identity and transport wrappers (spec §13.1, §17.3).

`content_sha256` hashes the decoded bytes, so one file delivered by two channels, with or without a gzip or bz2
wrapper, has one content hash and one raw-zone object. `delivery_sha256` hashes the bytes as delivered; it is what a
channel's own change signal (for example pining `artifact_meta.sha256`) is compared with.

The wrapper is detected by magic bytes only, never by a header or a file name (spec §7.2). Decompression streams with a
byte counter, refuses output above `MAX_DECODED_BYTES`, and fails loudly on a truncated or corrupt stream.
"""

from __future__ import annotations

import bz2
import hashlib
import zlib
from typing import Final, Literal

from lakehouse.errors import IntegrityError

Wrapper = Literal["none", "gzip", "bz2"]

MAX_DECODED_BYTES: Final = 4 * 1024**3
_GZIP_MAGIC: Final = b"\x1f\x8b"
_BZ2_MAGIC: Final = b"BZh"
_CHUNK: Final = 1024 * 1024


def _check_limit(out: bytearray, wrapper: Wrapper, max_decoded_bytes: int) -> None:
    if len(out) > max_decoded_bytes:
        raise IntegrityError(f"decoded {wrapper} output exceeds the limit of {max_decoded_bytes} bytes")


def _gzip_member(data: bytes, out: bytearray, max_decoded_bytes: int) -> bytes:
    """Decode one gzip member into `out`, at most _CHUNK output bytes per call; return the bytes after it."""
    decompressor = zlib.decompressobj(wbits=16 + zlib.MAX_WBITS)
    buf = data
    while not decompressor.eof:
        chunk = decompressor.decompress(buf, _CHUNK)
        if not buf and not chunk:
            # all input consumed, no output left, and no end-of-stream marker seen
            raise IntegrityError("truncated gzip stream: it ends before its end-of-stream marker")
        out += chunk
        _check_limit(out, "gzip", max_decoded_bytes)
        buf = decompressor.unconsumed_tail
    return decompressor.unused_data


def _bz2_member(data: bytes, out: bytearray, max_decoded_bytes: int) -> bytes:
    """Decode one bz2 stream into `out`, at most _CHUNK output bytes per call; return the bytes after it."""
    decompressor = bz2.BZ2Decompressor()
    out += decompressor.decompress(data, _CHUNK)
    _check_limit(out, "bz2", max_decoded_bytes)
    while not decompressor.eof:
        if decompressor.needs_input:
            raise IntegrityError("truncated bz2 stream: it ends before its end-of-stream marker")
        out += decompressor.decompress(b"", _CHUNK)
        _check_limit(out, "bz2", max_decoded_bytes)
    return decompressor.unused_data


def _decode(data: bytes, wrapper: Wrapper, max_decoded_bytes: int) -> bytes:
    out = bytearray()
    pending = data
    magic = _GZIP_MAGIC if wrapper == "gzip" else _BZ2_MAGIC
    while pending:
        if not pending.startswith(magic):
            raise IntegrityError(f"trailing bytes after the last {wrapper} member")
        try:
            if wrapper == "gzip":
                pending = _gzip_member(pending, out, max_decoded_bytes)
            else:
                pending = _bz2_member(pending, out, max_decoded_bytes)
        except (zlib.error, OSError, ValueError, EOFError) as exc:
            raise IntegrityError(f"corrupt {wrapper} stream: {exc}") from None
    return bytes(out)


def decode_transport(data: bytes, *, max_decoded_bytes: int = MAX_DECODED_BYTES) -> tuple[bytes, Wrapper]:
    """The decoded bytes and the wrapper found by magic bytes (`none` passes the bytes through unchanged)."""
    if data.startswith(_GZIP_MAGIC):
        return _decode(data, "gzip", max_decoded_bytes), "gzip"
    if data.startswith(_BZ2_MAGIC):
        return _decode(data, "bz2", max_decoded_bytes), "bz2"
    return data, "none"


def content_sha256(data: bytes) -> str:
    """SHA-256 of the decoded bytes, whatever transport wrapper they arrived in."""
    return hashlib.sha256(decode_transport(data)[0]).hexdigest()


def delivery_sha256(data: bytes) -> str:
    """SHA-256 of the bytes exactly as delivered."""
    return hashlib.sha256(data).hexdigest()
