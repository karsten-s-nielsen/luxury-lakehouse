"""Canonical object keys (spec §10.3.1).

One function maps every stored-file reference, and every storage listing, to a canonical key: the root name plus the
POSIX path relative to that root, for example `lake:data/main/dim_players/ducklake-<uuid>.parquet`. Manifests hold only
these keys, so cleanup compares like with like on every OS and every storage. A reference under neither root fails
loudly; it is never treated as an orphan.
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from pathlib import Path, PurePath, PurePosixPath, PureWindowsPath
from typing import Final, Literal

Root = Literal["lake", "restricted"]
ROOTS: Final[tuple[Root, ...]] = ("lake", "restricted")

_WINDOWS_ABSOLUTE: Final = re.compile(r"^(?:[A-Za-z]:[\\/]|\\\\)")
_HF_PREFIX: Final = "hf://buckets/"
_S3_PREFIX: Final = "s3://"


@dataclass(frozen=True)
class StorageRootInfo:
    """Where one root lives: a local folder, or an HF bucket (`<ns>/<bucket>`) with its gateway S3 path."""

    local_path: Path | None
    hf_bucket: str | None
    s3_bucket_path: str | None


@dataclass(frozen=True)
class RootMap:
    lake: StorageRootInfo
    restricted: StorageRootInfo

    def info(self, root: Root) -> StorageRootInfo:
        return self.lake if root == "lake" else self.restricted


def _check_relative_posix(path: str) -> str:
    if not path or path.startswith("/") or "\\" in path:
        raise ValueError(f"invalid key path {path!r}: it must be a non-empty relative POSIX path")
    if any(part in ("", ".", "..") for part in path.split("/")):
        raise ValueError(f"invalid key path {path!r}: empty, '.' or '..' part")
    return path


@dataclass(frozen=True)
class CanonicalKey:
    root: Root
    path: str

    def __post_init__(self) -> None:
        if self.root not in ROOTS:
            raise ValueError(f"unknown root {self.root!r}")
        _check_relative_posix(self.path)

    def __str__(self) -> str:
        return f"{self.root}:{self.path}"

    @classmethod
    def parse(cls, text: str) -> CanonicalKey:
        root, sep, path = text.partition(":")
        if not sep or root not in ROOTS:
            raise ValueError(f"invalid canonical key {text!r}: expected '<lake|restricted>:<path>'")
        return cls(root="lake" if root == "lake" else "restricted", path=path)


def _local_flavour(text: str) -> type[PurePath] | None:
    if _WINDOWS_ABSOLUTE.match(text):
        return PureWindowsPath
    if text.startswith("/"):
        return PurePosixPath
    return None


def _relative_parts(ref_parts: tuple[str, ...], root_parts: tuple[str, ...], *, casefold: bool) -> list[str] | None:
    if len(ref_parts) <= len(root_parts):
        return None

    def norm(part: str) -> str:
        return part.casefold() if casefold else part

    if [norm(p) for p in ref_parts[: len(root_parts)]] != [norm(p) for p in root_parts]:
        return None
    return list(ref_parts[len(root_parts) :])


def _match_local(ref: str, roots: RootMap) -> CanonicalKey | None:
    flavour = _local_flavour(ref)
    if flavour is None:
        return None
    ref_path = flavour(ref)
    if ".." in ref_path.parts:
        raise ValueError(f"invalid reference {ref!r}: '..' part")
    for root in ROOTS:
        local = roots.info(root).local_path
        if local is None:
            continue
        root_text = local.as_posix()
        if _local_flavour(root_text) is not flavour:
            continue
        rest = _relative_parts(ref_path.parts, flavour(root_text).parts, casefold=flavour is PureWindowsPath)
        if rest is not None:
            return CanonicalKey(root=root, path=_check_relative_posix("/".join(rest)))
    return None


def _match_prefixed(ref: str, roots: RootMap, *, scheme: str) -> CanonicalKey | None:
    for root in ROOTS:
        info = roots.info(root)
        if scheme == _HF_PREFIX:
            base = f"{_HF_PREFIX}{info.hf_bucket.strip('/')}/" if info.hf_bucket else None
        else:
            base = f"{info.s3_bucket_path.rstrip('/')}/" if info.s3_bucket_path else None
        if base and ref.startswith(base):
            return CanonicalKey(root=root, path=_check_relative_posix(ref[len(base) :]))
    return None


def canonical_key(ref: str, *, roots: RootMap, relative_to_data_path: bool) -> CanonicalKey:
    """The canonical key of a stored-file reference (spec §10.3.1).

    - `relative_to_data_path=True`: a DuckLake-written file, stored relative to the lake's `data_path`, gives
      `lake:data/...`.
    - `hf://buckets/<ns>/<bucket>/<key>` and `s3://<bucket path>/<key>`: `<root>:<key>` for the root whose bucket
      matches.
    - A local absolute path under a root: `<root>:` plus the path relative to that root, with `/` separators. Its form
      (drive letter or leading `/`) decides how it is parsed, never the host OS.
    - Anything under neither root raises `ValueError`.
    """
    if relative_to_data_path:
        return CanonicalKey(root="lake", path=_check_relative_posix("data/" + ref.replace("\\", "/")))
    if ref.startswith(_HF_PREFIX):
        key = _match_prefixed(ref, roots, scheme=_HF_PREFIX)
    elif ref.startswith(_S3_PREFIX):
        key = _match_prefixed(ref, roots, scheme=_S3_PREFIX)
    else:
        key = _match_local(ref, roots)
    if key is None:
        raise ValueError(f"reference {ref!r} is under neither root")
    return key
