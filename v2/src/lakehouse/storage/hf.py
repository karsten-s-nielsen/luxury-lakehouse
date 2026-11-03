"""HF storage: a lake bucket and a restricted bucket on the Hugging Face Hub (spec §10.4, §10.5, §29.9).

Writes go by key to one of two channels:

- The S3 gateway `https://s3.hf.co` (boto3, path style, `us-east-1`), only when gateway credentials are set (the
  Job): data files under `data/`, raw files under `raw/`, and the catalog-control objects that need a conditional
  write. Those are the lease generations `runs/lease/*`, the versions and manifests `catalog/v/*`, and
  `catalog/CURRENT`. `put_if_absent` sends `If-None-Match: *`; `replace_if_match` sends `If-Match: <ETag>`.
- `HfApi.batch_bucket_files`: every other object (config-side objects, progress, skip records, run results).

Without gateway credentials (the Space), a write to a gateway key raises `PermissionError`. With them, the Job also
reads, lists, stats and deletes the gateway keys through the gateway, so the lease and `CURRENT` read their own
writes on the channel that wrote them. Readers (DuckDB, DuckLake) use the `hf://buckets/...` URIs of `read_uri`.
"""

from __future__ import annotations

import builtins
import tempfile
from collections.abc import Iterable, Iterator
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Final, Protocol, cast

from botocore.exceptions import ClientError
from huggingface_hub import BucketFile, BucketFolder
from pydantic import SecretStr

from lakehouse.errors import ConflictError, IntegrityError, StorageLimitError
from lakehouse.storage.base import DbtSecret, ObjectInfo, validate_key, validate_prefix
from lakehouse.storage.keys import ROOTS, Root, RootMap
from lakehouse.storage.ledger import HubLedger, LeaseLedger, LedgerApi

__all__ = ["GATEWAY_ENDPOINT", "BucketApi", "HfStorage", "S3Client", "S3Credentials", "is_gateway_key"]

GATEWAY_ENDPOINT: Final = "https://s3.hf.co"
GATEWAY_REGION: Final = "us-east-1"
_GATEWAY_PREFIXES: Final = ("data/", "raw/", "runs/lease/", "catalog/v/")
_GATEWAY_KEYS: Final = frozenset({"catalog/CURRENT"})
_PRECONDITION_STATUS: Final = frozenset({409, 412})
_MISSING_CODES: Final = frozenset({"NoSuchKey", "NotFound", "404"})
# Objects above this size go up in parts; S3 allows parts of 5 MiB or more, except the last part.
_MULTIPART_THRESHOLD: int = 64 * 1024 * 1024
_PART_SIZE: int = 64 * 1024 * 1024
# The largest single-part conditional write: the gateway ignores `IfNoneMatch` on a multipart upload, so a conditional
# write is one part (T6 spec D2). Measured by the T6 spike in an HF Job (2026-11-01, job 6ae696b9...): single-part
# `put_object` with `IfNoneMatch="*"` succeeded at 64, 256, 1,024 and 2,048 MiB (10.1 s); 2,048 MiB was the largest
# size probed, so it is the limit.
SINGLE_PART_LIMIT: int = 2048 * 1024 * 1024
_HTTP_TIMEOUTS: Final = (10, 30)
_MAX_ATTEMPTS: Final = 3


def _sql_text(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def is_gateway_key(key: str) -> bool:
    """True for the keys that go through the S3 gateway (spec §29.9)."""
    return key.startswith(_GATEWAY_PREFIXES) or key in _GATEWAY_KEYS


def _is_gateway_prefix(prefix: str) -> bool:
    return prefix.startswith(_GATEWAY_PREFIXES) or prefix in _GATEWAY_KEYS


@dataclass(frozen=True)
class S3Credentials:
    """The gateway key pair, from the job secrets `HF_S3_ACCESS_KEY_ID` and `HF_S3_SECRET_ACCESS_KEY`."""

    access_key_id: SecretStr
    secret_access_key: SecretStr


class _Body(Protocol):
    def read(self) -> bytes: ...


class S3Client(Protocol):
    """The part of the boto3 S3 client that `HfStorage` uses."""

    def put_object(self, **kwargs: Any) -> dict[str, Any]: ...
    def get_object(self, **kwargs: Any) -> dict[str, Any]: ...
    def head_object(self, **kwargs: Any) -> dict[str, Any]: ...
    def delete_object(self, **kwargs: Any) -> dict[str, Any]: ...
    def list_objects_v2(self, **kwargs: Any) -> dict[str, Any]: ...
    def create_multipart_upload(self, **kwargs: Any) -> dict[str, Any]: ...
    def upload_part(self, **kwargs: Any) -> dict[str, Any]: ...
    def complete_multipart_upload(self, **kwargs: Any) -> dict[str, Any]: ...
    def abort_multipart_upload(self, **kwargs: Any) -> dict[str, Any]: ...


class BucketApi(Protocol):
    """The part of `huggingface_hub.HfApi` (2.3.0) that `HfStorage` uses, with the same signatures."""

    def batch_bucket_files(
        self,
        bucket_id: str,
        *,
        add: builtins.list[tuple[str | Path | bytes, str]] | None = None,
        delete: builtins.list[str] | None = None,
    ) -> object: ...

    def list_bucket_tree(
        self, bucket_id: str, prefix: str | None = None, *, recursive: bool | None = None
    ) -> Iterable[BucketFile | BucketFolder]: ...

    def get_bucket_paths_info(self, bucket_id: str, paths: Iterable[str]) -> Iterable[BucketFile]: ...

    def download_bucket_files(
        self,
        bucket_id: str,
        files: builtins.list[tuple[str | BucketFile, str | Path]],
        *,
        raise_on_missing_files: bool = False,
    ) -> None: ...


def _client_status(exc: ClientError) -> tuple[int, str]:
    response = exc.response
    status = response.get("ResponseMetadata", {}).get("HTTPStatusCode", 0)
    return int(status), str(response.get("Error", {}).get("Code", ""))


def _is_missing(exc: ClientError) -> bool:
    status, code = _client_status(exc)
    return status == 404 or code in _MISSING_CODES


def _is_precondition(exc: ClientError) -> bool:
    status, code = _client_status(exc)
    return status in _PRECONDITION_STATUS or code in {"PreconditionFailed", "ConditionalRequestConflict"}


def _split_s3_path(s3_path: str) -> tuple[str, str]:
    """`s3://<bucket>[/<prefix>]` gives the bucket and the key prefix (empty, or ending with `/`)."""
    if not s3_path.startswith("s3://"):
        raise ValueError(f"invalid gateway S3 path {s3_path!r}: it must start with s3://")
    bucket, _, prefix = s3_path.removeprefix("s3://").strip("/").partition("/")
    if not bucket:
        raise ValueError(f"invalid gateway S3 path {s3_path!r}: no bucket")
    return bucket, f"{prefix}/" if prefix else ""


def _default_s3_client(credentials: S3Credentials) -> S3Client:
    import boto3
    from botocore.config import Config

    config = Config(
        s3={"addressing_style": "path"},
        connect_timeout=_HTTP_TIMEOUTS[0],
        read_timeout=_HTTP_TIMEOUTS[1],
        retries={"max_attempts": _MAX_ATTEMPTS, "mode": "standard"},
    )
    # types-boto3 declares one overload per AWS service; only the s3 stubs are installed, so the others are Unknown.
    client = boto3.client(  # pyright: ignore[reportUnknownMemberType]
        "s3",
        endpoint_url=GATEWAY_ENDPOINT,
        region_name=GATEWAY_REGION,
        aws_access_key_id=credentials.access_key_id.get_secret_value(),
        aws_secret_access_key=credentials.secret_access_key.get_secret_value(),
        config=config,
        verify=True,
    )
    return cast("S3Client", client)


def _default_api(hf_token: SecretStr) -> BucketApi:
    from huggingface_hub import HfApi

    return HfApi(token=hf_token.get_secret_value())


class HfStorage:
    """The `Storage` of two HF buckets. The repr never shows a token or a key."""

    def __init__(
        self,
        roots: RootMap,
        *,
        hf_token: SecretStr,
        s3_key: S3Credentials | None,
        api: BucketApi | None = None,
        s3_client: S3Client | None = None,
        control_repo: str | None = None,
    ) -> None:
        self._buckets: dict[Root, str] = {}
        self._s3_paths: dict[Root, tuple[str, str]] = {}
        for root in ROOTS:
            info = roots.info(root)
            if info.hf_bucket is None:
                raise ValueError(f"HfStorage needs an HF bucket for the {root} root")
            self._buckets[root] = info.hf_bucket
            if s3_key is not None:
                if info.s3_bucket_path is None:
                    raise ValueError(f"the {root} root needs a gateway S3 path when gateway credentials are set")
                self._s3_paths[root] = _split_s3_path(info.s3_bucket_path)
        self._roots = roots
        self._s3_key = s3_key
        self._api = api if api is not None else _default_api(hf_token)
        self._s3: S3Client | None = None
        if s3_key is not None:
            self._s3 = s3_client if s3_client is not None else _default_s3_client(s3_key)
        # the lease ledger, the HF fence of record (T6 spec D1): a writer only (gateway credentials and a repository)
        self._ledger: HubLedger | None = None
        if s3_key is not None and control_repo is not None:
            self._ledger = HubLedger(cast("LedgerApi", self._api), control_repo)

    def __repr__(self) -> str:
        return (
            f"HfStorage(lake={self._buckets['lake']!r}, restricted={self._buckets['restricted']!r}, "
            f"gateway={self._s3 is not None})"
        )

    @property
    def roots(self) -> RootMap:
        return self._roots

    @property
    def single_part_limit(self) -> int | None:
        """The largest conditional write (T6 spec D2)."""
        return SINGLE_PART_LIMIT

    @property
    def hub_ledger(self) -> HubLedger | None:
        return self._ledger

    def lease_ledger(self) -> LeaseLedger | None:
        """The lease ledger of an HF writer, or `None` (a reader); `Lease` fences on it (T6 spec D1)."""
        return self._ledger

    # -- routing -------------------------------------------------------------------------------------------------

    def _gateway(self, key: str) -> S3Client | None:
        """The gateway client when `key` goes through the gateway on this host; `None` for the HfApi channel."""
        return self._s3 if is_gateway_key(key) else None

    def _require_gateway(self, key: str) -> S3Client:
        if self._s3 is None:
            raise PermissionError(f"{key} is written only through the S3 gateway, and this host has no gateway key")
        return self._s3

    def _s3_location(self, root: Root, key: str) -> tuple[str, str]:
        bucket, prefix = self._s3_paths[root]
        return bucket, prefix + key

    # -- writes --------------------------------------------------------------------------------------------------

    def put(self, root: Root, key: str, data: bytes | Path) -> None:
        validate_key(key)
        if not is_gateway_key(key):
            self._api.batch_bucket_files(self._buckets[root], add=[(data, key)])
            return
        self._s3_put(self._require_gateway(key), root, key, data, conditions={})

    def put_if_absent(self, root: Root, key: str, data: bytes | Path) -> bool:
        validate_key(key)
        if not is_gateway_key(key):
            raise ValueError(f"{key} has no conditional write on HF: only gateway keys support put_if_absent")
        s3 = self._require_gateway(key)
        try:
            self._s3_put(s3, root, key, data, conditions={"IfNoneMatch": "*"})
        except ClientError as exc:
            if _is_precondition(exc):
                return False
            raise
        return True

    def replace_if_match(self, root: Root, key: str, data: bytes, *, expected_tag: str) -> str:
        validate_key(key)
        if not is_gateway_key(key):
            raise ValueError(f"{key} has no conditional write on HF: only gateway keys support replace_if_match")
        s3 = self._require_gateway(key)
        try:
            return self._s3_put(s3, root, key, data, conditions={"IfMatch": expected_tag})
        except ClientError as exc:
            if _is_precondition(exc):
                raise ConflictError(f"{root}:{key} changed: its ETag is no longer {expected_tag}") from exc
            raise

    def _s3_put(self, s3: S3Client, root: Root, key: str, data: bytes | Path, *, conditions: dict[str, str]) -> str:
        """A conditional write (`conditions` set) is always ONE `put_object`, up to `SINGLE_PART_LIMIT`: the gateway
        ignores `IfNoneMatch` on `CompleteMultipartUpload` (T6 spec D2). A plain write above the threshold goes up
        in parts."""
        bucket, s3_key = self._s3_location(root, key)
        size = len(data) if isinstance(data, bytes) else data.stat().st_size
        if conditions and size > SINGLE_PART_LIMIT:
            raise StorageLimitError(
                f"{root}:{key} is {size} bytes; a conditional write on HF is one part of at most {SINGLE_PART_LIMIT} "
                "bytes (T6 spec D2)"
            )
        if isinstance(data, bytes):
            return str(s3.put_object(Bucket=bucket, Key=s3_key, Body=data, **conditions)["ETag"])
        if conditions or size <= _MULTIPART_THRESHOLD:
            with data.open("rb") as body:
                return str(s3.put_object(Bucket=bucket, Key=s3_key, Body=body, **conditions)["ETag"])
        return self._s3_put_multipart(s3, bucket, s3_key, data, conditions=conditions)

    def _s3_put_multipart(
        self, s3: S3Client, bucket: str, s3_key: str, path: Path, *, conditions: dict[str, str]
    ) -> str:
        upload_id = str(s3.create_multipart_upload(Bucket=bucket, Key=s3_key)["UploadId"])
        try:
            parts: builtins.list[dict[str, Any]] = []
            with path.open("rb") as src:
                number = 1
                while chunk := src.read(_PART_SIZE):
                    reply = s3.upload_part(Bucket=bucket, Key=s3_key, UploadId=upload_id, PartNumber=number, Body=chunk)
                    parts.append({"ETag": reply["ETag"], "PartNumber": number})
                    number += 1
            reply = s3.complete_multipart_upload(
                Bucket=bucket, Key=s3_key, UploadId=upload_id, MultipartUpload={"Parts": parts}, **conditions
            )
        except BaseException:
            s3.abort_multipart_upload(Bucket=bucket, Key=s3_key, UploadId=upload_id)
            raise
        return str(reply["ETag"])

    def delete(self, root: Root, key: str, *, missing_ok: bool = False) -> None:
        validate_key(key)
        s3 = self._require_gateway(key) if is_gateway_key(key) else None
        if not missing_ok and not self.exists(root, key):
            raise FileNotFoundError(f"{root}:{key}")
        if s3 is None:
            self._api.batch_bucket_files(self._buckets[root], delete=[key])
            return
        bucket, s3_key = self._s3_location(root, key)
        s3.delete_object(Bucket=bucket, Key=s3_key)

    # -- reads ---------------------------------------------------------------------------------------------------

    def get(self, root: Root, key: str) -> bytes:
        return self.get_versioned(root, key)[0]

    def get_versioned(self, root: Root, key: str) -> tuple[bytes, str]:
        validate_key(key)
        s3 = self._gateway(key)
        if s3 is not None:
            bucket, s3_key = self._s3_location(root, key)
            try:
                reply = s3.get_object(Bucket=bucket, Key=s3_key)
            except ClientError as exc:
                if _is_missing(exc):
                    raise FileNotFoundError(f"{root}:{key}") from exc
                raise
            body = cast("_Body", reply["Body"])
            return body.read(), str(reply["ETag"])
        info = self._api_file(root, key)
        return self._api_download(root, key), info.xet_hash

    def _api_file(self, root: Root, key: str) -> BucketFile:
        for info in self._api.get_bucket_paths_info(self._buckets[root], [key]):
            if info.path == key:
                return info
        raise FileNotFoundError(f"{root}:{key}")

    def _api_download(self, root: Root, key: str) -> bytes:
        from huggingface_hub.errors import EntryNotFoundError

        with tempfile.TemporaryDirectory(prefix="lakehouse-get-") as folder:
            local = Path(folder) / "object"
            try:
                self._api.download_bucket_files(self._buckets[root], [(key, local)], raise_on_missing_files=True)
            except EntryNotFoundError as exc:
                raise FileNotFoundError(f"{root}:{key}") from exc
            return local.read_bytes()

    def stat(self, root: Root, key: str) -> ObjectInfo:
        validate_key(key)
        s3 = self._gateway(key)
        if s3 is not None:
            bucket, s3_key = self._s3_location(root, key)
            try:
                reply = s3.head_object(Bucket=bucket, Key=s3_key)
            except ClientError as exc:
                if _is_missing(exc):
                    raise FileNotFoundError(f"{root}:{key}") from exc
                raise
            return ObjectInfo(key=key, size=int(reply["ContentLength"]), modified=reply["LastModified"])
        return self._api_object_info(root, key, self._api_file(root, key))

    def _api_object_info(self, root: Root, key: str, info: BucketFile) -> ObjectInfo:
        if info.uploaded_at is None:
            raise IntegrityError(f"{root}:{key} has no storage upload time; the sweep age fence needs one")
        return ObjectInfo(key=key, size=info.size, modified=info.uploaded_at)

    def exists(self, root: Root, key: str) -> bool:
        try:
            self.stat(root, key)
        except FileNotFoundError:
            return False
        return True

    def list(self, root: Root, prefix: str) -> Iterator[ObjectInfo]:
        validate_prefix(prefix)
        if self._s3 is not None and _is_gateway_prefix(prefix):
            yield from self._s3_list(self._s3, root, prefix)
            return
        folder, _, _ = prefix.rpartition("/")
        entries = self._api.list_bucket_tree(self._buckets[root], folder or None, recursive=True)
        files = sorted(
            (e for e in entries if isinstance(e, BucketFile) and e.path.startswith(prefix)), key=lambda e: e.path
        )
        for entry in files:
            yield self._api_object_info(root, entry.path, entry)

    def _s3_list(self, s3: S3Client, root: Root, prefix: str) -> Iterator[ObjectInfo]:
        bucket, base = self._s3_paths[root]
        request: dict[str, Any] = {"Bucket": bucket, "Prefix": base + prefix}
        while True:
            reply = s3.list_objects_v2(**request)
            for item in reply.get("Contents", []):
                key = str(item["Key"]).removeprefix(base)
                yield ObjectInfo(key=key, size=int(item["Size"]), modified=item["LastModified"])
            if not reply.get("IsTruncated"):
                return
            request["ContinuationToken"] = reply["NextContinuationToken"]

    def read_uri(self, root: Root, key: str) -> str:
        return f"hf://buckets/{self._buckets[root]}/{validate_key(key)}"

    def duckdb_secrets(self) -> builtins.list[str]:
        """The `hf://` reader secret (spec §10.4: the token comes from the credential chain, never from the SQL)
        and, on the Job, one gateway secret per root for the writer's `s3://` data path (§10.5.1). The gateway
        statements hold the gateway key: never log them."""
        statements = ["CREATE OR REPLACE SECRET lakehouse_hf (TYPE huggingface, PROVIDER credential_chain)"]
        if self._s3_key is None:
            return statements
        key_id = _sql_text(self._s3_key.access_key_id.get_secret_value())
        secret = _sql_text(self._s3_key.secret_access_key.get_secret_value())
        endpoint = GATEWAY_ENDPOINT.removeprefix("https://")
        for root in ROOTS:
            bucket, prefix = self._s3_paths[root]
            scope = f"s3://{bucket}/{prefix}".rstrip("/")
            statements.append(
                f"CREATE OR REPLACE SECRET lakehouse_gateway_{root} (TYPE s3, KEY_ID {key_id}, SECRET {secret}, "
                f"ENDPOINT {_sql_text(endpoint)}, URL_STYLE 'path', REGION {_sql_text(GATEWAY_REGION)}, "
                f"USE_SSL true, SCOPE {_sql_text(scope)})"
            )
        return statements

    def dbt_secrets(self) -> builtins.list[DbtSecret]:
        """The secrets of `duckdb_secrets` for the dbt profile: the gateway key pair is read from the Job's
        environment (`HF_S3_ACCESS_KEY_ID`, `HF_S3_SECRET_ACCESS_KEY`) by dbt's `env_var`."""
        secrets = [DbtSecret("huggingface", (("provider", "credential_chain"),))]
        if self._s3_key is None:
            return secrets
        endpoint = GATEWAY_ENDPOINT.removeprefix("https://")
        for root in ROOTS:
            bucket, prefix = self._s3_paths[root]
            secrets.append(
                DbtSecret(
                    "s3",
                    (
                        ("key_id", "{{ env_var('HF_S3_ACCESS_KEY_ID') }}"),
                        ("secret", "{{ env_var('HF_S3_SECRET_ACCESS_KEY') }}"),
                        ("endpoint", endpoint),
                        ("url_style", "path"),
                        ("region", GATEWAY_REGION),
                        ("use_ssl", "true"),
                        ("scope", f"s3://{bucket}/{prefix}".rstrip("/")),
                    ),
                )
            )
        return secrets
