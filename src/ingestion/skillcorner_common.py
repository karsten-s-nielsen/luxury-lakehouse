"""SkillCorner API client and shared data models.

Talks to the pining-for-the-data REST API to discover and retrieve
SkillCorner match artifacts (events, tracking, match metadata).
"""

from __future__ import annotations

import dataclasses
import logging
from datetime import datetime
from urllib.parse import quote, urlencode

import pydantic
import requests

from ingestion.utils import fetch_url

logger = logging.getLogger(__name__)

API_BASE_URL = "https://ozqgk9a3ji.execute-api.us-east-1.amazonaws.com/v1"
PROVIDER = "skillcorner"


class MatchInfo(pydantic.BaseModel):
    """A single match from the pining-for-the-data discovery endpoint."""

    id: str
    artifacts: dict[str, str]
    home: str
    away: str
    date: str
    updated_at: datetime
    visibility: str

    @pydantic.field_validator("id")
    @classmethod
    def _id_must_be_numeric(cls, v: str) -> str:
        """Defense-in-depth: match IDs are interpolated into replaceWhere SQL."""
        if not v.isdigit():
            msg = f"MatchInfo.id must be numeric, got {v!r}"
            raise ValueError(msg)
        return v


# SkillCorner ships two artifact layouts for the SAME data model (spec
# 2026-07-02-skillcorner-rm-private-format-ingestion): the public A-League broadcast
# feed and the private RM full-format feed. They differ ONLY in artifact-key naming +
# serialization (JSON/CSV/JSONL vs JSON/parquet/gzip-JSON); both normalize into the same
# bronze schema, so downstream (SPADL/AC) is serialization-blind.
FMT_ALEAGUE = "aleague"
FMT_RM = "rm"


@dataclasses.dataclass(frozen=True)
class ArtifactPlan:
    """Per-match artifact keys + serialization format, resolved from the manifest."""

    fmt: str  # FMT_ALEAGUE | FMT_RM
    match_key: str  # artifact key for match metadata (parse_match_json)
    events_key: str  # artifact key for events (CSV for A-League, parquet for RM)
    tracking_key: str  # artifact key for tracking (JSONL for A-League, gzip-JSON for RM)


def resolve_artifact_plan(match: MatchInfo) -> ArtifactPlan:
    """Resolve artifact keys + format from ``match.artifacts`` — NOT from ``visibility``.

    Format is detected from the actual artifact-key layout (decoupled from tier: a public
    match could in principle ship either layout, and vice versa). An unrecognized manifest
    raises loudly (never guess / never silently mis-fetch) — the fail-safe that surfaces a
    producer-side change, mirroring the pining ``visibility``-vocabulary contract.
    """
    keys = set(match.artifacts.keys())
    mid = match.id
    if f"{mid}_match" in keys:
        return ArtifactPlan(FMT_ALEAGUE, f"{mid}_match", f"{mid}_dynamic_events", f"{mid}_tracking_extrapolated")
    if {"metadata", "events", "tracking"} <= keys:
        return ArtifactPlan(FMT_RM, "metadata", "events", "tracking")
    msg = (
        f"Unrecognized SkillCorner artifact manifest for match {mid!r}: keys={sorted(keys)}. "
        "Expected A-League ({id}_match / {id}_dynamic_events / {id}_tracking_extrapolated) "
        "or RM (metadata / events / tracking)."
    )
    raise ValueError(msg)


def fetch_match_list(
    token: str,
    updated_since: str | None = None,
) -> list[MatchInfo]:
    """GET /v1/skillcorner/matches with optional updatedSince filter.

    Args:
        token: Bearer token for the pining-for-the-data API.
        updated_since: ISO 8601 UTC timestamp to filter matches updated after.

    Returns:
        List of MatchInfo objects for matches matching the filter.
    """
    url = f"{API_BASE_URL}/skillcorner/matches"
    if updated_since is not None:
        params = urlencode({"updatedSince": updated_since}, quote_via=quote)
        url = f"{url}?{params}"

    resp = fetch_url(url, headers={"Authorization": f"Bearer {token}"})
    data = resp.json()
    return [MatchInfo.model_validate(m) for m in data.get("matches", [])]


def fetch_artifact(
    match_id: str,
    artifact_key: str,
    token: str,
    stream: bool = False,
) -> requests.Response:
    """Fetch a single match artifact, following S3 302 redirect.

    Args:
        match_id: SkillCorner match ID (e.g. "1886347").
        artifact_key: Artifact key (e.g. "1886347_dynamic_events").
        token: Bearer token for the pining-for-the-data API.
        stream: If True, don't download body eagerly (use .iter_content()).

    Returns:
        Response object containing the artifact content.
    """
    url = f"{API_BASE_URL}/skillcorner/matches/{match_id}/{artifact_key}"
    return fetch_url(url, headers={"Authorization": f"Bearer {token}"}, stream=stream)


def resolve_pining_token() -> str:
    """Resolve the pining-for-the-data API token.

    Resolution order:
        1. ``PINING_FOR_THE_DATA_TOKEN`` environment variable (local dev, CI)
        2. Databricks secret scope ``pining``, key ``token`` (serverless)

    Raises:
        RuntimeError: If token cannot be found in any source.
    """
    import os

    token = os.environ.get("PINING_FOR_THE_DATA_TOKEN", "")
    if token:
        return token

    try:
        import base64

        from databricks.sdk import WorkspaceClient  # type: ignore[import-not-found]

        client = WorkspaceClient()
        resp = client.secrets.get_secret(scope="pining", key="token")
        encoded = resp.value or ""
        if encoded:
            return base64.b64decode(encoded).decode()
    except Exception:  # multi-source fallback (env var preferred, secrets is optional)
        logger.debug("Databricks secrets unavailable — trying env var only", exc_info=True)

    msg = (
        "PINING_FOR_THE_DATA_TOKEN not found in environment or Databricks secrets. "
        "Setup: databricks secrets put-secret --scope pining --key token"
    )
    raise RuntimeError(msg)
