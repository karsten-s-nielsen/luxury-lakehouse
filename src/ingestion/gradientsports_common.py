"""Gradient Sports API client and shared data models.

Talks to the pining-for-the-data REST API to discover and retrieve
Gradient Sports match artifacts (events, tracking).

Mirrors the SkillCorner client pattern (skillcorner_common.py).
"""

from __future__ import annotations

import logging
from datetime import datetime
from urllib.parse import quote, urlencode

import pydantic
import requests

from ingestion.utils import fetch_url

logger = logging.getLogger(__name__)

API_BASE_URL = "https://ozqgk9a3ji.execute-api.us-east-1.amazonaws.com/v1"
PROVIDER = "gradientsports"


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


def fetch_match_list(
    token: str,
    updated_since: str | None = None,
) -> list[MatchInfo]:
    """GET /v1/gradientsports/matches with optional updatedSince filter.

    Args:
        token: Bearer token for the pining-for-the-data API.
        updated_since: ISO 8601 UTC timestamp to filter matches updated after.

    Returns:
        List of MatchInfo objects for matches matching the filter.
    """
    url = f"{API_BASE_URL}/gradientsports/matches"
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
        match_id: Gradient Sports match ID.
        artifact_key: Artifact key (e.g. "12345_events").
        token: Bearer token for the pining-for-the-data API.
        stream: If True, don't download body eagerly (use .iter_content()).

    Returns:
        Response object containing the artifact content.
    """
    url = f"{API_BASE_URL}/gradientsports/matches/{match_id}/{artifact_key}"
    return fetch_url(url, headers={"Authorization": f"Bearer {token}"}, stream=stream)


def resolve_pining_token() -> str:
    """Resolve the pining-for-the-data API token.

    Delegates to the shared SkillCorner implementation — same token, same API.
    """
    from ingestion.skillcorner_common import resolve_pining_token as _resolve

    return _resolve()
