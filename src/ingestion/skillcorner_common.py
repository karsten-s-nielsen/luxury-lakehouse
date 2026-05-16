"""SkillCorner API client and shared data models.

Talks to the pining-for-the-data REST API to discover and retrieve
SkillCorner match artifacts (events, tracking, match metadata).
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
    except Exception:  # noqa: BLE001 — multi-source fallback (env var preferred, secrets is optional)
        logger.debug("Databricks secrets unavailable — trying env var only", exc_info=True)

    msg = (
        "PINING_FOR_THE_DATA_TOKEN not found in environment or Databricks secrets. "
        "Setup: databricks secrets put-secret --scope pining --key token"
    )
    raise RuntimeError(msg)
