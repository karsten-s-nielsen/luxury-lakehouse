"""The install origin for the socket.io origin lock (spec §6.2; live-web addendum §2)."""

from __future__ import annotations

import pytest

from lakehouse.web.socket import install_origin


def test_install_origin_per_mode() -> None:
    assert install_origin({"SPACE_HOST": "me-ops.hf.space"}, "hf") == "https://me-ops.hf.space"
    assert install_origin({"SPACE_ID": "me/ops"}, "hf") == "https://me-ops.hf.space"
    assert install_origin({"APP_PORT": "7860"}, "local") == ["http://localhost:7860", "http://127.0.0.1:7860"]
    with pytest.raises(ValueError, match="SPACE_HOST"):
        install_origin({}, "hf")  # never a wildcard or framework default


def test_install_origin_local_extra_origins() -> None:
    """Local mode appends each host listed in LAKEHOUSE_APP_ORIGINS (comma-separated, trimmed) to the localhost
    origins, so the Taipy socket accepts a browser on another LAN host. Never a wildcard."""
    assert install_origin(
        {"APP_PORT": "7860", "LAKEHOUSE_APP_ORIGINS": "http://192.168.68.59:7860 , http://box:7860"}, "local"
    ) == ["http://localhost:7860", "http://127.0.0.1:7860", "http://192.168.68.59:7860", "http://box:7860"]
    assert install_origin({"APP_PORT": "7860", "LAKEHOUSE_APP_ORIGINS": ""}, "local") == [
        "http://localhost:7860",
        "http://127.0.0.1:7860",
    ]
