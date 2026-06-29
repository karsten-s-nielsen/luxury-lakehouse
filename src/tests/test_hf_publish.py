"""Unit tests for src.ingestion.hf_publish (Kimball PR 4c).

Covers:
  - upload_hf_readme happy path for repo_type in {dataset, model, space}
  - Input validation: missing file, empty/whitespace file, bad repo_id,
    bad repo_type
  - LF normalization: CRLF / bare CR inputs are converted to LF
  - SHA-256 digest returned matches uploaded bytes
  - HfApi failures propagate (no silent swallow — ADR-002)
  - get_hf_card_path: dataset vs model dispatch, wheel-first resolution,
    path-traversal guard, basename-only validation
  - Dataset-card + model-card content invariants (frontmatter, non-empty,
    trailing newline, 2026-07-22 sunset date on dual-column cards)
"""

from __future__ import annotations

from pathlib import Path
from typing import ClassVar
from unittest.mock import MagicMock, patch

import pytest

from ingestion import hf_publish

# Every card filename must match an HF dataset repo basename under
# luxury-lakehouse/. Completeness of the live 19-dataset inventory is
# enforced by test_hf_publish_parity.py (which hits the live HF API);
# this module-level constant lets the local invariants run offline.
_EXPECTED_DATASET_CARDS: frozenset[str] = frozenset(
    {
        "expected-threat-grids.md",
        "football2vec-360-embeddings.md",
        "football2vec-360-training-data.md",
        "football2vec-player-embeddings.md",
        "football2vec-statsbomb-wyscout.md",
        "football2vec-training-data.md",
        "line-breaking-passes.md",
        "obso-pausa-inputs.md",
        "obso-pausa-values.md",
        "obso-trained-grids.md",
        "pining-for-the-data.md",
        "pitch-control-tracking.md",
        "psxg-predictions.md",
        "scoutgpt-training-data.md",
        "space-creation-values.md",
        "spadl-action-context-restricted.md",
        "spadl-vaep-action-values.md",
        "spadl-vaep-action-values-restricted.md",
        "statsbomb-shots-on-target.md",
        "xg-freeze-frame-data.md",
        "xg-shot-data.md",
    }
)

# Cards that emit dual legacy + canonical Kimball columns and therefore
# must document the 2026-07-22 sunset date per ADR-011. Each of these
# datasets has a publisher that selects both ``match_id`` (legacy) and
# ``match_key`` (canonical Kimball surrogate) — verified against the
# publish scripts 2026-04-24.
_DUAL_COLUMN_CARDS: frozenset[str] = frozenset(
    {
        "spadl-vaep-action-values.md",
        "spadl-vaep-action-values-restricted.md",
        "statsbomb-shots-on-target.md",
        "xg-shot-data.md",
    }
)

_EXPECTED_MODEL_CARDS: frozenset[str] = frozenset(
    {
        "defcon.md",
        "football2vec-360-model-card.md",
        "football2vec-l2-harvest.md",
        "football2vec-statsbomb-wyscout.md",
        "football2vec-v2-model-card.md",
        "obso-pausa.md",
        "off-ball-xt.md",
        "pitch-control.md",
        "psxg-model.md",
        "scoutgpt-l2-harvest.md",
        "scoutgpt-variant-learnable.md",
        "scoutgpt-variant-rope.md",
        "scoutgpt.md",
        "space-creation.md",
        "vaep-model.md",
        "xg-v2-model-card.md",
    }
)


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture()
def readme_file(tmp_path: Path) -> Path:
    p = tmp_path / "README.md"
    p.write_text("# hello\n\nsome content\n", encoding="utf-8")
    return p


# ---------------------------------------------------------------------------
# upload_hf_readme — happy paths
# ---------------------------------------------------------------------------


class TestUploadHfReadmeDataset:
    @patch("ingestion.hf_publish.HfApi")
    def test_uploads_dataset_readme(self, mock_hfapi_cls: MagicMock, readme_file: Path) -> None:
        mock_api = MagicMock()
        mock_api.upload_file.return_value = "https://huggingface.co/datasets/org/name/commit/abc123"
        mock_hfapi_cls.return_value = mock_api

        result = hf_publish.upload_hf_readme(
            repo_id="org/name",
            readme_path=readme_file,
            hf_token="fake_token",  # noqa: S106 — test fixture
        )

        mock_hfapi_cls.assert_called_once_with(token="fake_token")  # noqa: S106 — test fixture
        mock_api.upload_file.assert_called_once()
        call_kwargs = mock_api.upload_file.call_args.kwargs
        assert call_kwargs["path_in_repo"] == "README.md"
        assert call_kwargs["repo_id"] == "org/name"
        assert call_kwargs["repo_type"] == "dataset"
        assert call_kwargs["token"] == "fake_token"  # noqa: S105 — test fixture
        assert result["commit_url"] == "https://huggingface.co/datasets/org/name/commit/abc123"
        assert "sha256" in result


class TestUploadHfReadmeModel:
    @patch("ingestion.hf_publish.HfApi")
    def test_uploads_model_readme(self, mock_hfapi_cls: MagicMock, readme_file: Path) -> None:
        mock_api = MagicMock()
        mock_api.upload_file.return_value = "https://huggingface.co/org/name/commit/abc"
        mock_hfapi_cls.return_value = mock_api

        result = hf_publish.upload_hf_readme(
            repo_id="org/name",
            readme_path=readme_file,
            hf_token="fake_token",  # noqa: S106
            repo_type="model",
        )

        call_kwargs = mock_api.upload_file.call_args.kwargs
        assert call_kwargs["repo_type"] == "model"
        assert call_kwargs["path_in_repo"] == "README.md"
        assert result["commit_url"] == "https://huggingface.co/org/name/commit/abc"


class TestUploadHfReadmeSpace:
    @patch("ingestion.hf_publish.HfApi")
    def test_uploads_space_readme(self, mock_hfapi_cls: MagicMock, readme_file: Path) -> None:
        mock_api = MagicMock()
        mock_api.upload_file.return_value = "https://huggingface.co/spaces/org/name/commit/abc"
        mock_hfapi_cls.return_value = mock_api

        result = hf_publish.upload_hf_readme(
            repo_id="org/name",
            readme_path=readme_file,
            hf_token="fake_token",  # noqa: S106
            repo_type="space",
        )

        call_kwargs = mock_api.upload_file.call_args.kwargs
        assert call_kwargs["repo_type"] == "space"
        assert call_kwargs["path_in_repo"] == "README.md"
        assert result["commit_url"] == "https://huggingface.co/spaces/org/name/commit/abc"


# ---------------------------------------------------------------------------
# upload_hf_readme — validation
# ---------------------------------------------------------------------------


class TestValidation:
    def test_missing_file_raises_value_error(self, tmp_path: Path) -> None:
        missing = tmp_path / "does_not_exist.md"
        with pytest.raises(ValueError, match="README not found"):
            hf_publish.upload_hf_readme(
                repo_id="org/name",
                readme_path=missing,
                hf_token="t",  # noqa: S106
            )

    def test_empty_file_raises_value_error(self, tmp_path: Path) -> None:
        empty = tmp_path / "empty.md"
        empty.write_text("", encoding="utf-8")
        with pytest.raises(ValueError, match="README is empty"):
            hf_publish.upload_hf_readme(
                repo_id="org/name",
                readme_path=empty,
                hf_token="t",  # noqa: S106
            )

    def test_whitespace_only_file_raises_value_error(self, tmp_path: Path) -> None:
        ws = tmp_path / "ws.md"
        ws.write_text("   \n\n   ", encoding="utf-8")
        with pytest.raises(ValueError, match="README is empty"):
            hf_publish.upload_hf_readme(
                repo_id="org/name",
                readme_path=ws,
                hf_token="t",  # noqa: S106
            )

    def test_invalid_repo_id_no_slash(self, readme_file: Path) -> None:
        with pytest.raises(ValueError, match="Invalid repo_id"):
            hf_publish.upload_hf_readme(
                repo_id="no-slash-id",
                readme_path=readme_file,
                hf_token="t",  # noqa: S106
            )

    def test_invalid_repo_id_path_traversal(self, readme_file: Path) -> None:
        with pytest.raises(ValueError, match="Invalid repo_id"):
            hf_publish.upload_hf_readme(
                repo_id="org/../etc",
                readme_path=readme_file,
                hf_token="t",  # noqa: S106
            )

    def test_invalid_repo_id_two_slashes(self, readme_file: Path) -> None:
        with pytest.raises(ValueError, match="Invalid repo_id"):
            hf_publish.upload_hf_readme(
                repo_id="org/sub/path",
                readme_path=readme_file,
                hf_token="t",  # noqa: S106
            )

    def test_invalid_repo_type_raises(self, readme_file: Path) -> None:
        with pytest.raises(ValueError, match="Invalid repo_type"):
            hf_publish.upload_hf_readme(
                repo_id="org/name",
                readme_path=readme_file,
                hf_token="t",  # noqa: S106
                repo_type="bogus",  # type: ignore[arg-type]
            )


# ---------------------------------------------------------------------------
# upload_hf_readme — LF normalization
# ---------------------------------------------------------------------------


class TestLineEndingNormalization:
    @patch("ingestion.hf_publish.HfApi")
    def test_crlf_input_uploaded_as_lf(self, mock_hfapi_cls: MagicMock, tmp_path: Path) -> None:
        p = tmp_path / "README.md"
        p.write_bytes(b"# hello\r\nworld\r\n")
        mock_api = MagicMock()
        mock_api.upload_file.return_value = "https://x"
        mock_hfapi_cls.return_value = mock_api

        hf_publish.upload_hf_readme(
            repo_id="org/name",
            readme_path=p,
            hf_token="t",  # noqa: S106
        )

        uploaded = mock_api.upload_file.call_args.kwargs["path_or_fileobj"]
        assert b"\r" not in uploaded
        assert uploaded == b"# hello\nworld\n"

    @patch("ingestion.hf_publish.HfApi")
    def test_bare_cr_normalized(self, mock_hfapi_cls: MagicMock, tmp_path: Path) -> None:
        p = tmp_path / "README.md"
        p.write_bytes(b"# legacy\rmac\r")
        mock_api = MagicMock()
        mock_api.upload_file.return_value = "https://x"
        mock_hfapi_cls.return_value = mock_api

        hf_publish.upload_hf_readme(
            repo_id="org/name",
            readme_path=p,
            hf_token="t",  # noqa: S106
        )

        uploaded = mock_api.upload_file.call_args.kwargs["path_or_fileobj"]
        assert b"\r" not in uploaded
        assert uploaded == b"# legacy\nmac\n"


# ---------------------------------------------------------------------------
# upload_hf_readme — error propagation (ADR-002: no silent swallow)
# ---------------------------------------------------------------------------


class TestHfApiFailurePropagation:
    @patch("ingestion.hf_publish.HfApi")
    def test_api_error_propagates(self, mock_hfapi_cls: MagicMock, readme_file: Path) -> None:
        # Use a plain RuntimeError as the simulated SDK failure — the
        # invariant is that the helper does NOT catch whatever HfApi
        # raises. The specific exception class is not the interesting
        # property; the absence of a try/except is.
        mock_api = MagicMock()
        mock_api.upload_file.side_effect = RuntimeError("simulated API failure")
        mock_hfapi_cls.return_value = mock_api

        with pytest.raises(RuntimeError, match="simulated API failure"):
            hf_publish.upload_hf_readme(
                repo_id="org/name",
                readme_path=readme_file,
                hf_token="t",  # noqa: S106
            )


# ---------------------------------------------------------------------------
# upload_hf_readme — SHA-256 return value
# ---------------------------------------------------------------------------


class TestSha256InReturn:
    @patch("ingestion.hf_publish.HfApi")
    def test_sha256_matches_uploaded_bytes(self, mock_hfapi_cls: MagicMock, tmp_path: Path) -> None:
        import hashlib

        p = tmp_path / "README.md"
        content = b"# consistent content\n"
        p.write_bytes(content)
        mock_api = MagicMock()
        mock_api.upload_file.return_value = "https://x"
        mock_hfapi_cls.return_value = mock_api

        result = hf_publish.upload_hf_readme(
            repo_id="org/name",
            readme_path=p,
            hf_token="t",  # noqa: S106
        )
        assert result["sha256"] == hashlib.sha256(content).hexdigest()


# ---------------------------------------------------------------------------
# get_hf_card_path — dataset and model dispatch
# ---------------------------------------------------------------------------


class TestGetHfCardPath:
    def test_dataset_card_resolves_to_dataset_cards_dir(self) -> None:
        path = hf_publish.get_hf_card_path("spadl-vaep-action-values.md", kind="dataset")
        assert path.name == "spadl-vaep-action-values.md"
        assert "dataset-cards" in str(path)
        assert "model-cards" not in str(path)

    def test_model_card_resolves_to_model_cards_dir(self) -> None:
        path = hf_publish.get_hf_card_path("psxg-model.md", kind="model")
        assert path.name == "psxg-model.md"
        assert "model-cards" in str(path)
        assert "dataset-cards" not in str(path)

    def test_default_kind_is_dataset(self) -> None:
        path = hf_publish.get_hf_card_path("xg-shot-data.md")
        assert "dataset-cards" in str(path)

    def test_returns_path_even_if_file_not_yet_created(self) -> None:
        path = hf_publish.get_hf_card_path("does-not-exist.md", kind="dataset")
        assert isinstance(path, Path)

    def test_path_traversal_rejected(self) -> None:
        with pytest.raises(ValueError, match="Invalid card name"):
            hf_publish.get_hf_card_path("../../../etc/passwd", kind="dataset")

    def test_subdirectory_rejected(self) -> None:
        with pytest.raises(ValueError, match="Invalid card name"):
            hf_publish.get_hf_card_path("sub/dir/card.md", kind="dataset")

    def test_non_md_extension_rejected(self) -> None:
        with pytest.raises(ValueError, match="Invalid card name"):
            hf_publish.get_hf_card_path("card.txt", kind="dataset")

    def test_invalid_kind_rejected(self) -> None:
        with pytest.raises(ValueError, match="Invalid kind"):
            hf_publish.get_hf_card_path("x.md", kind="space")  # type: ignore[arg-type]

    def test_wheel_path_preferred_when_present(self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
        # Simulate wheel install: site-packages/ingestion + site-packages/docs/huggingface/...
        # docs/ is a literal sibling of the ingestion/ package in the force-included wheel.
        site_pkgs = tmp_path / "site-packages"
        (site_pkgs / "ingestion").mkdir(parents=True)
        (site_pkgs / "ingestion" / "__init__.py").write_text("", encoding="utf-8")
        cards_dir = site_pkgs / "docs" / "huggingface" / "dataset-cards"
        cards_dir.mkdir(parents=True)
        card = cards_dir / "wheel-test.md"
        card.write_text("# from wheel", encoding="utf-8")

        monkeypatch.setattr(
            hf_publish,
            "_WHEEL_INGESTION_FILE",
            site_pkgs / "ingestion" / "__init__.py",
        )
        resolved = hf_publish.get_hf_card_path("wheel-test.md", kind="dataset")
        assert resolved == card

    def test_wheel_path_model_kind(self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
        site_pkgs = tmp_path / "site-packages"
        (site_pkgs / "ingestion").mkdir(parents=True)
        (site_pkgs / "ingestion" / "__init__.py").write_text("", encoding="utf-8")
        models_dir = site_pkgs / "docs" / "huggingface" / "model-cards"
        models_dir.mkdir(parents=True)
        card = models_dir / "wheel-model.md"
        card.write_text("# from wheel model", encoding="utf-8")

        monkeypatch.setattr(
            hf_publish,
            "_WHEEL_INGESTION_FILE",
            site_pkgs / "ingestion" / "__init__.py",
        )
        resolved = hf_publish.get_hf_card_path("wheel-model.md", kind="model")
        assert resolved == card


# ---------------------------------------------------------------------------
# Restricted-data publishing helpers (ADR-049)
# ---------------------------------------------------------------------------


class TestRestrictedPublishing:
    """ADR-049: restricted split helpers + publisher/trainer lockstep guards."""

    def test_restricted_repo_id_naming_convention(self) -> None:
        assert hf_publish.restricted_repo_id("luxury-lakehouse/spadl-vaep-action-values") == (
            "luxury-lakehouse/spadl-vaep-action-values-restricted"
        )

    def test_restricted_providers_is_frozenset_of_known_sources(self) -> None:
        # The set may legitimately be EMPTY (all data public) — but every
        # member must be a known data_source partition value, lowercase.
        assert isinstance(hf_publish.RESTRICTED_HF_PROVIDERS, frozenset)
        for provider in hf_publish.RESTRICTED_HF_PROVIDERS:
            assert provider == provider.lower(), f"provider {provider!r} must be lowercase"
            assert "/" not in provider and " " not in provider

    def test_split_restricted_partitions_rows_disjointly(self) -> None:
        import pandas as pd

        df = pd.DataFrame(
            {
                "action_value_id": ["a", "b", "c", "d"],
                "data_source": ["statsbomb", "gradientsports", "wyscout", "gradientsports"],
            }
        )
        with patch.object(hf_publish, "RESTRICTED_HF_PROVIDERS", frozenset({"gradientsports"})):
            public_df, restricted_df = hf_publish.split_restricted(df, column="data_source")

        assert set(public_df["data_source"]) == {"statsbomb", "wyscout"}
        assert set(restricted_df["data_source"]) == {"gradientsports"}
        # Disjoint and complete — no row dropped, no row duplicated.
        assert len(public_df) + len(restricted_df) == len(df)
        assert set(public_df["action_value_id"]).isdisjoint(set(restricted_df["action_value_id"]))

    def test_split_restricted_empty_set_sends_everything_public(self) -> None:
        # The empty-set state is HEALTHY (ADR-049): all rows go public, the
        # restricted side is an empty frame (drives the sweep-only publish).
        import pandas as pd

        df = pd.DataFrame({"data_source": ["statsbomb", "gradientsports"]})
        with patch.object(hf_publish, "RESTRICTED_HF_PROVIDERS", frozenset()):
            public_df, restricted_df = hf_publish.split_restricted(df, column="data_source")

        assert len(public_df) == len(df)
        assert restricted_df.empty

    def test_split_restricted_access_tier_mode_fail_safe(self) -> None:
        # Default access_tier mode: only an explicit "public" row is public; restricted AND NULL/unknown
        # are held back (fail-safe, spec D1).
        import pandas as pd

        df = pd.DataFrame(
            {
                "data_source": ["skillcorner", "skillcorner", "gradientsports", "statsbomb"],
                "access_tier": ["public", "restricted", "restricted", None],
                "v": [1, 2, 3, 4],
            }
        )
        public_df, restricted_df = hf_publish.split_restricted(df, column="access_tier")
        assert sorted(public_df["v"].tolist()) == [1]
        assert sorted(restricted_df["v"].tolist()) == [2, 3, 4]
        assert len(public_df) + len(restricted_df) == len(df)

    def test_split_restricted_same_provider_in_both_partitions(self) -> None:
        # The new capability: one provider (SkillCorner) appears in BOTH repos.
        import pandas as pd

        df = pd.DataFrame(
            {"data_source": ["skillcorner", "skillcorner"], "access_tier": ["public", "restricted"], "v": [1, 2]}
        )
        public_df, restricted_df = hf_publish.split_restricted(df, column="access_tier")
        assert public_df["data_source"].tolist() == ["skillcorner"]
        assert restricted_df["data_source"].tolist() == ["skillcorner"]

    def test_split_restricted_custom_column(self) -> None:
        # FUTURE row-level seam: the column parameter is how access_tier
        # splits will plug in without changing call sites.
        import pandas as pd

        df = pd.DataFrame({"source": ["gradientsports", "metrica"]})
        with patch.object(hf_publish, "RESTRICTED_HF_PROVIDERS", frozenset({"gradientsports"})):
            public_df, restricted_df = hf_publish.split_restricted(df, column="source")

        assert list(public_df["source"]) == ["metrica"]
        assert list(restricted_df["source"]) == ["gradientsports"]


# ---------------------------------------------------------------------------
# Content invariants on the actual dataset + model cards in the repo
# ---------------------------------------------------------------------------


class TestDatasetCardContent:
    _CARDS_DIR: ClassVar[Path] = Path(__file__).parent.parent.parent / "docs" / "huggingface" / "dataset-cards"

    def test_all_expected_cards_exist(self) -> None:
        present = {p.name for p in self._CARDS_DIR.iterdir() if p.is_file()}
        missing = _EXPECTED_DATASET_CARDS - present
        assert not missing, f"Missing dataset cards: {sorted(missing)}"

    def test_cards_are_non_empty(self) -> None:
        for name in _EXPECTED_DATASET_CARDS:
            p = self._CARDS_DIR / name
            assert p.read_bytes().strip(), f"{name} is empty"

    def test_cards_have_yaml_frontmatter(self) -> None:
        import yaml

        for name in _EXPECTED_DATASET_CARDS:
            content = (self._CARDS_DIR / name).read_text(encoding="utf-8")
            assert content.startswith("---\n"), f"{name} missing frontmatter"
            end = content.index("---\n", 4)
            fm = yaml.safe_load(content[4:end])
            assert isinstance(fm, dict), f"{name} frontmatter is not a mapping"
            assert "license" in fm, f"{name} frontmatter missing license"

    def test_dual_column_cards_document_sunset_date(self) -> None:
        for name in _DUAL_COLUMN_CARDS:
            content = (self._CARDS_DIR / name).read_text(encoding="utf-8")
            assert "2026-07-22" in content, (
                f"{name} must document the 2026-07-22 sunset date for Kimball dual-column removal"
            )

    def test_cards_end_with_newline(self) -> None:
        for name in _EXPECTED_DATASET_CARDS:
            content = (self._CARDS_DIR / name).read_text(encoding="utf-8")
            assert content.endswith("\n"), f"{name} must end with a newline"


class TestModelCardContent:
    _CARDS_DIR: ClassVar[Path] = Path(__file__).parent.parent.parent / "docs" / "huggingface" / "model-cards"

    def test_all_expected_model_cards_exist(self) -> None:
        present = {p.name for p in self._CARDS_DIR.iterdir() if p.is_file()}
        missing = _EXPECTED_MODEL_CARDS - present
        assert not missing, f"Missing model cards: {sorted(missing)}"

    def test_model_cards_are_non_empty(self) -> None:
        for name in _EXPECTED_MODEL_CARDS:
            p = self._CARDS_DIR / name
            assert p.read_bytes().strip(), f"{name} is empty"

    def test_model_cards_end_with_newline(self) -> None:
        for name in _EXPECTED_MODEL_CARDS:
            content = (self._CARDS_DIR / name).read_text(encoding="utf-8")
            assert content.endswith("\n"), f"{name} must end with a newline"


class TestProviderConfigInjection:
    """Data-driven per-provider HF `configs:` injection (the per-provider subset fix)."""

    def test_build_provider_configs_default_plus_per_provider_sorted_deduped(self) -> None:
        cfgs = hf_publish.build_provider_configs(["skillcorner", "idsse", "metrica", "idsse"])
        names = [c["config_name"] for c in cfgs]
        assert names == ["all", "idsse", "metrica", "skillcorner"]  # 'all' first, providers sorted+deduped
        all_cfg = cfgs[0]
        assert all_cfg["default"] is True
        assert all_cfg["data_files"] == [{"split": "train", "path": "data/*.parquet"}]
        sc = next(c for c in cfgs if c["config_name"] == "skillcorner")
        assert sc["data_files"] == [{"split": "train", "path": "data/skillcorner.parquet"}]
        assert "default" not in sc  # only the 'all' config is the default

    def test_build_provider_configs_empty(self) -> None:
        cfgs = hf_publish.build_provider_configs([])
        assert [c["config_name"] for c in cfgs] == ["all"]

    def test_inject_preserves_other_frontmatter_and_body(self) -> None:
        import yaml

        card = "---\nlicense: cc-by-nc-4.0\ntags:\n  - soccer\n---\n\n# Title\n\nBody text.\n"
        out = hf_publish.inject_frontmatter_configs(card, hf_publish.build_provider_configs(["idsse"]))
        front = yaml.safe_load(out.split("---", 2)[1])
        assert front["license"] == "cc-by-nc-4.0"  # preserved
        assert front["tags"] == ["soccer"]  # preserved
        assert [c["config_name"] for c in front["configs"]] == ["all", "idsse"]  # injected
        assert "# Title" in out and "Body text." in out  # body preserved

    def test_inject_replaces_existing_configs(self) -> None:
        import yaml

        card = "---\nlicense: other\nconfigs:\n  - config_name: stale\n    data_files: x\n---\nbody\n"
        out = hf_publish.inject_frontmatter_configs(card, hf_publish.build_provider_configs(["gradientsports"]))
        front = yaml.safe_load(out.split("---", 2)[1])
        assert [c["config_name"] for c in front["configs"]] == ["all", "gradientsports"]  # replaced, no 'stale'

    def test_inject_malformed_frontmatter_raises(self) -> None:
        with pytest.raises(ValueError, match="closing frontmatter fence"):
            hf_publish.inject_frontmatter_configs("---\nlicense: x\n(no close)", [])

    @patch("ingestion.hf_publish.HfApi")
    def test_upload_injects_configs_into_uploaded_bytes(self, mock_hfapi_cls: MagicMock, tmp_path: Path) -> None:
        import yaml

        card = tmp_path / "card.md"
        card.write_text("---\nlicense: cc-by-nc-4.0\n---\n\n# C\n", encoding="utf-8")
        mock_api = MagicMock()
        mock_api.upload_file.return_value = "https://huggingface.co/datasets/org/name/commit/x"
        mock_hfapi_cls.return_value = mock_api

        hf_publish.upload_hf_readme(
            repo_id="org/name",
            readme_path=card,
            hf_token="fake_token",  # noqa: S106
            config_providers=["metrica", "idsse"],
        )
        uploaded = mock_api.upload_file.call_args.kwargs["path_or_fileobj"].decode("utf-8")
        front = yaml.safe_load(uploaded.split("---", 2)[1])
        assert [c["config_name"] for c in front["configs"]] == ["all", "idsse", "metrica"]
        assert front["license"] == "cc-by-nc-4.0"

    @patch("ingestion.hf_publish.HfApi")
    def test_upload_empty_providers_is_byte_identical(self, mock_hfapi_cls: MagicMock, tmp_path: Path) -> None:
        card = tmp_path / "card.md"
        body = "---\nlicense: x\n---\n\n# C\n"
        card.write_text(body, encoding="utf-8")
        mock_api = MagicMock()
        mock_api.upload_file.return_value = "u"
        mock_hfapi_cls.return_value = mock_api

        hf_publish.upload_hf_readme(
            repo_id="org/name",
            readme_path=card,
            hf_token="fake_token",  # noqa: S106 — test fixture
            config_providers=[],
        )
        uploaded = mock_api.upload_file.call_args.kwargs["path_or_fileobj"]
        assert uploaded == body.encode("utf-8")  # no injection, no 'configs:' key

    @patch("ingestion.hf_publish.HfApi")
    def test_upload_config_providers_on_non_dataset_raises(self, mock_hfapi_cls: MagicMock, tmp_path: Path) -> None:
        card = tmp_path / "card.md"
        card.write_text("---\nlicense: x\n---\nbody\n", encoding="utf-8")
        mock_hfapi_cls.return_value = MagicMock()
        with pytest.raises(ValueError, match="only valid for repo_type='dataset'"):
            hf_publish.upload_hf_readme(
                repo_id="org/name",
                readme_path=card,
                hf_token="fake_token",  # noqa: S106
                repo_type="model",
                config_providers=["x"],
            )
