"""The artifact that is validated must be the artifact that ships.

``_compile_requirements()`` used to be called from two places inside the upload path, so
``deploy staging`` and ``deploy production`` each ran their own ``uv pip compile``. Because a fresh
compile always takes the newest satisfying release, staging validated one pin set and production
shipped another — a staging gate that could not gate. Measured 2026-08-11: ``uv.lock`` and a fresh
production compile differ on **51 package versions, 50 of them backwards**, so the two resolutions
are meaningfully distinct rather than incidentally so.

See ADR-076 (Open Decision C → C2).
"""

from __future__ import annotations

import argparse
import ast
import hashlib
from pathlib import Path

import pytest

from scripts import manage_space

_REPO = Path(__file__).resolve().parents[2]


class TestRequirementsSeam:
    def test_compile_is_called_from_exactly_one_place(self) -> None:
        """The structural guarantee. Two call sites IS the defect; this fails if one comes back,
        regardless of how the flags behave.

        Parsed, never grepped. A ``source.count("_compile_requirements()")`` assertion is broken by
        any docstring that names the function — including the one on ``_prepare_requirements``
        explaining why a single call site exists. That formulation was written, found broken, and
        replaced before ADR-076 was accepted. The repo has an AST-guard precedent in
        ``src/tests/_delta_write_ast.py``.
        """
        tree = ast.parse((_REPO / "scripts" / "manage_space.py").read_text(encoding="utf-8"))

        def calls_it(node: ast.AST) -> bool:
            return any(
                isinstance(n, ast.Call) and isinstance(n.func, ast.Name) and n.func.id == "_compile_requirements"
                for n in ast.walk(node)
            )

        sites = [
            n
            for n in ast.walk(tree)
            if isinstance(n, ast.Call) and isinstance(n.func, ast.Name) and n.func.id == "_compile_requirements"
        ]
        assert len(sites) == 1, f"expected exactly 1 call site, found {len(sites)}"

        callers = [
            f.name
            for f in ast.walk(tree)
            if isinstance(f, ast.FunctionDef) and f.name != "_compile_requirements" and calls_it(f)
        ]
        assert callers == ["_prepare_requirements"], callers

    def test_verify_mode_rejects_a_mismatched_hash(self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
        """Fail-closed. Shipping an unverified file is the failure this seam exists to prevent."""
        req = tmp_path / "requirements.txt"
        req.write_text("flask==3.1.3\n", encoding="utf-8")
        monkeypatch.setattr(manage_space, "_requirements_path", lambda: req)
        with pytest.raises(manage_space.SpaceError, match="sha256 mismatch"):
            manage_space._prepare_requirements(compile_it=False, expect_sha256="0" * 64)

    def test_verify_mode_accepts_the_matching_hash_without_compiling(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        req = tmp_path / "requirements.txt"
        req.write_text("flask==3.1.3\n", encoding="utf-8")
        digest = hashlib.sha256(req.read_bytes()).hexdigest()
        monkeypatch.setattr(manage_space, "_requirements_path", lambda: req)
        monkeypatch.setattr(
            manage_space,
            "_compile_requirements",
            lambda: pytest.fail("must not compile when verifying a pinned artifact"),
        )
        assert manage_space._prepare_requirements(compile_it=False, expect_sha256=digest) == digest

    def test_verify_mode_requires_the_file_to_exist(self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setattr(manage_space, "_requirements_path", lambda: tmp_path / "absent.txt")
        with pytest.raises(manage_space.SpaceError, match="does not exist"):
            manage_space._prepare_requirements(compile_it=False, expect_sha256="0" * 64)

    def test_no_compile_without_an_expected_hash_is_rejected_by_the_cli(self) -> None:
        """``--no-compile`` alone would ship whatever happens to be on disk — a stale file from an
        earlier session included. Requiring the hash makes the safe path the only path."""
        with pytest.raises(SystemExit):
            manage_space.main(["deploy", "staging", "--no-compile"])

    def test_a_production_deploy_must_ship_a_validated_artifact(self) -> None:
        """Structural, not opt-in. Mirrors ``_require_force_for_production``: production gets the
        safety gate, staging keeps the ergonomic path where the artifact is produced."""
        with pytest.raises(SystemExit):
            manage_space.main(["deploy", "production"])

    def test_staging_may_still_compile(self) -> None:
        """The gate must not make the normal staging workflow impossible — that is how a safety
        check gets routed around."""
        parser = manage_space._build_parser()
        args = parser.parse_args(["deploy", "staging"])
        assert isinstance(args, argparse.Namespace)
        assert not args.no_compile
