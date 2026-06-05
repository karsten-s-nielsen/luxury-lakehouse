"""ADR-002 §4 writer/DDL parity guard for the SPADL/VAEP pipeline.

PR-LL1 (silly-kicks 1.5.0+ ``preserve_native``) extends ``_SPADL_SCHEMA`` and
``_VAEP_SCHEMA`` in ``src/ingestion/spadl_vaep.py`` with 4 provider-namespaced
StatsBomb-native columns. Spark's ``applyInPandas`` schemas declared in
``src/ingestion/spadl_conversion.py`` (per source) must agree column-for-column
with these DDL constants — otherwise the per-game UDF return shape drifts from
the live Delta table contract and the next ``write_delta_table`` /
``replaceWhere`` MERGE silently fails with ``DELTA_FAILED_TO_MERGE_FIELDS``
(same failure-class that caused four DEFCON outages during PR 6 — see
ADR-002 §4 + ``test_defcon_schema_parity.py`` for the canonical reference
application; ``test_pausa_writer_parity.py`` for the PAUSA peer guard).

The test below parses ``_SPADL_SCHEMA`` + ``_VAEP_SCHEMA`` and asserts every
writer column present in either applyInPandas StructType (StatsBomb path or
Wyscout path) agrees with the table contract.
"""

from __future__ import annotations

import re

import pytest

_DDL_TYPE_TO_SPARK_NAME = {
    "STRING": "string",
    "BIGINT": "bigint",
    "INT": "int",
    "DOUBLE": "double",
    "FLOAT": "float",
    "TIMESTAMP": "timestamp",
    "BOOLEAN": "boolean",
}


def _parse_ddl(ddl: str) -> dict[str, str]:
    """Return ``{col_name: spark_type_name}`` from a CREATE-TABLE-style DDL."""
    out: dict[str, str] = {}
    for raw in ddl.split(","):
        tok = raw.strip()
        if not tok:
            continue
        m = re.match(r"^([A-Za-z_][A-Za-z0-9_]*)\s+([A-Z]+)\b", tok)
        if not m:
            raise AssertionError(f"unparseable DDL fragment: {tok!r}")
        col, ddl_type = m.group(1), m.group(2)
        if ddl_type not in _DDL_TYPE_TO_SPARK_NAME:
            raise AssertionError(f"unknown DDL type {ddl_type!r} for column {col!r}")
        out[col] = _DDL_TYPE_TO_SPARK_NAME[ddl_type]
    return out


def _build_statsbomb_spadl_struct():  # type: ignore[no-untyped-def]
    """Replay the StatsBomb applyInPandas StructType in spadl_conversion.py."""
    pyspark_types = pytest.importorskip("pyspark.sql.types")
    BooleanType = pyspark_types.BooleanType  # noqa: N806
    DoubleType = pyspark_types.DoubleType  # noqa: N806
    LongType = pyspark_types.LongType  # noqa: N806
    StringType = pyspark_types.StringType  # noqa: N806
    StructField = pyspark_types.StructField  # noqa: N806
    StructType = pyspark_types.StructType  # noqa: N806

    return StructType(
        [
            StructField("game_id", LongType()),
            StructField("match_id", LongType()),
            StructField("original_event_id", StringType()),
            StructField("period_id", LongType()),
            StructField("time_seconds", DoubleType()),
            StructField("team_id", LongType()),
            StructField("player_id", LongType()),
            StructField("start_x", DoubleType()),
            StructField("start_y", DoubleType()),
            StructField("end_x", DoubleType()),
            StructField("end_y", DoubleType()),
            StructField("type_id", LongType()),
            StructField("result_id", LongType()),
            StructField("bodypart_id", LongType()),
            StructField("action_id", LongType()),  # LL2: surfaced from convert_to_actions
            StructField("competition_id", LongType()),
            StructField("season_id", LongType()),
            StructField("data_source", StringType()),
            StructField("statsbomb_possession_id", LongType()),
            StructField("statsbomb_possession_team_id", LongType()),
            StructField("statsbomb_play_pattern", StringType()),
            StructField("statsbomb_under_pressure", BooleanType()),
            # LL2: 6 post-conversion enrichment columns
            StructField("possession_id_heuristic", LongType()),
            StructField("gk_role", StringType()),
            StructField("gk_was_distributing", BooleanType()),
            StructField("gk_was_engaged", BooleanType()),
            StructField("gk_actions_in_possession", LongType()),
            StructField("defending_gk_player_id", LongType()),
            # LL2 Path B: native string identifiers (Kimball-aligned).
            StructField("team_id_native", StringType()),
            StructField("home_team_id_native", StringType()),
            StructField("competition_native_id", StringType()),
            StructField("season_native_id", StringType()),
            StructField("match_id_native", StringType()),
            # PR-LL2 Path B close-out (2026-04-29): silly-kicks 2.0.0 sportec
            # tackle qualifier columns.
            StructField("tackle_winner_player_id_native", StringType()),
            StructField("tackle_winner_player_key", LongType()),
            StructField("tackle_winner_team_id_native", StringType()),
            StructField("tackle_winner_team_key", LongType()),
            StructField("tackle_loser_player_id_native", StringType()),
            StructField("tackle_loser_player_key", LongType()),
            StructField("tackle_loser_team_id_native", StringType()),
            StructField("tackle_loser_team_key", LongType()),
            # silly-kicks 4.13.0 (sk ADR-018): is_synthetic provenance flag.
            StructField("is_synthetic", BooleanType()),
        ]
    )


def _build_wyscout_spadl_struct():  # type: ignore[no-untyped-def]
    """Replay the Wyscout applyInPandas StructType in spadl_conversion.py."""
    pyspark_types = pytest.importorskip("pyspark.sql.types")
    BooleanType = pyspark_types.BooleanType  # noqa: N806
    DoubleType = pyspark_types.DoubleType  # noqa: N806
    LongType = pyspark_types.LongType  # noqa: N806
    StringType = pyspark_types.StringType  # noqa: N806
    StructField = pyspark_types.StructField  # noqa: N806
    StructType = pyspark_types.StructType  # noqa: N806

    return StructType(
        [
            StructField("game_id", LongType()),
            StructField("match_id", LongType()),
            StructField("original_event_id", StringType()),
            StructField("period_id", LongType()),
            StructField("time_seconds", DoubleType()),
            StructField("team_id", LongType()),
            StructField("player_id", LongType()),
            StructField("start_x", DoubleType()),
            StructField("start_y", DoubleType()),
            StructField("end_x", DoubleType()),
            StructField("end_y", DoubleType()),
            StructField("type_id", LongType()),
            StructField("result_id", LongType()),
            StructField("bodypart_id", LongType()),
            StructField("action_id", LongType()),
            StructField("competition_id", LongType()),
            StructField("season_id", LongType()),
            StructField("data_source", StringType()),
            StructField("statsbomb_possession_id", LongType()),
            StructField("statsbomb_possession_team_id", LongType()),
            StructField("statsbomb_play_pattern", StringType()),
            StructField("statsbomb_under_pressure", BooleanType()),
            StructField("possession_id_heuristic", LongType()),
            StructField("gk_role", StringType()),
            StructField("gk_was_distributing", BooleanType()),
            StructField("gk_was_engaged", BooleanType()),
            StructField("gk_actions_in_possession", LongType()),
            StructField("defending_gk_player_id", LongType()),
            # LL2 Path B: native string identifiers (Kimball-aligned).
            StructField("team_id_native", StringType()),
            StructField("home_team_id_native", StringType()),
            StructField("competition_native_id", StringType()),
            StructField("season_native_id", StringType()),
            StructField("match_id_native", StringType()),
            # PR-LL2 Path B close-out (2026-04-29): silly-kicks 2.0.0 sportec
            # tackle qualifier columns.
            StructField("tackle_winner_player_id_native", StringType()),
            StructField("tackle_winner_player_key", LongType()),
            StructField("tackle_winner_team_id_native", StringType()),
            StructField("tackle_winner_team_key", LongType()),
            StructField("tackle_loser_player_id_native", StringType()),
            StructField("tackle_loser_player_key", LongType()),
            StructField("tackle_loser_team_id_native", StringType()),
            StructField("tackle_loser_team_key", LongType()),
            # silly-kicks 4.13.0 (sk ADR-018): is_synthetic provenance flag.
            StructField("is_synthetic", BooleanType()),
        ]
    )


class TestSpadlVaepWriterDdlParity:
    """spadl_conversion.py applyInPandas schemas must match _SPADL_SCHEMA DDL."""

    def test_statsbomb_struct_matches_spadl_ddl(self) -> None:
        from ingestion import spadl_vaep

        ddl = _parse_ddl(spadl_vaep._SPADL_SCHEMA)
        out = {f.name: f.dataType.simpleString() for f in _build_statsbomb_spadl_struct().fields}

        missing = [c for c in out if c not in ddl]
        assert not missing, (
            f"StatsBomb spadl writer emits columns absent from _SPADL_SCHEMA: {missing}. "
            "DELTA_FAILED_TO_MERGE_FIELDS will fire on next replaceWhere write to "
            "bronze.spadl_actions."
        )

        mismatched = {c: (out[c], ddl[c]) for c in out if out[c] != ddl[c]}
        assert not mismatched, (
            f"StatsBomb spadl writer/DDL type drift {mismatched}. Either widen the DDL or narrow the StructType."
        )

    def test_spadl_ddl_includes_new_preserve_native_columns(self) -> None:
        """LL1: silly-kicks 1.5.0+ preserve_native surfaces 4 statsbomb_* fields."""
        from ingestion import spadl_vaep

        ddl = _parse_ddl(spadl_vaep._SPADL_SCHEMA)
        for col in (
            "statsbomb_possession_id",
            "statsbomb_possession_team_id",
            "statsbomb_play_pattern",
            "statsbomb_under_pressure",
        ):
            assert col in ddl, (
                f"_SPADL_SCHEMA missing PR-LL1 column {col!r}. "
                "silly-kicks 1.5.0+ preserve_native passthrough requires this."
            )

    def test_vaep_ddl_includes_new_preserve_native_columns(self) -> None:
        """LL1: vaep_action_values must carry the same 4 statsbomb_* fields downstream."""
        from ingestion import spadl_vaep

        ddl = _parse_ddl(spadl_vaep._VAEP_SCHEMA)
        for col in (
            "statsbomb_possession_id",
            "statsbomb_possession_team_id",
            "statsbomb_play_pattern",
            "statsbomb_under_pressure",
        ):
            assert col in ddl, (
                f"_VAEP_SCHEMA missing PR-LL1 column {col!r}. "
                "VAEP scoring UDF must carry preserve_native fields through to bronze.vaep_action_values."
            )

    def test_vaep_dtypes_match_spadl_dtypes_for_new_columns(self) -> None:
        """The 4 statsbomb_* columns must have identical types in both DDLs
        (otherwise vaep_action_values would have a narrower / mismatched view)."""
        from ingestion import spadl_vaep

        spadl = _parse_ddl(spadl_vaep._SPADL_SCHEMA)
        vaep = _parse_ddl(spadl_vaep._VAEP_SCHEMA)
        for col in (
            "statsbomb_possession_id",
            "statsbomb_possession_team_id",
            "statsbomb_play_pattern",
            "statsbomb_under_pressure",
        ):
            assert spadl[col] == vaep[col], (
                f"PR-LL1 column {col!r} type drift: _SPADL_SCHEMA={spadl[col]!r} vs _VAEP_SCHEMA={vaep[col]!r}"
            )

    def test_spadl_ddl_includes_action_id(self) -> None:
        """LL2: action_id must be declared in _SPADL_SCHEMA (currently is — test
        guards against accidental removal)."""
        from ingestion import spadl_vaep

        ddl = _parse_ddl(spadl_vaep._SPADL_SCHEMA)
        assert "action_id" in ddl, "_SPADL_SCHEMA missing action_id (LL2)"

    def test_spadl_ddl_includes_enrichment_columns(self) -> None:
        """LL2: 6 enrichment columns from apply_spadl_enrichments must be in _SPADL_SCHEMA."""
        from ingestion import spadl_vaep

        ddl = _parse_ddl(spadl_vaep._SPADL_SCHEMA)
        for col in (
            "possession_id_heuristic",
            "gk_role",
            "gk_was_distributing",
            "gk_was_engaged",
            "gk_actions_in_possession",
            "defending_gk_player_id",
        ):
            assert col in ddl, f"_SPADL_SCHEMA missing LL2 enrichment column {col!r}"

    def test_vaep_ddl_includes_action_id(self) -> None:
        """LL2: action_id surfaced through to vaep_action_values (was 100% NULL pre-LL2)."""
        from ingestion import spadl_vaep

        ddl = _parse_ddl(spadl_vaep._VAEP_SCHEMA)
        assert "action_id" in ddl, "_VAEP_SCHEMA missing action_id (LL2 surfaces it)"

    def test_vaep_ddl_includes_enrichment_columns(self) -> None:
        """LL2: 6 enrichment columns must propagate through to vaep_action_values."""
        from ingestion import spadl_vaep

        ddl = _parse_ddl(spadl_vaep._VAEP_SCHEMA)
        for col in (
            "possession_id_heuristic",
            "gk_role",
            "gk_was_distributing",
            "gk_was_engaged",
            "gk_actions_in_possession",
            "defending_gk_player_id",
        ):
            assert col in ddl, f"_VAEP_SCHEMA missing LL2 enrichment column {col!r}"

    def test_spadl_dtypes_match_vaep_dtypes_for_enrichment_columns(self) -> None:
        """LL2: type parity for enrichment columns across both DDLs (mirrors the existing
        statsbomb_* parity test)."""
        from ingestion import spadl_vaep

        spadl = _parse_ddl(spadl_vaep._SPADL_SCHEMA)
        vaep = _parse_ddl(spadl_vaep._VAEP_SCHEMA)
        for col in (
            "action_id",
            "possession_id_heuristic",
            "gk_role",
            "gk_was_distributing",
            "gk_was_engaged",
            "gk_actions_in_possession",
            "defending_gk_player_id",
        ):
            assert spadl[col] == vaep[col], (
                f"LL2 column {col!r} type drift: _SPADL_SCHEMA={spadl[col]!r} vs _VAEP_SCHEMA={vaep[col]!r}"
            )

    def test_wyscout_struct_matches_spadl_ddl(self) -> None:
        """LL2: Wyscout writer parity (was untested in LL1 — gap closed)."""
        from ingestion import spadl_vaep

        ddl = _parse_ddl(spadl_vaep._SPADL_SCHEMA)
        out = {f.name: f.dataType.simpleString() for f in _build_wyscout_spadl_struct().fields}

        missing = [c for c in out if c not in ddl]
        assert not missing, f"Wyscout writer emits columns absent from _SPADL_SCHEMA: {missing}"

        mismatched = {c: (out[c], ddl[c]) for c in out if out[c] != ddl[c]}
        assert not mismatched, f"Wyscout writer/DDL type drift {mismatched}"

    def test_idsse_struct_matches_spadl_ddl(self) -> None:
        """LL2 Path B: IDSSE writer parity (NEW source — silly-kicks 1.7.0 sportec)."""
        from ingestion import spadl_vaep

        ddl = _parse_ddl(spadl_vaep._SPADL_SCHEMA)
        out = {f.name: f.dataType.simpleString() for f in _build_idsse_spadl_struct().fields}

        missing = [c for c in out if c not in ddl]
        assert not missing, f"IDSSE writer emits columns absent from _SPADL_SCHEMA: {missing}"

        mismatched = {c: (out[c], ddl[c]) for c in out if out[c] != ddl[c]}
        assert not mismatched, f"IDSSE writer/DDL type drift {mismatched}"

    def test_metrica_struct_matches_spadl_ddl(self) -> None:
        """LL2 Path B: Metrica writer parity (NEW source — silly-kicks 1.7.0 metrica)."""
        from ingestion import spadl_vaep

        ddl = _parse_ddl(spadl_vaep._SPADL_SCHEMA)
        out = {f.name: f.dataType.simpleString() for f in _build_metrica_spadl_struct().fields}

        missing = [c for c in out if c not in ddl]
        assert not missing, f"Metrica writer emits columns absent from _SPADL_SCHEMA: {missing}"

        mismatched = {c: (out[c], ddl[c]) for c in out if out[c] != ddl[c]}
        assert not mismatched, f"Metrica writer/DDL type drift {mismatched}"

    def test_spadl_ddl_includes_native_id_columns(self) -> None:
        """LL2 Path B + PR-LL3 S2: native (string) identifier columns aligning
        bronze.spadl_actions with the ADR-011 dim_competitions Kimball pattern."""
        from ingestion import spadl_vaep

        ddl = _parse_ddl(spadl_vaep._SPADL_SCHEMA)
        for col, expected_type in (
            ("team_id_native", "string"),
            ("home_team_id_native", "string"),
            ("competition_native_id", "string"),
            ("season_native_id", "string"),
            ("match_id_native", "string"),
            ("player_id_native", "string"),
        ):
            assert col in ddl, f"_SPADL_SCHEMA missing native-id column {col!r}"
            assert ddl[col] == expected_type, (
                f"_SPADL_SCHEMA {col!r} type drift: got {ddl[col]!r}, expected {expected_type!r}"
            )

    def test_vaep_ddl_includes_native_id_columns(self) -> None:
        """LL2 Path B: native-id columns must propagate through to vaep_action_values."""
        from ingestion import spadl_vaep

        ddl = _parse_ddl(spadl_vaep._VAEP_SCHEMA)
        for col, expected_type in (
            ("team_id_native", "string"),
            ("home_team_id_native", "string"),
            ("competition_native_id", "string"),
            ("season_native_id", "string"),
            ("match_id_native", "string"),
        ):
            assert col in ddl, f"_VAEP_SCHEMA missing LL2 native-id column {col!r}"
            assert ddl[col] == expected_type, (
                f"_VAEP_SCHEMA {col!r} type drift: got {ddl[col]!r}, expected {expected_type!r}"
            )

    @pytest.mark.parametrize(
        "struct_builder_name",
        [
            "_build_statsbomb_spadl_struct",
            "_build_wyscout_spadl_struct",
            "_build_idsse_spadl_struct",
            "_build_metrica_spadl_struct",
            "_build_vaep_scoring_struct",
        ],
    )
    def test_struct_includes_tackle_qualifiers(self, struct_builder_name: str) -> None:
        """PR-LL2 Path B close-out (2026-04-29): every applyInPandas StructType
        in the SPADL/VAEP pipeline must include the 4 silly-kicks 2.0.0 tackle
        qualifier columns (multi-source schema parity). NULL on non-sportec
        rows; populated on sportec TacklingGame rows where DFL XML qualifier present.
        ADR-018 + ADR-016."""
        builder = globals()[struct_builder_name]
        struct = builder()
        cols = {f.name for f in struct.fields}
        expected = {
            "tackle_winner_player_id_native",
            "tackle_winner_player_key",
            "tackle_winner_team_id_native",
            "tackle_winner_team_key",
            "tackle_loser_player_id_native",
            "tackle_loser_player_key",
            "tackle_loser_team_id_native",
            "tackle_loser_team_key",
        }
        missing = expected - cols
        assert not missing, (
            f"{struct_builder_name}: missing tackle qualifier columns {missing}. "
            "silly-kicks 2.0.0 SPORTEC_SPADL_COLUMNS extension requires these "
            "in every applyInPandas struct (multi-source parity)."
        )

    def test_spadl_ddl_includes_tackle_qualifiers(self) -> None:
        """PR-LL2 Path B close-out: 4 tackle qualifier columns in _SPADL_SCHEMA."""
        from ingestion import spadl_vaep

        ddl = _parse_ddl(spadl_vaep._SPADL_SCHEMA)
        for col, expected_type in (
            ("tackle_winner_player_id_native", "string"),
            ("tackle_winner_player_key", "bigint"),
            ("tackle_winner_team_id_native", "string"),
            ("tackle_winner_team_key", "bigint"),
            ("tackle_loser_player_id_native", "string"),
            ("tackle_loser_player_key", "bigint"),
            ("tackle_loser_team_id_native", "string"),
            ("tackle_loser_team_key", "bigint"),
        ):
            assert col in ddl, f"_SPADL_SCHEMA missing tackle qualifier column {col!r}"
            assert ddl[col] == expected_type, (
                f"_SPADL_SCHEMA {col!r} type drift: got {ddl[col]!r}, expected {expected_type!r}"
            )

    def test_vaep_ddl_includes_tackle_qualifiers(self) -> None:
        """PR-LL2 Path B close-out: 4 tackle qualifier columns must propagate through to vaep_action_values."""
        from ingestion import spadl_vaep

        ddl = _parse_ddl(spadl_vaep._VAEP_SCHEMA)
        for col, expected_type in (
            ("tackle_winner_player_id_native", "string"),
            ("tackle_winner_player_key", "bigint"),
            ("tackle_winner_team_id_native", "string"),
            ("tackle_winner_team_key", "bigint"),
            ("tackle_loser_player_id_native", "string"),
            ("tackle_loser_player_key", "bigint"),
            ("tackle_loser_team_id_native", "string"),
            ("tackle_loser_team_key", "bigint"),
        ):
            assert col in ddl, f"_VAEP_SCHEMA missing tackle qualifier column {col!r}"
            assert ddl[col] == expected_type, (
                f"_VAEP_SCHEMA {col!r} type drift: got {ddl[col]!r}, expected {expected_type!r}"
            )

    def test_spadl_ddl_includes_is_synthetic(self) -> None:
        """silly-kicks 4.13.0 (sk ADR-018): is_synthetic provenance flag in _SPADL_SCHEMA."""
        from ingestion import spadl_vaep

        ddl = _parse_ddl(spadl_vaep._SPADL_SCHEMA)
        assert "is_synthetic" in ddl, "_SPADL_SCHEMA missing is_synthetic (silly-kicks 4.13.0)"
        assert ddl["is_synthetic"] == "boolean", (
            f"_SPADL_SCHEMA is_synthetic type drift: got {ddl['is_synthetic']!r}, expected 'boolean'"
        )

    def test_vaep_ddl_includes_is_synthetic(self) -> None:
        """is_synthetic must propagate through to vaep_action_values."""
        from ingestion import spadl_vaep

        ddl = _parse_ddl(spadl_vaep._VAEP_SCHEMA)
        assert "is_synthetic" in ddl, "_VAEP_SCHEMA missing is_synthetic (silly-kicks 4.13.0)"
        assert ddl["is_synthetic"] == "boolean", (
            f"_VAEP_SCHEMA is_synthetic type drift: got {ddl['is_synthetic']!r}, expected 'boolean'"
        )

    def test_spadl_vaep_is_synthetic_type_parity(self) -> None:
        """is_synthetic must be BOOLEAN in BOTH DDLs (no narrowing across the boundary)."""
        from ingestion import spadl_vaep

        spadl = _parse_ddl(spadl_vaep._SPADL_SCHEMA)
        vaep = _parse_ddl(spadl_vaep._VAEP_SCHEMA)
        assert spadl["is_synthetic"] == vaep["is_synthetic"], (
            f"is_synthetic type drift: _SPADL_SCHEMA={spadl['is_synthetic']!r} vs _VAEP_SCHEMA={vaep['is_synthetic']!r}"
        )

    def test_vaep_per_game_projection_matches_output_cols(self) -> None:
        """PR-LL2 Path B close-out hot-fix (2026-04-30): regression test for the
        VAEP scoring UDF's inner per-game column-projection list. The list at
        ``_make_scoring_udf._udf.<inner>`` filters ``game_actions`` columns by
        membership in a hardcoded list, then the outer ``[_output_cols]``
        projection at the function tail asserts presence. If the inner list
        is missing a column that ``_output_cols`` requires, the UDF raises
        ``KeyError: '<col> not in index'`` at runtime — the LL1 latent-bug
        class re-applied to a different layer.

        This test parses the UDF source via AST, extracts both the inner
        passthrough list and ``_output_cols``, and asserts the inner list
        contains every passthrough column that would appear in
        ``_output_cols`` (excluding the 9 columns added during scoring).
        """
        import ast
        import inspect

        from ingestion import spadl_vaep

        # _make_scoring_udf is the closure-builder. Parse its source.
        src = inspect.getsource(spadl_vaep._make_scoring_udf)
        tree = ast.parse(src)

        # Collect all string-list literals nested in the function body.
        # The inner per-game projection list is the longest string-list
        # literal nested in a List comprehension iterator.
        string_lists: list[list[str]] = []
        for node in ast.walk(tree):
            if isinstance(node, ast.ListComp):
                # The list comp pattern: [c for c in [<list>] if ...]
                if isinstance(node.generators[0].iter, ast.List) and all(
                    isinstance(e, ast.Constant) and isinstance(e.value, str) for e in node.generators[0].iter.elts
                ):
                    string_lists.append(
                        [
                            str(e.value)
                            for e in node.generators[0].iter.elts
                            if isinstance(e, ast.Constant) and isinstance(e.value, str)
                        ]
                    )
            elif isinstance(node, ast.Assign):
                # _output_cols = _pd.Index([...]) — extract the Index arg
                for target in node.targets:
                    if isinstance(target, ast.Name) and target.id == "_output_cols":
                        if (
                            isinstance(node.value, ast.Call)
                            and isinstance(node.value.func, ast.Attribute)
                            and node.value.func.attr == "Index"
                            and isinstance(node.value.args[0], ast.List)
                        ):
                            string_lists.append(
                                [
                                    str(e.value)
                                    for e in node.value.args[0].elts
                                    if isinstance(e, ast.Constant) and isinstance(e.value, str)
                                ]
                            )

        # Heuristic: 2 string lists of substantial length — _output_cols + per-game projection
        long_lists = [lst for lst in string_lists if len(lst) > 10]
        assert len(long_lists) >= 2, (
            f"AST parse expected ≥2 long string lists in _make_scoring_udf; got {len(long_lists)}"
        )

        # Per-game projection columns + _output_cols columns (different lengths,
        # we want the smaller one to be a SUBSET of the larger PLUS the cols
        # added during scoring).
        scoring_added = {
            "action_type",
            "action_result",
            "bodypart",
            "offensive_value",
            "defensive_value",
            "vaep_value",
            "competition_id",
            "season_id",
            "data_source",
        }
        passthrough_renamed = {"type_name", "result_name", "bodypart_name"}

        # Pair: smaller list is per-game-projection, larger is _output_cols (or vice versa).
        # Both must include the 4 tackle qualifier columns.
        for col in (
            "tackle_winner_player_id_native",
            "tackle_winner_player_key",
            "tackle_winner_team_id_native",
            "tackle_winner_team_key",
            "tackle_loser_player_id_native",
            "tackle_loser_player_key",
            "tackle_loser_team_id_native",
            "tackle_loser_team_key",
        ):
            for lst in long_lists:
                assert col in lst, (
                    f"_make_scoring_udf string-list missing tackle qualifier column {col!r}. "
                    "Both the per-game projection list AND _output_cols must include it — "
                    "drift between them produces KeyError at runtime (the LL1 latent-bug class)."
                )

        # Stronger assertion: every column in _output_cols (excluding the
        # 9 columns added during scoring) must appear in the per-game
        # projection list, modulo the 3 documented rename mappings
        # (`type_name` → `action_type`, `result_name` → `action_result`,
        # `bodypart_name` → `bodypart`) which silly-kicks `add_names`
        # adds before scoring.
        sorted_lists = sorted(long_lists, key=len)
        per_game_proj = sorted_lists[0]
        output_cols_list = sorted_lists[-1]
        rename_back = {
            "action_type": "type_name",
            "action_result": "result_name",
            "bodypart": "bodypart_name",
        }
        required_in_per_game = set(output_cols_list) - scoring_added
        missing = required_in_per_game - (set(per_game_proj) | passthrough_renamed)
        missing_after_rename = {c for c in missing if rename_back.get(c, c) not in per_game_proj}
        assert not missing_after_rename, (
            f"_output_cols requires columns NOT in per-game projection list: "
            f"{sorted(missing_after_rename)}. Adding a column requires updating "
            "BOTH the per-game projection list AND _output_cols."
        )


def _build_metrica_spadl_struct():  # type: ignore[no-untyped-def]
    """Replay the Metrica applyInPandas StructType in spadl_conversion.py (LL2 Path B).

    Identical to IDSSE struct (multi-source parity) — both use the same
    LL2 Path B Kimball-aligned schema. Difference is only at the data level:
    Metrica match_ids hash from 'Sample_Game_N' strings; IDSSE from
    'idsse_J03WMX' strings.
    """
    return _build_idsse_spadl_struct()


def _build_idsse_spadl_struct():  # type: ignore[no-untyped-def]
    """Replay the IDSSE applyInPandas StructType in spadl_conversion.py (LL2 Path B).

    IDSSE uses silly_kicks.spadl.sportec.convert_to_actions (silly-kicks 1.7.0+).
    Output schema mirrors the StatsBomb / Wyscout structs, plus 4 LL2 Path B
    native-string identifier columns. Legacy BIGINT IDs (team_id / player_id /
    competition_id / season_id) are NULL for IDSSE; match_id / game_id are
    populated via deterministic SHA-256 hash of the bronze 'idsse_J03WMX' string.
    """
    pyspark_types = pytest.importorskip("pyspark.sql.types")
    BooleanType = pyspark_types.BooleanType  # noqa: N806
    DoubleType = pyspark_types.DoubleType  # noqa: N806
    LongType = pyspark_types.LongType  # noqa: N806
    StringType = pyspark_types.StringType  # noqa: N806
    StructField = pyspark_types.StructField  # noqa: N806
    StructType = pyspark_types.StructType  # noqa: N806

    return StructType(
        [
            StructField("game_id", LongType()),
            StructField("match_id", LongType()),
            StructField("original_event_id", StringType()),
            StructField("period_id", LongType()),
            StructField("time_seconds", DoubleType()),
            StructField("team_id", LongType()),
            StructField("player_id", LongType()),
            StructField("start_x", DoubleType()),
            StructField("start_y", DoubleType()),
            StructField("end_x", DoubleType()),
            StructField("end_y", DoubleType()),
            StructField("type_id", LongType()),
            StructField("result_id", LongType()),
            StructField("bodypart_id", LongType()),
            StructField("action_id", LongType()),
            StructField("competition_id", LongType()),
            StructField("season_id", LongType()),
            StructField("data_source", StringType()),
            StructField("statsbomb_possession_id", LongType()),
            StructField("statsbomb_possession_team_id", LongType()),
            StructField("statsbomb_play_pattern", StringType()),
            StructField("statsbomb_under_pressure", BooleanType()),
            StructField("possession_id_heuristic", LongType()),
            StructField("gk_role", StringType()),
            StructField("gk_was_distributing", BooleanType()),
            StructField("gk_was_engaged", BooleanType()),
            StructField("gk_actions_in_possession", LongType()),
            StructField("defending_gk_player_id", LongType()),
            StructField("team_id_native", StringType()),
            StructField("home_team_id_native", StringType()),
            StructField("competition_native_id", StringType()),
            StructField("season_native_id", StringType()),
            StructField("match_id_native", StringType()),
            # PR-LL2 Path B close-out (2026-04-29): silly-kicks 2.0.0 sportec
            # tackle qualifier columns. IDSSE writer populates these when
            # the DFL XML qualifier is present; NaN otherwise.
            StructField("tackle_winner_player_id_native", StringType()),
            StructField("tackle_winner_player_key", LongType()),
            StructField("tackle_winner_team_id_native", StringType()),
            StructField("tackle_winner_team_key", LongType()),
            StructField("tackle_loser_player_id_native", StringType()),
            StructField("tackle_loser_player_key", LongType()),
            StructField("tackle_loser_team_id_native", StringType()),
            StructField("tackle_loser_team_key", LongType()),
            # silly-kicks 4.13.0 (sk ADR-018): is_synthetic provenance flag.
            StructField("is_synthetic", BooleanType()),
        ]
    )


def _build_vaep_scoring_struct():  # type: ignore[no-untyped-def]
    """Replay the VAEP scoring applyInPandas StructType from spadl_vaep.run_pipeline.

    LL2: This test closes the LL1 latent-bug class — the original LL1 vaep_schema
    omitted statsbomb_* columns, which silently dropped them at the applyInPandas
    boundary. 0 of 7,151,510 StatsBomb rows ended up with non-NULL
    statsbomb_possession_id post-LL1. This struct must agree column-for-column
    with _VAEP_SCHEMA so the same drift cannot recur.
    """
    pyspark_types = pytest.importorskip("pyspark.sql.types")
    BooleanType = pyspark_types.BooleanType  # noqa: N806
    DoubleType = pyspark_types.DoubleType  # noqa: N806
    LongType = pyspark_types.LongType  # noqa: N806
    StringType = pyspark_types.StringType  # noqa: N806
    StructField = pyspark_types.StructField  # noqa: N806
    StructType = pyspark_types.StructType  # noqa: N806

    return StructType(
        [
            StructField("game_id", LongType()),
            StructField("match_id", LongType()),
            StructField("original_event_id", StringType()),
            StructField("period_id", LongType()),
            StructField("time_seconds", DoubleType()),
            StructField("team_id", LongType()),
            StructField("player_id", LongType()),
            StructField("start_x", DoubleType()),
            StructField("start_y", DoubleType()),
            StructField("end_x", DoubleType()),
            StructField("end_y", DoubleType()),
            StructField("type_id", LongType()),
            StructField("action_type", StringType()),
            StructField("result_id", LongType()),
            StructField("action_result", StringType()),
            StructField("bodypart_id", LongType()),
            StructField("bodypart", StringType()),
            StructField("offensive_value", DoubleType()),
            StructField("defensive_value", DoubleType()),
            StructField("vaep_value", DoubleType()),
            StructField("competition_id", LongType()),
            StructField("season_id", LongType()),
            StructField("data_source", StringType()),
            # LL2: action_id surfaced through to vaep_action_values
            StructField("action_id", LongType()),
            # PR-LL1 statsbomb_* (closes LL1 latent bug — must be in vaep_schema)
            StructField("statsbomb_possession_id", LongType()),
            StructField("statsbomb_possession_team_id", LongType()),
            StructField("statsbomb_play_pattern", StringType()),
            StructField("statsbomb_under_pressure", BooleanType()),
            # LL2: 6 enrichment columns
            StructField("possession_id_heuristic", LongType()),
            StructField("gk_role", StringType()),
            StructField("gk_was_distributing", BooleanType()),
            StructField("gk_was_engaged", BooleanType()),
            StructField("gk_actions_in_possession", LongType()),
            StructField("defending_gk_player_id", LongType()),
            # LL2 Path B: native string identifiers carried through from spadl_actions.
            StructField("player_id_native", StringType()),
            StructField("team_id_native", StringType()),
            StructField("home_team_id_native", StringType()),
            StructField("competition_native_id", StringType()),
            StructField("season_native_id", StringType()),
            StructField("match_id_native", StringType()),
            # PR-LL2 Path B close-out (2026-04-29): silly-kicks 2.0.0 sportec
            # tackle qualifier columns carried through to vaep_action_values.
            StructField("tackle_winner_player_id_native", StringType()),
            StructField("tackle_winner_player_key", LongType()),
            StructField("tackle_winner_team_id_native", StringType()),
            StructField("tackle_winner_team_key", LongType()),
            StructField("tackle_loser_player_id_native", StringType()),
            StructField("tackle_loser_player_key", LongType()),
            StructField("tackle_loser_team_id_native", StringType()),
            StructField("tackle_loser_team_key", LongType()),
            # silly-kicks 4.13.0 (sk ADR-018): is_synthetic provenance flag.
            StructField("is_synthetic", BooleanType()),
        ]
    )


class TestVaepScoringWriterDdlParity:
    """spadl_vaep.run_pipeline vaep_schema must match _VAEP_SCHEMA DDL.

    LL2: closes the LL1 latent-bug class. The original LL1 release shipped
    _VAEP_SCHEMA with 4 statsbomb_* columns + DDL-side ALTERed
    bronze.vaep_action_values to add them, but the actual applyInPandas
    StructType inside run_pipeline did not include statsbomb_*. Spark
    silently dropped them on every write — 0 of 7M rows ended up populated.
    This class makes the failure visible at unit-test time.
    """

    def test_vaep_scoring_struct_matches_vaep_ddl(self) -> None:
        from ingestion import spadl_vaep

        ddl = _parse_ddl(spadl_vaep._VAEP_SCHEMA)
        out = {f.name: f.dataType.simpleString() for f in _build_vaep_scoring_struct().fields}

        missing = [c for c in out if c not in ddl]
        assert not missing, (
            f"VAEP scoring writer emits columns absent from _VAEP_SCHEMA: {missing}. "
            "DELTA_FAILED_TO_MERGE_FIELDS will fire on next replaceWhere write."
        )

        ddl_only = [c for c in ddl if c not in out and c != "_ingested_at"]
        assert not ddl_only, (
            f"_VAEP_SCHEMA declares columns absent from VAEP scoring writer: {ddl_only}. "
            "These columns will be silently NULL-filled on every write — closing this gap is "
            "exactly the LL1 latent-bug class. Add them to vaep_schema in spadl_vaep.run_pipeline."
        )

        mismatched = {c: (out[c], ddl[c]) for c in out if out[c] != ddl[c]}
        assert not mismatched, f"VAEP scoring writer/DDL type drift {mismatched}"
