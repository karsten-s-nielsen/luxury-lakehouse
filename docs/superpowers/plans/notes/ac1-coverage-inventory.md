# AC-1 hexagon relocation — test-coverage inventory (Task A0.1, 2026-05-28)

Coverage of each function being relocated/copied, before the move:

| Function | Status | Existing test(s) |
|---|---|---|
| `_enrich_tracking_match` | direct-unit ✔ | `test_action_context_enrichment.py` |
| `_enrich_sb360_match` | direct-unit ✔ | `test_action_context_enrichment.py` |
| `_enrich_event_only_match` | direct-unit ✔ | `test_action_context_enrichment.py` |
| `_build_output` | direct-unit ✔ | `test_action_context_enrichment.py` (+ TF/DAG tests) |
| `_bronze_idsse_to_sportec_input` | direct-unit ✔ | `test_tracking_context_udf.py` |
| `_bronze_metrica_to_frames` | direct-unit ✔ | `test_tracking_context_converters.py`, `test_metrica_tracking_player_id.py` |
| `_bronze_skillcorner_to_frames` | direct-unit ✔ | `test_tracking_context_converters.py`, `test_tracking_context_skillcorner_local.py` |
| `_bronze_gradientsports_to_converter_input` | **NONE** ✗ | — add a pure unit test in A0.2 |
| `_RESULT_COLUMNS` | direct-unit ✔ | `test_action_context_schema_parity.py` |
| `_ACTION_CONTEXT_DDL` | direct-unit ✔ | `test_action_context_schema_parity.py` |

**Conclusion:** the pure net already exists for almost everything (so the move happens
under green local tests). The single gap is the GradientSports converter — A0.2 adds its
pure unit test + captures the pre-refactor behavior baseline (M10).
