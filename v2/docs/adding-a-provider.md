# Adding a data provider

This is the recipe for ingesting a new soccer-data provider. Most new data arrives through the **pining API**, so start
there; a direct subscription (pull / push / api) is the second path.

## The two interfaces you implement

A provider is one channel adapter plus one parser. They are independent packages (an import-linter contract): the
adapter never parses, the parser never fetches. Because they are independent, neither imports the other, so each side
keeps its own provider list. A new provider is added to BOTH sides.

- **`ChannelAdapter`** — `lakehouse/sources/base.py`. Lists a module's matches with their change signals
  (`list_entries`), gives the filter-editor preview (`preview`), and fetches one artifact as a `Delivery` (`fetch`).
  Implement `ListingSource` too when the source has source-level listings (a match / player / competition list).
- **`ProviderParser`** — `lakehouse/parsers/base.py`. Turns a match's `Delivery` objects into typed bronze tables and a
  `CoordinateSystem` per coordinate-bearing artifact (`parse`), and declares its fields (`field_inventory`) and bronze
  columns (`bronze_columns`). A raw field that no bronze column maps raises `UnmappedFieldError`. This is the real work;
  it is provider-specific and unavoidable. Implement `ListingParser` too when the adapter is a `ListingSource`.

## A pining provider (the usual path)

The data arrives through the pining API, so you reuse the shared `PiningAdapter` and write only an artifact parser. The
edit sites are:

1. **`ids/native.py`** — the native-id source of truth.
   - Add the provider to the `Provider` literal and to `PROVIDER_RANK` (`PROVIDERS` derives from it). The rank sets the
     canonical-pointer precedence (spec §14.3).
   - Add the native-id formats the provider needs: `<provider>_native_match_id`, `<provider>_native_team_id`,
     `<provider>_native_player_id`, plus `<provider>_native_competition_id` when the provider carries competition ids.
   - Add the matching `NativeMatchId.<provider>` / `NativeTeamId.<provider>` / `NativePlayerId.<provider>` classmethods.
   - Add a format-contract test in `tests/contract/test_format_contract.py` for each new format.
2. **`ids/channels.py`** — add a `case "<provider>":` to `canonical_native_match_id` (import the new match-id format).
   It maps the raw id each channel sends to the canonical native match id.
3. **`config/model.py`** — add `<provider>_pining` to the `ModuleId` literal and the `MODULE_IDS` tuple.
4. **`sources/pining.py`** — add one `PiningProvider(...)` row to `PINING_PROVIDERS`: its `<provider>_pining` module id
   and the map from each pining role key to the name your parser gives that artifact. (SkillCorner is the exception: its
   keys vary by `format_version`, so it carries no map and `PiningAdapter._artifact_name` resolves them.)
5. **`parsers/pining.py`** — add the provider to the `PROVIDERS` tuple (this builds its `<provider>_pining_matches` /
   `<provider>_pining_players` listing tables) and to the `_PLAYER_IDS` map (import its `<provider>_native_player_id`).
   This is the listing parser, separate from the artifact parser in step 6.
6. **`parsers/<provider>.py`** — write the artifact `ProviderParser` (and `ListingParser` if the source has listings)
   and register it in `wiring.default_parsers`.
7. **`resolve/model.py` + `resolve/records.py`** — add the provider to `resolve.model.PROVIDER_RANK` (the
   entity-resolution precedence) and to the `_PLAYER_IDS` / `_TEAM_IDS` / `_COMPETITION_IDS` / `_MATCH_IDS` maps in
   `resolve/records.py` (each keyed by the full `Provider` literal, mapping to the step-1 native-id formats).
8. **`bronze/schemas.py`** — add the provider's bronze tables / columns if its artifact shape is new.
9. **`config/defaults.yaml`** — add the module (off by default).
10. **`services/modules.py`** — add a `DISPLAY_NAMES["<provider>_pining"]` label; without it the Ops UI shows the raw
    module id.

`PiningAdapter`, `wiring.build_adapter` and `wiring._PINING_PROVIDERS` pick the new provider up from `PINING_PROVIDERS`
automatically; you do not touch them. `cli._PROVIDERS` and `ids/keys` derive from the `Provider` literal, so they need
no edit. Two completeness tests fail loudly on a miss: `test_pining_registries_agree_on_providers` (the two pining
lists — steps 4 and 5 — must name the same set) and `test_resolve_provider_maps_cover_every_provider` (every
`resolve` map in step 7 must key the full `Provider` literal).

## A new-channel provider (a direct subscription)

The data comes from the provider directly, not through pining. Do the same as above, except:

- Instead of a `PINING_PROVIDERS` row (step 4), write a `ChannelAdapter` for the subscription. It fetches however the
  subscription works — pull, push, or an API, with or without filtering — behind the `list_entries` / `preview` /
  `fetch` methods. Steps 5 and 6 (the parsers) still apply unless the source has no listings.
- Add a branch for the new `<provider>_<channel>` module id in `wiring.build_adapter`.
- Add the channel to the `Channel` literal (`ids/native.py`) if it is new.
- The `DISPLAY_NAMES` key (step 9) is `<provider>_<channel>`.

## Verify

- `wiring.build_adapter` builds the adapter with no network call.
- The parser maps every raw field (an unmapped field raises `UnmappedFieldError`).
- `test_pining_registries_agree_on_providers` passes (both provider lists agree).
- Run the local checks (the v2 CI gate):
  - `uv run ruff check .`
  - `uv run ruff format --check .`
  - `uv run pyright`
  - `uv run lint-imports`
  - `uv run pytest -m "not live and not hf and not bench"`
  - `uv run python scripts/pip_audit.py`
