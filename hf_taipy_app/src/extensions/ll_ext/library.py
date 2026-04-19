"""Taipy GUI extension library — custom visual elements for the Luxury Lakehouse app.

Elements
--------
- `ll_ext.combobox`: WAI-ARIA APG combobox-with-list-autocomplete wrapping
  MUI Autocomplete. Replaces the provisional `SidebarWidget(searchable=True)`
  composition of `|input|` + non-dropdown `|selector|`.

Usage (Taipy Markdown)
----------------------
    <|{selected_label}|ll_ext.combobox
      |lov={player_lov}
      |label=Player
      |placeholder=Type to search…
      |search={player_search_query}
      |on_change=on_player_change
      |on_search=on_player_search_change
    |>

The JavaScript bundle is built into `front-end/dist/library.js` and served
through Taipy's Flask extension blueprint at
`/taipy-extension/ll_ext/front-end/dist/library.js`.
"""

from __future__ import annotations

import hashlib
from pathlib import Path

from taipy.gui.extension import Element, ElementLibrary, ElementProperty, PropertyType


class LlExtLibrary(ElementLibrary):
    """Luxury Lakehouse custom Taipy GUI elements."""

    _LIB_NAME = "ll_ext"
    _BUNDLE_REL_PATH = "front-end/dist/library.js"

    def get_name(self) -> str:
        return self._LIB_NAME

    def get_elements(self) -> dict[str, Element]:
        return {
            # WAI-ARIA APG combobox-with-list-autocomplete.
            #
            # Default property: `value` — the currently selected label (the
            # Taipy LOV pattern in this project stores only labels, so the
            # "id" and "label" are identical strings).
            #
            # The listbox is HIDDEN when: input empty AND input not focused,
            # or after Escape / selection. It is VISIBLE when focused with
            # non-empty input, or on ArrowDown keystroke. Keyboard navigation
            # (Up/Down/Enter/Escape) and ARIA attributes (role=combobox,
            # aria-autocomplete=list, aria-controls, aria-expanded,
            # aria-activedescendant) are handled by MUI Autocomplete and
            # verified in staging Puppeteer tests.
            "combobox": Element(
                default_property="value",
                properties={
                    # Selected label (two-way; default property). Uses
                    # `lov_value` so Taipy's wire format matches what native
                    # LOV controls expect — a `dynamic_string` default of ""
                    # is serialized as the list-shaped `["\""]` sentinel,
                    # which surfaces to the JS side as a literal string and
                    # corrupts the input display.
                    "value": ElementProperty(PropertyType.lov_value),
                    # List of option labels. Taipy serializes list[str] to
                    # LoVElt[]; the client parses via `useLovListMemo`.
                    "lov": ElementProperty(PropertyType.lov_no_default),
                    # Search-input text (two-way; debounced client-side,
                    # fires `on_search` after `debounce_ms`). with_update=True
                    # signals Taipy that client->server propagation is needed,
                    # so the backend var name is published in `updateVars`.
                    "search": ElementProperty(PropertyType.dynamic_string, "", with_update=True),
                    # Static UX strings.
                    "label": ElementProperty(PropertyType.dynamic_string, ""),
                    "placeholder": ElementProperty(PropertyType.dynamic_string, "Type to search\u2026"),
                    "no_matches_label": ElementProperty(PropertyType.dynamic_string, "No matches"),
                    # Layout / accessibility.
                    "required": ElementProperty(PropertyType.boolean, True),
                    "class_name": ElementProperty(PropertyType.dynamic_string, ""),
                    "debounce_ms": ElementProperty(PropertyType.number, 300),
                    # Taipy callback function names (strings).
                    "on_change": ElementProperty(PropertyType.function),
                    "on_search": ElementProperty(PropertyType.function),
                },
                react_component="Combobox",
            ),
        }

    def get_scripts(self) -> list[str]:
        return [self._BUNDLE_REL_PATH]

    def get_version(self) -> str | None:
        """Short hash of the built bundle — appended as `?v=<hash>` to the
        script URL for cache-busting. Returns None if the bundle is missing
        (build step hasn't run); the extension is still registered but the
        script won't load until the bundle is built.
        """
        bundle = Path(__file__).parent / self._BUNDLE_REL_PATH
        if not bundle.exists():
            return None
        return hashlib.sha256(bundle.read_bytes()).hexdigest()[:12]
