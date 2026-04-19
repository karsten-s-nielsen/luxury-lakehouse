# ADR-009: Taipy GUI extensions ship in-repo under `hf_taipy_app/src/extensions/`

| Field | Value |
|---|---|
| **Date** | 2026-04-19 |
| **Status** | Accepted |
| **Deciders** | Karsten Nielsen |

## Context

Taipy 4.1 ships no native combobox / typeahead control. The preceding "Path A"
compromise rendered server-driven player search as a `<|input|>` over a
non-dropdown `<|selector|>` whose list was always visible (hf_taipy_app/CLAUDE.md,
"Server-driven autocomplete" rule). That pattern violated the WAI-ARIA APG
combobox-with-list-autocomplete visibility rules (listbox must be hidden when
input is empty and not focused) and permanently consumed ~220 px of sidebar
height even when the user was not searching. Eight dropdowns (shared `player`
plus `gk_*`, `pt_*`, `dv_*` × 2, `tac_*` × 2, `ps_*`) used the provisional
pattern across 6 Taipy pages.

Taipy GUI supports custom React elements via `taipy.gui.extension.ElementLibrary`
— an established escape hatch (`taipy/gui_core` ships one as the reference
implementation). An extension owns its own component code and is registered at
`Gui.run()` time through `gui.add_library(...)`. A UMD bundle is built with
webpack, served through the Taipy Flask blueprint at
`/taipy-extension/<library_name>/<path>`, and rendered inline in the Markdown
page via the fragment `<|…|<library_name>.<element_name>|…|>`.

The question this ADR settles is: *where does that extension live, and how does
its build pipeline integrate with the existing deploy path?*

## Decision

The Taipy GUI extension lives **in-repo at `hf_taipy_app/src/extensions/ll_ext/`
as a single Python package holding N React elements**. The front-end bundle is
built with webpack into `front-end/dist/library.js`, **checked in to git**, and
served by the existing Taipy Flask blueprint. No separate PyPI or npm package
is published. Types for the `taipy-gui` SDK are installed with `--no-save` so
absolute, machine-specific paths do not leak into `package.json`.

## Alternatives considered

| Option | Pros | Cons | Why rejected |
|---|---|---|---|
| A. Separate npm/PyPI package (`@luxury-lakehouse/taipy-combobox`) | Hard API boundary; externally publishable; clean version contract | Two repos, two CI pipelines, two release cadences; every API change forces a bump + requirements update + deploy cycle; over-engineered for a single known consumer (this project) | — |
| B. Keep provisional `searchable=True` pattern and extend it | Zero new toolchain; all-Python | WAI-ARIA APG violation stays; sidebar height pressure stays; CHI audit debt accumulates | Fails on the accessibility bar codified in hf_taipy_app/CLAUDE.md UX Standards |
| C. In-repo extension under `hf_taipy_app/src/extensions/ll_ext/`, bundle checked in | Single PR cycle covers extension + migration + tests; no version coordination; same library grows to hold future widgets; extraction to a standalone package later is a `git mv` if ever needed; HF Space Dockerfile stays Node-free | First Node/npm toolchain in the repo; binary diff noise when bundle is rebuilt; webpack build must be run locally before commit | — |

Option B was rejected before this cycle started — Path C was scoped specifically
to replace the provisional pattern. Option A was rejected during scope
discussion with the user (2026-04-18) on the basis that the "reusability" goal
was *across widgets inside this app*, not cross-project distribution.

## Consequences

### Positive

- WAI-ARIA APG combobox-with-list-autocomplete behaviour is correct: listbox
  hidden when input empty + not focused, opens on type / ArrowDown, closes on
  Escape / selection / blur, keyboard nav with `aria-activedescendant`
  highlighting, full ARIA attribute set on the input.
- Single `ll_ext` library can grow to host future custom widgets (date-range
  picker, virtualised tree, etc.) without a new release/package for each.
- The same repo-wide ruff / pyright / pytest gates cover both the extension's
  Python side (`library.py`) and the migrating Taipy pages.
- HF Space Dockerfile stays Node-free: only the built `library.js` is shipped
  inside the Python package directory; the Taipy Flask server reads it from
  disk and serves it without any Node / npm at container runtime.
- `manage_space.py` IGNORE_PATTERNS hardened in the same change to exclude
  `node_modules/**`, `dist/*.map`, `**/.env`, `**/package-lock.json` — deploys
  stay at ~130 files / ~5.7 MB instead of 9,800+ files.

### Negative

- Rebuilding the bundle produces a binary diff in `dist/library.js` on every
  edit to `Combobox.tsx` or the other components. This is intentional: the
  checked-in bundle is the deployment artefact.
- Any front-end contributor needs Node ≥ 20 and npm ≥ 10 locally. Node 24 /
  npm 11 verified; the project's README Tech-Stack matrix does not yet name
  Node as a dev prerequisite.
- The `postinstall` script (`scripts/install.js`) hard-codes one Taipy-GUI
  version assumption — it locates the installed `taipy-gui` via `pip show` and
  then `npm i --no-save <site-packages-path>/taipy/gui/webapp` to pull in the
  SDK type definitions. If Taipy-GUI ever changes that webapp layout, install
  breaks.

### Neutral

- `taipy-gui` is declared as a webpack `externals` alias (`'taipy-gui':
  'TaipyGui'`) so the SDK is resolved at runtime from Taipy's shared DLL, not
  bundled. MUI / React / emotion are similarly resolved via webpack's
  `DllReferencePlugin` against `taipy-gui-deps-manifest.json`.
- TypeScript strict mode + `target: es2020` is used for the extension. This
  differs from the repo's Python-wide `target-version = "py310"`; there is no
  shared config.

## CLAUDE.md Amendment

No repo-wide CLAUDE.md amendment. `hf_taipy_app/CLAUDE.md` "Server-driven
autocomplete" section was rewritten in the same commit to describe
`kind="combobox"` as the current mechanism and to note that the provisional
`searchable=True` flag was removed on 2026-04-18 once all eight migrations
landed.

## Related

- **Branch:** `ui/heat-map-context-and-filters`
- **Memory:** `project_path_c_taipy_combobox_extension.md` (Path C plan and
  APG research)
- **External references:**
  - W3C WAI-ARIA APG — combobox with list autocomplete:
    <https://www.w3.org/WAI/ARIA/apg/patterns/combobox/examples/combobox-autocomplete-list/>
  - Taipy extension reference template:
    <https://github.com/Avaiga/guiext-template>
  - Taipy GUI 4.1.1 `taipy.gui.extension` API:
    `.venv/Lib/site-packages/taipy/gui/extension/library.py`

## Notes

Layout at landing time:

```
hf_taipy_app/src/extensions/ll_ext/
  __init__.py                           # exports LlExtLibrary
  library.py                            # ElementLibrary subclass registering "combobox"
  front-end/
    .gitignore                          # node_modules, .env, *.map, package-lock.json
    package.json                        # webpack + ts-loader + typescript; react@18 dep
    tsconfig.json                       # target es2020, strict, jsx: react-jsx
    webpack.config.js                   # UMD bundle, DllReferencePlugin, externals: taipy-gui
    scripts/install.js                  # pip show taipy-gui → .env TAIPY_GUI_DIR; npm i --no-save <webapp>
    src/
      index.ts                          # export { Combobox }
      Combobox.tsx                      # React component (WAI-ARIA APG compliant)
    dist/
      library.js                        # checked-in UMD bundle (~200 KB minified)
      library.js.LICENSE.txt            # third-party attributions
```

`manage_space.py` IGNORE_PATTERNS additions (same commit):

```python
"**/node_modules",
"**/node_modules/**",
"**/dist/*.map",
"**/.env",
"**/.env.*",
"**/package-lock.json",
"**/.benchmarks",
"**/.benchmarks/**",
```
