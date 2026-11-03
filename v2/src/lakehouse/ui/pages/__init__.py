"""The eight Ops pages (spec §19; plan Task 28). Each module exposes `TITLE`, `ICON`, `ROUTE`, a declarative
`CONFIG` (`PageConfig`), a `refresh(state, services)` that fills the bound display variables, and a module-level
`page = Markdown(build_page(CONFIG))`."""
