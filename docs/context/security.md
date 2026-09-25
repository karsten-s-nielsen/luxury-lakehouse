# Context — Security Hardening

On-demand detail for the Security Hardening rules in `AGENTS.md`. Governing ADR for the evolve exec scope: [ADR-001](../superpowers/adrs/ADR-001-evolve-code-execution.md).

## Security Hardening

- **No secrets in code**: All authentication via Databricks runtime or environment variables. Never commit credentials, tokens, or connection strings.
- **HTTPS only**: All HTTP requests must use `https://`. Reject `http://` at the function level.
- **SSL verification**: Explicit `verify=True` on all `requests` calls. Never disable certificate verification.
- **Input validation**: Regex-validate all user-supplied identifiers (catalog, schema names) to prevent SQL injection. Pattern: `^[a-zA-Z_][a-zA-Z0-9_]*$`
- **Timeouts**: Every HTTP call must have explicit `(connect, read)` timeouts. Default: `(10, 30)`.
- **Retry with backoff**: Exponential backoff on transient errors (429, 5xx). Max 3 retries.
- **No dangerous builtins**: No `eval()`, `exec()`, `pickle.loads()`, or `subprocess.call(shell=True)`.
- **Scoped exception — `src/evolve/`**: `exec()` is permitted in `src/evolve/targets/*/evaluator.py` and `src/evolve/remote_worker.py` under the defense-in-depth policy documented in [ADR-001](../superpowers/adrs/ADR-001-evolve-code-execution.md): AST allowlist (parse-time) + restricted globals with `__builtins__: {}` (runtime) + subprocess isolation (backends). Gated by `code_evolution=True`. All other code must continue to avoid `exec()`/`eval()`.
- **Content validation**: Verify DataFrame schema and non-empty data before every Delta write. `write_delta_table`/`merge_delta_table` auto-drop top-level `void` (NullType) columns with a loud log — an all-NULL inferred column schema-evolves into an unscannable Delta void column that bricks `SELECT *` on the whole table (2026-06-10 `gradientsports_events` incident; guard + tests in `ingestion/utils.py::_strip_void_columns`).
- **Least privilege**: Scripts write only to the specified `{catalog}.{schema}.*` — never to arbitrary paths.
