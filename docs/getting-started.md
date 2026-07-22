# Getting Started

> **After completing this guide you will have:** a working local environment with passing tests, linting, and type checks — ready to explore or extend the platform.

## Prerequisites

> **Python 3.10 specifically — not newer.** Databricks serverless only supports 3.10, so the project pins to it to catch version-specific issues locally before they hit production. If you're on 3.12+, you'll need to install 3.10 alongside it — `uv` handles the rest.

- [Git](https://git-scm.com/)
- [Python 3.10](https://www.python.org/downloads/) (strict: >=3.10, <3.11)
- [uv](https://docs.astral.sh/uv/) (Python package manager)

## 1. Clone and Install

```bash
git clone https://github.com/karsten-s-nielsen/luxury-lakehouse.git
cd luxury-lakehouse
uv sync
```

**Verify:**

```bash
uv run python --version
# Expected: Python 3.10.x (NOT 3.11 or 3.12)
```

If you see a different Python version, ensure Python 3.10 is installed and set `uv python pin 3.10` in the repo root.

## 2. Verify the Environment

Run the same quality checks that CI enforces — if these pass locally, your PR will pass CI:

```bash
# Lint — catches unused imports, security anti-patterns, naming issues
uv run ruff check src/ scripts/

# Format — ensures consistent code style across all contributors
uv run ruff format --check src/ scripts/

# Type check — catches type mismatches before runtime
uv run pyright src/

# Unit tests — verifies correctness of analytics and pipeline logic
uv run pytest src/tests/ -x -q
```

**Verify:** All four commands exit with code 0. If `pyright` reports errors, run `uv sync` first — it installs the required type stubs.

## 3. Explore the Project

Now that your environment works, explore the codebase:

| Resource | What It Covers |
|----------|---------------|
| [README.md](../README.md) | Platform overview, architecture, data sources, analytics |
| [CLAUDE.md](../CLAUDE.md) | Engineering standards — **read this before contributing** |
| [ARCHITECTURE.md](../ARCHITECTURE.md) | Deep platform architecture with C4 diagrams |
| [Glossary](glossary.md) | Domain terminology (xG, VAEP, OBSO, etc.) |
| [C4 Diagrams](c4/architecture.html) | Interactive architecture diagrams (open in a browser) |

## 4. Databricks & dbt Setup (Optional)

> **Skip this section** if you only want to run tests and linting. Databricks access is needed for running dbt models and ingestion pipelines against real data.

### a. Configure environment variables

Copy the example file and fill in your Databricks credentials:

```bash
cp .env.example .env
# Edit .env with your values (see comments in the file for where to find them)
```

Load them into your shell:

```bash
# Option 1: manual (run in each terminal session)
export $(grep -v '^#' .env | xargs)

# Option 2: direnv (automatic, recommended)
cp .env .envrc && direnv allow
```

### b. Install dbt dependencies

```bash
uv sync --extra dbt          # Install dbt-core + dbt-databricks
cd dbt_project && uv run dbt deps    # Install dbt packages (dbt_utils, dbt_expectations)
```

### c. Run dbt

The SQL warehouse auto-stops after 10 minutes. Always use `ensure_warehouse.py` to start it first:

```bash
# Start warehouse + run dbt build
uv run python scripts/ensure_warehouse.py -- uv run dbt build --project-dir dbt_project --profiles-dir dbt_project

# Or just start the warehouse (no dbt command)
uv run python scripts/ensure_warehouse.py
```

**Verify:** `dbt build` completes with all models passing.

### d. Lakebase (Taipy app)

The Taipy dashboard connects to Lakebase (PostgreSQL) via OAuth M2M. Two setup steps are needed:

**Step 1: Create the Lakebase PG roles for the service principals** (one-time):

```bash
uv run python scripts/setup_lakebase_roles.py          # Create PG roles
uv run python scripts/setup_lakebase_roles.py --verify  # Confirm roles exist
```

This provisions **two** service-principal roles (see `DESIRED_SP_ROLES`):

- the **Taipy app SP** — a plain grantee (receives SELECT in Step 2);
- the **CI OIDC SP** (`terraform_ci`, repo var `DATABRICKS_CLIENT_ID`) — created as a member of `databricks_superuser` so the `lakebase-grants.yml` GitHub Action can run GRANT / `connect_as_superuser()` (it authenticates via GitHub OIDC, replacing the retired admin PAT — see [ADR-071](superpowers/adrs/ADR-071-ci-databricks-oidc-auth.md)). **Skipping the CI SP breaks `lakebase-grants.yml`** with `psycopg2 ... password authentication failed for user '<app-id>'`.

**Step 2: Grant SELECT access on synced tables** (after initial setup, or after synced table recreation):

```bash
uv run python scripts/run_lakebase_grants.py            # Apply grants
uv run python scripts/run_lakebase_grants.py --verify    # Confirm grants exist
```

Both scripts authenticate as a **workspace admin via OAuth** (PATs were retired 2026-07-21). Run `databricks auth login --profile OAUTH` once, then run the scripts under that profile (e.g. `DATABRICKS_CONFIG_PROFILE=OAUTH`, or export a bearer token from `Config(profile="OAUTH")` as `DATABRICKS_TOKEN`). The grants are applied to the service-principal UUIDs configured in the scripts.

> **When to re-run grants:** After creating new synced tables, or if the Taipy app connects to Lakebase but queries return empty results (the connection succeeds but the service principal has no SELECT permission).

**Step 3: Run locally:**

```bash
cd hf_taipy_app && python src/main.py
# Opens on http://localhost:7860
```

## 5. Next Steps

- **Interactive dashboard:** [Soccer Analytics Dashboard](https://huggingface.co/spaces/luxury-lakehouse/soccer-analytics-app)
- **Use pre-trained models:** See the [Hugging Face setup guide](huggingface-setup.md)
- **Understand the architecture:** Read [ARCHITECTURE.md](../ARCHITECTURE.md)
- **Contribute:** See [CONTRIBUTING.md](../CONTRIBUTING.md)

## 6. Common Issues

| Problem | Cause | Fix |
|---------|-------|-----|
| `uv: command not found` | uv not installed | Install via `curl -LsSf https://astral.sh/uv/install.sh \| sh` (Unix) or `powershell -ExecutionPolicy ByPass -c "irm https://astral.sh/uv/install.ps1 \| iex"` (Windows) |
| Wrong Python version (3.11+) | System Python is newer than 3.10 | Install Python 3.10 and run `uv python pin 3.10` in the repo root |
| `pytest` failures on Windows | Path separator differences | Ensure you are using Git Bash or WSL, not CMD/PowerShell directly |
| `pyright` reports many errors | Type stubs not installed | Run `uv sync` — this installs all dependencies including type stubs |
| Import errors in tests | Dependencies not installed | Run `uv sync` from the repo root (not a subdirectory) |
