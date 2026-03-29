# Getting Started

> **After completing this guide you will have:** a working local environment with passing tests, linting, and type checks — ready to explore or extend the platform.

## Prerequisites

- [Git](https://git-scm.com/)
- [Python 3.10](https://www.python.org/downloads/) (strict: >=3.10, <3.11 — Databricks serverless constraint means 3.11+ will cause failures)
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

Run the quality checks that CI enforces:

```bash
# Lint (should report 0 violations)
uv run ruff check src/

# Format check (should report 0 reformatted)
uv run ruff format --check src/

# Type check (should report 0 errors)
uv run pyright src/

# Unit tests (should all pass)
uv run pytest src/tests/ -x -q
```

**Verify:** All four commands exit with code 0 and no failures. If `pyright` reports errors, ensure you ran `uv sync` first — it installs all type stubs.

## 3. Explore the Project

Now that your environment works, explore the codebase:

| Resource | What It Covers |
|----------|---------------|
| [README.md](../README.md) | Platform overview, architecture, data sources, analytics |
| [CLAUDE.md](../CLAUDE.md) | Engineering standards — **read this before contributing** |
| [ARCHITECTURE.md](../ARCHITECTURE.md) | Deep platform architecture with C4 diagrams |
| [Glossary](glossary.md) | Domain terminology (xG, VAEP, OBSO, etc.) |
| [C4 Diagrams](c4/architecture.html) | Interactive architecture diagrams (open in a browser) |

## 4. Next Steps

- **Try the live demo:** [Soccer Analytics Dashboard](https://huggingface.co/spaces/luxury-lakehouse/soccer-analytics-app)
- **Use pre-trained models:** See the [Hugging Face setup guide](huggingface-setup.md)
- **Understand the architecture:** Read [ARCHITECTURE.md](../ARCHITECTURE.md)
- **Contribute:** See [CONTRIBUTING.md](../CONTRIBUTING.md)

## Common Issues

| Problem | Cause | Fix |
|---------|-------|-----|
| `uv: command not found` | uv not installed | Install via `curl -LsSf https://astral.sh/uv/install.sh \| sh` (Unix) or `powershell -ExecutionPolicy ByPass -c "irm https://astral.sh/uv/install.ps1 \| iex"` (Windows) |
| Wrong Python version (3.11+) | System Python is newer than 3.10 | Install Python 3.10 and run `uv python pin 3.10` in the repo root |
| `pytest` failures on Windows | Path separator differences | Ensure you are using Git Bash or WSL, not CMD/PowerShell directly |
| `pyright` reports many errors | Type stubs not installed | Run `uv sync` — this installs all dependencies including type stubs |
| Import errors in tests | Dependencies not installed | Run `uv sync` from the repo root (not a subdirectory) |
