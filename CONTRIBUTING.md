# Contributing to (Right! Luxury!) Lakehouse

Thank you for your interest in contributing! The engineering standards are strict — four checks must pass before merge — but the tradeoff is zero-regression confidence. Once you're set up, the feedback loop is fast.

## Engineering Standards

All contributions follow the standards in [CLAUDE.md](CLAUDE.md). The key constraints:

- **Python 3.10** (strict: >=3.10, <3.11 — Databricks serverless constraint)
- **Line length**: 120 characters maximum
- **Type annotations**: All public function signatures

## Development Setup

See the [Getting Started guide](docs/getting-started.md) for local environment setup.

## Required Checks

These are the same gates CI runs — if they pass locally, your PR will pass CI:

```bash
uv run ruff check src/ scripts/           # Lint
uv run ruff format --check src/ scripts/  # Format
uv run pyright src/                       # Type check
uv run pytest src/tests/ -v               # Unit tests
```

If a check fails, the command output will tell you exactly what to fix. `ruff check --fix` can auto-fix most lint issues.

## Pull Request Process

1. Fork the repository and create a feature branch
2. Make your changes, ensuring all checks pass
3. Write descriptive commit messages (see git history for style)
4. Open a PR with a clear title and description of what and why

## Questions?

Open a [GitHub Discussion](https://github.com/karsten-s-nielsen/luxury-lakehouse/discussions) or reach out via the project's [Hugging Face community](https://huggingface.co/luxury-lakehouse).
