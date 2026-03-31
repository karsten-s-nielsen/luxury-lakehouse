# Contributing to (Right! Luxury!) Lakehouse

Thank you for your interest in contributing!

## Engineering Standards

All contributions must follow the engineering standards documented in [CLAUDE.md](CLAUDE.md). Key requirements:

- **Python 3.10** (strict: >=3.10, <3.11 — Databricks serverless constraint)
- **Line length**: 120 characters maximum
- **Type annotations**: All public function signatures

## Development Setup

See the [Getting Started guide](docs/getting-started.md) for local environment setup.

## Required Checks

All of these must pass before submitting a PR:

```bash
uv run ruff check src/ scripts/           # Lint
uv run ruff format --check src/ scripts/  # Format
uv run pyright src/                       # Type check
uv run pytest src/tests/ -v               # Unit tests
```

## Pull Request Process

1. Fork the repository and create a feature branch
2. Make your changes, ensuring all checks pass
3. Write descriptive commit messages (see git history for style)
4. Open a PR with a clear title and description of what and why

## Questions?

Open a [GitHub Discussion](https://github.com/karsten-s-nielsen/luxury-lakehouse/discussions) or reach out via the project's [Hugging Face community](https://huggingface.co/luxury-lakehouse).
