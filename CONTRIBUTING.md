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

## Outside Contributions

This project is **open-source and open to forking and issues**, but is not currently accepting outside pull requests. The codebase is under active solo development with strict architectural conventions, and PRs from external contributors would be difficult to review productively at this stage.

**What you can do:**

- **Fork** the repository and adapt it for your own use (Apache 2.0 license)
- **Open issues** for bugs, questions, or feature suggestions
- **Star** the repo if you find it useful

This policy will be revisited as the project matures.

## Questions?

Open a [GitHub Issue](https://github.com/karsten-s-nielsen/luxury-lakehouse/issues) or reach out via the project's [Hugging Face community](https://huggingface.co/luxury-lakehouse).
