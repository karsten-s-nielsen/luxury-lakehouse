# Decision: PEP 723 Inline Script Metadata for HF Jobs Training Scripts

**Status:** Accepted
**Date:** 2026-04-02

## Context

ML training and inference scripts run on HF Jobs GPU containers (a10g-large: 46 GB, a10g-small: 15 GB). Each script has its own dependency set: a shared analytics wheel from HF Hub plus script-specific ML libraries (torch, jax, flax, socceraction, etc.). The platform needs a dependency management approach that works on HF Jobs without requiring per-script Docker image builds or a monolithic shared environment.

The key constraints are: HF Jobs uses `uv run` as its execution model, each script must declare its own deps without affecting others, and the luxury-lakehouse analytics wheel must be importable at runtime (it is not on PyPI — it is hosted at `luxury-lakehouse/build-artifacts` on HF Hub and uploaded by CI on main merges).

## Decision

Use PEP 723 inline script metadata (`# /// script`) in every HF Jobs training and inference script. The wheel URL for the luxury-lakehouse package is declared as the first dependency in each script's metadata block. Script-specific ML libraries follow. `hf jobs uv run` reads the metadata, creates an isolated environment, and runs the script — no Docker builds, no shared environment configuration.

## Alternatives Considered

| Option | Assessment |
|--------|------------|
| Dedicated Docker image per training job | Eliminates cold-start pip install, but adds Docker build/push ops for every dependency change; no HF Jobs native support for custom base images with private wheels |
| `requirements.txt` per script | No lockfile; dependency version drift across scripts; does not integrate with `uv run` metadata convention |
| Conda environments | Not supported on HF Jobs; `uv` is the only supported package manager |
| Single shared environment (all ML deps) | Image becomes multi-GB; conflicting dep versions between scripts (e.g., different JAX versions for different model architectures) |

## Consequences

**Positive:**
- Zero Docker ops — no Dockerfiles, no image builds, no registry pushes.
- Dependencies are declared in the script itself, making each script self-contained and auditable.
- `uv` resolves and pins the dependency graph at install time; cold-start is typically 30-90 seconds on HF Jobs.
- Script isolation: torch version for football2vec does not conflict with jax version for pitch control.

**Negative:**
- No hash pinning in PEP 723 metadata — dependency versions are specified as version constraints, not lockfile hashes. A malicious upstream package update could affect a script without detection.
- Every cold start includes a `pip install` phase (~30-90s). Warm restarts reuse the cached environment, but HF Jobs containers do not persist across job submissions.
- The luxury-lakehouse wheel URL is hardcoded in each script. Rotating the HF Hub repo name requires updating every script's metadata block.
