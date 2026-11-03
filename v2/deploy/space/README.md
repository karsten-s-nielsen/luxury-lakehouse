---
title: Luxury Lakehouse Ops
emoji: 🏞️
colorFrom: yellow
colorTo: gray
sdk: docker
app_port: 7860
hf_oauth: true
pinned: false
---

# Luxury Lakehouse — Ops

The operations UI for the v2 lakehouse: the data sources, what to download, the runs, the review queue, the schedule
and the settings. It is a public Space; only operators on the allow-list can sign in.

## Sign-in

Sign-in uses Hugging Face OAuth (`hf_oauth: true`). Only the Hugging Face accounts named in the `OPS_ALLOWED_USERS`
Space variable may use the Ops pages. Every page sits behind the sign-in guard.

## What it can and cannot do

This Space never holds the S3 gateway key, so it cannot write catalog-control objects (restore, relocate or a large
sweep). It reads the lake and triggers the owner-created HF scheduled job; the job does the writing (spec §29.9).

## Configuration

The authoritative list — every name, required/optional, exact token scope, and the `check_hf_token.py` verify command
— is `docs/ops/deploy.md`, section "Space configuration (the authoritative list)". The tables below are a quick
reference; if they ever differ, deploy.md wins.

Set these as **Space variables** (not secrets):

| Variable | Meaning |
| --- | --- |
| `LAKEHOUSE_LAKE_ROOT` | the lake bucket, `hf://buckets/<ns>/<name>` |
| `LAKEHOUSE_RESTRICTED_ROOT` | the restricted bucket |
| `OPS_ALLOWED_USERS` | the Hugging Face user names allowed to sign in |
| `LAKEHOUSE_HF_SCHEDULED_JOB_ID` | the owner-created scheduled job the Ops app triggers |
| `OAUTH_CLIENT_ID`, `OPENID_PROVIDER_URL`, `OAUTH_SCOPES` | the OAuth client (HF sets these when `hf_oauth` is on) |

Set these as **Space secrets**:

| Secret | Meaning |
| --- | --- |
| `OAUTH_CLIENT_SECRET` | the OAuth client secret (HF sets it) |
| `HF_TOKEN` | the **data** token: a **fine-grained** token with **Write on the lake bucket only** (bucket-level Write, for the Space's config writes through `HfApi`; with no gateway key it cannot write data or catalog), granted on the bucket itself (not the user namespace), and nothing on `restricted` or the `-control` ledger repo. Every page and bucket read uses it. Scopes: `docs/ops/deploy.md`, "HF credentials and token scopes" |
| `LAKEHOUSE_HF_JOBS_TOKEN` | the **job-control** token for the Settings schedule (status, reschedule, suspend, resume, trigger). A scheduled job under a **personal** namespace needs a classic **`write`-role** token here — HF's fine-grained Jobs permission is org-only. It is read only by the schedule seam, never the data path (`test_jobs_token_confinement.py`). Omit it when the job is under an **org** (then a fine-grained org-Jobs token is the single `HF_TOKEN`, and the schedule falls back to it). |
| `LAKEHOUSE_SESSION_SECRET` | the session signing key |
| `PINING_LIST_TOKEN` | the list-only pining token, so the app lists private pining tournaments; without it only public tournaments are listed (optional) |

The image installs the uv-locked dependency set with `--no-deps` and runs as a non-root user. It is built by Hugging
Face; nothing is pushed to an external registry.
