---
title: Jobs
emoji: 📉
colorFrom: indigo
colorTo: yellow
sdk: docker
pinned: false
---

# Lakehouse v2 — Job image

This Docker Space holds the v2 Job image only. The scheduled ingest job (`lakehouse-ingest`) and every one-off
`hf jobs run` use it as their image (`hf.co/spaces/karstenskyt/jobs`). The image's command is the scheduled run,
so the Space itself boots into an error (`RUNTIME_ERROR`): that is the expected state of an image-only Space, not a
failure.

Deploy it with the full stage described in `docs/ops/deploy.md` ("Job image and scheduled job"). Keep this
README in the stage: the upload mirrors the stage (`delete_patterns=["**"]`), and a stage without it removes the
Space's front matter, so HF reports `CONFIG_ERROR`.
