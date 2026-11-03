# The v2 Ops Space (spec §19, §22.2; plan Task 29). A public Docker Space: HF OAuth sign-in with an allow-list, the
# Taipy Ops pages on the Flask app. It never holds the gateway key, so it cannot write catalog-control objects
# (§29.9); it reads and triggers the owner-created HF scheduled job only. HF builds this image.
#
# python:3.12-slim pinned by digest (3.12-slim, 2026-10-27); Databricks serverless needs 3.10 but the Space does not.
FROM python:3.12-slim@sha256:12316debbfb86890f6b2d825820612741ba527ad5a44e3902759141d572bfe8f

# uv for the locked install, pinned to the version the repo uses.
RUN pip install --no-cache-dir uv==0.9.28

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    UV_NO_CACHE=1 \
    HF_HOME=/home/app/.cache/huggingface \
    APP_PORT=7860

# A non-root user owns the app and its cache (HF Spaces run as this user).
RUN useradd --create-home --uid 1000 app
WORKDIR /home/app/app

# Install the exact locked set with no resolver pass (--no-deps), as v1 does; the project installs without its deps.
COPY pyproject.toml uv.lock ./
RUN uv export --frozen --no-dev --no-emit-project -o /tmp/requirements.txt \
    && uv pip install --system --no-deps -r /tmp/requirements.txt \
    && rm /tmp/requirements.txt

# The source, the dbt project and the UI styles (the test tree is not needed in the Space).
COPY src ./src
COPY dbt ./dbt
RUN uv pip install --system --no-deps . \
    && chown -R app:app /home/app

USER app
EXPOSE 7860
# The Space runs the web app. Roots and the allow-list come from the Space variables; secrets from the Space secrets.
CMD ["python", "-m", "lakehouse.web"]
