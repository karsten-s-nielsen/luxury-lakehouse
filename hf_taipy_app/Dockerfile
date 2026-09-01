FROM python:3.10-slim@sha256:87579103010e46d38cae5e8d6c979f2cdbc9ef753daf603813c74cc7e995f6a7

RUN apt-get update && apt-get install -y --no-install-recommends \
    curl \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
COPY dist/luxury_lakehouse-*.whl /tmp/
RUN pip install --no-cache-dir /tmp/luxury_lakehouse-*.whl && rm /tmp/*.whl
# requirements.txt is a complete uv-resolved lockfile (uv pip compile --extra
# taipy-app), so install it with --no-deps: pip installs the pinned versions
# as-is instead of re-resolving. Required because [tool.uv] override-dependencies
# lifts taipy-gui 4.1.2's `flask-socketio<5.5` cap to >=5.5.1 (the Flask-3.1.3
# `_session` compatibility fix); a plain `pip install -r` re-derives that cap from
# metadata and fails with ResolutionImpossible. Matches how audit_resolutions.py
# already audits this file (--no-deps): "the file is a complete locked resolution".
# See ADR-083.
RUN pip install --no-cache-dir --no-deps -r requirements.txt

RUN useradd -m -u 1000 appuser

WORKDIR /app
COPY --chown=appuser:appuser src/ src/
COPY --chown=appuser:appuser workflow-cards/ workflow-cards/
COPY --chown=appuser:appuser static/ static/

ENV PYTHONPATH=/app/src

USER appuser

EXPOSE 7860

HEALTHCHECK CMD curl --fail http://localhost:7860/health || exit 1

STOPSIGNAL SIGTERM

CMD ["python", "src/main.py"]
