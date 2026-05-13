FROM python:3.10-slim@sha256:87579103010e46d38cae5e8d6c979f2cdbc9ef753daf603813c74cc7e995f6a7

RUN apt-get update && apt-get install -y --no-install-recommends \
    curl \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
COPY dist/luxury_lakehouse-*.whl /tmp/
RUN pip install --no-cache-dir /tmp/luxury_lakehouse-*.whl && rm /tmp/*.whl
RUN pip install --no-cache-dir -r requirements.txt

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
