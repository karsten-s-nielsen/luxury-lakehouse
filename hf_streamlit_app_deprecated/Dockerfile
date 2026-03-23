FROM python:3.10-slim@sha256:87579103010e46d38cae5e8d6c979f2cdbc9ef753daf603813c74cc7e995f6a7

# System deps for psycopg2-binary and matplotlib
RUN apt-get update && apt-get install -y --no-install-recommends \
    curl \
    && rm -rf /var/lib/apt/lists/*

RUN pip install --no-cache-dir \
    streamlit==1.45.1 \
    psycopg2-binary==2.9.10 \
    databricks-sdk==0.50.0 \
    requests==2.32.5 \
    pandas==2.2.3 \
    numpy==1.26.4 \
    scipy==1.15.3 \
    plotly==6.6.0 \
    matplotlib==3.10.8 \
    mplsoccer==1.6.1 \
    pydantic==2.12.5 \
    pydantic-settings==2.13.1

# Non-root user (SEC-AUDIT-200 F4)
RUN useradd -m -u 1000 appuser

WORKDIR /app

# Copy source code (Streamlit app + analytics modules)
COPY --chown=appuser:appuser src/ src/
COPY --chown=appuser:appuser .streamlit/ .streamlit/

ENV PYTHONPATH=/app/src

USER appuser

EXPOSE 7860

HEALTHCHECK CMD curl --fail http://localhost:7860/_stcore/health || exit 1

ENTRYPOINT ["streamlit", "run", "src/streamlit_app/app.py", \
    "--server.port=7860", \
    "--server.address=0.0.0.0", \
    "--server.headless=true", \
    "--browser.gatherUsageStats=false"]
