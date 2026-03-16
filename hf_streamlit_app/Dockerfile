FROM python:3.10-slim

# System deps for psycopg2-binary and matplotlib
RUN apt-get update && apt-get install -y --no-install-recommends \
    curl \
    && rm -rf /var/lib/apt/lists/*

RUN pip install --no-cache-dir \
    streamlit==1.45.1 \
    psycopg2-binary==2.9.10 \
    databricks-sdk==0.50.0 \
    requests>=2.31.0 \
    pandas>=2.0.0 \
    numpy>=1.24.0 \
    scipy>=1.11.0 \
    plotly>=5.15.0 \
    matplotlib>=3.7.0 \
    mplsoccer>=1.2.0 \
    pydantic>=2.0.0 \
    pydantic-settings>=2.0.0

WORKDIR /app

# Copy source code (Streamlit app + analytics modules)
COPY src/ src/
COPY .streamlit/ .streamlit/

ENV PYTHONPATH=/app/src

EXPOSE 7860

HEALTHCHECK CMD curl --fail http://localhost:7860/_stcore/health || exit 1

ENTRYPOINT ["streamlit", "run", "src/streamlit_app/app.py", \
    "--server.port=7860", \
    "--server.address=0.0.0.0", \
    "--server.headless=true", \
    "--browser.gatherUsageStats=false"]
