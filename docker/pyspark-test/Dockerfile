# Reusable local PySpark test image for the tracking-marts executor refactor (and future Spark-dispatch
# tests). Headless (pytest, not Jupyter) and Python 3.10 to match the repo lock (serverless is 3.10; the
# jupyter/pyspark-notebook image is 3.11 -> pandas-3 risk with silly-kicks 4.123). pyspark 3.5 matches the
# Databricks serverless Spark major; the dispatch LOGIC (repartition / sortWithinPartitions / mapInPandas /
# parquet spill) is faithful under local[*]. Serverless-only aspects (UC Volume paths, Spark Connect) stay
# live-validated.
#
# Build (from repo root):
#   docker build -t ll-pyspark-test -f docker/pyspark-test/Dockerfile .
# Run the Spark-dispatch tests (mount the repo; nothing is written to the image):
#   docker run --rm -v "${PWD}:/work" ll-pyspark-test \
#     python -m pytest src/tests/tracking_marts/test_two_stage_dispatch.py -p no:cacheprovider -o addopts="" -q
FROM python:3.10-slim-bookworm

# OpenJDK 17 (headless) for Spark; procps for Spark's process probing. Spark 3.5 supports JDK 8/11/17 —
# NOT 21 (bookworm's `default-jre` is 21, on which Arrow's sun.misc.Unsafe/DirectByteBuffer access is
# blocked even with --add-opens). Pin 17 explicitly.
RUN apt-get update \
    && apt-get install -y --no-install-recommends openjdk-17-jre-headless procps \
    && apt-get clean && rm -rf /var/lib/apt/lists/*

# Pins chosen to match the repo: pandas 2.3.3 (uv.lock), silly-kicks 4.123.0 + the extras the lakehouse
# installs, kloppy (metrica/skillcorner converters), pyspark 3.5 (serverless Spark major). The lakehouse
# packages themselves are mounted at runtime (PYTHONPATH=/work/src) so the image need not rebuild on edits.
RUN pip install --no-cache-dir \
    "pyspark==3.5.1" \
    "pandas==2.3.3" \
    "numpy" \
    "pyarrow" \
    "kloppy" \
    "silly-kicks[ghost-gk,parse-dfl]==4.128.0" \
    "pytest"

# Runtime + test-harness deps the lakehouse import chain / root conftest need that are not sk/pyspark:
#   matplotlib — root conftest sets a headless backend
#   requests / requests-cache — ingestion.utils (imported transitively via ingestion.action_context)
# sk 4.128 DAS is NATIVE (no accessible-space runtime dep — that was the retired 4.123 `[das]` extra).
# Separate layer so the heavy pyspark/sk layer above stays cached when this list grows.
RUN pip install --no-cache-dir "matplotlib" "requests" "requests-cache"

ENV PYTHONPATH=/work/src
ENV PYARROW_IGNORE_TIMEZONE=1
# Arrow off-heap buffers need these JVM opens on JDK 17+ (bookworm's default-jre); without them
# mapInPandas/parquet Arrow serialization fails with "sun.misc.Unsafe / DirectByteBuffer not available".
# Databricks serverless (JDK 8/11) does not need them — this is a local-container concern only.
ENV JAVA_TOOL_OPTIONS="--add-opens=java.base/java.nio=ALL-UNNAMED --add-opens=java.base/java.lang=ALL-UNNAMED --add-opens=java.base/sun.nio.ch=ALL-UNNAMED -Dio.netty.tryReflectionSetAccessible=true"
WORKDIR /work
CMD ["python", "-m", "pytest", "src/tests/tracking_marts/test_two_stage_dispatch.py", "-p", "no:cacheprovider", "-o", "addopts=", "-q"]
