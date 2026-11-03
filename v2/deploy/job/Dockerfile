# The v2 Job image (spec §17, §22.2, §29.9; plan Task 29). HF Jobs is the only production writer. The owner creates
# the scheduled job once, with the gateway key and the pining owner token as Job secrets; the job runs this image.
# It holds the whole test tree too, so the owner-launched hf_job and hf_space checks run inside it (Task 33).
#
# python:3.12-slim pinned by digest (3.12-slim, 2026-10-27).
FROM python:3.12-slim@sha256:12316debbfb86890f6b2d825820612741ba527ad5a44e3902759141d572bfe8f

RUN pip install --no-cache-dir uv==0.9.28

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    UV_NO_CACHE=1 \
    HF_HOME=/home/app/.cache/huggingface

RUN useradd --create-home --uid 1000 app
WORKDIR /home/app/app

# The exact locked set, dev group included (the Job runs pytest for the hf markers), installed with --no-deps.
COPY pyproject.toml uv.lock ./
RUN uv export --frozen --no-emit-project -o /tmp/requirements.txt \
    && uv pip install --system --no-deps -r /tmp/requirements.txt \
    && rm /tmp/requirements.txt

# The source, the dbt project and the tests (the hf_job and hf_space markers run here). `scripts/` rides along because
# the unit suite loads `scripts/pip_audit.py` at collection time; without it pytest collection fails in the image and
# interrupts the whole hf run (Task 33).
COPY src ./src
COPY dbt ./dbt
COPY tests ./tests
COPY scripts ./scripts
COPY pyproject.toml ./
RUN uv pip install --system --no-deps . \
    && chown -R app:app /home/app

USER app
# The scheduled run, under the secret-masking wrapper; the roots and S3 paths come from the Job variables, the
# gateway key and the pining owner token from the Job secrets at run time.
CMD ["python", "-m", "lakehouse.mask", "--", "lakehouse", "run", "--trigger", "schedule"]
