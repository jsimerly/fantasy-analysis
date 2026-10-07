# Image for the machine_learning Cloud Run job(s). Mirrors data_engineering/Dockerfile: uv-managed
# environment from the lockfile, code copied in, one script per job via --command/--args.
#   gcloud run jobs deploy ml-weekly-refresh --command python --args=scripts/weekly_refresh.py
# (see .github/workflows/deploy-machine-learning.yaml). CPU only: the weekly refresh trains the
# in-season and career models in minutes; GPU training (device=cuda) stays local / Vertex.
FROM ghcr.io/astral-sh/uv:python3.11-bookworm-slim

WORKDIR /app

COPY pyproject.toml uv.lock ./
RUN uv sync --frozen --no-dev

COPY src/ ./src/
COPY scripts/ ./scripts/
COPY leagues/ ./leagues/

ENV PYTHONPATH=/app/src:/app/scripts \
    PATH="/app/.venv/bin:$PATH" \
    PYTHONIOENCODING=utf-8

ENTRYPOINT ["python"]
CMD ["scripts/weekly_refresh.py", "--dry-run"]
