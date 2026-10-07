# Single image for every data_engineering Cloud Run job. Each job runs one module:
#   gcloud run jobs deploy <name> --command python --args=-m,<package.module>
# (see .github/workflows/deploy-data-engineering.yaml)
FROM ghcr.io/astral-sh/uv:python3.11-bookworm-slim

WORKDIR /app

# Install the consolidated dependency set from the lockfile first (cached layer).
# No [build-system] in pyproject => uv installs deps only, not the project itself.
COPY pyproject.toml uv.lock ./
RUN uv sync --frozen --no-dev

# Application code (all packages live under src/).
COPY src/ ./src/

ENV PYTHONPATH=/app/src \
    PATH="/app/.venv/bin:$PATH"

# Jobs override --command/--args at deploy time; default just prints the version.
ENTRYPOINT ["python"]
CMD ["--version"]
