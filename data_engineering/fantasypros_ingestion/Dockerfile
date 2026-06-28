FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

ENV PYTHONPATH=/app

# This is a one-time historical backfill normally run LOCALLY (Sleeper/FantasyPros rate-limited,
# not compute-bound). Image kept for convention / optional future deploy.
CMD ["python", "projections_scraper.py"]
