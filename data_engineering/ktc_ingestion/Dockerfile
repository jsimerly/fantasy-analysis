FROM python:3.10-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r -requirements.txt

COPY . .

# This is overridden by --args on deploy, so we can use this for multple jobs
CMD ["python", "incremental_dynasty.py"]
