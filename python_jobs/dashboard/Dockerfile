FROM python:3.11-slim

WORKDIR /app

# Install curl for healthcheck
RUN apt-get update && apt-get install -y --no-install-recommends curl ca-certificates \
    && rm -rf /var/lib/apt/lists/*

# Build context is repo root (.).
COPY requirements /tmp/requirements
RUN pip install --no-cache-dir -r /tmp/requirements/dashboard.txt

COPY python_jobs/dashboard/ ./

EXPOSE 8501

CMD ["streamlit", "run", "app.py", "--server.port=8501", "--server.address=0.0.0.0"]
