FROM python:3.11-slim

WORKDIR /app

RUN apt-get update && apt-get install -y --no-install-recommends curl ca-certificates \
    && rm -rf /var/lib/apt/lists/*

COPY python_jobs/text_to_sql/requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

COPY python_jobs/text_to_sql/ ./

EXPOSE 8000

CMD ["uvicorn", "app:app", "--host", "0.0.0.0", "--port", "8000"]
