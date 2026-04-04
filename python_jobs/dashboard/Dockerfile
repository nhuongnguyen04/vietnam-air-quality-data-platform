FROM python:3.11-slim

WORKDIR /app

# Build context is repo root (.) — copy requirements + code from python_jobs/dashboard
COPY python_jobs/dashboard/requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

COPY python_jobs/dashboard/ ./

EXPOSE 8501

CMD ["streamlit", "run", "app.py", "--server.port=8501", "--server.address=0.0.0.0"]
