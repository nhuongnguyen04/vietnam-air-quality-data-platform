# Use Apache Airflow 3.1.7 official image as base
FROM apache/airflow:3.1.7

# Set Airflow home
ENV AIRFLOW_HOME=/opt/airflow

# Install system dependencies
USER root
RUN apt-get update && apt-get install -y --no-install-recommends \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Create dbt and python jobs directories
USER root
RUN mkdir -p /opt/dbt /opt/python/jobs && chown -R airflow:0 /opt/dbt /opt/python/jobs
USER airflow

# Copy and install Python dependencies from root directory
COPY --chown=airflow:0 requirements.txt /opt/airflow/requirements.txt
RUN pip install --no-cache-dir -r /opt/airflow/requirements.txt

# Install additional Airflow providers for Airflow 3.x
RUN pip install --no-cache-dir \
    apache-airflow-providers-http \
    apache-airflow-providers-sqlite \
    apache-airflow-providers-postgres

# Copy dbt project
COPY --chown=airflow:0 dbt/dbt_tranform /opt/dbt/dbt_tranform

# Copy Airflow DAGs and Plugins
COPY --chown=airflow:0 airflow/dags /opt/airflow/dags
COPY --chown=airflow:0 airflow/plugins /opt/airflow/plugins

# Copy Python Jobs (referenced by DAGs)
COPY --chown=airflow:0 python_jobs /opt/python/jobs

# Copy entrypoint script
COPY --chown=airflow:0 airflow/config/entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh

# Set environment variables
ENV DBT_PROFILES_DIR=/opt/dbt/dbt_tranform
ENV PATH="/opt/airflow:${PATH}"

# Default command
ENTRYPOINT ["/entrypoint.sh"]
