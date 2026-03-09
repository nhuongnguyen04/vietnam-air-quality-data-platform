# Use Apache Airflow 3.1.7 official image as base
FROM apache/airflow:3.1.7

# Set Airflow home
ENV AIRFLOW_HOME=/opt/airflow

# Install system dependencies
USER root
RUN apt-get update && apt-get install -y --no-install-recommends \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Create dbt directory as root
RUN mkdir -p /opt/dbt

USER airflow

# Copy and install Python dependencies from root directory
COPY --chown=airflow:airflow requirements.txt /opt/airflow/requirements.txt
RUN pip install --no-cache-dir -r /opt/airflow/requirements.txt

# Install additional Airflow providers for Airflow 3.x
RUN pip install --no-cache-dir \
    apache-airflow-providers-http \
    apache-airflow-providers-sqlite \
    apache-airflow-providers-postgres

# Create dbt directory and copy dbt project
COPY --chown=airflow:airflow dbt/dbt_tranform /opt/dbt/dbt_tranform

# Copy entrypoint script
COPY --chown=airflow:airflow airflow/config/entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh

# Set environment variables
ENV DBT_PROFILES_DIR=/opt/dbt/dbt_tranform
ENV PATH="/opt/airflow:${PATH}"

# Default command
ENTRYPOINT ["/entrypoint.sh"]
