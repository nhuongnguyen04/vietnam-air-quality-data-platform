FROM apache/airflow:3.1.7

USER root
RUN apt-get update && apt-get install -y --no-install-recommends \
    curl \
    && rm -rf /var/lib/apt/lists/*

COPY --chown=airflow:0 requirements.txt /opt/airflow/requirements.txt
COPY --chown=airflow:0 dbt/dbt_tranform /opt/dbt/dbt_tranform
COPY --chown=airflow:0 airflow/dags /opt/airflow/dags
COPY --chown=airflow:0 airflow/plugins /opt/airflow/plugins
COPY --chown=airflow:0 python_jobs /opt/python/jobs
COPY --chown=airflow:0 airflow/config/entrypoint.sh /entrypoint.sh

RUN chmod -R 775 /opt/dbt/dbt_tranform /opt/python/jobs && \
    chmod +x /entrypoint.sh

USER airflow
RUN pip install --no-cache-dir -r /opt/airflow/requirements.txt