#!/bin/bash
set -e

# Initialize or migrate Airflow database automatically
echo "Migrating Airflow database..."
if ! airflow db migrate; then
    echo "Migration failed, trying fresh database init..."
    airflow db init
fi

# Create necessary directories
# Use AIRFLOW_UID/AIRFLOW_GID from env (set by docker-compose or host uid/gid)
AIRFLOW_UID=${AIRFLOW_UID:-50000}
AIRFLOW_GID=${AIRFLOW_GID:-0}

mkdir -p /opt/airflow/logs
mkdir -p /opt/airflow/dags
mkdir -p /opt/airflow/plugins
mkdir -p /opt/airflow/logs/dag_processor
mkdir -p /opt/airflow/logs/dag_processor_manager
mkdir -p /opt/airflow/logs/scheduler
mkdir -p /opt/airflow/logs/triggerer

# Set ownership on mounted volumes so local user (host uid) can also write
if [ -n "$AIRFLOW_UID" ] && [ "$AIRFLOW_UID" != "$(id -u airflow 2>/dev/null)" ]; then
    echo "Adjusting ownership of /opt/dbt to UID $AIRFLOW_UID..."
    mkdir -p /opt/dbt/dbt_tranform/logs /opt/dbt/dbt_tranform/target /opt/dbt/dbt_tranform/dbt_packages
    chown -R ${AIRFLOW_UID}:${AIRFLOW_GID} /opt/dbt
fi

# Handle Airflow 3.x command changes
# airflow webserver -> airflow api-server
# airflow scheduler remains the same
# Default to api-server when no command is provided.
if [ $# -eq 0 ]; then
    set -- api-server
fi

case "$1" in
    webserver)
        echo "Airflow setup complete. Starting api-server..."
        exec airflow api-server
        ;;
    *)
        echo "Airflow setup complete. Starting ${1:-api-server}..."
        exec airflow "$@"
        ;;
esac
