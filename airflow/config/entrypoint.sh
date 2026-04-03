#!/bin/bash
set -e

# Initialize or migrate Airflow database automatically
# Skip if requested or if running as root (usually for permission setup)
if [ "${AIRFLOW_SKIP_DB_MIGRATE:-false}" != "true" ] && [ "$(id -u)" != "0" ]; then
    echo "Migrating Airflow database..."
    if ! airflow db migrate; then
        echo "Migration failed, trying fresh database init..."
        airflow db init
    fi
else
    echo "Skipping Airflow database migration (root user or skipped by config)."
fi

# Create necessary directories
mkdir -p /opt/airflow/logs
mkdir -p /opt/airflow/dags
mkdir -p /opt/airflow/plugins
mkdir -p /opt/airflow/logs/dag_processor
mkdir -p /opt/airflow/logs/dag_processor_manager
mkdir -p /opt/airflow/logs/scheduler
mkdir -p /opt/airflow/logs/triggerer

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
