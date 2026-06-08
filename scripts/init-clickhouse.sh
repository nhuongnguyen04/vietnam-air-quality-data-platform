#!/bin/bash
set -e

echo "Starting ClickHouse initialization script..."

# Check if environment variables are set
if [ -z "$CLICKHOUSE_DB" ] || [ -z "$CLICKHOUSE_OM_READER_PASSWORD" ] || [ -z "$TEXT_TO_SQL_CLICKHOUSE_PASSWORD" ]; then
    echo "ERROR: Required environment variables are not set!"
    exit 1
fi

echo "Substituting environment variables in template and executing..."
sed -e "s/\${CLICKHOUSE_DB}/${CLICKHOUSE_DB}/g" \
    -e "s/\${CLICKHOUSE_OM_READER_PASSWORD}/${CLICKHOUSE_OM_READER_PASSWORD}/g" \
    -e "s/\${TEXT_TO_SQL_CLICKHOUSE_PASSWORD}/${TEXT_TO_SQL_CLICKHOUSE_PASSWORD}/g" \
    /docker-entrypoint-initdb.d/init-clickhouse.sql.template | clickhouse-client -u "${CLICKHOUSE_USER:-admin}" --password "${CLICKHOUSE_PASSWORD:-admin123456}"

echo "ClickHouse initialization complete."
