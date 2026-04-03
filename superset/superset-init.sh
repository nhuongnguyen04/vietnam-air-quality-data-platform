#!/bin/bash
set -e

echo "[superset-init] Waiting for Superset to be ready..."
# Health check loop — chờ cho đến khi Superset thực sự sẵn sàng
for i in $(seq 1 20); do
  if curl --fail -s http://localhost:8088/health > /dev/null 2>&1; then
    echo "[superset-init] Superset is healthy."
    break
  fi
  echo "[superset-init] Waiting... ($i/20)"
  sleep 15
done

echo "[superset-init] Creating admin user..."
superset fab create-admin \
  --username "${SUPERSET_ADMIN_USER:-admin}" \
  --password "${SUPERSET_ADMIN_PASSWORD:-admin}" \
  --firstname Admin \
  --lastname User \
  --email admin@vietnam-aqi.local \
  || echo "[superset-init] Admin user already exists, skipping."

echo "[superset-init] Setting up roles..."
superset init \
  || echo "[superset-init] superset init already done, skipping."

echo "[superset-init] Importing dashboards..."
if [ -d /app/dashboards ] && [ "$(ls -A /app/dashboards 2>/dev/null)" ]; then
  for dashboard_file in /app/dashboards/*.zip; do
    if [ -f "$dashboard_file" ]; then
      echo "[superset-init] Importing $dashboard_file..."
      superset import-dashboards -f "$dashboard_file" --force || true
    fi
  done
else
  echo "[superset-init] No dashboard ZIP files found in /app/dashboards — skipping import."
fi

echo "[superset-init] Initialization complete."
