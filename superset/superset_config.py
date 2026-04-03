"""
Superset configuration for Vietnam Air Quality Data Platform.

This module configures Superset 4.x to connect to ClickHouse and enables
public read-only access via guest tokens (no login required).

Author: Air Quality Data Platform
"""

import os

from superset.config import *

# ClickHouse connection via SQLAlchemy URI (clickhouse+connect driver)
CLICKHOUSE_USER = os.environ.get("CLICKHOUSE_USER", "admin")
CLICKHOUSE_PASSWORD = os.environ.get("CLICKHOUSE_PASSWORD", "admin123456")
CLICKHOUSE_HOST = os.environ.get("CLICKHOUSE_HOST", "clickhouse")
CLICKHOUSE_PORT = os.environ.get("CLICKHOUSE_PORT", "8123")
CLICKHOUSE_DB = os.environ.get("CLICKHOUSE_DB", "air_quality")

CLICKHOUSE_URI = (
    f"clickhouse+connect://{CLICKHOUSE_USER}:{CLICKHOUSE_PASSWORD}"
    f"@{CLICKHOUSE_HOST}:{CLICKHOUSE_PORT}/{CLICKHOUSE_DB}"
)

SQLALCHEMY_DATABASE_URI = CLICKHOUSE_URI
SQLALCHEMY_EXTRAS = {"connect_args": {"connect_timeout": 30}}

# Guest/public read-only access (no login required)
# Bắt buộc cho Superset 4.x: cả hai flag cần được set
PUBLIC_ROLE_LIKE_GAMMA = True
GUEST_TOKEN_JWT_SECRET = os.environ.get("SUPERSET_GUEST_TOKEN_SECRET", "change-me-in-prod")
ALLOW_GUEST_DASHBOARD_ACCESS = True

# Cache: 15 min TTL — phù hợp với dag_transform mỗi 45 min
CACHE_TIMEOUT = 900

# Feature flags for Superset 4.x
FEATURE_FLAGS = {
    "DASHBOARD_NATIVE_FILTERS": True,
    "ALERT_REPORTS": True,
    "ENABLE_TEMPLATE_PROCESSING": True,
    "SQL_LAB_BACKEND_PERSISTENCE": True,
}

# Security: disable CSRF for internal read-only deployment
WTF_CSRF_ENABLED = False

# Landing page: SQL Lab for public users
PUBLIC_USER_LANDING_PAGE = "/sqllab/"
