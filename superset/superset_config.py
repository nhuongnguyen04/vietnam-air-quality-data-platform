"""
Superset configuration for Vietnam Air Quality Data Platform.

This module configures Superset 4.x to connect to ClickHouse and enables
public read-only access via guest tokens (no login required).

Author: Air Quality Data Platform
"""

import os
from datetime import timedelta
from superset.config import *

# --- ClickHouse Connection ---
CLICKHOUSE_USER = os.environ.get("CLICKHOUSE_USER", "admin")
CLICKHOUSE_PASSWORD = os.environ.get("CLICKHOUSE_PASSWORD", "admin123456")
CLICKHOUSE_HOST = os.environ.get("CLICKHOUSE_HOST", "clickhouse")
CLICKHOUSE_PORT = os.environ.get("CLICKHOUSE_PORT", "8123")
CLICKHOUSE_DB = os.environ.get("CLICKHOUSE_DB", "air_quality")

# --- Authentication (SQLite metadata DB — Flask-AppBuilder) ---
# Flask-AppBuilder yêu cầu SQLite/PostgreSQL, KHÔNG hỗ trợ ClickHouse.
# Dùng SQLite tại /app/superset_home/superset.db (mounted volume).
# AUTH_DB = Flask session-based auth (mặc định của Superset).
SUPERSET_HOME = os.environ.get("SUPERSET_HOME", "/app/superset_home")
SQLALCHEMY_DATABASE_URI = f"sqlite:///{SUPERSET_HOME}/superset.db"
AUTH_TYPE = AUTH_DB  # noqa: F821 — imported by superset.config import *

# --- Cookie Security ---
# Gán lại sau superset.config import — tránh bị override bởi base image defaults
SESSION_COOKIE_SECURE = False   # HTTP deployment

# --- ClickHouse Connection (chỉ cho analytical queries, không phải metadata) ---
# Đặt dưới dạng custom engine URL để Superset có thể tạo database connection khi người dùng add ClickHouse
CLICKHOUSE_URI = (
    f"clickhouse+http://{CLICKHOUSE_USER}:{CLICKHOUSE_PASSWORD}"
    f"@{CLICKHOUSE_HOST}:{CLICKHOUSE_PORT}/{CLICKHOUSE_DB}"
)

# --- Metadata Caching (Performance Improvement) ---
# Tăng tốc độ load schema, danh sách table từ ClickHouse
CACHE_CONFIG = {
    'CACHE_TYPE': 'RedisCache',
    'CACHE_DEFAULT_TIMEOUT': 86400, # 24 giờ
    'CACHE_KEY_PREFIX': 'superset_metadata_',
    'CACHE_REDIS_URL': os.environ.get("REDIS_URL", "redis://redis:6379/0"),
}
DATA_CACHE_CONFIG = CACHE_CONFIG # Dùng cho kết quả query
FILTER_STATE_CACHE_CONFIG = CACHE_CONFIG # Dùng cho dashboard filters
EXPLORE_FORM_DATA_CACHE_CONFIG = CACHE_CONFIG

# --- Logging Configuration ---
LOG_LEVEL = 'INFO'
LOG_FORMAT = '%(asctime)s:%(levelname)s:%(name)s:%(message)s'

# --- Session Security Settings ---
SESSION_COOKIE_HTTPONLY = True  # Ngăn chặn XSS truy cập cookie
SESSION_COOKIE_SECURE = False   # False vì Superset chạy qua HTTP (không phải HTTPS trong môi trường dev)
SESSION_COOKIE_SAMESITE = 'Lax'
PERMANENT_SESSION_LIFETIME = timedelta(days=1)

# --- Guest/Public Access ---
PUBLIC_ROLE_LIKE_GAMMA = True
GUEST_TOKEN_JWT_SECRET = os.environ.get("SUPERSET_GUEST_TOKEN_SECRET")
SECRET_KEY = os.environ.get("SUPERSET_SECRET_KEY")
ALLOW_GUEST_DASHBOARD_ACCESS = True

# --- Row Limits (Query Optimization) ---
# Giới hạn số dòng trả về để tránh treo trình duyệt và quá tải ClickHouse
ROW_LIMIT = 500000
SQL_MAX_ROW = 100000
DEFAULT_VIZ_RELATIVE_START = "-1 day"

# --- Feature Flags ---
FEATURE_FLAGS = {
    "DASHBOARD_NATIVE_FILTERS": True,
    "ALERT_REPORTS": True,
    "ENABLE_TEMPLATE_PROCESSING": True,
    "SQL_LAB_BACKEND_PERSISTENCE": True,
}

# --- Rate Limiting API ---
# Disabled cho internal deployment bằng cách set True/False tùy môi trường
RATELIMIT_ENABLED = os.environ.get("RATELIMIT_ENABLED", "False").lower() == "true"
AUTH_RATE_LIMIT = "10 per minute"

# --- Other Security & Landing Page ---
WTF_CSRF_ENABLED = False # Disable CSRF cho internal read-only deployment
PUBLIC_USER_LANDING_PAGE = "/sqllab/"