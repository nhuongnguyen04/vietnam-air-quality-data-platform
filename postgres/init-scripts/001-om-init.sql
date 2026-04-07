-- postgres/init-scripts/001-om-init.sql
-- Phase 4: OpenMetadata PostgreSQL metadata store
-- Chạy tự động khi postgres container khởi động lần đầu
-- Dùng chung với Airflow metadata (database: airflow)

-- Tạo database cho OM
CREATE DATABASE openmetadata_db;

-- Tạo user cho OM
DO $$
BEGIN
    IF NOT EXISTS (SELECT FROM pg_catalog.pg_roles WHERE rolname = 'openmetadata_user') THEN
        CREATE USER openmetadata_user WITH PASSWORD 'openmetadata_password';
    END IF;
END
$$;

-- Grant privileges
GRANT ALL PRIVILEGES ON DATABASE openmetadata_db TO openmetadata_user;
ALTER DATABASE openmetadata_db OWNER TO openmetadata_user;

-- Grant schema permissions (after database is created)
\c openmetadata_db
GRANT ALL ON SCHEMA public TO openmetadata_user;
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA public TO openmetadata_user;
GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA public TO openmetadata_user;
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT ALL ON TABLES TO openmetadata_user;
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT ALL ON SEQUENCES TO openmetadata_user;
