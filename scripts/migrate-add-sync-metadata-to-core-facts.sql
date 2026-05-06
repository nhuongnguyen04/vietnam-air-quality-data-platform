ALTER TABLE air_quality.fct_air_quality_ward_level_hourly
    ADD COLUMN IF NOT EXISTS raw_loaded_at DateTime DEFAULT last_ingested_at,
    ADD COLUMN IF NOT EXISTS raw_sync_run_id String DEFAULT '',
    ADD COLUMN IF NOT EXISTS raw_sync_started_at DateTime DEFAULT raw_loaded_at;

ALTER TABLE air_quality.fct_air_quality_province_level_hourly
    ADD COLUMN IF NOT EXISTS raw_loaded_at DateTime DEFAULT last_ingested_at,
    ADD COLUMN IF NOT EXISTS raw_sync_run_id String DEFAULT '',
    ADD COLUMN IF NOT EXISTS raw_sync_started_at DateTime DEFAULT raw_loaded_at;

ALTER TABLE air_quality.fct_air_quality_ward_level_daily
    ADD COLUMN IF NOT EXISTS raw_loaded_at DateTime DEFAULT last_ingested_at,
    ADD COLUMN IF NOT EXISTS raw_sync_run_id String DEFAULT '',
    ADD COLUMN IF NOT EXISTS raw_sync_started_at DateTime DEFAULT raw_loaded_at;

ALTER TABLE air_quality.fct_air_quality_province_level_daily
    ADD COLUMN IF NOT EXISTS raw_loaded_at DateTime DEFAULT last_ingested_at,
    ADD COLUMN IF NOT EXISTS raw_sync_run_id String DEFAULT '',
    ADD COLUMN IF NOT EXISTS raw_sync_started_at DateTime DEFAULT raw_loaded_at;

ALTER TABLE air_quality.fct_air_quality_ward_level_monthly
    ADD COLUMN IF NOT EXISTS raw_loaded_at DateTime DEFAULT last_ingested_at,
    ADD COLUMN IF NOT EXISTS raw_sync_run_id String DEFAULT '',
    ADD COLUMN IF NOT EXISTS raw_sync_started_at DateTime DEFAULT raw_loaded_at;

ALTER TABLE air_quality.fct_air_quality_province_level_monthly
    ADD COLUMN IF NOT EXISTS raw_loaded_at DateTime DEFAULT last_ingested_at,
    ADD COLUMN IF NOT EXISTS raw_sync_run_id String DEFAULT '',
    ADD COLUMN IF NOT EXISTS raw_sync_started_at DateTime DEFAULT raw_loaded_at;
