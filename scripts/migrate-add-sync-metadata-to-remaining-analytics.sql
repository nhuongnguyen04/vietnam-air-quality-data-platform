ALTER TABLE air_quality.fct_other_measurements_hourly
    ADD COLUMN IF NOT EXISTS raw_loaded_at DateTime DEFAULT ingest_time,
    ADD COLUMN IF NOT EXISTS raw_sync_run_id String DEFAULT '',
    ADD COLUMN IF NOT EXISTS raw_sync_started_at DateTime DEFAULT raw_loaded_at;

ALTER TABLE air_quality.dm_aqi_compliance_standards
    ADD COLUMN IF NOT EXISTS raw_loaded_at DateTime DEFAULT ingest_time,
    ADD COLUMN IF NOT EXISTS raw_sync_run_id String DEFAULT '',
    ADD COLUMN IF NOT EXISTS raw_sync_started_at DateTime DEFAULT raw_loaded_at;

ALTER TABLE air_quality.dm_aqi_health_impact_summary
    ADD COLUMN IF NOT EXISTS raw_loaded_at DateTime DEFAULT ingest_time,
    ADD COLUMN IF NOT EXISTS raw_sync_run_id String DEFAULT '',
    ADD COLUMN IF NOT EXISTS raw_sync_started_at DateTime DEFAULT raw_loaded_at;

ALTER TABLE air_quality.dm_pollutant_source_fingerprint
    ADD COLUMN IF NOT EXISTS raw_loaded_at DateTime DEFAULT ingest_time,
    ADD COLUMN IF NOT EXISTS raw_sync_run_id String DEFAULT '',
    ADD COLUMN IF NOT EXISTS raw_sync_started_at DateTime DEFAULT raw_loaded_at;

ALTER TABLE air_quality.dm_aqi_temporal_patterns
    ADD COLUMN IF NOT EXISTS raw_loaded_at DateTime DEFAULT ingest_time,
    ADD COLUMN IF NOT EXISTS raw_sync_run_id String DEFAULT '',
    ADD COLUMN IF NOT EXISTS raw_sync_started_at DateTime DEFAULT raw_loaded_at;

ALTER TABLE air_quality.dm_traffic_pollution_correlation_daily
    ADD COLUMN IF NOT EXISTS raw_loaded_at DateTime DEFAULT now(),
    ADD COLUMN IF NOT EXISTS raw_sync_run_id String DEFAULT '',
    ADD COLUMN IF NOT EXISTS raw_sync_started_at DateTime DEFAULT raw_loaded_at;

ALTER TABLE air_quality.dm_weather_pollution_correlation_daily
    ADD COLUMN IF NOT EXISTS raw_loaded_at DateTime DEFAULT now(),
    ADD COLUMN IF NOT EXISTS raw_sync_run_id String DEFAULT '',
    ADD COLUMN IF NOT EXISTS raw_sync_started_at DateTime DEFAULT raw_loaded_at;

ALTER TABLE air_quality.fct_aqi_weather_traffic_unified
    ADD COLUMN IF NOT EXISTS raw_loaded_at DateTime DEFAULT now(),
    ADD COLUMN IF NOT EXISTS raw_sync_run_id String DEFAULT '',
    ADD COLUMN IF NOT EXISTS raw_sync_started_at DateTime DEFAULT raw_loaded_at;
