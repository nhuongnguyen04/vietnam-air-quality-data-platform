-- Backfill raw_loaded_at for existing raw tables without losing deterministic history.
-- Existing rows inherit ingest_time; new rows inserted after this migration use now().

ALTER TABLE air_quality.raw_aqiin_measurements
    ADD COLUMN IF NOT EXISTS raw_loaded_at DateTime DEFAULT ingest_time AFTER ingest_time;
ALTER TABLE air_quality.raw_aqiin_measurements
    ADD COLUMN IF NOT EXISTS raw_sync_run_id String DEFAULT '' AFTER raw_loaded_at;
ALTER TABLE air_quality.raw_aqiin_measurements
    ADD COLUMN IF NOT EXISTS raw_sync_started_at DateTime DEFAULT raw_loaded_at AFTER raw_sync_run_id;
ALTER TABLE air_quality.raw_aqiin_measurements
    ADD COLUMN IF NOT EXISTS raw_source_file_name String DEFAULT '' AFTER raw_sync_started_at;
ALTER TABLE air_quality.raw_aqiin_measurements
    ADD COLUMN IF NOT EXISTS raw_source_file_id String DEFAULT '' AFTER raw_source_file_name;

ALTER TABLE air_quality.raw_openweather_measurements
    ADD COLUMN IF NOT EXISTS raw_loaded_at DateTime DEFAULT ingest_time AFTER ingest_time;
ALTER TABLE air_quality.raw_openweather_measurements
    ADD COLUMN IF NOT EXISTS raw_sync_run_id String DEFAULT '' AFTER raw_loaded_at;
ALTER TABLE air_quality.raw_openweather_measurements
    ADD COLUMN IF NOT EXISTS raw_sync_started_at DateTime DEFAULT raw_loaded_at AFTER raw_sync_run_id;
ALTER TABLE air_quality.raw_openweather_measurements
    ADD COLUMN IF NOT EXISTS raw_source_file_name String DEFAULT '' AFTER raw_sync_started_at;
ALTER TABLE air_quality.raw_openweather_measurements
    ADD COLUMN IF NOT EXISTS raw_source_file_id String DEFAULT '' AFTER raw_source_file_name;

ALTER TABLE air_quality.raw_openweather_meteorology
    ADD COLUMN IF NOT EXISTS raw_loaded_at DateTime DEFAULT ingest_time AFTER ingest_time;
ALTER TABLE air_quality.raw_openweather_meteorology
    ADD COLUMN IF NOT EXISTS raw_sync_run_id String DEFAULT '' AFTER raw_loaded_at;
ALTER TABLE air_quality.raw_openweather_meteorology
    ADD COLUMN IF NOT EXISTS raw_sync_started_at DateTime DEFAULT raw_loaded_at AFTER raw_sync_run_id;
ALTER TABLE air_quality.raw_openweather_meteorology
    ADD COLUMN IF NOT EXISTS raw_source_file_name String DEFAULT '' AFTER raw_sync_started_at;
ALTER TABLE air_quality.raw_openweather_meteorology
    ADD COLUMN IF NOT EXISTS raw_source_file_id String DEFAULT '' AFTER raw_source_file_name;

ALTER TABLE air_quality.raw_tomtom_traffic
    ADD COLUMN IF NOT EXISTS raw_loaded_at DateTime DEFAULT ingest_time AFTER ingest_time;
ALTER TABLE air_quality.raw_tomtom_traffic
    ADD COLUMN IF NOT EXISTS raw_sync_run_id String DEFAULT '' AFTER raw_loaded_at;
ALTER TABLE air_quality.raw_tomtom_traffic
    ADD COLUMN IF NOT EXISTS raw_sync_started_at DateTime DEFAULT raw_loaded_at AFTER raw_sync_run_id;
ALTER TABLE air_quality.raw_tomtom_traffic
    ADD COLUMN IF NOT EXISTS raw_source_file_name String DEFAULT '' AFTER raw_sync_started_at;
ALTER TABLE air_quality.raw_tomtom_traffic
    ADD COLUMN IF NOT EXISTS raw_source_file_id String DEFAULT '' AFTER raw_source_file_name;

ALTER TABLE air_quality.raw_aqiin_measurements MATERIALIZE COLUMN raw_loaded_at;
ALTER TABLE air_quality.raw_aqiin_measurements MATERIALIZE COLUMN raw_sync_started_at;
ALTER TABLE air_quality.raw_openweather_measurements MATERIALIZE COLUMN raw_loaded_at;
ALTER TABLE air_quality.raw_openweather_measurements MATERIALIZE COLUMN raw_sync_started_at;
ALTER TABLE air_quality.raw_openweather_meteorology MATERIALIZE COLUMN raw_loaded_at;
ALTER TABLE air_quality.raw_openweather_meteorology MATERIALIZE COLUMN raw_sync_started_at;
ALTER TABLE air_quality.raw_tomtom_traffic MATERIALIZE COLUMN raw_loaded_at;
ALTER TABLE air_quality.raw_tomtom_traffic MATERIALIZE COLUMN raw_sync_started_at;

ALTER TABLE air_quality.raw_aqiin_measurements
    MODIFY COLUMN raw_loaded_at DateTime DEFAULT now();
ALTER TABLE air_quality.raw_aqiin_measurements
    MODIFY COLUMN raw_sync_started_at DateTime DEFAULT raw_loaded_at;

ALTER TABLE air_quality.raw_openweather_measurements
    MODIFY COLUMN raw_loaded_at DateTime DEFAULT now();
ALTER TABLE air_quality.raw_openweather_measurements
    MODIFY COLUMN raw_sync_started_at DateTime DEFAULT raw_loaded_at;

ALTER TABLE air_quality.raw_openweather_meteorology
    MODIFY COLUMN raw_loaded_at DateTime DEFAULT now();
ALTER TABLE air_quality.raw_openweather_meteorology
    MODIFY COLUMN raw_sync_started_at DateTime DEFAULT raw_loaded_at;

ALTER TABLE air_quality.raw_tomtom_traffic
    MODIFY COLUMN raw_loaded_at DateTime DEFAULT now();
ALTER TABLE air_quality.raw_tomtom_traffic
    MODIFY COLUMN raw_sync_started_at DateTime DEFAULT raw_loaded_at;
