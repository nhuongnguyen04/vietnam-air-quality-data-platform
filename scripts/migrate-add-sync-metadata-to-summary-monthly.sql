ALTER TABLE air_quality.fct_air_quality_summary_monthly
    ADD COLUMN IF NOT EXISTS ingest_time DateTime DEFAULT now(),
    ADD COLUMN IF NOT EXISTS raw_loaded_at DateTime DEFAULT ingest_time,
    ADD COLUMN IF NOT EXISTS raw_sync_run_id String DEFAULT '',
    ADD COLUMN IF NOT EXISTS raw_sync_started_at DateTime DEFAULT raw_loaded_at;
