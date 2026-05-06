ALTER TABLE air_quality.fct_air_quality_summary_daily
    RENAME COLUMN IF EXISTS max_raw_loaded_at TO raw_loaded_at;

ALTER TABLE air_quality.fct_air_quality_summary_daily
    RENAME COLUMN IF EXISTS latest_raw_sync_run_id TO raw_sync_run_id;

ALTER TABLE air_quality.fct_air_quality_summary_daily
    RENAME COLUMN IF EXISTS latest_raw_sync_started_at TO raw_sync_started_at;
