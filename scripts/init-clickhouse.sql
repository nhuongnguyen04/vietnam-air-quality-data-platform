-- Vietnam Air Quality Data Platform - ClickHouse Initialization Script
-- Focus: OpenWeather Air Pollution API + AQI.in Widget Scraper
-- Last Updated: 2026-04-09

-- 1. Create Database
CREATE DATABASE IF NOT EXISTS ${CLICKHOUSE_DB};
USE ${CLICKHOUSE_DB};

-- 2. Ingestion Control Table
-- Tracks run metadata and freshness for each data source
CREATE TABLE IF NOT EXISTS ingestion_control
(
    source              LowCardinality(String),
    last_run           DateTime DEFAULT now(),
    last_success       DateTime,
    records_ingested   UInt64 DEFAULT 0,
    lag_seconds        Int64 DEFAULT 0,
    error_message      String DEFAULT '',
    updated_at         DateTime DEFAULT now()
)
ENGINE = ReplacingMergeTree(updated_at)
ORDER BY source
SETTINGS index_granularity = 8192;

-- 3. OpenWeather Air Pollution (62 Vietnam Provinces)
-- Current observations
CREATE TABLE IF NOT EXISTS raw_openweather_measurements
(
    source              LowCardinality(String) DEFAULT 'openweather',
    ingest_time        DateTime DEFAULT now(),
    ingest_batch_id    String,
    ingest_date        Date MATERIALIZED toDate(ingest_time),

    station_id         String,                    -- openweather:{city}:{lat}:{lon}
    city_name          String,
    latitude           Float64,
    longitude          Float64,

    timestamp_utc      DateTime,
    parameter          LowCardinality(String),    -- pm25, pm10, o3, no2, so2, co, nh3, no
    value              Float32,
    aqi_reported       UInt8,                     -- OpenWeather's own AQI (1-5)
    quality_flag       LowCardinality(String) DEFAULT 'valid',

    raw_payload        String CODEC(ZSTD(1))
)
ENGINE = ReplacingMergeTree(ingest_time)
PARTITION BY toYYYYMM(ingest_date)
ORDER BY (station_id, timestamp_utc, parameter)
SETTINGS index_granularity = 8192;

-- 4. AQI.in Measurements (Vietnam Monitoring Stations)
-- Unified schema: station_name is the primary identifier
CREATE TABLE IF NOT EXISTS raw_aqiin_measurements
(
    source              LowCardinality(String) DEFAULT 'aqiin',
    ingest_time         DateTime DEFAULT now(),
    ingest_batch_id     String,
    ingest_date         Date MATERIALIZED toDate(ingest_time),

    station_name        String,

    timestamp_utc      DateTime,
    parameter          LowCardinality(String),    -- pm25, pm10, o3, no2, so2, co, temp, hum
    value              Float32,
    aqi_reported       UInt16,

    unit              LowCardinality(String) DEFAULT 'µg/m³',
    quality_flag      LowCardinality(String) DEFAULT 'valid',

    raw_payload        String CODEC(ZSTD(1))
)
ENGINE = MergeTree()
PARTITION BY toYYYYMM(ingest_date)
ORDER BY (station_name, timestamp_utc, parameter)
SETTINGS index_granularity = 8192;

-- 5. OpenMetadata Access (Read-only)
CREATE USER IF NOT EXISTS om_reader IDENTIFIED WITH sha256_password BY 'om_reader_secure_pass';
GRANT SELECT ON ${CLICKHOUSE_DB}.* TO om_reader;
GRANT SELECT ON system.* TO om_reader;
GRANT SELECT ON system.query_log TO om_reader;

-- 6. Pre-aggregated Materialized Views
-- Hourly aggregation for dashboarding
CREATE MATERIALIZED VIEW IF NOT EXISTS mv_hourly_station_aqi
ENGINE = SummingMergeTree()
ORDER BY (datetime_hour, station_id)
PARTITION BY toYYYYMM(datetime_hour)
AS
SELECT
    toStartOfHour(timestamp_utc)                              AS datetime_hour,
    station_id,
    source,
    parameter                                                 AS pollutant,
    avg(value)                                                AS avg_value,
    count()                                                   AS measurement_count,
    max(value)                                                AS max_value,
    min(value)                                                AS min_value,
    now()                                                     AS updated_at
FROM (
    -- Unified source selection
    SELECT station_name AS station_id, timestamp_utc, source, parameter, value FROM raw_aqiin_measurements
    UNION ALL
    SELECT station_id, timestamp_utc, source, parameter, value FROM raw_openweather_measurements
)
WHERE timestamp_utc IS NOT NULL AND value IS NOT NULL
GROUP BY datetime_hour, station_id, source, pollutant;

-- Daily aggregation
CREATE MATERIALIZED VIEW IF NOT EXISTS mv_daily_station_summary
ENGINE = SummingMergeTree()
ORDER BY (date, station_id)
PARTITION BY toYYYYMM(date)
AS
SELECT
    toDate(datetime_hour)                                    AS date,
    station_id,
    source,
    avg(avg_value)                                           AS avg_aqi,
    max(max_value)                                            AS max_aqi,
    min(min_value)                                           AS min_aqi,
    sum(measurement_count)                                     AS total_measurements,
    now()                                                     AS updated_at
FROM mv_hourly_station_aqi
GROUP BY date, station_id, source;
