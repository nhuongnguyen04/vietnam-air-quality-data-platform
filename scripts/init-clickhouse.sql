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

-- 7. TomTom Traffic Flow Data
-- Raw 3-hourly samples
CREATE TABLE IF NOT EXISTS raw_tomtom_traffic
(
    source              LowCardinality(String) DEFAULT 'tomtom',
    ingest_time        DateTime DEFAULT now(),
    station_name       String,
    latitude           Float64,
    longitude          Float64,
    timestamp_utc      DateTime,
    current_speed      Float32,
    free_flow_speed    Float32,
    confidence         Float32,
    raw_payload        String CODEC(ZSTD(1))
)
ENGINE = MergeTree()
PARTITION BY toYYYYMM(timestamp_utc)
ORDER BY (station_name, timestamp_utc)
SETTINGS index_granularity = 8192;

-- Calculated 1-hourly traffic (interpolated by Python)
CREATE TABLE IF NOT EXISTS raw_tomtom_traffic_hourly
(
    source              LowCardinality(String) DEFAULT 'tomtom_calculated',
    station_name       String,
    latitude           Float64,
    longitude          Float64,
    hour_utc           DateTime,
    congestion_ratio   Float32,
    data_quality_flag  LowCardinality(String), -- real-time, interpolated, baseline
    updated_at         DateTime DEFAULT now()
)
ENGINE = ReplacingMergeTree(updated_at)
PARTITION BY toYYYYMM(hour_utc)
ORDER BY (station_name, hour_utc)
SETTINGS index_granularity = 8192;

-- 8. OpenWeather Meteorology (New Feed)
CREATE TABLE IF NOT EXISTS raw_openweather_meteorology
(
    source              LowCardinality(String) DEFAULT 'openweather',
    province           String,
    latitude           Float64,
    longitude          Float64,
    timestamp_utc      DateTime,
    temp               Float32,
    feels_like         Float32,
    humidity           UInt8,
    pressure           UInt16,
    wind_speed         Float32,
    wind_deg           UInt16,
    clouds_all         UInt8,
    ingest_time        DateTime DEFAULT now()
)
ENGINE = ReplacingMergeTree(ingest_time)
PARTITION BY toYYYYMM(timestamp_utc)
ORDER BY (province, timestamp_utc)
SETTINGS index_granularity = 8192;
