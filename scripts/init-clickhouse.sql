-- Tạo database
CREATE DATABASE IF NOT EXISTS ${CLICKHOUSE_DB};

USE ${CLICKHOUSE_DB};

-- ============================================
-- Phase 2.4: Drop stale tables
-- D-05: raw_waqi_measurements was never created (WAQI == AQICN same API)
-- D-05: raw_waqi_measurements dropped to avoid confusion
-- ============================================
DROP TABLE IF EXISTS raw_waqi_measurements;

-- ============================================
-- OpenAQ Decommission (Plan 1.04)
-- D-07 / D-34: Rename OpenAQ tables to raw_openaq_*_archived (NOT DROP)
-- D-34: Retain data for rollback safety
-- To restore: RENAME TABLE raw_openaq_*_archived TO raw_openaq_*
-- ============================================

-- DISABLED (Plan 1.04): raw_openaq_measurements table
-- Original CREATE TABLE body preserved in git history.
-- Table was renamed to raw_openaq_measurements_archived via RENAME TABLE below.

-- DISABLED (Plan 1.04): raw_openaq_locations table
-- Table was renamed to raw_openaq_locations_archived via RENAME TABLE below.

-- DISABLED (Plan 1.04): raw_openaq_parameters table
-- Table was renamed to raw_openaq_parameters_archived via RENAME TABLE below.

-- DISABLED (Plan 1.04): raw_openaq_sensors table
-- Table was renamed to raw_openaq_sensors_archived via RENAME TABLE below.


-- ============================================
-- RENAME OpenAQ tables to archived (Plan 1.04)
-- D-07 / D-34: Data preserved for rollback safety
-- ============================================
RENAME TABLE raw_openaq_measurements TO raw_openaq_measurements_archived;
RENAME TABLE raw_openaq_locations TO raw_openaq_locations_archived;
RENAME TABLE raw_openaq_parameters TO raw_openaq_parameters_archived;
RENAME TABLE raw_openaq_sensors TO raw_openaq_sensors_archived;


-- AQICN Measurements table (từ feed API)
-- NGUYÊN TẮC: Giữ nguyên 100% dữ liệu gốc từ API, không convert hay xử lý
-- Dữ liệu từ API endpoint: /feed/@{station_id}/?token=...
-- LƯU Ý: Thông tin station (name, lat, lon, url, location) KHÔNG lưu ở đây,
-- chỉ lưu station_id. Thông tin station nằm trong raw_aqicn_stations.
CREATE TABLE IF NOT EXISTS raw_aqicn_measurements
(
    -- Metadata ingest (bắt buộc theo quy ước dự án)
    source               LowCardinality(String) DEFAULT 'aqicn',
    ingest_time          DateTime DEFAULT now(),  -- Metadata: thời gian ingest (cho phép lưu nhiều phiên bản)
    ingest_batch_id      String,
    ingest_date          Date MATERIALIZED toDate(ingest_time),

    -- FK đến station (thông tin station nằm trong raw_aqicn_stations)
    station_id           String,                      -- data.idx từ API (ví dụ: '1583')

    -- Thời gian đo (giữ nguyên 100% từ API, không convert)
    time_s               Nullable(String),            -- data.time.s (ví dụ: "2026-01-25 18:00:00")
    time_tz              Nullable(String),            -- data.time.tz (ví dụ: "+07:00")
    time_v               Nullable(String),            -- data.time.v (Unix timestamp dạng string)
    time_iso             Nullable(String),            -- data.time.iso (ví dụ: "2026-01-25T18:00:00+07:00")

    -- Chỉ số AQI tổng hợp (giữ nguyên dạng string từ API)
    aqi                  Nullable(String),            -- data.aqi (ví dụ: "171" hoặc có thể là "-")

    -- Dominant pollutant (giữ nguyên từ API)
    dominentpol          Nullable(String),            -- data.dominentpol (ví dụ: "pm25")

    -- Chỉ số chi tiết (pollutant cụ thể) - normalize từ iaqi
    pollutant            LowCardinality(String),      -- tên chỉ số từ iaqi keys (pm25, pm10, dew, h, p, t, w, ...)
    value                Nullable(String),            -- giá trị từ iaqi.{pollutant}.v - giữ nguyên dạng string

    -- Attributions (giữ nguyên JSON array dạng string)
    attributions         Nullable(String),            -- data.attributions - JSON array dạng string

    -- Debug info (giữ nguyên từ API)
    debug_sync          Nullable(String),            -- data.debug.sync (nếu có)


    -- Full JSON gốc để audit/debug/reprocess - GIỮ NGUYÊN 100%
    raw_payload          String CODEC(ZSTD(1))
)
ENGINE = MergeTree()  -- Append-only: lưu tất cả dữ liệu lịch sử
PARTITION BY toYYYYMM(ingest_date)
ORDER BY (station_id, time_v, pollutant, ingest_time)
SETTINGS index_granularity = 8192, allow_nullable_key = 1;

-- AQICN Forecast table (từ feed API - forecast.daily)
-- NGUYÊN TẮC: Giữ nguyên 100% dữ liệu gốc từ API, không convert hay xử lý
CREATE TABLE IF NOT EXISTS raw_aqicn_forecast
(
    -- Metadata ingest (bắt buộc theo quy ước dự án)
    source               LowCardinality(String) DEFAULT 'aqicn',
    ingest_time          DateTime DEFAULT now(),
    ingest_batch_id      String,
    ingest_date          Date MATERIALIZED toDate(ingest_time),

    -- Foreign key đến measurement
    station_id           String,                      -- idx của trạm (ví dụ: '1583')
    measurement_time_v  Nullable(String),            -- time.v từ measurement để link

    -- Forecast type và pollutant
    forecast_type        LowCardinality(String),      -- "daily"
    pollutant            LowCardinality(String),      -- "pm10", "pm25", "uvi", ...

    -- Forecast data (giữ nguyên 100% từ API, không convert)
    day                  Nullable(String),            -- forecast.daily.{pollutant}[].day (ví dụ: "2026-01-23")
    avg                  Nullable(String),            -- forecast.daily.{pollutant}[].avg - giữ nguyên dạng string
    max                  Nullable(String),            -- forecast.daily.{pollutant}[].max - giữ nguyên dạng string
    min                  Nullable(String),            -- forecast.daily.{pollutant}[].min - giữ nguyên dạng string

    -- Full JSON gốc của forecast item để audit/debug/reprocess - GIỮ NGUYÊN 100%
    raw_forecast_item    String CODEC(ZSTD(1))
)
ENGINE = ReplacingMergeTree(ingest_time)
PARTITION BY toYYYYMM(ingest_date)
ORDER BY (station_id, measurement_time_v, forecast_type, pollutant, day, ingest_time)
SETTINGS index_granularity = 8192;

-- Stations metadata table (master data từ crawl.html + feed API)
-- NGUYÊN TẮC: Giữ nguyên 100% dữ liệu gốc từ API, không convert hay xử lý
-- Dùng ReplacingMergeTree để tự động deduplicate dựa trên station_id + date
-- Station IDs được crawl từ https://aqicn.org/city/vietnam/ (showHistorical)
-- Thông tin station lấy từ feed API: /feed/@{station_id}/?token=...
CREATE TABLE IF NOT EXISTS raw_aqicn_stations
(
    -- Metadata ingest (bắt buộc theo quy ước dự án)
    source               LowCardinality(String) DEFAULT 'aqicn',
    ingest_time          DateTime DEFAULT now(),
    ingest_batch_id      String,
    ingest_date          Date MATERIALIZED toDate(ingest_time),

    -- Primary key (từ crawl.html showHistorical)
    station_id           String,                      -- x từ showHistorical (ví dụ: '1583', '8632', 'A573400')
    
    -- Thông tin trạm đo (từ feed API data.city.*)
    station_name         Nullable(String),            -- data.city.name (ví dụ: 'Hanoi, Vietnam (Hà Nội)')
    latitude             Nullable(String),            -- data.city.geo[0] - giữ nguyên dạng string
    longitude            Nullable(String),            -- data.city.geo[1] - giữ nguyên dạng string
    
    -- Thời gian đo (từ feed API data.time.*)
    station_time         Nullable(String),            -- data.time.iso - giữ nguyên ISO format với timezone
    
    -- Chỉ số AQI tổng hợp (giữ nguyên dạng string từ API)
    aqi                  Nullable(String),            -- data.aqi - giữ nguyên (có thể là "39", "171", "-")
    
    -- URL và location (từ feed API data.city.*)
    city_url             Nullable(String),            -- data.city.url
    city_location        Nullable(String),            -- data.city.location

    -- Full JSON gốc để audit/debug/reprocess - GIỮ NGUYÊN 100%
    raw_payload          String CODEC(ZSTD(1))
)
ENGINE = ReplacingMergeTree(ingest_time)
PARTITION BY toYYYYMM(ingest_date)
ORDER BY (station_id, ingest_date, ingest_time)
SETTINGS index_granularity = 8192, allow_nullable_key = 1;

-- DISABLED (Plan 1.04): raw_openaq_parameters table
-- Table was renamed to raw_openaq_parameters_archived via RENAME TABLE above.

-- DISABLED (Plan 1.04): raw_openaq_sensors table
-- Table was renamed to raw_openaq_sensors_archived via RENAME TABLE above.
    
    -- Latest measurement info
    latest_datetime_utc Nullable(DateTime),            -- latest.datetime.utc
    latest_datetime_local Nullable(String),            -- latest.datetime.local
    latest_value Nullable(Float32),                     -- latest.value
    latest_latitude Nullable(Float64),                  -- latest.coordinates.latitude
    latest_longitude Nullable(Float64),                 -- latest.coordinates.longitude
    -- Full JSON payload for audit/debugging
    raw_payload String CODEC(ZSTD(1))
)
ENGINE = ReplacingMergeTree(ingest_time)
PARTITION BY toYYYYMM(ingest_date)
ORDER BY (sensor_id)
SETTINGS index_granularity = 8192;

-- ============================================
-- Ingestion Control Table (Plan 0.5)
-- Tracks run metadata for each data source
-- Consumed by Grafana freshness dashboards (Phase 3.4) and alerting (Phase 5.2)
-- ============================================
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

-- NOTE: WAQI and AQICN are the same API (api.waqi.info). Only AQICN ingestion
-- is active; raw_waqi_measurements table has been removed (dead data source).

-- ============================================
-- Sensors.Community Measurements (Plan 1.03)
-- Source: api.sensor.community/v1/feeds/
-- Vietnam bbox: lat=16.0, latDelta=7.5, lng=105.0, lngDelta=7.0
-- D-05: Insert ALL data; quality_flag = valid/implausible/outlier
-- D-29: ReplacingMergeTree(ingest_time), server-side dedup
-- D-23, D-26: quality_flag logic applied during ingestion
-- ============================================
CREATE TABLE IF NOT EXISTS raw_sensorscm_measurements
(
    source              LowCardinality(String) DEFAULT 'sensorscm',
    ingest_time         DateTime DEFAULT now(),
    ingest_batch_id     String,
    ingest_date         Date MATERIALIZED toDate(ingest_time),

    sensor_id           UInt32,
    station_id          UInt32,                    -- id from API (reused as station_id)

    latitude            Float64,
    longitude           Float64,

    timestamp_utc       Nullable(DateTime),
    parameter           LowCardinality(String),    -- pm10, pm25, temperature, humidity
    value               Float32,
    unit                String DEFAULT 'µg/m³',

    sensor_type         LowCardinality(String),    -- SDS011, PMS5003, etc.
    quality_flag        LowCardinality(String),     -- valid | implausible | outlier (D-05, D-23, D-26)

    raw_payload         String CODEC(ZSTD(1))
)
ENGINE = ReplacingMergeTree(ingest_time)
PARTITION BY toYYYYMM(ingest_date)
ORDER BY (station_id, timestamp_utc, parameter)
SETTINGS index_granularity = 8192;

-- Sensors/sensor metadata
-- Stores per-sensor metadata (sensor_type, location) deduplicated on sensor_id
CREATE TABLE IF NOT EXISTS raw_sensorscm_sensors
(
    source              LowCardinality(String) DEFAULT 'sensorscm',
    ingest_time         DateTime DEFAULT now(),
    ingest_batch_id     String,
    ingest_date         Date MATERIALIZED toDate(ingest_time),

    sensor_id           UInt32,
    station_id          UInt32,
    latitude            Float64,
    longitude           Float64,
    sensor_type         LowCardinality(String),
    is_indoor          Bool DEFAULT false,

    raw_payload         String CODEC(ZSTD(1))
)
ENGINE = ReplacingMergeTree(ingest_time)
PARTITION BY toYYYYMM(ingest_date)
ORDER BY (sensor_id)
SETTINGS index_granularity = 8192;

-- NOTE: Sensors.Community has NO historical backfill (D-24).
-- Real-time polling only. dag_sensorscm_poll (*/10 * * * *) added in PLAN-1-05.

-- ============================================
-- OpenWeather Air Pollution Measurements (Plan 1.01)
-- Source: OpenWeather Air Pollution API (city-centroid polling)
-- D-01: ReplacingMergeTree(ingest_time), server-side dedup
-- D-02: No Python-side dedup
-- ============================================
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
    aqi_reported       UInt8,                     -- OpenWeather's own AQI (1-5); canonical AQI in Phase 2 dbt (D-16)
    quality_flag       LowCardinality(String) DEFAULT 'valid',  -- valid | implausible

    raw_payload        String CODEC(ZSTD(1))
)
ENGINE = ReplacingMergeTree(ingest_time)
PARTITION BY toYYYYMM(ingest_date)
ORDER BY (station_id, timestamp_utc, parameter)
SETTINGS index_granularity = 8192;

-- Note: OpenWeather historical backfill available from Nov 2020 via /air_pollution/history
-- Run dag_ingest_historical with --source openweather --start-date 2026-01-01 --end-date 2026-03-31

-- Optional: tạo user nếu cần (default đã có)
-- CREATE USER IF NOT EXISTS ${CLICKHOUSE_USER} IDENTIFIED WITH sha256_password BY '${CLICKHOUSE_PASSWORD}';
-- GRANT ALL ON ${CLICKHOUSE_DB}.* TO ${CLICKHOUSE_USER};

-- ============================================
-- Mart Tables (dbt-managed — here for schema visibility)
-- Phase 2.4: Created by dbt run --select +fct_hourly_aqi
-- Do NOT edit manually — dbt owns these tables
-- ============================================

-- dim_locations: Station dimension with ReplacingMergeTree
CREATE TABLE IF NOT EXISTS dim_locations
(
    station_id              String,
    source                 LowCardinality(String),
    station_name           String,
    latitude               Float64,
    longitude              Float64,
    city                   String,
    province               String,
    location_type          String,
    sensor_quality_tier    LowCardinality(String),
    is_active              Bool,
    first_seen             DateTime,
    last_seen              DateTime,
    location_version       DateTime
)
ENGINE = ReplacingMergeTree(location_version)
ORDER BY station_id
PARTITION BY toYYYYMM(last_seen)
TTL last_seen + INTERVAL 365 DAY;

-- dim_time: Time dimension (seed-loaded)
CREATE TABLE IF NOT EXISTS dim_time
(
    datetime_hour          DateTime,
    date                   Date,
    day_of_week            String,
    hour_of_day            UInt8,
    is_weekend             Bool,
    month                  UInt8,
    year                   UInt16,
    vietnam_tz_offset      String
)
ENGINE = ReplacingMergeTree()
ORDER BY datetime_hour;

-- dim_pollutants: Pollutant dimension with EPA breakpoint data
CREATE TABLE IF NOT EXISTS dim_pollutants
(
    pollutant_key           String,
    display_name            String,
    unit                   String,
    epa_aqi_bp_lo          Float64,
    epa_aqi_bp_hi          Float64,
    conc_bp_lo             Float64,
    conc_bp_hi             Float64,
    health_effects         String
)
ENGINE = ReplacingMergeTree()
ORDER BY pollutant_key;

-- fct_hourly_aqi: Hourly fact with AggregatingMergeTree
-- NOTE: Requires microbatch incremental run. Initial build: dbt run --full-refresh
-- Query layer: fct_hourly_aqi_final (not this table directly)
CREATE TABLE IF NOT EXISTS fct_hourly_aqi
(
    datetime_hour              DateTime,
    station_id                 String,
    pollutant                  String,
    avg_value                  AggregateFunction(avg, Float64),
    avg_aqi                   AggregateFunction(avg, Float64),
    measurement_count          AggregateFunction(count),
    max_value                  AggregateFunction(max, Float64),
    min_value                  AggregateFunction(min, Float64),
    exceedance_count_150       AggregateFunction(countIf, Float64),
    exceedance_count_200       AggregateFunction(countIf, Float64),
    invalid_count              AggregateFunction(countIf, Float64),
    avg_aqi_reported          AggregateFunction(avg, Float64),
    dominant_pollutant_state  AggregateFunction(argMax, String, Float64),
    sensor_quality_tier        LowCardinality(String),
    source                    LowCardinality(String)
)
ENGINE = AggregatingMergeTree()
ORDER BY (datetime_hour, station_id, pollutant)
PARTITION BY toYYYYMM(datetime_hour)
TTL datetime_hour + INTERVAL 90 DAY;

-- fct_hourly_aqi_final: Query layer view for fct_hourly_aqi
CREATE VIEW IF NOT EXISTS fct_hourly_aqi_final AS
SELECT
    datetime_hour,
    station_id,
    pollutant,
    round(avgMerge(avg_value), 2)                                    AS avg_value,
    round(avgMerge(avg_aqi), 2)                                      AS normalized_aqi,
    countMerge(measurement_count)                                     AS measurement_count,
    round(maxMerge(max_value), 2)                                     AS max_value,
    round(minMerge(min_value), 2)                                     AS min_value,
    countMerge(exceedance_count_150)                                   AS exceedance_count_150,
    countMerge(exceedance_count_200)                                   AS exceedance_count_200,
    countMerge(invalid_count)                                         AS invalid_count,
    round(avgMerge(avg_aqi_reported), 2)                              AS avg_aqi_reported,
    finalizeAggregation(dominant_pollutant_state)                     AS dominant_pollutant,
    sensor_quality_tier,
    source
FROM fct_hourly_aqi
GROUP BY datetime_hour, station_id, pollutant, sensor_quality_tier, source;

-- fct_daily_aqi_summary: Daily fact with AggregatingMergeTree
CREATE TABLE IF NOT EXISTS fct_daily_aqi_summary
(
    date                    Date,
    station_id              String,
    avg_aqi_state          AggregateFunction(avg, Float64),
    avg_value_state         AggregateFunction(avg, Float64),
    min_aqi_state          AggregateFunction(min, Float64),
    max_aqi_state          AggregateFunction(max, Float64),
    hourly_count_state      AggregateFunction(count),
    exceedance_count_150_state AggregateFunction(sum, UInt64),
    exceedance_count_200_state AggregateFunction(sum, UInt64),
    dominant_pollutant_state AggregateFunction(argMax, String, Float64),
    sensor_quality_tier     LowCardinality(String),
    source                 LowCardinality(String)
)
ENGINE = AggregatingMergeTree()
ORDER BY (date, station_id)
PARTITION BY toYYYYMM(date);

-- fct_daily_aqi_summary_final: Query layer view
CREATE VIEW IF NOT EXISTS fct_daily_aqi_summary_final AS
SELECT
    date,
    station_id,
    round(avgMerge(avg_aqi_state), 2)                          AS avg_aqi,
    round(avgMerge(avg_value_state), 2)                         AS avg_value,
    round(minMerge(min_aqi_state), 2)                           AS min_aqi,
    round(maxMerge(max_aqi_state), 2)                           AS max_aqi,
    countMerge(hourly_count_state)                              AS hourly_count,
    countMerge(exceedance_count_150_state)                      AS exceedance_count_150,
    countMerge(exceedance_count_200_state)                      AS exceedance_count_200,
    finalizeAggregation(dominant_pollutant_state)                AS dominant_pollutant,
    sensor_quality_tier,
    source
FROM fct_daily_aqi_summary
GROUP BY date, station_id, sensor_quality_tier, source;

-- fact_aqi_alerts: AQI threshold breach events
CREATE TABLE IF NOT EXISTS fact_aqi_alerts
(
    station_id              String,
    datetime_hour           DateTime,
    threshold_breached      String,
    normalized_aqi          Float64,
    dominant_pollutant      String,
    source                 LowCardinality(String),
    sensor_quality_tier    LowCardinality(String),
    created_at             DateTime
)
ENGINE = AggregatingMergeTree()
ORDER BY (station_id, datetime_hour, threshold_breached);

-- fact_aqi_alerts_final: Query layer view
CREATE VIEW IF NOT EXISTS fact_aqi_alerts_final AS
SELECT * FROM fact_aqi_alerts;

-- mart_air_quality__dashboard: Pre-joined dashboard mart
CREATE TABLE IF NOT EXISTS mart_air_quality__dashboard
(
    date                    Date,
    station_id              String,
    station_name            String,
    latitude                Float64,
    longitude               Float64,
    city                    String,
    province                String,
    source                 LowCardinality(String),
    sensor_quality_tier    LowCardinality(String),
    avg_aqi                Float64,
    min_aqi                Float64,
    max_aqi                Float64,
    hourly_count            UInt64,
    exceedance_count_150    UInt64,
    exceedance_count_200    UInt64,
    dominant_pollutant       String
)
ENGINE = AggregatingMergeTree()
ORDER BY (date, station_id)
PARTITION BY toYYYYMM(date)
TTL date + INTERVAL 90 DAY;

-- ============================================
-- ClickHouse Native Materialized Views (Phase 2.4)
-- Pre-aggregated views for fast dashboard queries
-- Updated automatically on raw table INSERT
-- ============================================

-- mv_hourly_station_aqi: Pre-aggregated hourly AQI by station
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
    SELECT station_id, timestamp_utc, source, parameter, value FROM raw_aqicn_measurements
    UNION ALL
    SELECT concat('OPENWEATHER_', upper(city_name)), timestamp_utc, 'openweather', parameter, value FROM raw_openweather_measurements
    UNION ALL
    SELECT concat('SENSORSCM_', toString(sensor_id)), timestamp_utc, 'sensorscm', parameter, value FROM raw_sensorscm_measurements
)
WHERE timestamp_utc IS NOT NULL AND value IS NOT NULL
GROUP BY datetime_hour, station_id, source, pollutant;

-- mv_daily_station_summary: Pre-aggregated daily summary
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