-- Tạo database
CREATE DATABASE IF NOT EXISTS ${CLICKHOUSE_DB};

USE ${CLICKHOUSE_DB};

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

-- ============================================
-- WAQI / World Air Quality Index Measurements (Plan 1.02)
-- Source: api.waqi.info/feed/geo:bbox (one call for all Vietnam stations)
-- Vietnam bbox: geo:8.4;102.1;23.4;109.5
-- D-01: ReplacingMergeTree(ingest_time), server-side dedup
-- D-02: No Python-side dedup
-- ============================================
CREATE TABLE IF NOT EXISTS raw_waqi_measurements
(
    source              LowCardinality(String) DEFAULT 'waqi',
    ingest_time        DateTime DEFAULT now(),
    ingest_batch_id    String,
    ingest_date        Date MATERIALIZED toDate(ingest_time),

    station_id         String,                    -- waqi:{city_slug}
    city_name          String,
    latitude           Float64,
    longitude          Float64,

    timestamp_utc      DateTime,
    parameter          LowCardinality(String),    -- pm25, pm10, o3, no2, so2, co
    value              Float32,
    aqi_reported       UInt16,                   -- WAQI's AQI value
    dominant_pollutant LowCardinality(String),     -- Dominant pollutant string
    quality_flag       LowCardinality(String) DEFAULT 'valid',  -- valid | implausible

    raw_payload        String CODEC(ZSTD(1))
)
ENGINE = ReplacingMergeTree(ingest_time)
PARTITION BY toYYYYMM(ingest_date)
ORDER BY (station_id, timestamp_utc, parameter)
SETTINGS index_granularity = 8192;

-- NOTE: WAQI historical data is limited (~30 days). Backfill via dag_ingest_historical --source waqi.

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