-- Tạo database
CREATE DATABASE IF NOT EXISTS ${CLICKHOUSE_DB};

USE ${CLICKHOUSE_DB};

-- Raw table cho OpenAQ Measurements (append-only, MergeTree)
-- Chỉ lưu dữ liệu measurement-specific, không lưu metadata đã có trong master tables
-- Master tables: raw_openaq_locations, raw_openaq_parameters, raw_openaq_sensors
-- 
-- LƯU Ý: Deduplication được xử lý trong Python jobs (check duplicate trước khi insert)
-- Unique key dựa trên dữ liệu thật: (location_id, sensor_id, parameter_id, period_datetime_from_utc, period_datetime_to_utc)
-- ingest_time chỉ là metadata về lịch sử ingest, không dùng để deduplicate
CREATE TABLE IF NOT EXISTS raw_openaq_measurements
(
    -- Metadata fields (lịch sử ingest)
    source          LowCardinality(String) DEFAULT 'openaq',
    ingest_time     DateTime DEFAULT now(),  -- Metadata: thời gian ingest (không dùng để deduplicate)
    ingest_batch_id String,
    ingest_date     Date MATERIALIZED toDate(ingest_time),
    
    -- Foreign keys (tham chiếu đến các bảng master)
    location_id     UInt32,                          -- FK → raw_openaq_locations
    sensor_id       UInt32,                          -- FK → raw_openaq_sensors
    parameter_id    UInt32,                          -- FK → raw_openaq_parameters
    
    -- Core measurement data (chỉ lưu giá trị measurement, không lưu parameter metadata)
    value           Float32,
    
    -- Period/Time information (thông tin về period của measurement này)
    period_label    LowCardinality(String),          -- period.label (e.g., "raw")
    period_interval String,                          -- period.interval (e.g., "01:00:00")
    period_datetime_from_utc DateTime,               -- period.datetimeFrom.utc (part of unique key)
    period_datetime_from_local String,              -- period.datetimeFrom.local (keep as string for timezone info)
    period_datetime_to_utc DateTime,                -- period.datetimeTo.utc (part of unique key)
    period_datetime_to_local String,                -- period.datetimeTo.local
    
    -- Coverage information (coverage của measurement period này, khác với sensor coverage tổng thể)
    coverage_expected_count UInt32,                  -- coverage.expectedCount
    coverage_expected_interval String,              -- coverage.expectedInterval
    coverage_observed_count UInt32,                  -- coverage.observedCount
    coverage_observed_interval String,               -- coverage.observedInterval
    coverage_percent_complete Float32,               -- coverage.percentComplete
    coverage_percent_coverage Float32,               -- coverage.percentCoverage
    coverage_datetime_from_utc DateTime,            -- coverage.datetimeFrom.utc
    coverage_datetime_to_utc DateTime,               -- coverage.datetimeTo.utc
    
    -- Flag information
    flag_has_flags  Bool,                            -- flagInfo.hasFlags
    
    -- Coordinates tại thời điểm measurement (có thể khác location coordinates do mobile sensors)
    latitude        Nullable(Float64),               -- coordinates.latitude
    longitude       Nullable(Float64),               -- coordinates.longitude
    
    -- Summary
    summary         Nullable(String),                -- summary
    
    -- Full JSON payload for audit/debugging
    raw_payload     String CODEC(ZSTD(1))
)
ENGINE = MergeTree()  -- Append-only: deduplication được xử lý trong Python jobs
PARTITION BY toYYYYMM(ingest_date)
ORDER BY (location_id, sensor_id, parameter_id, period_datetime_from_utc, period_datetime_to_utc)
SETTINGS index_granularity = 8192;

-- Locations metadata table
-- Dùng ReplacingMergeTree để tự động deduplicate dựa trên location_id
CREATE TABLE IF NOT EXISTS raw_openaq_locations
(
    source LowCardinality(String) DEFAULT 'openaq',
    ingest_time DateTime DEFAULT now(),
    ingest_batch_id String,
    ingest_date Date MATERIALIZED toDate(ingest_time),
    location_id UInt32,
    name String,
    locality Nullable(String),
    timezone String,
    country_id UInt32,
    country_code String,
    country_name String,
    owner_id UInt32,
    owner_name String,
    provider_id UInt32,
    provider_name String,
    is_mobile Bool,
    is_monitor Bool,
    latitude Float64,
    longitude Float64,
    raw_sensors String,
    datetime_first Nullable(String),  -- Cho phép NULL
    datetime_last Nullable(String),    -- Cho phép NULL
    raw_payload String CODEC(ZSTD(1))
)
ENGINE = ReplacingMergeTree(ingest_time)
PARTITION BY toYYYYMM(ingest_date)
ORDER BY (location_id)
SETTINGS index_granularity = 8192;


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

-- Parameters metadata table (master data)
-- Dùng ReplacingMergeTree để tự động deduplicate dựa trên parameter_id
CREATE TABLE IF NOT EXISTS raw_openaq_parameters
(
    source LowCardinality(String) DEFAULT 'openaq',
    ingest_time DateTime DEFAULT now(),
    ingest_batch_id String,
    ingest_date Date MATERIALIZED toDate(ingest_time),
    parameter_id UInt32,
    name String,
    display_name Nullable(String),
    units String,
    description Nullable(String),
    raw_payload String CODEC(ZSTD(1))
)
ENGINE = ReplacingMergeTree(ingest_time)
PARTITION BY toYYYYMM(ingest_date)
ORDER BY (parameter_id)
SETTINGS index_granularity = 8192;

-- Sensors metadata table (master data)
-- Dùng ReplacingMergeTree để tự động deduplicate dựa trên sensor_id
-- Dữ liệu từ API endpoint: /v3/locations/{locations_id}/sensors
CREATE TABLE IF NOT EXISTS raw_openaq_sensors
(
    source LowCardinality(String) DEFAULT 'openaq',
    ingest_time DateTime DEFAULT now(),
    ingest_batch_id String,
    ingest_date Date MATERIALIZED toDate(ingest_time),
    
    -- Primary key
    sensor_id UInt32,                                    -- id từ API response
    
    -- Foreign keys
    location_id UInt32,                                  -- FK → raw_openaq_locations
    parameter_id UInt32,                                 -- FK → raw_openaq_parameters (từ parameter.id)
    
    -- Sensor basic info
    name String,                                         -- name từ API (e.g., "pm25 µg/m³")
    
    -- Datetime information
    datetime_first_utc Nullable(DateTime),              -- datetimeFirst.utc
    datetime_first_local Nullable(String),              -- datetimeFirst.local (keep as string for timezone)
    datetime_last_utc Nullable(DateTime),               -- datetimeLast.utc
    datetime_last_local Nullable(String),               -- datetimeLast.local (keep as string for timezone)
    
    -- Coverage information
    coverage_expected_count Nullable(UInt32),           -- coverage.expectedCount
    coverage_expected_interval Nullable(String),        -- coverage.expectedInterval
    coverage_observed_count Nullable(UInt32),           -- coverage.observedCount
    coverage_observed_interval Nullable(String),        -- coverage.observedInterval
    coverage_percent_complete Nullable(Float32),        -- coverage.percentComplete
    coverage_percent_coverage Nullable(Float32),       -- coverage.percentCoverage
    coverage_datetime_from_utc Nullable(DateTime),      -- coverage.datetimeFrom.utc
    coverage_datetime_to_utc Nullable(DateTime),       -- coverage.datetimeTo.utc
    
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

-- Optional: tạo user nếu cần (default đã có)
-- CREATE USER IF NOT EXISTS ${CLICKHOUSE_USER} IDENTIFIED WITH sha256_password BY '${CLICKHOUSE_PASSWORD}';
-- GRANT ALL ON ${CLICKHOUSE_DB}.* TO ${CLICKHOUSE_USER};