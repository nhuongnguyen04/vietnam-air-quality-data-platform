{{ config(materialized='view') }}

-- AQI.in staging: clean raw measurements
-- Removes duplicates, normalizes parameters, extracts province from station_id
-- D-AQI-01: Added unit + quality_flag to align with unified schema (Phase 6)

with measurements as (
    select * from {{ source('aqiin', 'raw_aqiin_measurements') }}
),
ranked as (
    select
        *,
        row_number() over (
            partition by station_id, timestamp_utc, parameter
            order by ingest_time desc
        ) as rn
    from measurements
),
cleaned as (
    select * from ranked where rn = 1
),
normalized as (
    select
        'aqiin'                                            AS source,
        station_id,
        station_name,
        province,

        -- Normalize province names (some are duplicated like "ha-nam" vs "ha-nam-province")
        case
            when province like '%province' then substring(province, 1, length(province) - 9)
            else province
        end                                                AS province_clean,

        timestamp_utc,
        toDate(timestamp_utc)                              AS measurement_date,

        -- Normalize parameter names to canonical form
        case
            when parameter in ('pm25', 'pm2.5', 'pm2_5', 'pm25_concentration')
                then 'pm25'
            when parameter in ('pm10', 'pm10_concentration')
                then 'pm10'
            when parameter in ('co', 'carbon_monoxide', 'co_concentration')
                then 'co'
            when parameter in ('so2', 'sulfur_dioxide', 'so2_concentration')
                then 'so2'
            when parameter in ('no2', 'nitrogen_dioxide', 'no2_concentration')
                then 'no2'
            when parameter in ('o3', 'ozone', 'o3_concentration')
                then 'o3'
            when parameter in ('temp', 'temperature')
                then 'temp'
            when parameter in ('hum', 'humidity')
                then 'hum'
            else parameter
        end                                                AS parameter_clean,

        value,
        aqi_reported,

        -- D-AQI-01: unit based on parameter type
        case
            when parameter in ('pm25', 'pm2.5', 'pm2_5', 'pm25_concentration',
                              'pm10', 'pm10_concentration')
                then 'µg/m³'
            when parameter in ('co', 'carbon_monoxide', 'co_concentration')
                then 'ppm'
            when parameter in ('no2', 'nitrogen_dioxide', 'no2_concentration',
                              'o3', 'ozone', 'o3_concentration',
                              'so2', 'sulfur_dioxide', 'so2_concentration',
                              'nh3', 'ammonia', 'no')
                then 'ppb'
            when parameter in ('temp', 'temperature')
                then '°C'
            when parameter in ('hum', 'humidity')
                then '%'
            else 'µg/m³'
        end                                                AS unit,

        -- D-AQI-01: quality_flag — AQI.in community sensors, always 'valid' by default
        'valid'                                            AS quality_flag,

        raw_payload,
        ingest_time,
        ingest_batch_id
    from cleaned
)
select * from normalized