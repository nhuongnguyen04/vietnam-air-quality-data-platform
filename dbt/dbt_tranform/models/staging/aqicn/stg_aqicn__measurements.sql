{{ config(materialized='view') }}

with source as (
    select * from {{ source('aqicn', 'raw_aqicn_measurements') }}
    where {{ filter_vietnam_aqicn() }}
),
deduplicated as (
    select *,
           row_number() over (
               partition by station_id, time_v, pollutant
               order by ingest_time desc
           ) as rn
    from source
),
canonical as (
    select
        'aqicn'                                                             AS source,
        concat('AQICN_', station_id)                                         AS station_id,
        toDateTime(toInt64OrNull(time_v))                                   AS timestamp_utc,
        {{ standardize_pollutant_name('pollutant') }}                        AS parameter,
        toFloat64OrNull(value)                                              AS value,
        case
            when {{ standardize_pollutant_name('pollutant') }} in ('pm25', 'pm10') then 'µg/m³'
            when {{ standardize_pollutant_name('pollutant') }} in ('o3', 'no2', 'so2') then 'ppb'
            when {{ standardize_pollutant_name('pollutant') }} = 'co' then 'ppm'
            else 'unknown'
        end                                                                 AS unit,
        'valid'                                                             AS quality_flag,
        toInt32OrNull(aqi)                                                  AS aqi_reported,
        ingest_time,
        raw_payload
    from deduplicated
    where rn = 1
)
select * from canonical
