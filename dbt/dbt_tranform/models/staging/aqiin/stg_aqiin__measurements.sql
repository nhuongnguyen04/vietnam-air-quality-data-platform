{{ config(
    materialized='incremental',
    engine='ReplacingMergeTree',
    unique_key='(station_name, timestamp_utc, parameter)',
    order_by='(timestamp_utc, station_name, parameter)',
    partition_by='toYYYYMM(timestamp_utc)',
    query_settings={
        'max_threads': 2,
        'max_bytes_before_external_sort': 100000000,
        'max_bytes_before_external_group_by': 100000000
    }
) }}

with incremental_source as (
    select
        station_name,
        timestamp_utc,
        CAST(parameter AS String) as parameter,
        value,
        CAST(unit AS String) as unit,
        aqi_reported,
        CAST(quality_flag AS String) as quality_flag,
        ingest_time
    from {{ source('aqiin', 'raw_aqiin_measurements') }}
    {% if is_incremental() %}
    where ingest_time >= (
        select max(ingest_time) - interval 24 hour
        from {{ this }}
    )
    {% endif %}
),

deduplicated as (
    select
        station_name,
        timestamp_utc,
        parameter,
        argMax(value, ingest_time) as raw_value,
        argMax(unit, ingest_time) as raw_unit,
        argMax(aqi_reported, ingest_time) as aqi_reported,
        argMax(quality_flag, ingest_time) as quality_flag,
        max(ingest_time) as latest_ingest_time
    from incremental_source
    group by
        station_name,
        timestamp_utc,
        parameter
),

normalized as (
    select
        'aqiin' as source,
        station_name,
        timestamp_utc,
        parameter,
        -- Standardize units to µg/m³ at 25°C, 1 atm
        case
            when raw_unit = 'ppb' then
                case
                    when parameter = 'o3' then raw_value * 1.96
                    when parameter = 'no2' then raw_value * 1.88
                    when parameter = 'so2' then raw_value * 2.62
                    else raw_value
                end
            when raw_unit = 'ppm' and parameter = 'co' then raw_value * 1145
            else raw_value
        end as value,
        aqi_reported,
        'µg/m³' as unit,
        quality_flag,
        latest_ingest_time as ingest_time
    from deduplicated
)

select * from normalized
