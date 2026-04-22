{{ config(
    materialized='incremental',
    engine='ReplacingMergeTree',
    unique_key='(ward_code, timestamp_utc, parameter)',
    order_by='(province, timestamp_utc, ward_code, parameter)',
    partition_by='toYYYYMM(timestamp_utc)'
) }}

with incremental_source as (
    select
        ward_code,
        ward_name,
        province_name,
        latitude,
        longitude,
        timestamp_utc,
        parameter,
        value,
        aqi_reported,
        ingest_time
    from {{ source('openweather', 'raw_openweather_measurements') }}
    {% if is_incremental() %}
    where ingest_time >= (
        select max(ingest_time) - interval 24 hour
        from {{ this }}
    )
    {% endif %}
),

prepared as (
    select
        {{ dbt_utils.generate_surrogate_key([
            "concat('ward_code:', ward_code)",
            "concat('timestamp_utc:', toString(timestamp_utc))",
            "concat('parameter:', parameter)"
        ]) }} as dedup_key,
        ward_code,
        ward_name,
        province_name,
        latitude,
        longitude,
        timestamp_utc,
        parameter,
        value,
        aqi_reported,
        ingest_time
    from incremental_source
),

deduplicated as (
    select
        dedup_key,
        argMax(ward_code, ingest_time) as ward_code,
        argMax(ward_name, ingest_time) as ward_name,
        argMax(province_name, ingest_time) as province_name,
        argMax(latitude, ingest_time) as latitude,
        argMax(longitude, ingest_time) as longitude,
        argMax(timestamp_utc, ingest_time) as timestamp_utc,
        argMax(parameter, ingest_time) as parameter,
        argMax(value, ingest_time) as value,
        argMax(aqi_reported, ingest_time) as aqi_reported,
        max(ingest_time) as latest_ingest_time
    from prepared
    group by dedup_key
),

cleaned as (
    select
        dedup_key,
        'openweather' as source,
        ward_code,
        ward_name,
        province_name as province,
        latitude,
        longitude,
        timestamp_utc,
        parameter,
        value,
        aqi_reported,
        'valid' as quality_flag,
        latest_ingest_time as ingest_time
    from deduplicated
)

select * from cleaned
