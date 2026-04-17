{{ config(
    engine='ReplacingMergeTree',
    unique_key='(ward_code, timestamp_utc, parameter)',
    order_by='(province, timestamp_utc, ward_code, parameter)',
    partition_by='toYYYYMM(timestamp_utc)'
) }}

with source as (
    select * from {{ source('openweather', 'raw_openweather_measurements') }}
),

deduplicated as (
    select
        *,
        row_number() over (
            partition by
                ward_code,
                timestamp_utc,
                parameter
            order by ingest_time desc
        ) as rn
    from source
),

cleaned as (
    select
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
        ingest_time
    from deduplicated
    where rn = 1
)

select * from cleaned
