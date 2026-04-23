{{ config(
    materialized='incremental',
    engine='ReplacingMergeTree',
    incremental_strategy='append',
    unique_key='(source, ward_code, timestamp_utc, parameter)',
    order_by='(province, timestamp_utc, ward_code, source, parameter)',
    partition_by='toYYYYMM(timestamp_utc)',
    query_settings={
        'max_bytes_before_external_sort': 100000000,
        'max_bytes_before_external_group_by': 100000000
    }
) }}

with measurements as (
    select
        source,
        measurement_dedup_key,
        ward_code,
        province,
        latitude,
        longitude,
        timestamp_utc,
        parameter,
        value,
        source_weight,
        aqi_reported,
        quality_flag,
        ingest_time,
        region_3,
        region_8
    from {{ ref('int_core__measurements_unified') }}
    {% if is_incremental() %}
    -- Process last 6 hours to ensure window functions capture all pollutants for an hour
    where timestamp_utc >= (select max(timestamp_utc) - interval 6 hour from {{ this }})
    {% endif %}
),

normalized_for_us as (
    select
        *,
        -- Convert units for US AQI macros (which expect ppm for CO and ppb for others)
        -- Raw values are assumed to be µg/m³
        case
            when parameter = 'co' then value / 1145.0
            when parameter = 'no2' then value * 0.532
            when parameter = 'so2' then value * 0.382
            when parameter = 'o3' then value * 0.510
            else value
        end as value_us_standard
    from measurements
),

aqi_rows as (
    select
        source,
        measurement_dedup_key,
        ward_code,
        province,
        latitude,
        longitude,
        timestamp_utc,
        parameter,
        value,
        source_weight,
        aqi_reported,
        quality_flag,
        ingest_time,
        region_3,
        region_8,
        value_us_standard,
        {{ calculate_aqi('parameter', 'value_us_standard') }} as aqi_us,
        {{ calculate_aqi_vn('parameter', 'value') }}           as aqi_vn
    from normalized_for_us
    where parameter in ('pm25', 'pm10', 'co', 'no2', 'so2', 'o3')
),

aqi_with_max as (
    select
        source,
        measurement_dedup_key,
        ward_code,
        province,
        latitude,
        longitude,
        timestamp_utc,
        parameter,
        value,
        source_weight,
        aqi_reported,
        quality_flag,
        ingest_time,
        region_3,
        region_8,
        value_us_standard,
        aqi_us,
        aqi_vn,
        max(aqi_us) over (partition by province, ward_code, timestamp_utc) as max_aqi_us_in_hour,
        max(aqi_vn) over (partition by province, ward_code, timestamp_utc) as max_aqi_vn_in_hour
    from aqi_rows
),

aqi_final as (
    select
        source,
        measurement_dedup_key,
        ward_code,
        province,
        latitude,
        longitude,
        timestamp_utc,
        parameter,
        value,
        source_weight,
        aqi_reported,
        quality_flag,
        ingest_time,
        region_3,
        region_8,
        value_us_standard,
        aqi_us,
        aqi_vn,
        max_aqi_us_in_hour,
        max_aqi_vn_in_hour,
        aqi_us = max_aqi_us_in_hour as is_dominant_us,
        aqi_vn = max_aqi_vn_in_hour as is_dominant_vn
    from aqi_with_max
),

other_rows as (
    select
        source,
        measurement_dedup_key,
        ward_code,
        province,
        latitude,
        longitude,
        timestamp_utc,
        parameter,
        value,
        source_weight,
        aqi_reported,
        quality_flag,
        ingest_time,
        region_3,
        region_8,
        value_us_standard,
        cast(null, 'Nullable(Float64)') as aqi_us,
        cast(null, 'Nullable(Float64)') as aqi_vn,
        cast(null, 'Nullable(Float64)') as max_aqi_us_in_hour,
        cast(null, 'Nullable(Float64)') as max_aqi_vn_in_hour,
        false as is_dominant_us,
        false as is_dominant_vn
    from normalized_for_us
    where parameter not in ('pm25', 'pm10', 'co', 'no2', 'so2', 'o3')
)

select
    source,
    measurement_dedup_key,
    ward_code,
    province,
    latitude,
    longitude,
    timestamp_utc,
    parameter,
    value,
    source_weight,
    aqi_reported,
    quality_flag,
    ingest_time,
    region_3,
    region_8,
    value_us_standard,
    aqi_us,
    aqi_vn,
    max_aqi_us_in_hour,
    max_aqi_vn_in_hour,
    is_dominant_us,
    is_dominant_vn
from aqi_final

union all

select
    source,
    measurement_dedup_key,
    ward_code,
    province,
    latitude,
    longitude,
    timestamp_utc,
    parameter,
    value,
    source_weight,
    aqi_reported,
    quality_flag,
    ingest_time,
    region_3,
    region_8,
    value_us_standard,
    aqi_us,
    aqi_vn,
    max_aqi_us_in_hour,
    max_aqi_vn_in_hour,
    is_dominant_us,
    is_dominant_vn
from other_rows
