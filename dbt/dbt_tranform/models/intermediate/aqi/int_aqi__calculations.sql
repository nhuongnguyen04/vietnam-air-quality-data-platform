{{ config(
    materialized='incremental',
    engine='ReplacingMergeTree',
    unique_key='(source, ward_code, timestamp_utc, parameter)',
    order_by='(province, timestamp_utc, ward_code, source, parameter)',
    partition_by='toYYYYMM(timestamp_utc)'
) }}

with measurements as (
    select * from {{ ref('int_core__measurements_unified') }}
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

calculated as (
    select
        *,
        {{ calculate_aqi('parameter', 'value_us_standard') }} as aqi_us,
        {{ calculate_aqi_vn('parameter', 'value') }}           as aqi_vn
    from normalized_for_us
),

with_max as (
    select
        *,
        max(aqi_us) over (partition by province, ward_code, timestamp_utc) as max_aqi_us_in_hour,
        max(aqi_vn) over (partition by province, ward_code, timestamp_utc) as max_aqi_vn_in_hour
    from calculated
)

select
    *,
    case when aqi_us = max_aqi_us_in_hour then true else false end as is_dominant_us,
    case when aqi_vn = max_aqi_vn_in_hour then true else false end as is_dominant_vn
from with_max
