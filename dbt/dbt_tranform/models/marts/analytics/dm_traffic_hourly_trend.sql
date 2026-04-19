{{ config(
    materialized='incremental',
    engine='ReplacingMergeTree',
    unique_key='(province, ward_code, datetime_hour)',
    order_by='(province, date, assumeNotNull(ward_code))',
    partition_by='toYYYYMM(date)'
) }}

with unified as (
    select
        datetime_hour,
        date,
        province,
        ward_code,
        region_3,
        region_8,
        congestion_index,
        pm25,
        co,
        traffic_pollution_impact_score,
        traffic_contribution_pct
    from {{ ref('fct_aqi_weather_traffic_unified') }}
    where congestion_index is not null -- Only wards with traffic data
    {% if is_incremental() %}
    and datetime_hour >= (select max(datetime_hour) - interval 1 day from {{ this }})
    {% endif %}
)

select * from unified
