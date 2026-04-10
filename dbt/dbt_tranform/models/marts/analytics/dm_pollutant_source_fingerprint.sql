{{ config(
    materialized='incremental',
    engine='MergeTree',
    order_by='(province, date)',
    partition_by='toYYYYMM(date)'
) }}

with hourly_data as (
    select
        date,
        province,
        district,
        -- Need both to calculate ratio
        pm25_value,
        pm10_value,
        ingest_time
    from {{ ref('fct_air_quality_summary_hourly') }}
    where pm25_value > 0 and pm10_value > 0
    {% if is_incremental() %}
    and ingest_time > (select max(ingest_time) from {{ this }})
    {% endif %}
),

source_calc as (
    select
        date,
        province,
        district,
        avg(pm25_value) as avg_pm25,
        avg(pm10_value) as avg_pm10,
        avg(pm25_value) / avg(pm10_value) as pm25_pm10_ratio,
        case
            when avg(pm25_value) / avg(pm10_value) > 0.6 then 'Combustion/Traffic'
            when avg(pm25_value) / avg(pm10_value) < 0.4 then 'Dust/Construction'
            else 'Mixed'
        end as probable_source,
        max(ingest_time) as ingest_time
    from hourly_data
    group by date, province, district
)

select * from source_calc
