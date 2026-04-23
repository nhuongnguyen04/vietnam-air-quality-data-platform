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
        ward_code,
        region_3,
        region_8,
        pm25_avg as pm25_value,
        pm10_avg as pm10_value,
        last_ingested_at as ingest_time
    from {{ ref('fct_air_quality_ward_level_hourly') }}
    where pm25_avg > 0 and pm10_avg > 0
    {% if is_incremental() %}
    and last_ingested_at > (select max(ingest_time) from {{ this }})
    {% endif %}
),

source_calc as (
    select
        date,
        province,
        ward_code,
        any(region_3) as region_3,
        any(region_8) as region_8,
        avg(pm25_value) as pm25,
        avg(pm10_value) as pm10,
        avg(pm25_value) / nullIf(avg(pm10_value), 0) as pm25_pm10_ratio,
        case
            when avg(pm25_value) / nullIf(avg(pm10_value), 0) > 0.6 then 'Combustion/Traffic'
            when avg(pm25_value) / nullIf(avg(pm10_value), 0) < 0.4 then 'Dust/Construction'
            else 'Mixed'
        end as probable_source,
        max(ingest_time) as ingest_time
    from hourly_data
    group by date, province, ward_code
)

select * from source_calc
