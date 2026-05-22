{{ config(
    materialized='table',
    tags=['pipeline_v2']
) }}

with active_aqiin as (
    select 
        province,
        ward_code,
        max(datetime_hour) as latest_hour
    from {{ ref('int_aqiin__ward_hourly') }}
    where datetime_hour >= now() - interval 7 day
    group by province, ward_code
),

active_ow as (
    select 
        province,
        ward_code,
        max(datetime_hour) as latest_hour
    from {{ ref('int_ow__ward_hourly') }}
    where datetime_hour >= now() - interval 7 day
    group by province, ward_code
),

admin_province_totals as (
    select 
        province,
        count(distinct ward_code) as total_ward_count
    from {{ ref('dim_administrative_units') }}
    where province is not null and province != ''
    group by province
),

providential_coverage as (
    select
        t.province as province,
        t.total_ward_count as total_ward_count,
        coalesce(count(distinct a.ward_code), 0) as aqiin_ward_count,
        coalesce(count(distinct o.ward_code), 0) as ow_ward_count,
        coalesce(count(distinct a.ward_code), 0) * 100.0 / nullIf(t.total_ward_count, 0) as aqiin_coverage_pct,
        max(a.latest_hour) as aqiin_latest_hour,
        max(o.latest_hour) as ow_latest_hour
    from admin_province_totals t
    left join active_aqiin a on t.province = a.province
    left join active_ow o on t.province = o.province
    group by t.province, t.total_ward_count
)

select * from providential_coverage
