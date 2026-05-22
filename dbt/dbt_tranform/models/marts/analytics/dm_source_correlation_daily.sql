{{ config(
    materialized='table',
    tags=['pipeline_v2']
) }}

with aqiin_prov as (
    select 
        date, 
        province,
        avg(avg_aqi_vn) as aqiin_aqi,
        avg(pm25_avg) as aqiin_pm25,
        count(distinct ward_code) as aqiin_wards
    from {{ ref('int_aqiin__ward_daily') }}
    group by date, province
),

ow_prov as (
    select 
        date, 
        province,
        avg(avg_aqi_vn) as ow_aqi,
        avg(pm25_avg) as ow_pm25,
        count(distinct ward_code) as ow_wards
    from {{ ref('int_ow__ward_daily') }}
    group by date, province
),

joined as (
    select
        coalesce(a.date, o.date) as date,
        coalesce(a.province, o.province) as province,

        -- Value per source
        a.aqiin_aqi as aqiin_aqi,
        o.ow_aqi as ow_aqi,
        a.aqiin_pm25 as aqiin_pm25,
        o.ow_pm25 as ow_pm25,

        -- Bias and MAE
        a.aqiin_aqi - o.ow_aqi as aqi_bias,
        abs(a.aqiin_aqi - o.ow_aqi) as aqi_mae,

        -- Percentage difference
        (a.aqiin_aqi - o.ow_aqi) / nullIf(o.ow_aqi, 0) * 100 as aqi_pct_diff,

        -- Category agreement
        case
            when a.aqiin_aqi > 150 and o.ow_aqi > 150 then 'both_unhealthy'
            when a.aqiin_aqi <= 50 and o.ow_aqi <= 50 then 'both_good'
            when a.aqiin_aqi is null or o.ow_aqi is null then 'one_source_only'
            else 'disagree'
        end as category_agreement,

        a.aqiin_wards as aqiin_wards,
        o.ow_wards as ow_wards,
        a.aqiin_wards > 0 as has_ground_data,
        a.aqiin_wards * 100.0 / nullIf(o.ow_wards, 0) as ground_coverage_pct

    from aqiin_prov a
    full join ow_prov o on a.date = o.date and a.province = o.province
)

select * from joined
