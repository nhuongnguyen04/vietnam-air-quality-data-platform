{{ config(materialized='table') }}

with data_quality as (
    select
        unified_station_id,
        measurement_date as date,
        missing_data_rate,
        data_freshness_hours,
        freshness_score,
        avg_data_quality_score,
        overall_data_quality_score,
        is_outlier
    from {{ ref('int_data_quality') }}
),

station_uptime as (
    select
        unified_station_id,
        date,
        case
            when missing_data_rate <= 10 then 100
            when missing_data_rate <= 25 then 90
            when missing_data_rate <= 50 then 75
            when missing_data_rate <= 75 then 50
            else 25
        end as uptime_pct
    from data_quality
),

coverage_summary as (
    select
        date,
        unified_station_id,
        uptime_pct,
        (100 - missing_data_rate) as completeness_pct,
        data_freshness_hours,
        freshness_score,
        overall_data_quality_score as data_quality_score,
        is_outlier
    from data_quality dq
    left join station_uptime su on dq.unified_station_id = su.unified_station_id and dq.date = su.date
)

select
    date,
    unified_station_id,
    uptime_pct,
    completeness_pct,
    data_freshness_hours,
    freshness_score,
    data_quality_score,
    is_outlier,
    case
        when data_quality_score >= 80 then 'High'
        when data_quality_score >= 60 then 'Medium'
        when data_quality_score >= 40 then 'Low'
        else 'Very Low'
    end as quality_category
from coverage_summary

