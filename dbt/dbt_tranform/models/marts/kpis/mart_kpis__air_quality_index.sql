{{ config(materialized='table') }}

with daily_summary as (
    select
        date,
        unified_station_id,
        avg_aqi as daily_avg_aqi,
        max_aqi,
        min_aqi,
        exceedance_count
    from {{ ref('mart_air_quality__daily_summary') }}
),

daily_kpis as (
    select
        date,
        'day' as period_type,
        avg(daily_avg_aqi) as avg_aqi,
        max(max_aqi) as max_aqi,
        min(min_aqi) as min_aqi,
        countIf(daily_avg_aqi > 100) as days_exceeding_moderate,
        countIf(daily_avg_aqi > 150) as days_exceeding_unhealthy_sensitive,
        countIf(daily_avg_aqi > 200) as days_exceeding_unhealthy,
        countIf(daily_avg_aqi > 300) as days_exceeding_very_unhealthy,
        sum(exceedance_count) as total_exceedance_count,
        count(distinct unified_station_id) as station_count
    from daily_summary
    group by date
),

weekly_kpis as (
    select
        toStartOfWeek(date) as week_start,
        'week' as period_type,
        avg(daily_avg_aqi) as avg_aqi,
        max(max_aqi) as max_aqi,
        min(min_aqi) as min_aqi,
        countIf(daily_avg_aqi > 100) as days_exceeding_moderate,
        countIf(daily_avg_aqi > 150) as days_exceeding_unhealthy_sensitive,
        countIf(daily_avg_aqi > 200) as days_exceeding_unhealthy,
        countIf(daily_avg_aqi > 300) as days_exceeding_very_unhealthy,
        sum(exceedance_count) as total_exceedance_count,
        count(distinct unified_station_id) as station_count
    from daily_summary
    group by toStartOfWeek(date)
),

monthly_kpis as (
    select
        toStartOfMonth(date) as month_start,
        'month' as period_type,
        avg(daily_avg_aqi) as avg_aqi,
        max(max_aqi) as max_aqi,
        min(min_aqi) as min_aqi,
        countIf(daily_avg_aqi > 100) as days_exceeding_moderate,
        countIf(daily_avg_aqi > 150) as days_exceeding_unhealthy_sensitive,
        countIf(daily_avg_aqi > 200) as days_exceeding_unhealthy,
        countIf(daily_avg_aqi > 300) as days_exceeding_very_unhealthy,
        sum(exceedance_count) as total_exceedance_count,
        count(distinct unified_station_id) as station_count
    from daily_summary
    group by toStartOfMonth(date)
),

aqi_distribution as (
    select
        date,
        countIf(daily_avg_aqi <= 50) as count_good,
        countIf(daily_avg_aqi > 50 and daily_avg_aqi <= 100) as count_moderate,
        countIf(daily_avg_aqi > 100 and daily_avg_aqi <= 150) as count_unhealthy_sensitive,
        countIf(daily_avg_aqi > 150 and daily_avg_aqi <= 200) as count_unhealthy,
        countIf(daily_avg_aqi > 200 and daily_avg_aqi <= 300) as count_very_unhealthy,
        countIf(daily_avg_aqi > 300) as count_hazardous,
        count(*) as total_days
    from daily_summary
    group by date
),

with_distribution_pct as (
    select
        d.*,
        (d.count_good::Float64 / d.total_days) * 100 as pct_good,
        (d.count_moderate::Float64 / d.total_days) * 100 as pct_moderate,
        (d.count_unhealthy_sensitive::Float64 / d.total_days) * 100 as pct_unhealthy_sensitive,
        (d.count_unhealthy::Float64 / d.total_days) * 100 as pct_unhealthy,
        (d.count_very_unhealthy::Float64 / d.total_days) * 100 as pct_very_unhealthy,
        (d.count_hazardous::Float64 / d.total_days) * 100 as pct_hazardous
    from aqi_distribution d
)

select
    date as period_date,
    'day' as period_type,
    k.avg_aqi,
    k.max_aqi,
    k.min_aqi,
    k.days_exceeding_moderate,
    k.days_exceeding_unhealthy_sensitive,
    k.days_exceeding_unhealthy,
    k.days_exceeding_very_unhealthy,
    k.total_exceedance_count,
    k.station_count,
    d.pct_good,
    d.pct_moderate,
    d.pct_unhealthy_sensitive,
    d.pct_unhealthy,
    d.pct_very_unhealthy,
    d.pct_hazardous
from daily_kpis k
left join with_distribution_pct d on k.date = d.date

union all

select
    week_start as period_date,
    'week' as period_type,
    avg_aqi,
    max_aqi,
    min_aqi,
    days_exceeding_moderate,
    days_exceeding_unhealthy_sensitive,
    days_exceeding_unhealthy,
    days_exceeding_very_unhealthy,
    total_exceedance_count,
    station_count,
    null as pct_good,
    null as pct_moderate,
    null as pct_unhealthy_sensitive,
    null as pct_unhealthy,
    null as pct_very_unhealthy,
    null as pct_hazardous
from weekly_kpis

union all

select
    month_start as period_date,
    'month' as period_type,
    avg_aqi,
    max_aqi,
    min_aqi,
    days_exceeding_moderate,
    days_exceeding_unhealthy_sensitive,
    days_exceeding_unhealthy,
    days_exceeding_very_unhealthy,
    total_exceedance_count,
    station_count,
    null as pct_good,
    null as pct_moderate,
    null as pct_unhealthy_sensitive,
    null as pct_unhealthy,
    null as pct_very_unhealthy,
    null as pct_hazardous
from monthly_kpis

