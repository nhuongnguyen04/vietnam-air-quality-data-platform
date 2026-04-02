{{ config(materialized='table') }}

-- AQI KPIs at day/week/month granularity.
-- All aggregates resolved in a single layer to avoid nested aggregate errors.
with daily_raw as (
    select
        date,
        unified_station_id,
        avg_aqi,
        max_aqi                                          AS max_aqi,
        min_aqi                                          AS min_aqi,
        if(avg_aqi > 100, 1, 0)                        AS exceeds_moderate,
        if(avg_aqi > 150, 1, 0)                        AS exceeds_sensitive,
        if(avg_aqi > 200, 1, 0)                        AS exceeds_unhealthy,
        if(avg_aqi > 300, 1, 0)                        AS exceeds_very_unhealthy,
        if(avg_aqi <= 50, 1, 0)                        AS is_good,
        if(avg_aqi > 50 and avg_aqi <= 100, 1, 0)    AS is_moderate,
        if(avg_aqi > 100 and avg_aqi <= 150, 1, 0)   AS is_sensitive,
        if(avg_aqi > 150 and avg_aqi <= 200, 1, 0)   AS is_unhealthy,
        if(avg_aqi > 200 and avg_aqi <= 300, 1, 0)   AS is_very_unhealthy,
        if(avg_aqi > 300, 1, 0)                        AS is_hazardous
    from {{ ref('mart_air_quality__daily_summary') }}
),

daily_kpis as (
    select
        date AS period_start,
        'day' as period_type,
        round(avg(avg_aqi), 2)                                        AS avg_aqi,
        max(max_aqi)                                                    AS max_aqi,
        min(min_aqi)                                                    AS min_aqi,
        sum(exceeds_moderate)                                          AS days_exceeding_moderate,
        sum(exceeds_sensitive)                                          AS days_exceeding_unhealthy_sensitive,
        sum(exceeds_unhealthy)                                          AS days_exceeding_unhealthy,
        sum(exceeds_very_unhealthy)                                     AS days_exceeding_very_unhealthy,
        count(distinct unified_station_id)                              AS station_count,
        -- AQI category distribution percentages
        round(100.0 * sum(is_good) / count(*), 1)                          AS pct_good,
        round(100.0 * sum(is_moderate) / count(*), 1)                    AS pct_moderate,
        round(100.0 * sum(is_sensitive) / count(*), 1)                   AS pct_unhealthy_sensitive,
        round(100.0 * sum(is_unhealthy) / count(*), 1)                   AS pct_unhealthy,
        round(100.0 * sum(is_very_unhealthy) / count(*), 1)             AS pct_very_unhealthy,
        round(100.0 * sum(is_hazardous) / count(*), 1)                  AS pct_hazardous
    from daily_raw
    group by date
),

weekly_kpis as (
    select
        toStartOfWeek(date)                                                    AS period_start,
        'week' as period_type,
        round(avg(avg_aqi), 2)                                                  AS avg_aqi,
        max(max_aqi)                                                            AS max_aqi,
        min(min_aqi)                                                            AS min_aqi,
        sum(exceeds_moderate)                                                   AS days_exceeding_moderate,
        sum(exceeds_sensitive)                                                  AS days_exceeding_unhealthy_sensitive,
        sum(exceeds_unhealthy)                                                  AS days_exceeding_unhealthy,
        sum(exceeds_very_unhealthy)                                             AS days_exceeding_very_unhealthy,
        count(distinct unified_station_id)                                      AS station_count,
        null::Nullable(Float64)                                                 AS pct_good,
        null::Nullable(Float64)                                                 AS pct_moderate,
        null::Nullable(Float64)                                                 AS pct_unhealthy_sensitive,
        null::Nullable(Float64)                                                 AS pct_unhealthy,
        null::Nullable(Float64)                                                 AS pct_very_unhealthy,
        null::Nullable(Float64)                                                 AS pct_hazardous
    from daily_raw
    group by toStartOfWeek(date)
),

monthly_kpis as (
    select
        toStartOfMonth(date)                                                     AS period_start,
        'month' as period_type,
        round(avg(avg_aqi), 2)                                                   AS avg_aqi,
        max(max_aqi)                                                             AS max_aqi,
        min(min_aqi)                                                             AS min_aqi,
        sum(exceeds_moderate)                                                    AS days_exceeding_moderate,
        sum(exceeds_sensitive)                                                    AS days_exceeding_unhealthy_sensitive,
        sum(exceeds_unhealthy)                                                   AS days_exceeding_unhealthy,
        sum(exceeds_very_unhealthy)                                             AS days_exceeding_very_unhealthy,
        count(distinct unified_station_id)                                      AS station_count,
        null::Nullable(Float64)                                                  AS pct_good,
        null::Nullable(Float64)                                                  AS pct_moderate,
        null::Nullable(Float64)                                                  AS pct_unhealthy_sensitive,
        null::Nullable(Float64)                                                  AS pct_unhealthy,
        null::Nullable(Float64)                                                  AS pct_very_unhealthy,
        null::Nullable(Float64)                                                  AS pct_hazardous
    from daily_raw
    group by toStartOfMonth(date)
)

select
    period_start                                                             AS period_date,
    period_type,
    avg_aqi,
    max_aqi,
    min_aqi,
    days_exceeding_moderate,
    days_exceeding_unhealthy_sensitive,
    days_exceeding_unhealthy,
    days_exceeding_very_unhealthy,
    station_count,
    pct_good,
    pct_moderate,
    pct_unhealthy_sensitive,
    pct_unhealthy,
    pct_very_unhealthy,
    pct_hazardous
from daily_kpis

union all

select
    period_start                                                             AS period_date,
    period_type,
    avg_aqi,
    max_aqi,
    min_aqi,
    days_exceeding_moderate,
    days_exceeding_unhealthy_sensitive,
    days_exceeding_unhealthy,
    days_exceeding_very_unhealthy,
    station_count,
    pct_good,
    pct_moderate,
    pct_unhealthy_sensitive,
    pct_unhealthy,
    pct_very_unhealthy,
    pct_hazardous
from weekly_kpis

union all

select
    period_start                                                             AS period_date,
    period_type,
    avg_aqi,
    max_aqi,
    min_aqi,
    days_exceeding_moderate,
    days_exceeding_unhealthy_sensitive,
    days_exceeding_unhealthy,
    days_exceeding_very_unhealthy,
    station_count,
    pct_good,
    pct_moderate,
    pct_unhealthy_sensitive,
    pct_unhealthy,
    pct_very_unhealthy,
    pct_hazardous
from monthly_kpis
