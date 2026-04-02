{{ config(materialized='table') }}

-- Trend analysis: 7/30/90-day moving averages and WoW/MoM/YoY comparisons
with daily_data as (
    select
        unified_station_id,
        date,
        avg_aqi
    from {{ ref('mart_air_quality__daily_summary') }}
),

with_ma as (
    select
        *,
        avg(avg_aqi) over (
            partition by unified_station_id
            order by date
            rows between 6 preceding and current row
        ) as aqi_7d_ma,
        avg(avg_aqi) over (
            partition by unified_station_id
            order by date
            rows between 29 preceding and current row
        ) as aqi_30d_ma,
        avg(avg_aqi) over (
            partition by unified_station_id
            order by date
            rows between 89 preceding and current row
        ) as aqi_90d_ma
    from daily_data
),

with_lags as (
    select
        *,
        lag(avg_aqi, 7) over (
            partition by unified_station_id
            order by date
        ) as aqi_7d_ago,
        lag(avg_aqi, 30) over (
            partition by unified_station_id
            order by date
        ) as aqi_30d_ago,
        lag(avg_aqi, 365) over (
            partition by unified_station_id
            order by date
        ) as aqi_365d_ago
    from with_ma
)

select
    unified_station_id,
    date,
    avg_aqi as aqi_value,
    aqi_7d_ma,
    aqi_30d_ma,
    aqi_90d_ma,
    aqi_7d_ago,
    aqi_30d_ago,
    aqi_365d_ago,
    -- WoW / MoM / YoY absolute change
    if(aqi_7d_ago is not null, avg_aqi - aqi_7d_ago, null)       AS wow_change,
    if(aqi_30d_ago is not null, avg_aqi - aqi_30d_ago, null)     AS mom_change,
    if(aqi_365d_ago is not null, avg_aqi - aqi_365d_ago, null)   AS yoy_change,
    -- WoW / MoM / YoY percentage change
    if(aqi_7d_ago is not null and aqi_7d_ago > 0,
        ((avg_aqi - aqi_7d_ago) / aqi_7d_ago) * 100, null)          AS wow_change_pct,
    if(aqi_30d_ago is not null and aqi_30d_ago > 0,
        ((avg_aqi - aqi_30d_ago) / aqi_30d_ago) * 100, null)        AS mom_change_pct,
    if(aqi_365d_ago is not null and aqi_365d_ago > 0,
        ((avg_aqi - aqi_365d_ago) / aqi_365d_ago) * 100, null)      AS yoy_change_pct
from with_lags
