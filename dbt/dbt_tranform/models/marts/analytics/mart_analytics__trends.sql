{{ config(materialized='table') }}

with daily_data as (
    select
        unified_station_id,
        date,
        avg_aqi,
        avg_pm25,
        avg_pm10
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
    avg_pm25,
    avg_pm10,
    aqi_7d_ma,
    aqi_30d_ma,
    aqi_90d_ma,
    aqi_7d_ago,
    aqi_30d_ago,
    aqi_365d_ago,
    case
        when aqi_7d_ago is not null 
        then avg_aqi - aqi_7d_ago
        else null
    end as wow_change,
    case
        when aqi_30d_ago is not null 
        then avg_aqi - aqi_30d_ago
        else null
    end as mom_change,
    case
        when aqi_365d_ago is not null 
        then avg_aqi - aqi_365d_ago
        else null
    end as yoy_change,
    case
        when aqi_7d_ago is not null and aqi_7d_ago > 0
        then ((avg_aqi - aqi_7d_ago) / aqi_7d_ago) * 100
        else null
    end as wow_change_pct,
    case
        when aqi_30d_ago is not null and aqi_30d_ago > 0
        then ((avg_aqi - aqi_30d_ago) / aqi_30d_ago) * 100
        else null
    end as mom_change_pct,
    case
        when aqi_365d_ago is not null and aqi_365d_ago > 0
        then ((avg_aqi - aqi_365d_ago) / aqi_365d_ago) * 100
        else null
    end as yoy_change_pct
from with_lags

