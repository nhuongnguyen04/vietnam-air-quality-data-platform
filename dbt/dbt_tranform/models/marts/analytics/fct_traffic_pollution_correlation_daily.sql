{{ config(
    materialized='table',
    schema=var('analytics_schema', 'analytics')
) }}

WITH daily_stats AS (
    SELECT
        date,
        province,
        station_name,
        location_type,
        avg(pm25) as avg_pm25,
        avg(congestion_index) as avg_congestion,
        max(pm25) as peak_pm25,
        max(congestion_index) as peak_congestion
    FROM {{ ref('dm_aqi_weather_traffic_unified') }}
    GROUP BY 1, 2, 3, 4
)

SELECT
    province,
    location_type,
    count(DISTINCT station_name) as stations_count,
    
    -- Correlation indicator: How much pollution varies with congestion at the aggregate level
    coalesce(avg(avg_pm25), 0) as pm25_daily_avg,
    coalesce(avg(avg_congestion), 0) as congestion_daily_avg,
    
    -- Pollution per congestion unit (Exploitation metric)
    CAST(coalesce(avg(avg_pm25) / nullif(avg(avg_congestion), 0), 0) AS Float32) as pollution_density_index,
    
    -- Rank provinces by pollution within their geography type
    rank() OVER (PARTITION BY location_type ORDER BY avg(avg_pm25) DESC) as pollution_rank_in_type

FROM daily_stats
GROUP BY 1, 2
