{{ config(
    materialized='table',
    schema=var('analytics_schema', 'analytics')
) }}

WITH provincial_daily AS (
    SELECT
        date,
        province,
        avg(pm25) as pm25_avg,
        avg(aqi_vn) as aqi_avg,
        max(provincial_population) as population
    FROM {{ ref('dm_aqi_weather_traffic_unified') }}
    GROUP BY 1, 2
)

SELECT
    province,
    population,
    round(avg(pm25_avg), 2) as time_weighted_pm25,
    
    -- Real-world impact: Total PM2.5 "Mass" exposed to the population
    CAST(coalesce(avg(pm25_avg) * population / 1000000.0, 0) AS Float32) as total_exposure_index_m,
    
    -- Risk Classification
    case
        when avg(pm25_avg) > 50 then 'CRITICAL'
        when avg(pm25_avg) > 35 then 'HIGH RISK'
        when avg(pm25_avg) > 15 then 'MODERATE'
        else 'LOW'
    end as risk_category,
    
    -- Ranking based on total impact
    rank() OVER (ORDER BY (avg(pm25_avg) * population) DESC) as national_risk_rank

FROM provincial_daily
GROUP BY 1, 2
