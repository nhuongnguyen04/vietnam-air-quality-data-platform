{{ config(
    materialized='table'
) }}

WITH provincial_daily AS (
    SELECT
        date,
        province,
        pm25_prov_avg as pm25_avg,
        prov_avg_aqi_vn as aqi_avg
    FROM {{ ref('fct_air_quality_province_level_daily') }}
),

pop AS (
    SELECT location_name, total_population 
    FROM {{ ref('stg_core__population') }}
)

SELECT
    p.province,
    pop.total_population as population,
    round(avg(p.pm25_avg), 2) as time_weighted_pm25,
    
    -- Real-world impact: Total PM2.5 "Mass" exposed to the population
    CAST(coalesce(avg(p.pm25_avg) * pop.total_population / 1000000.0, 0) AS Float32) as total_exposure_index_m,
    
    -- Risk Classification based on Total Exposure (Population Impact)
    case
        when (avg(p.pm25_avg) * pop.total_population / 1000000.0) > 200 then 'CRITICAL'
        when (avg(p.pm25_avg) * pop.total_population / 1000000.0) > 100 then 'HIGH RISK'
        when (avg(p.pm25_avg) * pop.total_population / 1000000.0) > 25 then 'MODERATE'
        else 'LOW'
    end as risk_category,
    
    -- Ranking based on total impact
    rank() OVER (ORDER BY (avg(p.pm25_avg) * pop.total_population) DESC) as national_risk_rank

FROM provincial_daily p
LEFT JOIN pop ON p.province = pop.location_name
GROUP BY 1, 2
