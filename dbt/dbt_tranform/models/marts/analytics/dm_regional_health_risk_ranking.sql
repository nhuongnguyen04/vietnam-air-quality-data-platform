{{ config(
    materialized='table',
    engine='MergeTree',
    order_by='national_risk_rank'
) }}

WITH provincial_stats AS (
    SELECT
        p.province,
        any(p.region_3) as region_3,
        any(p.region_8) as region_8,
        any(pop.total_population) as population,
        round(avg(p.pm25_prov_avg), 2) as time_weighted_pm25,
        avg(p.pm25_prov_avg) * any(pop.total_population) / 1000000.0 as total_exposure_metric
    FROM {{ ref('fct_air_quality_province_level_daily') }} p
    LEFT JOIN {{ ref('stg_core__population') }} pop ON p.province = pop.location_name
    -- Use last 30 days for ranking to be representative
    WHERE p.date >= today() - interval 30 day
    GROUP BY p.province
)

SELECT
    province,
    region_3,
    region_8,
    population,
    time_weighted_pm25,
    CAST(total_exposure_metric AS Float32) as total_exposure_index_m,
    
    -- Risk Classification
    case
        when total_exposure_metric > 200 then 'CRITICAL'
        when total_exposure_metric > 100 then 'HIGH RISK'
        when total_exposure_metric > 25 then 'MODERATE'
        else 'LOW'
    end as risk_category,
    
    rank() OVER (ORDER BY total_impact_metric DESC) as national_risk_rank
FROM (
    select 
        *,
        total_exposure_metric as total_impact_metric
    from provincial_stats
)
