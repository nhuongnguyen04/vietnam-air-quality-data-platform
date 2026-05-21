{{ config(
    materialized='table',
    engine='MergeTree',
    order_by='national_risk_rank'
) }}

-- =============================================================================
-- dm_regional_health_risk_ranking
-- Dual-ranking model: ranks provinces by both raw pollution (PM2.5) and
-- population-weighted exposure. Uses WHO 2021 AQG and QCVN 05:2023 thresholds.
--
-- References:
--   - WHO Global Air Quality Guidelines 2021 (annual PM2.5 = 5 µg/m³)
--   - QCVN 05:2023/BTNMT (annual PM2.5 = 25 µg/m³, tightened to 45 from 2026)
--   - Population: seed/vietnam_wards_with_osm.csv (post-NQ 202/2025, 34 provinces)
-- =============================================================================

WITH provincial_stats AS (
    SELECT
        p.province,
        any(p.region_3) as region_3,
        any(p.region_8) as region_8,
        any(pop.total_population) as population,
        round(avg(p.pm25_avg), 2) as time_weighted_pm25,
        round(avg(p.confidence_score), 2) as confidence_score,
        topK(1)(p.confidence_level)[1] as confidence_level,
        topK(1)(p.source_mix)[1] as source_mix,
        avg(p.pm25_avg) * any(pop.total_population) / 1000000.0 as total_exposure_metric
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
    confidence_score,
    confidence_level,
    source_mix,
    CAST(total_exposure_metric AS Float32) as total_exposure_index_m,

    -- Pollution-based risk: uses WHO/QCVN science-based thresholds on PM2.5
    case
        when time_weighted_pm25 > 50  then 'CRITICAL'        -- Exceeds QCVN daily limit
        when time_weighted_pm25 > 25  then 'HIGH RISK'       -- Exceeds QCVN annual limit
        when time_weighted_pm25 > 15  then 'MODERATE'         -- Exceeds WHO 24h guideline
        else 'LOW'                                            -- Below WHO guideline
    end as risk_category,

    -- Exposure-based risk: population-weighted (for public health prioritization)
    case
        when total_exposure_metric > 200 then 'CRITICAL'
        when total_exposure_metric > 100 then 'HIGH RISK'
        when total_exposure_metric > 25  then 'MODERATE'
        else 'LOW'
    end as exposure_risk_category,

    -- Dual rankings
    rank() OVER (ORDER BY time_weighted_pm25 DESC)       as pollution_rank,
    rank() OVER (ORDER BY total_exposure_metric DESC)    as exposure_rank,

    -- Primary ranking: pollution-based (most intuitive for users)
    rank() OVER (ORDER BY time_weighted_pm25 DESC)       as national_risk_rank,

    -- Data lineage note
    'Population: seed post-NQ202/2025 (34 provinces). PM2.5: 30-day avg.' as data_note
FROM provincial_stats
