{{ config(
    materialized='table'
) }}

WITH daily_stats AS (
    SELECT
        date,
        province,
        district,
        avg(pm25) as avg_pm25,
        avg(temp) as avg_temp,
        avg(humidity) as avg_humidity,
        avg(wind_speed) as avg_wind_speed
    FROM {{ ref('dm_aqi_weather_traffic_unified') }}
    GROUP BY 1, 2, 3
),

weather_modes AS (
    SELECT
        province,
        district,
        avgIf(pm25, wind_speed < 1.0) as stagnant_pm25_avg,
        avgIf(pm25, wind_speed >= 2.0) as dispersive_pm25_avg,
        countIf(wind_speed < 1.0) / count(*) as stagnant_air_probability
    FROM {{ ref('dm_aqi_weather_traffic_unified') }}
    GROUP BY 1, 2
),

final_metrics AS (
    SELECT
        d.province,
        d.district,
        avg(d.avg_pm25) as pm25_daily_avg,
        avg(d.avg_temp) as temp_daily_avg,
        avg(d.avg_humidity) as humidity_daily_avg,
        avg(d.avg_wind_speed) as wind_daily_avg,
        
        -- Custom Index: Higher index means higher risk (Low wind dispersal)
        CAST(coalesce(avg(d.avg_pm25) / nullif(avg(d.avg_wind_speed), 0), 0) AS Float32) as wind_dispersal_risk_index,
        
        -- Weather Influence %: (Stagnant - Dispersive) / Avg
        CAST(
            coalesce(
                (m.stagnant_pm25_avg - m.dispersive_pm25_avg) / nullif(avg(d.avg_pm25), 0), 0
            ) * 100 AS Float32
        ) as weather_influence_pct,
        
        m.stagnant_air_probability
    FROM daily_stats d
    LEFT JOIN weather_modes m ON d.province = m.province AND d.district = m.district
    GROUP BY 1, 2, m.stagnant_pm25_avg, m.dispersive_pm25_avg, m.stagnant_air_probability
)

SELECT
    *,
    rank() OVER (PARTITION BY province ORDER BY wind_dispersal_risk_index DESC) as weather_impact_rank_in_province,
    rank() OVER (ORDER BY wind_dispersal_risk_index DESC) as overall_weather_impact_rank
FROM final_metrics
