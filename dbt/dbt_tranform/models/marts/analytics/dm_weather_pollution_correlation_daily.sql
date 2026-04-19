{{ config(
    materialized='incremental',
    engine='MergeTree',
    order_by='(province, date)',
    partition_by='toYYYYMM(date)'
) }}

WITH source_data AS (
    SELECT
        date,
        province,
        ward_code,
        region_3,
        region_8,
        pm25,
        temperature as temp,
        humidity,
        wind_speed
    FROM {{ ref('fct_aqi_weather_traffic_unified') }}
    {% if is_incremental() %}
    WHERE date >= (select max(date) - interval 1 day from {{ this }})
    {% endif %}
),

daily_stats AS (
    SELECT
        date,
        province,
        ward_code,
        region_3,
        region_8,
        avg(pm25) as avg_pm25,
        avg(temp) as avg_temp,
        avg(humidity) as avg_humidity,
        avg(wind_speed) as avg_wind_speed
    FROM source_data
    GROUP BY 1, 2, 3, 4, 5
),

weather_modes AS (
    SELECT
        date,
        province,
        ward_code,
        avgIf(pm25, wind_speed < 1.0) as stagnant_pm25_avg,
        avgIf(pm25, wind_speed >= 2.0) as dispersive_pm25_avg,
        countIf(wind_speed < 1.0) / count(*) as stagnant_air_probability
    FROM source_data
    GROUP BY 1, 2, 3
),

final_metrics AS (
    SELECT
        d.date,
        d.province,
        d.ward_code,
        d.region_3,
        d.region_8,
        d.avg_pm25 as pm25_daily_avg,
        d.avg_temp as temp_daily_avg,
        d.avg_humidity as humidity_daily_avg,
        d.avg_wind_speed as wind_daily_avg,
        
        -- Custom Index: Higher index means higher risk (Low wind dispersal)
        CAST(coalesce(d.avg_pm25 / nullif(d.avg_wind_speed, 0), 0) AS Float32) as wind_dispersal_risk_index,
        
        -- Weather Influence %
        CAST(
            coalesce(
                (m.stagnant_pm25_avg - m.dispersive_pm25_avg) / nullif(d.avg_pm25, 0), 0
            ) * 100 AS Float32
        ) as weather_influence_pct,
        
        m.stagnant_air_probability
    FROM daily_stats d
    LEFT JOIN weather_modes m 
        ON d.date = m.date 
        AND d.province = m.province 
        AND d.ward_code = m.ward_code
)

SELECT
    *,
    now() as dbt_updated_at
FROM final_metrics
