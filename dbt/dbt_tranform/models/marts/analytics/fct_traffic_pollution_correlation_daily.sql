-- depends_on: {{ ref('dm_aqi_weather_traffic_unified') }}
{{ config(
    materialized='table'
) }}

WITH daily_stats AS (
    SELECT
        date,
        province,
        ward_code,
        -- Derive location_type
        case
            when province IN ('TP.Hà Nội', 'TP.Hồ Chí Minh', 'TP.Đà Nẵng', 'TP.Hải Phòng', 'TP.Cần Thơ') then 'Urban'
            when province IN ('Bình Dương', 'Đồng Nai', 'Bà Rịa - Vung Tau', 'Bắc Ninh', 'Bắc Giang', 'Hưng Yên', 'Quảng Ninh', 'Thái Nguyên') then 'Industrial'
            else 'Rural'
        end as location_type,
        avg(pm25) as avg_pm25,
        avg(congestion_index) as avg_congestion
    FROM {{ ref('dm_aqi_weather_traffic_unified') }}
    GROUP BY 1, 2, 3
),

baseline_stats AS (
    SELECT
        province,
        ward_code,
        avg(pm25) as background_pm25
    FROM {{ ref('dm_aqi_weather_traffic_unified') }}
    WHERE toHour(datetime_hour) BETWEEN 2 AND 4
    GROUP BY 1, 2
),

final_metrics AS (
    SELECT
        d.province,
        d.ward_code,
        d.location_type,
        avg(d.avg_pm25) as pm25_daily_avg,
        avg(d.avg_congestion) as congestion_daily_avg,
        -- Traffic Impact Score (P * C)
        CAST(avg(d.avg_pm25) * avg(d.avg_congestion) AS Float32) as traffic_pollution_impact_score,
        
        -- Traffic Contribution % (Simplified Source Apportionment)
        CAST(
            coalesce(
                (avg(d.avg_pm25) - b.background_pm25) / nullif(avg(d.avg_pm25), 0), 0
            ) * 100 AS Float32
        ) as traffic_contribution_pct
    FROM daily_stats d
    LEFT JOIN baseline_stats b ON d.province = b.province AND d.ward_code = b.ward_code
    GROUP BY 1, 2, 3, b.background_pm25
)

SELECT
    *,
    rank() OVER (PARTITION BY province ORDER BY traffic_pollution_impact_score DESC) as traffic_impact_rank_in_province,
    rank() OVER (ORDER BY traffic_pollution_impact_score DESC) as overall_traffic_impact_rank
FROM final_metrics
WHERE congestion_daily_avg > 0.005
