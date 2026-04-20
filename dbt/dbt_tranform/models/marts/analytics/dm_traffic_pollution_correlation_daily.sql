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
        datetime_hour,
        pm25,
        pm10,
        congestion_index
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
        -- Location classification logic
        case
            when province IN ('TP.Hà Nội', 'TP.Hồ Chí Minh', 'TP.Đà Nẵng', 'TP.Hải Phòng', 'TP.Cần Thơ') then 'Urban'
            when province IN ('Bình Dương', 'Đồng Nai', 'Bà Rịa - Vũng Tàu', 'Bắc Ninh', 'Bắc Giang', 'Hưng Yên', 'Quảng Ninh', 'Thái Nguyên') then 'Industrial'
            else 'Rural'
        end as location_type,
        avg(pm25) as avg_pm25,
        avg(pm10) as avg_pm10,
        avg(congestion_index) as avg_congestion,
        sum(pm25) as sum_pm25,
        sum(pm10) as sum_pm10,
        count(*) as total_hours
    FROM source_data
    GROUP BY 1, 2, 3, 4, 5, 6
),

baseline_stats AS (
    SELECT
        date,
        province,
        ward_code,
        avg(pm25) as background_pm25,
        avg(pm10) as background_pm10,
        sum(pm25) as background_pm25_sum,
        sum(pm10) as background_pm10_sum,
        count(*) as background_hours
    FROM source_data
    WHERE toHour(datetime_hour) BETWEEN 2 AND 4
    GROUP BY 1, 2, 3
),

final_metrics AS (
    SELECT
        d.date,
        d.province,
        d.ward_code,
        d.region_3,
        d.region_8,
        d.location_type,
        d.avg_pm25 as pm25_daily_avg,
        d.avg_pm10 as pm10_daily_avg,
        d.avg_congestion as congestion_daily_avg,
        
        -- Aggregatable components
        d.sum_pm25,
        d.sum_pm10,
        d.total_hours,
        b.background_pm25_sum,
        b.background_pm10_sum,
        b.background_hours,

        -- Traffic Impact Score
        CAST(d.avg_pm25 * d.avg_congestion AS Float32) as traffic_pollution_impact_score,
        
        -- Traffic Contribution % (Attributable Fraction logic)
        CAST(
            coalesce(
                (d.avg_pm25 - b.background_pm25) / nullif(d.avg_pm25, 0), 0
            ) * 100 AS Float32
        ) as traffic_contribution_pct
    FROM daily_stats d
    LEFT JOIN baseline_stats b 
        ON d.date = b.date 
        AND d.province = b.province 
        AND d.ward_code = b.ward_code
)

SELECT
    *,
    now() as dbt_updated_at
FROM final_metrics
WHERE congestion_daily_avg > 0
