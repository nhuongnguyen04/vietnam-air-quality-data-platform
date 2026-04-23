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
        region_3,
        region_8,
        datetime_hour,
        pm25,
        pm10,
        avg_congestion,
        traffic_ward_count,
        positive_traffic_ward_count,
        traffic_coverage_ratio
    FROM {{ ref('dm_traffic_hourly_trend') }}
    {% if is_incremental() %}
    WHERE date >= (select max(date) - interval 1 day from {{ this }})
    {% endif %}
),

daily_stats AS (
    SELECT
        date,
        province,
        any(region_3) as region_3,
        any(region_8) as region_8,
        -- Location classification logic
        any(case
            when province IN ('Hà Nội', 'Hồ Chí Minh', 'Đà Nẵng', 'Hải Phòng', 'Cần Thơ', 'TP.Hà Nội', 'TP.Hồ Chí Minh', 'TP.Đà Nẵng', 'TP.Hải Phòng', 'TP.Cần Thơ') then 'Urban'
            when province IN ('Bình Dương', 'Đồng Nai', 'Bà Rịa - Vũng Tàu', 'Bắc Ninh', 'Bắc Giang', 'Hưng Yên', 'Quảng Ninh', 'Thái Nguyên') then 'Industrial'
            else 'Rural'
        end) as location_type,
        avg(pm25) as avg_pm25,
        avg(pm10) as avg_pm10,
        avg(avg_congestion) as avg_congestion,
        sum(pm25) as sum_pm25,
        sum(pm10) as sum_pm10,
        count(*) as total_hours,
        avg(traffic_ward_count) as avg_traffic_ward_count,
        avg(positive_traffic_ward_count) as avg_positive_traffic_ward_count,
        avg(traffic_coverage_ratio) as avg_traffic_coverage_ratio
    FROM source_data
    GROUP BY date, province
),

congestion_thresholds AS (
    SELECT
        date,
        province,
        quantileExact(0.25)(avg_congestion) as low_congestion_threshold,
        quantileExact(0.75)(avg_congestion) as high_congestion_threshold
    FROM source_data
    GROUP BY 1, 2
),

congestion_band_stats AS (
    SELECT
        s.date,
        s.province,
        avgIf(s.pm25, s.avg_congestion <= t.low_congestion_threshold) as pm25_low_congestion_avg,
        avgIf(s.pm25, s.avg_congestion >= t.high_congestion_threshold) as pm25_high_congestion_avg,
        countIf(s.avg_congestion <= t.low_congestion_threshold) as low_congestion_hours,
        countIf(s.avg_congestion >= t.high_congestion_threshold) as high_congestion_hours,
        any(t.low_congestion_threshold) as low_congestion_threshold,
        any(t.high_congestion_threshold) as high_congestion_threshold
    FROM source_data s
    INNER JOIN congestion_thresholds t
        ON s.date = t.date
        AND s.province = t.province
    GROUP BY 1, 2
),

final_metrics AS (
    SELECT
        d.date,
        d.province,
        '' as ward_code,
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
        d.avg_traffic_ward_count,
        d.avg_positive_traffic_ward_count,
        d.avg_traffic_coverage_ratio,
        c.low_congestion_threshold,
        c.high_congestion_threshold,
        c.pm25_low_congestion_avg,
        c.pm25_high_congestion_avg,
        c.low_congestion_hours,
        c.high_congestion_hours,
        CAST(
            if(
                d.total_hours >= 24
                AND c.low_congestion_hours >= 3
                AND c.high_congestion_hours >= 3,
                c.pm25_high_congestion_avg - c.pm25_low_congestion_avg,
                NULL
            ) AS Nullable(Float32)
        ) as pm25_congestion_uplift,

        -- Observational co-movement score; not a causal impact estimate.
        CAST(d.avg_pm25 * d.avg_congestion AS Float32) as traffic_pollution_impact_score,

        -- Deprecated compatibility column. Causal attribution is not estimated here.
        CAST(NULL AS Nullable(Float32)) as traffic_contribution_pct
    FROM daily_stats d
    LEFT JOIN congestion_band_stats c
        ON d.date = c.date
        AND d.province = c.province
)

SELECT
    *,
    now() as dbt_updated_at
FROM final_metrics
WHERE congestion_daily_avg > 0
