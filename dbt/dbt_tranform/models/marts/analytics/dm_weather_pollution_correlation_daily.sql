-- depends_on: {{ ref('dm_weather_hourly_trend') }}
{{ config(
    materialized='incremental',
    on_schema_change='sync_all_columns',
    incremental_strategy='delete_insert',
    engine='ReplacingMergeTree(dbt_updated_at)',
    unique_key=['province', 'ward_code', 'date', 'source_mix'],
    order_by='(province, date, source_mix, ward_code)',
    partition_by='toYYYYMM(date)',
    query_settings={
        'max_threads': 1,
        'max_block_size': 4096,
        'max_bytes_before_external_sort': 67108864,
        'max_bytes_before_external_group_by': 67108864,
        'optimize_aggregation_in_order': 1
    }
) }}

WITH source_data AS (
    SELECT
        date,
        province,
        ward_code,
        region_3,
        region_8,
        pm25,
        pm10,
        temperature as temp,
        humidity,
        wind_speed,
        source_mix,
        confidence_score,
        confidence_level,
        raw_loaded_at,
        raw_sync_run_id,
        raw_sync_started_at
    FROM {{ ref('dm_weather_hourly_trend') }}
    WHERE {{ downstream_incremental_predicate('raw_sync_run_id', 'raw_loaded_at') }}
),

daily_stats AS (
    SELECT
        date,
        province,
        ward_code,
        source_mix,
        any(region_3) as region_3,
        any(region_8) as region_8,
        avg(pm25) as avg_pm25,
        avg(pm10) as avg_pm10,
        avg(temp) as avg_temp,
        avg(humidity) as avg_humidity,
        avg(wind_speed) as avg_wind_speed,
        sum(pm25) as sum_pm25,
        sum(pm10) as sum_pm10,
        avg(confidence_score) as confidence_score,
        topK(1)(confidence_level)[1] as confidence_level,
        count(*) as total_hours
    FROM source_data
    GROUP BY date, province, ward_code, source_mix
),

weather_modes AS (
    SELECT
        date,
        province,
        ward_code,
        source_mix,
        sumIf(pm25, wind_speed < 1.0) as stagnant_pm25_sum,
        countIf(wind_speed < 1.0) as stagnant_hours,
        sumIf(pm25, wind_speed >= 2.0) as dispersive_pm25_sum,
        countIf(wind_speed >= 2.0) as dispersive_hours,
        sumIf(pm10, wind_speed < 1.0) as stagnant_pm10_sum,
        sumIf(pm10, wind_speed >= 2.0) as dispersive_pm10_sum,
        countIf(wind_speed < 1.0) / count(*) as stagnant_air_probability
    FROM source_data
    GROUP BY date, province, ward_code, source_mix
),

final_metrics AS (
    SELECT
        d.date,
        d.province,
        d.ward_code,
        d.region_3,
        d.region_8,
        d.avg_pm25 as pm25_daily_avg,
        d.avg_pm10 as pm10_daily_avg,
        d.avg_temp as temp_daily_avg,
        d.avg_humidity as humidity_daily_avg,
        d.avg_wind_speed as wind_daily_avg,
        
        -- Aggregatable components
        d.sum_pm25,
        d.sum_pm10,
        d.total_hours,
        m.stagnant_pm25_sum,
        m.stagnant_pm10_sum,
        m.stagnant_hours,
        m.dispersive_pm25_sum,
        m.dispersive_pm10_sum,
        m.dispersive_hours,

        -- Custom Index: Higher index means higher risk (Low wind dispersal)
        CAST(coalesce(d.avg_pm25 / nullif(d.avg_wind_speed, 0), 0) AS Float32) as wind_dispersal_risk_index,
        
        -- Weather Influence % (Attributable Fraction logic: (Stagnant - Dispersive) / Stagnant)
        CAST(
            coalesce(
                nullIf(
                    ( (m.stagnant_pm25_sum / nullif(m.stagnant_hours, 0)) - (m.dispersive_pm25_sum / nullif(m.dispersive_hours, 0)) ) / nullif(m.stagnant_pm25_sum / nullif(m.stagnant_hours, 0), 0),
                    NaN
                ),
                0
            ) * 100 AS Float32
        ) as weather_influence_pct,

        m.stagnant_air_probability,
        d.confidence_score,
        d.confidence_level,
        d.source_mix
        
    FROM daily_stats d
    LEFT JOIN weather_modes m 
        ON d.date = m.date 
        AND d.province = m.province 
        AND d.ward_code = m.ward_code
        AND d.source_mix = m.source_mix
)

SELECT
    date,
    province,
    ward_code,
    region_3,
    region_8,
    pm25_daily_avg,
    pm10_daily_avg,
    temp_daily_avg,
    humidity_daily_avg,
    wind_daily_avg,
    sum_pm25,
    sum_pm10,
    total_hours,
    stagnant_pm25_sum,
    stagnant_pm10_sum,
    stagnant_hours,
    dispersive_pm25_sum,
    dispersive_pm10_sum,
    dispersive_hours,
    wind_dispersal_risk_index,
    weather_influence_pct,
    stagnant_air_probability,
    confidence_score,
    confidence_level,
    source_mix,
    now() as dbt_updated_at
FROM final_metrics
WHERE province != '' AND ward_code != ''
