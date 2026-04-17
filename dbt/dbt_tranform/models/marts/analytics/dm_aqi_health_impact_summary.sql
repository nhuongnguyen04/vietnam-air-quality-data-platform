{{ config(
    materialized='incremental',
    engine='MergeTree',
    order_by='(province, date)',
    partition_by='toYYYYMM(date)'
) }}

with hourly_summary as (
    select
        date,
        province,
        ward_code,
        hourly_avg_aqi_us as final_aqi_us,
        -- Calculate dominant pollutant for health advice
        case 
            when pm25_hourly_aqi >= pm10_hourly_aqi and pm25_hourly_aqi >= co_hourly_aqi and pm25_hourly_aqi >= no2_hourly_aqi and pm25_hourly_aqi >= so2_hourly_aqi and pm25_hourly_aqi >= o3_hourly_aqi then 'pm25'
            when pm10_hourly_aqi >= co_hourly_aqi and pm10_hourly_aqi >= no2_hourly_aqi and pm10_hourly_aqi >= so2_hourly_aqi and pm10_hourly_aqi >= o3_hourly_aqi then 'pm10'
            when co_hourly_aqi >= no2_hourly_aqi and co_hourly_aqi >= so2_hourly_aqi and co_hourly_aqi >= o3_hourly_aqi then 'co'
            when no2_hourly_aqi >= so2_hourly_aqi and no2_hourly_aqi >= o3_hourly_aqi then 'no2'
            when so2_hourly_aqi >= o3_hourly_aqi then 'so2'
            else 'o3'
        end as pollutant_key,
        last_ingested_at as ingest_time
    from {{ ref('fct_air_quality_ward_level_hourly') }}
    {% if is_incremental() %}
    where last_ingested_at > (select max(ingest_time) from {{ this }})
    {% endif %}
),

health_benchmarks as (
    -- Casting String columns to Float64 for ClickHouse comparison
    select
        pollutant_key,
        health_effects,
        toFloat64(epa_aqi_bp_lo) as epa_aqi_bp_lo,
        toFloat64(epa_aqi_bp_hi) as epa_aqi_bp_hi
    from {{ ref('pollutants') }}
),

impact_joined as (
    select
        h.date,
        h.province,
        h.ward_code,
        h.final_aqi_us,
        h.pollutant_key,
        h.ingest_time,
        b.health_effects,
        case
            when h.final_aqi_us <= 50 then 'Good'
            when h.final_aqi_us <= 100 then 'Moderate'
            when h.final_aqi_us <= 150 then 'Unhealthy for Sensitive Groups'
            when h.final_aqi_us <= 200 then 'Unhealthy'
            when h.final_aqi_us <= 300 then 'Very Unhealthy'
            else 'Hazardous'
        end as aqi_category
    from hourly_summary h
    left join health_benchmarks b
        on h.pollutant_key = b.pollutant_key
        and h.final_aqi_us >= b.epa_aqi_bp_lo
        and h.final_aqi_us <= b.epa_aqi_bp_hi
),

daily_impact_stats as (
    select
        date,
        province,
        ward_code,
        count(*) as total_hours,
        countIf(aqi_category in ('Unhealthy', 'Very Unhealthy', 'Hazardous')) as high_risk_hours,
        countIf(aqi_category in ('Unhealthy', 'Very Unhealthy', 'Hazardous')) / count(*) as high_risk_exposure_pct,
        -- Take the most common health advice for the day
        argMax(health_effects, final_aqi_us) as primary_health_advice,
        max(ingest_time) as ingest_time
    from impact_joined
    group by date, province, ward_code
)

select * from daily_impact_stats
