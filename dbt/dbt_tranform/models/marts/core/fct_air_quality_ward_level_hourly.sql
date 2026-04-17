{{ config(
    materialized='incremental',
    engine='ReplacingMergeTree',
    unique_key='(province, ward_code, datetime_hour)',
    order_by='(province, date, assumeNotNull(ward_code))',
    partition_by='toYYYYMM(date)'
) }}

with summary as (
    select * from {{ ref('fct_air_quality_summary_hourly') }}
    {% if is_incremental() %}
    where datetime_hour >= (select max(datetime_hour) - interval 1 day from {{ this }})
    {% endif %}
),

consolidated as (
    select
        datetime_hour,
        date,
        province,
        ward_code,
        region_3,
        region_8,
        
        -- Weighted Average logic (Ground=5, Model=1)
        sum(final_aqi_us * source_weight) / sum(source_weight) as hourly_avg_aqi_us,
        sum(final_aqi_vn * source_weight) / sum(source_weight) as hourly_avg_aqi_vn,
        
        -- Concentrations (Weighted)
        sum(pm25_value * source_weight) / sum(source_weight) as pm25_hourly_avg,
        sum(pm10_value * source_weight) / sum(source_weight) as pm10_hourly_avg,
        sum(co_value   * source_weight) / sum(source_weight) as co_hourly_avg,
        sum(no2_value  * source_weight) / sum(source_weight) as no2_hourly_avg,
        sum(so2_value  * source_weight) / sum(source_weight) as so2_hourly_avg,
        sum(o3_value   * source_weight) / sum(source_weight) as o3_hourly_avg,

        -- Sub-AQIs (Weighted)
        sum(pm25_aqi * source_weight) / sum(source_weight) as pm25_hourly_aqi,
        sum(pm10_aqi * source_weight) / sum(source_weight) as pm10_hourly_aqi,
        sum(co_aqi   * source_weight) / sum(source_weight) as co_hourly_aqi,
        sum(no2_aqi  * source_weight) / sum(source_weight) as no2_hourly_aqi,
        sum(so2_aqi  * source_weight) / sum(source_weight) as so2_hourly_aqi,
        sum(o3_aqi   * source_weight) / sum(source_weight) as o3_hourly_aqi,
        
        max(ingest_time) as last_ingested_at
        
    from summary
    group by
        datetime_hour,
        date,
        province,
        ward_code,
        region_3,
        region_8
),

ward_coords as (
    select
        ward_code,
        any(lat) as latitude,
        any(lon) as longitude
    from {{ ref('stg_core__administrative_units') }}
    group by ward_code
),

final as (
    select
        c.*,
        w.latitude,
        w.longitude,
        -- Logic for main pollutant at ward level
        case 
            when pm25_hourly_aqi >= pm10_hourly_aqi and pm25_hourly_aqi >= co_hourly_aqi and pm25_hourly_aqi >= no2_hourly_aqi and pm25_hourly_aqi >= so2_hourly_aqi and pm25_hourly_aqi >= o3_hourly_aqi then 'pm25'
            when pm10_hourly_aqi >= co_hourly_aqi and pm10_hourly_aqi >= no2_hourly_aqi and pm10_hourly_aqi >= so2_hourly_aqi and pm10_hourly_aqi >= o3_hourly_aqi then 'pm10'
            when co_hourly_aqi >= no2_hourly_aqi and co_hourly_aqi >= so2_hourly_aqi and co_hourly_aqi >= o3_hourly_aqi then 'co'
            when no2_hourly_aqi >= so2_hourly_aqi and no2_hourly_aqi >= o3_hourly_aqi then 'no2'
            when so2_hourly_aqi >= o3_hourly_aqi then 'so2'
            else 'o3'
        end as main_pollutant
    from consolidated c
    left join ward_coords w on c.ward_code = w.ward_code
)

select * from final
