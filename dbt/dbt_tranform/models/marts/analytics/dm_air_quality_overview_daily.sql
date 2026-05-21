{{ config(
    materialized='incremental',
    on_schema_change='sync_all_columns',
    incremental_strategy='delete_insert',
    engine='ReplacingMergeTree(last_ingested_at)',
    unique_key=['province', 'ward_code', 'date'],
    order_by='(province, date, assumeNotNull(ward_code))',
    partition_by='toYYYYMM(date)'
) }}

with ward_daily as (
    select * from {{ ref('fct_air_quality_ward_level_daily') }}
    where {{ downstream_incremental_predicate('raw_sync_run_id', 'raw_loaded_at') }}
),

admin_units as (
    select 
        ward_code,
        ward_name,
        province,
        region_3,
        region_8,
        latitude,
        longitude,
        population
    from {{ ref('dim_administrative_units') }}
    where ward_code is not null
      and ward_code != ''
      and province is not null
      and province != ''
      and ward_name is not null
      and ward_name != ''
),

final as (
    select
        w.date as date,
        a.province as province,
        a.ward_code as ward_code,
        a.ward_name as ward_name,
        a.region_3 as region_3,
        a.region_8 as region_8,
        a.latitude as latitude,
        a.longitude as longitude,
        a.population as population,
        
        -- Standardized columns for dashboard
        w.avg_aqi_us as avg_aqi_us,
        w.max_aqi_us as max_aqi_us,
        w.avg_aqi_vn as avg_aqi_vn,
        w.max_aqi_vn as max_aqi_vn,
        w.main_pollutant as main_pollutant,
        
        -- Standardized concentrations
        w.pm25_avg as pm25_avg,
        w.pm10_avg as pm10_avg,
        w.co_avg as co_avg,
        w.no2_avg as no2_avg,
        w.so2_avg as so2_avg,
        w.o3_avg as o3_avg,
        w.aqiin_observation_count as aqiin_observation_count,
        w.openweather_observation_count as openweather_observation_count,
        w.source_mix as source_mix,
        w.confidence_score as confidence_score,
        w.confidence_level as confidence_level,
        
        w.last_ingested_at as last_ingested_at
    from ward_daily w
    inner join admin_units a on w.ward_code = a.ward_code
    where w.ward_code is not null
      and w.ward_code != ''
)

select * from final
