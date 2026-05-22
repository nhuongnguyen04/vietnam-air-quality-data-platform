{% macro process_ward_hourly_to_daily(ward_hourly_ref) %}
with daily_agg as (
    select
        date,
        province,
        ward_code,
        any(region_3) as region_3,
        any(region_8) as region_8,
        
        avg(avg_aqi_us) as _avg_aqi_us,
        max(avg_aqi_us) as _max_aqi_us,
        min(avg_aqi_us) as _min_aqi_us,
        
        avg(avg_aqi_vn) as _avg_aqi_vn,
        max(avg_aqi_vn) as _max_aqi_vn,
        min(avg_aqi_vn) as _min_aqi_vn,
        
        -- Concentrations
        avg(pm25_avg) as _pm25_avg,
        avg(pm10_avg) as _pm10_avg,
        avg(co_avg)   as _co_avg,
        avg(no2_avg)  as _no2_avg,
        avg(so2_avg)  as _so2_avg,
        avg(o3_avg)   as _o3_avg,

        -- Daily average sub-AQIs
        avg(pm25_aqi) as _pm25_aqi,
        avg(pm10_aqi) as _pm10_aqi,
        avg(co_aqi)   as _co_aqi,
        avg(no2_aqi)  as _no2_aqi,
        avg(so2_aqi)  as _so2_aqi,
        avg(o3_aqi)   as _o3_aqi,

        sum(observation_count) as observation_count,
        
        max(last_ingested_at) as last_ingested_at,
        max(raw_loaded_at) as max_raw_loaded_at,
        argMax(raw_sync_run_id, raw_loaded_at) as latest_raw_sync_run_id,
        argMax(raw_sync_started_at, raw_loaded_at) as latest_raw_sync_started_at
        
    from {{ ward_hourly_ref }}
    group by date, province, ward_code
)
select
    date, province, ward_code, region_3, region_8, last_ingested_at,
    _avg_aqi_us as avg_aqi_us,
    _max_aqi_us as max_aqi_us,
    _min_aqi_us as min_aqi_us,
    _avg_aqi_vn as avg_aqi_vn,
    _max_aqi_vn as max_aqi_vn,
    _min_aqi_vn as min_aqi_vn,
    _pm25_avg as pm25_avg,
    _pm10_avg as pm10_avg,
    _co_avg as co_avg,
    _no2_avg as no2_avg,
    _so2_avg as so2_avg,
    _o3_avg as o3_avg,
    _pm25_aqi as pm25_aqi,
    _pm10_aqi as pm10_aqi,
    _co_aqi as co_aqi,
    _no2_aqi as no2_aqi,
    _so2_aqi as so2_aqi,
    _o3_aqi as o3_aqi,
    observation_count,
    {{ get_main_pollutant('_pm25_aqi', '_pm10_aqi', '_co_aqi', '_no2_aqi', '_so2_aqi', '_o3_aqi') }} as main_pollutant,
    max_raw_loaded_at as raw_loaded_at,
    latest_raw_sync_run_id as raw_sync_run_id,
    latest_raw_sync_started_at as raw_sync_started_at
from daily_agg
{% endmacro %}


{% macro process_ward_daily_to_monthly(ward_daily_ref) %}
with monthly_agg as (
    select
        toStartOfMonth(date) as month,
        province,
        ward_code,
        any(region_3) as region_3,
        any(region_8) as region_8,
        
        avg(avg_aqi_us) as _avg_aqi_us,
        max(max_aqi_us) as _max_aqi_us,
        min(min_aqi_us) as _min_aqi_us,
        
        avg(avg_aqi_vn) as _avg_aqi_vn,
        max(max_aqi_vn) as _max_aqi_vn,
        min(min_aqi_vn) as _min_aqi_vn,
        
        -- Concentrations
        avg(pm25_avg) as _pm25_avg,
        avg(pm10_avg) as _pm10_avg,
        avg(co_avg)   as _co_avg,
        avg(no2_avg)  as _no2_avg,
        avg(so2_avg)  as _so2_avg,
        avg(o3_avg)   as _o3_avg,

        -- Monthly average sub-AQIs
        avg(pm25_aqi) as _pm25_aqi,
        avg(pm10_aqi) as _pm10_aqi,
        avg(co_aqi)   as _co_aqi,
        avg(no2_aqi)  as _no2_aqi,
        avg(so2_aqi)  as _so2_aqi,
        avg(o3_aqi)   as _o3_aqi,

        sum(observation_count) as observation_count,
        count(*) as samples_count,
        
        max(last_ingested_at) as last_ingested_at,
        max(raw_loaded_at) as max_raw_loaded_at,
        argMax(raw_sync_run_id, raw_loaded_at) as latest_raw_sync_run_id,
        argMax(raw_sync_started_at, raw_loaded_at) as latest_raw_sync_started_at
        
    from {{ ward_daily_ref }}
    group by month, province, ward_code
)
select
    month, province, ward_code, region_3, region_8, samples_count, last_ingested_at,
    _avg_aqi_us as avg_aqi_us,
    _max_aqi_us as max_aqi_us,
    _min_aqi_us as min_aqi_us,
    _avg_aqi_vn as avg_aqi_vn,
    _max_aqi_vn as max_aqi_vn,
    _min_aqi_vn as min_aqi_vn,
    _pm25_avg as pm25_avg,
    _pm10_avg as pm10_avg,
    _co_avg as co_avg,
    _no2_avg as no2_avg,
    _so2_avg as so2_avg,
    _o3_avg as o3_avg,
    _pm25_aqi as pm25_aqi,
    _pm10_aqi as pm10_aqi,
    _co_aqi as co_aqi,
    _no2_aqi as no2_aqi,
    _so2_aqi as so2_aqi,
    _o3_aqi as o3_aqi,
    observation_count,
    {{ get_main_pollutant('_pm25_aqi', '_pm10_aqi', '_co_aqi', '_no2_aqi', '_so2_aqi', '_o3_aqi') }} as main_pollutant,
    max_raw_loaded_at as raw_loaded_at,
    latest_raw_sync_run_id as raw_sync_run_id,
    latest_raw_sync_started_at as raw_sync_started_at
from monthly_agg
{% endmacro %}


{% macro process_ward_hourly_to_province_hourly(ward_hourly_ref) %}
with province_hourly as (
    select
        datetime_hour,
        date,
        province,
        region_3,
        region_8,

        -- Provincial average
        avg(avg_aqi_us) as avg_aqi_us,
        avg(avg_aqi_vn) as avg_aqi_vn,

        -- Concentrations
        avg(pm25_avg) as pm25_avg,
        avg(pm10_avg) as pm10_avg,
        avg(co_avg)   as co_avg,
        avg(no2_avg)  as no2_avg,
        avg(so2_avg)  as so2_avg,
        avg(o3_avg)   as o3_avg,

        -- Sub-AQIs
        avg(pm25_aqi) as pm25_aqi,
        avg(pm10_aqi) as pm10_aqi,
        avg(co_aqi)   as co_aqi,
        avg(no2_aqi)  as no2_aqi,
        avg(so2_aqi)  as so2_aqi,
        avg(o3_aqi)   as o3_aqi,

        count(distinct ward_code) as total_ward_count,
        sum(observation_count) as observation_count,

        max(last_ingested_at)                       as last_ingested_at,
        max(raw_loaded_at)                          as max_raw_loaded_at,
        argMax(raw_sync_run_id, raw_loaded_at)      as latest_raw_sync_run_id,
        argMax(raw_sync_started_at, raw_loaded_at)  as latest_raw_sync_started_at

    from {{ ward_hourly_ref }}
    group by
        datetime_hour,
        date,
        province,
        region_3,
        region_8
)
select
    datetime_hour,
    date,
    province,
    region_3,
    region_8,
    avg_aqi_us,
    avg_aqi_vn,
    pm25_avg,
    pm10_avg,
    co_avg,
    no2_avg,
    so2_avg,
    o3_avg,
    pm25_aqi,
    pm10_aqi,
    co_aqi,
    no2_aqi,
    so2_aqi,
    o3_aqi,
    total_ward_count,
    observation_count,
    'observed' as source_mix,
    1.0 as confidence_score,
    'high' as confidence_level,
    last_ingested_at,
    max_raw_loaded_at as raw_loaded_at,
    latest_raw_sync_run_id as raw_sync_run_id,
    latest_raw_sync_started_at as raw_sync_started_at,
    {{ get_main_pollutant('pm25_aqi', 'pm10_aqi', 'co_aqi', 'no2_aqi', 'so2_aqi', 'o3_aqi') }} as main_pollutant
from province_hourly
{% endmacro %}


{% macro process_ward_daily_to_province_daily(ward_daily_ref) %}
with province_daily as (
    select
        date,
        province,
        region_3,
        region_8,

        -- Provincial average
        avg(avg_aqi_us) as avg_aqi_us,
        max(max_aqi_us) as max_aqi_us,
        min(min_aqi_us) as min_aqi_us,
        avg(avg_aqi_vn) as avg_aqi_vn,
        max(max_aqi_vn) as max_aqi_vn,
        min(min_aqi_vn) as min_aqi_vn,

        -- Concentrations
        avg(pm25_avg) as pm25_avg,
        avg(pm10_avg) as pm10_avg,
        avg(co_avg)   as co_avg,
        avg(no2_avg)  as no2_avg,
        avg(so2_avg)  as so2_avg,
        avg(o3_avg)   as o3_avg,

        -- Sub-AQIs
        avg(pm25_aqi) as pm25_aqi,
        avg(pm10_aqi) as pm10_aqi,
        avg(co_aqi)   as co_aqi,
        avg(no2_aqi)  as no2_aqi,
        avg(so2_aqi)  as so2_aqi,
        avg(o3_aqi)   as o3_aqi,

        count(distinct ward_code) as total_ward_count,
        sum(observation_count) as observation_count,

        max(last_ingested_at)                       as last_ingested_at,
        max(raw_loaded_at)                          as max_raw_loaded_at,
        argMax(raw_sync_run_id, raw_loaded_at)      as latest_raw_sync_run_id,
        argMax(raw_sync_started_at, raw_loaded_at)  as latest_raw_sync_started_at

    from {{ ward_daily_ref }}
    group by
        date,
        province,
        region_3,
        region_8
)
select
    date,
    province,
    region_3,
    region_8,
    avg_aqi_us,
    max_aqi_us,
    min_aqi_us,
    avg_aqi_vn,
    max_aqi_vn,
    min_aqi_vn,
    pm25_avg,
    pm10_avg,
    co_avg,
    no2_avg,
    so2_avg,
    o3_avg,
    pm25_aqi,
    pm10_aqi,
    co_aqi,
    no2_aqi,
    so2_aqi,
    o3_aqi,
    total_ward_count,
    observation_count,
    'observed' as source_mix,
    1.0 as confidence_score,
    'high' as confidence_level,
    last_ingested_at,
    max_raw_loaded_at as raw_loaded_at,
    latest_raw_sync_run_id as raw_sync_run_id,
    latest_raw_sync_started_at as raw_sync_started_at,
    {{ get_main_pollutant('pm25_aqi', 'pm10_aqi', 'co_aqi', 'no2_aqi', 'so2_aqi', 'o3_aqi') }} as main_pollutant
from province_daily
{% endmacro %}


{% macro process_ward_monthly_to_province_monthly(ward_monthly_ref) %}
with province_monthly as (
    select
        month,
        province,
        region_3,
        region_8,

        -- Provincial average
        avg(avg_aqi_us) as avg_aqi_us,
        max(max_aqi_us) as max_aqi_us,
        min(min_aqi_us) as min_aqi_us,
        avg(avg_aqi_vn) as avg_aqi_vn,
        max(max_aqi_vn) as max_aqi_vn,
        min(min_aqi_vn) as min_aqi_vn,

        -- Concentrations
        avg(pm25_avg) as pm25_avg,
        avg(pm10_avg) as pm10_avg,
        avg(co_avg)   as co_avg,
        avg(no2_avg)  as no2_avg,
        avg(so2_avg)  as so2_avg,
        avg(o3_avg)   as o3_avg,

        -- Sub-AQIs
        avg(pm25_aqi) as pm25_aqi,
        avg(pm10_aqi) as pm10_aqi,
        avg(co_aqi)   as co_aqi,
        avg(no2_aqi)  as no2_aqi,
        avg(so2_aqi)  as so2_aqi,
        avg(o3_aqi)   as o3_aqi,

        count(distinct ward_code) as total_ward_count,
        sum(observation_count) as observation_count,

        max(last_ingested_at)                       as last_ingested_at,
        max(raw_loaded_at)                          as max_raw_loaded_at,
        argMax(raw_sync_run_id, raw_loaded_at)      as latest_raw_sync_run_id,
        argMax(raw_sync_started_at, raw_loaded_at)  as latest_raw_sync_started_at

    from {{ ward_monthly_ref }}
    group by
        month,
        province,
        region_3,
        region_8
)
select
    month,
    province,
    region_3,
    region_8,
    avg_aqi_us,
    max_aqi_us,
    min_aqi_us,
    avg_aqi_vn,
    max_aqi_vn,
    min_aqi_vn,
    pm25_avg,
    pm10_avg,
    co_avg,
    no2_avg,
    so2_avg,
    o3_avg,
    pm25_aqi,
    pm10_aqi,
    co_aqi,
    no2_aqi,
    so2_aqi,
    o3_aqi,
    total_ward_count,
    observation_count,
    'observed' as source_mix,
    1.0 as confidence_score,
    'high' as confidence_level,
    last_ingested_at,
    max_raw_loaded_at as raw_loaded_at,
    latest_raw_sync_run_id as raw_sync_run_id,
    latest_raw_sync_started_at as raw_sync_started_at,
    {{ get_main_pollutant('pm25_aqi', 'pm10_aqi', 'co_aqi', 'no2_aqi', 'so2_aqi', 'o3_aqi') }} as main_pollutant
from province_monthly
{% endmacro %}
