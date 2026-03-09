{{ config(materialized='table') }}

with daily_pollutants as (
    select
        date,
        unified_station_id,
        'pm25' as pollutant,
        avg_pm25 as avg_concentration,
        max_pm25 as max_concentration
    from {{ ref('mart_air_quality__daily_summary') }}
    where avg_pm25 is not null
    
    union all
    
    select
        date,
        unified_station_id,
        'pm10' as pollutant,
        avg_pm10 as avg_concentration,
        max_pm10 as max_concentration
    from {{ ref('mart_air_quality__daily_summary') }}
    where avg_pm10 is not null
    
    union all
    
    select
        date,
        unified_station_id,
        'o3' as pollutant,
        avg_o3 as avg_concentration,
        max_o3 as max_concentration
    from {{ ref('mart_air_quality__daily_summary') }}
    where avg_o3 is not null
    
    union all
    
    select
        date,
        unified_station_id,
        'no2' as pollutant,
        avg_no2 as avg_concentration,
        max_no2 as max_concentration
    from {{ ref('mart_air_quality__daily_summary') }}
    where avg_no2 is not null
    
    union all
    
    select
        date,
        unified_station_id,
        'co' as pollutant,
        avg_co as avg_concentration,
        max_co as max_concentration
    from {{ ref('mart_air_quality__daily_summary') }}
    where avg_co is not null
    
    union all
    
    select
        date,
        unified_station_id,
        'so2' as pollutant,
        avg_so2 as avg_concentration,
        max_so2 as max_concentration
    from {{ ref('mart_air_quality__daily_summary') }}
    where avg_so2 is not null
),

with_standards as (
    select
        date,
        pollutant,
        avg_concentration,
        max_concentration,
        count(*) as measurement_count,
        -- WHO/EPA standards (µg/m³ for most, ppm for CO)
        case
            when pollutant = 'pm25' then 15.0  -- WHO annual guideline
            when pollutant = 'pm10' then 45.0  -- WHO annual guideline
            when pollutant = 'o3' then 100.0    -- WHO 8-hour guideline (ppb)
            when pollutant = 'no2' then 25.0   -- WHO annual guideline
            when pollutant = 'co' then 4.0     -- WHO 8-hour guideline (ppm)
            when pollutant = 'so2' then 40.0   -- WHO 24-hour guideline
            else null
        end as who_standard,
        case
            when pollutant = 'pm25' then 12.0  -- EPA annual standard
            when pollutant = 'pm10' then 54.0    -- EPA 24-hour standard
            when pollutant = 'o3' then 70.0     -- EPA 8-hour standard (ppb)
            when pollutant = 'no2' then 100.0   -- EPA 1-hour standard (ppb)
            when pollutant = 'co' then 9.0      -- EPA 8-hour standard (ppm)
            when pollutant = 'so2' then 75.0   -- EPA 1-hour standard (ppb)
            else null
        end as epa_standard
    from daily_pollutants
    group by date, pollutant, avg_concentration, max_concentration
),

with_exceedances as (
    select
        *,
        countIf(avg_concentration > who_standard) as exceedance_count_who,
        countIf(avg_concentration > epa_standard) as exceedance_count_epa,
        (countIf(avg_concentration > who_standard)::Float64 / measurement_count) * 100 as exceedance_rate_who,
        (countIf(avg_concentration > epa_standard)::Float64 / measurement_count) * 100 as exceedance_rate_epa
    from with_standards
    group by date, pollutant, avg_concentration, max_concentration, measurement_count, who_standard, epa_standard
)

select
    date,
    pollutant,
    avg(avg_concentration) as avg_concentration,
    max(max_concentration) as max_concentration,
    sum(measurement_count) as total_measurements,
    max(who_standard) as who_standard,
    max(epa_standard) as epa_standard,
    sum(exceedance_count_who) as exceedance_count_who,
    sum(exceedance_count_epa) as exceedance_count_epa,
    avg(exceedance_rate_who) as exceedance_rate_who,
    avg(exceedance_rate_epa) as exceedance_rate_epa
from with_exceedances
group by date, pollutant

