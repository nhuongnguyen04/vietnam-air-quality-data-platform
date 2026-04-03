{{ config(materialized='view') }}

-- Thin wrapper: exposes fct_hourly_aqi for mart and analytics models.
select * from {{ ref('fct_hourly_aqi') }}
