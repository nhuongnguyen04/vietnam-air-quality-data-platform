-- depends_on: {{ ref('source_calibration') }}
{{ config(materialized='view') }}

-- Source calibration factors for AQI calculation.
-- openweather: Grid-model estimates systematically underestimate urban PM2.5 in Vietnam.
-- calibration_factor > 1.0 means we amplify the measured value to correct for underestimation.
-- Method: regression vs AQICN ground-truth on overlapping data periods.
select
    source,
    parameter,
    calibration_factor,
    calibration_method,
    notes
from {{ ref('source_calibration') }}