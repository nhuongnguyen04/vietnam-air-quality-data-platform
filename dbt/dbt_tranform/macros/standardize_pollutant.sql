{% macro standardize_pollutant_name(pollutant) %}
    CASE
        WHEN LOWER({{ pollutant }}) IN ('pm2.5', 'pm25', 'pm2_5') THEN 'pm25'
        WHEN LOWER({{ pollutant }}) IN ('pm10', 'pm_10') THEN 'pm10'
        WHEN LOWER({{ pollutant }}) IN ('o3', 'ozone') THEN 'o3'
        WHEN LOWER({{ pollutant }}) IN ('co', 'carbon_monoxide') THEN 'co'
        WHEN LOWER({{ pollutant }}) IN ('no2', 'nitrogen_dioxide') THEN 'no2'
        WHEN LOWER({{ pollutant }}) IN ('so2', 'sulfur_dioxide') THEN 'so2'
        WHEN LOWER({{ pollutant }}) IN ('pm1', 'pm_1') THEN 'pm1'
        WHEN LOWER({{ pollutant }}) IN ('t', 'temp') THEN 'temperature'
        WHEN LOWER({{ pollutant }}) IN ('h', 'rh') THEN 'humidity'
        WHEN LOWER({{ pollutant }}) IN ('p', 'press') THEN 'pressure'
        WHEN LOWER({{ pollutant }}) IN ('w', 'wind') THEN 'wind_speed'
        WHEN LOWER({{ pollutant }}) IN ('dew') THEN 'dew_point'
        ELSE LOWER({{ pollutant }})
    END
{% endmacro %}

