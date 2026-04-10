{% macro calculate_aqi_pm25(concentration) %}
    -- US EPA AQI calculation for PM2.5 (µg/m³)
    CASE
        WHEN {{ concentration }} <= 12.0 THEN
            ((50 - 0) / (12.0 - 0.0)) * ({{ concentration }} - 0.0) + 0
        WHEN {{ concentration }} <= 35.4 THEN
            ((100 - 51) / (35.4 - 12.1)) * ({{ concentration }} - 12.1) + 51
        WHEN {{ concentration }} <= 55.4 THEN
            ((150 - 101) / (55.4 - 35.5)) * ({{ concentration }} - 35.5) + 101
        WHEN {{ concentration }} <= 150.4 THEN
            ((200 - 151) / (150.4 - 55.5)) * ({{ concentration }} - 55.5) + 151
        WHEN {{ concentration }} <= 250.4 THEN
            ((300 - 201) / (250.4 - 150.5)) * ({{ concentration }} - 150.5) + 201
        WHEN {{ concentration }} <= 350.4 THEN
            ((400 - 301) / (350.4 - 250.5)) * ({{ concentration }} - 250.5) + 301
        WHEN {{ concentration }} <= 500.4 THEN
            ((500 - 401) / (500.4 - 350.5)) * ({{ concentration }} - 350.5) + 401
        ELSE NULL
    END
{% endmacro %}

{% macro calculate_aqi_pm10(concentration) %}
    -- US EPA AQI calculation for PM10 (µg/m³)
    CASE
        WHEN {{ concentration }} <= 54 THEN
            ((50 - 0) / (54 - 0)) * ({{ concentration }} - 0) + 0
        WHEN {{ concentration }} <= 154 THEN
            ((100 - 51) / (154 - 55)) * ({{ concentration }} - 55) + 51
        WHEN {{ concentration }} <= 254 THEN
            ((150 - 101) / (254 - 155)) * ({{ concentration }} - 155) + 101
        WHEN {{ concentration }} <= 354 THEN
            ((200 - 151) / (354 - 255)) * ({{ concentration }} - 255) + 151
        WHEN {{ concentration }} <= 424 THEN
            ((300 - 201) / (424 - 355)) * ({{ concentration }} - 355) + 201
        WHEN {{ concentration }} <= 504 THEN
            ((400 - 301) / (504 - 425)) * ({{ concentration }} - 425) + 301
        WHEN {{ concentration }} <= 604 THEN
            ((500 - 401) / (604 - 505)) * ({{ concentration }} - 505) + 401
        ELSE NULL
    END
{% endmacro %}

{% macro calculate_aqi_o3(concentration) %}
    -- US EPA AQI calculation for O3 (ppb) - 8-hour average
    CASE
        WHEN {{ concentration }} <= 54 THEN
            ((50 - 0) / (54 - 0)) * ({{ concentration }} - 0) + 0
        WHEN {{ concentration }} <= 70 THEN
            ((100 - 51) / (70 - 55)) * ({{ concentration }} - 55) + 51
        WHEN {{ concentration }} <= 85 THEN
            ((150 - 101) / (85 - 71)) * ({{ concentration }} - 71) + 101
        WHEN {{ concentration }} <= 105 THEN
            ((200 - 151) / (105 - 86)) * ({{ concentration }} - 86) + 151
        WHEN {{ concentration }} <= 200 THEN
            ((300 - 201) / (200 - 106)) * ({{ concentration }} - 106) + 201
        ELSE NULL
    END
{% endmacro %}

{% macro calculate_aqi_no2(concentration) %}
    -- US EPA AQI calculation for NO2 (ppb) - 1-hour average
    CASE
        WHEN {{ concentration }} <= 53 THEN
            ((50 - 0) / (53 - 0)) * ({{ concentration }} - 0) + 0
        WHEN {{ concentration }} <= 100 THEN
            ((100 - 51) / (100 - 54)) * ({{ concentration }} - 54) + 51
        WHEN {{ concentration }} <= 360 THEN
            ((150 - 101) / (360 - 101)) * ({{ concentration }} - 101) + 101
        WHEN {{ concentration }} <= 649 THEN
            ((200 - 151) / (649 - 361)) * ({{ concentration }} - 361) + 151
        WHEN {{ concentration }} <= 1249 THEN
            ((300 - 201) / (1249 - 650)) * ({{ concentration }} - 650) + 201
        WHEN {{ concentration }} <= 1649 THEN
            ((400 - 301) / (1649 - 1250)) * ({{ concentration }} - 1250) + 301
        WHEN {{ concentration }} <= 2049 THEN
            ((500 - 401) / (2049 - 1650)) * ({{ concentration }} - 1650) + 401
        ELSE NULL
    END
{% endmacro %}

{% macro calculate_aqi_co(concentration) %}
    -- US EPA AQI calculation for CO (ppm) - 8-hour average
    CASE
        WHEN {{ concentration }} <= 4.4 THEN
            ((50 - 0) / (4.4 - 0)) * ({{ concentration }} - 0) + 0
        WHEN {{ concentration }} <= 9.4 THEN
            ((100 - 51) / (9.4 - 4.5)) * ({{ concentration }} - 4.5) + 51
        WHEN {{ concentration }} <= 12.4 THEN
            ((150 - 101) / (12.4 - 9.5)) * ({{ concentration }} - 9.5) + 101
        WHEN {{ concentration }} <= 15.4 THEN
            ((200 - 151) / (15.4 - 12.5)) * ({{ concentration }} - 12.5) + 151
        WHEN {{ concentration }} <= 30.4 THEN
            ((300 - 201) / (30.4 - 15.5)) * ({{ concentration }} - 15.5) + 201
        WHEN {{ concentration }} <= 40.4 THEN
            ((400 - 301) / (40.4 - 30.5)) * ({{ concentration }} - 30.5) + 301
        WHEN {{ concentration }} <= 50.4 THEN
            ((500 - 401) / (50.4 - 40.5)) * ({{ concentration }} - 40.5) + 401
        ELSE NULL
    END
{% endmacro %}

{% macro calculate_aqi_so2(concentration) %}
    -- US EPA AQI calculation for SO2 (ppb) - 1-hour average
    CASE
        WHEN {{ concentration }} <= 35 THEN
            ((50 - 0) / (35 - 0)) * ({{ concentration }} - 0) + 0
        WHEN {{ concentration }} <= 75 THEN
            ((100 - 51) / (75 - 36)) * ({{ concentration }} - 36) + 51
        WHEN {{ concentration }} <= 185 THEN
            ((150 - 101) / (185 - 76)) * ({{ concentration }} - 76) + 101
        WHEN {{ concentration }} <= 304 THEN
            ((200 - 151) / (304 - 186)) * ({{ concentration }} - 186) + 151
        WHEN {{ concentration }} <= 604 THEN
            ((300 - 201) / (604 - 305)) * ({{ concentration }} - 305) + 201
        WHEN {{ concentration }} <= 804 THEN
            ((400 - 301) / (804 - 605)) * ({{ concentration }} - 605) + 301
        WHEN {{ concentration }} <= 1004 THEN
            ((500 - 401) / (1004 - 805)) * ({{ concentration }} - 805) + 401
        ELSE NULL
    END
{% endmacro %}

{% macro get_aqi_category(aqi_value) %}
    CASE
        WHEN {{ aqi_value }} IS NULL THEN NULL
        WHEN {{ aqi_value }} <= 50 THEN 'Good'
        WHEN {{ aqi_value }} <= 100 THEN 'Moderate'
        WHEN {{ aqi_value }} <= 150 THEN 'Unhealthy for Sensitive Groups'
        WHEN {{ aqi_value }} <= 200 THEN 'Unhealthy'
        WHEN {{ aqi_value }} <= 300 THEN 'Very Unhealthy'
        WHEN {{ aqi_value }} <= 500 THEN 'Hazardous'
        ELSE 'Hazardous'
    END
{% endmacro %}

{% macro calculate_aqi(pollutant, concentration) %}
    -- Main macro to calculate AQI based on pollutant type
    CASE
        WHEN LOWER({{ pollutant }}) = 'pm25' THEN {{ calculate_aqi_pm25(concentration) }}
        WHEN LOWER({{ pollutant }}) = 'pm10' THEN {{ calculate_aqi_pm10(concentration) }}
        WHEN LOWER({{ pollutant }}) = 'o3' THEN {{ calculate_aqi_o3(concentration) }}
        WHEN LOWER({{ pollutant }}) = 'no2' THEN {{ calculate_aqi_no2(concentration) }}
        WHEN LOWER({{ pollutant }}) = 'co' THEN {{ calculate_aqi_co(concentration) }}
        WHEN LOWER({{ pollutant }}) = 'so2' THEN {{ calculate_aqi_so2(concentration) }}
        ELSE NULL
    END
{% endmacro %}

