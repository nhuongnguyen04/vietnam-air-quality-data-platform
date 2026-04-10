{% macro calculate_aqi_vn_pm25(concentration) %}
    -- Vietnam AQI calculation for PM2.5 (Decision 1459/QD-TCMT)
    CASE
        WHEN {{ concentration }} <= 25 THEN
            ((50 - 0) / (25 - 0)) * ({{ concentration }} - 0) + 0
        WHEN {{ concentration }} <= 50 THEN
            ((100 - 51) / (50 - 26)) * ({{ concentration }} - 26) + 51
        WHEN {{ concentration }} <= 80 THEN
            ((150 - 101) / (80 - 51)) * ({{ concentration }} - 51) + 101
        WHEN {{ concentration }} <= 150 THEN
            ((200 - 151) / (150 - 81)) * ({{ concentration }} - 81) + 151
        WHEN {{ concentration }} <= 250 THEN
            ((300 - 201) / (250 - 151)) * ({{ concentration }} - 151) + 201
        WHEN {{ concentration }} <= 350 THEN
            ((400 - 301) / (350 - 251)) * ({{ concentration }} - 251) + 301
        WHEN {{ concentration }} <= 500 THEN
            ((500 - 401) / (500 - 351)) * ({{ concentration }} - 351) + 401
        ELSE NULL
    END
{% endmacro %}

{% macro calculate_aqi_vn_pm10(concentration) %}
    -- Vietnam AQI calculation for PM10 (Decision 1459/QD-TCMT)
    CASE
        WHEN {{ concentration }} <= 50 THEN
            ((50 - 0) / (50 - 0)) * ({{ concentration }} - 0) + 0
        WHEN {{ concentration }} <= 150 THEN
            ((100 - 51) / (150 - 51)) * ({{ concentration }} - 51) + 51
        WHEN {{ concentration }} <= 250 THEN
            ((150 - 101) / (250 - 151)) * ({{ concentration }} - 151) + 101
        WHEN {{ concentration }} <= 350 THEN
            ((200 - 151) / (350 - 251)) * ({{ concentration }} - 251) + 151
        WHEN {{ concentration }} <= 420 THEN
            ((300 - 201) / (420 - 351)) * ({{ concentration }} - 351) + 201
        WHEN {{ concentration }} <= 500 THEN
            ((400 - 301) / (500 - 421)) * ({{ concentration }} - 421) + 301
        WHEN {{ concentration }} <= 600 THEN
            ((500 - 401) / (600 - 501)) * ({{ concentration }} - 501) + 401
        ELSE NULL
    END
{% endmacro %}

{% macro calculate_aqi_vn_o3(concentration) %}
    -- Vietnam AQI calculation for O3 (1h, µg/m³)
    CASE
        WHEN {{ concentration }} <= 160 THEN
            ((50 - 0) / (160 - 0)) * ({{ concentration }} - 0) + 0
        WHEN {{ concentration }} <= 200 THEN
            ((100 - 51) / (200 - 161)) * ({{ concentration }} - 161) + 51
        WHEN {{ concentration }} <= 240 THEN
            ((150 - 101) / (240 - 201)) * ({{ concentration }} - 201) + 101
        WHEN {{ concentration }} <= 280 THEN
            ((200 - 151) / (280 - 241)) * ({{ concentration }} - 241) + 151
        WHEN {{ concentration }} <= 400 THEN
            ((300 - 201) / (400 - 281)) * ({{ concentration }} - 281) + 201
        WHEN {{ concentration }} <= 500 THEN
            ((400 - 301) / (500 - 401)) * ({{ concentration }} - 401) + 301
        WHEN {{ concentration }} <= 600 THEN
            ((500 - 401) / (600 - 501)) * ({{ concentration }} - 501) + 401
        ELSE NULL
    END
{% endmacro %}

{% macro calculate_aqi_vn_so2(concentration) %}
    -- Vietnam AQI calculation for SO2 (1h, µg/m³)
    CASE
        WHEN {{ concentration }} <= 125 THEN
            ((50 - 0) / (125 - 0)) * ({{ concentration }} - 0) + 0
        WHEN {{ concentration }} <= 350 THEN
            ((100 - 51) / (350 - 126)) * ({{ concentration }} - 126) + 51
        WHEN {{ concentration }} <= 550 THEN
            ((150 - 101) / (550 - 351)) * ({{ concentration }} - 351) + 101
        WHEN {{ concentration }} <= 800 THEN
            ((200 - 151) / (800 - 551)) * ({{ concentration }} - 551) + 151
        WHEN {{ concentration }} <= 1600 THEN
            ((300 - 201) / (1600 - 801)) * ({{ concentration }} - 801) + 201
        WHEN {{ concentration }} <= 2100 THEN
            ((400 - 301) / (2100 - 1601)) * ({{ concentration }} - 1601) + 301
        WHEN {{ concentration }} <= 2630 THEN
            ((500 - 401) / (2630 - 2101)) * ({{ concentration }} - 2101) + 401
        ELSE NULL
    END
{% endmacro %}

{% macro calculate_aqi_vn_no2(concentration) %}
    -- Vietnam AQI calculation for NO2 (1h, µg/m³)
    CASE
        WHEN {{ concentration }} <= 40 THEN
            ((50 - 0) / (40 - 0)) * ({{ concentration }} - 0) + 0
        WHEN {{ concentration }} <= 80 THEN
            ((100 - 51) / (80 - 41)) * ({{ concentration }} - 41) + 51
        WHEN {{ concentration }} <= 180 THEN
            ((150 - 101) / (180 - 81)) * ({{ concentration }} - 81) + 101
        WHEN {{ concentration }} <= 280 THEN
            ((200 - 151) / (280 - 181)) * ({{ concentration }} - 181) + 151
        WHEN {{ concentration }} <= 565 THEN
            ((300 - 201) / (565 - 281)) * ({{ concentration }} - 281) + 201
        WHEN {{ concentration }} <= 750 THEN
            ((400 - 301) / (750 - 566)) * ({{ concentration }} - 566) + 301
        WHEN {{ concentration }} <= 940 THEN
            ((500 - 401) / (940 - 751)) * ({{ concentration }} - 751) + 401
        ELSE NULL
    END
{% endmacro %}

{% macro calculate_aqi_vn_co(concentration) %}
    -- Vietnam AQI calculation for CO (1h, µg/m³)
    -- Note: 1 ppm CO ~ 1145 µg/m³
    CASE
        WHEN {{ concentration }} <= 10000 THEN
            ((50 - 0) / (10000 - 0)) * ({{ concentration }} - 0) + 0
        WHEN {{ concentration }} <= 30000 THEN
            ((100 - 51) / (30000 - 10001)) * ({{ concentration }} - 10001) + 51
        WHEN {{ concentration }} <= 45000 THEN
            ((150 - 101) / (45000 - 30001)) * ({{ concentration }} - 30001) + 101
        WHEN {{ concentration }} <= 60000 THEN
            ((200 - 151) / (60000 - 45001)) * ({{ concentration }} - 45001) + 151
        WHEN {{ concentration }} <= 90000 THEN
            ((300 - 201) / (90000 - 60001)) * ({{ concentration }} - 60001) + 201
        WHEN {{ concentration }} <= 120000 THEN
            ((400 - 301) / (120000 - 90001)) * ({{ concentration }} - 90001) + 301
        WHEN {{ concentration }} <= 150000 THEN
            ((500 - 401) / (150000 - 120001)) * ({{ concentration }} - 120001) + 401
        ELSE NULL
    END
{% endmacro %}

{% macro calculate_aqi_vn(pollutant, concentration) %}
    -- Main macro for Vietnam AQI (Decision 1459/QD-TCMT)
    CASE
        WHEN LOWER({{ pollutant }}) = 'pm25' THEN {{ calculate_aqi_vn_pm25(concentration) }}
        WHEN LOWER({{ pollutant }}) = 'pm10' THEN {{ calculate_aqi_vn_pm10(concentration) }}
        WHEN LOWER({{ pollutant }}) = 'o3' THEN {{ calculate_aqi_vn_o3(concentration) }}
        WHEN LOWER({{ pollutant }}) = 'no2' THEN {{ calculate_aqi_vn_no2(concentration) }}
        WHEN LOWER({{ pollutant }}) = 'so2' THEN {{ calculate_aqi_vn_so2(concentration) }}
        WHEN LOWER({{ pollutant }}) = 'co' THEN {{ calculate_aqi_vn_co(concentration) }}
        ELSE NULL
    END
{% endmacro %}
