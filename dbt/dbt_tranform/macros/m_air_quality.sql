{% macro get_main_pollutant(pm25, pm10, co, no2, so2, o3) %}
    case 
        when {{ pm25 }} >= {{ pm10 }} and {{ pm25 }} >= {{ co }} and {{ pm25 }} >= {{ no2 }} and {{ pm25 }} >= {{ so2 }} and {{ pm25 }} >= {{ o3 }} then 'pm25'
        when {{ pm10 }} >= {{ co }} and {{ pm10 }} >= {{ no2 }} and {{ pm10 }} >= {{ so2 }} and {{ pm10 }} >= {{ o3 }} then 'pm10'
        when {{ co }} >= {{ no2 }} and {{ co }} >= {{ so2 }} and {{ co }} >= {{ o3 }} then 'co'
        when {{ no2 }} >= {{ so2 }} and {{ no2 }} >= {{ o3 }} then 'no2'
        when {{ so2 }} >= {{ o3 }} then 'so2'
        else 'o3'
    end
{% endmacro %}
