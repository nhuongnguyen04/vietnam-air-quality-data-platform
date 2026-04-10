{% macro filter_vietnam_aqicn() %}
    station_name LIKE '%Vietnam%'
{% endmacro %}

{% macro filter_vietnam_openaq() %}
    country_code = 'VN'
{% endmacro %}

