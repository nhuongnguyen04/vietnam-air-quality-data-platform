{% macro parse_unix_timestamp(unix_timestamp_str) %}
    toDateTime(toInt64OrNull({{ unix_timestamp_str }}))
{% endmacro %}

{% macro parse_iso_timestamp(iso_timestamp_str) %}
    parseDateTimeBestEffortOrNull({{ iso_timestamp_str }})
{% endmacro %}

{% macro parse_date_string(date_str) %}
    toDateOrNull({{ date_str }})
{% endmacro %}

