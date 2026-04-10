{% test range(model, column_name, min_value=none, max_value=none) %}

with validation as (
    select
        {{ column_name }} as field
    from {{ model }}
),

validation_errors as (
    select
        field
    from validation
    where 
        {% if min_value is not none and max_value is not none %}
            field < {{ min_value }} or field > {{ max_value }}
        {% elif min_value is not none %}
            field < {{ min_value }}
        {% elif max_value is not none %}
            field > {{ max_value }}
        {% endif %}
)

select *
from validation_errors

{% endtest %}
