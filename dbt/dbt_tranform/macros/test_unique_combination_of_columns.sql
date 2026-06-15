{% test unique_combination_of_columns(model, combination_of_columns, row_filter=None) %}

with validation_errors as (
    select
        {% for column_name in combination_of_columns -%}
            {{ column_name }}{% if not loop.last %},{% endif %}
        {%- endfor %},
        count(*) as duplicate_count
    from {{ model }}
    {% if row_filter %}
    where {{ row_filter }}
    {% endif %}
    group by
        {% for column_name in combination_of_columns -%}
            {{ column_name }}{% if not loop.last %},{% endif %}
        {%- endfor %}
    having count(*) > 1
)

select *
from validation_errors

{% endtest %}
