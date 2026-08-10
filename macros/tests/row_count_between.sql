{% test row_count_between(model, min_value=1, max_value=none) %}
{#
    Fails when the table's row count is outside [min_value, max_value].
    Omit max_value for "at least N rows" checks.

    Usage:
      models:
        - name: fct_orders
          tests:
            - scherlok.row_count_between:
                min_value: 100
                max_value: 1000000
#}

with row_count as (
    select count(*) as cnt from {{ model }}
)

select cnt
from row_count
where cnt < {{ min_value }}
{% if max_value is not none %}
   or cnt > {{ max_value }}
{% endif %}

{% endtest %}
