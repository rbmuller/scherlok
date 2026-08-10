{% test unique_proportion(model, column_name, min_rate=0.9) %}
{#
    Fails when the ratio of distinct values to total rows drops below
    `min_rate`. Catches cardinality collapses (e.g. a join producing
    duplicates, or an enum column losing variety).

    Usage:
      columns:
        - name: user_id
          tests:
            - scherlok.unique_proportion:
                min_rate: 0.99
#}

with metrics as (
    select
        count(*) as total_rows,
        count(distinct {{ column_name }}) as distinct_count
    from {{ model }}
)

select total_rows, distinct_count
from metrics
where total_rows > 0
  and cast(distinct_count as {{ dbt.type_float() }}) / cast(total_rows as {{ dbt.type_float() }}) < {{ min_rate }}

{% endtest %}
