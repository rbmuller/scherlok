{% test not_null_proportion(model, column_name, max_rate=0.05) %}
{#
    Fails when the proportion of NULLs in a column exceeds `max_rate`.
    Default: 5% — override per-column in schema.yml.

    Usage:
      columns:
        - name: email
          tests:
            - scherlok.not_null_proportion:
                max_rate: 0.01
#}

with metrics as (
    select
        count(*) as total_rows,
        sum(case when {{ column_name }} is null then 1 else 0 end) as null_count
    from {{ model }}
)

select total_rows, null_count
from metrics
where total_rows > 0
  and cast(null_count as {{ dbt.type_float() }}) / cast(total_rows as {{ dbt.type_float() }}) > {{ max_rate }}

{% endtest %}
