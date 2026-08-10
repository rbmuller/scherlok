{% test recency(model, column_name, days=1, hours=0) %}
{#
    Fails when the most recent value in a timestamp/date column is older
    than the configured threshold. Catches stale tables.

    Usage:
      columns:
        - name: updated_at
          tests:
            - scherlok.recency:
                days: 2
#}

{% set total_hours = days * 24 + hours %}

with freshness as (
    select max({{ column_name }}) as latest_value
    from {{ model }}
)

select latest_value
from freshness
where latest_value < {{ dbt.dateadd('hour', -1 * total_hours, dbt.current_timestamp()) }}
   or latest_value is null

{% endtest %}
