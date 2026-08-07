{% test volume_anomaly(model, sensitivity=3.0, min_samples=5) %}
{#
    Statistical anomaly detection on row count using Shewhart control limits.
    Requires the scherlok_metrics model to be running (captures row count
    history). Passes silently during the baseline period (< min_samples runs).

    Fails when:  current_count < mean - k*stddev  OR  current_count > mean + k*stddev

    Usage:
      models:
        - name: fct_orders
          tests:
            - scherlok.volume_anomaly:
                sensitivity: 2.5
                min_samples: 7
#}

with current_count as (
    select count(*) as row_count from {{ model }}
),

historical as (
    select row_count
    from {{ ref('scherlok_metrics') }}
    where table_name = '{{ model.name }}'
      and measured_at < {{ dbt.current_timestamp() }}
    order by measured_at desc
    limit 30
),

stats as (
    select
        avg(cast(row_count as {{ dbt.type_float() }})) as mean_count,
        stddev(cast(row_count as {{ dbt.type_float() }})) as stddev_count,
        count(*) as sample_count
    from historical
)

select
    c.row_count as current_row_count,
    s.mean_count,
    s.stddev_count,
    s.sample_count
from current_count c
cross join stats s
where s.sample_count >= {{ min_samples }}
  and s.stddev_count > 0
  and (
    c.row_count < s.mean_count - {{ sensitivity }} * s.stddev_count
    or c.row_count > s.mean_count + {{ sensitivity }} * s.stddev_count
  )

{% endtest %}
