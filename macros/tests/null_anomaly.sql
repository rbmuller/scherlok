{% test null_anomaly(model, column_name, sensitivity=3.0, min_samples=5) %}
{#
    Detects sudden spikes in NULL rate for a column using Shewhart control
    limits against historical baselines. Requires scherlok_column_metrics.

    Usage:
      columns:
        - name: email
          tests:
            - scherlok.null_anomaly:
                sensitivity: 2.5
#}

with current_metrics as (
    select
        count(*) as total_rows,
        sum(case when {{ column_name }} is null then 1 else 0 end) as null_count,
        case
            when count(*) > 0
            then cast(sum(case when {{ column_name }} is null then 1 else 0 end) as {{ dbt.type_float() }})
                 / cast(count(*) as {{ dbt.type_float() }})
            else 0
        end as null_rate
    from {{ model }}
),

historical as (
    select null_rate
    from {{ ref('scherlok_column_metrics') }}
    where table_name = '{{ model.name }}'
      and column_name = '{{ column_name }}'
      and measured_at < {{ dbt.current_timestamp() }}
    order by measured_at desc
    limit 30
),

stats as (
    select
        avg(null_rate) as mean_rate,
        stddev(null_rate) as stddev_rate,
        count(*) as sample_count
    from historical
)

select
    c.null_rate as current_null_rate,
    s.mean_rate,
    s.stddev_rate,
    s.sample_count
from current_metrics c
cross join stats s
where s.sample_count >= {{ min_samples }}
  and s.stddev_rate > 0
  and c.null_rate > s.mean_rate + {{ sensitivity }} * s.stddev_rate

{% endtest %}
