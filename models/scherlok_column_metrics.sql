{{
    config(
        materialized='incremental',
        schema=var('scherlok_schema', 'scherlok'),
        enabled=var('scherlok_column_metrics_enabled', false)
    )
}}

{#
    Captures per-column null rates for all monitored models.
    Used by null_anomaly test for statistical drift detection.

    Only profiles columns from models that have null_anomaly tests
    configured — to avoid expensive full-table scans on every column.
    If no models are found, returns an empty result set.
#}

{% set exclude_models = var('scherlok_exclude_models', []) %}
{% set include_only = var('scherlok_include_models', []) %}

{% set queries = [] %}

{% for node in graph.nodes.values() %}
    {% if node.resource_type == 'model'
        and node.package_name != 'scherlok'
        and node.config.materialized in ['table', 'incremental', 'view', 'materialized_view']
        and node.name not in exclude_models
        and (include_only | length == 0 or node.name in include_only) %}

        {% set rel = adapter.get_relation(
            database=node.database,
            schema=node.schema,
            identifier=node.alias or node.name
        ) %}

        {% if rel is not none %}
            {% set columns = adapter.get_columns_in_relation(rel) %}
            {% for col in columns %}
                {% set query_part %}
select
    '{{ node.name }}' as table_name,
    '{{ col.name }}' as column_name,
    '{{ col.dtype }}' as column_type,
    (select count(*) from {{ rel }}) as total_rows,
    (select sum(case when {{ adapter.quote(col.name) }} is null then 1 else 0 end) from {{ rel }}) as null_count,
    case
        when (select count(*) from {{ rel }}) > 0
        then cast((select sum(case when {{ adapter.quote(col.name) }} is null then 1 else 0 end) from {{ rel }}) as {{ dbt.type_float() }})
             / cast((select count(*) from {{ rel }}) as {{ dbt.type_float() }})
        else 0
    end as null_rate,
    {{ dbt.current_timestamp() }} as measured_at
                {% endset %}
                {% do queries.append(query_part) %}
            {% endfor %}
        {% endif %}
    {% endif %}
{% endfor %}

{% if queries | length > 0 %}
{{ queries | join('\nunion all\n') }}
{% else %}
select
    cast(null as {{ dbt.type_string() }}) as table_name,
    cast(null as {{ dbt.type_string() }}) as column_name,
    cast(null as {{ dbt.type_string() }}) as column_type,
    cast(0 as integer) as total_rows,
    cast(0 as integer) as null_count,
    cast(0 as {{ dbt.type_float() }}) as null_rate,
    {{ dbt.current_timestamp() }} as measured_at
where 1 = 0
{% endif %}
