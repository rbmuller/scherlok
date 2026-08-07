{{
    config(
        materialized='incremental',
        schema=var('scherlok_schema', 'scherlok'),
        enabled=var('scherlok_metrics_enabled', true)
    )
}}

{#
    Auto-discovers all materialized models in the project and captures
    row counts per run. Append-only — each dbt run adds one row per model.

    First run = baseline. Subsequent runs build the history that
    volume_anomaly uses for statistical detection.

    Configure via dbt_project.yml vars:
      scherlok_exclude_models: ['staging_model_to_skip']
      scherlok_include_models: []   # empty = all models
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
            {% set query_part %}
select
    '{{ node.name }}' as table_name,
    '{{ node.schema }}' as schema_name,
    (select count(*) from {{ rel }}) as row_count,
    {{ dbt.current_timestamp() }} as measured_at
            {% endset %}
            {% do queries.append(query_part) %}
        {% endif %}
    {% endif %}
{% endfor %}

{% if queries | length > 0 %}
{{ queries | join('\nunion all\n') }}
{% else %}
select
    cast(null as {{ dbt.type_string() }}) as table_name,
    cast(null as {{ dbt.type_string() }}) as schema_name,
    cast(0 as integer) as row_count,
    {{ dbt.current_timestamp() }} as measured_at
where 1 = 0
{% endif %}
