{{
    config(
        materialized='incremental',
        schema=var('scherlok_schema', 'scherlok'),
        enabled=var('scherlok_column_metrics_enabled', false)
    )
}}

{#
    Captures per-column null rates for columns with null_anomaly tests.
    Used by null_anomaly test for statistical drift detection.

    Only profiles columns from models that have null_anomaly tests
    configured — to avoid expensive full-table scans on every column.
    If no models are found, returns an empty result set.
#}

{% set exclude_models = var('scherlok_exclude_models', []) %}
{% set include_only = var('scherlok_include_models', []) %}

{% set null_anomaly_targets = [] %}

{# Generic test nodes carry the model and column needed for selective profiling. #}
{% for test_node in graph.nodes.values() %}
    {% if test_node.resource_type == 'test' %}
        {% set test_metadata = test_node.test_metadata | default(none, true) %}
        {% if test_metadata is not none
            and test_metadata.name | default('', true) == 'null_anomaly' %}
            {% set test_namespace = test_metadata.namespace | default(none, true) %}
            {% if test_namespace == 'scherlok'
                or (test_namespace is none
                    and test_node.package_name | default('', true) == 'scherlok') %}
                {% set attached_node_id = test_node.attached_node | default(none, true) %}
                {% if attached_node_id is none %}
                    {% set depends_on = test_node.depends_on | default({}, true) %}
                    {% set dependency_ids = depends_on.nodes | default([], true) %}
                    {% set model_dependency_ids = [] %}
                    {% for dependency_id in dependency_ids %}
                        {% set dependency_node = graph.nodes.get(dependency_id) %}
                        {% if dependency_node is not none
                            and dependency_node.resource_type == 'model'
                            and dependency_node.package_name != 'scherlok' %}
                            {% do model_dependency_ids.append(dependency_id) %}
                        {% endif %}
                    {% endfor %}
                    {% if model_dependency_ids | length == 1 %}
                        {% set attached_node_id = model_dependency_ids[0] %}
                    {% endif %}
                {% endif %}

                {% set column_name = test_node.column_name | default(none, true) %}
                {% if column_name is none %}
                    {% set test_kwargs = test_metadata.kwargs | default({}, true) %}
                    {% set column_name = test_kwargs.column_name | default(none, true) %}
                {% endif %}

                {% if attached_node_id is not none and column_name is not none %}
                    {% set target = {
                        'model_id': attached_node_id,
                        'column_name': column_name
                    } %}
                    {% if target not in null_anomaly_targets %}
                        {% do null_anomaly_targets.append(target) %}
                    {% endif %}
                {% endif %}
            {% endif %}
        {% endif %}
    {% endif %}
{% endfor %}

{% set queries = [] %}

{% for node in graph.nodes.values() %}
    {% if node.resource_type == 'model'
        and node.package_name != 'scherlok'
        and node.config.materialized in ['table', 'incremental', 'view', 'materialized_view']
        and node.name not in exclude_models
        and (include_only | length == 0 or node.name in include_only) %}

        {% set model_id = node.unique_id | default(
            'model.' ~ node.package_name ~ '.' ~ node.name,
            true
        ) %}
        {% set target_columns = [] %}
        {% for target in null_anomaly_targets %}
            {% if target.model_id == model_id and target.column_name not in target_columns %}
                {% do target_columns.append(target.column_name) %}
            {% endif %}
        {% endfor %}

        {% if target_columns | length > 0 %}
            {% set rel = adapter.get_relation(
                database=node.database,
                schema=node.schema,
                identifier=node.alias or node.name
            ) %}

            {% if rel is not none %}
                {% set columns = adapter.get_columns_in_relation(rel) %}
                {% for col in columns %}
                    {% set is_target = namespace(found=false) %}
                    {% for target_column in target_columns %}
                        {% if col.name == target_column or col.name | lower == target_column | lower %}
                            {% set is_target.found = true %}
                        {% endif %}
                    {% endfor %}

                    {% if is_target.found %}
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
                    {% endif %}
                {% endfor %}
            {% endif %}
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
