{% if execute %}
    {% set current_models = [] %}
    {% if graph.nodes.values() is not none %}
        {% for node in graph.nodes.values() %}
            {% do current_models.append(node.name) %}
        {% endfor %}
    {% endif %}
{% endif %}

select
    *,
    concat(
        replace(table_type, 'BASE ', ''),
        concat('`{{target.database}}.{{target.schema}}', '.', concat(table_name, '`'))
    ) as command
from {{ target.database }}.{{ target.schema }}.INFORMATION_SCHEMA.TABLES
where
    upper(table_name) not in (
        {%- for model in current_models -%}
        '{{ model.upper() }}' {%- if not loop.last -%}, {% endif %}
        {%- endfor -%}
    )
