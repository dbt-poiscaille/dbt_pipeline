-- depends_on: {{ ref('old_relations') }}
{% if execute %}
    {% set commands = dbt_utils.get_column_values(ref('old_relations'), 'command') %}
    {% set database_schema = target.database ~ "." ~ target.schema %}

    {% if commands is not none %}
        {% for command in commands %}
            {% do log('DROPPED ' ~ command, info=true) %}
            {% set drop_query %}
                DROP {{command}};
            {% endset %}
            {% do run_query(drop_query) %}
        {% endfor %}

        {% do log(commands|length ~ " old tables/views were dropped from " ~ database_schema, info=true) %}
    {% endif %}

{% endif %}

SELECT * FROM {{ ref('old_relations') }}