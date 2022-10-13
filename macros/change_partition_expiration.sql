{% macro change_partition_exp(schema, expiration_time_in_days) %}
    {% set relations = dbt_utils.get_relations_by_pattern(
        schema_pattern=schema,
        table_pattern='%'
    ) %}

    {% for relation in relations %}
        {% set sql %}
            ALTER TABLE {{relation}} SET OPTIONS (partition_expiration_days = SAFE_CAST('{{expiration_time_in_days}}' AS INT64));
        {% endset %}
        {{ log(sql, info=True)}}
        {% do run_query(sql) %}
    {% endfor %}
{% endmacro %}

--example run-operation : dbt run-operation change_partition_exp --args '{schema: streamkap, expiration_time_in_days: null}'
