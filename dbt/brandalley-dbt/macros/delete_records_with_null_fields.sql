{% macro delete_records_with_null_fields(schema, null_fields) %}
    {% set relations = dbt_utils.get_relations_by_pattern(
        schema_pattern=schema,
        table_pattern='%'
    ) %}

    {% for relation in relations %}
        {% set sql %}
            DELETE {{relation}} WHERE 1 = 1
            {% for null_field in null_fields -%}
                and {{null_field}} is null
            {% endfor -%}
        {% endset %}
        {{ log(sql, info=True)}}
        {% do run_query(sql) %}
    {% endfor %}
{% endmacro %}

--{{ delete_records_with_null_fields('streamkap', ['_streamkap_ts_ms','_streamkap_offset'])}}

--example run-operation : dbt run-operation delete_records_with_null_fields --args '{schema: streamkap, null_fields: ['_streamkap_ts_ms','_streamkap_offset']}'
