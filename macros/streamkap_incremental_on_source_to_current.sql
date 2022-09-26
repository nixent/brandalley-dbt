{% macro streamkap_incremental_on_source_to_current(source_name, source_schema='streamkap', id_field=1, time_field='_streamkap_source_ts_ms', deleted_field='__deleted') -%}

SELECT {{dbt_utils.star(source(
        source_schema,
        source_name
    ))}}
FROM     
    {{ source(
        source_schema,
        source_name
    ) }}
WHERE 1=1
and cast({{deleted_field}} as boolean) = false
{% if is_incremental() -%}
and _streamkap_source_ts_ms > (select max({{time_field}}) from {{this}})  
{%- endif %}
QUALIFY ROW_NUMBER() OVER (PARTITION BY {{id_field}} ORDER BY {{time_field}} DESC) = 1

{%- endmacro -%}