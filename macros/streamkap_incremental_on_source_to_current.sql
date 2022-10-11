{% macro streamkap_incremental_on_source_to_current(
        source_name,
        source_schema = 'streamkap',
        id_field = 1,
        order_fields = ['_streamkap_ts_ms', '_streamkap_offset'],
        deleted_field='__deleted'
    ) -%}
SELECT
    {{ dbt_utils.star(
        source(
            source_schema,
            source_name
        ),
        except = [deleted_field]
    ) }},
    cast({{deleted_field}} as boolean) as {{deleted_field}}
FROM
    {{ source(
        source_schema,
        source_name
    ) }}
WHERE
    1 = 1
{% if is_incremental() -%}
AND _streamkap_source_ts_ms > (
    SELECT
        MAX(
            {{ time_field }}
        )
    FROM
        {{ this }}
)
{%- endif %}
qualify ROW_NUMBER() over (
    {%- if id_field is string %}
    PARTITION BY 
        {{ id_field }}
    {%- else %}
    PARTITION BY 
        {{ id_field | join(', ') }}
    {%- endif %}
    {%- if order_fields is string %}
    ORDER BY 
        {{ order_fields }}
    {%- else %}
    ORDER BY 
        {{ order_fields | join(' DESC, ') }} DESC
    {%- endif %}
) = 1
{%- endmacro -%}
