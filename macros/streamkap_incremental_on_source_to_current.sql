--not ideal but if you want to not use an offset field in addition to the required order time field, pass in an empty string

{% macro streamkap_incremental_on_source_to_current(
        source_name,
        source_schema = 'streamkap',
        id_field = 1,
        order_time_field = '_streamkap_ts_ms',
        order_offset_field = '_streamkap_offset',
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
AND {{order_time_field}} > (
    SELECT
        MAX(
            {{ order_time_field }}
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
    {%- if order_offset_field == '' %}
    ORDER BY 
        {{ order_time_field }}
    {%- else %}
    ORDER BY 
        {{ order_time_field }} DESC, {{order_offset_field}} DESC
    {%- endif %}
) = 1
{%- endmacro -%}
