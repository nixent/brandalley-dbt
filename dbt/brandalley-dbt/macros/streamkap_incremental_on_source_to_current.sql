{# /* 
    This macro is used to deduplicate the streamkap current (CDC changes of Magento tables), and gives the latest state of the table in Magento.

    Required parameters are:
        - source_name                   (name of the streamkap table to be deduped)
        - id_field                      (unique key to dedupe on) 

    Optional parameters are:
        - source_schema                 (BQ schema of the table to dedupe)
        - insert_time_field             (insert timestamp to run incremental logic on)
        - order_time_field              (timestamp to order deduplication on)
        - order_offset_field            (offset to order deduplication on after order_time_field)
        - deleted_field                 (column used by streamkap to describe if the row has been deleted from Magento, and hence should be removed from the latest state table)

    NOTE: It's not ideal but if you want to not use an offset field in addition to the required order time field, pass in an empty string
*/ #}

{% macro streamkap_incremental_on_source_to_current(
        source_name,
        id_field,
        source_schema = 'streamkap',
        insert_time_field = '_streamkap_ts_ms',
        order_time_field = '_streamkap_source_ts_ms',
        order_offset_field = '_streamkap_offset',
        deleted_field='__deleted'
    ) -%}

{% if '/streamkap_current_fr/' in model.path %}
    {% set source_schema = 'streamkap_fr' %}
{% endif %}

{% if '/streamkap_current_reactor/' in model.path %}
    {% set source_schema = 'streamkap_reactor' %}
{% endif %}

SELECT
    {{ dbt_utils.star(
        source(
            source_schema,
            source_name
        ),
        except = [deleted_field]
    ) }},
    cast({{deleted_field}} as boolean) as {{deleted_field}},
    current_timestamp                  as bq_last_processed_at
FROM
    {{ source(
        source_schema,
        source_name
    ) }}
WHERE
    1 = 1
{% if is_incremental() -%}
AND {{insert_time_field}} >= ((
    SELECT
        MAX(
            {{ insert_time_field }}
        )
    FROM
        {{ this }}
) - 1000000)
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
        {{ order_time_field }} DESC
    {%- else %}
    ORDER BY 
        {{ order_time_field }} DESC, {{order_offset_field}} DESC
    {%- endif %}
) = 1
{%- endmacro -%}
