{% macro streamkap_incremental_on_source_to_current(
        source_name,
        source_schema = 'streamkap',
        id_field = 1,
        time_field = '_streamkap_source_ts_ms', 
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
    PARTITION BY {{ id_field }}
    ORDER BY
        {{ time_field }} DESC
) = 1
{%- endmacro -%}
