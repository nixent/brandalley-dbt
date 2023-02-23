{# /* 
    This macro is used to automatically drop any tables in BigQuery that were both created and last queried over 30 days ago, and any schemas created more than 7 days ago with no tables in them.
*/ #}

{% macro drop_unused_tables() %}

{% set tables_sql %}
    with last_queries as (
    select
        start_time,
        rt.table_id,
        rt.dataset_id
    from region-europe-west2.INFORMATION_SCHEMA.JOBS, unnest(referenced_tables) as rt
    -- exclude dbt tests
    where not query like '%count(*) as failures%'
    qualify row_number() over (partition by table_id, dataset_id order by start_time desc) = 1
    )

    select 
        t.table_schema,
        t.table_name,
        t.creation_time,
        lq.start_time,
        date_diff(current_date, date(coalesce(lq.start_time, t.creation_time)), day) as days_since_last_interaction
    from region-europe-west2.INFORMATION_SCHEMA.TABLES t
    left join last_queries lq 
        on t.table_schema = lq.dataset_id and t.table_name = lq.table_id
    where t.table_schema not in ('streamkap', 'streamkap_current')
        and date_diff(current_date, date(coalesce(lq.start_time, t.creation_time)), day) >= 30
{% endset %}

{% set schemas_sql %}
    with table_counts as (
        select
            table_schema,
            count(table_name) as tables_in_schema
    from region-europe-west2.INFORMATION_SCHEMA.TABLES
    group by 1
    )

    select 
        s.schema_name,
        coalesce(s.last_modified_time, s.creation_time) as last_interacted_at,
        tc.tables_in_schema
    from region-europe-west2.INFORMATION_SCHEMA.SCHEMATA s
    left join table_counts tc
        on tc.table_schema = s.schema_name
    where date_diff(current_date, date(coalesce(s.last_modified_time, s.creation_time)), day) >= 7
        and tc.tables_in_schema is null
{% endset %}

{% set table_results = run_query(tables_sql) %}

{% for row in table_results %}
    {% set drop_table_sql = 'drop table if exists ' ~ row[0] ~ '.' ~ row[1] ~ ';' %}
    {{ log(drop_table_sql, True) }}
{% endfor %}

{% set schema_results = run_query(schemas_sql) %}

{% for row in schema_results %}
    {% set drop_schema_sql = 'drop schema if exists ' ~ row[0] ~ ';' %}
    {{ log(drop_schema_sql, True) }}
{% endfor %}

{% endmacro %}