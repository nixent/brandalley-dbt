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
        case 
            when t.table_type in ('BASE TABLE', 'CLONE') then 'table'
            else t.table_type
        end as table_type,
        t.creation_time,
        lq.start_time,
        date_diff(current_date, date(coalesce(lq.start_time, t.creation_time)), day) as days_since_last_interaction
    from region-europe-west2.INFORMATION_SCHEMA.TABLES t
    left join last_queries lq 
        on t.table_schema = lq.dataset_id and t.table_name = lq.table_id
    where (t.table_schema not in ('streamkap', 'streamkap_fr', 'streamkap_reactor', 'streamkap_m2' 'streamkap_current', 'prod', 'analytics_280799085')
        and date_diff(current_date, date(coalesce(lq.start_time, t.creation_time)), day) >= 30
        and table_type in ('BASE TABLE', 'VIEW', 'CLONE'))
        and table_name not like 'ifg_%'
        or (t.table_schema like '%dbt_cloud_pr%' and date_diff(current_date, date(coalesce(lq.start_time, t.creation_time)), day) >= 3)
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
    where 
        (lower(s.schema_name) not like 'ml_%' and lower(s.schema_name) not in ('kmeans', 'kmeans1', 'propensity'))
        and ((date_diff(current_date, date(coalesce(s.last_modified_time, s.creation_time)), day) >= 7 and tc.tables_in_schema is null)
            or (s.schema_name like '%dbt_cloud_pr%' and date_diff(current_date, date(coalesce(s.last_modified_time, s.creation_time)), day) >= 3))
{% endset %}

{% set table_results = run_query(tables_sql) %}

{% if table_results %}
{% for row in table_results %}
    {% set drop_table_sql = 'drop ' ~ row[2] ~ ' if exists ' ~ row[0] ~ '.' ~ row[1] ~ ';' %}
    {{ log(drop_table_sql, True) }}
    {% do run_query(drop_table_sql) %}
{% endfor %}
{% endif %}

{% set schema_results = run_query(schemas_sql) %}

{% if schema_results %}
{% for row in schema_results %}
    {% set drop_schema_sql = 'drop schema if exists ' ~ row[0] ~ ';' %}
    {{ log(drop_schema_sql, True) }}
    {% do run_query(drop_schema_sql) %}
{% endfor %}
{% endif %}

{% endmacro %}