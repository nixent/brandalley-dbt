{% macro reset_dev_db() %}
{#-
Run using
    $ dbt run-operation reset_dev_db

-#}

{# Drop all schemas in dev to ensure full data replacement #}
{% set schemas_in_dev %}
    select 
        catalog_name, 
        schema_name
    from datawarehouse-dev-371019.region-europe-west2.INFORMATION_SCHEMA.SCHEMATA
{% endset %}

{% set dev_schema_results = run_query(schemas_in_dev) %}

{% for row in dev_schema_results %}
    {% set drop_schema_sql = 'DROP SCHEMA IF EXISTS `datawarehouse-dev-371019`.' ~ row[1] ~ ' CASCADE;' %}
    {{ log(drop_schema_sql, True) }}
    {% do run_query(drop_schema_sql) %}
{% endfor %}

{# Recreate all prod schemas in dev db #}
{% set schemas_in_prod %}
    select 
        catalog_name, 
        schema_name
    from datawarehouse-358408.region-europe-west2.INFORMATION_SCHEMA.SCHEMATA
    where schema_name not in ('streamkap')
{% endset %}

{% set prod_schema_results = run_query(schemas_in_prod) %}

{% for row in prod_schema_results %}
    {% set create_schema_sql = 'CREATE SCHEMA `datawarehouse-dev-371019`.' ~ row[1] ~ ' OPTIONS(location="europe-west2");' %}
    {{ log(create_schema_sql, True) }}
    {% do run_query(create_schema_sql) %}
{% endfor %}

{# Clone all tables in prod schemas #}
{% set tables_in_prod %}
    select 
        table_catalog, 
        table_schema||'.'||table_name as table_name
    from region-europe-west2.INFORMATION_SCHEMA.TABLES 
    where table_type = 'BASE TABLE' and table_schema not in ('streamkap') and table_name not like '%dbt_tmp%'
{% endset %}

{% set prod_tables_results = run_query(tables_in_prod) %}

{% for row in prod_tables_results %}
    {% set clone_table_sql = 'CREATE OR REPLACE TABLE `datawarehouse-dev-371019`.' ~ row[1] ~ ' CLONE `datawarehouse-358408`.' ~ row[1] ~ ';' %}
    {{ log(clone_table_sql, True) }}
    {% do run_query(clone_table_sql) %}
  {% endfor %}

{% endmacro %}
