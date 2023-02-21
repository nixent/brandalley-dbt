{% macro reset_dev_db() %}
{#-
Run using
    $ dbt run-operation reset_dev_db

-#}

{% set tables_in_prod %}
    select 
        table_catalog, 
        table_schema||'.'||table_name as table_name
    from region-europe-west2.INFORMATION_SCHEMA.TABLES 
    where table_type = 'BASE TABLE' and table_schema in ('magento', 'emarsys', 'streamkap_current', 'streamkap')
{% endset %}

{% set results = run_query(tables_in_prod) %}

{% for row in results %}
    {% set clone_table_sql = 'CREATE OR REPLACE TABLE datawarehouse-dev-371019.' ~ row[0] ~ '.' ~ row[1] ~ ' CLONE datawarehouse-358408.' ~ row[0] ~ '.' ~ row[1] ~ ';' %}
    {{ log(clone_table_sql, True) }}
  {% endfor %}

{% endmacro %}
