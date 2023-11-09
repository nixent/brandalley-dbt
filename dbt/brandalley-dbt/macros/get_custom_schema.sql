{% macro generate_schema_name(custom_schema_name, node) -%}

    {%- set default_schema = target.schema -%}
    {%- if target.name.startswith('testing') -%}

        {{ default_schema }}_{{ custom_schema_name | trim }}

    {%- elif (env_var('DBT_LOCATION', false) == 'EU' or target.name == 'prod_eu') and (node.name == 'email_events' or node.name == 'email_campaigns_latest_version') -%}

        emarsys_eu_dbt

    {%- elif env_var('DBT_LOCATION', false) == 'EU' or target.name == 'prod_eu' -%}

        emarsys_eu

    {%- elif custom_schema_name is none -%}

        {{ default_schema }}

    {%- else -%}

        {{ custom_schema_name | trim }}

    {%- endif -%}

{%- endmacro %}