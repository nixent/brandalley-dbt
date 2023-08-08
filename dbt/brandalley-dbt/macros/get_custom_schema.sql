{% macro generate_schema_name(custom_schema_name, node) -%}

    {%- set default_schema = target.schema -%}
    {%- if target.name.startswith('testing') -%}

        {{ default_schema }}_{{ custom_schema_name | trim }}

    {%- elif env_var('DBT_LOCATION', 'EU') == 'EU' -%}

        emarsys_eu

    {%- elif custom_schema_name is none -%}

        {{ default_schema }}

    {%- else -%}

        {{ custom_schema_name | trim }}

    {%- endif -%}

{%- endmacro %}