{% macro snowflake__create_external_table(source_node) %}
    {{ log("create external table (execute mode :" ~ execute ~ ")")}}
    {%- set columns = source_node.columns.values() -%}
    {%- set external = source_node.external -%}
    {%- set partitions = external.partitions -%}

    {%- set is_csv = dbt_external_tables.is_csv(external.file_format) -%}
    {%- set is_avro = dbt_external_tables.is_avro(external.file_format) -%}
    {%- set relation = source(source_node.source_name, source_node.name) -%}
                        

    
    {%- if not columns and is_avro -%}
        {%- set columns = dbt_external_tables.read_avro_schema(source_node) -%}
        {% set stage_exists = true %}
        {# {%- set columns = [] -%} #}
    {%- endif -%}

    {% if not stage_exists %}
        {{ dbt_external_tables.create_external_stage(source_node) }}
    {% endif %}

    {# https://docs.snowflake.net/manuals/sql-reference/sql/create-external-table.html #}
    {# This assumes you have already created an external stage #}
    create or replace external table {{source(source_node.source_name, source_node.name)}}

    {%- if columns or partitions -%}
    (
        {%- if partitions -%}{%- for partition in partitions %}
            {{partition.name}} {{partition.data_type}} as {{partition.expression}}{{- ',' if not loop.last or columns|length > 0 -}}
        {%- endfor -%}{%- endif -%}
        {%- for column in columns %}
            {%- set column_quoted = adapter.quote(column.name) if column.quote else column.name %}
            {%- set col_expression -%}
                {%- if is_csv -%}nullif(value:c{{loop.index}},''){# special case: get columns by ordinal position #}
                {%- else -%}nullif(value:{{column.name}},''){# standard behavior: get columns by name #}
                {%- endif -%}
            {%- endset %}
            {{column_quoted}} {{column.data_type}} as ({{col_expression}}::{{column.data_type}})
            {{- ',' if not loop.last -}}
        {% endfor %}
    )
    {%- endif -%}
    {% if partitions %} partition by ({{partitions|map(attribute='name')|join(', ')}}) {% endif %}
    location = @{{get_external_stage_name(relation)}}
    {% if external.auto_refresh -%} auto_refresh = {{external.auto_refresh}} {%- endif %}
    {% if external.pattern -%} pattern = '{{external.pattern}}' {%- endif %}
    file_format = {{external.file_format}}
{% endmacro %}
