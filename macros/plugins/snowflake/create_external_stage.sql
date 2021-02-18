{% macro get_external_stage_name(relation) %}
    {{return(relation.database + '.'+ relation.schema + '.STAGE_' + relation.identifier + "_DBT")}}
{% endmacro %}

{% macro create_external_stage(source_node) %}
    {% set stage_storage_integration = "S3_INTEGRATION_PROD" %}
    {{ log("create stage (execute mode :" ~ execute ~ ")")}}
    {{ log(source_node)}}
    {% set external = source_node.external %}
    
    {#{%- set partition_names = external.partitions | map(attribute='name') -%}#}

    {% set stage_url = "s3://" + env_var('S3_BUCKET') + "/raw/" + source_node.source_name + "/" +  source_node.name + "/" %}

    {# {% if not external.partitions|length %} #}
    {% if external.partitions is none %}
        {{log("xternal.partitions is not defined")}}
        {% set stage_url = stage_url + env_var('DATE_TO_LOAD') + "/"  %}
    {% else %}
        {{log("xternal.partitions is defined")}}
        {{log(external.partitions)}}
    {% endif %}

    {# {%- set stage_url = "s3://" + env_var('S3_BUCKET') + "/" + external.s3_prefix -%} #}
    
    {%- set relation = source(source_node.source_name, source_node.name) -%}
    
    CREATE SCHEMA IF NOT EXISTS {{relation.database}}.{{relation.schema}};
    CREATE OR REPLACE STAGE {{get_external_stage_name(relation)}} URL='{{stage_url}}' storage_integration = {{stage_storage_integration}};
    
{% endmacro %}