{% macro read_avro_schema(source_node) %}
    {# get table columns, based on a colocated .avsc file #}
    {% if execute %}
        {% set stage_name = dbt_external_tables.create_external_stage(source_node) %}

        {% set relation = source(source_node.source_name, source_node.name) %}

        {% set file_format_name = relation.database + '.'+ relation.schema + '.FILE_FORMAT_' + relation.identifier + "_DBT" %}

        {% set avro_schema_query %}
                {{ dbt_external_tables.create_external_stage(source_node) }}
                CREATE FILE FORMAT IF NOT EXISTS {{file_format_name}} TYPE = JSON ;

            with myschema AS (
                select $1 from @{{get_external_stage_name(relation)}}
                (file_format => '{{file_format_name}}', pattern => '.*.avsc')
                QUALIFY (row_number() OVER(order by metadata$filename desc)) = 1
            ), unioned_type AS (    
                SELECT s.value:name as name, s.value:type AS type
                FROM myschema
                , LATERAL FLATTEN(input => myschema.$1:fields) s 
            )
            SELECT TRIM(u.name,'\"') AS name
            ,   CASE TRIM(s.value,'\"')
                WHEN 'long' THEN 'bigint'
                ELSE TRIM(s.value,'\"')
                END AS data_type
            FROM unioned_type u
            , LATERAL FLATTEN(input => u.type) s 
            WHERE s.value <> 'null'
        {% endset %}
        
        {{ log(avro_schema_query)}}
    
        {% set result = run_query(avro_schema_query) %}
        
        {% set columns = result.rename(['name', 'data_type']) %}
    {% else %}
        {% set columns = [] %}
    {% endif %}
    {{return(columns)}}

{% endmacro %}