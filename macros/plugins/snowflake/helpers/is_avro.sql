{% macro is_avro(file_format) %}

    {% set ff_ltrimmed = file_format|lower|replace(' ','') %}

    {% if 'type=' in ff_ltrimmed %}
    
        {% if 'type=avro' in ff_ltrimmed %}

            {{return(true)}}

        {% else %}

            {{return(false)}}
            
        {% endif %}
    {% else %}

        {{return(false)}}

    {% endif %}

{% endmacro %}
