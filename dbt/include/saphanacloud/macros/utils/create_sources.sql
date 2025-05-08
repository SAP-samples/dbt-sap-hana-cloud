{% macro create_sources() -%}
    {{ return(adapter.dispatch('create_sources','saphanacloud')()) }}
{% endmacro %}


{% macro default__create_sources() -%}
    {{ exceptions.raise_compiler_error("`create_sources has not been implemented for this adapter.") }}
{%- endmacro %}


{% macro saphanacloud__create_sources() -%}

    {#################################
     Get all existing virtual tables
    #################################}


    {% set read_query %}
        SELECT 
            '"' || SCHEMA_NAME || '"."' || TABLE_NAME || '"' AS SOURCE_RELATION_NAME 
        FROM SYS.VIRTUAL_TABLES
    {% endset %}


    {% set existing_virtual_tables = run_query(read_query) %}

    {% if execute %}
    {% set existing_virtual_tables_list = existing_virtual_tables.columns[0].values() %}
    {% else %}
    {% set existing_virtual_tables_list = [] %}
    {% endif %}
    

    {#################################
     Create virtual tables if not already exists
    #################################}

    {% if execute %}

        {% for source in graph.sources.values() %}

            {% if source.source_meta.virtual_table and source.relation_name not in existing_virtual_tables_list %}


                {% set virtualtable -%}
                
                    CREATE VIRTUAL TABLE {{ source.relation_name }} AT "{{ source.source_name }}"."{{ source.source_meta.remote_database }}"."{{ source.source_meta.remote_schema }}"."{{ source.name }}" 

                {%- endset %}

                {{ print("Create virtual table: " ~ source.relation_name) }}

                {% do run_query(virtualtable) %}


            {% endif %}

        {% endfor %}

    {% endif %}



{%- endmacro %}