{% macro create_empty_table_as(temporary, relation, query_partitions, sql) -%}
    {{ return(adapter.dispatch('create_empty_table_as','partitioning')(temporary, relation, query_partitions, sql)) }}
{% endmacro %}


{% macro default__create_empty_table_as(temporary, relation, query_partitions, sql) -%}
    {{ exceptions.raise_compiler_error("`create_empty_table_as has not been implemented for this adapter.") }}
{%- endmacro %}


{% macro saphanacloud__create_empty_table_as(temporary, relation, query_partitions, sql) -%}
    
    {% set empty_filter = [] %}

    {% for query_partition in query_partitions %}
        {{ empty_filter.append({
            'partition_name': query_partition.name,
            'expression': '1=2'
            }) 
        }}
    {% endfor %}


    {% set tmp_create__empty_table_sql = get_partitioned_sql(sql, empty_filter) %}

    {% set create__empty_table_sql = get_create_table_as_sql(temporary, relation, tmp_create__empty_table_sql) | replace(';', ' WITH NO DATA WITHOUT CONSTRAINT;')  %}

    {% do run_query(create__empty_table_sql) %}

{%- endmacro %}