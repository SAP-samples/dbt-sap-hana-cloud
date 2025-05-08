{% macro get_partitioned_sql(sql, filter_conditions) -%}
    {{ return(adapter.dispatch('get_partitioned_sql','partitioning')(sql, filter_conditions)) }}
{% endmacro %}


{% macro default__get_partitioned_sql(sql, filter_conditions) -%}
    {{ exceptions.raise_compiler_error("`get_partitioned_sql has not been implemented for this adapter.") }}
{%- endmacro %}


{% macro saphanacloud__get_partitioned_sql(sql, filter_conditions) -%}

    {% set sql_with_wrapper %}
    select 
        *
    from (
        {{ sql }}
    ) t
    where 1=1
    {% endset %}


    {% set partitioned_sql = {'query': sql_with_wrapper} %}

    {% for filter_condition in filter_conditions %}

        {% if filter_condition.partition_name %}
            {% do partitioned_sql.update({'query': partitioned_sql['query']|replace(filter_condition.partition_name, '(' ~ filter_condition.expression ~ ')') }) %}
        {% else %}
            {% do partitioned_sql.update({'query': partitioned_sql['query'] ~ ' and ' ~ filter_condition.expression}) %}
        {% endif %}
        
    {% endfor %}

    {{ return(partitioned_sql.query) }}

{%- endmacro %}