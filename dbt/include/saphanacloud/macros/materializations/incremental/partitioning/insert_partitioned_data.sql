{% macro insert_partitioned_data(sql, relation, filter_conditions) -%}
    {{ return(adapter.dispatch('insert_partitioned_data','partitioning')(sql, relation, filter_conditions)) }}
{% endmacro %}


{% macro default__insert_partitioned_data(sql, relation, filter_conditions) -%}
    {{ exceptions.raise_compiler_error("`insert_partitioned_data has not been implemented for this adapter.") }}
{%- endmacro %}


{% macro saphanacloud__insert_partitioned_data(sql, relation, filter_conditions) -%}

    -- get target columns in empty table
    {% set dest_columns = adapter.get_columns_in_relation(relation) %}

    -- set row counter
    {%- set loop_vars = {'sum_rows_inserted': 0} -%}


    {% for filter_condition in filter_conditions %}

        -- print start logs
        {%- set msg = modules.datetime.datetime.utcnow().strftime('%H:%M:%S') ~ "  Model " ~ this ~ ":  Running for partition " ~ loop.index ~ "; filter conditions: " ~ filter_condition|map(attribute = 'expression')|join(', ')  -%}
        {{ print(msg) }}

        -- generate sql query for the transformation
        {% set sql_for_partition = get_partitioned_sql(sql, filter_condition) %}

        -- insert data into table
        {% set name = 'main-' ~ filter_condition %}
        {% call statement(name, fetch_result=True) -%}
            
            insert into {{ relation }} ({{  get_quoted_csv(dest_columns | map(attribute="name")) }}) 
            (
                {{ sql_for_partition }}
            )

        {%- endcall %}

        -- get rows inserted by partition
        {% set result = load_result('main-' ~ filter_condition) %}
        {% set rows_inserted = result['response']['rows_affected'] %}
        {% set sum_rows_inserted = loop_vars['sum_rows_inserted'] + rows_inserted %}
        {% if loop_vars.update({'sum_rows_inserted': sum_rows_inserted}) %} {% endif %}  



        -- print ends logs
        {%- set msg = modules.datetime.datetime.utcnow().strftime('%H:%M:%S') ~ "  Model " ~ this ~ ":  Ran for partition " ~ loop.index ~ "; " ~ rows_inserted ~ " record(s) inserted" -%}
        {{ print(msg) }}
        
    {% endfor %}

    {%- set status_string = "INSERT " ~ loop_vars['sum_rows_inserted'] -%}

    {{ return(status_string) }}
    

{%- endmacro %}