{% macro saphanacloud__create_csv_table(model, agate_table) %}
  {%- set column_override = model['config'].get('column_types', {}) -%}
  {%- set quote_seed_column = model['config'].get('quote_columns', None) -%}

  {%- set default_string_type = 'VARCHAR(5000)' -%} 

  {% set sql %}
    create table {{ this.render() }} (
        {%- for col_name in agate_table.column_names -%}
            {%- set inferred_type = adapter.convert_type(agate_table, loop.index0) | lower -%}
            {%- set column_name = (col_name | string) -%}

            {%- if col_name in column_override %}
              {%- set type = column_override.get(col_name) -%}
            {%- else %}
              {%- if inferred_type == 'string' or inferred_type == 'varchar' -%}
                {%- set type = default_string_type -%}
              {%- else -%}
                {%- set type = inferred_type -%}
              {%- endif -%}
            {%- endif -%}

            {{ adapter.quote_seed_column(column_name, quote_seed_column) }} {{ type }} {%- if not loop.last -%}, {%- endif -%}
        {%- endfor -%}
    )
  {% endset %}

  {% call statement('_') -%}
    {{ sql }}
  {%- endcall %}

  {{ return(sql) }}
{% endmacro %}

 

{% macro saphanacloud__basic_load_csv_rows(model, agate_table) %}

    {% set cols_sql = get_seed_column_quoted_csv(model, agate_table.column_names) %}
    
    {% set statements = [] %}

    {% for row in agate_table.rows %}
        {% set sql %}
            insert into {{ this.render() }} ({{ cols_sql }}) values(
            {%- for column in agate_table.column_names -%}
                 {%- if row[column] is boolean -%}
                  {{ row[column] }} 
              {%- else -%}
                  '{{ row[column] }}' 
              {%- endif -%}
                {%- if not loop.last %},{%- endif %}
            {%- endfor %})
        {% endset %}
        
        {# Add the query to execute the insertion #}
        {% do adapter.add_query(sql, abridge_sql_log=True) %}

        {# Write each insert query to the compiled SQL #}
        {{ write(sql) }}

        {# Collect all queries for logging #}
        {% do statements.append(sql) %}
    {% endfor %}

    {# Return all SQL statements concatenated for the compiled file #}
    {{ return(statements | join(';\n')) }}
{% endmacro %}

{% macro saphanacloud__load_csv_rows(model, agate_table) %}
  {{ return(saphanacloud__basic_load_csv_rows(model, agate_table)) }}
{% endmacro %}


{% macro saphanacloud__reset_csv_table(model, full_refresh, old_relation, agate_table) %}
    {% set sql = "" %}
    {% if full_refresh %}
        {{ adapter.drop_relation(old_relation) }}
        {% set sql = create_csv_table(model, agate_table) %}
    {% else %}
        {{ adapter.truncate_relation(old_relation) }}
        {% set sql = "truncate table " ~ old_relation.render() %}
    {% endif %}

    {{ return(sql) }}
{% endmacro %}