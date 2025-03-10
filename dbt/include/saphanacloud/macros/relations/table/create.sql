
{% macro saphanacloud__create_table_as(temporary, relation, compiled_code, language='sql') %}

  {%- set identifier = relation.identifier -%}
  {%- set schema = relation.schema -%}
  {%- set database = relation.database -%}
  {% set material = config.get('materialized', none) %}
  {% set table_type = config.get('table_type', none) %}
   {%- if table_type is not none -%}
      {% if material == 'table' or material == 'incremental' %}
        {% if table_type == 'row' %}
          {% set type = "ROW" %}
        {% elif table_type == 'column' %}
          {% set type = "COLUMN" %}
        {% elif table_type == 'none' %}
          {% set type = "" %}
        {% else %}
          {% do exceptions.raise_compiler_error(
          "Incorrect table type"
          ~ table_type ~ " was specified."
          )%}
        {% endif %}
      {% else %}
        {% do exceptions.raise_compiler_error(
          "Table type only works for incremental and table."
          )%}
      {% endif %}
   {% else %}
      {% set type = '' %}
   {% endif %}

  {% set temp_table = '' %}
  {% set sql_header = config.get('sql_header', none) %}
  {% set contract_config = config.get('contract') %}
  {% set temporary = config.get('temporary', false) %}
  {%- set partition_clause = config.get('partition_config', {}).get('clause') -%}

  {% if temporary %}
    {% set temp_table = "GLOBAL TEMPORARY" %}
  {% endif %}


  {{ sql_header if sql_header is not none }}

  {%- if contract_config.enforced and not temporary -%}
    {% set create_sql %}

          {{ get_assert_columns_equivalent(sql) }}
        
            CREATE {{ temp_table }}{{ type }} TABLE {{ relation }} {{ get_table_columns_and_constraints() }};
            
            INSERT INTO 
            {{ relation }} 
            {%- set sql = get_select_subquery(sql) %}
            ({{ sql }});

    {% endset %}

    {% do return(create_sql) %}
  {% else %}
    {% set create_sql %}
      CREATE {{ temp_table }}{{ type }} TABLE {{ relation }} AS (
        {{ compiled_code }}
      );
    {% endset %}
    {% do return(create_sql) %}
  {% endif %}


{% endmacro %}