{% macro get_existing_primary_key_columns(target_relation, unique_key) %}
  {% set schema_name = target_relation.schema %}
  {% set table_name = target_relation.identifier %}

  {% set sql %}
    SELECT COLUMN_NAME
    FROM CONSTRAINTS
    WHERE SCHEMA_NAME = '{{ schema_name }}'
      AND TABLE_NAME = '{{ table_name }}'
      AND IS_PRIMARY_KEY = 'TRUE';
  {% endset %}
  
  {% set results = run_query(sql) %}
  
  {% if results and results.rows | length > 0 %}
    {% set normalized_unique_key = unique_key if unique_key is sequence and unique_key is not string else [unique_key] %}
    {% set primary_key_columns = results.columns[0].values() | map('lower') | sort %}
    {% set sorted_unique_key = normalized_unique_key | map('lower') | sort %}

    {% if primary_key_columns == sorted_unique_key %}
      {{ return(True) }}
    {% else %}
      {{ exceptions.raise_compiler_error(
          "A primary key already exists: " ~ primary_key_columns ~ 
          ". It does not match the provided unique key: " ~ sorted_unique_key) }}
    {% endif %}
  {% else %}
    {{ return(False) }}
  {% endif %}
{% endmacro %}


{% macro generate_alter_primary_key_sql(target_relation, unique_key, existing_pk_columns) %}
  
  {% if not existing_pk_columns %}
    {% if unique_key is sequence and unique_key is not mapping and unique_key is not string %}
              {% set alter_sql %}
                  ALTER TABLE {{ target_relation }}
                  ADD PRIMARY KEY ({{ unique_key | join(', ') }});
              {% endset %}
          {% else %}
              {% set alter_sql %}
                  ALTER TABLE {{ target_relation }}
                  ADD PRIMARY KEY ({{ unique_key }});
              {% endset %}
          {% endif %}
  {% endif %}
  {{ return(alter_sql) }}
{% endmacro %}