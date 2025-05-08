
{% macro saphanacloud__alter_column_type(relation,column_name,new_column_type) -%}
  ALTER TABLE {{ column.relation }} AFTER COLUMN {{ column.name }} SET DATA TYPE {{ new_data_type}};
{% endmacro %}

{% macro saphanacloud__check_schema_exists(schema_name) -%}
  SELECT COUNT(*) > 0 FROM SYS.SCHEMAS WHERE SCHEMA_NAME = '{{ schema_name.upper() }}';
{% endmacro %}

{% macro saphanacloud__check_index_exists(relation, index_name) -%}
  {%- set schema_name = relation.schema -%}
  {%- set table_name = relation.identifier -%}

  SELECT INDEX_NAME 
  FROM INDEXES 
  WHERE SCHEMA_NAME = '{{ schema_name }}' 
    AND TABLE_NAME = upper('{{ table_name }}') 
    AND INDEX_NAME = '{{ index_name }}';

 {%- endmacro %}   

{% macro saphanacloud__get_create_index_sql(relation, index_dict) -%}
  {%- set index_config = adapter.parse_index(index_dict) -%}
  {%- set comma_separated_columns = ", ".join(index_config.columns) -%}
  {%- set index_name = index_config.render(relation) -%}
  {%- set schema_name = relation.schema -%}
  {%- set table_name = relation.identifier -%}
  {%- set index_type = index_config.type -%}

  {# Check if the index exists #}
  {% set existing_index_query = saphanacloud__check_index_exists(relation, index_name) %}
  
  {% set existing_index_result = run_query(existing_index_query) %}
  {% if index_type and index_type is not none %}
    {%- set i_type -%}
      {{ index_type }} 
    {%- endset -%}
  {%- endif -%}
 
  {% if existing_index_result.rows | length == 0 %}
    CREATE {% if index_config.unique %} UNIQUE {% endif %}{{ i_type }} INDEX "{{ index_name }}"
    ON {{ relation }} ({{ comma_separated_columns }});
  {% else %}
    {{ log("Index already exists: " ~ index_name, info=True) }}
  {% endif %}
{%- endmacro %}

{%- macro saphanacloud__get_drop_index_sql(relation, index_name) -%}
  {% set schema_name = relation.schema %}
  {% set table_name = relation.identifier %}
  {# Check if the index exists #}
  {% set existing_index_query = saphanacloud__check_index_exists(relation, index_name) %}
  {% set existing_index_result = run_query(existing_index_query) %}

  {% if existing_index_result.rows | length > 0 %}
    DROP INDEX "{{ schema_name }}"."{{ index_name }}" ;
  {%- endif -%}
{%- endmacro -%}

{% macro saphanacloud__create_schema(schema_name) %}
  {% set schema_check_query %}
    SELECT SCHEMA_NAME
    FROM SCHEMAS
    WHERE SCHEMA_NAME = '{{ schema_name }}'
  {% endset %}

  {% set schema_exists = run_query(schema_check_query).rows | length > 0 %}

  {% if not schema_exists %}
    {%- call statement('schema_name') -%}
      CREATE SCHEMA {{ schema_name }};
    {%- endcall -%}
  {% endif %}

{% endmacro %}

{% macro saphanacloud__drop_relation(relation) -%}
{%- if relation is not none -%}
  {%- if relation.is_view -%}
    {# If the existing relation is a view, drop the view #}
    {%- call statement('drop_view', fetch_result=True) -%}
      {{ saphanacloud__drop_view_if_exists(relation) }}
    {%- endcall -%}
  {%- else -%}
    {# If the existing relation is not a view (i.e., a table), drop the table #}
    {%- call statement('drop_table', fetch_result=True) -%}
      {{ saphanacloud__drop_table(relation) }}
    {%- endcall -%}
  {%- endif -%}
{%- endif -%}
{% endmacro %}

{% macro saphanacloud__drop_schema(relation) -%}
 DROP SCHEMA '{{ relation.schema }}' CASCADE;
{% endmacro %}


{% macro saphanacloud__get_columns_in_relation(relation) %}

    {% call statement('get_columns_in_relation', fetch_result=True) %}
            WITH columns AS (
              -- Fetch columns from tables
              SELECT
                  SCHEMA_NAME AS table_schema,
                  TABLE_NAME AS table_name,
                  COLUMN_NAME AS column_name,
                  DATA_TYPE_NAME AS data_type,
                  LENGTH AS character_maximum_length,
                  CASE WHEN DATA_TYPE_NAME LIKE '%NUMERIC%' THEN LENGTH ELSE NULL END AS numeric_precision,
                  SCALE AS numeric_scale,
                  POSITION AS ordinal_position
              FROM
                  SYS.TABLE_COLUMNS
              WHERE
                  SCHEMA_NAME = ('{{ relation.schema }}')
                  AND TABLE_NAME = ('{{ relation.identifier }}')

              UNION ALL

              -- Fetch columns from views
              SELECT
                  SCHEMA_NAME AS table_schema,
                  VIEW_NAME AS table_name,
                  COLUMN_NAME AS column_name,
                  DATA_TYPE_NAME AS data_type,
                  LENGTH AS character_maximum_length,
                  CASE WHEN DATA_TYPE_NAME LIKE '%NUMERIC%' THEN LENGTH ELSE NULL END AS numeric_precision,
                  SCALE AS numeric_scale,
                  POSITION AS ordinal_position
              FROM
                  SYS.VIEW_COLUMNS
              WHERE
                  SCHEMA_NAME = ('{{ relation.schema }}')
                  AND VIEW_NAME = ('{{ relation.identifier }}')
            )

            -- Final select to retrieve the column information
            SELECT
                column_name AS "name",
                data_type AS "type",
                character_maximum_length AS "character_maximum_length",
                numeric_precision AS "numeric_precision",
                numeric_scale AS "numeric_scale"
            FROM columns
            ORDER BY ordinal_position;
      {% endcall %}
    
    -- Execute the query and capture the results
    {% set table = load_result('get_columns_in_relation').table %}
    -- Return the columns from the query result

    {{ return(sql_convert_columns_in_relation(table)) }} 
{% endmacro %}


{% macro saphanacloud__get_timestamp_field(relation) %}
    {% set query %}
        SELECT COLUMN_NAME 
        FROM SYS.TABLE_COLUMNS 
        WHERE SCHEMA_NAME = '{{ relation.schema }}'
        AND TABLE_NAME = '{{ relation.identifier }}'
        AND DATA_TYPE_NAME IN ('TIMESTAMP', 'DATE', 'DATETIME')
    {% endset %}

    {% set result = run_query(query) %}
    
    {% if result and result.columns[0].values() | length > 0 %}
        {# Return the first column name that matches the timestamp condition #}
        {{ return(result.columns[0].values()[0]) }}
    {% else %}
        {# Return None if no timestamp field is found #}
        {{ return(None) }}
    {% endif %}
{% endmacro %}



{% macro saphanacloud__list_relations_without_caching(database_name, schema_name) %}
  {% set schema_name = schema_name | replace('"', '') %}
  {% set database_name = database_name | replace('"', '') %}
  
  {% set schema_name = adapter.quote(schema_name) %}
  {% set database_name = adapter.quote(database_name) %}

   {% set query %}
       SELECT
           CAST(TABLE_NAME AS VARCHAR) AS TABLE_NAME,
           'TABLE' AS TABLE_TYPE,
           '{{ database_name | trim('""') }}' AS DATABASE_NAME,
           '{{ schema_name | trim('""') }}' AS OWNER
       FROM
           SYS.TABLES
       WHERE
           SCHEMA_NAME = '{{ schema_name | replace('""', '') | trim('""') }}'
       UNION
       SELECT
           CAST(VIEW_NAME AS VARCHAR) AS TABLE_NAME,
           'VIEW' AS TABLE_TYPE,
           '{{ database_name | trim('""') }}' AS DATABASE_NAME,
           '{{ schema_name | trim('""') }}' AS OWNER
       FROM
           SYS.VIEWS
       WHERE
           SCHEMA_NAME = '{{ schema_name | replace('""', '') | trim('""') }}'
   {% endset %}

   {% set results = run_query(query) %}

    {% if execute and results %}
        {% set relations = [] %}
        {% for row in results.rows %}
            {% set relation_type = 
                'table' if row['TABLE_TYPE'] == 'TABLE' else 'view' 
            %}
            {% set relation = adapter.Relation.create(
                database=row['DATABASE_NAME'] | upper | trim('""'),
                schema=schema_name | upper | trim('""'),
                identifier=row['TABLE_NAME'],
                type=relation_type
            ) %}

            {% do relations.append(relation) %}
        {% endfor %}

    {% else %}
        {% set relations = [] %}
    {% endif %}

   {{ return(relations) }}
{% endmacro %}




{% macro saphanacloud__list_schemas(database) %}
  {% set results = run_query("SELECT SCHEMA_NAME FROM SYS.SCHEMAS;") %}
  {% set schemas = results.columns[0].values() %}
  
  {{ return(schemas) }}
{% endmacro %}

{% macro list_schemas()-%}
 {{return (adapter.dispatch('list_schemas', 'saphanacloud')())}}
{% endmacro %}

{% macro saphanacloud__rename_relation(from_relation, to_relation) -%}
  {% set query %}
    RENAME TABLE {{ from_relation }} TO {{ to_relation }};
  {% endset %}
  {% set results = run_query(query) %}
{% endmacro %}

{% macro saphanacloud__truncate_relation(relation) -%}
  {% set query %}
    TRUNCATE TABLE {{ relation }};
  {% endset %}
  {% set results = run_query(query) %}
{% endmacro %}

/*

Example 3 of 3 of required macros that does not have a default implementation.
 ** Good example of building out small methods ** please refer to impl.py for implementation of now() in postgres plugin
{% macro postgres__current_timestamp() -%}
  now()
{%- endmacro %}

*/

{% macro saphanacloud__current_timestamp() -%}
    CURRENT_TIMESTAMP
{% endmacro %}

{% macro get_snapshot_get_time_data_type() %}
    {% set snapshot_time = adapter.dispatch('snapshot_get_time', 'dbt')() %}
    {% set time_data_type_sql = 'select ' ~ snapshot_time ~ ' as dbt_snapshot_time from dummy' %}
    {% set snapshot_time_column_schema = get_column_schema_from_query(time_data_type_sql) %}
    {% set time_data_type = snapshot_time_column_schema[0].dtype %}
    {{ return(time_data_type or none) }}
{% endmacro %}

{% macro saphanacloud__get_tables_in_schema(schema_name) -%}
    SELECT TABLE_NAME FROM SYS.TABLES WHERE SCHEMA_NAME = '{{ schema_name.upper() }}'
{# docs show not to be implemented currently. #}
{% endmacro %}

{% macro saphanacloud__get_columns_in_table(schema_name, table_name) -%}
    SELECT COLMN_NAME, DATA_TYPE FROM SYS.TABLE_COLUMNS
    WHERE SCHEMA_NAME = '{{ schema_name.upper() }}'
    WHERE TABLE_NAME = '{{ table_name.upper() }}'
{% endmacro %}

{% macro set_custom_state() %}
    {{return(run_query("SET SCHEMA '{{ target.schema }}'"))}}
    -- {# Store the current session variables or states if needed #}
    -- {% set original_state = run_query('SELECT SESSION_CONTEXT(\'my_context_variable\')') %}

    -- {# Set any custom session variables or states required for the upcoming query #}
    -- {{ run_query('SET SESSION_CONTEXT(\'my_context_variable\', \'new_value\')') }}

    -- {# Return the original state so that it can be restored later #}
    -- {{ return(original_state) }}
{% endmacro %}

{% macro saphanacloud__make_temp_relation(base_relation, suffix) %}
    {% set tmp_identifier = base_relation.identifier ~ suffix %}
    {% set tmp_relation = base_relation.incorporate(path = {
        "identifier": tmp_identifier
    }) -%}

    {%- set tmp_relation = tmp_relation.include(database=false, schema=false) -%}
    {% do return(tmp_relation) %}
{% endmacro %}

{% macro saphanacloud__get_empty_schema_sql(columns) %}
    
    {%- set col_err = [] -%}
    {%- set col_naked_numeric = [] -%}

    SELECT
    {% for i in columns %}
        {%- set col = columns[i] -%}
        
        {# Error tracking for missing data types #}
        {%- if col['data_type'] is not defined -%}
            {%- do col_err.append(col['name']) -%}
        
        {# Check for numeric types missing precision/scale #}
        {%- elif col['data_type'].strip().lower() in ('numeric', 'decimal', 'number') and 
                not col.get('precision') and not col.get('scale') -%}
            {%- do col_naked_numeric.append(col['name']) -%}
        {%- endif -%}
        
        {# Set column name, quoted if specified #}
        {% set col_name =  col['name'] %}

        {# Data type casting for SAP HANA-specific types #}
        {%- if col['data_type'] | lower in ['clob', 'nclob'] -%}
            CAST(NULL AS NCLOB) AS {{ col_name }}{{ ", " if not loop.last }}
        
        {%- elif col['data_type'] | lower == 'blob' -%}
            CAST(NULL AS BLOB) AS {{ col_name }}{{ ", " if not loop.last }}
        
        {%- elif col['data_type'] | lower in ['varchar', 'nvarchar'] -%}
            CAST(NULL AS {{ col['data_type'] }}(255)) AS {{ col_name }}{{ ", " if not loop.last }}
        
        {%- elif col['data_type'] | lower in ['decimal', 'numeric'] -%}
            CAST(NULL AS {{ col['data_type'] }}({{ col.get('precision', 18) }}, {{ col.get('scale', 2) }})) AS {{ col_name }}{{ ", " if not loop.last }}
        
        {%- elif col['data_type'] | lower == 'char' -%}
            CAST(NULL AS CHAR(1)) AS {{ col_name }}{{ ", " if not loop.last }}
        
        {# Default handling for other data types #}
        {%- else -%}
            CAST(NULL AS {{ col['data_type'] }}) AS {{ col_name }}{{ ", " if not loop.last }}
        {%- endif -%}

    {%- endfor %}

    FROM DUMMY 

    {# Raise an error if data types are missing #}
    {%- if (col_err | length) > 0 -%}
        {{ exceptions.column_type_missing(column_names=col_err) }}
    
    {# Warning for numeric types missing precision/scale #}
    {%- elif (col_naked_numeric | length) > 0 -%}
        {{ exceptions.warn("Detected columns with numeric type and unspecified precision/scale, which can cause unintended rounding: " ~ col_naked_numeric) }}
    {%- endif %}
{% endmacro %}

{% macro saphanacloud__get_select_subquery(sql) %}
    select
    {% for column in model['columns'] %}
      {{ column }}{{ ", " if not loop.last }}
    {% endfor %}
    from (
        {{ sql }}
    ) model_subq
{%- endmacro %}

{% macro check_and_update_column_sizes(target_relation, temp_relation) %}
    {% set target_columns = adapter.get_columns_in_relation(target_relation) %}
    {% set temp_columns = adapter.get_columns_in_relation(temp_relation) %}

    {% set altered_columns = [] %}

    {% for temp_col in temp_columns %}
        {%- for target_col in target_columns if target_col.column == temp_col.column %}
            {% if temp_col.char_size is not none and target_col.char_size is not none and temp_col.char_size > target_col.char_size %}
                {% set column_info = {
                    "column": temp_col.column,
                    "dtype": temp_col.dtype,
                    "new_size": temp_col.char_size,
                    "old_size": target_col.char_size
                } %}
                {% do altered_columns.append(column_info) %}
            {% endif %}
        {%- endfor %}
    {% endfor %}

    {% if altered_columns | length > 0 %}
        {% for col in altered_columns %}
            {% set alter_sql %}
                ALTER TABLE {{ target_relation }} 
                ALTER ("{{ col['column'] }}" {{ col['dtype'] }} ({{ col['new_size'] }}));
            {% endset %}
            {% do run_query(alter_sql) %}
        {% endfor %}
    {% else %}
    {% endif %}
    
{% endmacro %}