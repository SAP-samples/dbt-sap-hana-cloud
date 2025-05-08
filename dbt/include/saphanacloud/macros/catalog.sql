{% macro saphanacloud__get_catalog_tables_sql(information_schema) %}
    select
        schema_name as owner,
        '{{ target.database }}'  as table_database,  -- Include database for tables
        schema_name as table_schema,
        table_name,
        case
            when table_type = 'COLUMN' then 'COLUMN TABLE'
            when table_type = 'ROW' then 'BASE TABLE'
            else 'UNKNOWN'
        end as table_type
    from sys.tables
    where schema_name not like 'SYS%'
    
    union all
    
    -- Include views explicitly
    select
        schema_name as owner,
        '{{ target.database }}'  as table_database,  -- Include database for views
        schema_name as table_schema,
        view_name as table_name,
        'VIEW' as table_type
    from sys.views
    where schema_name not like 'SYS%'
{%- endmacro %}

{% macro saphanacloud__get_catalog_columns_sql(information_schema) %}
    -- Get columns for tables
    select
        schema_name as owner,
        '{{ target.database }}'  as table_database,  -- Include database for columns in tables
        schema_name as table_schema,
        table_name,
        column_name,
        data_type_name as data_type,
        length as character_maximum_length,
        is_nullable as is_nullable,
        default_value as column_default,
        ROW_NUMBER() OVER (PARTITION BY schema_name, table_name ORDER BY column_name) as ordinal_position
    from sys.table_columns
    where schema_name not like 'SYS%'
    
    union all
    
    -- Get columns for views
    select
        schema_name as owner,
        '{{ target.database }}'  as table_database,  -- Include database for columns in views
        schema_name as table_schema,
        view_name as table_name,
        column_name,
        data_type_name as data_type,
        length as character_maximum_length,
        is_nullable as is_nullable,
        null as column_default,  -- Views don't have default values
        ROW_NUMBER() OVER (PARTITION BY schema_name, view_name ORDER BY column_name) as ordinal_position
    from sys.view_columns
    where schema_name not like 'SYS%'
{%- endmacro %}

{% macro saphanacloud__get_catalog_results_sql() %}
    select
        tables.owner as "table_owner",
        tables.table_database as "table_database",  -- Include database
        tables.table_schema as "table_schema",
        tables.table_name as "table_name",
        tables.table_type as "table_type",
        columns.column_name as "column_name",
        columns.data_type as "column_type",
        columns.is_nullable as "is_nullable",
        columns.column_default as "column_default",
        columns.ordinal_position as "column_index"
    from tables
    inner join columns
        on columns.table_database = tables.table_database  -- Join by database
        and columns.table_schema = tables.table_schema
        and columns.table_name = tables.table_name
        and columns.owner = tables.owner
{%- endmacro %}

{% macro saphanacloud__get_catalog_schemas_where_clause_sql(schemas) %}
    where (
        {%- for schema in schemas -%}
            upper(tables.table_schema) = upper('{{ schema }}'){%- if not loop.last %} or {% endif -%}
        {%- endfor -%}
    )
{%- endmacro %}

{% macro saphanacloud__get_catalog_relations_where_clause_sql(relations) %}
    where (
        {%- for relation in relations -%}
            {% if relation.schema and relation.identifier %}
                (
                    upper(tables.table_schema) = upper('{{ relation.schema }}')
                    and upper(tables.table_name) = upper('{{ relation.identifier }}')
                )
            {% elif relation.schema %}
                (
                    upper(tables.table_schema) = upper('{{ relation.schema }}')
                )
            {% else %}
                {% do exceptions.raise_compiler_error(
                    '`get_catalog_relations` requires a list of relations, each with a schema'
                ) %}
            {% endif %}
            {%- if not loop.last %} or {% endif -%}
        {%- endfor -%}
    )
{%- endmacro %}

{% macro saphanacloud__get_catalog(information_schema, schemas) %}

    {% set query %}
        with tables as (
            {{ saphanacloud__get_catalog_tables_sql(information_schema) }}
        ),
        columns as (
            {{ saphanacloud__get_catalog_columns_sql(information_schema) }}
        )
        {{ saphanacloud__get_catalog_results_sql() }}
        {{ saphanacloud__get_catalog_schemas_where_clause_sql(schemas) }}
        order by
        tables.table_database,  -- Order by database
        tables.table_schema,
        tables.table_name,
        columns.ordinal_position
    {% endset %}
    {{ return(run_query(query)) }}
{%- endmacro %}

{% macro saphanacloud__get_catalog_relations(information_schema, relations) %}

    {% set query %}
        with tables as (
            {{ saphanacloud__get_catalog_tables_sql(information_schema) }}
        ),
        columns as (
            {{ saphanacloud__get_catalog_columns_sql(information_schema) }}
        )
        {{ saphanacloud__get_catalog_results_sql() }}
        {{ saphanacloud__get_catalog_relations_where_clause_sql(relations) }}
        order by
        tables.table_database,  -- Order by database
        tables.table_schema,
        tables.table_name,
        columns.ordinal_position
    {% endset %}
    {{ return(run_query(query)) }}
{%- endmacro %}
