{% macro saphanacloud__get_incremental_append_sql(arg_dict) %}

  {% do return(get_insert_into_sql(arg_dict["target_relation"], arg_dict["temp_relation"], arg_dict["dest_columns"])) %}

{% endmacro %}

{% macro get_insert_into_sql(target_relation, temp_relation, dest_columns) %}

    {%- set unique_keys = config.get("unique_key", []) -%}
    {% set incremental_predicates = config.get('predicates', none) or config.get('incremental_predicates', none) %}
    {% if unique_keys and unique_keys | length > 0 %}
        {% do return(saphanacloud__get_merge_sql(target_relation, temp_relation, unique_keys, dest_columns, incremental_predicates)) %}
    {% else %}
    {%- set dest_cols_csv = get_quoted_csv(dest_columns | map(attribute="name")) -%}

    insert into {{ target_relation }} ({{ dest_cols_csv }})
    (
        select {{ dest_cols_csv }}
        from {{ temp_relation }}
    );
    {% endif %}
{% endmacro %}