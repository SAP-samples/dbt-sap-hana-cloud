{% macro saphanacloud_get_tmp_relation_type(strategy, unique_key, language) %}
{%- set tmp_relation_type = config.get('tmp_relation_type') -%}
  /* {#
       High-level principles:
       - Use a table if multiple statements are needed (DELETE + INSERT).
       - Use a view if a single statement (MERGE or INSERT) is run for faster incremental processing.
       - For Python models, always use a table for the temp relation.
  #} */

  {% if language == "python" and tmp_relation_type is not none %}
    {% do exceptions.raise_compiler_error(
      "Python models currently only support 'table' for tmp_relation_type but "
       ~ tmp_relation_type ~ " was specified."
    ) %}
  {% endif %}

  {% if strategy == "delete+insert" and tmp_relation_type is not none and tmp_relation_type != "table" and unique_key is not none %}
    {% do exceptions.raise_compiler_error(
      "In order to maintain consistent results when `unique_key` is not none, the `delete+insert` strategy only supports `table` for `tmp_relation_type` but "
      ~ tmp_relation_type ~ " was specified."
      )
  %}
  {% endif %}

  {% if language != "sql" %}
    {{ return("table") }}
  {% elif tmp_relation_type == "table" %}
    {{ return("table") }}
  {% elif tmp_relation_type == "view" %}
    {{ return("view") }}
  {% elif strategy in ("default", "merge", "append") %}
    {{ return("view") }}
  {% elif strategy == "delete+insert" and unique_key is none %}
    {{ return("view") }}
  {% else %}
    {{ return("table") }}
  {% endif %}
{% endmacro %}

{% materialization incremental, adapter='saphanacloud', supported_languages=['sql', 'python'] -%}

  {#-- Set vars --#}
  {%- set full_refresh_mode = (should_full_refresh()) -%}
  {%- set language = model['language'] -%}
  {%- set identifier = this.name -%}
  {%- set target_relation = api.Relation.create(
    identifier=identifier,
    schema=schema,
    database=database,
    type='table',
    table_format=config.get('table_format', 'default')
  ) -%}
  {% set existing_relation = load_relation(this) %}
  {%- set unique_key = config.get('unique_key') -%}
  {% set incremental_strategy = config.get('incremental_strategy') or 'default' %}+
  {#-- The temp relation will be a view or table, depending on upsert/merge strategy --#}
  {% set tmp_relation_type = saphanacloud_get_tmp_relation_type(incremental_strategy, unique_key, language) %}
  {% set tmp_relation_identifier = this.identifier ~ '_tmp' %}
  {%- set unique_as_primary = config.get('unique_as_primary') -%}

  {% set tmp_relation = api.Relation.create(
    identifier=tmp_relation_identifier,
    schema=schema,
    database=database,
    type=tmp_relation_type
  ) -%}

  {% set grant_config = config.get('grants') %}
  {% set contract_config = config.get('contract') %}
  {% set temporary = config.get('temporary', false) %}  
  {% set code = 'OK'%}
  {% set rows_affected = "" %}

  {% set on_schema_change = incremental_validate_on_schema_change(config.get('on_schema_change'), default='ignore') %}

  {{ run_hooks(pre_hooks) }}

  {% if unique_key and unique_as_primary%}
      {% if unique_as_primary is not boolean %}
          {% do exceptions.raise_compiler_error("`unique_as_primary` must be a boolean.") %}

      {% else %}
          {% set existing_pk_columns = get_existing_primary_key_columns(target_relation, unique_key) %}
          {% set alter_sql = generate_alter_primary_key_sql(target_relation, unique_key,existing_pk_columns)%}
      {% endif %}
{% endif %}

  {% if existing_relation is none %}

  {%- if contract_config.enforced and not temporary -%}
    {% set build_sql = saphanacloud__create_table_as(False, target_relation, model.compiled_sql, language) %} 

  {%- else -%}
    {% set build_sql = create_table_as(False, target_relation, model.compiled_sql, language) %} 

  {%- endif -%}

  {% elif existing_relation.is_view %}

    {#-- Can't overwrite a view with a table - we must drop --#}

    {% do adapter.drop_relation(existing_relation) %}
        {%- if contract_config.enforced and not temporary -%}
                {% set build_sql = saphanacloud__create_table_as(False, target_relation, model.compiled_sql, language) %} 

        {%- else -%}
            {% set build_sql = create_table_as(False, target_relation, model.compiled_sql, language) %} 

        {%- endif -%}

  {% elif full_refresh_mode %}
      {{ drop_relation_if_exists(existing_relation) }}

        {%- if contract_config.enforced and not temporary -%}
                {% set build_sql = saphanacloud__create_table_as(False, target_relation, model.compiled_sql, language) %} 

        {%- else -%}
            {% set build_sql = create_table_as(False, target_relation, model.compiled_sql, language) %}
        {%- endif -%}

  {% else %}

    {#-- Create the temp relation, either as a view or as a table --#}
    {% if tmp_relation_type == 'view' %}
        {%- call statement('create_tmp_relation') -%}
          {{ saphanacloud__create_view_as_with_temp_flag(tmp_relation, compiled_code, True) }}
        {%- endcall -%}
    {% else %}

        {%- call statement('create_tmp_relation', language=language) -%}
          {{ create_table_as(True, tmp_relation, compiled_code,  language) }}
        {%- endcall -%}
    {% endif %}



    {% do adapter.expand_target_column_types(
           from_relation=tmp_relation,
           to_relation=target_relation) %}
    {#-- Process schema changes. Returns dict of changes if successful. Use source columns for upserting/merging --#}

    {% set dest_columns = process_schema_changes(on_schema_change, tmp_relation, existing_relation) %}
    {% do check_and_update_column_sizes(existing_relation, tmp_relation) %}
    {% if not dest_columns %}
      {% set dest_columns = adapter.get_columns_in_relation(existing_relation) %}
    {% endif %}

    {#-- Get the incremental_strategy, the macro to use for the strategy, and build the sql --#}
    {% set incremental_predicates = config.get('predicates', none) or config.get('incremental_predicates', none) %}
    {% set strategy_sql_macro_func = adapter.get_incremental_strategy_macro(context, incremental_strategy) %}
    {% set strategy_arg_dict = ({'target_relation': target_relation, 'temp_relation': tmp_relation, 'unique_key': unique_key, 'dest_columns': dest_columns, 'incremental_predicates': incremental_predicates }) %}
    {% set build_sql = strategy_sql_macro_func(strategy_arg_dict) %}


  {% endif %}

    {%- call statement('main', fetch_result=True) -%}
      DO
      BEGIN
      {{build_sql}}
      END;
    {%- endcall -%}

    {#-- unique as primary key alter sql block--#}
    {% if not existing_pk_columns and unique_as_primary %}
      {% do run_query(alter_sql) %}
    {% endif %}

  {% do drop_relation(tmp_relation) %}

   {% set should_revoke = should_revoke(existing_relation, full_refresh_mode) %}
  {% do apply_grants(target_relation, grant_config, should_revoke=should_revoke) %}

  {% do persist_docs(target_relation, model) %}

  {% if existing_relation is none or existing_relation.is_view or should_full_refresh() %}
    {% do create_indexes(target_relation) %}
  {% endif %}

  {{ run_hooks(post_hooks) }}

  -- `COMMIT` happens here
  {% do adapter.commit() %}

  {% set target_relation = target_relation.incorporate(type='table') %}

  {{ return({'relations': [target_relation]}) }}

{% endmaterialization %}
