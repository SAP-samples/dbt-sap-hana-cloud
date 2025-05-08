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



{% materialization incremental, adapter='saphanacloud', supported_languages=['sql'] -%}

  {% set partitioned_query = false %}


  -- Validating query partitions

  {% if config.get('query_partitions') %}

      {% set partitioned_query = true %}
      
      {% set query_partitions = config.get('query_partitions') %}


      {% if query_partitions is not iterable or query_partitions is string or query_partitions is mapping %}
        {{ exceptions.raise_compiler_error("query partitions must be an array") }}
      {% endif %}

      {% if not query_partitions %}
        {{ exceptions.raise_compiler_error("no partitioning column is set") }}
      {% endif %}

      {% if query_partitions|length > 2 %}
        {{ exceptions.raise_compiler_error("the query can be partitioned by a maximum of two columns at once") }}
      {% endif %}

      {% for query_partition in query_partitions %}
          {% if query_partition.column is not defined %}
              {{ exceptions.raise_compiler_error("partition column not specified") }}
          {% elif query_partition.type is not defined %}
              {{ exceptions.raise_compiler_error("partition type not specified") }}
          {% elif query_partition.type != 'list' and  query_partition.type != 'range' %}
              {{ exceptions.raise_compiler_error("partition type must be either list or range") }}
          {% elif query_partition.partitions is not defined %}
              {{ exceptions.raise_compiler_error("partition columns not specified") }}
          {% elif query_partition.default_partition_required is not defined %}
              {{ exceptions.raise_compiler_error("default partition required flag not set") }}
          {% elif query_partition.default_partition_required != True and  query_partition.default_partition_required != False %}
              {{ exceptions.raise_compiler_error("default_partition_required must be either True or False") }}     
          {% endif %}
      {% endfor %}
      

  {% endif %}


  {% set table_type = config.get('table_type') or 'column' %}


  -- relations
  {%- set existing_relation = load_cached_relation(this) -%}
  {%- set target_relation = this.incorporate(type='table') -%}
  {%- set temp_relation = make_temp_relation(target_relation)-%}
  {%- set intermediate_relation = make_intermediate_relation(target_relation)-%}
  {%- set backup_relation_type = 'table' if existing_relation is none else existing_relation.type -%}
  {%- set backup_relation = make_backup_relation(target_relation, backup_relation_type) -%}


  -- configs
  {%- set unique_key = config.get('unique_key') -%}
  {%- set unique_as_primary = config.get('unique_as_primary') -%}
  {%- set full_refresh_mode = (should_full_refresh() or existing_relation.is_view) -%}
  {%- set on_schema_change = incremental_validate_on_schema_change(config.get('on_schema_change'), default='ignore') -%}


  -- the temp_ and backup_ relations should not already exist in the database; get_relation
  -- will return None in that case. Otherwise, we get a relation that we can drop
  -- later, before we try to use this name for the current operation. This has to happen before
  -- BEGIN, in a separate transaction
  {%- set preexisting_intermediate_relation = load_cached_relation(intermediate_relation)-%}
  {%- set preexisting_intermediate_relation = load_cached_relation(temp_relation)-%}
  {%- set preexisting_backup_relation = load_cached_relation(backup_relation) -%}
   -- grab current tables grants config for comparision later on
  {% set grant_config = config.get('grants') %}
  {{ drop_relation_if_exists(preexisting_intermediate_relation) }}
  {{ drop_relation_if_exists(temp_relation) }}
  {{ drop_relation_if_exists(preexisting_backup_relation) }}

  {{ run_hooks(pre_hooks, inside_transaction=False) }}

  -- `BEGIN` happens here:
  {{ run_hooks(pre_hooks, inside_transaction=True) }}

  {% set to_drop = [] %}
  

  -- Case 1 - no partitioning (default dbt)

  {% if partitioned_query==false %}

      {% if existing_relation is none %}
          {% set build_sql = get_create_table_as_sql(False, target_relation, sql) %}
      {% elif full_refresh_mode %}
          {% set build_sql = get_create_table_as_sql(False, intermediate_relation, sql) %}
          {% set need_swap = true %}
      {% else %}
        {% do run_query(get_create_table_as_sql(True, temp_relation, sql)) %}
        {% do adapter.expand_target_column_types(
                from_relation=temp_relation,
                to_relation=target_relation) %}
        {#-- Process schema changes. Returns dict of changes if successful. Use source columns for upserting/merging --#}
        {% set dest_columns = process_schema_changes(on_schema_change, temp_relation, existing_relation) %}
        {% if not dest_columns %}
          {% set dest_columns = adapter.get_columns_in_relation(existing_relation) %}
        {% endif %}

        {#-- Get the incremental_strategy, the macro to use for the strategy, and build the sql --#}
        {% set incremental_strategy = config.get('incremental_strategy') or 'default' %}
        {% set incremental_predicates = config.get('predicates', none) or config.get('incremental_predicates', none) %}
        {% set strategy_sql_macro_func = adapter.get_incremental_strategy_macro(context, incremental_strategy) %}
        {% set strategy_arg_dict = ({'target_relation': target_relation, 'temp_relation': temp_relation, 'unique_key': unique_key, 'dest_columns': dest_columns, 'incremental_predicates': incremental_predicates }) %}
        {% set build_sql = strategy_sql_macro_func(strategy_arg_dict) %}

      {% endif %}

      {% call statement("main") %}
          {{ build_sql }}
      {% endcall %}

      {% do to_drop.append(temp_relation) %}


  -- Case 2 - Partitioning
  {% elif partitioned_query == true %}

      {% set filter_conditions = get_filter_conditions(partition_type, partition_column, partition_values|sort() ) %}
    
      {% if existing_relation is none %}
          {% set re = create_empty_table_as(False, target_relation, query_partitions, sql) %}
          {% set re = insert_partitioned_data(sql, target_relation, filter_conditions) %}
            {% call noop_statement('main', re) -%}
          -- no-op (required otherwise an error will be displayed)
          {%- endcall %}
      {% elif full_refresh_mode %}
          {% set re = create_empty_table_as(False, intermediate_relation, query_partitions, sql) %}
          {% set re = insert_partitioned_data(sql, intermediate_relation, filter_conditions) %}
          {% call noop_statement('main', re) -%}
          -- no-op (required otherwise an error will be displayed)
          {%- endcall %}
          {% set need_swap = true %}
      {% else %}
        {% do create_empty_table_as(True, temp_relation, query_partitions, sql) %}
        {% do insert_partitioned_data(sql, temp_relation, filter_conditions) %}

        {% do adapter.expand_target_column_types(
                from_relation=temp_relation,
                to_relation=target_relation) %}
        {#-- Process schema changes. Returns dict of changes if successful. Use source columns for upserting/merging --#}
        {% set dest_columns = process_schema_changes(on_schema_change, temp_relation, existing_relation) %}
        {% if not dest_columns %}
          {% set dest_columns = adapter.get_columns_in_relation(existing_relation) %}
        {% endif %}

        {#-- Get the incremental_strategy, the macro to use for the strategy, and build the sql --#}
        {% set incremental_strategy = config.get('incremental_strategy') or 'default' %}
        {% set incremental_predicates = config.get('predicates', none) or config.get('incremental_predicates', none) %}
        {% set strategy_sql_macro_func = adapter.get_incremental_strategy_macro(context, incremental_strategy) %}
        {% set strategy_arg_dict = ({'target_relation': target_relation, 'temp_relation': temp_relation, 'unique_key': unique_key, 'dest_columns': dest_columns, 'incremental_predicates': incremental_predicates }) %}
        {% set build_sql = strategy_sql_macro_func(strategy_arg_dict) %}

        {% call statement("main") %}
          {{ build_sql }}
        {% endcall %}

        {% do to_drop.append(temp_relation) %}


      {% endif %}



  {% endif %}


  {#-- unique keys as primary key block--#}
  {% if unique_key and unique_as_primary%}
      {% if unique_as_primary is not boolean %}
          {% do exceptions.raise_compiler_error("`unique_as_primary` must be a boolean.") %}

      {% else %}
          {% if need_swap %} 
              {% set relation_alter_primary_keys = intermediate_relation %}
          {% else %}
              {% set relation_alter_primary_keys = target_relation %}
          {% endif %}
          {% set existing_pk_columns = get_existing_primary_key_columns(relation_alter_primary_keys, unique_key) %}
          {% if not existing_pk_columns %}
            {% set alter_sql = generate_alter_primary_key_sql(relation_alter_primary_keys, unique_key,existing_pk_columns)%}
            {% do run_query(alter_sql) %}
          {% endif %}  
      {% endif %}
  {% endif %} 



  {#-- swapping relations --#}
  {% if need_swap %}
      {% do adapter.rename_relation(existing_relation, backup_relation) %}
      {% do adapter.rename_relation(intermediate_relation, target_relation) %}
      {% do to_drop.append(backup_relation) %}
  {% endif %}

  {% set should_revoke = should_revoke(existing_relation, full_refresh_mode) %}
  {% do apply_grants(target_relation, grant_config, should_revoke=should_revoke) %}

  {% do persist_docs(target_relation, model) %}


  {% if existing_relation is none or existing_relation.is_view or should_full_refresh() %}
    {% do create_indexes(target_relation) %}
  {% endif %}

  {{ run_hooks(post_hooks, inside_transaction=True) }}

  -- `COMMIT` happens here
  {% do adapter.commit() %}

  {% for rel in to_drop %}
      {% do adapter.drop_relation(rel) %}
  {% endfor %}

  {{ run_hooks(post_hooks, inside_transaction=False) }}

  {{ return({'relations': [target_relation]}) }}

{%- endmaterialization %}
