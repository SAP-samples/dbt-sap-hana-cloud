{% materialization table, adapter='saphanacloud', supported_languages=['sql', 'python'] -%}

  {% set relation = adapter.dispatch('list_relations_without_caching')(adapter.Relation.create(schema=this.schema)) %}
  

   {% set grant_config = config.get('grants') %}
   {% set language = model['language'] %}
   {%- set existing_relation = load_cached_relation(this) -%}
  
   {% set target_relation = api.Relation.create(
       database=this.database,
       identifier=this.identifier,
       schema=this.schema,
       type = 'table'
   ) %}
   {%- set intermediate_relation =  make_intermediate_relation(target_relation) -%}
   {%- set preexisting_intermediate_relation = load_cached_relation(intermediate_relation) -%}
   {%- set backup_relation_type = 'table' if existing_relation is none else existing_relation.type -%}
   {%- set backup_relation = make_backup_relation(target_relation, backup_relation_type) -%}
  -- as above, the backup_relation should not already exist
  {%- set preexisting_backup_relation = load_cached_relation(backup_relation) -%}
  
  -- drop the temp relations if they exist already in the database
  {% if preexisting_backup_relation is not none %}
   {{ drop_relation_if_exists(preexisting_intermediate_relation) }}
  {% endif %}

  {% if preexisting_backup_relation is not none %}
   {{ drop_relation_if_exists(preexisting_backup_relation) }}
  {% endif %}

   {% set contract_config = config.get('contract') %}
   {% set temporary = config.get('temporary', false) %}


  {% set sql_header = config.get('sql_header', none) %}
  
  {{ run_hooks(pre_hooks, inside_transaction=False) }}

  -- `BEGIN` happens here:
  {{ run_hooks(pre_hooks, inside_transaction=True) }}
  {% set code = 'OK'%}
  {% set rows_affected = "" %}
   {%- if contract_config.enforced and not temporary -%}
   
    {% set build_sql = saphanacloud__create_table_as(False, intermediate_relation, model.compiled_sql, language) %} 
    {% set statements = build_sql.split(';') %} 

    {% for statement in statements %}
        {% set trimmed_statement = statement.strip() %}  {# Remove extra whitespace #}

        {% if trimmed_statement != '' %} 
            {% do run_query(trimmed_statement) %} 
        {% endif %}
    {% endfor %}

    {%- call noop_statement('main', code ~ '' ~ rows_affected, code, rows_affected) -%}
      {{ saphanacloud__create_table_as(False, target_relation, model.compiled_sql, language) }}
    {%- endcall -%}

   {% elif not is_incremental() %}
   
    {% set build_sql = saphanacloud__create_table_as(False, intermediate_relation, model.compiled_sql, language) %} 
    {% do run_query(build_sql) %} 

    {%- call noop_statement('main', code ~ ' ' ~ rows_affected, code, rows_affected) -%}
      {{ saphanacloud__create_table_as(False, target_relation, model.compiled_sql, language) }}
    {%- endcall -%}
  {% else %}
    {%- call statement('main', fetch_result=True) -%}
      INSERT INTO {{ intermediate_relation }} (
        {{ model.compiled_sql }}
      );
    {%- endcall -%}
  {% endif %}
 
    -- cleanup
  {% if existing_relation is not none %}
 
     /* Do the equivalent of rename_if_exists. 'existing_relation' could have been dropped
        since the variable was first set. */
    {% set existing_relation = load_cached_relation(existing_relation) %}
    {% if existing_relation is not none %}
    
      {% if existing_relation.is_view %}
        {% do adapter.drop_relation(existing_relation) %}
      {% else %}
        {{ adapter.rename_relation(existing_relation, backup_relation) }}
      {% endif %}
    {% endif %}
  {% endif %}
  

  {{ adapter.rename_relation(intermediate_relation, target_relation) }}

  {% do create_indexes(target_relation) %}

  {{ run_hooks(post_hooks, inside_transaction=True) }}

  {% set should_revoke = should_revoke(existing_relation, full_refresh_mode=True) %}
  {% do apply_grants(target_relation, grant_config, should_revoke=should_revoke) %}

  {% do persist_docs(target_relation, model) %}

  -- `COMMIT` happens here
  {{ adapter.commit() }}

  -- finally, drop the existing/backup relation after the commit
  {{ drop_relation_if_exists(backup_relation) }}

  {{ run_hooks(post_hooks, inside_transaction=False) }}

   {% do return({'relations': [target_relation]}) %}

{%- endmaterialization %}
