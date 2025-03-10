{%- materialization view, adapter='saphanacloud' -%}
  {% set relation = adapter.dispatch('list_relations_without_caching')(adapter.Relation.create(schema=this.schema)) %}

   {% set grant_config = config.get('grants') %}
   {%- set identifier = model['alias'] -%}
   {%- set backup_identifier = model['alias'] + '__dbt_backup' -%}

   {%- set old_relation = adapter.get_relation(database=database, schema=schema, identifier=identifier) -%}
  
   {% set target_relation = api.Relation.create(
       database=this.database,
       identifier=this.identifier,
       schema=this.schema,
       type = 'view'
   ) %}

  {%- set backup_relation_type = 'view' if old_relation is none else old_relation.type -%}
  {%- set backup_relation = api.Relation.create(identifier=backup_identifier,
                                                schema=this.schema, database=this. database,
                                                type=backup_relation_type) -%}

    -- as above, the backup_relation should not already exist
  {%- set preexisting_backup_relation = adapter.get_relation(identifier=backup_identifier,
                                                             schema=this.schema,
                                                             database=this.database) -%}

  {{ run_hooks(pre_hooks, inside_transaction=False) }}

  {{ drop_relation_if_exists(preexisting_backup_relation) }}

  -- `BEGIN` happens here:
  {{ run_hooks(pre_hooks, inside_transaction=True) }}

  -- if old_relation was a table
  {% if old_relation is not none and old_relation.type == 'table' %}
    {{ adapter.rename_relation(old_relation, backup_relation) }}
  {% endif %}

   {% set sql = model.compiled_sql %}


   {% call statement('main', fetch_result=True) %}
     {{ saphanacloud__create_or_replace_view(target_relation, sql) }}
   {% endcall %}

   {% do persist_docs(target_relation, model) %}

  {{ run_hooks(post_hooks, inside_transaction=True) }}

  {{ adapter.commit() }}

  {{ drop_relation_if_exists(backup_relation) }}

  {{ run_hooks(post_hooks, inside_transaction=False) }}

  {% set should_revoke = should_revoke(old_relation, full_refresh_mode=True) %}
  {% do apply_grants(target_relation, grant_config, should_revoke=should_revoke) %}

   {% if config.get('grant_access_to') %}
     {% for grant_target_dict in config.get('grant_access_to') %}
       {% do adapter.grant_access_to(this, 'view', None, grant_target_dict) %}
     {% endfor %}
   {% endif %}

   {% do return({'relations': [target_relation]}) %}

{%- endmaterialization %}
