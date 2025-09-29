{% materialization realtime, adapter='saphanacloud' %}
  {%- set identifier = model['alias'] -%}
  {%- set tmp_identifier = model['alias'] + '__dbt_tmp' -%}
  {%- set backup_identifier = model['alias'] + '__dbt_backup' -%}

  {%- set config = model['config'] -%}
  {%- set subscription_target_table = config.get('target_table') -%}
  {%- set virtual_table = config.get('virtual_table') -%}
  {%- set chunk_size = config.get('chunk_size') -%}
  
  {%- if not subscription_target_table -%}
    {{ exceptions.raise_compiler_error("target_table must be specified in config for realtime materialization") }}
  {%- endif -%}
  {%- if not virtual_table -%}
    {{ exceptions.raise_compiler_error("virtual_table must be specified in config for realtime materialization") }}
  {%- endif -%}

  {%- set target_relation = api.Relation.create(identifier=identifier, schema=schema, database=database, type='table') -%}
  {%- set tmp_relation = api.Relation.create(identifier=tmp_identifier, schema=schema, database=database, type='table') -%}
  {%- set backup_relation = api.Relation.create(identifier=backup_identifier, schema=schema, database=database, type='table') -%}
  {%- set virtual_relation = api.Relation.create(identifier=virtual_table, schema=schema, database=database, type='table') -%}
  {%- set subscription_target_relation = api.Relation.create(identifier=subscription_target_table, schema=schema, database=database, type='table') -%}

  {%- set subscription_exists = check_remote_subscription_exists(target_relation) -%}

  {%- set full_refresh_mode = (should_full_refresh()) -%}

  {{ run_hooks(pre_hooks, inside_transaction=false) }}

  -- `BEGIN` happens here for transactions
  {{ run_hooks(pre_hooks, inside_transaction=true) }}
  {% if full_refresh_mode or not subscription_exists %}
    
      {{ log("Creating real-time materialization with remote subscription for " ~ target_relation, info=true) }}
      
      -- Step 1: Drop existing remote subscription if it exists
      {{ drop_remote_subscription_if_exists(target_relation) }}
      
      -- Step 2: Create remote subscription
      {{ create_remote_subscription(target_relation, virtual_relation, subscription_target_relation) }}
            
      -- Step 3: Perform full load
      {{ perform_full_load(target_relation, chunk_size) }}
      
      -- Step 4: Distribute the remote subscription
      {{ distribute_remote_subscription(target_relation) }}

    {% set re = "Materialization Complete " %}
    {% call noop_statement('main',re) -%}
      -- no-op (required otherwise an error will be displayed)
    {%- endcall %}
  {% else %}
  {% set re = "Error: " %}
  {% call noop_statement('main', re) -%}
           {{ log("Real-time materialization " ~ target_relation ~ " already exists and is running in background. No action needed.", info=true) }}
    {%- endcall %}
  {% endif %}

  -- `COMMIT` happens here for transactions
  {{ run_hooks(post_hooks, inside_transaction=false) }}

  {{ return({'relations': [target_relation]}) }}

{% endmaterialization %}