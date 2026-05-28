{%- macro drop_remote_subscription_if_exists(relation) -%}
  
  {%- set subscription_exists = check_remote_subscription_exists(relation) -%}
  
  {% if subscription_exists %}
    {% call statement('drop_subscription') %}
      DROP REMOTE SUBSCRIPTION {{ relation }}
    {% endcall %}
    {{ log("Dropped existing remote subscription: " ~ relation, info=true) }}
  {% endif %}
{%- endmacro -%}

{%- macro create_remote_subscription(relation, virtual_relation, subscription_target_relation) -%}
  
  {% call statement('create_subscription') %}
    CREATE REMOTE SUBSCRIPTION {{ relation }}
    ON {{ virtual_relation }}
    TARGET TABLE {{ subscription_target_relation }}
  {% endcall %}
  
  {{ log("Created remote subscription: " ~ relation, info=true) }}
{%- endmacro -%}

{%- macro prepare_initial_load(relation, chunk_size) -%}
  
  {% call statement('prepare_chunk_size') %}
    ALTER REMOTE SUBSCRIPTION {{ relation }} INITIAL LOAD PREPARE CHUNK SIZE {{ chunk_size }}
  {% endcall %}
  
  {{ log("Preparing Initial Load for remote subscription: " ~ relation, info=true) }}
{%- endmacro -%}

{%- macro execute_initial_load(relation) -%}
  
  {% call statement('execute_initial_load') %}
    ALTER REMOTE SUBSCRIPTION {{ relation }} INITIAL LOAD EXECUTE
  {% endcall %}
  
  {{ log("Executing Initial Load for remote subscription: " ~ relation, info=true) }}
{%- endmacro -%}

{%- macro finalize_initial_load(relation) -%}
  
  {% call statement('finalize_initial_load') %}
    ALTER REMOTE SUBSCRIPTION {{ relation }} INITIAL LOAD FINALIZE
  {% endcall %}
  
  {{ log("Finalizing Initial Load for remote subscription: " ~ relation, info=true) }}
{%- endmacro -%}

{%- macro distribute_remote_subscription(relation) -%}
  
  {% call statement('distribute_subscription') %}
    ALTER REMOTE SUBSCRIPTION {{ relation }} DISTRIBUTE
  {% endcall %}
  
  {{ log("Distributed remote subscription: " ~ relation, info=true) }}
{%- endmacro -%}

{%- macro perform_full_load(relation, chunk_size) -%}
  
  {{ prepare_initial_load(relation, chunk_size) }}
  {{ wait_for_prepare_completion(relation) }}
  
  {{ execute_initial_load(relation) }}
  {{ wait_for_execute_completion(relation) }}
  
  {{ finalize_initial_load(relation) }}
  {{ wait_for_finalize_completion(relation) }}
  
{%- endmacro -%}

{%- macro wait_for_prepare_completion(relation) -%}
  {{ log("Waiting for preparation to complete for: " ~ relation, info=true) }}
  
  {% for i in range(60) %}
    {%- set is_complete = check_preparation_status(relation, i) -%}
    {% if is_complete %}
      {% break %}
    {% endif %}
  {% endfor %}
{%- endmacro -%}

{%- macro wait_for_execute_completion(relation) -%}
  {{ log("Waiting for execution to complete for: " ~ relation, info=true) }}
  
  {% for i in range(120) %}
    {%- set is_complete = check_execution_status(relation, i) -%}
    {% if is_complete %}
      {% break %}
    {% endif %}
  {% endfor %}
{%- endmacro -%}

{%- macro wait_for_finalize_completion(relation) -%}
  {{ log("Waiting for finalization to complete for: " ~ relation, info=true) }}
  
  {% for i in range(60) %}
    {%- set is_complete = check_finalization_status(relation, i) -%}
    {% if is_complete %}
      {% break %}
    {% endif %}
  {% endfor %}
{%- endmacro -%}

{%- macro check_preparation_status(relation, attempt) -%}
  {% call statement('check_prepare_status_' ~ attempt, fetch_result=true) %}
    SELECT COUNT(*) as prepared_chunks
    FROM M_REMOTE_SUBSCRIPTION_INITIAL_LOADS 
    WHERE REMOTE_SUBSCRIPTION_SCHEMA_NAME = '{{ relation.schema }}'
    AND REMOTE_SUBSCRIPTION_NAME = '{{ relation.identifier }}'
    AND EXECUTION_STATUS = 'PREPARED'
  {% endcall %}
  
  {%- set prepare_result = load_result('check_prepare_status_' ~ attempt) -%}
  
  {% if prepare_result['data'][0][0] > 0 %}
    {{ log("Preparation completed. " ~ prepare_result['data'][0][0] ~ " chunks prepared.", info=true) }}
    {{ return(true) }}
  {% endif %}
  
  {% if attempt >= 59 %}
    {{ log("Warning: Preparation may still be in progress after timeout", info=true) }}
  {% endif %}
  
  {{ return(false) }}
{%- endmacro -%}

{%- macro check_execution_status(relation, attempt) -%}
  {% call statement('check_execute_status_' ~ attempt, fetch_result=true) %}
    SELECT 
      COUNT(*) as total_chunks,
      SUM(CASE WHEN EXECUTION_STATUS = 'LOADED' THEN 1 ELSE 0 END) as loaded_chunks
    FROM M_REMOTE_SUBSCRIPTION_INITIAL_LOADS 
    WHERE REMOTE_SUBSCRIPTION_SCHEMA_NAME = '{{ relation.schema }}'
    AND REMOTE_SUBSCRIPTION_NAME = '{{ relation.identifier }}'
  {% endcall %}
  
  {%- set execute_result = load_result('check_execute_status_' ~ attempt) -%}
  {%- set total_chunks = execute_result['data'][0][0] -%}
  {%- set loaded_chunks = execute_result['data'][0][1] -%}
  
  {% if total_chunks > 0 and loaded_chunks == total_chunks %}
    {{ log("Execution completed. All " ~ total_chunks ~ " chunks loaded.", info=true) }}
    {{ return(true) }}
  {% else %}
    {{ log("Loading in progress: " ~ loaded_chunks ~ "/" ~ total_chunks ~ " chunks loaded.", info=true) }}
  {% endif %}
  
  {% if attempt >= 119 %}
    {{ log("Warning: Execution may still be in progress after timeout", info=true) }}
  {% endif %}
  
  {{ return(false) }}
{%- endmacro -%}

{%- macro check_finalization_status(relation, attempt) -%}
  {% call statement('check_finalize_status_' ~ attempt, fetch_result=true) %}
    SELECT COUNT(*) as remaining_chunks
    FROM M_REMOTE_SUBSCRIPTION_INITIAL_LOADS 
    WHERE REMOTE_SUBSCRIPTION_SCHEMA_NAME = '{{ relation.schema }}'
    AND REMOTE_SUBSCRIPTION_NAME = '{{ relation.identifier }}'
  {% endcall %}
  
  {%- set finalize_result = load_result('check_finalize_status_' ~ attempt) -%}
  
  {% if finalize_result['data'][0][0] == 0 %}
    {{ log("Finalization completed. All chunks have been processed and removed.", info=true) }}
    {{ return(true) }}
  {% else %}
    {{ log("Finalization in progress. " ~ finalize_result['data'][0][0] ~ " chunks still remaining.", info=true) }}
  {% endif %}
  
  {% if attempt >= 59 %}
    {{ log("Warning: Finalization may still be in progress after timeout", info=true) }}
  {% endif %}
  
  {{ return(false) }}
{%- endmacro -%}

{%- macro check_remote_subscription_exists(target_relation) -%}
  {% call statement('check_subscription_exists', fetch_result=true) %}
    SELECT COUNT(*) as subscription_count 
    FROM SYS.REMOTE_SUBSCRIPTIONS 
    WHERE SUBSCRIPTION_NAME = '{{ target_relation.identifier }}'
    AND SCHEMA_NAME = '{{ target_relation.schema }}'
  {% endcall %}
  
  {%- set result = load_result('check_subscription_exists') -%}
  {{ return(result['data'][0][0] > 0) }}
{%- endmacro -%}