{% macro get_filter_conditions(partition_type, partition_column, partition_values) -%}
    {{ return(adapter.dispatch('get_filter_conditions','partitioning')(partition_type, partition_column, partition_values)) }}
{% endmacro %}


{% macro default__get_filter_conditions(partition_type, partition_column, partition_values) -%}
    {{ exceptions.raise_compiler_error("`get_filter_conditions has not been implemented for this adapter.") }}
{%- endmacro %}


{% macro saphanacloud__get_filter_conditions(partition_type, partition_column, partition_values) -%}

  {% set query_partitions = config.get('query_partitions') %}

  {%- set all_filter_conditions = [] -%}

  {% for partition in query_partitions %}

      {% set partition_type = partition.type %}
      {% set partition_name = partition.name %}
      {% set partition_column = partition.column %}
      {% set partition_values = partition.partitions %}
      {% set default_partition_required = partition.default_partition_required %}

      {% set filter_conditions = [] %}

      {% if partition_type == 'range' %}

        {% for p in partition_values %}

          {% if loop.index == 1 %}   
            {% set filter_condition = partition_column ~  " < '" ~ p ~ "'" %}
            {% set filter_condition = {'partition_name': partition_name, 'expression': filter_condition} %}
            {{ filter_conditions.append(filter_condition) }}
          {% else %}
            {% set filter_condition = partition_column ~  " >= '" ~ partition_values[(loop.index-2)] ~ "' AND " ~ partition_column ~  " < '" ~ p ~ "'" %}
            {% set filter_condition = {'partition_name': partition_name, 'expression': filter_condition} %}
            {{ filter_conditions.append(filter_condition) }}
          {% endif %}
          
          {% if loop.last %}   
            {% set filter_condition = partition_column ~  " >= '" ~ p ~ "' OR " ~ partition_column ~  " IS NULL" %}
            {% set filter_condition = {'partition_name': partition_name, 'expression': filter_condition} %}
            {{ filter_conditions.append(filter_condition) }}
          {% endif %}

        {% endfor %}

      {% elif partition_type == 'list' %}

        {% for p in partition_values %}

          {% set filter_condition = partition_column ~  " IN ('" ~ p ~ "')"  %}
          {% set filter_condition = {'partition_name': partition_name, 'expression': filter_condition} %}
          {{ filter_conditions.append(filter_condition) }}

        {% endfor %}

        {# Add default partition #}
        {% if default_partition_required %}
            {% set joined_all_values = partition_values | join("','") %}
            {% set filter_condition = partition_column ~  " NOT IN ('" ~ joined_all_values ~ "') OR " ~ partition_column ~ " IS NULL" %}
            {% set filter_condition = {'partition_name': partition_name, 'expression': filter_condition} %}
            {{ filter_conditions.append(filter_condition) }}
        {% endif %}

      {% endif %}

      {{ all_filter_conditions.append(filter_conditions) }}

  {% endfor %}


  {% if all_filter_conditions[1] %}
      {% set merged_filter_conditions = [] %}
      {% for item1 in all_filter_conditions[0] %}
        {% for item2 in all_filter_conditions[1] %}
          {{ merged_filter_conditions.append([item1, item2]) }}
        {% endfor %}
      {% endfor %}
  {% else %}
      {% set merged_filter_conditions = [] %}
      {% for item1 in all_filter_conditions[0] %}
          {{ merged_filter_conditions.append([item1]) }}
      {% endfor %}
  {% endif %}
  

  {{ return(merged_filter_conditions) }}

{%- endmacro %}