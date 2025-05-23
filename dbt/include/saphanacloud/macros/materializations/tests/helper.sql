{% macro get_test_sql(main_sql, fail_calc, warn_if, error_if, limit) -%}
  {{ adapter.dispatch('get_test_sql', 'dbt')(main_sql, fail_calc, warn_if, error_if, limit) }}
{%- endmacro %}


{% macro saphanacloud__get_test_sql(main_sql, fail_calc, warn_if, error_if, limit) -%}
    select
      {{ fail_calc }} as failures,
      case when {{ fail_calc }} {{ warn_if }} then 1 else 0 end as should_warn,
      case when {{ fail_calc }} {{ error_if }} then 1 else 0 end as should_error
    from (
      {{ main_sql }}
      {%- if limit is not none -%}
        limit {{ limit }}
      {%- endif -%}
    ) dbt_internal_test
{%- endmacro %}