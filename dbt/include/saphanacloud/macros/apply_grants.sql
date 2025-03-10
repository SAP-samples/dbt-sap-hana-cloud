{% macro saphanacloud__get_show_grant_sql(relation) %}
    SELECT *
    FROM GRANTED_PRIVILEGES
    WHERE OBJECT_NAME = ('{{ relation.identifier }}')
    {% if relation.schema %}
        AND SCHEMA_NAME = ('{{ relation.schema }}')
    {% endif %}
{% endmacro %}

{%- macro saphanacloud__support_multiple_grantees_per_dcl_statement() -%}
    {{ return(False) }}
{%- endmacro -%}

{% macro saphanacloud__call_dcl_statements(dcl_statement_list) %}
    {# Run each grant/revoke statement against the database. This is the culmination of apply_grants() #}
     {% for dcl_statement in dcl_statement_list %}
        {% do run_query(dcl_statement) %}
     {% endfor %}
{% endmacro %}