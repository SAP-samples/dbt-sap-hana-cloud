{% macro saphanacloud__drop_view_if_exists(view_relation) %}
  {% set schema_name = view_relation.schema %}
  {% set view_name = view_relation.identifier %}
  
  {% set query %}
  DO BEGIN
    DECLARE view_exists INT;

    -- Check if the view exists, ignoring case
    SELECT COUNT(*) INTO view_exists
    FROM SYS.VIEWS
    WHERE UPPER(SCHEMA_NAME) = UPPER('{{ schema_name }}')
    AND UPPER(VIEW_NAME) = UPPER('{{ view_name }}');

    -- If the view exists, drop it
    IF view_exists = 1 THEN
        EXEC 'DROP VIEW {{ view_relation }}';
    END IF;
  END;
  {% endset %}
  {{ run_query(query) }}
  {{ return(query) }}
{% endmacro %}
