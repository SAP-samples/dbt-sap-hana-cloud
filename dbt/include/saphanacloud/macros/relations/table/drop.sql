{% macro saphanacloud__drop_table(relation) %}
    {% set schema_name = relation.schema %}
    {% set table_name = relation.identifier %}
  
    {% set query %}
        DO BEGIN
            DECLARE v_table_exists INT;
            -- Convert schema_name and table_name to uppercase
            SELECT COUNT(*) INTO v_table_exists 
            FROM M_TABLES 
            WHERE UPPER(SCHEMA_NAME) = UPPER('{{ schema_name }}') 
            AND UPPER(TABLE_NAME) = UPPER('{{ table_name }}');
            
            IF v_table_exists > 0 THEN
                EXECUTE IMMEDIATE 'DROP TABLE {{ relation }}';
            END IF;
        END;
    {% endset %}
    {{ run_query(query) }}
    {{ return(query) }}

{% endmacro %}