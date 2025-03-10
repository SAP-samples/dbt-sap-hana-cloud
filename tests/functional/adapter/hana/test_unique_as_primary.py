import pytest
from dbt.tests.util import run_dbt, run_dbt_and_capture
import re

models__incremental_with_trinary_unique_key_sql = """
{{
  config(
    materialized = "incremental",
    unique_key = ['id', 'name', 'county'],
    unique_as_primary = true
  )
}}

select
    id,
    name,
    county,
    updated_at
from (
  select 1 as id, 'Alice' as name, 'Kings' as county, current_timestamp as updated_at from dummy
  union all
  select 2 as id, 'Bob' as name, 'Mercer' as county, current_timestamp as updated_at from dummy
) data

{% if is_incremental() %}
  where updated_at > (select max(updated_at) from {{ this }})
{% endif %}
"""

models__incremental_with_unary_unique_key_sql = """
{{
  config(
    materialized = "incremental",
    unique_key = 'id',
    unique_as_primary = true
  )
}}

select
    id,
    name,
    updated_at
from (
  select 1 as id, 'Alice' as name, current_timestamp as updated_at from dummy
  union all
  select 2 as id, 'Bob' as name, current_timestamp as updated_at from dummy
) data

{% if is_incremental() %}
  where updated_at > (select max(updated_at) from {{ this }})
{% endif %}
"""

models__incremental_with_empty_unique_key_sql = """
{{
  config(
    materialized = "incremental",
    unique_key = ['id', 'name', 'county'],
    unique_as_primary = 'yes'
  )
}}

select
    id,
    name,
    updated_at
from (
  select 1 as id, 'Alice' as name, current_timestamp as updated_at from dummy
  union all
  select 2 as id, 'Bob' as name, current_timestamp as updated_at from dummy
) data

{% if is_incremental() %}
  where updated_at > (select max(updated_at) from {{ this }})
{% endif %}
"""

class TestIncrementalUniqueKeyAsPrimary:

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "incremental_with_unary_unique_key.sql": models__incremental_with_unary_unique_key_sql,
            "incremental_with_trinary_unique_key.sql": models__incremental_with_trinary_unique_key_sql,
        }

    def test_unary_primary_key_creation(self, project):
        """Test that primary keys are created correctly for a model with a unary unique key."""
        # Run the model with a full refresh
        results = run_dbt(["run", "--full-refresh", "--select", "incremental_with_unary_unique_key"])
        assert len(results) == 1, "Expected exactly one model to run successfully."

        # Verify primary key creation
        model_name = "incremental_with_unary_unique_key"
        expected_keys = ["id"]
        unique_key_query = f"""
            SELECT COLUMN_NAME
            FROM CONSTRAINTS
            WHERE SCHEMA_NAME = '{project.test_schema}'
              AND TABLE_NAME = '{model_name}'
              AND IS_PRIMARY_KEY = 'TRUE';
        """
        result = project.run_sql(unique_key_query, fetch="all")
        primary_key_columns = [row[0].lower() for row in result]
        assert sorted(primary_key_columns) == sorted(expected_keys), (
            f"Primary key mismatch for {model_name}. "
            f"Expected: {expected_keys}, Found: {primary_key_columns}"
        )

    def test_trinary_primary_key_creation(self, project):
        """Test that primary keys are created correctly for a model with a trinary unique key."""
        # Run the model with a full refresh
        results = run_dbt(["run", "--full-refresh", "--select", "incremental_with_trinary_unique_key"])
        assert len(results) == 1, "Expected exactly one model to run successfully."

        # Verify primary key creation
        model_name = "incremental_with_trinary_unique_key"
        expected_keys = ["id", "name", "county"]
        unique_key_query = f"""
            SELECT COLUMN_NAME
            FROM CONSTRAINTS
            WHERE SCHEMA_NAME = '{project.test_schema}'
              AND TABLE_NAME = '{model_name}'
              AND IS_PRIMARY_KEY = 'TRUE';
        """
        result = project.run_sql(unique_key_query, fetch="all")
        primary_key_columns = [row[0].lower() for row in result]
        assert sorted(primary_key_columns) == sorted(expected_keys), (
            f"Primary key mismatch for {model_name}. "
            f"Expected: {expected_keys}, Found: {primary_key_columns}"
        )


class TestIncrementalUniqueKeyAsPrimaryInvalidConfig:
    @pytest.fixture(scope="class")
    def models(self):
        return {
                "incremental_with_empty_unique_key.sql": models__incremental_with_empty_unique_key_sql
        }

    def test_empty_unique_key(self, project):
        results, output = run_dbt_and_capture(expect_pass=False)
        assert len(results) == 1
        assert re.search(r"`unique_as_primary` must be a boolean.", output)