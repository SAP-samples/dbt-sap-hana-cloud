import os
import pytest
from dbt.tests.util import run_dbt

from tests.functional.adapter.hana.test_incremental_table_types import (
    BaseIncrementalPredicates
)

models__delete_insert_empty_incremental_predicates_sql = """
{{ config(
    materialized = 'incremental',
    unique_key = 'id',
) }}

{% if not is_incremental() %}

select 1 as id, 'hello' as msg, 'blue' as color from dummy
union all
select 2 as id, 'goodbye' as msg, 'red' as color from dummy

{% else %}

-- delete will not happen on the above record where id = 2, so new record will be inserted instead
select 1 as id, 'hey' as msg, 'blue' as color from dummy
union all
select 2 as id, 'yo' as msg, 'green' as color from dummy
union all
select 3 as id, 'anyway' as msg, 'purple' as color from dummy

{% endif %}
"""
models__delete_insert_row_incremental_predicates_sql = """
{{ config(
    materialized = 'incremental',
    unique_key = 'id',
    table_type='row'
) }}

{% if not is_incremental() %}

select 1 as id, 'hello' as msg, 'blue' as color from dummy
union all
select 2 as id, 'goodbye' as msg, 'red' as color from dummy

{% else %}

-- delete will not happen on the above record where id = 2, so new record will be inserted instead
select 1 as id, 'hey' as msg, 'blue' as color from dummy
union all
select 2 as id, 'yo' as msg, 'green' as color from dummy
union all
select 3 as id, 'anyway' as msg, 'purple' as color from dummy

{% endif %}
"""
models__delete_insert_column_incremental_predicates_sql = """
{{ config(
    materialized = 'incremental',
    unique_key = 'id',
    table_type='column'
) }}

{% if not is_incremental() %}

select 1 as id, 'hello' as msg, 'blue' as color from dummy
union all
select 2 as id, 'goodbye' as msg, 'red' as color from dummy

{% else %}

-- delete will not happen on the above record where id = 2, so new record will be inserted instead
select 1 as id, 'hey' as msg, 'blue' as color from dummy
union all
select 2 as id, 'yo' as msg, 'green' as color from dummy
union all
select 3 as id, 'anyway' as msg, 'purple' as color from dummy

{% endif %}
"""

class BaseTableTypeTests:
    """
    Base class for testing table types (row vs. column) in SAP HANA.
    Subclasses must define models and expected table types.
    """

    @pytest.fixture(scope="class")
    def models(self):
        raise NotImplementedError("Subclasses must implement the `models` fixture.")

    @pytest.fixture(scope="class")
    def expected_table_types(self):
        """
        Provide expected table types for the models.
        Format: {"model_name": "row" or "column"}
        """
        raise NotImplementedError("Subclasses must implement the `expected_table_types` fixture.")

    def test_table_types(self, project, expected_table_types):
        # Run the models
        results = run_dbt(["run"])
        assert len(results) == len(expected_table_types), "Mismatch in number of executed models."

        # Validate each model's table type
        for model_name, expected_type in expected_table_types.items():
            schema_name = project.test_schema
            query = f"""
            SELECT IS_COLUMN_TABLE
            FROM TABLES
            WHERE SCHEMA_NAME = '{schema_name.upper()}'
              AND TABLE_NAME = '{model_name.upper()}';
            """
            result = project.run_sql(query, fetch="one")
            assert result is not None, f"Table {schema_name}.{model_name} does not exist."

            actual_type = "column" if result[0] == 'TRUE' else "row"
            assert (
                actual_type == expected_type
            ), f"Expected {expected_type} table for {model_name}, but got {actual_type}."


# class TestRowColumnTables(BaseTableTypeTests):
#     @pytest.fixture(scope="class")
#     def models(self):
#         return {
#             "row_table.sql": """
#             {{ config(
#                 materialized='table',
#                 table_type='row'
#             ) }}
#             SELECT 1 AS id, 'test' AS name from dummy
#             """,
#             "column_table.sql": """
#             {{ config(
#                 materialized='table',
#                 table_type='column'
#             ) }}
#             SELECT 1 AS id, 'test' AS name from dummy
#             """,
#         }

#     @pytest.fixture(scope="class")
#     def expected_table_types(self):
#         return {
#             "row_table": "row",
#             "column_table": "column",
#         }
    
class TestIncrementalPredicatesDeleteInsertEmpty(BaseIncrementalPredicates):
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "delete_insert_incremental_predicates.sql": models__delete_insert_empty_incremental_predicates_sql
        }

    def test_incremental_predicates(self, project):
        """
        Validate the incremental predicates with delete+insert strategy and table type.
        """
        expected_table_type = "column"  

        # Expected results
        expected_fields = self.get_expected_fields(
            relation="expected_delete_insert_incremental_predicates", seed_rows=4
        )

        # Actual test results
        test_case_fields = self.get_test_fields(
            project,
            seed="expected_delete_insert_incremental_predicates",
            incremental_model="delete_insert_incremental_predicates",
        )

        # Assert correctness, including table type
        self.check_scenario_correctness(expected_fields, test_case_fields, project, expected_table_type)



class TestIncrementalPredicatesDeleteInsertRow(BaseIncrementalPredicates):
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "delete_insert_incremental_predicates.sql": models__delete_insert_row_incremental_predicates_sql
        }

    def test_incremental_predicates(self, project):
        """
        Validate the incremental predicates with delete+insert strategy and table type.
        """
        expected_table_type = "row"  

        # Expected results
        expected_fields = self.get_expected_fields(
            relation="expected_delete_insert_incremental_predicates", seed_rows=4
        )

        # Actual test results
        test_case_fields = self.get_test_fields(
            project,
            seed="expected_delete_insert_incremental_predicates",
            incremental_model="delete_insert_incremental_predicates",
        )

        # Assert correctness, including table type
        self.check_scenario_correctness(expected_fields, test_case_fields, project, expected_table_type)

class TestIncrementalPredicatesDeleteInsertColumn(BaseIncrementalPredicates):
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "delete_insert_incremental_predicates.sql": models__delete_insert_column_incremental_predicates_sql
        }

    def test_incremental_predicates(self, project):
        """
        Validate the incremental predicates with delete+insert strategy and table type.
        """
        expected_table_type = "column"  

        # Expected results
        expected_fields = self.get_expected_fields(
            relation="expected_delete_insert_incremental_predicates", seed_rows=4
        )

        # Actual test results
        test_case_fields = self.get_test_fields(
            project,
            seed="expected_delete_insert_incremental_predicates",
            incremental_model="delete_insert_incremental_predicates",
        )

        # Assert correctness, including table type
        self.check_scenario_correctness(expected_fields, test_case_fields, project, expected_table_type)