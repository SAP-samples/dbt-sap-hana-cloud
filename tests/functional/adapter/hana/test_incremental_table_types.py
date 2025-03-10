from collections import namedtuple
import pytest
from dbt.tests.util import check_relations_equal, run_dbt



seeds__expected_delete_insert_incremental_predicates_csv = """id,msg,color
1,hey,blue
2,goodbye,red
2,yo,green
3,anyway,purple
"""

ResultHolder = namedtuple(
    "ResultHolder",
    ["seed_count", "model_count", "seed_rows", "inc_test_model_count", "relation"],
)


class BaseIncrementalPredicates:
    """
    Base test class for testing incremental predicates with delete+insert strategy.
    """

    @pytest.fixture(scope="class")
    def models(self):
        raise NotImplementedError("Subclasses must implement the `models` fixture.")

    @pytest.fixture(scope="class")
    def seeds(self):
        return {
            "expected_delete_insert_incremental_predicates.csv": seeds__expected_delete_insert_incremental_predicates_csv
        }

    @pytest.fixture(scope="class")
    def project_config_update(self):
        """
        Update project configuration to use incremental predicates and delete+insert strategy.
        """
        return {
            "models": {
                "+incremental_predicates": ["id != 2"],
                "+incremental_strategy": "delete+insert",
            }
        }

    def run_incremental_model(self, model_name):
        """
        Runs the specified incremental model and returns the count of executed models.
        """
        results = run_dbt(["run", "--select", model_name])
        return len(results)

    def get_test_fields(self, project, seed, incremental_model):
        """
        Prepare test fields by running seeds and models.
        """
        # Full-refresh the seed
        seed_count = len(run_dbt(["seed", "--select", seed, "--full-refresh"]))

        # Full-refresh the incremental model
        model_count = len(run_dbt(["run", "--select", incremental_model, "--full-refresh"]))

        # Count rows in the seed table
        row_count_query = f'SELECT * FROM "{project.test_schema}"."{seed}"'
        seed_rows = len(project.run_sql(row_count_query, fetch="all"))

        # Run incremental updates
        inc_test_model_count = self.run_incremental_model(incremental_model)

        return ResultHolder(seed_count, model_count, seed_rows, inc_test_model_count, incremental_model)

    def check_table_type(self, project, table_name, expected_table_type):
        """
        Validate that the generated table matches the expected type (row/column).
        """
        schema_name = project.test_schema
        query = f"""
        SELECT IS_COLUMN_TABLE
        FROM TABLES
        WHERE SCHEMA_NAME = '{schema_name}'
          AND TABLE_NAME = '{table_name}';
        """
        result = project.run_sql(query, fetch="one")
        assert result is not None, f"Table {schema_name}.{table_name} does not exist."

        actual_table_type = "column" if result[0] == 'TRUE' else "row"
        assert (
            actual_table_type == expected_table_type
        ), f"Expected {expected_table_type} table but got {actual_table_type} for {table_name}."

    def check_scenario_correctness(self, expected_fields, test_case_fields, project, expected_table_type):
        """
        Invoke assertions to verify correct build functionality, including table type.
        """
        # 1. Test seed(s) should build afresh
        assert expected_fields.seed_count == test_case_fields.seed_count

        # 2. Test model(s) should build afresh
        assert expected_fields.model_count == test_case_fields.model_count

        # 3. Seeds should have intended row counts post update
        assert expected_fields.seed_rows == test_case_fields.seed_rows

        # 4. Incremental test model(s) should be updated
        assert expected_fields.inc_test_model_count == test_case_fields.inc_test_model_count

        # 5. Check table type
        self.check_table_type(project, test_case_fields.relation, expected_table_type)

        # 6. Verify the final table matches the expected table
        check_relations_equal(
            project.adapter, [expected_fields.relation, test_case_fields.relation]
        )

    def get_expected_fields(self, relation, seed_rows):
        """
        Define expected field results for validation.
        """
        return ResultHolder(
            seed_count=1,
            model_count=1,
            inc_test_model_count=1,
            seed_rows=seed_rows,
            relation=relation,
        )

    def test_incremental_predicates(self, project):
        """
        Validate the incremental predicates with delete+insert strategy and table type.
        """
        expected_table_type = "row"  # Change to "column" if testing column tables

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