import re
import pytest

from tests.functional.adapter.hana.fixtures import (
    models__incremental_sql,
    models__table_sql,
    models__row_table_sql,
    models_invalid__invalid_columns_type_sql,
    models_invalid__invalid_type_sql,
    models_invalid__invalid_unique_config_sql,
    models_invalid__missing_columns_sql,
    seeds__seed_csv,
    snapshots__colors_sql,
)
from dbt.tests.util import run_dbt, run_dbt_and_capture

INDEX_DEFINITION_PATTERN = re.compile(r"\((.+)\)\s+unique\s+index\s+(\w+)\Z")


class TestHanaIndex:
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "table1.sql": models__table_sql,
            "table2.sql": models__row_table_sql,
            "incremental.sql": models__incremental_sql,
        }

    @pytest.fixture(scope="class")
    def seeds(self):
        return {"seed.csv": seeds__seed_csv}

    @pytest.fixture(scope="class")
    def snapshots(self):
        return {"colors.sql": snapshots__colors_sql}

    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {
            "config-version": 2,
            "seeds": {
                "quote_columns": False,
                "indexes": [
                    {"columns": ["country_code"], "unique": False, "type": "INVERTED VALUE"},
                    {"columns": ["country_code", "country_name"], "unique": True, "type": "INVERTED VALUE"},
                ],
            },
            "vars": {
                "version": 1,
            },
        }

    def test_table(self, project, unique_schema):
        results = run_dbt(["run", "--models", "table1"])
        assert len(results) == 1

        indexes = self.get_indexes("table1", project, unique_schema)
        expected = [
            {"columns": "column_b", "unique": False, "type": "inverted value"},
            {"columns": "column_a, column_b", "unique": False, "type": "inverted value"},
            {"columns": "column_b, column_a", "unique": True, "type": "inverted value"},
            {"columns": "column_b, column_c", "unique": True, "type": "inverted hash"},
            {"columns": "column_a, column_c", "unique": True, "type": "inverted individual"},
            
        ]
        assert len(indexes) == len(expected)

    def test_row_table(self, project, unique_schema):
        results = run_dbt(["run", "--models", "table2"])
        assert len(results) == 1

        indexes = self.get_indexes("table2", project, unique_schema)
        expected = [
            {"columns": "column_a", "unique": False, "type": "btree"},
            {"columns": "column_b", "unique": True, "type": "btree"},
            {"columns": "column_a, column_b", "unique": False, "type": "cpbtree"},
            {"columns": "column_b, column_a", "unique": True, "type": "cpbtree"},
            
        ]
        assert len(indexes) == len(expected)

    def test_incremental(self, project, unique_schema):
        for additional_argument in [[], [], ["--full-refresh"]]:
            results = run_dbt(["run", "--models", "incremental"] + additional_argument)
            assert len(results) == 1

            indexes = self.get_indexes("incremental", project, unique_schema)
            expected = [
                {"columns": "column_a", "unique": False, "type": "inverted value"},
                {"columns": "column_a, column_b", "unique": True, "type": "inverted individual"},
            ]
            assert len(indexes) == len(expected)

    def test_seed(self, project, unique_schema):
        for additional_argument in [[], [], ["--full-refresh"]]:
            results = run_dbt(["seed"] + additional_argument)
            assert len(results) == 1

            indexes = self.get_indexes("seed", project, unique_schema)
            expected = [
                {"columns": "country_code", "unique": False, "type": "inverted value"},
                {"columns": "country_code, country_name", "unique": True, "type": "inverted value"},
            ]
            assert len(indexes) == len(expected)

    def test_snapshot(self, project, unique_schema):
        for version in [1, 2]:
            results = run_dbt(["snapshot", "--vars", f"version: {version}"])
            assert len(results) == 1

            indexes = self.get_indexes("colors", project, unique_schema)
            expected = [
                {"columns": "id", "unique": False, "type": "inverted value"},
                {"columns": "id, color", "unique": True, "type": "inverted hash"},
            ]
            assert len(indexes) == len(expected)

    def get_indexes(self, table_name, project, unique_schema):
        sql = f"""
             SELECT
                I.INDEX_NAME AS index_name,
                I.INDEX_TYPE AS index_type,
                STRING_AGG(C.COLUMN_NAME, ', ') AS column_names,
                I.CONSTRAINT AS is_unique
             FROM INDEXES I
             JOIN INDEX_COLUMNS C
                ON I.INDEX_OID = C.INDEX_OID
             WHERE I.SCHEMA_NAME = '{unique_schema}'
                AND I.TABLE_NAME = '{table_name}'
                GROUP BY I.INDEX_NAME, I.INDEX_TYPE, I.CONSTRAINT
                ORDER BY I.INDEX_NAME;
        """
        results = project.run_sql(sql, fetch="all")
        return self.parse_index_definitions(results)

    def parse_index_definitions(self, rows):
        parsed_indexes = []
        for row in rows:
            index_type = row[2].lower()
            columns = row[3]
            unique = row[1] == "TRUE"
            parsed_indexes.append({
                "columns": columns,
                "unique": unique,
                "type": index_type,
            })
        return parsed_indexes

    def assertCountEqual(self, a, b):
        assert len(a) == len(b)


class TestHanaInvalidIndex:
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "invalid_unique_config.sql": models_invalid__invalid_unique_config_sql,
            "invalid_type.sql": models_invalid__invalid_type_sql,
            "invalid_columns_type.sql": models_invalid__invalid_columns_type_sql,
            "missing_columns.sql": models_invalid__missing_columns_sql,
        }

    def test_invalid_index_configs(self, project):
        results, output = run_dbt_and_capture(expect_pass=False)
        assert len(results) == 4
        assert re.search(r"'column_a, column_b' is not of type 'array'", output)
        assert re.search(r"'yes' is not of type 'boolean'", output)
        assert re.search(r"'columns' is a required property", output)
        assert re.search(r'incorrect syntax near "NON_EXISTENT_TYPE"', output)
