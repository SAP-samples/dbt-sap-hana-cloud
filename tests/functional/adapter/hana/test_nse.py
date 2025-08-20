import pytest
from dbt.tests.util import run_dbt

def get_nse_page_loadable_status(project, schema_name, table_name):
    query = f"""
    SELECT LOAD_UNIT
    FROM TABLES
    WHERE SCHEMA_NAME = '{schema_name}'
      AND TABLE_NAME = '{table_name}'
    """
    result = project.run_sql(query, fetch="one")
    return result[0] if result else None

def get_column_nse_status(project, schema_name, table_name, column_name):
    query = f"""
    SELECT LOAD_UNIT
    FROM M_CS_COLUMNS
    WHERE SCHEMA_NAME = '{schema_name}'
      AND TABLE_NAME = '{table_name}'
      AND COLUMN_NAME = '{column_name}'
    """
    result = project.run_sql(query, fetch="one")
    return result[0] if result else None

class BaseNSETableTest:
    table_name = None
    
    def test_table_nse_page_loadable(self, project):
        run_dbt(["run"])
        schema_name = project.test_schema
        status = get_nse_page_loadable_status(project, schema_name, self.table_name)
        assert status == 'PAGE', f"NSE PAGE LOADABLE not set for table {self.table_name}"

class BaseNSEColumnTest:
    column_table_name = None
    columns_to_test = None
    
    def test_column_nse_page_loadable(self, project):
        run_dbt(["run"])
        schema_name = project.test_schema
        for col in self.columns_to_test:
            status = get_column_nse_status(project, schema_name, self.column_table_name, col)
            assert status == 'PAGE', f"NSE PAGE LOADABLE not set for column {col} in table {self.column_table_name}"

class TestNSETableMaterialized(BaseNSETableTest):
    table_name = "nse_table_materialized"
    
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "nse_table_materialized.sql": """
            {{ config(
                materialized='table',
                nse_page_loadable={"type": "table"}
            ) }}
            SELECT 1 AS id, 'test' AS name FROM dummy
            """,
        }

class TestNSETableColumns(BaseNSEColumnTest):
    column_table_name = "nse_table_columns"
    columns_to_test = ["ID", "NAME"]
    
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "nse_table_columns.sql": """
            {{ config(
                materialized='table',
                nse_page_loadable={"type": "column", "names": ["ID", "NAME"]}
            ) }}
            SELECT 1 AS id, 'test' AS name FROM dummy
            """,
        }

class TestNSEIncrementalMaterialized(BaseNSETableTest):
    table_name = "nse_incremental_materialized"
    
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "nse_incremental_materialized.sql": """
            {{ config(
                materialized='incremental',
                nse_page_loadable={"type": "table"}
            ) }}
            SELECT 1 AS id, 'test' AS name FROM dummy
            """,
        }

class TestNSEIncrementalColumns(BaseNSEColumnTest):
    column_table_name = "nse_incremental_columns"
    columns_to_test = ["ID", "NAME"]
    
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "nse_incremental_columns.sql": """
            {{ config(
                materialized='incremental',
                nse_page_loadable={"type": "column", "names": ["ID", "NAME"]}
            ) }}
            SELECT 1 AS id, 'test' AS name FROM dummy
            """,
        }
    
class TestNSEInvalidColumn(BaseNSEColumnTest):
    column_table_name = "nse_invalid_column"
    columns_to_test = ["ID", "NOT_A_COLUMN"]  # NOT_A_COLUMN does not exist

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "nse_invalid_column.sql": """
            {{ config(
                materialized='table',
                nse_page_loadable={"type": "column", "names": ["ID", "NOT_A_COLUMN"]}
            ) }}
            SELECT 1 AS id, 'test' AS name FROM dummy
            """,
        }

    def test_column_nse_page_loadable(self, project):
        run_dbt(["run"], expect_pass=False) 

class TestNSEInvalidType(BaseNSETableTest):
    table_name = "nse_invalid_type"

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "nse_invalid_type.sql": """
            {{ config(
                materialized='table',
                nse_page_loadable={"type": "invalid_type"}
            ) }}
            SELECT 1 AS id, 'test' AS name FROM dummy
            """,
        }

    def test_table_nse_page_loadable(self, project):
        run_dbt(["run"], expect_pass=False)