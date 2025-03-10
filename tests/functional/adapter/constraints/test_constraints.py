import pytest

from tests.functional.adapter.constraints.fixtures import (model_schema_yml,
                                                           constrained_model_schema_yml,
                                                           my_model_sql,
                                                           my_model_with_nulls_sql,
                                                           my_model_wrong_name_sql,
                                                           my_model_wrong_order_sql,
                                                           my_model_view_wrong_name_sql,
                                                           my_model_view_wrong_order_sql,
                                                           my_model_incremental_wrong_name_sql,
                                                           my_model_incremental_wrong_order_sql,
                                                           my_model_incremental_with_nulls_sql,
                                                           my_incremental_model_sql)


from dbt.tests.adapter.constraints.test_constraints import (
    BaseTableConstraintsColumnsEqual,
    BaseViewConstraintsColumnsEqual,
    BaseIncrementalConstraintsColumnsEqual,
    BaseConstraintsRuntimeDdlEnforcement,
    BaseConstraintsRollback,
    BaseIncrementalConstraintsRuntimeDdlEnforcement,
    BaseIncrementalConstraintsRollback,
    BaseTableContractSqlHeader,
    BaseIncrementalContractSqlHeader,
    BaseModelConstraintsRuntimeEnforcement,
    BaseConstraintQuotedColumn,
    BaseIncrementalForeignKeyConstraint,
)

_expected_sql_hana = """
DO
BEGIN
create table <model_identifier>(
    "id" integer not null primary key,
    check ("id" > 0),
    color char(10),
    date_day date
    ); 
insert into <model_identifier>(
    select id,
    color,
    date_day 
    from(
        select 
        'blue' as color,
        1 as id,
        to_date('2019-01-01','yyyy-mm-dd')as date_day 
        from dummy)
    model_subq);
END;
"""


class HanaColumnsEqualSetup:

    @pytest.fixture
    def string_type(self):
        return "CHAR"

    @pytest.fixture
    def int_type(self):
        return "INTEGER"
    
    @pytest.fixture
    def schema_string_type(self, string_type):
        return string_type

    @pytest.fixture
    def schema_int_type(self, int_type):
        return int_type

    @pytest.fixture
    def data_types(self, schema_int_type, int_type, string_type):
        # sql_column_value, schema_data_type, error_data_type
        return [
            ["1", schema_int_type, int_type],
            ["'1'", string_type, string_type],
            ["TO_DATE('2019-01-01', 'YYYY-MM-DD')", 'date', "DATE"],
            ["true", "boolean", "BOOLEAN"]
            ]
            

class TestHanaTableConstraintsColumnsEqual(HanaColumnsEqualSetup, BaseTableConstraintsColumnsEqual):

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "my_model_wrong_order.sql": my_model_wrong_order_sql,
            "my_model_wrong_name.sql": my_model_wrong_name_sql,
            "constraints_schema.yml": model_schema_yml,
        }


class TestHanaViewConstraintsColumnsEqual(HanaColumnsEqualSetup, BaseViewConstraintsColumnsEqual):
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "my_model_wrong_order.sql": my_model_view_wrong_order_sql,
            "my_model_wrong_name.sql": my_model_view_wrong_name_sql,
            "constraints_schema.yml": model_schema_yml,
        }


class TestHanaIncrementalConstraintsColumnsEqual(HanaColumnsEqualSetup, BaseIncrementalConstraintsColumnsEqual):
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "my_model_wrong_order.sql": my_model_incremental_wrong_order_sql,
            "my_model_wrong_name.sql": my_model_incremental_wrong_name_sql,
            "constraints_schema.yml": model_schema_yml,
        }



class TestHanaTableConstraintsRuntimeDdlEnforcement(BaseConstraintsRuntimeDdlEnforcement):
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "my_model.sql": my_model_wrong_order_sql,
            "constraints_schema.yml": model_schema_yml,
        }

    @pytest.fixture(scope="class")
    def expected_sql(self):
        return _expected_sql_hana


class TestHanaTableConstraintsRollback(BaseConstraintsRollback):

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "my_model.sql": my_model_sql,
            "constraints_schema.yml": model_schema_yml,
        }

    @pytest.fixture(scope="class")
    def null_model_sql(self):
        return my_model_with_nulls_sql

    @pytest.fixture(scope="class")
    def expected_error_messages(self):
        return ['cannot insert NULL or update to NULL: "USR_AYVM3D4C3AYWKTWMVUC8U518I"."(DO statement)"']


class TestHanaIncrementalConstraintsRuntimeDdlEnforcement(
    BaseIncrementalConstraintsRuntimeDdlEnforcement
):
        @pytest.fixture(scope="class")
        def models(self):
            return {
                "my_model.sql": my_model_incremental_wrong_order_sql,
                "constraints_schema.yml": model_schema_yml,
            }

        @pytest.fixture(scope="class")
        def expected_sql(self):
            return _expected_sql_hana


class TestHanaIncrementalConstraintsRollback(BaseIncrementalConstraintsRollback):
        @pytest.fixture(scope="class")
        def models(self):
            return {
                "my_model.sql": my_incremental_model_sql,
                "constraints_schema.yml": model_schema_yml,
            }

        @pytest.fixture(scope="class")
        def null_model_sql(self):
            return my_model_incremental_with_nulls_sql

        @pytest.fixture(scope="class")
        def expected_error_messages(self):
            return ['cannot insert NULL or update to NULL: "USR_AYVM3D4C3AYWKTWMVUC8U518I"."(DO statement)"']


class TestHanaModelConstraintsRuntimeEnforcement(BaseModelConstraintsRuntimeEnforcement):
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "my_model.sql": my_model_sql,
            "constraints_schema.yml": constrained_model_schema_yml,
        }

    @pytest.fixture(scope="class")
    def expected_sql(self):
        return """
    DO
    BEGIN
    create table <model_identifier> (
        "id" integer not null,
        color char(20),
        date_day date,
    
        check ("id" > 0),
        primary key ("id"),
        constraint strange_uniqueness_requirement unique (color, date_day)
    ) ; 
    insert into <model_identifier>(
        select id,
        color,
        date_day 
        from(
            select 
            1 as id,
            'blue' as color,
            to_date('2019-01-01','yyyy-mm-dd')as date_day 
            from dummy)
    model_subq);
    END;
    """