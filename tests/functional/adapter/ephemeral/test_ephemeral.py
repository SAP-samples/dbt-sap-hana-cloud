import pytest
import os
import re

import pytest

from dbt.tests.util import check_relations_equal, run_dbt

from dbt.tests.util import run_dbt, check_relations_equal

from dbt.tests.adapter.ephemeral.test_ephemeral import BaseEphemeral

from dbt.tests.adapter.ephemeral.test_ephemeral import (
    models__super_dependent_sql,
    models__base__female_only_sql,
models__base__base_sql,
models__base__base_copy_sql,
ephemeral_errors__base__base_copy_sql,
models__dependent_sql,
models__double_dependent_sql,
ephemeral_errors__dependent_sql,
seeds__seed_csv
)

models__base__base_sql = """
{{ config(materialized='ephemeral') }}

select * from "{{ this.schema }}"."seed"

"""


ephemeral_errors__base__base_sql = """
{{ config(materialized='ephemeral') }}

select * from "{{ this.schema }}"."seed"

"""

class BaseEphemeralMulti:
    @pytest.fixture(scope="class")
    def seeds(self):
        return {
            "seed.csv": seeds__seed_csv,
        }

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "dependent.sql": models__dependent_sql,
            "double_dependent.sql": models__double_dependent_sql,
            "super_dependent.sql": models__super_dependent_sql,
            "base": {
                "female_only.sql": models__base__female_only_sql,
                "base.sql": models__base__base_sql,
                "base_copy.sql": models__base__base_copy_sql,
            },
        }

    def test_ephemeral_multi(self, project):
        run_dbt(["seed"])
        results = run_dbt(["run"])
        assert len(results) == 3

        check_relations_equal(project.adapter, ["seed", "dependent"])
        check_relations_equal(project.adapter, ["seed", "double_dependent"])
        check_relations_equal(project.adapter, ["seed", "super_dependent"])
        assert os.path.exists("./target/run/test/models/double_dependent.sql")
        with open("./target/run/test/models/double_dependent.sql", "r") as fp:
            sql_file = fp.read()

        sql_file = re.sub(r"\d+", "", sql_file)
        expected_sql = (
            'create view "dbt"."test_test_ephemeral"."double_dependent__dbt_tmp" as ('
            "with __dbt__cte__base as ("
            'select * from "test_test_ephemeral"."seed"'
            "),  __dbt__cte__base_copy as ("
            "select * from __dbt__cte__base"
            ")-- base_copy just pulls from base. Make sure the listed"
            "-- graph of CTEs all share the same dbt_cte__base cte"
            "select * from __dbt__cte__base where gender = 'Male'"
            "union all"
            "select * from __dbt__cte__base_copy where gender = 'Female'"
            ");"
        )
        sql_file = "".join(sql_file.split())
        expected_sql = "".join(expected_sql.split())
        assert sql_file == expected_sql


class TestEphemeralMultiHana(BaseEphemeralMulti):

    def test_ephemeral_multi(self, project):
        run_dbt(["seed"])
        results = run_dbt(["run"])
        assert len(results) == 3

        check_relations_equal(project.adapter, ["seed", "dependent"])
        check_relations_equal(project.adapter, ["seed", "double_dependent"])
        check_relations_equal(project.adapter, ["seed", "super_dependent"])

class BaseEphemeralErrorHandling(BaseEphemeral):
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "dependent.sql": ephemeral_errors__dependent_sql,
            "base": {
                "base.sql": ephemeral_errors__base__base_sql,
                "base_copy.sql": ephemeral_errors__base__base_copy_sql,
            },
        }