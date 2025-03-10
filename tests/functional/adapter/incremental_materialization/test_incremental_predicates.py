import pytest
from dbt.tests.adapter.incremental.test_incremental_predicates import BaseIncrementalPredicates
from collections import namedtuple

from dbt.tests.util import run_dbt

ResultHolder = namedtuple(
    "ResultHolder",
    [
        "seed_count",
        "model_count",
        "seed_rows",
        "inc_test_model_count",
        "opt_model_count",
        "relation",
    ],
)


models__delete_insert_incremental_predicates_sql = """
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

models__merge_incremental_predicates_sql = """
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


class TestIncrementalPredicatesDeleteInsert(BaseIncrementalPredicates):
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "delete_insert_incremental_predicates.sql": models__delete_insert_incremental_predicates_sql
        }

    def get_test_fields(
        self, project, seed, incremental_model, update_sql_file, opt_model_count=None
    ):
        seed_count = len(run_dbt(["seed", "--select", seed, "--full-refresh"]))

        model_count = len(run_dbt(["run", "--select", incremental_model, "--full-refresh"]))
        # pass on kwarg
        relation = incremental_model
        # update seed in anticipation of incremental model update
        row_count_query = 'select * from "{}"."{}"'.format(project.test_schema, seed)
        # project.run_sql_file(Path("seeds") / Path(update_sql_file + ".sql"))
        seed_rows = len(project.run_sql(row_count_query, fetch="all"))

        # propagate seed state to incremental model according to unique keys
        inc_test_model_count = self.update_incremental_model(incremental_model=incremental_model)

        return ResultHolder(
            seed_count, model_count, seed_rows, inc_test_model_count, opt_model_count, relation
        )
    
class TestIncrementalPredicatesMergeHana(BaseIncrementalPredicates):

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "delete_insert_incremental_predicates.sql": models__merge_incremental_predicates_sql
        }

    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {
            "models": {
                "+incremental_predicates": ["dbt_internal_dest.id != 2"],
                "+incremental_strategy": "merge"
            }
        }
    
    def get_test_fields(
        self, project, seed, incremental_model, update_sql_file, opt_model_count=None
    ):
        seed_count = len(run_dbt(["seed", "--select", seed, "--full-refresh"]))

        model_count = len(run_dbt(["run", "--select", incremental_model, "--full-refresh"]))
        # pass on kwarg
        relation = incremental_model
        # update seed in anticipation of incremental model update
        row_count_query = 'select * from "{}"."{}"'.format(project.test_schema, seed)
        # project.run_sql_file(Path("seeds") / Path(update_sql_file + ".sql"))
        seed_rows = len(project.run_sql(row_count_query, fetch="all"))

        # propagate seed state to incremental model according to unique keys
        inc_test_model_count = self.update_incremental_model(incremental_model=incremental_model)

        return ResultHolder(
            seed_count, model_count, seed_rows, inc_test_model_count, opt_model_count, relation
        )


class TestPredicatesMergeHana(BaseIncrementalPredicates):

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "delete_insert_incremental_predicates.sql": models__merge_incremental_predicates_sql
        }

    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {
            "models": {
                "+predicates": ["dbt_internal_dest.id != 2"],
                "+incremental_strategy": "merge"
            }
        }
    
    def get_test_fields(
        self, project, seed, incremental_model, update_sql_file, opt_model_count=None
    ):
        seed_count = len(run_dbt(["seed", "--select", seed, "--full-refresh"]))

        model_count = len(run_dbt(["run", "--select", incremental_model, "--full-refresh"]))
        # pass on kwarg
        relation = incremental_model
        # update seed in anticipation of incremental model update
        row_count_query = 'select * from "{}"."{}"'.format(project.test_schema, seed)
        # project.run_sql_file(Path("seeds") / Path(update_sql_file + ".sql"))
        seed_rows = len(project.run_sql(row_count_query, fetch="all"))

        # propagate seed state to incremental model according to unique keys
        inc_test_model_count = self.update_incremental_model(incremental_model=incremental_model)

        return ResultHolder(
            seed_count, model_count, seed_rows, inc_test_model_count, opt_model_count, relation
        )