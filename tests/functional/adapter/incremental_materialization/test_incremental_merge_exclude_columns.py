import pytest
from dbt.tests.adapter.incremental.test_incremental_merge_exclude_columns import (
    BaseMergeExcludeColumns,
)
from collections import namedtuple

from dbt.tests.util import run_dbt
ResultHolder = namedtuple(
    "ResultHolder",
    [
        "seed_count",
        "model_count",
        "seed_rows",
        "inc_test_model_count",
        "relation",
    ],
)
models__merge_exclude_columns_sql = """
{{ config(
    materialized = 'incremental',
    unique_key = 'id',
    incremental_strategy='merge',
    merge_exclude_columns=['msg']
) }}

{% if not is_incremental() %}

-- data for first invocation of model

select 1 as id, 'hello' as msg, 'blue' as color from dummy
union all
select 2 as id, 'goodbye' as msg, 'red' as color from dummy

{% else %}

-- data for subsequent incremental update

select 1 as id, 'hey' as msg, 'blue' as color from dummy
union all
select 2 as id, 'yo' as msg, 'green' as color from dummy
union all
select 3 as id, 'anyway' as msg, 'purple' as color from dummy

{% endif %}
"""

class TestBaseMergeExcludeColumnsHana(BaseMergeExcludeColumns):
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "merge_exclude_columns.sql": models__merge_exclude_columns_sql
        }
    
    def get_test_fields(self, project, seed, incremental_model, update_sql_file):
        seed_count = len(run_dbt(["seed", "--select", seed, "--full-refresh"]))

        model_count = len(run_dbt(["run", "--select", incremental_model, "--full-refresh"]))

        relation = incremental_model
        # update seed in anticipation of incremental model update
        row_count_query = 'select * from "{}"."{}"'.format(project.test_schema, seed)

        seed_rows = len(project.run_sql(row_count_query, fetch="all"))

        # propagate seed state to incremental model according to unique keys
        inc_test_model_count = self.update_incremental_model(incremental_model=incremental_model)

        return ResultHolder(seed_count, model_count, seed_rows, inc_test_model_count, relation)