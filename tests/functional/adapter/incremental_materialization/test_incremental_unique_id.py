import pytest
from collections import namedtuple
from pathlib import Path
from dbt.tests.adapter.incremental.test_incremental_unique_id import (BaseIncrementalUniqueKey,
                                                                      seeds__seed_csv,
                                                                      models__empty_str_unique_key_sql,
                                                                      models__expected__one_str__overwrite_sql,
                                                                      models__empty_unique_key_list_sql,
                                                                      models__no_unique_key_sql,
                                                                      models__not_found_unique_key_sql,
                                                                      models__str_unique_key_sql,
                                                                      models__trinary_unique_key_list_sql,
                                                                      models__unary_unique_key_list_sql,
                                                                      models__nontyped_trinary_unique_key_list_sql,
                                                                      models__duplicated_unary_unique_key_list_sql,
                                                                      models__not_found_unique_key_list_sql,
                                                                      models__expected__unique_key_list__inplace_overwrite_sql)
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

models__expected__unique_key_list__inplace_overwrite_sql = """
SELECT
    'CT' AS state,
    'Hartford' AS county,
    'Hartford' AS city,
    TO_DATE('2022-02-14', 'YYYY-MM-DD') as last_visit_date FROM DUMMY
union all
SELECT 'MA','Suffolk','Boston',TO_DATE('2020-02-12',  'YYYY-MM-DD') FROM DUMMY
union all
SELECT 'NJ','Mercer','Trenton',TO_DATE('2022-01-01',  'YYYY-MM-DD') FROM DUMMY
union all
SELECT 'NY','Kings','Brooklyn', TO_DATE('2021-04-02',  'YYYY-MM-DD') FROM DUMMY
union all
SELECT 'NY','New York','Manhattan', TO_DATE('2021-04-01',  'YYYY-MM-DD') FROM DUMMY
union all
SELECT 'PA','Philadelphia','Philadelphia', TO_DATE('2021-05-21', 'YYYY-MM-DD') FROM DUMMY
"""

models__expected__one_str__overwrite_sql  = """
SELECT
    'CT' AS state,
    'Hartford' AS county,
    'Hartford' AS city,
    TO_DATE('2022-02-14', 'YYYY-MM-DD') as last_visit_date FROM DUMMY
union all
SELECT 'MA','Suffolk','Boston',TO_DATE('2020-02-12',  'YYYY-MM-DD') FROM DUMMY
union all
SELECT 'NJ','Mercer','Trenton',TO_DATE('2022-01-01',  'YYYY-MM-DD') FROM DUMMY
union all
SELECT 'NY','Kings','Brooklyn', TO_DATE('2021-04-02',  'YYYY-MM-DD') FROM DUMMY
union all
SELECT 'NY','New York','Manhattan', TO_DATE('2021-04-01',  'YYYY-MM-DD') FROM DUMMY
union all
SELECT 'PA','Philadelphia','Philadelphia', TO_DATE('2021-05-21', 'YYYY-MM-DD') FROM DUMMY
"""

seeds__duplicate_insert_sql = """
-- Insert statement which when applied to seed.csv triggers the inplace
--   overwrite strategy of incremental models. Seed and incremental model
--   diverge.

-- insert new row, which should not be in incremental model
--  with primary or first three columns unique
insert into "{schema}"."seed"
    (state, county, city, last_visit_date)
values ('CT','Hartford','Hartford','2022-02-14');

"""

seeds__add_new_rows_sql = """
-- Insert statement which when applied to seed.csv sees incremental model
--   grow in size while not (necessarily) diverging from the seed itself.

-- insert two new rows, both of which should be in incremental model
--   with any unique columns
insert into "{schema}"."seed"
    (state, county, city, last_visit_date)
values ('WA','King','Seattle','2022-02-01');

insert into "{schema}"."seed"
    (state, county, city, last_visit_date)
values ('CA','Los Angeles','Los Angeles','2022-02-01');

"""

class TestIncrementalUniqueKey(BaseIncrementalUniqueKey):

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "trinary_unique_key_list.sql": models__trinary_unique_key_list_sql,
            "nontyped_trinary_unique_key_list.sql": models__nontyped_trinary_unique_key_list_sql,
            "unary_unique_key_list.sql": models__unary_unique_key_list_sql,
            "not_found_unique_key.sql": models__not_found_unique_key_sql,
            "empty_unique_key_list.sql": models__empty_unique_key_list_sql,
            "no_unique_key.sql": models__no_unique_key_sql,
            "empty_str_unique_key.sql": models__empty_str_unique_key_sql,
            "str_unique_key.sql": models__str_unique_key_sql,
            "duplicated_unary_unique_key_list.sql": models__duplicated_unary_unique_key_list_sql,
            "not_found_unique_key_list.sql": models__not_found_unique_key_list_sql,
            "expected": {
                "one_str__overwrite.sql": models__expected__one_str__overwrite_sql,
                "unique_key_list__inplace_overwrite.sql": models__expected__unique_key_list__inplace_overwrite_sql,
            },
        }

    @pytest.fixture(scope="class")
    def seeds(self):
        return {
            "duplicate_insert.sql": seeds__duplicate_insert_sql,
            "seed.csv": seeds__seed_csv,
            "add_new_rows.sql": seeds__add_new_rows_sql,
        }
    
    def get_test_fields(
        self, project, seed, incremental_model, update_sql_file, opt_model_count=None
    ):
        """build a test case and return values for assertions
        [INFO] Models must be in place to test incremental model
        construction and merge behavior. Database touches are side
        effects to extract counts (which speak to health of unique keys)."""
        # idempotently create some number of seeds and incremental models'''

        seed_count = len(run_dbt(["seed", "--select", seed, "--full-refresh"]))

        model_count = len(run_dbt(["run", "--select", incremental_model, "--full-refresh"]))
        # pass on kwarg
        relation = incremental_model
        # update seed in anticipation of incremental model update
        row_count_query = 'select * from "{}"."{}"'.format(project.test_schema, seed)
        project.run_sql_file(Path("seeds") / Path(update_sql_file + ".sql"))
        seed_rows = len(project.run_sql(row_count_query, fetch="all"))

        # propagate seed state to incremental model according to unique keys
        inc_test_model_count = self.update_incremental_model(incremental_model=incremental_model)

        return ResultHolder(
            seed_count, model_count, seed_rows, inc_test_model_count, opt_model_count, relation
        )