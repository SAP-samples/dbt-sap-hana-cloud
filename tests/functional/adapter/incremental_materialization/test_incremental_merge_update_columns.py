import datetime
from pathlib import Path

import pytest
from dbt.tests.util import run_dbt

# seeds/my_seed.csv
seed_csv = """
user_id,user_name,birth_date,income,last_login_date
1,Easton,1981-05-20,40000,2022-04-25T08:57:59
2,Lillian,1978-09-03,54000,2022-04-25T08:58:59
3,Jeremiah,1982-03-11,10000,2022-04-25T09:57:59
4,Nolan,1976-05-06,900000,2022-04-25T09:58:59
""".lstrip()

# models/my_incr_model.sql
my_incr_model_sql = """
{{config(materialized='incremental', 
         unique_key='user_id',
         merge_update_columns=['user_name', 'income', 'last_login_date'])}}
SELECT * FROM {{ ref('seed') }}
{% if is_incremental() %}
    WHERE last_login_date > (SELECT max(last_login_date) FROM {{ this }})
{% endif %}
"""

# seeds/add_new_rows.sql
seeds__add_new_rows_sql = """
-- insert two new rows, both of which should be in incremental model
INSERT INTO "{schema}"."seed" (user_id, user_name, birth_date, income, last_login_date) VALUES 
        (2, 'Lillian Sr.', '1982-02-03', 200000, '2022-05-01 06:01:31');
INSERT INTO "{schema}"."seed" (user_id, user_name, birth_date, income, last_login_date) VALUES 
        (5, 'John Doe', '1992-10-01', 300000, '2022-06-01 06:01:31');
"""

class TestIncrementalMergeUpdateColumns:

    @pytest.fixture(scope="class")
    def seeds(self):
        return {
            "seed.csv": seed_csv,
            "add_new_rows.sql": seeds__add_new_rows_sql
        }

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "my_incr_model.sql": my_incr_model_sql,
        }
    
    def normalize_row(row):
        normalized = []
        for field in row:
            if isinstance(field, datetime.datetime):
                # Normalize datetime (strip microseconds)
                normalized.append(field.replace(microsecond=0))
            elif isinstance(field, datetime.date):
                # Ensure date remains a date
                normalized.append(field)
            else:
                # Keep other types as they are
                normalized.append(field)
        return tuple(normalized)

    def test_run_dbt(self, project):
        """
        Steps:
            - Run seed
            - Run incremental model
            - Add new rows
            - Run incremental model

        Expected SQL to run:
        
        merge into "dbt_test"."my_incr_model" target
          using "o$pt_my_incr_model150332" temp
          on (
                temp."USER_ID" = target."USER_ID"
            )
        when matched then
          update set
          target."USER_NAME" = temp."USER_NAME",
          target."INCOME" = temp."INCOME",
          target."LAST_LOGIN_DATE" = temp."LAST_LOGIN_DATE"
        when not matched then
          insert("USER_ID", "USER_NAME", "BIRTH_DATE", "INCOME", "LAST_LOGIN_DATE")
          values (
            temp."USER_ID",
            temp."USER_NAME",
            temp."BIRTH_DATE",
            temp."INCOME",
            temp."LAST_LOGIN_DATE"
          )
        """

        def normalize_rows(rows):
            def normalize_field(field):
                if isinstance(field, datetime.datetime):
                    return field.replace(microsecond=0)  # Strip microseconds
                elif isinstance(field, datetime.date):
                    return field  # Keep as date
                else:
                    return field  # Leave other types unchanged

            return [tuple(normalize_field(field) for field in row) for row in rows]

        # Step 1: Run seed
        results = run_dbt(['seed'])
        assert len(results) == 1

        # Step 2: Run the initial incremental model
        results = run_dbt(['run'])
        assert len(results) == 1

        # Step 3: Add new rows to the seed data
        project.run_sql_file(Path("seeds") / Path("add_new_rows.sql"))

        # Step 4: Run the incremental model again
        results = run_dbt(['run'])
        assert len(results) == 1

        # Validate the data for user_id 2
        user_id_2_query = 'SELECT * FROM "{}"."{}" WHERE user_id = {}'.format(project.test_schema,
                                                                              'my_incr_model',
                                                                              2)
        expected_result = [(2, 'Lillian Sr.',
                            datetime.date(1978, 9, 3),
                            200000,
                            datetime.datetime(2022, 5, 1, 6, 1, 31).replace(microsecond=0))]

        result = project.run_sql(user_id_2_query, fetch="all")

        normalized_result = normalize_rows(result)

        assert normalized_result == expected_result

        # Validate the data for user_id 5
        used_id_5_query = 'SELECT * FROM "{}"."{}" WHERE user_id = {}'.format(project.test_schema,
                                                                              'my_incr_model',
                                                                              5)
        expected_result = [(5, 'John Doe',
                            datetime.date(1992, 10, 1),
                            300000,
                            datetime.datetime(2022, 6, 1, 6, 1, 31).replace(microsecond=0))]

        result = project.run_sql(used_id_5_query, fetch="all")

        normalized_result = normalize_rows(result)
        
        assert normalized_result == expected_result
