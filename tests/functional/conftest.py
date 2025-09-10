import pytest
import os
from hdbcli import dbapi
# Import the fuctional fixtures as a plugin
# Note: fixtures with session scope need to be local

pytest_plugins = ["dbt.tests.fixtures.project"]


# The profile dictionary, used to write out profiles.yml
@pytest.fixture(scope="class")
def dbt_profile_target():
   
    return {
        'type' : 'saphanacloud' , # This should match the `type` returned by your adapter's `Credentials` class
        'host' : os.getenv('DBT_HANA_HOST'),  # Replace with your SAP HANA host address
        'port': os.getenv('DBT_HANA_PORT'),  # The default port for SAP HANA; change if necessary
        'user': os.getenv('DBT_HANA_USER'),  # Your SAP HANA username
        'password': os.getenv('DBT_HANA_PASSWORD'),  # Your SAP HANA password
        'database': os.getenv('DBT_HANA_DATABASE'),  # The database you want to connect to
        'schema': os.getenv('DBT_HANA_SCHEMA'), # The schema you want to use
        'threads': 1
    }

@pytest.fixture(scope="class")
def unique_schema(request, prefix) -> str:
    return os.getenv('DBT_HANA_SCHEMA')