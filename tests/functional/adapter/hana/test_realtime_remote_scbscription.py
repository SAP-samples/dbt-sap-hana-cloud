import re
import pytest
from dbt.tests.util import run_dbt, run_dbt_and_capture
from dbt.exceptions import CompilationError, DbtRuntimeError


# create the virtual table and target table in SAP HANA Cloud before running these tests  and run these tests seprately

# realtime_model_with_chunk_size = """
# {{ config(
#     materialized='realtime',
#     target_table='TARGET_ORDERS',
#     virtual_table='SOURCE_ORDERS',
#     chunk_size=1000
# ) }}

# """

# # Test models for negative scenarios
# realtime_model_missing_target = """
# {{ config(
#     materialized='realtime',
#     virtual_table='SOURCE_ORDERS'
# ) }}

# """

# realtime_model_missing_virtual = """
# {{ config(
#     materialized='realtime',
#     target_table='TARGET_ORDERS'
# ) }}

# """

# realtime_model_empty_config = """
# {{ config(materialized='realtime') }}


# """


# class TestRealtimeMaterialization:
#     """Test suite for realtime materialization"""

#     @pytest.fixture(scope="class")
#     def models(self):
#         return {
#             "realtime_with_chunk.sql": realtime_model_with_chunk_size,
#         }


# class TestRealtimePositiveScenarios(TestRealtimeMaterialization):
#     """Positive test scenarios for realtime materialization"""
        
#     def test_realtime_with_chunk_size(self, project):
#         """Test realtime materialization with chunk size configuration"""
#         # Run dbt
#         results = run_dbt(["run", "--select", "realtime_with_chunk"])
        
#         # Assert successful run
#         assert len(results) == 1
#         assert results[0].status == "success"
        
#         # Verify subscription was created successfully
#         # Note: We don't check relation creation as this is a remote subscription
#         # The success status indicates the subscription was properly configured


# class TestRealtimeNegativeScenarios:
#     @pytest.fixture(scope="class")
#     def models(self):
#         return {
#             "realtime_missing_target.sql": realtime_model_missing_target,
#             "realtime_missing_virtual.sql": realtime_model_missing_virtual,
#             "realtime_empty_config.sql": realtime_model_empty_config,
#         }

#     def test_invalid_configs(self, project):
#         results, output = run_dbt_and_capture(expect_pass=False)
#         assert len(results) == 3
#         assert re.search(r"target_table must be specified in config for realtime materialization", output)
#         assert re.search(r"virtual_table must be specified in config for realtime materialization", output)
#         assert re.search(r"target_table must be specified in config for realtime materialization", output)
