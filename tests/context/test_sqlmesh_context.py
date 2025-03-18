import logging

import pytest

from dagster_sqlmesh.controller.base import PlanOptions, RunOptions
from tests.conftest import SQLMeshTestContext

logger = logging.getLogger(__name__)


# def test_basic_sqlmesh_context(sample_sqlmesh_test_context: SQLMeshTestContext):
#     sample_sqlmesh_test_context.plan_and_run(
#         environment="dev",
#     )

#     staging_model_count = sample_sqlmesh_test_context.query(
#         """
#     SELECT COUNT(*) as items FROM sqlmesh_example__dev.staging_model_1
#     """
#     )
#     assert staging_model_count[0][0] == 7


# def test_sqlmesh_context(sample_sqlmesh_test_context: SQLMeshTestContext):
#     logger.debug("SQLMESH MATERIALIZATION 1")
#     sample_sqlmesh_test_context.plan_and_run(
#         environment="dev",
#         start="2023-01-01",
#         end="2024-01-01",
#         execution_time="2024-01-02",
#     )

#     staging_model_count = sample_sqlmesh_test_context.query(
#         """
#     SELECT COUNT(*) as items FROM sqlmesh_example__dev.staging_model_1
#     """
#     )
#     assert staging_model_count[0][0] == 5

#     logger.debug("SQLMESH MATERIALIZATION 2")
#     sample_sqlmesh_test_context.plan_and_run(
#         environment="dev",
#         start="2024-01-01",
#         end="2024-07-07",
#         execution_time="2024-07-08",
#     )

#     staging_model_count = sample_sqlmesh_test_context.query(
#         """
#     SELECT COUNT(*) FROM sqlmesh_example__dev.staging_model_1
#     """
#     )
#     assert staging_model_count[0][0] == 7

#     test_source_model_count = sample_sqlmesh_test_context.query(
#         """
#     SELECT COUNT(*) FROM sqlmesh_example__dev.staging_model_3
#     """
#     )
#     assert test_source_model_count[0][0] == 2

#     sample_sqlmesh_test_context.append_to_test_source(
#         polars.DataFrame(
#             {
#                 "id": [3, 4, 5],
#                 "name": ["test", "test", "test"],
#             }
#         )
#     )
#     logger.debug("SQLMESH MATERIALIZATION 3")
#     sample_sqlmesh_test_context.plan_and_run(
#         environment="dev",
#         end="2024-07-10",
#         execution_time="2024-07-10",
#         # restate_models=["sqlmesh_example.staging_model_3"],
#     )

#     staging_model_count = sample_sqlmesh_test_context.query(
#         """
#     SELECT COUNT(*) FROM sqlmesh_example__dev.staging_model_1
#     """
#     )
#     assert staging_model_count[0][0] == 7

#     test_source_model_count = sample_sqlmesh_test_context.query(
#         """
#     SELECT COUNT(*) FROM sqlmesh_example__dev.staging_model_3
#     """
#     )
#     assert test_source_model_count[0][0] == 5

#     logger.debug("SQLMESH MATERIALIZATION 4 - should be no changes")
#     sample_sqlmesh_test_context.plan_and_run(
#         environment="dev",
#         end="2024-07-10",
#         execution_time="2024-07-10",
#     )

#     logger.debug("SQLMESH MATERIALIZATION 5")
#     sample_sqlmesh_test_context.append_to_test_source(
#         polars.DataFrame(
#             {
#                 "id": [6],
#                 "name": ["test"],
#             }
#         )
#     )
#     sample_sqlmesh_test_context.plan_and_run(
#         environment="dev",
#         # restate_models=["sqlmesh_example.staging_model_3"],
#     )

#     print(
#         sample_sqlmesh_test_context.query(
#             """
#     SELECT * FROM sources.test_source
#     """
#         )
#     )

#     test_source_model_count = sample_sqlmesh_test_context.query(
#         """
#     SELECT COUNT(*) FROM sqlmesh_example__dev.staging_model_3
#     """
#     )
#     assert test_source_model_count[0][0] == 6


# def test_given_selective_models_when_planning_and_running_then_only_selected_models_update(
#     sample_sqlmesh_test_context: SQLMeshTestContext,
# ):
#     """Test selective model execution with independent model path (staging_model_3)."""
#     # Initial run to set up all models
#     sample_sqlmesh_test_context.plan_and_run(
#         environment="dev",
#         start="2023-01-01",
#         end="2024-01-01",
#         execution_time="2024-01-02",
#     )

#     # Get initial counts
#     initial_staging_3_count = sample_sqlmesh_test_context.query(
#         "SELECT COUNT(*) FROM sqlmesh_example__dev.staging_model_3"
#     )[0][0]

#     # Add new data to test source
#     sample_sqlmesh_test_context.append_to_test_source(
#         polars.DataFrame(
#             {
#                 "id": [7, 8, 9],
#                 "name": ["new_data", "new_data", "new_data"],
#             }
#         )
#     )

#     # Run with selective model execution
#     sample_sqlmesh_test_context.plan_and_run(
#         environment="dev",
#         end="2024-07-15",
#         execution_time="2024-07-15",
#         plan_options=PlanOptions(
#             select_models=["sqlmesh_example.staging_model_3"],
#             enable_preview=True,
#         ),
#         run_options=RunOptions(
#             select_models=["sqlmesh_example.staging_model_3"],
#         ),
#     )

#     # Verify only staging_model_3 was updated
#     final_staging_3_count = sample_sqlmesh_test_context.query(
#         "SELECT COUNT(*) FROM sqlmesh_example__dev.staging_model_3"
#     )[0][0]
#     assert final_staging_3_count > initial_staging_3_count


def test_given_no_auto_upstream_and_skip_backfill_enabled_when_running_full_model_then_only_full_model_updates(
    sample_sqlmesh_test_context: SQLMeshTestContext,
):
    """Test backfill and no_auto_upstream (both enabled) behavior with dependent models (staging -> intermediate -> full).

    Model chain:
    1. staging_model_1 (INCREMENTAL_BY_TIME_RANGE) reads from seed_model_1
       - Contains id, item_id, event_date
    2. staging_model_2 (VIEW) reads from seed_model_2
       - Contains id, item_name
    3. intermediate_model_1 (INCREMENTAL_BY_TIME_RANGE)
       - Joins staging_1 and staging_2 on id
    4. full_model (FULL)
       - Groups by item_id and counts distinct ids
       - Count will remain same unless new item_ids are added to seed data
    """
    # Initial run to set up all models
    sample_sqlmesh_test_context.plan_and_run(
        environment="dev",
        start="2023-01-01",
        end="2024-01-01",
        execution_time="2024-01-02",
    )

    # Get initial counts for the model chain
    initial_counts = {
        "staging_1": sample_sqlmesh_test_context.query(
            "SELECT COUNT(*) FROM sqlmesh_example__dev.staging_model_1"
        )[0][0],
        "staging_2": sample_sqlmesh_test_context.query(
            "SELECT COUNT(*) FROM sqlmesh_example__dev.staging_model_2"
        )[0][0],
        "intermediate": sample_sqlmesh_test_context.query(
            "SELECT COUNT(*) FROM sqlmesh_example__dev.intermediate_model_1"
        )[0][0],
        "full": sample_sqlmesh_test_context.query(
            "SELECT COUNT(*) FROM sqlmesh_example__dev.full_model"
        )[0][0],
    }

    print(f"initial_counts: {initial_counts}")

    # Actual test: with backfill enabled, changes should propagate through the chain
    sample_sqlmesh_test_context.plan_and_run(
        environment="dev",
        start="2023-01-01",  # TODO SQLMesh may have a bug where it doesn't update full_model if start is not set (full model has the wrong snapshot as a parent)
        end="2024-07-15",
        execution_time="2024-07-15",
        plan_options=PlanOptions(
            select_models=[
                "sqlmesh_example.full_model",
            ],
            skip_backfill=True,
            enable_preview=True,
        ),
        run_options=RunOptions(
            select_models=[
                "sqlmesh_example.full_model",
            ],
            no_auto_upstream=True,
        ),
    )

    # Verify changes propagated through the chain
    final_counts = {
        "seed_1": sample_sqlmesh_test_context.query(
            "SELECT COUNT(*) FROM sqlmesh_example__dev.seed_model_1"
        )[0][0],
        "staging_1": sample_sqlmesh_test_context.query(
            "SELECT COUNT(*) FROM sqlmesh_example__dev.staging_model_1"
        )[0][0],
        "staging_2": sample_sqlmesh_test_context.query(
            "SELECT COUNT(*) FROM sqlmesh_example__dev.staging_model_2"
        )[0][0],
        "intermediate": sample_sqlmesh_test_context.query(
            "SELECT COUNT(*) FROM sqlmesh_example__dev.intermediate_model_1"
        )[0][0],
        "full": sample_sqlmesh_test_context.query(
            "SELECT COUNT(*) FROM sqlmesh_example__dev.full_model"
        )[0][0],
    }
    print(f"initial_counts: {initial_counts}")
    print(f"final_counts: {final_counts}")
    print(
        f"intermediate_model_1: {
            sample_sqlmesh_test_context.query(
                'SELECT * FROM sqlmesh_example__dev.intermediate_model_1',
                return_df=True,
            )
        }"
    )
    print(
        f"full_model: {
            sample_sqlmesh_test_context.query(
                'SELECT * FROM sqlmesh_example__dev.full_model', return_df=True
            )
        }"
    )

    # With no_auto_upstream enabled and skip_backfill enabled:
    assert final_counts["staging_1"] == initial_counts["staging_1"], (
        "staging_model_1 should remain unchanged since skip_backfill is enabled and auto_upstream is disabled"
    )
    assert final_counts["staging_2"] == initial_counts["staging_2"], (
        "staging_model_2 should remain unchanged since seed_model_2 hasn't changed and skip_backfill is enabled and auto_upstream is disabled"
    )
    assert final_counts["intermediate"] == initial_counts["intermediate"], (
        "intermediate should remain unchanged since skip_backfill is enabled and auto_upstream is disabled"
    )
    assert final_counts["full"] == initial_counts["full"], (
        "full model count should remain unchanged since no new item_ids were added as the upstream models did not change"
    )


if __name__ == "__main__":
    pytest.main([__file__])
