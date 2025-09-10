import logging

import polars

from dagster_sqlmesh.testing import SQLMeshTestContext

logger = logging.getLogger(__name__)


def test_basic_sqlmesh_context(sample_sqlmesh_test_context: SQLMeshTestContext):
    sample_sqlmesh_test_context.plan_and_run(
        environment="dev",
    )

    staging_model_count = sample_sqlmesh_test_context.query(
        """
    SELECT COUNT(*) as items FROM sqlmesh_example__dev.staging_model_1
    """
    )
    assert staging_model_count[0][0] == 7


def test_sqlmesh_context(sample_sqlmesh_test_context: SQLMeshTestContext):
    logger.debug("SQLMESH MATERIALIZATION 1")
    sample_sqlmesh_test_context.plan_and_run(
        environment="dev",
        start="2023-01-01",
        end="2024-01-01",
        execution_time="2024-01-02",
    )

    staging_model_count = sample_sqlmesh_test_context.query(
        """
    SELECT COUNT(*) as items FROM sqlmesh_example__dev.staging_model_1
    """
    )
    assert staging_model_count[0][0] == 5

    logger.debug("SQLMESH MATERIALIZATION 2")
    sample_sqlmesh_test_context.plan_and_run(
        environment="dev",
        start="2024-01-01",
        end="2024-07-07",
        execution_time="2024-07-08",
    )

    staging_model_count = sample_sqlmesh_test_context.query(
        """
    SELECT COUNT(*) FROM sqlmesh_example__dev.staging_model_1
    """
    )
    assert staging_model_count[0][0] == 7

    test_source_model_count = sample_sqlmesh_test_context.query(
        """
    SELECT COUNT(*) FROM sqlmesh_example__dev.staging_model_3
    """
    )
    assert test_source_model_count[0][0] == 2

    sample_sqlmesh_test_context.append_to_test_source(
        polars.DataFrame(
            {
                "id": [3, 4, 5],
                "name": ["test", "test", "test"],
            }
        )
    )
    logger.debug("SQLMESH MATERIALIZATION 3")
    sample_sqlmesh_test_context.plan_and_run(
        environment="dev",
        end="2024-07-10",
        execution_time="2024-07-10",
        # restate_models=["sqlmesh_example.staging_model_3"],
    )

    staging_model_count = sample_sqlmesh_test_context.query(
        """
    SELECT COUNT(*) FROM sqlmesh_example__dev.staging_model_1
    """
    )
    assert staging_model_count[0][0] == 7

    test_source_model_count = sample_sqlmesh_test_context.query(
        """
    SELECT COUNT(*) FROM sqlmesh_example__dev.staging_model_3
    """
    )
    assert test_source_model_count[0][0] == 5

    logger.debug("SQLMESH MATERIALIZATION 4 - should be no changes")
    sample_sqlmesh_test_context.plan_and_run(
        environment="dev",
        end="2024-07-10",
        execution_time="2024-07-10",
    )

    logger.debug("SQLMESH MATERIALIZATION 5")
    sample_sqlmesh_test_context.append_to_test_source(
        polars.DataFrame(
            {
                "id": [6],
                "name": ["test"],
            }
        )
    )
    sample_sqlmesh_test_context.plan_and_run(
        environment="dev",
        # restate_models=["sqlmesh_example.staging_model_3"],
    )

    print(
        sample_sqlmesh_test_context.query(
            """
    SELECT * FROM sources.test_source
    """
        )
    )

    test_source_model_count = sample_sqlmesh_test_context.query(
        """
    SELECT COUNT(*) FROM sqlmesh_example__dev.staging_model_3
    """
    )
    assert test_source_model_count[0][0] == 6


def test_restating_models(sample_sqlmesh_test_context: SQLMeshTestContext):
    sample_sqlmesh_test_context.plan_and_run(
        environment="dev",
        start="2023-01-01",
        end="2024-01-01",
        execution_time="2024-01-02",
    )

    count_query = sample_sqlmesh_test_context.query(
        """
    SELECT COUNT(*) FROM sqlmesh_example__dev.staging_model_4
    """
    )
    assert count_query[0][0] == 366

    feb_sum_query = sample_sqlmesh_test_context.query(
        """
    SELECT SUM(value) FROM sqlmesh_example__dev.staging_model_4 WHERE time >= '2023-02-01' AND time < '2023-02-28'
    """
    )
    march_sum_query = sample_sqlmesh_test_context.query(
        """
    SELECT SUM(value) FROM sqlmesh_example__dev.staging_model_4 WHERE time >= '2023-03-01' AND time < '2023-03-31'
    """
    )
    intermediate_2_query = sample_sqlmesh_test_context.query(
        """
    SELECT * FROM sqlmesh_example__dev.intermediate_model_2
    """
    )

    # Restate the model for the month of March
    sample_sqlmesh_test_context.plan_and_run(
        environment="dev",
        start="2023-03-01",
        end="2023-03-31",
        execution_time="2024-01-02",
        select_models=["sqlmesh_example.staging_model_4"],
        restate_selected=True,
        skip_run=True,
    )

    # Check that the sum of values for February and March are the same
    feb_sum_query_restate = sample_sqlmesh_test_context.query(
        """
    SELECT SUM(value) FROM sqlmesh_example__dev.staging_model_4 WHERE time >= '2023-02-01' AND time < '2023-02-28'
    """
    )
    march_sum_query_restate = sample_sqlmesh_test_context.query(
        """
    SELECT SUM(value) FROM sqlmesh_example__dev.staging_model_4 WHERE time >= '2023-03-01' AND time < '2023-03-31'
        """
    )
    intermediate_2_query_restate = sample_sqlmesh_test_context.query(
        """
    SELECT * FROM sqlmesh_example__dev.intermediate_model_2
    """
    )

    assert (
        feb_sum_query_restate[0][0] == feb_sum_query[0][0]
    ), "February sum should not change"
    assert (
        march_sum_query_restate[0][0] != march_sum_query[0][0]
    ), "March sum should change"
    assert (
        intermediate_2_query_restate[0][0] == intermediate_2_query[0][0]
    ), "Intermediate model should not change during restate"
