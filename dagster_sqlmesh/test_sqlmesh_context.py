import logging

import polars
from .conftest import SQLMeshTestContext

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
