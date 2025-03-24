import logging

import pytest

from dagster_sqlmesh.controller.base import PlanOptions
from tests.conftest import SQLMeshTestContext

logger = logging.getLogger(__name__)


@pytest.mark.parametrize(
    "no_auto_upstream,skip_backfill,expected_changes",
    [
        (
            False,
            False,
            {
                "staging_1": ">=",  # Should increase (skip_backfill disabled)
                "staging_2": "==",  # No change expected (seed unchanged)
                "intermediate": ">=",  # Should increase (skip_backfill disabled)
                "full": ">=",  # Should increase (upstream changes)
            },
        ),
    ],
    ids=[
        "only_skip_backfill",
    ],
)
# @pytest.mark.skip(reason="Work in progress test")
def test_given_model_chain_when_running_with_different_flags_then_behaves_as_expected(
    sample_sqlmesh_test_context: SQLMeshTestContext,
    no_auto_upstream: bool,
    skip_backfill: bool,
    expected_changes: dict[str, str],
):
    """Test how no_auto_upstream and skip_backfill flags affect model chain updates.

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

    Args:
        model_change_test_context: Test context fixture with model modification capabilities
        no_auto_upstream: Whether to disable automatic upstream model updates
        skip_backfill: Whether to skip backfilling data
        expected_changes: Dict mapping model names to expected count changes
            "==" means counts should be equal
            ">=" means final count should be greater than or equal to initial
    """
    # Initial run to set up all models
    sample_sqlmesh_test_context.plan_and_run(
        environment="dev",
    )


    print(
        f"intermediate_model_1 first run: {
            sample_sqlmesh_test_context.query(
                'SELECT * FROM sqlmesh_example__dev.intermediate_model_1',
                return_df=True,
            )
        }"
    )
    print(
        f"full_model first run: {
            sample_sqlmesh_test_context.query(
                'SELECT * FROM sqlmesh_example__dev.full_model',
                return_df=True,
            )
        }"
    )

    # # Modify intermediate_model_1 sql to cause breaking change
    sample_sqlmesh_test_context.modify_model_file(
        "intermediate_model_1.sql",
        """
        MODEL (
        name sqlmesh_example.intermediate_model_1,
        kind INCREMENTAL_BY_TIME_RANGE (
            time_column event_date
        ),
        start '2020-01-01',
        cron '@daily',
        grain (id, event_date)
        );

        SELECT
        main.id,
        main.item_id,
        main.event_date,
        CONCAT(sub.item_name, ' - modified1') as item_name
        FROM sqlmesh_example.staging_model_1 AS main
        INNER JOIN sqlmesh_example.staging_model_2 as sub
        ON main.id = sub.id
        WHERE
        event_date BETWEEN @start_date AND @end_date

        """,
    )


    # Run with specified flags
    sample_sqlmesh_test_context.plan_and_run(
        environment="dev",
        plan_options=PlanOptions(
            skip_backfill=skip_backfill,
            enable_preview=True,
        ),
    )

    print(
        f"intermediate_model_1 after first model change to upstream model: {
            sample_sqlmesh_test_context.query(
                'SELECT * FROM sqlmesh_example__dev.intermediate_model_1',
                return_df=True,
            )
        }"
    )

    print(
        f"full_model after first model change to upstream model: {
            sample_sqlmesh_test_context.query(
                'SELECT * FROM sqlmesh_example__dev.full_model',
                return_df=True,
            )
        }"
    )

    raise Exception("Stop here")



if __name__ == "__main__":
    pytest.main([__file__])
