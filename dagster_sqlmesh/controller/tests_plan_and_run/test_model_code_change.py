import logging

import pandas as pd
import pytest
from pandas.testing import assert_frame_equal

from dagster_sqlmesh.conftest import SQLMeshTestContext
from dagster_sqlmesh.controller.base import PlanOptions

logger = logging.getLogger(__name__)


@pytest.mark.parametrize(
    "skip_backfill,expected_changes",
    [
        (
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
        "skip_backfill = False",
    ],
)
# @pytest.mark.skip(reason="Work in progress test")
def test_given_model_chain_when_running_with_different_flags_then_behaves_as_expected(
    sample_sqlmesh_test_context: SQLMeshTestContext,
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

    # Store initial timestamp before making changes
    full_model_df_initial = (
        sample_sqlmesh_test_context.query(
            """
        SELECT *
        FROM sqlmesh_example__dev.full_model
        """,
            return_df=True,
        )
        .sort_values(by="item_id")
        .reset_index(drop=True)
    )
    last_updated_initial = full_model_df_initial["last_updated_at"].iloc[0]

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
        CONCAT('item - ', main.item_id) as item_name
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
            skip_tests=True,  # This is because the test_full_model.yaml is not updated to reflect the new model changes
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

    full_model_df = (
        sample_sqlmesh_test_context.query(
            """
        SELECT *
        FROM sqlmesh_example__dev.full_model
        """,
            return_df=True,
        )
        .sort_values(by="item_id")
        .reset_index(drop=True)
    )

    expected_full_model_df = pd.DataFrame(
        {
            "item_id": pd.Series([1, 2, 3], dtype="int32"),
            "item_name": ["item - 1", "item - 2", "item - 3"],
            "num_orders": [5, 1, 1],
        }
    )

    print("full_model_df")
    print(full_model_df.drop(columns=["last_updated_at"]))
    print("expected_full_model_df")
    print(expected_full_model_df)

    assert_frame_equal(
        full_model_df.drop(columns=["last_updated_at"]),
        expected_full_model_df,
        check_like=True,
    )

    # Store the last_updated_at timestamps after the model change
    last_updated_after_change = full_model_df["last_updated_at"].iloc[0]

    # Verify the timestamp changed after the model modification
    assert last_updated_after_change > last_updated_initial, (
        "Expected last_updated_at timestamp to change after model modification"
    )

    # Run plan and run again with no changes
    sample_sqlmesh_test_context.plan_and_run(
        environment="dev",
        plan_options=PlanOptions(
            skip_backfill=skip_backfill,
            enable_preview=True,
            skip_tests=True,
        ),
    )

    # Get the new timestamps
    full_model_df_no_changes = (
        sample_sqlmesh_test_context.query(
            """
        SELECT *
        FROM sqlmesh_example__dev.full_model
        """,
            return_df=True,
        )
        .sort_values(by="item_id")
        .reset_index(drop=True)
    )
    last_updated_no_changes = full_model_df_no_changes["last_updated_at"].iloc[0]

    # Verify timestamps haven't changed
    assert last_updated_after_change == last_updated_no_changes, (
        "Expected last_updated_at timestamps to remain unchanged when no model changes were made"
    )


if __name__ == "__main__":
    pytest.main([__file__])
