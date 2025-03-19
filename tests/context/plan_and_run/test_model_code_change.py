import logging

import pytest

from dagster_sqlmesh.controller.base import PlanOptions, RunOptions
from tests.conftest import SQLMeshTestContext

logger = logging.getLogger(__name__)


@pytest.mark.parametrize(
    "no_auto_upstream,skip_backfill,expected_changes",
    [
        (
            True,
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
def test_given_model_chain_when_running_with_different_flags_then_behaves_as_expected(
    model_change_test_context: SQLMeshTestContext,
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
    model_change_test_context.plan_and_run(
        environment="dev",
        start="2023-02-01",
        end="2023-02-03",
        execution_time="2023-02-03",
    )

    # Get initial counts for the model chain
    initial_counts = {
        "staging_1": model_change_test_context.query(
            "SELECT COUNT(*) FROM sqlmesh_example__dev.staging_model_1"
        )[0][0],
        "staging_2": model_change_test_context.query(
            "SELECT COUNT(*) FROM sqlmesh_example__dev.staging_model_2"
        )[0][0],
        "intermediate": model_change_test_context.query(
            "SELECT COUNT(*) FROM sqlmesh_example__dev.intermediate_model_1"
        )[0][0],
        "full": model_change_test_context.query(
            "SELECT COUNT(*) FROM sqlmesh_example__dev.full_model"
        )[0][0],
    }

    print(f"initial_counts: {initial_counts}")
    print(
        f"intermediate_model_1 first run: {
            model_change_test_context.query(
                'SELECT * FROM sqlmesh_example__dev.intermediate_model_1',
                return_df=True,
            )
        }"
    )

    # Modify staging_model_1 to include more data
    model_change_test_context.modify_model_file(
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
        CONCAT(sub.item_name, ' - modified') as item_name
        FROM sqlmesh_example.staging_model_1 AS main
        INNER JOIN sqlmesh_example.staging_model_2 as sub
        ON main.id = sub.id
        WHERE
        event_date BETWEEN @start_date AND @end_date

        """,
    )

    # Run with specified flags
    model_change_test_context.plan_and_run(
        environment="dev",
        start="2023-02-01",
        end="2023-02-03",
        execution_time="2023-02-03",
        plan_options=PlanOptions(
            select_models=[
                "sqlmesh_example.intermediate_model_1",
            ],
            skip_backfill=skip_backfill,
            enable_preview=True,
        ),
        run_options=RunOptions(
            select_models=[
                "sqlmesh_example.intermediate_model_1",
            ],
            no_auto_upstream=no_auto_upstream,
        ),
    )

    # Get final counts and debug info
    final_counts = {
        "seed_1": model_change_test_context.query(
            "SELECT COUNT(*) FROM sqlmesh_example__dev.seed_model_1"
        )[0][0],
        "staging_1": model_change_test_context.query(
            "SELECT COUNT(*) FROM sqlmesh_example__dev.staging_model_1"
        )[0][0],
        "staging_2": model_change_test_context.query(
            "SELECT COUNT(*) FROM sqlmesh_example__dev.staging_model_2"
        )[0][0],
        "intermediate": model_change_test_context.query(
            "SELECT COUNT(*) FROM sqlmesh_example__dev.intermediate_model_1"
        )[0][0],
        "full": model_change_test_context.query(
            "SELECT COUNT(*) FROM sqlmesh_example__dev.full_model"
        )[0][0],
    }
    print(f"first_model_change_counts: {final_counts}")
    # print(
    #     f"intermediate_model_1 after first model change to upstream model: {
    #         model_change_test_context.query(
    #             'SELECT * FROM sqlmesh_example__dev.intermediate_model_1',
    #             return_df=True,
    #         )
    #     }"
    # )

    # # Modify staging_model_1 to include more data
    # model_change_test_context.modify_model_file(
    #     "staging_model_2.sql",
    #     """
    #     MODEL (
    #     name sqlmesh_example.staging_model_2,
    #     grain id
    #     );

    #     SELECT
    #     id,
    #     CONCAT(item_name, ' - modified again') as item_name
    #     FROM
    #     sqlmesh_example.seed_model_2
    #     """,
    # )

    # # Run with specified flags
    # model_change_test_context.plan_and_run(
    #     environment="dev",
    #     start="2023-02-01",
    #     end="2023-02-03",
    #     execution_time="2023-02-03",
    #     plan_options=PlanOptions(
    #         select_models=[
    #             "sqlmesh_example.staging_model_2",
    #         ],
    #         skip_backfill=skip_backfill,
    #         enable_preview=True,
    #     ),
    #     # run_options=RunOptions(
    #     #     select_models=[
    #     #         "sqlmesh_example.staging_model_1",
    #     #     ],
    #     #     no_auto_upstream=no_auto_upstream,
    #     # ),
    # )

    # print(
    #     f"intermediate_model_1 after second model change to upstream model: {
    #         model_change_test_context.query(
    #             'SELECT * FROM sqlmesh_example__dev.intermediate_model_1',
    #             return_df=True,
    #         )
    #     }"
    # )

    raise Exception("Stop here")

    # # Verify counts match expectations
    # for model, expected_change in expected_changes.items():
    #     if expected_change == "==":
    #         assert final_counts[model] == initial_counts[model], (
    #             f"{model} count should remain unchanged when "
    #             f"no_auto_upstream={no_auto_upstream} and skip_backfill={skip_backfill}"
    #         )
    #     elif expected_change == ">=":
    #         assert final_counts[model] >= initial_counts[model], (
    #             f"{model} count should increase when "
    #             f"no_auto_upstream={no_auto_upstream} and skip_backfill={skip_backfill}"
    #         )
    #     else:
    #         raise ValueError(f"Invalid expected change: {expected_change}")


if __name__ == "__main__":
    pytest.main([__file__])
