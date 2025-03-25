import logging

import pytest

from dagster_sqlmesh.conftest import SQLMeshTestContext
from dagster_sqlmesh.controller.base import PlanOptions, RunOptions

logger = logging.getLogger(__name__)


@pytest.mark.parametrize(
    "no_auto_upstream,skip_backfill,expected_changes",
    [
        (
            True,
            True,
            {
                "staging_1": "==",  # No change expected
                "staging_2": "==",  # No change expected (seed unchanged)
                "intermediate": "==",  # No change expected
                "full": "==",  # No change expected
            },
        ),
        (
            False,
            True,
            {
                "staging_1": ">=",  # Should increase (no_auto_upstream disabled)
                "staging_2": "==",  # No change expected (seed unchanged)
                "intermediate": ">=",  # Should increase (no_auto_upstream disabled)
                "full": ">=",  # Should increase (upstream changes)
            },
        ),
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
        (
            False,
            False,
            {
                "staging_1": ">=",  # Should increase (both disabled)
                "staging_2": "==",  # No change expected (seed unchanged)
                "intermediate": ">=",  # Should increase (both disabled)
                "full": ">=",  # Should increase (upstream changes)
            },
        ),
    ],
    ids=[
        "both_enabled",
        "only_skip_backfill",
        "only_no_auto_upstream",
        "both_disabled",
    ],
)
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
        sample_sqlmesh_test_context: Test context fixture
        no_auto_upstream: Whether to disable automatic upstream model updates
        skip_backfill: Whether to skip backfilling data
        expected_changes: Dict mapping model names to expected count changes
            "==" means counts should be equal
            ">=" means final count should be greater than or equal to initial
    """
    # Initial run to set up all models
    sample_sqlmesh_test_context.plan_and_run(
        environment="dev",
        start="2023-01-01",
        end="2024-01-01",
        plan_options=PlanOptions(
            execution_time="2024-01-02",
        ),
        run_options=RunOptions(
            execution_time="2024-01-02",
        ),
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

    # Run with specified flags
    sample_sqlmesh_test_context.plan_and_run(
        environment="dev",
        start="2023-01-01",  # TODO SQLMesh may have a bug where it doesn't update full_model if start is not set (full model has the wrong snapshot as a parent)
        end="2024-07-15",
        select_models=["sqlmesh_example.full_model"],
        plan_options=PlanOptions(
            skip_backfill=skip_backfill,
            enable_preview=True,
            execution_time="2024-07-15",
        ),
        run_options=RunOptions(
            no_auto_upstream=no_auto_upstream,
            execution_time="2024-07-15",
        ),
    )

    # Get final counts and debug info
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
    print(f"final_counts: {final_counts}")
    print(
        f"intermediate_model_1: {
            (
                sample_sqlmesh_test_context.query(
                    'SELECT * FROM sqlmesh_example__dev.intermediate_model_1',
                    return_df=True,
                )
            )
        }"
    )
    print(
        f"full_model: {
            (
                sample_sqlmesh_test_context.query(
                    'SELECT * FROM sqlmesh_example__dev.full_model', return_df=True
                )
            )
        }"
    )

    # Verify counts match expectations
    for model, expected_change in expected_changes.items():
        if expected_change == "==":
            assert final_counts[model] == initial_counts[model], (
                f"{model} count should remain unchanged when "
                f"no_auto_upstream={no_auto_upstream} and skip_backfill={skip_backfill}"
            )
        elif expected_change == ">=":
            assert final_counts[model] >= initial_counts[model], (
                f"{model} count should increase when "
                f"no_auto_upstream={no_auto_upstream} and skip_backfill={skip_backfill}"
            )
        else:
            raise ValueError(f"Invalid expected change: {expected_change}")


if __name__ == "__main__":
    pytest.main([__file__])
