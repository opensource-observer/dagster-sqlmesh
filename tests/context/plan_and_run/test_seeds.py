import logging

import pytest

from dagster_sqlmesh.controller.base import PlanOptions, RunOptions
from tests.conftest import SQLMeshTestContext

logger = logging.getLogger(__name__)


@pytest.mark.parametrize(
    "skip_backfill",
    [True, False],
    ids=["skip_backfill_enabled", "skip_backfill_disabled"],
)
def test_given_seed_model_when_selected_then_only_builds_seed(
    sample_sqlmesh_test_context: SQLMeshTestContext,
    skip_backfill: bool,
):
    """Test that selecting a seed model only builds that model.

    This test verifies that when selecting only a seed model:
    1. Only the selected seed model is built
    2. No other models are built or affected
    3. The behavior is consistent regardless of skip_backfill setting
    """
    # Run with only seed_model_1 selected - no need for initial setup since
    # we're testing what gets built in the first place
    sample_sqlmesh_test_context.plan_and_run(
        environment="dev",
        start="2023-01-01",
        end="2024-07-15",
        execution_time="2024-07-15",
        plan_options=PlanOptions(
            select_models=["sqlmesh_example.seed_model_1"],
            skip_backfill=skip_backfill,
            enable_preview=True,
        ),
        run_options=RunOptions(
            select_models=["sqlmesh_example.seed_model_1"],
        ),
    )

    # Verify seed_model_1 exists and has data
    seed_1_count = sample_sqlmesh_test_context.query(
        "SELECT COUNT(*) FROM sqlmesh_example__dev.seed_model_1"
    )[0][0]
    assert seed_1_count > 0, "Selected seed model should be built with data"

    # Verify no other models were built by checking if their tables exist
    other_models = [
        "seed_model_2",
        "staging_model_1",
        "staging_model_2",
        "intermediate_model_1",
        "full_model",
    ]

    for model in other_models:
        # Using information_schema to check if table exists
        table_exists = sample_sqlmesh_test_context.query(
            f"""
            SELECT COUNT(*) 
            FROM information_schema.tables 
            WHERE table_schema = 'sqlmesh_example__dev' 
            AND table_name = '{model}'
            """
        )[0][0]
        assert table_exists == 0, (
            f"{model} should not be built when only seed_model_1 is selected"
        )


if __name__ == "__main__":
    pytest.main([__file__])
