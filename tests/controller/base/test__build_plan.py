import logging

import pytest

from dagster_sqlmesh.controller.base import PlanOptions
from tests.conftest import SQLMeshTestContext


@pytest.fixture
def logger() -> logging.Logger:
    logger = logging.getLogger(__name__)
    logger.setLevel(logging.DEBUG)
    return logger


def test_given_basic_plan_when_building_then_returns_expected_plan_properties(
    sample_sqlmesh_test_context: SQLMeshTestContext,
    logger: logging.Logger,
) -> None:
    """Test basic plan creation with default options."""
    controller = sample_sqlmesh_test_context.create_controller(
        enable_debug_console=True
    )

    with controller.instance("dev") as mesh:
        builder = mesh._get_builder(
            context=mesh.context,
            environment="dev",
            plan_options=PlanOptions(),
        )
        plan = mesh._build_plan(
            builder=builder,
        )

    assert plan is not None
    # Verify plan properties
    assert plan.is_dev is True
    assert not plan.skip_backfill
    assert not plan.forward_only
    assert not plan.include_unmodified
    assert plan.end_bounded


def test_given_skip_backfill_when_building_then_plan_reflects_backfill_status(
    sample_sqlmesh_test_context: SQLMeshTestContext,
    logger: logging.Logger,
) -> None:
    """Test plan creation with skip_backfill option."""
    controller = sample_sqlmesh_test_context.create_controller(
        enable_debug_console=True
    )

    with controller.instance("dev") as mesh:
        builder = mesh._get_builder(
            context=mesh.context,
            environment="dev",
            plan_options=PlanOptions(skip_backfill=True),
        )
        plan = mesh._build_plan(
            builder=builder,
        )

    assert plan is not None
    assert plan.skip_backfill is True
    # When skip_backfill is True, no backfill should be executed even if models need it
    assert not plan.empty_backfill  # Models do need backfill
    assert plan.skip_backfill  # But backfill is skipped


def test_given_selected_models_when_building_then_plan_shows_correct_model_selection(
    sample_sqlmesh_test_context: SQLMeshTestContext,
    logger: logging.Logger,
) -> None:
    """Test plan creation with specific model selection."""
    controller = sample_sqlmesh_test_context.create_controller(
        enable_debug_console=True
    )
    selected_models = {
        "sqlmesh_example.staging_model_1",
        "sqlmesh_example.staging_model_3",
    }

    with controller.instance("dev") as mesh:
        builder = mesh._get_builder(
            context=mesh.context,
            environment="dev",
            plan_options=PlanOptions(select_models=list(selected_models)),
        )
        plan = mesh._build_plan(
            builder=builder,
        )

    assert plan is not None
    assert plan.selected_models_to_backfill is not None

    # Convert selected models to the fully qualified format
    fully_qualified_models = {
        f'"db"."{schema}"."{name}"'
        for model in selected_models
        for schema, name in [model.split(".")]
    }

    # Verify selected models match what's in the plan
    assert plan.selected_models_to_backfill == fully_qualified_models

    # Verify the number of directly modified models matches
    assert len(plan.directly_modified) == len(selected_models)

    # Log the model formats to help debug any mismatches
    logger.debug(f"Selected models to backfill: {plan.selected_models_to_backfill}")
    logger.debug(f"Directly modified models: {plan.directly_modified}")

    # Extract just the model names from SnapshotId objects
    directly_modified_models = {
        str(snapshot_id).split("<")[1].split(":")[0]  # Extract between < and :
        for snapshot_id in plan.directly_modified
    }

    # Verify the models appear in directly modified (they should be the same models)
    assert directly_modified_models == fully_qualified_models


def test_given_exception_when_building_then_returns_none(
    sample_sqlmesh_test_context: SQLMeshTestContext,
    logger: logging.Logger,
) -> None:
    """Test error handling in plan creation."""
    controller = sample_sqlmesh_test_context.create_controller(
        enable_debug_console=True
    )

    with controller.instance("dev") as mesh:
        # Force an error by providing an invalid model
        builder = mesh._get_builder(
            context=mesh.context,
            environment="dev",
            plan_options=PlanOptions(select_models=["non_existent_model"]),
        )
        plan = mesh._build_plan(
            builder=builder,
        )

    assert plan is None


if __name__ == "__main__":
    pytest.main([__file__])
