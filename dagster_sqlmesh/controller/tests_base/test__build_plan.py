import pytest

from dagster_sqlmesh.conftest import SQLMeshTestContext
from dagster_sqlmesh.controller.base import PlanOptions
from dagster_sqlmesh.utils import snapshot_id_to_model_name


def test_given_basic_plan_when_building_then_returns_expected_plan_properties(
    sample_sqlmesh_test_context: SQLMeshTestContext,
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

    print(f"Selected models to backfill: {plan.selected_models_to_backfill}")
    print(f"Directly modified models: {plan.directly_modified}")

    # Extract just the model names from SnapshotId objects
    directly_modified_models = {
        snapshot_id_to_model_name(snapshot_id) for snapshot_id in plan.directly_modified
    }

    # Verify the models appear in directly modified (they should be the same models)
    assert directly_modified_models == fully_qualified_models


def test_given_all_plan_options_when_building_then_plan_reflects_all_settings(
    sample_sqlmesh_test_context: SQLMeshTestContext,
) -> None:
    """Test plan creation with all possible options set to verify complete configuration."""
    controller = sample_sqlmesh_test_context.create_controller(
        enable_debug_console=True
    )

    # Create a comprehensive set of plan options
    plan_options = PlanOptions(
        skip_backfill=True,
        forward_only=True,
        include_unmodified=True,
        select_models=[
            "sqlmesh_example.staging_model_1",
            "sqlmesh_example.staging_model_3",
        ],
        no_gaps=True,
        allow_destructive_models={
            "sqlmesh_example.staging_model_1",
            "sqlmesh_example.staging_model_3",
        },
        start="2024-01-01",
        end="2024-01-02",
        execution_time="2024-01-02",
        effective_from="2024-01-01",
        no_auto_categorization=True,
        enable_preview=True,
    )

    with controller.instance("dev") as mesh:
        builder = mesh._get_builder(
            context=mesh.context,
            environment="dev",
            plan_options=plan_options,
        )
        plan = mesh._build_plan(
            builder=builder,
        )

    # Basic plan properties
    assert plan.is_dev is True
    assert plan.skip_backfill is True
    assert plan.forward_only is True
    assert plan.include_unmodified is True
    assert plan.end_bounded is True
    assert plan.no_gaps is True

    # Environment and timing properties
    assert plan.provided_start == "2024-01-01"
    assert plan.provided_end == "2024-01-02"
    assert plan.execution_time == "2024-01-02"
    assert plan.effective_from == "2024-01-01"

    # Model selection and backfill properties
    selected_models = plan.selected_models_to_backfill
    assert selected_models is not None
    assert len(selected_models) == 2
    assert '"db"."sqlmesh_example"."staging_model_1"' in selected_models
    assert '"db"."sqlmesh_example"."staging_model_3"' in selected_models

    # Destructive models
    assert plan.allow_destructive_models == {
        '"db"."sqlmesh_example"."staging_model_1"',
        '"db"."sqlmesh_example"."staging_model_3"',
    }

    # Verify computed properties have expected values
    assert plan.start == "2024-01-01"  # Should match provided_start
    assert plan.end == "2024-01-02"  # Should match provided_end
    assert not plan.requires_backfill  # Should be False since skip_backfill is True
    assert plan.has_changes  # Should be True since we selected models
    assert (
        not plan.has_unmodified_unpromoted
    )  # Should be False since we're not including unmodified
    assert len(plan.categorized) > 0  # Should have categorized models
    assert len(plan.uncategorized) == 0  # Should have no uncategorized models
    assert not plan.metadata_updated  # Should have no metadata updates
    assert len(plan.new_snapshots) > 0  # Should have new snapshots
    assert (
        len(plan.missing_intervals) > 0
    )  # Should have missing intervals even with skip_backfill


if __name__ == "__main__":
    pytest.main([__file__])
