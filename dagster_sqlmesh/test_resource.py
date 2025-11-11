import typing as t
from dataclasses import dataclass

import dagster as dg
import pytest

from dagster_sqlmesh.resource import (
    DagsterSQLMeshEventHandler,
    ModelMaterializationStatus,
    PlanOrRunFailedError,
)
from dagster_sqlmesh.testing import setup_testing_sqlmesh_test_context
from dagster_sqlmesh.testing.context import SQLMeshTestContext, TestSQLMeshResource


@dataclass(kw_only=True)
class SQLMeshResourceInitialization:
    dagster_instance: dg.DagsterInstance
    dagster_context: dg.AssetExecutionContext
    test_context: SQLMeshTestContext
    resource: TestSQLMeshResource


def setup_sqlmesh_resource(
    sample_sqlmesh_project: str, sample_sqlmesh_db_path: str, enable_model_failure: bool
):
    dg_instance = dg.DagsterInstance.local_temp()
    dg_context = dg.build_asset_context(
        instance=dg_instance,
    )
    test_context = setup_testing_sqlmesh_test_context(
        db_path=sample_sqlmesh_db_path,
        project_path=sample_sqlmesh_project,
        variables={"enable_model_failure": enable_model_failure},
    )
    test_context.initialize_test_source()
    resource = test_context.create_resource()
    return SQLMeshResourceInitialization(
        dagster_context=dg_context,
        test_context=test_context,
        resource=resource,
        dagster_instance=dg_instance,
    )


@pytest.fixture
def sample_sqlmesh_resource_initialization(
    sample_sqlmesh_project: str, sample_sqlmesh_db_path: str
):
    return setup_sqlmesh_resource(
        sample_sqlmesh_project=sample_sqlmesh_project,
        sample_sqlmesh_db_path=sample_sqlmesh_db_path,
        enable_model_failure=False,
    )


def test_sqlmesh_resource_should_report_no_errors(
    sample_sqlmesh_resource_initialization: SQLMeshResourceInitialization,
):
    resource = sample_sqlmesh_resource_initialization.resource
    dg_context = sample_sqlmesh_resource_initialization.dagster_context
    context_config = sample_sqlmesh_resource_initialization.test_context.context_config

    success = True
    try:
        for result in resource.run(context=dg_context, config=context_config):
            pass
    except PlanOrRunFailedError as e:
        success = False
        print(f"Plan or run failed with errors: {e.errors}")
    except Exception as e:
        success = False
        print(f"An unexpected error occurred: {e}")
    assert success, "Expected no errors, but an error was raised during the run."


def test_sqlmesh_resource_properly_reports_errors(
    sample_sqlmesh_project: str, sample_sqlmesh_db_path: str
):
    sqlmesh_resource_initialization = setup_sqlmesh_resource(
        sample_sqlmesh_project=sample_sqlmesh_project,
        sample_sqlmesh_db_path=sample_sqlmesh_db_path,
        enable_model_failure=True,
    )
    resource = sqlmesh_resource_initialization.resource
    dg_context = sqlmesh_resource_initialization.dagster_context
    context_config = sqlmesh_resource_initialization.test_context.context_config

    caught_failure = False
    try:
        for result in resource.run(context=dg_context, config=context_config):
            pass
    except PlanOrRunFailedError as e:
        caught_failure = True

        expected_error_found = False
        for err in e.errors:
            if "staging_model_5" in str(err):
                expected_error_found = True
                break
        assert expected_error_found, "Expected error not found in the error list."

    assert caught_failure, "Expected an error to be raised, but it was not."


def test_sqlmesh_resource_properly_reports_errors_not_thrown(
    sample_sqlmesh_resource_initialization: SQLMeshResourceInitialization,
):
    dg_context = sample_sqlmesh_resource_initialization.dagster_context
    resource = sample_sqlmesh_resource_initialization.resource
    context_config = sample_sqlmesh_resource_initialization.test_context.context_config

    def event_handler_factory(
        *args: t.Any, **kwargs: t.Any
    ) -> DagsterSQLMeshEventHandler:
        """Custom event handler factory for the SQLMesh resource."""
        handler = DagsterSQLMeshEventHandler(*args, **kwargs)
        # Load it with an error
        handler._errors = [Exception("testerror")]
        return handler

    resource.set_event_handler_factory(event_handler_factory)

    caught_failure = False
    try:
        for result in resource.run(context=dg_context, config=context_config):
            pass
    except PlanOrRunFailedError as e:
        caught_failure = True

        expected_error_found = False
        for err in e.errors:
            print(f"Found error: {err}")
            if "testerror" in str(err):
                expected_error_found = True
                break
        assert (
            expected_error_found
        ), "Expected error 'testerror' not found in the error list."

    assert caught_failure, "Expected an error to be raised, but it was not."


def test_sqlmesh_resource_should_properly_materialize_results_when_no_plan_is_run(
    sample_sqlmesh_resource_initialization: SQLMeshResourceInitialization,
):
    """We had an issue with sqlmesh not properly materializing sqlmesh models if
    the plan ended up not having any changes and all models were already up to
    date.

    This test is to ensure that doesn't regress.
    """

    resource = sample_sqlmesh_resource_initialization.resource
    dg_context = sample_sqlmesh_resource_initialization.dagster_context
    dg_instance = sample_sqlmesh_resource_initialization.dagster_instance
    context_config = sample_sqlmesh_resource_initialization.test_context.context_config

    # First run should materialize all models
    initial_results: list[dg.MaterializeResult] = []
    for result in resource.run(context=dg_context, config=context_config):
        initial_results.append(result)
        assert result.asset_key is not None, "Expected asset key to be present."
        dg_instance.report_runless_asset_event(
            dg.AssetMaterialization(
                asset_key=result.asset_key,
                metadata=result.metadata,
            )
        )

    # All metadata times should be set to the same time
    initial_times: set[float] = set()
    for result in initial_results:
        assert result.metadata is not None, "Expected metadata to be present."
        status = ModelMaterializationStatus.from_dagster_metadata(dict(result.metadata))
        initial_times.add(status.created_at.timestamp())
        initial_times.add(status.last_backfill.timestamp())
        initial_times.add(status.last_updated_or_restated.timestamp())
        initial_times.add(status.last_promoted.timestamp())
    assert len(initial_times) == 1, "Expected all metadata times to be the same."

    assert len(initial_results) > 0, "Expected initial results to be non-empty."

    original_created_at = next(iter(initial_times))

    # Second run should also materialize all models
    second_results: list[dg.MaterializeResult] = []
    for result in resource.run(context=dg_context, config=context_config):
        second_results.append(result)

    assert len(second_results) > 0, "Expected second results to be non-empty."
    assert len(initial_results) == len(
        second_results
    ), "Expected initial and second results to have the same number of materialized assets"

    # Assert that all models were not updated
    second_times: set[float] = set()
    for result in second_results:
        assert result.metadata is not None, "Expected metadata to be present."
        status = ModelMaterializationStatus.from_dagster_metadata(dict(result.metadata))
        second_times.add(status.created_at.timestamp())
        second_times.add(status.last_backfill.timestamp())
        second_times.add(status.last_updated_or_restated.timestamp())
        second_times.add(status.last_promoted.timestamp())
    assert (
        len(second_times) == 1
    ), "Expected all metadata times to be the same in the second run."

    # Third run will restate the full model
    third_results: list[dg.MaterializeResult] = []
    for result in resource.run(
        context=dg_context,
        config=context_config,
        restate_models=["sqlmesh_example.full_model"],
    ):
        third_results.append(result)

    assert len(third_results) > 0, "Expected third results to be non-empty."
    assert len(third_results) == len(
        initial_results
    ), "Expected third results to have the same number of materialized assets as initial results"

    # The full model's metadata should indicate it was updated all others should
    # be promoted
    for result in third_results:
        assert result.metadata is not None, "Expected metadata to be present."
        status = ModelMaterializationStatus.from_dagster_metadata(dict(result.metadata))
        assert (
            status.created_at.timestamp() == original_created_at
        ), "Expected created_at to be unchanged for all models"

        # Oddly, sqlmesh promotes everything during a restatement
        assert (
            status.last_promoted.timestamp() > status.created_at.timestamp()
        ), "Expected last_promoted to be updated for all models"

        if status.is_match("sqlmesh_example.full_model", ignore_catalog=True):
            assert (
                status.last_updated_or_restated.timestamp()
                > status.created_at.timestamp()
            ), "Expected full model to be updated."
            assert (
                status.last_backfill.timestamp() > status.created_at.timestamp()
            ), "Expected only full model to be backfilled"
        else:
            assert (
                status.last_updated_or_restated.timestamp()
                == status.created_at.timestamp()
            ), f"{status.model_fqn} updated. Expected only full model to be updated"
            assert (
                status.last_backfill.timestamp() == status.created_at.timestamp()
            ), f"{status.model_fqn} run. Expected only full model to be run"
