import typing as t

import dagster as dg

from dagster_sqlmesh.resource import DagsterSQLMeshEventHandler, PlanOrRunFailedError
from dagster_sqlmesh.testing import setup_testing_sqlmesh_test_context


def test_sqlmesh_resource_should_report_no_errors(
    sample_sqlmesh_project: str, sample_sqlmesh_db_path: str
):
    dg_context = dg.build_asset_context()
    test_context = setup_testing_sqlmesh_test_context(
        db_path=sample_sqlmesh_db_path,
        project_path=sample_sqlmesh_project,
        variables={"enable_model_failure": False}
    )
    test_context.initialize_test_source()
    resource = test_context.create_resource() 

    success = True
    try:
        for result in resource.run(dg_context):
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
    dg_context = dg.build_asset_context()
    test_context = setup_testing_sqlmesh_test_context(
        db_path=sample_sqlmesh_db_path,
        project_path=sample_sqlmesh_project,
        variables={"enable_model_failure": True}
    )
    test_context.initialize_test_source()
    resource = test_context.create_resource()

    caught_failure = False
    try:
        for result in resource.run(dg_context):
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
    sample_sqlmesh_project: str, sample_sqlmesh_db_path: str
):
    dg_context = dg.build_asset_context()
    test_context = setup_testing_sqlmesh_test_context(
        db_path=sample_sqlmesh_db_path,
        project_path=sample_sqlmesh_project,
        variables={"enable_model_failure": False}
    )
    test_context.initialize_test_source()
    resource = test_context.create_resource()
    def event_handler_factory(*args: t.Any, **kwargs: t.Any) -> DagsterSQLMeshEventHandler:
        """Custom event handler factory for the SQLMesh resource."""
        handler = DagsterSQLMeshEventHandler(*args, **kwargs)
        # Load it with an error
        handler._errors = [Exception("testerror")]
        return handler
    resource.set_event_handler_factory(event_handler_factory)

    caught_failure = False
    try:
        for result in resource.run(dg_context):
            pass
    except PlanOrRunFailedError as e:
        caught_failure = True

        expected_error_found = False
        for err in e.errors:
            print(f"Found error: {err}")
            if "testerror" in str(err):
                expected_error_found = True
                break
        assert expected_error_found, "Expected error 'testerror' not found in the error list."

    assert caught_failure, "Expected an error to be raised, but it was not."

