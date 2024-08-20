from dagster_sqlmesh.asset import (
    sqlmesh_context_to_asset_outs,
    SQLMeshDagsterTranslator,
)
from dagster_sqlmesh.conftest import SQLMeshTestContext


def test_sqlmesh_context_to_asset_outs(sample_sqlmesh_test_context: SQLMeshTestContext):
    controller = sample_sqlmesh_test_context.create_controller()
    translator = SQLMeshDagsterTranslator()
    outs = sqlmesh_context_to_asset_outs(controller.context, translator)
    assert len(list(outs.deps)) == 1
    assert len(outs.outs) == 7


def test_sqlmesh_dagster_asset():
    pass
