from dagster_sqlmesh.asset import (
    SQLMeshDagsterTranslator,
)
from dagster_sqlmesh.conftest import SQLMeshTestContext


def test_sqlmesh_context_to_asset_outs(sample_sqlmesh_test_context: SQLMeshTestContext):
    controller = sample_sqlmesh_test_context.create_controller()
    translator = SQLMeshDagsterTranslator()
    outs = controller.to_asset_outs("dev", translator)
    assert len(list(outs.deps)) == 1
    assert len(outs.outs) == 7
