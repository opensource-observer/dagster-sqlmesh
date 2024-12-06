import typing as t
import logging

from dagster import (
    multi_asset,
    RetryPolicy,
)

from dagster_sqlmesh.controller import DagsterSQLMeshController
from dagster_sqlmesh.translator import SQLMeshDagsterTranslator

from .config import SQLMeshContextConfig

logger = logging.getLogger(__name__)


# Define a SQLMesh Asset
def sqlmesh_assets(
    *,
    environment: str,
    config: SQLMeshContextConfig,
    name: t.Optional[str] = None,
    dagster_sqlmesh_translator: t.Optional[SQLMeshDagsterTranslator] = None,
    compute_kind: str = "sqlmesh",
    op_tags: t.Optional[t.Mapping[str, t.Any]] = None,
    required_resource_keys: t.Optional[t.Set[str]] = None,
    retry_policy: t.Optional[RetryPolicy] = None,
):
    controller = DagsterSQLMeshController.setup_with_config(config)
    if not dagster_sqlmesh_translator:
        dagster_sqlmesh_translator = SQLMeshDagsterTranslator()
    conversion = controller.to_asset_outs(environment, dagster_sqlmesh_translator)

    return multi_asset(
        name=name,
        outs=conversion.outs,
        deps=conversion.deps,
        internal_asset_deps=conversion.internal_asset_deps,
        op_tags=op_tags,
        compute_kind=compute_kind,
        retry_policy=retry_policy,
        required_resource_keys=required_resource_keys,
    )
