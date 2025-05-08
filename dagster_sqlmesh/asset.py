import logging
import typing as t

from dagster import AssetsDefinition, RetryPolicy, multi_asset
from sqlmesh import Context

from dagster_sqlmesh.config import SQLMeshContextConfig
from dagster_sqlmesh.controller import (
    ContextCls,
    ContextFactory,
    DagsterSQLMeshController,
)
from dagster_sqlmesh.translator import SQLMeshDagsterTranslator

logger = logging.getLogger(__name__)


# Define a SQLMesh Asset
def sqlmesh_assets(
    *,
    environment: str,
    config: SQLMeshContextConfig,
    context_factory: ContextFactory[ContextCls] = lambda **kwargs: Context(**kwargs),
    name: str | None = None,
    dagster_sqlmesh_translator: SQLMeshDagsterTranslator | None = None,
    compute_kind: str = "sqlmesh",
    op_tags: t.Mapping[str, t.Any] | None = None,
    required_resource_keys: set[str] | None = None,
    retry_policy: RetryPolicy | None = None,
    # For now we don't set this by default
    enabled_subsetting: bool = False,
) -> t.Callable[[t.Callable[..., t.Any]], AssetsDefinition]:
    controller = DagsterSQLMeshController.setup_with_config(config=config, context_factory=context_factory)
    if not dagster_sqlmesh_translator:
        dagster_sqlmesh_translator = SQLMeshDagsterTranslator()
    conversion = controller.to_asset_outs(environment, translator=dagster_sqlmesh_translator)

    return multi_asset(
        name=name,
        outs=conversion.outs,
        deps=conversion.deps,
        internal_asset_deps=conversion.internal_asset_deps,
        op_tags=op_tags,
        compute_kind=compute_kind,
        retry_policy=retry_policy,
        can_subset=enabled_subsetting,
        required_resource_keys=required_resource_keys,
    )
