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
from dagster_sqlmesh.types import SQLMeshMultiAssetOptions

logger = logging.getLogger(__name__)

def sqlmesh_to_multi_asset_options(
    *,
    environment: str,
    config: SQLMeshContextConfig,
    context_factory: ContextFactory[ContextCls] = lambda **kwargs: Context(**kwargs),
    dagster_sqlmesh_translator: SQLMeshDagsterTranslator | None = None,
) -> SQLMeshMultiAssetOptions:
    """Converts sqlmesh project into a SQLMeshMultiAssetOptions object which is
    an intermediate representation of the SQLMesh project that can be used to
    create a dagster multi_asset definition."""
    controller = DagsterSQLMeshController.setup_with_config(
        config=config, context_factory=context_factory
    )
    if not dagster_sqlmesh_translator:
        dagster_sqlmesh_translator = SQLMeshDagsterTranslator()

    conversion = controller.to_asset_outs(
        environment,
        translator=dagster_sqlmesh_translator,
    )
    return conversion

def sqlmesh_asset_from_multi_asset_options(
    *,
    sqlmesh_multi_asset_options: SQLMeshMultiAssetOptions,
    name: str | None = None,
    compute_kind: str = "sqlmesh",
    op_tags: t.Mapping[str, t.Any] | None = None,
    required_resource_keys: set[str] | None = None,
    retry_policy: RetryPolicy | None = None,
    enabled_subsetting: bool = False,
) -> t.Callable[[t.Callable[..., t.Any]], AssetsDefinition]:
    """Creates a dagster multi_asset definition from a SQLMeshMultiAssetOptions object."""
    kwargs: dict[str, t.Any] = {}
    if enabled_subsetting:
        kwargs["can_subset"] = True

    #asset_deps = sqlmesh_multi_asset_options.to_asset_deps()   
    #print("Asset deps boop:", asset_deps)  # Debugging line

    return multi_asset(
        outs=sqlmesh_multi_asset_options.to_asset_outs(),
        deps=sqlmesh_multi_asset_options.to_asset_deps(),
        internal_asset_deps=sqlmesh_multi_asset_options.to_internal_asset_deps(),
        name=name,
        compute_kind=compute_kind,
        op_tags=op_tags,
        required_resource_keys=required_resource_keys,
        retry_policy=retry_policy,
        **kwargs,
    )

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
    conversion = sqlmesh_to_multi_asset_options(
        environment=environment,
        config=config,
        context_factory=context_factory,
        dagster_sqlmesh_translator=dagster_sqlmesh_translator,
    )
    
    return sqlmesh_asset_from_multi_asset_options(
        sqlmesh_multi_asset_options=conversion,
        name=name,
        compute_kind=compute_kind,
        op_tags=op_tags,
        required_resource_keys=required_resource_keys,
        retry_policy=retry_policy,
        enabled_subsetting=enabled_subsetting,
    )
