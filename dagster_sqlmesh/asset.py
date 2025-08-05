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

class MultiAssetFactory(t.Protocol):
    def __call__(self, 
        multi_asset_options: SQLMeshMultiAssetOptions,
        compute_kind: str,
        name: str | None = None,
        op_tags: t.Mapping[str, t.Any] | None = None,
        required_resource_keys: set[str] | None = None,
        retry_policy: RetryPolicy | None = None,
        enabled_subsetting: bool = False,
    ) -> t.Callable[[t.Callable[..., t.Any]], AssetsDefinition]:
        """Factory function to create a multi asset definition from SQLMeshMultiAssetOptions."""
        ...

def _multi_asset_factory(
    multi_asset_options: SQLMeshMultiAssetOptions,
    compute_kind: str,
    name: str | None = None,
    op_tags: t.Mapping[str, t.Any] | None = None,
    required_resource_keys: set[str] | None = None,
    retry_policy: RetryPolicy | None = None,
    enabled_subsetting: bool = False,
) -> t.Callable[[t.Callable[..., t.Any]], AssetsDefinition]:
    """Factory function to create a multi asset definition from SQLMeshMultiAssetOptions."""
    return multi_asset(
        name=name,
        outs=multi_asset_options.to_asset_outs(),
        deps=multi_asset_options.to_asset_deps(),
        internal_asset_deps=multi_asset_options.to_internal_asset_deps(),
        op_tags=op_tags,
        compute_kind=compute_kind,
        retry_policy=retry_policy,
        can_subset=enabled_subsetting,
        required_resource_keys=required_resource_keys,
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
    multi_asset_factory: MultiAssetFactory = _multi_asset_factory,
) -> t.Callable[[t.Callable[..., t.Any]], AssetsDefinition]:
    controller = DagsterSQLMeshController.setup_with_config(
        config=config, context_factory=context_factory
    )
    if not dagster_sqlmesh_translator:
        dagster_sqlmesh_translator = SQLMeshDagsterTranslator()

    conversion = controller.to_asset_outs(
        environment,
        translator=dagster_sqlmesh_translator,
    )
    
    return multi_asset_factory(
        multi_asset_options=conversion,
        name=name,
        compute_kind=compute_kind,
        op_tags=op_tags,
        required_resource_keys=required_resource_keys,
        retry_policy=retry_policy,
        enabled_subsetting=enabled_subsetting,
    )
