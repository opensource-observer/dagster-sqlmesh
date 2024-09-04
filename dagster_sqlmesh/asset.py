from dataclasses import dataclass, field
from typing import (
    Union,
    Iterable,
    Dict,
    Mapping,
    Set,
    Any,
    Optional,
)
import logging

from sqlmesh.core.context import Context
from sqlmesh.core.console import Console
from sqlmesh.core.model import Model
from dagster import (
    AssetDep,
    multi_asset,
    AssetCheckResult,
    AssetMaterialization,
    AssetOut,
    AssetKey,
    RetryPolicy,
)
from dagster._core.definitions.asset_dep import CoercibleToAssetDep

from .config import SQLMeshContextConfig
from .console import EventConsole, ConsoleEventHandler, DebugEventConsole
from .utils import sqlmesh_model_name_to_key
from .context import DagsterSQLMeshContext

logger = logging.getLogger(__name__)


MultiAssetResponse = Iterable[Union[AssetCheckResult, AssetMaterialization]]


@dataclass(kw_only=True)
class SQLMeshParsedFQN:
    catalog: str
    schema: str
    view_name: str


def parse_fqn(fqn: str):
    split_fqn = fqn.split(".")

    # Remove any quotes
    split_fqn = list(map(lambda a: a.strip("'\""), split_fqn))
    return SQLMeshParsedFQN(
        catalog=split_fqn[0], schema=split_fqn[1], view_name=split_fqn[2]
    )


@dataclass(kw_only=True)
class SQLMeshModelDep:
    fqn: str
    model: Optional[Model] = None

    def parse_fqn(self):
        return parse_fqn(self.fqn)


@dataclass(kw_only=True)
class SQLMeshMultiAssetOptions:
    outs: Dict[str, AssetOut] = field(default_factory=lambda: {})
    deps: Iterable[CoercibleToAssetDep] = field(default_factory=lambda: {})
    internal_asset_deps: Dict[str, Set[AssetKey]] = field(default_factory=lambda: {})


class SQLMeshDagsterTranslator:
    def get_asset_key_from_model(self, context: Context, model: Model) -> AssetKey:
        return AssetKey(model.view_name)

    def get_asset_key_fqn(self, context: Context, fqn: str) -> AssetKey:
        parsed_fqn = parse_fqn(fqn)
        return AssetKey(parsed_fqn.view_name)

    # def get_asset_deps(
    #     self, context: Context, model: Model, deps: List[SQLMeshModelDep]
    # ) -> List[AssetKey]:
    #     asset_keys: List[AssetKey] = []
    #     for dep in deps:
    #         if dep.model:
    #             asset_keys.append(AssetKey(dep.model.view_name))
    #         else:
    #             parsed_fqn = dep.parse_fqn()
    #             asset_keys.append(AssetKey([parsed_fqn.view_name]))
    #     return asset_keys


# Define a SQLMesh Asset
def sqlmesh_assets(
    *,
    config: SQLMeshContextConfig,
    name: Optional[str] = None,
    dagster_sqlmesh_translator: Optional[SQLMeshDagsterTranslator] = None,
    compute_kind: str = "sqlmesh",
    op_tags: Optional[Mapping[str, Any]] = None,
    required_resource_keys: Optional[Set[str]] = None,
    retry_policy: Optional[RetryPolicy] = None,
):
    controller = setup_sqlmesh_controller(config)
    if not dagster_sqlmesh_translator:
        dagster_sqlmesh_translator = SQLMeshDagsterTranslator()
    conversion = controller.to_asset_outs(dagster_sqlmesh_translator)

    return multi_asset(
        name=name,
        outs=conversion.outs,
        deps=conversion.deps,
        internal_asset_deps=conversion.internal_asset_deps,
        op_tags=op_tags,
        compute_kind=compute_kind,
        retry_policy=retry_policy,
        required_resource_keys=required_resource_keys,
        # can_subset=True,
    )


@dataclass
class SQLMeshController:
    console: EventConsole
    context: DagsterSQLMeshContext

    def add_event_handler(self, handler: ConsoleEventHandler):
        return self.console.add_handler(handler)

    def remove_event_handler(self, handler_id: str):
        return self.console.remove_handler(handler_id)

    def to_asset_outs(
        self, translator: SQLMeshDagsterTranslator
    ) -> SQLMeshMultiAssetOptions:
        context = self.context
        dag = context.dag
        output = SQLMeshMultiAssetOptions()
        depsMap: Dict[str, CoercibleToAssetDep] = {}
        for model_fqn, deps in dag.graph.items():
            logger.debug(f"model found: {model_fqn}")
            model = context.get_model(model_fqn)
            if not model:
                # If no model is returned this seems to be an asset dependency
                continue
            asset_out = translator.get_asset_key_from_model(
                context,
                model,
            )
            model_deps = [
                SQLMeshModelDep(fqn=dep, model=context.get_model(dep)) for dep in deps
            ]
            internal_asset_deps: Set[AssetKey] = set()
            for dep in model_deps:
                if dep.model:
                    internal_asset_deps.add(
                        translator.get_asset_key_from_model(context, dep.model)
                    )
                else:
                    key = translator.get_asset_key_fqn(context, dep.fqn)
                    internal_asset_deps.add(key)
                    # create an external dep
                    depsMap[dep.parse_fqn().view_name] = AssetDep(key)
            model_key = sqlmesh_model_name_to_key(model.name)
            output.outs[model_key] = AssetOut(key=asset_out, is_required=False)
            output.internal_asset_deps[model_key] = internal_asset_deps

        output.deps = list(depsMap.values())
        return output


def setup_sqlmesh_controller(
    config: SQLMeshContextConfig,
    debug_console: Optional[Console] = None,
    log_override: Optional[logging.Logger] = None,
):
    console = EventConsole(log_override=log_override)
    if debug_console:
        console = DebugEventConsole(debug_console)
    options: Dict[str, Any] = dict(
        paths=config.path,
        gateway=config.gateway,
        console=console,
    )
    if config.sqlmesh_config:
        options["config"] = config.sqlmesh_config
    context = DagsterSQLMeshContext(**options)
    return SQLMeshController(
        console=console,
        context=context,
    )
