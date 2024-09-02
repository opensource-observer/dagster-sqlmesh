from dataclasses import dataclass, field
import typing as t
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
from sqlmesh.utils.date import TimeLike
from sqlmesh.core.snapshot import Snapshot
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
from sqlmesh.utils.errors import (
    ConfigError,
)
from dagster._core.definitions.asset_dep import CoercibleToAssetDep
from sqlmesh.core.scheduler import Scheduler

from dagster_sqlmesh.scheduler import DagsterSQLMeshScheduler
from .config import SQLMeshContextConfig
from .console import ConsoleEvent, EventConsole, ConsoleEventHandler, DebugEventConsole
from .utils import sqlmesh_model_name_to_key

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
        can_subset=True,
    )


class DagsterSQLMeshContext(Context):
    """Custom sqlmesh context so that we can inject selected models to the
    scheduler when running sqlmesh."""

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._selected_models: t.Set[str] = set()

    def set_selected_models(self, models: t.Set[str]):
        self._selected_models = models

    def scheduler(self, environment: str | None = None) -> Scheduler:
        """Return a custom scheduler. This uses copy pasted code from the
        original context. We will make a PR upstream to make this a little more
        reliable"""

        snapshots: t.Iterable[Snapshot]
        if environment is not None:
            stored_environment = self.state_sync.get_environment(environment)
            if stored_environment is None:
                raise ConfigError(f"Environment '{environment}' was not found.")
            snapshots = self.state_sync.get_snapshots(
                stored_environment.snapshots
            ).values()
        else:
            snapshots = self.snapshots.values()

        if not snapshots:
            raise ConfigError("No models were found")

        selected_snapshots: t.Set[str] = set()
        if len(self._selected_models) > 0:
            matched_snapshots = filter(
                lambda a: a.model.name in self._selected_models, snapshots
            )
            selected_snapshots = set(map(lambda a: a.name, matched_snapshots))

        return DagsterSQLMeshScheduler(
            selected_snapshots,
            snapshots,
            self.snapshot_evaluator,
            self.state_sync,
            default_catalog=self.default_catalog,
            max_workers=self.concurrent_tasks,
            console=self.console,
            notification_target_manager=self.notification_target_manager,
        )

    def run_with_selected_models(
        self,
        environment: t.Optional[str] = None,
        start: t.Optional[TimeLike] = None,
        end: t.Optional[TimeLike] = None,
        execution_time: t.Optional[TimeLike] = None,
        skip_janitor: bool = False,
        ignore_cron: bool = False,
        select_models: t.Optional[t.Collection[str]] = None,
    ):
        if select_models:
            self.set_selected_models(set(select_models))

        return self.run(
            environment=environment,
            start=start,
            end=end,
            execution_time=execution_time,
            skip_janitor=skip_janitor,
            ignore_cron=ignore_cron,
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


def debug_events(ev: ConsoleEvent):
    print(ev)


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
