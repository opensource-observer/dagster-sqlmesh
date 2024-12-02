from dataclasses import dataclass, field
import typing as t
import logging
import threading
from contextlib import contextmanager

import sqlglot
from sqlglot import exp
from sqlmesh.utils.date import TimeLike
from sqlmesh.core.context import Context
from sqlmesh.core.plan import PlanBuilder
from sqlmesh.core.config import CategorizerConfig
from sqlmesh.core.console import Console
from sqlmesh.core.model import Model

from dagster_sqlmesh.events import ConsoleGenerator

from .config import SQLMeshContextConfig

from .console import (
    EventConsole,
    ConsoleEventHandler,
    DebugEventConsole,
    SnapshotCategorizer,
)
from .utils import sqlmesh_model_name_to_key

logger = logging.getLogger(__name__)


class PlanOptions(t.TypedDict):
    start: t.NotRequired[TimeLike]
    end: t.NotRequired[TimeLike]
    execution_time: t.NotRequired[TimeLike]
    create_from: t.NotRequired[str]
    skip_tests: t.NotRequired[bool]
    restate_models: t.NotRequired[t.Iterable[str]]
    no_gaps: t.NotRequired[bool]
    skip_backfill: t.NotRequired[bool]
    forward_only: t.NotRequired[bool]
    allow_destructive_models: t.NotRequired[t.Collection[str]]
    no_auto_categorization: t.NotRequired[bool]
    effective_from: t.NotRequired[TimeLike]
    include_unmodified: t.NotRequired[bool]
    select_models: t.NotRequired[t.Collection[str]]
    backfill_models: t.NotRequired[t.Collection[str]]
    categorizer_config: t.NotRequired[CategorizerConfig]
    enable_preview: t.NotRequired[bool]
    run: t.NotRequired[bool]


class RunOptions(t.TypedDict):
    start: t.NotRequired[TimeLike]
    end: t.NotRequired[TimeLike]
    execution_time: t.NotRequired[TimeLike]
    skip_janitor: t.NotRequired[bool]
    ignore_cron: t.NotRequired[bool]
    select_models: t.NotRequired[t.Collection[str]]


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
    model: t.Optional[Model] = None

    def parse_fqn(self):
        return parse_fqn(self.fqn)


class SQLMeshController:
    config: SQLMeshContextConfig
    console: EventConsole
    logger: logging.Logger

    @classmethod
    def setup(
        cls,
        config: SQLMeshContextConfig,
        debug_console: t.Optional[Console] = None,
        log_override: t.Optional[logging.Logger] = None,
    ):
        console = EventConsole(log_override=log_override)
        if debug_console:
            console = DebugEventConsole(debug_console)
        controller = SQLMeshController(
            console=console,
            config=config,
        )
        return controller

    def __init__(self, config: SQLMeshContextConfig, console: EventConsole):
        self.config = config
        self.console = console
        self.logger = logger

    def set_logger(self, logger: logging.Logger):
        self.logger = logger

    def add_event_handler(self, handler: ConsoleEventHandler):
        return self.console.add_handler(handler)

    def remove_event_handler(self, handler_id: str):
        return self.console.remove_handler(handler_id)

    def models_dag(self):
        with self.context() as context:
            return context.dag

    def _create_context(self):
        options: t.Dict[str, t.Any] = dict(
            paths=self.config.path,
            gateway=self.config.gateway,
            console=self.console,
        )
        if self.config.sqlmesh_config:
            options["config"] = self.config.sqlmesh_config
        return Context(**options)

    @contextmanager
    def context(self):
        context = self._create_context()
        yield context
        context.close()

    def plan(
        self,
        environment: str,
        plan_options: t.Optional[PlanOptions],
        categorizer: t.Optional[SnapshotCategorizer] = None,
        default_catalog: t.Optional[str] = None,
    ):
        # Runs things in thread
        def run_sqlmesh_thread(
            logger: logging.Logger,
            context: Context,
            controller: SQLMeshController,
            environment: str,
            plan_options: PlanOptions,
            default_catalog: str,
        ):
            try:
                builder = t.cast(
                    PlanBuilder,
                    context.plan_builder(
                        environment=environment,
                        **plan_options,
                    ),
                )
                logger.debug("applying plan")
                controller.console.plan(
                    builder,
                    auto_apply=True,
                    default_catalog=default_catalog,
                )
            except Exception as e:
                logger.error(e)
                raise e

        generator = ConsoleGenerator(self.logger)
        event_id = self.add_event_handler(generator)

        with self.context() as context:
            thread = threading.Thread(
                target=run_sqlmesh_thread,
                args=(
                    self.logger,
                    context,
                    self,
                    environment,
                    plan_options,
                    default_catalog,
                ),
            )
            thread.start()

            for event in generator.events(thread):
                yield event

            thread.join()

            self.remove_event_handler(event_id)

    def run(self, environment: str, run_options: RunOptions):
        # Runs things in thread
        def run_sqlmesh_thread(
            logger: logging.Logger,
            context: Context,
            environment: str,
            run_options: RunOptions,
        ):
            logger.debug("running the plan")
            try:
                context.run(environment=environment, **run_options)
            except Exception as e:
                logger.error(e)
                raise e

        generator = ConsoleGenerator(self.logger)
        event_id = self.add_event_handler(generator)

        with self.context() as context:
            thread = threading.Thread(
                target=run_sqlmesh_thread,
                args=(
                    self.logger,
                    context,
                    environment,
                    run_options,
                ),
            )
            thread.start()

            for event in generator.events(thread):
                yield event

            thread.join()
            self.remove_event_handler(event_id)


# @dataclass
# class SQLMeshController:
#     """Allows controlling sqlmesh as a library"""

#     console: EventConsole
#     context: Context
#     config: SQLMeshContextConfig

#     def add_event_handler(self, handler: ConsoleEventHandler):
#         return self.console.add_handler(handler)

#     def remove_event_handler(self, handler_id: str):
#         return self.console.remove_handler(handler_id)

#     # def to_asset_outs(
#     #     self, translator: SQLMeshDagsterTranslator
#     # ) -> SQLMeshMultiAssetOptions:
#     #     context = self.context
#     #     dag = context.dag
#     #     output = SQLMeshMultiAssetOptions()
#     #     depsMap: Dict[str, CoercibleToAssetDep] = {}

#     #     for model_fqn, deps in dag.graph.items():
#     #         logger.debug(f"model found: {model_fqn}")
#     #         model = context.get_model(model_fqn)
#     #         if not model:
#     #             # If no model is returned this seems to be an asset dependency
#     #             continue
#     #         asset_out = translator.get_asset_key_from_model(
#     #             context,
#     #             model,
#     #         )
#     #         model_deps = [
#     #             SQLMeshModelDep(fqn=dep, model=context.get_model(dep)) for dep in deps
#     #         ]
#     #         internal_asset_deps: Set[AssetKey] = set()
#     #         for dep in model_deps:
#     #             if dep.model:
#     #                 internal_asset_deps.add(
#     #                     translator.get_asset_key_from_model(context, dep.model)
#     #                 )
#     #             else:
#     #                 table = translator.get_fqn_to_table(context, dep.fqn)
#     #                 key = translator.get_asset_key_fqn(context, dep.fqn)
#     #                 internal_asset_deps.add(key)
#     #                 # create an external dep
#     #                 depsMap[table.name] = AssetDep(key)
#     #         model_key = sqlmesh_model_name_to_key(model.name)
#     #         output.outs[model_key] = AssetOut(key=asset_out, is_required=False)
#     #         output.internal_asset_deps[model_key] = internal_asset_deps

#     #     output.deps = list(depsMap.values())
#     #     return output

#     def models_graph(self):
#         """Returns a graph of the models"""
#         pass

#     def reload_context(self, path: str):
#         """Reload the context"""
#         pass

#     def plan_and_run(self, select_models: t.Optional[t.Set[str]] = None):
#         pass


# def setup_sqlmesh_controller(
#     config: SQLMeshContextConfig,
#     debug_console: t.Optional[Console] = None,
#     log_override: t.Optional[logging.Logger] = None,
# ):
#     console = EventConsole(log_override=log_override)
#     if debug_console:
#         console = DebugEventConsole(debug_console)
#     options: t.Dict[str, t.Any] = dict(
#         paths=config.path,
#         gateway=config.gateway,
#         console=console,
#     )
#     if config.sqlmesh_config:
#         options["config"] = config.sqlmesh_config
#     context = Context(**options)
#     return SQLMeshController(
#         console=console,
#         context=context,
#         config=config,
#     )
