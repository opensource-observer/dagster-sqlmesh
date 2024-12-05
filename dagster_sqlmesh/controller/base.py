from dataclasses import dataclass
import typing as t
import logging
import threading
from contextlib import contextmanager

from sqlmesh.utils.date import TimeLike
from sqlmesh.core.context import Context
from sqlmesh.core.plan import PlanBuilder
from sqlmesh.core.config import CategorizerConfig
from sqlmesh.core.console import Console
from sqlmesh.core.model import Model

from ..events import ConsoleGenerator
from ..config import SQLMeshContextConfig
from ..console import (
    ConsoleException,
    EventConsole,
    ConsoleEventHandler,
    DebugEventConsole,
    SnapshotCategorizer,
)

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


class SQLMeshInstance:
    config: SQLMeshContextConfig
    console: EventConsole
    logger: logging.Logger
    context: Context
    environment: str

    def __init__(
        self,
        environment: str,
        console: EventConsole,
        config: SQLMeshContextConfig,
        context: Context,
        logger: logging.Logger,
    ):
        self.environment = environment
        self.console = console
        self.config = config
        self.context = context
        self.logger = logger

    @contextmanager
    def console_context(self, handler: ConsoleEventHandler):
        id = self.console.add_handler(handler)
        yield
        self.console.remove_handler(id)

    def plan(
        self,
        categorizer: t.Optional[SnapshotCategorizer] = None,
        default_catalog: t.Optional[str] = None,
        **plan_options: t.Unpack[PlanOptions],
    ):
        context = self.context

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
                logger.debug("dagster-sqlmesh: plan")
                controller.console.plan(
                    builder,
                    auto_apply=True,
                    default_catalog=default_catalog,
                )
            except Exception as e:
                controller.console.exception(e)

        generator = ConsoleGenerator(self.logger)
        with self.console_context(generator):
            thread = threading.Thread(
                target=run_sqlmesh_thread,
                args=(
                    self.logger,
                    context,
                    self,
                    self.environment,
                    plan_options,
                    default_catalog,
                ),
            )
            thread.start()

            for event in generator.events(thread):
                match event:
                    case ConsoleException(e):
                        raise e
                    case _:
                        yield event

            thread.join()

    def run(self, **run_options: t.Unpack[RunOptions]):
        # Runs things in thread
        def run_sqlmesh_thread(
            logger: logging.Logger,
            context: Context,
            controller: SQLMeshController,
            environment: str,
            run_options: RunOptions,
        ):
            logger.debug("dagster-sqlmesh: run")
            try:
                context.run(environment=environment, **run_options)
            except Exception as e:
                controller.console.exception(e)

        generator = ConsoleGenerator(self.logger)
        with self.console_context(generator):
            thread = threading.Thread(
                target=run_sqlmesh_thread,
                args=(
                    self.logger,
                    self.context,
                    self,
                    self.environment,
                    run_options or {},
                ),
            )
            thread.start()

            for event in generator.events(thread):
                match event:
                    case ConsoleException(e):
                        raise e
                    case _:
                        yield event

            thread.join()

    def plan_and_run(
        self,
        categorizer: t.Optional[SnapshotCategorizer] = None,
        default_catalog: t.Optional[str] = None,
        plan_options: t.Optional[PlanOptions] = None,
        run_options: t.Optional[RunOptions] = None,
    ):
        run_options = run_options or {}
        plan_options = plan_options or {}

        yield from self.plan(categorizer, default_catalog, **plan_options)
        yield from self.run(**run_options)

    def models(self):
        return self.context.models

    def models_dag(self):
        return self.context.dag


class SQLMeshController:
    """Allows control of sqlmesh via a python interface. It is not suggested to
    use the constructor of this class directly, but instead use the provided
    `setup` class method"""

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
        controller = cls(
            console=console,
            config=config,
        )
        return controller

    def __init__(self, config: SQLMeshContextConfig, console: EventConsole):
        self.config = config
        self.console = console
        self.logger = logger
        self._context_open = False

    def set_logger(self, logger: logging.Logger):
        self.logger = logger

    def add_event_handler(self, handler: ConsoleEventHandler):
        return self.console.add_handler(handler)

    def remove_event_handler(self, handler_id: str):
        return self.console.remove_handler(handler_id)

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
    def instance(self, environment: str):
        if self._context_open:
            raise Exception("Only one sqlmesh instance at a time")

        context = self._create_context()
        self._context_open = True
        try:
            yield SQLMeshInstance(
                environment, self.console, self.config, context, self.logger
            )
        finally:
            self._context_open = False
            context.close()

    def run(
        self,
        environment: str,
        **run_options: t.Unpack[RunOptions],
    ):
        with self.instance(environment) as mesh:
            yield from mesh.run(**run_options)

    def plan(
        self,
        environment: str,
        categorizer: t.Optional[SnapshotCategorizer],
        default_catalog: t.Optional[str],
        plan_options: PlanOptions,
    ):
        with self.instance(environment) as mesh:
            yield from mesh.plan(categorizer, default_catalog, **plan_options)

    def plan_and_run(
        self,
        environment: str,
        categorizer: t.Optional[SnapshotCategorizer] = None,
        default_catalog: t.Optional[str] = None,
        plan_options: t.Optional[PlanOptions] = None,
        run_options: t.Optional[RunOptions] = None,
    ):
        with self.instance(environment) as mesh:
            yield from mesh.plan_and_run(
                categorizer=categorizer,
                default_catalog=default_catalog,
                plan_options=plan_options,
                run_options=run_options,
            )
