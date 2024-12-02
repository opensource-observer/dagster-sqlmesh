import typing as t
import threading
import logging

from dagster import (
    ConfigurableResource,
    AssetExecutionContext,
    MaterializeResult,
)
from sqlmesh import Model
from sqlmesh.utils.date import TimeLike
from sqlmesh.utils.dag import DAG
from sqlmesh.core.plan.builder import PlanBuilder
from sqlmesh.core.config import CategorizerConfig
from sqlmesh.core.snapshot import Snapshot
from sqlmesh.core.context import Context as SQLMeshContext

from .asset import (
    SQLMeshController,
    setup_sqlmesh_controller,
)
from .utils import sqlmesh_model_name_to_key
from .events import ConsoleGenerator
from .config import SQLMeshContextConfig
from . import console


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


def _run_sqlmesh_thread(
    logger: logging.Logger,
    controller: SQLMeshController,
    environment: str,
    plan_options: PlanOptions,
    run_options: RunOptions,
):
    try:
        builder = t.cast(
            PlanBuilder,
            controller.context.plan_builder(
                environment=environment,
                **plan_options,
            ),
        )
        plan = builder.build()
        logger.debug("applying plan")
        controller.context.apply(plan)
        logger.debug("running through the scheduler")
        controller.context.run(environment=environment, **run_options)
        logger.debug("done")
    except Exception as e:
        logger.error(e)


class MaterializationTracker:
    def __init__(self, sorted_dag: t.List[str], logger: logging.Logger):
        self.logger = logger
        self._batches: t.Dict[Snapshot, int] = {}
        self._count: t.Dict[Snapshot, int] = {}
        self._complete_update_status: t.Dict[str, bool] = {}
        self._sorted_dag = sorted_dag
        self._current_index = 0

    def plan(self, batches: t.Dict[Snapshot, int]):
        self._batches = batches
        self._count: t.Dict[Snapshot, int] = {}

        incomplete_names = set()
        for snapshot, count in self._batches.items():
            incomplete_names.add(snapshot.name)
            self._count[snapshot] = 0

        # Anything not in the plan should be listed as completed and queued for
        # notification
        self._complete_update_status = {
            name: False for name in (set(self._sorted_dag) - incomplete_names)
        }

    def update(self, snapshot: Snapshot, _batch_idx: int):
        self._count[snapshot] += 1
        current_count = self._count[snapshot]
        expected_count = self._batches[snapshot]
        if self._batches[snapshot] == self._count[snapshot]:
            self._complete_update_status[snapshot.name] = True
        return (current_count, expected_count)

    def notify_queue_next(self) -> t.Tuple[str, bool] | None:
        if self._current_index >= len(self._sorted_dag):
            return None
        check_name = self._sorted_dag[self._current_index]
        if check_name in self._complete_update_status:
            self._current_index += 1
            return (check_name, self._complete_update_status[check_name])
        return None


class SQLMeshEventLogContext:
    def __init__(
        self,
        handler: "DagsterSQLMeshEventHandler",
        event: console.ConsoleEvent,
    ):
        self._handler = handler
        self._event = event

    def ensure_standard_obj(self, obj: t.Optional[t.Dict[str, t.Any]]):
        obj = obj or {}
        obj["_event_type"] = self.event_name
        return obj

    def info(self, message: str, obj: t.Optional[t.Dict[str, t.Any]] = None):
        self.log("info", message, obj)

    def debug(self, message: str, obj: t.Optional[t.Dict[str, t.Any]] = None):
        self.log("debug", message, obj)

    def warning(self, message: str, obj: t.Optional[t.Dict[str, t.Any]] = None):
        self.log("warning", message, obj)

    def error(self, message: str, obj: t.Optional[t.Dict[str, t.Any]] = None):
        self.log("warning", message, obj)

    def log(self, level: str | int, message: str, obj: t.Optional[t.Dict[str, t.Any]]):
        self._handler.log(level, message, self.ensure_standard_obj(obj))

    @property
    def event_name(self):
        return self._event.__class__.__name__


class DagsterSQLMeshEventHandler:
    def __init__(
        self,
        context: AssetExecutionContext,
        sqlmesh_context: SQLMeshContext,
        models_map: t.Dict[str, Model],
        dag: DAG,
        prefix: str,
    ):
        self._sqlmesh_context = sqlmesh_context
        self._models_map = models_map
        self._prefix = prefix
        self._context = context
        self._logger = context.log
        self._tracker = MaterializationTracker(dag.sorted[:], self._logger)
        self._stage = "plan"

    def process_events(self, event: console.ConsoleEvent):
        self.report_event(event)

        notify = self._tracker.notify_queue_next()
        while notify is not None:
            completed_name, update_status = notify
            if not self._sqlmesh_context.get_model(completed_name):
                notify = self._tracker.notify_queue_next()
                continue
            model = self._models_map[completed_name]
            output_key = sqlmesh_model_name_to_key(model.name)
            asset_key = self._context.asset_key_for_output(output_key)
            # asset_key = translator.get_asset_key_from_model(
            #     controller.context, model
            # )
            yield MaterializeResult(
                asset_key=asset_key,
                metadata={
                    "updated": update_status,
                    "duration_ms": 0,
                },
            )
            notify = self._tracker.notify_queue_next()

    def report_event(self, event: console.ConsoleEvent):
        log_context = self.log_context(event)

        match event:
            case console.StartPlanEvaluation(plan):
                log_context.info(
                    "Starting Plan Evaluation",
                    {
                        "plan": plan,
                    },
                )
            case console.StopPlanEvaluation:
                log_context.info("Plan evaluation completed")
            case console.StartEvaluationProgress(
                batches, environment_naming_info, default_catalog
            ):
                self.update_stage("run")
                log_context.info(
                    "Starting Run",
                    {
                        "default_catalog": default_catalog,
                        "environment_naming_info": environment_naming_info,
                        "backfill_queue": {
                            snapshot.model.name: count
                            for snapshot, count in batches.items()
                        },
                    },
                )
                self._tracker.plan(batches)
            case console.UpdateSnapshotEvaluationProgress(
                snapshot, batch_idx, duration_ms
            ):
                done, expected = self._tracker.update(snapshot, batch_idx)

                log_context.info(
                    "Snapshot progress update",
                    {
                        "asset_key": sqlmesh_model_name_to_key(snapshot.model.name),
                        "progress": f"{done}/{expected}",
                        "duration_ms": duration_ms,
                    },
                )

            case console.LogSuccess(success):
                self.update_stage("done")
                if success:
                    log_context.info("sqlmesh ran successfully")
                else:
                    log_context.error("sqlmesh failed")
                    raise Exception("sqlmesh failed during run")

            case _:
                log_context.debug("Received event")

    def log_context(self, event: console.ConsoleEvent):
        return SQLMeshEventLogContext(self, event)

    def log(
        self,
        level: str | int,
        message: str,
        obj: t.Optional[t.Dict[str, t.Any]] = None,
    ):
        obj = obj or {}
        final_obj = obj.copy()
        final_obj["message"] = message
        final_obj["_sqlmesh_stage"] = self._stage
        self._logger.log(level, final_obj)

    def update_stage(self, stage: str):
        self._stage = stage


class SQLMeshResource(ConfigurableResource):
    config: SQLMeshContextConfig

    def run(
        self,
        context: AssetExecutionContext,
        environment: str = "dev",
        plan_options: t.Optional[PlanOptions] = None,
        run_options: t.Optional[RunOptions] = None,
    ) -> t.Iterable[MaterializeResult]:
        """Execute SQLMesh based on the configuration given"""
        plan_options = plan_options or {}
        run_options = run_options or {}

        logger = context.log

        controller = self.get_controller(logger)
        dag = controller.context.dag

        generator = ConsoleGenerator(logger)

        recorder_handler_id = controller.add_event_handler(generator)

        plan_options["select_models"] = []

        thread = threading.Thread(
            target=_run_sqlmesh_thread,
            args=(
                logger,
                controller,
                environment,
                plan_options or {},
                run_options or {},
            ),
        )

        models_map = controller.context.models.copy()
        if context.selected_output_names:
            models_map = {}
            for key, model in controller.context.models.items():
                if (
                    sqlmesh_model_name_to_key(model.name)
                    in context.selected_output_names
                ):
                    models_map[key] = model

        thread.start()
        event_handler = DagsterSQLMeshEventHandler(
            context, controller.context, models_map, dag, "sqlmesh: "
        )

        for event in generator.events(thread):
            yield from event_handler.process_events(event)

        thread.join()

        controller.remove_event_handler(recorder_handler_id)
        controller.context.close()

    def get_controller(self, log_override: t.Optional[logging.Logger] = None):
        return setup_sqlmesh_controller(self.config, log_override=log_override)
