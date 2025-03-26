import logging
import typing as t

from dagster import AssetExecutionContext, ConfigurableResource, MaterializeResult
from sqlmesh import Model
from sqlmesh.core.context import Context as SQLMeshContext
from sqlmesh.core.snapshot import Snapshot
from sqlmesh.utils.dag import DAG
from sqlmesh.utils.date import TimeLike

from . import console
from .config import SQLMeshContextConfig
from .controller import PlanOptions, RunOptions
from .controller.dagster import DagsterSQLMeshController
from .utils import sqlmesh_model_name_to_key


class MaterializationTracker:
    """Tracks sqlmesh materializations and notifies dagster in the correct
    order. This is necessary because sqlmesh may skip some materializations that
    have no changes and those will be reported as completed out of order."""

    def __init__(self, sorted_dag: list[str], logger: logging.Logger) -> None:
        self.logger = logger
        self._batches: dict[Snapshot, int] = {}
        self._count: dict[Snapshot, int] = {}
        self._complete_update_status: dict[str, bool] = {}
        self._sorted_dag = sorted_dag
        self._current_index = 0

    def plan(self, batches: dict[Snapshot, int]) -> None:
        self._batches = batches
        self._count: dict[Snapshot, int] = {}

        incomplete_names = set()
        for snapshot, count in self._batches.items():
            incomplete_names.add(snapshot.name)
            self._count[snapshot] = 0

        # Anything not in the plan should be listed as completed and queued for
        # notification
        self._complete_update_status = {
            name: False for name in (set(self._sorted_dag) - incomplete_names)
        }

    def update(self, snapshot: Snapshot, _batch_idx: int) -> tuple[int, int]:
        self._count[snapshot] += 1
        current_count = self._count[snapshot]
        expected_count = self._batches[snapshot]
        if self._batches[snapshot] == self._count[snapshot]:
            self._complete_update_status[snapshot.name] = True
        return (current_count, expected_count)

    def notify_queue_next(self) -> tuple[str, bool] | None:
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

    def ensure_standard_obj(self, obj: dict[str, t.Any] | None) -> dict[str, t.Any]:
        obj = obj or {}
        obj["_event_type"] = self.event_name
        return obj

    def info(self, message: str, obj: dict[str, t.Any] | None = None) -> None:
        self.log("info", message, obj)

    def debug(self, message: str, obj: dict[str, t.Any] | None = None) -> None:
        self.log("debug", message, obj)

    def warning(self, message: str, obj: dict[str, t.Any] | None = None) -> None:
        self.log("warning", message, obj)

    def error(self, message: str, obj: dict[str, t.Any] | None = None) -> None:
        self.log("error", message, obj)

    def log(self, level: str | int, message: str, obj: dict[str, t.Any] | None) -> None:
        self._handler.log(level, message, self.ensure_standard_obj(obj))

    @property
    def event_name(self):
        return self._event.__class__.__name__


class DagsterSQLMeshEventHandler:
    def __init__(
        self,
        context: AssetExecutionContext,
        models_map: dict[str, Model],
        dag: DAG[t.Any],
        prefix: str,
    ) -> None:
        self._models_map = models_map
        self._prefix = prefix
        self._context = context
        self._logger = context.log
        self._tracker = MaterializationTracker(dag.sorted[:], self._logger)
        self._stage = "plan"

    def process_events(
        self, sqlmesh_context: SQLMeshContext, event: console.ConsoleEvent
    ) -> t.Iterator[MaterializeResult]:
        self.report_event(event)

        notify = self._tracker.notify_queue_next()
        while notify is not None:
            completed_name, update_status = notify

            # If the model is not in the context, we can skip any notification
            # This will happen for external models
            if not sqlmesh_context.get_model(completed_name):
                notify = self._tracker.notify_queue_next()
                continue

            model = self._models_map.get(completed_name)

            # We allow selecting models. That value is mapped to models_map.
            # If the model is not in models_map, we can skip any notification
            if model:
                output_key = sqlmesh_model_name_to_key(model.name)
                asset_key = self._context.asset_key_for_output(output_key)
                yield MaterializeResult(
                    asset_key=asset_key,
                    metadata={
                        "updated": update_status,
                        "duration_ms": 0,
                    },
                )
            notify = self._tracker.notify_queue_next()

    def report_event(self, event: console.ConsoleEvent) -> None:
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
            case console.LogError(message):
                log_context.error(
                    f"sqlmesh reported an error: {message}",
                )
            case console.LogFailedModels(models):
                if len(models) != 0:
                    failed_models = "\n".join(
                        [f"{model!s}\n{model.__cause__!s}" for model in models]
                    )
                    log_context.error(f"sqlmesh failed models: {failed_models}")
            case _:
                log_context.debug("Received event")

    def log_context(self, event: console.ConsoleEvent) -> SQLMeshEventLogContext:
        return SQLMeshEventLogContext(self, event)

    def log(
        self,
        level: str | int,
        message: str,
        obj: dict[str, t.Any] | None = None,
    ) -> None:
        if level == "error":
            self._logger.error(message)
            return

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
        *,
        environment: str = "dev",
        start: TimeLike | None = None,
        end: TimeLike | None = None,
        restate_selected: bool = False,
        plan_options: PlanOptions | None = None,
        run_options: RunOptions | None = None,
    ) -> t.Iterable[MaterializeResult]:
        """Execute SQLMesh based on the configuration given"""
        plan_options = plan_options or {}
        run_options = run_options or {}

        logger = context.log

        controller = self.get_controller(logger)

        with controller.instance(environment) as mesh:
            dag = mesh.models_dag()

            select_models = []

            models = mesh.models()
            models_map = models.copy()
            all_available_models = set(
                [model.fqn for model, _ in mesh.non_external_models_dag()]
            )
            if context.selected_output_names:
                models_map = {}
                for key, model in models.items():
                    if (
                        sqlmesh_model_name_to_key(model.name)
                        in context.selected_output_names
                    ):
                        logger.info(f"selected model: {model.name}")

                        models_map[key] = model
                        select_models.append(model.name)
            selected_models_set = set(models_map.keys())

            if all_available_models == selected_models_set:
                logger.info("all models selected")

                # Setting this to none to allow sqlmesh to select all models and
                # also remove any models
                select_models = None

            event_handler = DagsterSQLMeshEventHandler(
                context, models_map, dag, "sqlmesh: "
            )

            for event in mesh.plan_and_run(
                start=start,
                end=end,
                select_models=select_models,
                restate_selected=restate_selected,
                plan_options=plan_options,
                run_options=run_options,
            ):
                yield from event_handler.process_events(mesh.context, event)

    def get_controller(
        self, log_override: logging.Logger | None = None
    ) -> DagsterSQLMeshController:
        return DagsterSQLMeshController.setup_with_config(
            self.config, log_override=log_override
        )
