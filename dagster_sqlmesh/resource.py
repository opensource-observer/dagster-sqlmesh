import logging
import typing as t
from types import MappingProxyType

from dagster import (
    AssetExecutionContext,
    ConfigurableResource,
    MaterializeResult,
)
from dagster._core.errors import DagsterInvalidPropertyError
from sqlmesh import Model
from sqlmesh.core.context import Context as SQLMeshContext
from sqlmesh.core.snapshot import Snapshot, SnapshotInfoLike, SnapshotTableInfo
from sqlmesh.utils.dag import DAG
from sqlmesh.utils.date import TimeLike
from sqlmesh.utils.errors import SQLMeshError

from dagster_sqlmesh import console
from dagster_sqlmesh.config import SQLMeshContextConfig
from dagster_sqlmesh.controller import PlanOptions, RunOptions
from dagster_sqlmesh.controller.base import (
    DEFAULT_CONTEXT_FACTORY,
    ContextCls,
    ContextFactory,
)
from dagster_sqlmesh.controller.dagster import DagsterSQLMeshController
from dagster_sqlmesh.utils import get_asset_key_str


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
        self.finished_promotion = False

    def init_complete_update_status(self, snapshots: list[SnapshotTableInfo]) -> None:
        planned_model_names = set()
        for snapshot in snapshots:
            planned_model_names.add(snapshot.name)

        # Anything not in the plan should be listed as completed and queued for
        # notification
        self._complete_update_status = {
            name: False for name in (set(self._sorted_dag) - planned_model_names)
        }

    def update_promotion(self, snapshot: SnapshotInfoLike, promoted: bool) -> None:
        self._complete_update_status[snapshot.name] = promoted

    def stop_promotion(self) -> None:
        self.finished_promotion = True

    def plan(self, batches: dict[Snapshot, int]) -> None:
        self._batches = batches
        self._count: dict[Snapshot, int] = {}

        for snapshot, _ in self._batches.items():
            self._count[snapshot] = 0

    def update_plan(self, snapshot: Snapshot, _batch_idx: int) -> tuple[int, int]:
        self._count[snapshot] += 1
        current_count = self._count[snapshot]
        expected_count = self._batches[snapshot]
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


class GenericSQLMeshError(Exception):
    pass


class FailedModelError(Exception):
    def __init__(self, model_name: str, message: str | None) -> None:
        super().__init__(message)
        self.model_name = model_name
        self.message = message


class PlanOrRunFailedError(Exception):
    def __init__(self, stage: str, message: str, errors: list[Exception]) -> None:
        super().__init__(message)
        self.stage = stage
        self.errors = errors


class DagsterSQLMeshEventHandler:
    def __init__(
        self,
        context: AssetExecutionContext,
        models_map: dict[str, Model],
        dag: DAG[t.Any],
        prefix: str,
        is_testing: bool = False,
    ) -> None:
        self._models_map = models_map
        self._prefix = prefix
        self._context = context
        self._logger = context.log
        self._tracker = MaterializationTracker(
            sorted_dag=dag.sorted[:], logger=self._logger
        )
        self._stage = "plan"
        self._errors: list[Exception] = []
        self._is_testing = is_testing

    def process_events(self, event: console.ConsoleEvent) -> None:
        self.report_event(event)

    def notify_success(
        self, sqlmesh_context: SQLMeshContext
    ) -> t.Iterator[MaterializeResult]:
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
                # Passing model.fqn to get internal unique asset key
                output_key = get_asset_key_str(model.fqn)
                if not self._is_testing:
                    # Stupidly dagster when testing cannot use the following
                    # method so we must specifically skip this when testing
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
            case console.StartPlanEvaluation(plan=plan):
                self._tracker.init_complete_update_status(plan.environment.snapshots)
                log_context.info(
                    "Starting Plan Evaluation",
                    {
                        "plan": plan,
                    },
                )
            case console.StopPlanEvaluation:
                log_context.info("Plan evaluation completed")
            case console.StartEvaluationProgress(
                batched_intervals=batches,
                environment_naming_info=environment_naming_info,
                default_catalog=default_catalog,
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
                snapshot=snapshot, batch_idx=batch_idx, duration_ms=duration_ms
            ):
                done, expected = self._tracker.update_plan(snapshot, batch_idx)

                log_context.info(
                    "Snapshot progress update",
                    {
                        "asset_key": get_asset_key_str(snapshot.model.name),
                        "progress": f"{done}/{expected}",
                        "duration_ms": duration_ms,
                    },
                )
            case console.LogSuccess(success=success):
                self.update_stage("done")
                if success:
                    log_context.info("sqlmesh ran successfully")
                else:
                    log_context.error("sqlmesh failed. check collected errors")
            case console.LogError(message=message):
                log_context.error(
                    f"sqlmesh reported an error: {message}",
                )
                self._errors.append(GenericSQLMeshError(message))
            case console.LogFailedModels(errors=errors):
                if len(errors) != 0:
                    failed_models = "\n".join(
                        [f"{error.node!s}\n{error.__cause__!s}" for error in errors]
                    )
                    log_context.error(f"sqlmesh failed models: {failed_models}")
                    for error in errors:
                        self._errors.append(
                            FailedModelError(error.node, str(error.__cause__))
                        )
            case console.UpdatePromotionProgress(snapshot=snapshot, promoted=promoted):
                log_context.info(
                    "Promotion progress update",
                    {
                        "snapshot": snapshot.name,
                        "promoted": promoted,
                    },
                )
                self._tracker.update_promotion(snapshot, promoted)
            case console.StopPromotionProgress(success=success):
                self._tracker.stop_promotion()
                if success:
                    log_context.info("Promotion completed successfully")
                else:
                    log_context.error("Promotion failed")
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

    @property
    def stage(self) -> str:
        return self._stage

    @property
    def errors(self) -> list[Exception]:
        return self._errors[:]


class SQLMeshResource(ConfigurableResource):
    config: SQLMeshContextConfig
    is_testing: bool = False

    def run(
        self,
        context: AssetExecutionContext,
        *,
        context_factory: ContextFactory[ContextCls] = DEFAULT_CONTEXT_FACTORY,
        environment: str = "dev",
        start: TimeLike | None = None,
        end: TimeLike | None = None,
        restate_models: list[str] | None = None,
        select_models: list[str] | None = None,
        restate_selected: bool = False,
        skip_run: bool = False,
        plan_options: PlanOptions | None = None,
        run_options: RunOptions | None = None,
    ) -> t.Iterable[MaterializeResult]:
        """Execute SQLMesh based on the configuration given"""
        plan_options = plan_options or {}
        run_options = run_options or {}

        logger = context.log

        controller = self.get_controller(
            context_factory=context_factory, log_override=logger
        )

        with controller.instance(environment) as mesh:
            dag = mesh.models_dag()

            models = mesh.models()
            models_map = models.copy()
            all_available_models = set(
                [model.fqn for model, _ in mesh.non_external_models_dag()]
            )
            selected_models_set, models_map, select_models = (
                self._get_selected_models_from_context(context=context, models=models)
            )

            if all_available_models == selected_models_set or select_models is None:
                logger.info("all models selected")

                # Setting this to none to allow sqlmesh to select all models and
                # also remove any models
                select_models = None
            else:
                logger.info(f"selected models: {select_models}")

            event_handler = self.create_event_handler(
                context=context,
                models_map=models_map,
                dag=dag,
                prefix="sqlmesh: ",
                is_testing=self.is_testing,
            )

            def raise_for_sqlmesh_errors(
                event_handler: DagsterSQLMeshEventHandler,
                additional_errors: list[Exception] | None = None,
            ) -> None:
                additional_errors = additional_errors or []
                errors = event_handler.errors
                if len(errors) + len(additional_errors) == 0:
                    return
                for error in errors:
                    logger.error(
                        f"sqlmesh encountered the following error during sqlmesh {event_handler.stage}: {error}"
                    )
                raise PlanOrRunFailedError(
                    event_handler.stage,
                    f"sqlmesh failed during {event_handler.stage} with {len(event_handler.errors) + 1} errors",
                    [*errors, *additional_errors],
                )

            try:
                for event in mesh.plan_and_run(
                    start=start,
                    end=end,
                    select_models=select_models,
                    restate_models=restate_models,
                    restate_selected=restate_selected,
                    skip_run=skip_run,
                    plan_options=plan_options,
                    run_options=run_options,
                ):
                    logger.debug(f"sqlmesh event: {event}")
                    event_handler.process_events(event)
            except SQLMeshError as e:
                logger.error(f"sqlmesh error: {e}")
                raise_for_sqlmesh_errors(event_handler, [GenericSQLMeshError(str(e))])
            # Some errors do not raise exceptions immediately, so we need to check
            # the event handler for any errors that may have been collected.
            raise_for_sqlmesh_errors(event_handler)

            yield from event_handler.notify_success(mesh.context)

    def create_event_handler(
        self,
        *,
        context: AssetExecutionContext,
        dag: DAG[str],
        models_map: dict[str, Model],
        prefix: str,
        is_testing: bool,
    ) -> DagsterSQLMeshEventHandler:
        return DagsterSQLMeshEventHandler(
            context=context,
            dag=dag,
            models_map=models_map,
            prefix=prefix,
            is_testing=is_testing,
        )

    def _get_selected_models_from_context(
        self, context: AssetExecutionContext, models: MappingProxyType[str, Model]
    ) -> tuple[set[str], dict[str, Model], list[str] | None]:
        models_map = models.copy()
        try:
            selected_output_names = set(context.selected_output_names)
        except (DagsterInvalidPropertyError, AttributeError) as e:
            # Special case for direct execution context when testing. This is related to:
            # https://github.com/dagster-io/dagster/issues/23633
            if "DirectOpExecutionContext" in str(e):
                context.log.warning("Caught an error that is likely a direct execution")
                return (set(models_map.keys()), models_map, None)
            else:
                raise e

        select_models: list[str] = []
        models_map = {}
        for key, model in models.items():
            if get_asset_key_str(model.fqn) in selected_output_names:
                models_map[key] = model
                select_models.append(model.name)
        return (
            set(models_map.keys()),
            models_map,
            select_models,
        )

    def get_controller(
        self,
        context_factory: ContextFactory[ContextCls],
        log_override: logging.Logger | None = None,
    ) -> DagsterSQLMeshController[ContextCls]:
        return DagsterSQLMeshController.setup_with_config(
            config=self.config,
            context_factory=context_factory,
            log_override=log_override,
        )
