import logging
import typing as t
from datetime import UTC, datetime
from types import MappingProxyType

import dagster as dg
import sqlglot
from dagster._core.errors import DagsterInvalidPropertyError
from pydantic import BaseModel, Field
from sqlglot import exp
from sqlmesh import Model
from sqlmesh.core.context import Context as SQLMeshContext
from sqlmesh.core.plan import Plan as SQLMeshPlan
from sqlmesh.core.snapshot import Snapshot, SnapshotInfoLike
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

if t.TYPE_CHECKING:
    from dagster_sqlmesh.translator import SQLMeshDagsterTranslator

logger = logging.getLogger(__name__)


def _START_OF_UNIX_TIME():
    dt = datetime.strptime("1970-01-01T00:00:00Z", "%Y-%m-%dT%H:%M:%SZ")
    return dt.astimezone(UTC)


class ModelMaterializationStatus(BaseModel):
    model_fqn: str

    # The last time this model was updated or restated
    created_at: datetime = Field(default_factory=lambda: datetime.now(UTC))
    snapshot_id: str

    last_updated_or_restated: datetime = Field(default_factory=_START_OF_UNIX_TIME)
    last_promoted: datetime = Field(default_factory=_START_OF_UNIX_TIME)
    last_backfill: datetime = Field(default_factory=_START_OF_UNIX_TIME)

    def update_or_restate_now(self):
        """Shortcut function to set last_updated_or_restated time to now"""
        self.last_updated_or_restated = datetime.now(UTC)

    def promote_now(self):
        """Shortcut function to set last_promoted time to now"""
        self.last_promoted = datetime.now(UTC)

    def backfill_now(self):
        """Shortcut function to set last_backfill time to now"""
        self.last_backfill = datetime.now(UTC)

    def as_dagster_metadata(
        self, previous: "ModelMaterializationStatus | None"
    ) -> dict[str, dg.MetadataValue]:
        if previous:
            # if the previous materialization status exists then we compare all
            # of the dates and take the _largest_ for all dates except
            # `created_at`
            last_updated_or_restated = dg.MetadataValue.timestamp(
                max(previous.last_updated_or_restated, self.last_updated_or_restated)
            )
            last_promoted = dg.MetadataValue.timestamp(
                max(previous.last_promoted, self.last_promoted)
            )
            last_backfill = dg.MetadataValue.timestamp(
                max(previous.last_backfill, self.last_backfill)
            )
            created_at = dg.MetadataValue.timestamp(previous.created_at)
        else:
            # If there is no previous materialization status all dates can use
            # the created_at timestamp
            created_at = dg.MetadataValue.timestamp(self.created_at)
            last_updated_or_restated = dg.MetadataValue.timestamp(self.created_at)
            last_promoted = dg.MetadataValue.timestamp(self.created_at)
            last_backfill = dg.MetadataValue.timestamp(self.created_at)

        return {
            "snapshot_id": dg.MetadataValue.text(self.snapshot_id),
            "model_fqn": dg.MetadataValue.text(self.model_fqn),
            "created_at": created_at,
            "last_updated_or_restated": last_updated_or_restated,
            "last_promoted": last_promoted,
            "last_backfill": last_backfill,
        }

    @classmethod
    def from_dagster_metadata(
        cls, metadata: dict[str, t.Any]
    ) -> "ModelMaterializationStatus":
        # convert metadata values
        converted: dict[str, dg.MetadataValue] = {}
        for key, value in metadata.items():
            assert isinstance(
                value, dg.MetadataValue
            ), f"Expected MetadataValue for {key}, got {type(value)}"
            converted[key] = value

        return cls.model_validate(
            dict(
                model_fqn=converted["model_fqn"].value,
                snapshot_id=converted["snapshot_id"].value,
                created_at=converted["created_at"].value,
                last_updated_or_restated=converted["last_updated_or_restated"].value,
                last_promoted=converted["last_promoted"].value,
                last_backfill=converted["last_backfill"].value,
            )
        )

    def as_glot_table(self) -> exp.Table:
        return sqlglot.to_table(self.model_fqn)

    def is_match(self, input: str, ignore_catalog: bool = False) -> bool:
        """Tests if the passed in string matches this model's table

        Args:
            input (str): The input string to match against the model's table.
            ignore_catalog (bool): Whether to use to only match table db and
            name (default: False)

        Returns:
            bool - True if the input string matches the model's table, False
            otherwise.
        """
        table = self.as_glot_table()

        input_as_table = sqlglot.to_table(input)

        if input_as_table.name != table.name:
            return False
        if input_as_table.db != table.db:
            return False

        if not ignore_catalog:
            if input_as_table.catalog != table.catalog:
                return False
        return True


class MaterializationTracker:
    """Tracks sqlmesh materializations and notifies dagster in the correct
    order. This is necessary because sqlmesh may skip some materializations that
    have no changes and those will be reported as completed out of order."""

    def __init__(self, sorted_dag: list[str], logger: logging.Logger) -> None:
        self.logger = logger
        self._batches: dict[Snapshot, int] = {}
        self._count: dict[Snapshot, int] = {}
        self._model_metadata: dict[str, ModelMaterializationStatus] = {}
        self._non_model_names: set[str] = set()
        self._sorted_dag = sorted_dag
        self._current_index = 0
        self.finished_promotion = False

    def initialize_from_plan(self, plan: SQLMeshPlan):
        # Initialize all of the model materialization statuses
        # Existing snapshots
        snapshots_by_name = {
            snapshot.name: snapshot for snapshot in plan.snapshots.values()
        }

        created_at = datetime.now(UTC)

        # Include new snapshots
        for snapshot in plan.context_diff.new_snapshots.values():
            snapshots_by_name[snapshot.name] = snapshot

        for model_fqn in self._sorted_dag:
            snapshot = snapshots_by_name.get(model_fqn)

            if not snapshot:
                self._non_model_names.add(model_fqn)
                continue

            if snapshot.is_external:
                self._non_model_names.add(model_fqn)
                continue

            self._model_metadata[snapshot.name] = ModelMaterializationStatus(
                model_fqn=snapshot.model.fqn,
                snapshot_id=snapshot.identifier,
                created_at=created_at,
            )

        # Update all of the model status that are to be updated or restated in this plan
        # This condition was taken from a condition found in sqlmesh's `Context`
        # object. It's used to determine if there are any changes in the plan
        if (
            not plan.context_diff.has_changes
            and not plan.requires_backfill
            and not plan.has_unmodified_unpromoted
        ):
            self.logger.info("No changes detected, adding all models to the queue")
        else:
            context_diff = plan.context_diff
            for snapshot in context_diff.new_snapshots.values():
                self._model_metadata[snapshot.name].update_or_restate_now()
            for snapshots in context_diff.modified_snapshots.values():
                self._model_metadata[snapshots[0].name].update_or_restate_now()
            for snapshot_id in plan.restatements.keys():
                self._model_metadata[
                    plan.snapshots[snapshot_id].name
                ].update_or_restate_now()

    def update_promotion(self, snapshot: SnapshotInfoLike, promoted: bool) -> None:
        if promoted:
            self._model_metadata[snapshot.name].promote_now()

    def update_run(self, snapshot: SnapshotInfoLike) -> None:
        self._model_metadata[snapshot.name].backfill_now()

    def stop_promotion(self) -> None:
        self.finished_promotion = True

    def plan(self, batches: dict[Snapshot, int]) -> None:
        self._batches = batches
        self._count: dict[Snapshot, int] = {}

        for snapshot, _ in self._batches.items():
            self._count[snapshot] = 0
            self._model_metadata[snapshot.name].backfill_now()

    def update_plan(self, snapshot: Snapshot, _batch_idx: int) -> tuple[int, int]:
        self._count[snapshot] += 1
        current_count = self._count[snapshot]
        expected_count = self._batches[snapshot]
        return (current_count, expected_count)

    def notify_queue_next(self) -> tuple[str, ModelMaterializationStatus] | None:
        """Notifies about the next materialization in the queue. At the end of a
        sqlmesh run the `all_up_to_date` flag should be set to True.

        Returns:
            A tuple containing the name of the materialization and its status,
            or None if there are no more model statuses left in the queue
        """
        if self._current_index >= len(self._sorted_dag):
            return None
        self.logger.debug(
            f"MaterializationTracker index {self._current_index}",
            extra=dict(
                current_index=self._current_index,
            ),
        )

        while True:
            model_name_for_notification = self._sorted_dag[self._current_index]

            if model_name_for_notification in self._non_model_names:
                self._current_index += 1
                self.logger.debug(
                    f"skipping non-model snapshot {model_name_for_notification}"
                )
                continue

            if model_name_for_notification in self._model_metadata:
                self._current_index += 1
                return (
                    model_name_for_notification,
                    self._model_metadata[model_name_for_notification],
                )
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
        context: dg.AssetExecutionContext,
        models_map: dict[str, Model],
        dag: DAG[t.Any],
        prefix: str,
        translator: "SQLMeshDagsterTranslator",
        is_testing: bool = False,
        materializations_enabled: bool = True,
    ) -> None:
        """Dagster event handler for SQLMesh models.

        The handler is responsible for reporting events from sqlmesh to dagster.

        Args:
            context: The Dagster asset execution context.
            models_map: A mapping of model names to their SQLMesh model instances.
            dag: The directed acyclic graph representing the SQLMesh models.
            prefix: A prefix to use for all asset keys generated by this handler.
            translator: The SQLMesh Dagster translator instance.
            is_testing: Whether the handler is being used in a testing context.
            materializations_enabled: Whether the handler is to generate
                materializations, this should be disabled if you with to run a
                sqlmesh plan or run in an environment different from the normal
                target environment.
        """
        self._models_map = models_map
        self._prefix = prefix
        self._context = context
        self._logger = context.log
        self._translator = translator
        self._tracker = MaterializationTracker(
            sorted_dag=dag.sorted[:], logger=self._logger
        )
        self._stage = "plan"
        self._errors: list[Exception] = []
        self._is_testing = is_testing
        self._materializations_enabled = materializations_enabled

    def process_events(self, event: console.ConsoleEvent) -> None:
        self.report_event(event)

    def notify_success(
        self, sqlmesh_context: SQLMeshContext
    ) -> t.Iterator[dg.MaterializeResult[t.Any]]:
        notify = self._tracker.notify_queue_next()

        while notify is not None:
            completed_name, materialization_status = notify

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
                output_key = self._translator.get_asset_key_str(model.fqn)
                if self._is_testing:
                    asset_key = dg.AssetKey(["testing", output_key])
                    self._logger.warning(
                        f"Generated fake asset key for testing: {asset_key.to_user_string()}"
                    )
                else:
                    asset_key = self._context.asset_key_for_output(output_key)
                if self._materializations_enabled:
                    yield self.create_materialize_result(
                        self._context, asset_key, materialization_status
                    )
                else:
                    self._logger.debug(
                        f"Materializations disabled. Would have materialized for {asset_key.to_user_string()}"
                    )
            notify = self._tracker.notify_queue_next()
        else:
            self._logger.debug("No more materializations to process")

    def create_materialize_result(
        self,
        context: dg.AssetExecutionContext,
        asset_key: dg.AssetKey,
        current_materialization_status: ModelMaterializationStatus,
    ) -> dg.MaterializeResult[t.Any]:
        last_materialization = context.instance.get_latest_materialization_event(
            asset_key
        )

        if not last_materialization:
            self._logger.debug(
                f"No materialization found for {asset_key.to_user_string()}, all dates will be set to now."
            )
            last_materialization_status = None
        else:
            assert (
                last_materialization.asset_materialization is not None
            ), "Expected asset materialization to be present."
            try:
                last_materialization_status = (
                    ModelMaterializationStatus.from_dagster_metadata(
                        dict(last_materialization.asset_materialization.metadata)
                    )
                )
            except Exception as e:
                self._logger.warning(
                    f"Failed to validate last materialization for {asset_key.to_user_string()}: {e}. Ignoring and using the current status"
                )
                last_materialization_status = None

        return dg.MaterializeResult(
            asset_key=asset_key,
            metadata=current_materialization_status.as_dagster_metadata(
                last_materialization_status
            ),
        )

    def report_event(self, event: console.ConsoleEvent) -> None:
        log_context = self.log_context(event)

        match event:
            case console.PlanBuilt(plan=plan):
                log_context.info(
                    "Plan built",
                    {
                        "snapshots": [s.name for s in plan.environment.snapshots],
                        "models_to_backfill": plan.models_to_backfill,
                        "empty_backfill": plan.empty_backfill,
                        "requires_backfill": plan.requires_backfill,
                    },
                )
                self._tracker.initialize_from_plan(plan)
            case console.StartPlanEvaluation(plan=plan):
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

                if done == expected:
                    log_context.info(
                        "Snapshot progress complete",
                        {
                            "asset_key": self._translator.get_asset_key_str(snapshot.model.name),
                        },
                    )
                    self._tracker.update_run(snapshot)
                else:
                    log_context.info(
                        "Snapshot progress update",
                        {
                            "asset_key": self._translator.get_asset_key_str(snapshot.model.name),
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


class SQLMeshResource(dg.ConfigurableResource):
    is_testing: bool = False

    def run(
        self,
        context: dg.AssetExecutionContext,
        *,
        config: SQLMeshContextConfig,
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
        materializations_enabled: bool = True,
    ) -> t.Iterable[dg.MaterializeResult[t.Any]]:
        """Execute SQLMesh based on the configuration given"""
        plan_options = plan_options or {}
        run_options = run_options or {}

        logger = context.log

        controller = self.get_controller(
            config=config,
            context_factory=context_factory, 
            log_override=logger
        )

        with controller.instance(environment) as mesh:
            dag = mesh.models_dag()

            models = mesh.models()
            models_map = models.copy()
            all_available_models = set(
                [model.fqn for model, _ in mesh.non_external_models_dag()]
            )
            selected_models_set, models_map, select_models = (
                self._get_selected_models_from_context(context=context, config=config, models=models)
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
                config=config,
                models_map=models_map,
                dag=dag,
                prefix="sqlmesh: ",
                is_testing=self.is_testing,
                materializations_enabled=materializations_enabled,
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
            logger.info(f"sqlmesh run completed for {len(models_map)} models")
            # Some errors do not raise exceptions immediately, so we need to check
            # the event handler for any errors that may have been collected.
            raise_for_sqlmesh_errors(event_handler)

            yield from event_handler.notify_success(mesh.context)

            logger.debug("sqlmesh selected all models notified of completion")

    def create_event_handler(
        self,
        *,
        context: dg.AssetExecutionContext,
        config: SQLMeshContextConfig,
        dag: DAG[str],
        models_map: dict[str, Model],
        prefix: str,
        is_testing: bool,
        materializations_enabled: bool,
    ) -> DagsterSQLMeshEventHandler:
        translator = config.get_translator()
        return DagsterSQLMeshEventHandler(
            context=context,
            dag=dag,
            models_map=models_map,
            prefix=prefix,
            translator=translator,
            is_testing=is_testing,
            materializations_enabled=materializations_enabled,
        )

    def _get_selected_models_from_context(
        self, 
        context: dg.AssetExecutionContext, 
        config: SQLMeshContextConfig,
        models: MappingProxyType[str, Model]
    ) -> tuple[set[str], dict[str, Model], list[str] | None]:
        models_map = models.copy()
        try:
            selected_output_names = set(context.op_execution_context.selected_output_names)
        except (DagsterInvalidPropertyError, AttributeError) as e:
            # Special case for direct execution context when testing. This is related to:
            # https://github.com/dagster-io/dagster/issues/23633
            if "DirectOpExecutionContext" in str(e):
                context.log.warning("Caught an error that is likely a direct execution")
                return (set(models_map.keys()), models_map, None)
            else:
                raise e

        translator = config.get_translator()
        select_models: list[str] = []
        models_map = {}
        for key, model in models.items():
            if translator.get_asset_key_str(model.fqn) in selected_output_names:
                models_map[key] = model
                select_models.append(model.name)
        return (
            set(models_map.keys()),
            models_map,
            select_models,
        )

    def get_controller(
        self,
        config: SQLMeshContextConfig,
        context_factory: ContextFactory[ContextCls],
        log_override: logging.Logger | None = None,
    ) -> DagsterSQLMeshController[ContextCls]:
        return DagsterSQLMeshController.setup_with_config(
            config=config,
            context_factory=context_factory,
            log_override=log_override,
        )
