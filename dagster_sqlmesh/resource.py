import typing as t
import threading
import logging

from dagster import ConfigurableResource, AssetExecutionContext, MaterializeResult
from sqlmesh.utils.date import TimeLike
from sqlmesh.core.plan.builder import PlanBuilder
from sqlmesh.core.config import CategorizerConfig
from sqlmesh.core.snapshot import Snapshot

from .asset import (
    SQLMeshController,
    setup_sqlmesh_controller,
)
from .utils import key_to_sqlmesh_model_name, sqlmesh_model_name_to_key
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
        controller.context.run_with_selected_models(
            environment=environment, **run_options
        )
        logger.debug("done")
    except Exception as e:
        logger.error(e)


class MaterializationTracker:
    def __init__(self, logger: logging.Logger):
        self.logger = logger
        self._batches: t.Dict[Snapshot, int] = {}
        self._count: t.Dict[Snapshot, int] = {}

    def plan(self, batches: t.Dict[Snapshot, int]):
        self._batches = batches
        self._count: t.Dict[Snapshot, int] = {snap: 0 for snap in batches.keys()}

    def update(self, snapshot: Snapshot, _batch_idx: int) -> bool:
        self._count[snapshot] += 1
        if self._batches[snapshot] == self._count[snapshot]:
            return True
        return False


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

        generator = ConsoleGenerator(logger)

        recorder_handler_id = controller.add_event_handler(generator)

        # selected_output_names = context.selected_output_names
        # select_models = None
        # if selected_output_names:
        #     select_models = set(map(key_to_sqlmesh_model_name, selected_output_names))

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
        updated = set()

        tracker = MaterializationTracker(logger)

        thread.start()
        for event in generator.events(thread):
            match event:
                case console.StartPlanEvaluation(_plan):
                    logger.debug("Starting plan evaluation")
                case console.StartEvaluationProgress(
                    batches, environment_naming_info, default_catalog
                ):
                    logger.debug("STARTING EVALUATION")
                    logger.debug(batches)
                    logger.debug(environment_naming_info)
                    logger.debug(default_catalog)
                    tracker.plan(batches)
                case console.UpdatePromotionProgress(snapshot, promoted):
                    logger.debug("UPDATE PROMOTION PROGRESS")
                    logger.debug(snapshot)
                    logger.debug(promoted)
                case console.LogSuccess(success):
                    if success:
                        all_models = set(models_map.keys())
                        noop_models = all_models - updated
                        for model_name in controller.context.dag.sorted:
                            if model_name not in noop_models:
                                continue
                            model = models_map[model_name]
                            output_key = sqlmesh_model_name_to_key(model.name)
                            logger.debug(f"MODEL_NAME={model.name}")
                            logger.debug(
                                f"ASSET_KEY_FROM_CONTEXT={context.asset_key_for_output(output_key)}"
                            )
                            asset_key = context.asset_key_for_output(output_key)
                            # asset_key = translator.get_asset_key_from_model(
                            #     controller.context, model
                            # )
                            yield MaterializeResult(
                                asset_key=asset_key,
                                metadata={
                                    "updated": False,
                                    "duration_ms": 0,
                                },
                            )
                        logger.info("sqlmesh ran successfully")
                        break
                    else:
                        raise Exception("sqlmesh failed during run")
                case console.StartSnapshotEvaluationProgress(snapshot):
                    logger.debug("START SNAPSHOT EVALUATION")
                    logger.debug(snapshot.name)
                case console.UpdateSnapshotEvaluationProgress(
                    snapshot, batch_idx, duration_ms
                ):
                    logger.debug("UPDATE SNAPSHOT EVALUATION")
                    logger.debug(snapshot.name)
                    logger.debug(batch_idx)
                    logger.debug(duration_ms)
                    if tracker.update(snapshot, batch_idx):
                        model = snapshot.model
                        output_key = sqlmesh_model_name_to_key(model.name)
                        logger.debug("Success during update snapshot evaluation")
                        logger.debug(f"MODEL_NAME={model.name}")
                        logger.debug(
                            f"ASSET_KEY_FROM_CONTEXT={context.asset_key_for_output(output_key)}"
                        )
                        asset_key = context.asset_key_for_output(output_key)
                        yield MaterializeResult(
                            asset_key=asset_key,
                            metadata={"updated": True, "duration_ms": duration_ms},
                        )
                        updated.add(snapshot.model.fqn)
                case _:
                    logger.debug("Unhandled event")
                    logger.debug(event)
        thread.join()

        controller.remove_event_handler(recorder_handler_id)
        controller.context.close()

    def get_controller(self, log_override: t.Optional[logging.Logger] = None):
        return setup_sqlmesh_controller(self.config, log_override=log_override)
