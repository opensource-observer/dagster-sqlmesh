import typing as t

from dagster import ConfigurableResource, AssetExecutionContext, MaterializeResult
from sqlmesh.utils.date import TimeLike
from sqlmesh.core.plan.builder import PlanBuilder
from sqlmesh.core.config import CategorizerConfig

from dagster_sqlmesh.asset import SQLMeshDagsterTranslator, setup_sqlmesh_controller
from dagster_sqlmesh.events import ConsoleRecorder
from .config import SQLMeshContextConfig


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


class SQLMeshResource(ConfigurableResource):
    config: SQLMeshContextConfig

    def run(
        self,
        context: AssetExecutionContext,
        translator: SQLMeshDagsterTranslator,
        environment: str = "dev",
        plan_options: t.Optional[PlanOptions] = None,
        run_options: t.Optional[RunOptions] = None,
    ) -> t.List[MaterializeResult]:
        """Execute SQLMesh based on the configuration given"""
        logger = context.log
        controller = self.get_controller()
        controller.context.plan()
        recorder = ConsoleRecorder()

        recorder_handler_id = controller.add_event_handler(recorder)
        logger.debug("start")
        builder = t.cast(
            PlanBuilder,
            controller.context.plan_builder(
                environment=environment,
                **(plan_options or {}),
            ),
        )
        logger.debug("making plan")
        plan = builder.build()
        logger.debug("applying plan")
        controller.context.apply(plan)
        logger.debug("running through the scheduler")
        controller.context.run(environment=environment, **(run_options or {}))
        controller.remove_event_handler(recorder_handler_id)
        controller.context.close()

        materialized: t.List[MaterializeResult] = []
        for updated in recorder._updated:
            asset_key = translator.get_asset_key_from_model(
                controller.context, updated.model
            )
            materialized.append(
                MaterializeResult(
                    asset_key=asset_key,
                    metadata={
                        "updated": True,
                    },
                )
            )
        logger.debug(recorder._updated)
        return materialized

    def get_controller(self):
        return setup_sqlmesh_controller(self.config)
