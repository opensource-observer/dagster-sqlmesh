import typing as t
from datetime import date, timedelta

from sqlmesh.core.context import Context
from sqlmesh.core.environment import Environment
from sqlmesh.utils.date import TimeLike, to_date
from sqlmesh.core import constants as c
from sqlmesh.core.snapshot import Snapshot, SnapshotId
from sqlmesh.core.model import Model
from sqlmesh.utils.errors import (
    ConfigError,
)
from sqlmesh.utils import UniqueKeyDict
from sqlmesh.core.scheduler import Scheduler
from sqlmesh.core.config import CategorizerConfig
from sqlmesh.core.plan import PlanBuilder
from sqlmesh.utils.dag import DAG

from dagster_sqlmesh.scheduler import DagsterSQLMeshScheduler


# class DagsterSQLMeshContext(Context):
#     """Custom sqlmesh context so that we can inject selected models to the
#     scheduler when running sqlmesh."""

#     def __init__(self, *args, **kwargs):
#         super().__init__(*args, **kwargs)
#         self._selected_models: t.Set[str] = set()

#     def set_selected_models(self, models: t.Set[str]):
#         self._selected_models = models

#     def scheduler(self, environment: str | None = None) -> Scheduler:
#         """Return a custom scheduler. This uses copy pasted code from the
#         original context. We will make a PR upstream to make this a little more
#         reliable"""

#         snapshots: t.Iterable[Snapshot]
#         if environment is not None:
#             stored_environment = self.state_sync.get_environment(environment)
#             if stored_environment is None:
#                 raise ConfigError(f"Environment '{environment}' was not found.")
#             snapshots = self.state_sync.get_snapshots(
#                 stored_environment.snapshots
#             ).values()
#         else:
#             snapshots = self.snapshots.values()

#         if not snapshots:
#             raise ConfigError("No models were found")

#         selected_snapshots: t.Set[str] = set()
#         if len(self._selected_models) > 0:
#             matched_snapshots = filter(
#                 lambda a: a.model.name in self._selected_models, snapshots
#             )
#             selected_snapshots = set(map(lambda a: a.name, matched_snapshots))

#         return DagsterSQLMeshScheduler(
#             selected_snapshots,
#             snapshots,
#             self.snapshot_evaluator,
#             self.state_sync,
#             default_catalog=self.default_catalog,
#             max_workers=self.concurrent_tasks,
#             console=self.console,
#             notification_target_manager=self.notification_target_manager,
#         )

#     def run_with_selected_models(
#         self,
#         environment: t.Optional[str] = None,
#         start: t.Optional[TimeLike] = None,
#         end: t.Optional[TimeLike] = None,
#         execution_time: t.Optional[TimeLike] = None,
#         skip_janitor: bool = False,
#         ignore_cron: bool = False,
#         select_models: t.Optional[t.Collection[str]] = None,
#     ):
#         if select_models:
#             self.set_selected_models(set(select_models))

#         return self.run(
#             environment=environment,
#             start=start,
#             end=end,
#             execution_time=execution_time,
#             skip_janitor=skip_janitor,
#             ignore_cron=ignore_cron,
#         )

#     def model_selectable_plan_builder(
#         self,
#         environment: t.Optional[str] = None,
#         *,
#         start: t.Optional[TimeLike] = None,
#         end: t.Optional[TimeLike] = None,
#         execution_time: t.Optional[TimeLike] = None,
#         create_from: t.Optional[str] = None,
#         skip_tests: bool = False,
#         restate_models: t.Optional[t.Iterable[str]] = None,
#         no_gaps: bool = False,
#         skip_backfill: bool = False,
#         forward_only: t.Optional[bool] = None,
#         allow_destructive_models: t.Optional[t.Collection[str]] = None,
#         no_auto_categorization: t.Optional[bool] = None,
#         effective_from: t.Optional[TimeLike] = None,
#         include_unmodified: t.Optional[bool] = None,
#         select_models: t.Optional[t.Collection[str]] = None,
#         backfill_models: t.Optional[t.Collection[str]] = None,
#         categorizer_config: t.Optional[CategorizerConfig] = None,
#         enable_preview: t.Optional[bool] = None,
#         run: bool = False,
#     ) -> PlanBuilder:
#         """Creates a plan builder.

#         Args:
#             environment: The environment to diff and plan against.
#             start: The start date of the backfill if there is one.
#             end: The end date of the backfill if there is one.
#             execution_time: The date/time reference to use for execution time. Defaults to now.
#             create_from: The environment to create the target environment from if it
#                 doesn't exist. If not specified, the "prod" environment will be used.
#             skip_tests: Unit tests are run by default so this will skip them if enabled
#             restate_models: A list of either internal or external models, or tags, that need to be restated
#                 for the given plan interval. If the target environment is a production environment,
#                 ALL snapshots that depended on these upstream tables will have their intervals deleted
#                 (even ones not in this current environment). Only the snapshots in this environment will
#                 be backfilled whereas others need to be recovered on a future plan application. For development
#                 environments only snapshots that are part of this plan will be affected.
#             no_gaps:  Whether to ensure that new snapshots for models that are already a
#                 part of the target environment have no data gaps when compared against previous
#                 snapshots for same models.
#             skip_backfill: Whether to skip the backfill step. Default: False.
#             forward_only: Whether the purpose of the plan is to make forward only changes.
#             allow_destructive_models: Models whose forward-only changes are allowed to be destructive.
#             no_auto_categorization: Indicates whether to disable automatic categorization of model
#                 changes (breaking / non-breaking). If not provided, then the corresponding configuration
#                 option determines the behavior.
#             categorizer_config: The configuration for the categorizer. Uses the categorizer configuration defined in the
#                 project config by default.
#             effective_from: The effective date from which to apply forward-only changes on production.
#             include_unmodified: Indicates whether to include unmodified models in the target development environment.
#             select_models: A list of model selection strings to filter the models that should be included into this plan.
#             backfill_models: A list of model selection strings to filter the models for which the data should be backfilled.
#             enable_preview: Indicates whether to enable preview for forward-only models in development environments.
#             run: Whether to run latest intervals as part of the plan application.

#         Returns:
#             The plan builder.
#         """
#         # FIXME... this should actually be something we push upstream. For now
#         # this exists here as a test of it's efficacy
#         environment = environment or self.config.default_target_environment
#         environment = Environment.sanitize_name(environment)
#         is_dev = environment != c.PROD

#         if include_unmodified is None:
#             include_unmodified = self.config.plan.include_unmodified

#         if skip_backfill and not no_gaps and not is_dev:
#             raise ConfigError(
#                 "When targeting the production environment either the backfill should not be skipped or the lack of data gaps should be enforced (--no-gaps flag)."
#             )

#         if run and is_dev:
#             raise ConfigError(
#                 "The '--run' flag is only supported for the production environment."
#             )

#         self._run_plan_tests(skip_tests=skip_tests)

#         environment_ttl = (
#             self.environment_ttl
#             if environment not in self.pinned_environments
#             else None
#         )

#         model_selector = self._new_selector()

#         if allow_destructive_models:
#             expanded_destructive_models = model_selector.expand_model_selections(
#                 allow_destructive_models
#             )
#         else:
#             expanded_destructive_models = None

#         if backfill_models:
#             backfill_models = model_selector.expand_model_selections(backfill_models)
#         else:
#             backfill_models = None

#         models_override: t.Optional[UniqueKeyDict[str, Model]] = None
#         if select_models:
#             models_override = model_selector.select_models(
#                 select_models,
#                 environment,
#                 fallback_env_name=create_from or c.PROD,
#                 ensure_finalized_snapshots=self.config.plan.use_finalized_state,
#             )
#             if not backfill_models:
#                 # Only backfill selected models unless explicitly specified.
#                 backfill_models = model_selector.expand_model_selections(select_models)

#         expanded_restate_models = None
#         if restate_models is not None:
#             expanded_restate_models = model_selector.expand_model_selections(
#                 restate_models
#             )

#         snapshots = self._snapshots(models_override)
#         context_diff = self._context_diff(
#             environment or c.PROD,
#             snapshots=snapshots,
#             create_from=create_from,
#             force_no_diff=restate_models is not None
#             or (backfill_models is not None and not backfill_models),
#             ensure_finalized_snapshots=self.config.plan.use_finalized_state,
#         )

#         # CUSTOMIZATION NOTICE: This is where we have a change from the main sqlmesh
#         if (
#             not include_unmodified
#             and backfill_models is None
#             and expanded_restate_models is None
#         ):
#             # Only backfill modified and added models.
#             # This ensures that no models outside the impacted sub-DAG(s) will be backfilled unexpectedly.
#             backfill_models = {
#                 *context_diff.modified_snapshots,
#                 *[s.name for s in context_diff.added],
#             }

#         # If no end date is specified, use the max interval end from prod
#         # to prevent unintended evaluation of the entire DAG.
#         default_end: t.Optional[int] = None
#         default_start: t.Optional[date] = None
#         max_interval_end_per_model: t.Optional[t.Dict[str, int]] = None
#         if not run and not end:
#             models_for_interval_end: t.Optional[t.Set[str]] = None
#             if backfill_models is not None:
#                 models_for_interval_end = set()
#                 models_stack = list(backfill_models)
#                 while models_stack:
#                     next_model = models_stack.pop()
#                     if next_model not in snapshots:
#                         continue
#                     models_for_interval_end.add(next_model)
#                     models_stack.extend(
#                         s.name
#                         for s in snapshots[next_model].parents
#                         if s.name not in models_for_interval_end
#                     )

#             max_interval_end_per_model = self.state_sync.max_interval_end_per_model(
#                 c.PROD,
#                 models=models_for_interval_end,
#                 ensure_finalized_snapshots=self.config.plan.use_finalized_state,
#             )
#             if max_interval_end_per_model:
#                 default_end = max(max_interval_end_per_model.values())
#                 default_start = to_date(
#                     min(max_interval_end_per_model.values())
#                 ) - timedelta(days=1)

#         return ModelSelectablePlanBuilder(
#             context_diff=context_diff,
#             start=start,
#             end=end,
#             execution_time=execution_time,
#             apply=self.apply,
#             restate_models=expanded_restate_models,
#             backfill_models=backfill_models,
#             no_gaps=no_gaps,
#             skip_backfill=skip_backfill,
#             is_dev=is_dev,
#             forward_only=(
#                 forward_only
#                 if forward_only is not None
#                 else self.config.plan.forward_only
#             ),
#             allow_destructive_models=expanded_destructive_models,
#             environment_ttl=environment_ttl,
#             environment_suffix_target=self.config.environment_suffix_target,
#             environment_catalog_mapping=self.config.environment_catalog_mapping,
#             categorizer_config=categorizer_config or self.auto_categorize_changes,
#             auto_categorization_enabled=not no_auto_categorization,
#             effective_from=effective_from,
#             include_unmodified=include_unmodified,
#             default_start=default_start,
#             default_end=default_end,
#             enable_preview=(
#                 enable_preview
#                 if enable_preview is not None
#                 else self.config.plan.enable_preview
#             ),
#             end_bounded=not run,
#             ensure_finalized_snapshots=self.config.plan.use_finalized_state,
#             engine_schema_differ=self.engine_adapter.SCHEMA_DIFFER,
#             interval_end_per_model=max_interval_end_per_model,
#             console=self.console,
#         )
