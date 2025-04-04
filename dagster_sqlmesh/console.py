import logging
import typing as t
import unittest
import uuid
from collections.abc import Callable
from dataclasses import dataclass, field

from sqlglot.expressions import Alter
from sqlmesh.core.console import Console
from sqlmesh.core.context_diff import ContextDiff
from sqlmesh.core.environment import EnvironmentNamingInfo
from sqlmesh.core.linter.rule import RuleViolation
from sqlmesh.core.model import Model
from sqlmesh.core.plan import EvaluatablePlan, PlanBuilder
from sqlmesh.core.snapshot import Snapshot, SnapshotChangeCategory, SnapshotInfoLike
from sqlmesh.core.table_diff import RowDiff, SchemaDiff, TableDiff
from sqlmesh.utils.concurrency import NodeExecutionFailedError

logger = logging.getLogger(__name__)


@dataclass
class StartMigrationProgress:
    total_tasks: int


@dataclass
class UpdateMigrationProgress:
    num_tasks: int


@dataclass
class StopMigrationProgress:
    pass


@dataclass
class StartPlanEvaluation:
    evaluatable_plan: EvaluatablePlan


@dataclass
class StopPlanEvaluation:
    pass


@dataclass
class StartEvaluationProgress:
    batches: dict[Snapshot, int]
    environment_naming_info: EnvironmentNamingInfo
    default_catalog: str | None


@dataclass
class StartSnapshotEvaluationProgress:
    snapshot: Snapshot


@dataclass
class UpdateSnapshotEvaluationProgress:
    snapshot: Snapshot
    batch_idx: int
    duration_ms: int | None


@dataclass
class StopEvaluationProgress:
    success: bool = True


@dataclass
class StartCreationProgress:
    total_tasks: int
    environment_naming_info: EnvironmentNamingInfo
    default_catalog: str | None


@dataclass
class UpdateCreationProgress:
    snapshot: SnapshotInfoLike


@dataclass
class StopCreationProgress:
    success: bool = True


@dataclass
class StartCleanup:
    ignore_ttl: bool


@dataclass
class UpdateCleanupProgress:
    object_name: str


@dataclass
class StopCleanup:
    success: bool = True


@dataclass
class StartPromotionProgress:
    total_tasks: int
    environment_naming_info: EnvironmentNamingInfo
    default_catalog: str | None


@dataclass
class UpdatePromotionProgress:
    snapshot: SnapshotInfoLike
    promoted: bool


@dataclass
class StopPromotionProgress:
    success: bool = True


@dataclass
class UpdateSnapshotMigrationProgress:
    num_tasks: int


@dataclass
class LogMigrationStatus:
    success: bool = True


@dataclass
class StartSnapshotMigrationProgress:
    total_tasks: int


@dataclass
class StopSnapshotMigrationProgress:
    success: bool = True


@dataclass
class StartEnvMigrationProgress:
    total_tasks: int


@dataclass
class UpdateEnvMigrationProgress:
    num_tasks: int


@dataclass
class StopEnvMigrationProgress:
    success: bool = True


@dataclass
class ShowModelDifferenceSummary:
    context_diff: ContextDiff
    environment_naming_info: EnvironmentNamingInfo
    default_catalog: str | None
    no_diff: bool = True


@dataclass
class PlanEvent:
    plan_builder: PlanBuilder
    auto_apply: bool
    default_catalog: str | None
    no_diff: bool = False
    no_prompts: bool = False


@dataclass
class LogTestResults:
    result: unittest.result.TestResult
    output: str | None
    target_dialect: str


@dataclass
class ShowSQL:
    sql: str


@dataclass
class LogStatusUpdate:
    message: str


@dataclass
class LogError:
    message: str


@dataclass
class LogWarning:
    short_message: str
    long_message: str | None = None


@dataclass
class LogSuccess:
    message: str


@dataclass
class LogFailedModels:
    errors: list[NodeExecutionFailedError[str]]


@dataclass
class LogSkippedModels:
    snapshot_names: set[str]


@dataclass
class LogDestructiveChange:
    snapshot_name: str
    dropped_column_names: list[str]
    alter_expressions: list[Alter]
    dialect: str
    error: bool = True


@dataclass
class LoadingStart:
    message: str | None = None
    id: uuid.UUID = field(default_factory=uuid.uuid4)


@dataclass
class LoadingStop:
    id: uuid.UUID


@dataclass
class ShowSchemaDiff:
    schema_diff: SchemaDiff


@dataclass
class ShowRowDiff:
    row_diff: RowDiff
    show_sample: bool = True
    skip_grain_check: bool = False


@dataclass
class ConsoleException:
    exception: Exception


@dataclass
class PrintEnvironments:
    environments_summary: dict[str, int]


@dataclass
class ShowTableDiffSummary:
    table_diff: TableDiff


ConsoleEvent = (
    StartPlanEvaluation
    | StopPlanEvaluation
    | StartEvaluationProgress
    | StartSnapshotEvaluationProgress
    | UpdateSnapshotEvaluationProgress
    | StopEvaluationProgress
    | StartCreationProgress
    | UpdateCreationProgress
    | StopCreationProgress
    | StartCleanup
    | UpdateCleanupProgress
    | StopCleanup
    | StartPromotionProgress
    | UpdatePromotionProgress
    | StopPromotionProgress
    | UpdateSnapshotMigrationProgress
    | LogMigrationStatus
    | StopSnapshotMigrationProgress
    | StartEnvMigrationProgress
    | UpdateEnvMigrationProgress
    | StopEnvMigrationProgress
    | ShowModelDifferenceSummary
    | PlanEvent
    | LogTestResults
    | ShowSQL
    | LogStatusUpdate
    | LogError
    | LogWarning
    | LogSuccess
    | LogFailedModels
    | LogSkippedModels
    | LogDestructiveChange
    | LoadingStart
    | LoadingStop
    | ShowSchemaDiff
    | ShowRowDiff
    | StartMigrationProgress
    | UpdateMigrationProgress
    | StopMigrationProgress
    | StartSnapshotMigrationProgress
    | ConsoleException
    | PrintEnvironments
    | ShowTableDiffSummary
)

ConsoleEventHandler = Callable[[ConsoleEvent], None]

SnapshotCategorizer = t.Callable[
    [Snapshot, PlanBuilder, str | None], SnapshotChangeCategory
]


class EventConsole(Console):
    """
    A console implementation that manages and publishes events related to
    SQLMesh operations. The sqlmesh console implementation is mostly for it's
    CLI application and doesn't take into account using sqlmesh as a library.
    This event pub/sub interface allows us to capture events and choose how we
    wish to handle it with N number of handlers.

    This class extends the Console class and provides functionality to handle
    various events during SQLMesh processes such as plan evaluation, creation,
    promotion, migration, and testing.
    """

    categorizer: SnapshotCategorizer | None = None

    def __init__(self, log_override: logging.Logger | None = None) -> None:
        self._handlers: dict[str, ConsoleEventHandler] = {}
        self.logger = log_override or logger
        self.id = str(uuid.uuid4())
        self.logger.debug(f"EventConsole[{self.id}]: created")
        self.categorizer = None

    def add_snapshot_categorizer(self, categorizer: SnapshotCategorizer) -> None:
        self.categorizer = categorizer

    def start_plan_evaluation(self, plan: EvaluatablePlan) -> None:
        self.publish(StartPlanEvaluation(plan))

    def stop_plan_evaluation(self) -> None:
        self.publish(StopPlanEvaluation())

    def start_evaluation_progress(
        self,
        batches: dict[Snapshot, int],
        environment_naming_info: EnvironmentNamingInfo,
        default_catalog: str | None,
    ) -> None:
        self.publish(
            StartEvaluationProgress(batches, environment_naming_info, default_catalog)
        )

    def start_snapshot_evaluation_progress(self, snapshot: Snapshot) -> None:
        self.publish(StartSnapshotEvaluationProgress(snapshot))

    def update_snapshot_evaluation_progress(
        self, snapshot: Snapshot, batch_idx: int, duration_ms: int | None
    ) -> None:
        self.publish(UpdateSnapshotEvaluationProgress(snapshot, batch_idx, duration_ms))

    def stop_evaluation_progress(self, success: bool = True) -> None:
        self.publish(StopEvaluationProgress(success))

    def start_creation_progress(
        self,
        total_tasks: int,
        environment_naming_info: EnvironmentNamingInfo,
        default_catalog: str | None,
    ) -> None:
        self.publish(
            StartCreationProgress(total_tasks, environment_naming_info, default_catalog)
        )

    def update_creation_progress(self, snapshot: SnapshotInfoLike) -> None:
        self.publish(UpdateCreationProgress(snapshot))

    def stop_creation_progress(self, success: bool = True) -> None:
        self.publish(StopCreationProgress(success))

    def start_cleanup(self, ignore_ttl: bool) -> bool:
        event = StartCleanup(ignore_ttl)
        self.publish(event)
        return True  # Assuming the cleanup should always proceed, or modify as needed

    def update_cleanup_progress(self, object_name: str) -> None:
        self.publish(UpdateCleanupProgress(object_name))

    def stop_cleanup(self, success: bool = True) -> None:
        self.publish(StopCleanup(success))

    def start_promotion_progress(
        self,
        total_tasks: int,
        environment_naming_info: EnvironmentNamingInfo,
        default_catalog: str | None,
    ) -> None:
        self.publish(
            StartPromotionProgress(
                total_tasks, environment_naming_info, default_catalog
            )
        )

    def update_promotion_progress(
        self, snapshot: SnapshotInfoLike, promoted: bool
    ) -> None:
        self.publish(UpdatePromotionProgress(snapshot, promoted))

    def stop_promotion_progress(self, success: bool = True) -> None:
        self.publish(StopPromotionProgress(success))

    def start_snapshot_migration_progress(self, total_tasks: int) -> None:
        self.publish(StartSnapshotMigrationProgress(total_tasks))

    def update_snapshot_migration_progress(self, num_tasks: int) -> None:
        self.publish(UpdateSnapshotMigrationProgress(num_tasks))

    def log_migration_status(self, success: bool = True) -> None:
        self.publish(LogMigrationStatus(success))

    def stop_snapshot_migration_progress(self, success: bool = True) -> None:
        self.publish(StopSnapshotMigrationProgress(success))

    def start_env_migration_progress(self, total_tasks: int) -> None:
        self.publish(StartEnvMigrationProgress(total_tasks))

    def update_env_migration_progress(self, num_tasks: int) -> None:
        self.publish(UpdateEnvMigrationProgress(num_tasks))

    def stop_env_migration_progress(self, success: bool = True) -> None:
        self.publish(StopEnvMigrationProgress(success))

    def show_model_difference_summary(
        self,
        context_diff: ContextDiff,
        environment_naming_info: EnvironmentNamingInfo,
        default_catalog: str | None,
        no_diff: bool = True,
    ) -> None:
        self.publish(
            ShowModelDifferenceSummary(
                context_diff,
                environment_naming_info,
                default_catalog,
                no_diff,
            )
        )

    def plan(
        self,
        plan_builder: PlanBuilder,
        auto_apply: bool,
        default_catalog: str | None,
        no_diff: bool = False,
        no_prompts: bool = False,
    ) -> None:
        self.logger.debug("building plan created")
        plan = plan_builder.build()
        self.logger.debug(f"plan created: {plan}")

        for snapshot in plan.uncategorized:
            if self.categorizer:
                plan_builder.set_choice(
                    snapshot, self.categorizer(snapshot, plan_builder, default_catalog)
                )

        if auto_apply:
            plan_builder.apply()

    def log_test_results(
        self,
        result: unittest.result.TestResult,
        output: str | None,
        target_dialect: str,
    ) -> None:
        self.publish(LogTestResults(result, output, target_dialect))

    def show_sql(self, sql: str) -> None:
        self.publish(ShowSQL(sql))

    def log_status_update(self, message: str) -> None:
        self.publish(LogStatusUpdate(message))

    def log_error(self, message: str) -> None:
        self.publish(LogError(message))

    def log_warning(self, short_message: str, long_message: str | None = None) -> None:
        self.publish(LogWarning(short_message, long_message))

    def log_success(self, message: str) -> None:
        self.publish(LogSuccess(message))

    def log_failed_models(self, errors: list[NodeExecutionFailedError[str]]) -> None:
        self.publish(LogFailedModels(errors))

    def log_skipped_models(self, snapshot_names: set[str]) -> None:
        self.publish(LogSkippedModels(snapshot_names))

    def log_destructive_change(
        self,
        snapshot_name: str,
        dropped_column_names: list[str],
        alter_expressions: list[Alter],
        dialect: str,
        error: bool = True,
    ) -> None:
        self.publish(
            LogDestructiveChange(
                snapshot_name, dropped_column_names, alter_expressions, dialect, error
            )
        )

    def loading_start(self, message: str | None = None) -> uuid.UUID:
        event_id = uuid.uuid4()
        self.publish(LoadingStart(message, event_id))
        return event_id

    def loading_stop(self, id: uuid.UUID) -> None:
        self.publish(LoadingStop(id))

    def show_schema_diff(self, schema_diff: SchemaDiff) -> None:
        self.publish(ShowSchemaDiff(schema_diff))

    def show_row_diff(
        self,
        row_diff: RowDiff,
        show_sample: bool = True,
        skip_grain_check: bool = False,
    ) -> None:
        self.publish(ShowRowDiff(row_diff, show_sample, skip_grain_check))

    def publish(self, event: ConsoleEvent) -> None:
        self.logger.debug(
            f"EventConsole[{self.id}]: sending event to {len(self._handlers)}"
        )
        for handler in self._handlers.values():
            handler(event)

    def add_handler(self, handler: ConsoleEventHandler) -> str:
        handler_id = str(uuid.uuid4())
        self.logger.debug(f"EventConsole[{self.id}]: Adding handler {handler_id}")
        self._handlers[handler_id] = handler
        return handler_id

    def remove_handler(self, handler_id: str) -> None:
        del self._handlers[handler_id]

    def exception(self, exc: Exception) -> None:
        self.publish(ConsoleException(exc))

    def print_environments(self, environments_summary: dict[str, int]) -> None:
        self.publish(PrintEnvironments(environments_summary))

    def show_table_diff_summary(self, table_diff: TableDiff) -> None:
        self.publish(ShowTableDiffSummary(table_diff))

    def show_linter_violations(
        self,
        violations: list[RuleViolation],
        model: Model,
        is_error: bool = False,
    ) -> None:
        """Show linting violations from SQLMesh.

        Args:
            violations: List of linting violations to display
            model: The model being linted
            is_error: Whether the violations are errors
        """
        self.publish(LogWarning("Linting violations found", str(violations)))


class DebugEventConsole(EventConsole):
    """A console that wraps an existing console and logs all events to a logger"""

    def __init__(self, console: Console):
        super().__init__()
        self._console = console

    def start_plan_evaluation(self, plan: EvaluatablePlan) -> None:
        super().start_plan_evaluation(plan)
        self._console.start_plan_evaluation(plan)

    def stop_plan_evaluation(self) -> None:
        super().stop_plan_evaluation()
        self._console.stop_plan_evaluation()

    def start_evaluation_progress(
        self,
        batches: dict[Snapshot, int],
        environment_naming_info: EnvironmentNamingInfo,
        default_catalog: str | None,
    ) -> None:
        super().start_evaluation_progress(
            batches, environment_naming_info, default_catalog
        )
        self._console.start_evaluation_progress(
            batches, environment_naming_info, default_catalog
        )

    def start_snapshot_evaluation_progress(self, snapshot: Snapshot) -> None:
        super().start_snapshot_evaluation_progress(snapshot)
        self._console.start_snapshot_evaluation_progress(snapshot)

    def update_snapshot_evaluation_progress(
        self, snapshot: Snapshot, batch_idx: int, duration_ms: int | None
    ) -> None:
        super().update_snapshot_evaluation_progress(snapshot, batch_idx, duration_ms)
        self._console.update_snapshot_evaluation_progress(
            snapshot, batch_idx, duration_ms
        )

    def stop_evaluation_progress(self, success: bool = True) -> None:
        super().stop_evaluation_progress(success)
        self._console.stop_evaluation_progress(success)

    def start_creation_progress(
        self,
        total_tasks: int,
        environment_naming_info: EnvironmentNamingInfo,
        default_catalog: str | None,
    ) -> None:
        super().start_creation_progress(
            total_tasks, environment_naming_info, default_catalog
        )
        self._console.start_creation_progress(
            total_tasks, environment_naming_info, default_catalog
        )

    def update_creation_progress(self, snapshot: SnapshotInfoLike) -> None:
        super().update_creation_progress(snapshot)
        self._console.update_creation_progress(snapshot)

    def stop_creation_progress(self, success: bool = True) -> None:
        super().stop_creation_progress(success)
        self._console.stop_creation_progress(success)

    def update_cleanup_progress(self, object_name: str) -> None:
        super().update_cleanup_progress(object_name)
        self._console.update_cleanup_progress(object_name)

    def start_promotion_progress(
        self,
        total_tasks: int,
        environment_naming_info: EnvironmentNamingInfo,
        default_catalog: str | None,
    ) -> None:
        super().start_promotion_progress(
            total_tasks, environment_naming_info, default_catalog
        )
        self._console.start_promotion_progress(
            total_tasks, environment_naming_info, default_catalog
        )

    def update_promotion_progress(
        self, snapshot: SnapshotInfoLike, promoted: bool
    ) -> None:
        super().update_promotion_progress(snapshot, promoted)
        self._console.update_promotion_progress(snapshot, promoted)

    def stop_promotion_progress(self, success: bool = True) -> None:
        super().stop_promotion_progress(success)
        self._console.stop_promotion_progress(success)

    def show_model_difference_summary(
        self,
        context_diff: ContextDiff,
        environment_naming_info: EnvironmentNamingInfo,
        default_catalog: str | None,
        no_diff: bool = True,
    ) -> None:
        super().show_model_difference_summary(
            context_diff,
            environment_naming_info,
            default_catalog,
            no_diff,
        )
        self._console.show_model_difference_summary(
            context_diff,
            environment_naming_info,
            default_catalog,
            no_diff,
            # ignored_snapshot_ids,
        )

    def plan(
        self,
        plan_builder: PlanBuilder,
        auto_apply: bool,
        default_catalog: str | None,
        no_diff: bool = False,
        no_prompts: bool = False,
    ) -> None:
        super().plan(plan_builder, auto_apply, default_catalog, no_diff, no_prompts)
        self._console.plan(
            plan_builder, auto_apply, default_catalog, no_diff, no_prompts
        )

    def log_test_results(
        self,
        result: unittest.result.TestResult,
        output: str | None,
        target_dialect: str,
    ) -> None:
        super().log_test_results(result, output, target_dialect)
        self._console.log_test_results(result, output, target_dialect)

    def show_sql(self, sql: str) -> None:
        super().show_sql(sql)
        self._console.show_sql(sql)

    def log_status_update(self, message: str) -> None:
        super().log_status_update(message)
        self._console.log_status_update(message)

    def log_error(self, message: str) -> None:
        super().log_error(message)
        self._console.log_error(message)

    def log_success(self, message: str) -> None:
        super().log_success(message)
        self._console.log_success(message)

    def loading_start(self, message: str | None = None) -> uuid.UUID:
        event_id = super().loading_start(message)
        self._console.loading_start(message)
        return event_id

    def loading_stop(self, id: uuid.UUID) -> None:
        super().loading_stop(id)
        self._console.loading_stop(id)

    def show_schema_diff(self, schema_diff: SchemaDiff) -> None:
        super().show_schema_diff(schema_diff)
        self._console.show_schema_diff(schema_diff)

    def show_row_diff(
        self,
        row_diff: RowDiff,
        show_sample: bool = True,
        skip_grain_check: bool = False,
    ) -> None:
        super().show_row_diff(row_diff, show_sample)
        self._console.show_row_diff(row_diff, show_sample, skip_grain_check)

    def show_linter_violations(
        self,
        violations: list[RuleViolation],
        model: Model,
        is_error: bool = False,
    ) -> None:
        super().show_linter_violations(violations, model, is_error)
        self._console.show_linter_violations(violations, model, is_error)
