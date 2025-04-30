import inspect
import logging
import textwrap
import typing as t
import unittest
import uuid
from dataclasses import dataclass, field

from sqlglot.expressions import Alter
from sqlmesh.core.console import Console
from sqlmesh.core.context_diff import ContextDiff
from sqlmesh.core.environment import EnvironmentNamingInfo
from sqlmesh.core.plan import EvaluatablePlan, PlanBuilder
from sqlmesh.core.snapshot import Snapshot, SnapshotChangeCategory, SnapshotInfoLike
from sqlmesh.core.table_diff import RowDiff, SchemaDiff, TableDiff
from sqlmesh.utils.concurrency import NodeExecutionFailedError

logger = logging.getLogger(__name__)

@dataclass(kw_only=True)
class BaseConsoleEvent:
    unknown_args: dict[str, t.Any] = field(default_factory=dict)

@dataclass(kw_only=True)
class StartMigrationProgress(BaseConsoleEvent):
    total_tasks: int

@dataclass(kw_only=True)
class UpdateMigrationProgress(BaseConsoleEvent):
    num_tasks: int

@dataclass(kw_only=True)
class StopMigrationProgress(BaseConsoleEvent):
    pass

@dataclass(kw_only=True)
class StartPlanEvaluation(BaseConsoleEvent):
    plan: EvaluatablePlan

@dataclass(kw_only=True)
class StopPlanEvaluation(BaseConsoleEvent):
    pass

@dataclass(kw_only=True)
class StartEvaluationProgress(BaseConsoleEvent):
    batched_intervals: dict[Snapshot, int]
    environment_naming_info: EnvironmentNamingInfo
    default_catalog: str | None

@dataclass(kw_only=True)
class StartSnapshotEvaluationProgress(BaseConsoleEvent):
    snapshot: Snapshot

@dataclass(kw_only=True)
class UpdateSnapshotEvaluationProgress(BaseConsoleEvent):
    snapshot: Snapshot
    batch_idx: int
    duration_ms: int | None

@dataclass(kw_only=True)
class StopEvaluationProgress(BaseConsoleEvent):
    success: bool = True

@dataclass(kw_only=True)
class StartCreationProgress(BaseConsoleEvent):
    snapshots: list[Snapshot]
    environment_naming_info: EnvironmentNamingInfo
    default_catalog: str | None

@dataclass(kw_only=True)
class UpdateCreationProgress(BaseConsoleEvent):
    snapshot: SnapshotInfoLike

@dataclass(kw_only=True)
class StopCreationProgress(BaseConsoleEvent):
    success: bool = True

@dataclass(kw_only=True)
class StartCleanup(BaseConsoleEvent):
    ignore_ttl: bool

@dataclass(kw_only=True)
class UpdateCleanupProgress(BaseConsoleEvent):
    object_name: str

@dataclass(kw_only=True)
class StopCleanup(BaseConsoleEvent):
    success: bool = True

@dataclass(kw_only=True)
class StartPromotionProgress(BaseConsoleEvent):
    total_tasks: int
    environment_naming_info: EnvironmentNamingInfo
    default_catalog: str | None

@dataclass(kw_only=True)
class UpdatePromotionProgress(BaseConsoleEvent):
    snapshot: SnapshotInfoLike
    promoted: bool

@dataclass(kw_only=True)
class StopPromotionProgress(BaseConsoleEvent):
    success: bool = True

@dataclass(kw_only=True)
class UpdateSnapshotMigrationProgress(BaseConsoleEvent):
    num_tasks: int

@dataclass(kw_only=True)
class LogMigrationStatus(BaseConsoleEvent):
    success: bool = True

@dataclass(kw_only=True)
class StartSnapshotMigrationProgress(BaseConsoleEvent):
    total_tasks: int

@dataclass(kw_only=True)
class StopSnapshotMigrationProgress(BaseConsoleEvent):
    success: bool = True

@dataclass(kw_only=True)
class StartEnvMigrationProgress(BaseConsoleEvent):
    total_tasks: int

@dataclass(kw_only=True)
class UpdateEnvMigrationProgress(BaseConsoleEvent):
    num_tasks: int

@dataclass(kw_only=True)
class StopEnvMigrationProgress(BaseConsoleEvent):
    success: bool = True

@dataclass(kw_only=True)
class ShowModelDifferenceSummary(BaseConsoleEvent):
    context_diff: ContextDiff
    environment_naming_info: EnvironmentNamingInfo
    default_catalog: str | None
    no_diff: bool = True

@dataclass(kw_only=True)
class Plan(BaseConsoleEvent):
    plan_builder: PlanBuilder
    auto_apply: bool
    default_catalog: str | None
    no_diff: bool = False
    no_prompts: bool = False

@dataclass(kw_only=True)
class LogTestResults(BaseConsoleEvent):
    result: unittest.result.TestResult
    output: str | None
    target_dialect: str

@dataclass(kw_only=True)
class ShowSQL(BaseConsoleEvent):
    sql: str

@dataclass(kw_only=True)
class LogStatusUpdate(BaseConsoleEvent):
    message: str

@dataclass(kw_only=True)
class LogError(BaseConsoleEvent):
    message: str

@dataclass(kw_only=True)
class LogWarning(BaseConsoleEvent):
    short_message: str
    long_message: str | None = None

@dataclass(kw_only=True)
class LogSuccess(BaseConsoleEvent):
    message: str

@dataclass(kw_only=True)
class LogFailedModels(BaseConsoleEvent):
    errors: list[NodeExecutionFailedError[str]]

@dataclass(kw_only=True)
class LogSkippedModels(BaseConsoleEvent):
    snapshot_names: set[str]

@dataclass(kw_only=True)
class LogDestructiveChange(BaseConsoleEvent):
    snapshot_name: str
    dropped_column_names: list[str]
    alter_expressions: list[Alter]
    dialect: str
    error: bool = True

@dataclass(kw_only=True)
class LoadingStart(BaseConsoleEvent):
    message: str | None = None
    id: uuid.UUID = field(default_factory=uuid.uuid4)

@dataclass(kw_only=True)
class LoadingStop(BaseConsoleEvent):
    id: uuid.UUID

@dataclass(kw_only=True)
class ShowSchemaDiff(BaseConsoleEvent):
    schema_diff: SchemaDiff

@dataclass(kw_only=True)
class ShowRowDiff(BaseConsoleEvent):
    row_diff: RowDiff
    show_sample: bool = True
    skip_grain_check: bool = False

@dataclass(kw_only=True)
class ConsoleException(BaseConsoleEvent):
    exception: Exception

@dataclass(kw_only=True)
class PrintEnvironments(BaseConsoleEvent):
    environments_summary: dict[str, int]

@dataclass(kw_only=True)
class ShowTableDiffSummary(BaseConsoleEvent):
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
    #| StartPromotionProgress
    | UpdatePromotionProgress
    | StopPromotionProgress
    | UpdateSnapshotMigrationProgress
    | LogMigrationStatus
    | StopSnapshotMigrationProgress
    | StartEnvMigrationProgress
    | UpdateEnvMigrationProgress
    | StopEnvMigrationProgress
    | ShowModelDifferenceSummary
    | Plan
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

ConsoleEventHandler = t.Callable[[ConsoleEvent], None]

SnapshotCategorizer = t.Callable[
    [Snapshot, PlanBuilder, str | None], SnapshotChangeCategory
]

T = t.TypeVar("T")

def get_console_event_by_name(
    event_name: str,
) -> type[ConsoleEvent] | None:
    """Get the console event class by name."""
    known_events_classes = t.get_args(ConsoleEvent)
    console_event_map: dict[str, type[ConsoleEvent]] = {
        event.__name__: event for event in known_events_classes
    }
    return console_event_map.get(event_name)

class IntrospectingConsole(Console):
    """An event console that dynamically implements methods based on the current
    sqlmesh console object. If a method is specified it's validated against the
    current sqlmesh version's implementation"""

    events: t.ClassVar[list[type[ConsoleEvent]]]

    def __init_subclass__(cls):
        super().__init_subclass__()

        known_events_classes = cls.events
        known_events: list[str] = []
        for known_event in known_events_classes:
            assert inspect.isclass(known_event), "event must be a class"
            known_events.append(known_event.__name__)
            

        # Iterate through all the available abstract methods in console
        for method_name in Console.__abstractmethods__:
            # Check if the method is not already implemented
            if hasattr(cls, method_name):
                if not getattr(getattr(cls, method_name), '__isabstractmethod__', False):
                    logger.debug(f"Skipping {method_name} as it is abstract")
                    continue
            logger.debug(f"Checking {method_name}")

            # if the method doesn't exist we automatically create a method by
            # inspecting the method's arguments. Anything that matches "known"
            # events has it's values checked. The dataclass should define the
            # required fields and everything else should be sent to a catchall
            # argument in the dataclass for the event
            
            # Convert method name from snake_case to camel case
            camel_case_method_name = "".join(
                word.capitalize()
                for i, word in enumerate(method_name.split("_"))
            )

            if camel_case_method_name in known_events:
                logger.debug(f"Creating {method_name} for {camel_case_method_name}")
                signature = inspect.signature(getattr(Console, method_name))
                handler = cls.create_event_handler(method_name, camel_case_method_name, signature)
                setattr(cls, method_name, handler)
            else:
                logger.debug(f"Creating {method_name} for unknown event")
                signature = inspect.signature(getattr(Console, method_name))
                handler = cls.create_unknown_event_handler(method_name, signature)
                setattr(cls, method_name, handler)

    @classmethod
    def create_event_handler(cls, method_name: str, event_name: str, signature: inspect.Signature):
        func_signature, call_params = cls.create_signatures_and_params(signature)

        event_handler_str = textwrap.dedent(f"""
        def {method_name}({", ".join(func_signature)}):
            self.publish_known_event('{event_name}', {", ".join(call_params)})
        """)
        exec(event_handler_str)
        return t.cast(t.Callable[[t.Any], t.Any], locals()[method_name])

    @classmethod
    def create_signatures_and_params(cls, signature: inspect.Signature):
        func_signature: list[str] = []
        call_params: list[str] = []
        for param_name, param in signature.parameters.items():
            if param_name == "self":
                func_signature.append("self")
                continue

            if param.default is inspect._empty:
                param_type_name = param.annotation
                if not isinstance(param_type_name, str):
                    param_type_name = param_type_name.__name__
                func_signature.append(f"{param_name}: '{param_type_name}'")
            else:
                default_value = param.default
                param_type_name = param.annotation
                if not isinstance(param_type_name, str):
                    param_type_name = param_type_name.__name__
                if isinstance(param.default, str):
                    default_value = f"'{param.default}'"
                func_signature.append(f"{param_name}: '{param_type_name}' = {default_value}")
            call_params.append(f"{param_name}={param_name}")
        return (func_signature, call_params)

    @classmethod
    def create_unknown_event_handler(cls, method_name: str, signature: inspect.Signature):
        func_signature, call_params = cls.create_signatures_and_params(signature)

        event_handler_str = textwrap.dedent(f"""
        def {method_name}({", ".join(func_signature)}):
            self.publish_unknown_event('{method_name}', {", ".join(call_params)})
        """)
        exec(event_handler_str)
        return t.cast(t.Callable[[t.Any], t.Any], locals()[method_name])

    def __init__(self, log_override: logging.Logger | None = None) -> None:
        self._handlers: dict[str, ConsoleEventHandler] = {}
        self.logger = log_override or logger
        self.id = str(uuid.uuid4())
        self.logger.debug(f"EventConsole[{self.id}]: created")
        self.categorizer = None

    def publish_known_event(self, event_name: str, **kwargs: t.Any) -> None:
        console_event = get_console_event_by_name(event_name)
        assert console_event is not None, f"Event {event_name} not found"
        
        expected_kwargs_fields = console_event.__dataclass_fields__
        expected_kwargs: dict[str, t.Any] = {}
        unknown_args: dict[str, t.Any] = {}
        for key, value in kwargs.items():
            if key not in expected_kwargs_fields:
                unknown_args[key] = value
            else:
                expected_kwargs[key] = value
        
        event = console_event(**expected_kwargs, unknown_args=unknown_args)

        self.publish(event)

    def publish(self, event: ConsoleEvent) -> None:
        self.logger.debug(
            f"EventConsole[{self.id}]: sending event {event.__class__.__name__} to {len(self._handlers)}"
        )
        for handler in self._handlers.values():
            handler(event)

    def publish_unknown_event(self, event_name: str, **kwargs: t.Any) -> None:
        self.logger.debug(
            f"EventConsole[{self.id}]: sending unknown '{event_name}' event to {len(self._handlers)} handlers"
        )
        self.logger.debug(f"EventConsole[{self.id}]: unknown event {event_name} {kwargs}")

    def add_handler(self, handler: ConsoleEventHandler) -> str:
        handler_id = str(uuid.uuid4())
        self.logger.debug(f"EventConsole[{self.id}]: Adding handler {handler_id}")
        self._handlers[handler_id] = handler
        return handler_id

    def remove_handler(self, handler_id: str) -> None:
        del self._handlers[handler_id]
                 

class EventConsole(IntrospectingConsole):
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

    events: t.ClassVar[list[type[ConsoleEvent]]] = [
        Plan,
        StartPlanEvaluation,
        StopPlanEvaluation,
        StartEvaluationProgress,
        StopEvaluationProgress,
        UpdatePromotionProgress,
        StopPromotionProgress,
        StartSnapshotEvaluationProgress,
        UpdateSnapshotEvaluationProgress,
        LogError,
        LogWarning,
        LogSuccess,
        LogFailedModels,
        LogSkippedModels,
        LogTestResults,
        ConsoleException,
        PrintEnvironments,
        ShowTableDiffSummary,
    ]

    def exception(self, exc: Exception) -> None:
        self.publish(ConsoleException(exception=exc))

    def add_snapshot_categorizer(
        self, categorizer: SnapshotCategorizer
    ) -> None:
        self.categorizer = categorizer


class DebugEventConsole(EventConsole):
    """A console that wraps an existing console and logs all events to a logger"""

    def __init__(self, console: Console):
        super().__init__()
        self._console = console
