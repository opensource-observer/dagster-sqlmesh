import logging
import typing as t
from dataclasses import dataclass

import duckdb
import polars
from sqlmesh import Context
from sqlmesh.core.config import (
    Config as SQLMeshConfig,
    DuckDBConnectionConfig,
    GatewayConfig,
    ModelDefaultsConfig,
)
from sqlmesh.utils.date import TimeLike

from dagster_sqlmesh.config import SQLMeshContextConfig
from dagster_sqlmesh.controller.base import PlanOptions, RunOptions
from dagster_sqlmesh.controller.dagster import DagsterSQLMeshController
from dagster_sqlmesh.events import ConsoleRecorder
from dagster_sqlmesh.resource import DagsterSQLMeshEventHandler, SQLMeshResource

logger = logging.getLogger(__name__)


def setup_testing_sqlmesh_context_config(*, db_path: str, project_path: str, variables: dict[str, t.Any] | None = None) -> SQLMeshContextConfig:
    config = SQLMeshConfig(
        gateways={
            "local": GatewayConfig(connection=DuckDBConnectionConfig(database=db_path)),
        },
        default_gateway="local",
        model_defaults=ModelDefaultsConfig(dialect="duckdb"),
        variables=variables or {},
    )
    config_as_dict = config.dict()
    context_config = SQLMeshContextConfig(
        path=project_path, gateway="local", config_override=config_as_dict
    )
    return context_config

def setup_testing_sqlmesh_test_context(
    *,
    db_path: str,
    project_path: str,
    variables: dict[str, t.Any] | None = None,
) -> "SQLMeshTestContext":
    context_config = setup_testing_sqlmesh_context_config(
        db_path=db_path, project_path=project_path, variables=variables
    )
    return SQLMeshTestContext(db_path=db_path, context_config=context_config)


class TestSQLMeshResource(SQLMeshResource):
    """A test SQLMesh resource that can be used in tests.

    This resource is a subclass of SQLMeshResource and is used to run SQLMesh in tests.
    It allows for easy setup and teardown of the SQLMesh context.
    """

    def __init__(self, config: SQLMeshContextConfig, is_testing: bool = False):
        super().__init__(config=config, is_testing=is_testing)
        def default_event_handler_factory(*args: t.Any, **kwargs: t.Any) -> DagsterSQLMeshEventHandler:
            """Default event handler factory for the SQLMesh resource."""
            return DagsterSQLMeshEventHandler(*args, **kwargs)
        self._event_handler_factory = default_event_handler_factory

    def set_event_handler_factory(self, event_handler_factory: t.Callable[..., DagsterSQLMeshEventHandler]) -> None:
        """Set the event handler for the SQLMesh resource.

        Args:
            event_handler (DagsterSQLMeshEventHandler): The event handler to set.
        """
        self._event_handler_factory = event_handler_factory

    def create_event_handler(self, *args: t.Any, **kwargs: t.Any) -> DagsterSQLMeshEventHandler:
        """Create a new event handler for the SQLMesh resource.

        Args:
            *args: Positional arguments to pass to the event handler.
            **kwargs: Keyword arguments to pass to the event handler.

        Returns:
            DagsterSQLMeshEventHandler: The created event handler.
        """
        return self._event_handler_factory(*args, **kwargs)


@dataclass
class SQLMeshTestContext:
    """A test context for running SQLMesh"""

    db_path: str
    context_config: SQLMeshContextConfig

    def create_controller(self) -> DagsterSQLMeshController[Context]:
        return DagsterSQLMeshController.setup_with_config(
            config=self.context_config, 
        )

    def create_resource(self) -> TestSQLMeshResource:
        return TestSQLMeshResource(
            config=self.context_config, is_testing=True,
        )

    def query(self, *args: t.Any, **kwargs: t.Any) -> list[t.Any]:
        conn = duckdb.connect(self.db_path)
        return conn.sql(*args, **kwargs).fetchall()

    def initialize_test_source(self) -> None:
        conn = duckdb.connect(self.db_path)
        conn.sql(
            """
        CREATE SCHEMA sources;
        """
        )
        conn.sql(
            """
        CREATE TABLE sources.test_source (id INTEGER, name VARCHAR);
        """
        )
        conn.sql(
            """
        INSERT INTO sources.test_source (id, name)
        VALUES (1, 'abc'), (2, 'def');
        """
        )
        conn.close()

    def append_to_test_source(self, df: polars.DataFrame):
        logger.debug("appending data to the test source")
        conn = duckdb.connect(self.db_path)
        conn.sql(
            """
        INSERT INTO sources.test_source 
        SELECT * FROM df 
        """
        )

    def plan_and_run(
        self,
        *,
        environment: str,
        execution_time: TimeLike | None = None,
        start: TimeLike | None = None,
        end: TimeLike | None = None,
        select_models: list[str] | None = None,
        restate_selected: bool = False,
        skip_run: bool = False,
    ):
        """Runs plan and run on SQLMesh with the given configuration and record all of the generated events.

        Args:
            environment (str): The environment to run SQLMesh in.
            execution_time (TimeLike, optional): The execution timestamp for the run. Defaults to None.
            enable_debug_console (bool, optional): Flag to enable debug console. Defaults to False.
            start (TimeLike, optional): Start time for the run interval. Defaults to None.
            end (TimeLike, optional): End time for the run interval. Defaults to None.
            restate_models (List[str], optional): List of models to restate. Defaults to None.

        Returns:
            None: The function records events to a debug console but doesn't return anything.

        Note:
            TimeLike can be any time-like object that SQLMesh accepts (datetime, str, etc.).
            The function creates a controller and recorder to capture all SQLMesh events during execution.
        """
        controller = self.create_controller()
        recorder = ConsoleRecorder()
        # controller.add_event_handler(ConsoleRecorder())
        plan_options = PlanOptions(
            enable_preview=True,
        )
        run_options = RunOptions()
        if execution_time:
            plan_options["execution_time"] = execution_time
            run_options["execution_time"] = execution_time

        for event in controller.plan_and_run(
            environment,
            start=start,
            end=end,
            select_models=select_models,
            restate_selected=restate_selected,
            plan_options=plan_options,
            run_options=run_options,
            skip_run=skip_run,
        ):
            recorder(event)
