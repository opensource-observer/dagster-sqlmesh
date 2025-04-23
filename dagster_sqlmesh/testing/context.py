import logging
import typing as t
from dataclasses import dataclass

import duckdb
import polars
from sqlmesh import Context
from sqlmesh.utils.date import TimeLike

from dagster_sqlmesh.config import SQLMeshContextConfig
from dagster_sqlmesh.controller.base import PlanOptions, RunOptions
from dagster_sqlmesh.controller.dagster import DagsterSQLMeshController
from dagster_sqlmesh.events import ConsoleRecorder

logger = logging.getLogger(__name__)


@dataclass
class SQLMeshTestContext:
    """A test context for running SQLMesh"""

    db_path: str
    context_config: SQLMeshContextConfig

    def create_controller(self) -> DagsterSQLMeshController[Context]:
        return DagsterSQLMeshController.setup_with_config(
            config=self.context_config, 
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
