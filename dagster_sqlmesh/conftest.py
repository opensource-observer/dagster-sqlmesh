import logging
import sys
import tempfile
import shutil
import os
from dataclasses import dataclass
import typing as t

import pytest
import duckdb
import polars
from sqlmesh.utils.date import TimeLike
from sqlmesh.core.console import get_console
from sqlmesh.core.config import (
    Config as SQLMeshConfig,
    GatewayConfig,
    DuckDBConnectionConfig,
    ModelDefaultsConfig,
)

from dagster_sqlmesh.config import SQLMeshContextConfig
from dagster_sqlmesh.events import ConsoleRecorder
from dagster_sqlmesh.controller.base import PlanOptions, RunOptions
from dagster_sqlmesh.controller.dagster import DagsterSQLMeshController

logger = logging.getLogger(__name__)


@pytest.fixture(scope="session", autouse=True)
def setup_debug_logging_for_tests():
    root_logger = logging.getLogger(__name__.split(".")[0])
    root_logger.setLevel(logging.DEBUG)

    logging.basicConfig(stream=sys.stdout, level=logging.DEBUG)


@pytest.fixture
def sample_sqlmesh_project():
    """Creates a temporary sqlmesh project by copying the sample project"""
    with tempfile.TemporaryDirectory() as tmp_dir:
        project_dir = shutil.copytree(
            "sample/sqlmesh_project", os.path.join(tmp_dir, "project")
        )
        db_path = os.path.join(project_dir, "db.db")
        if os.path.exists(db_path):
            os.remove(os.path.join(project_dir, "db.db"))

        # Initialize the "source" data
        yield str(project_dir)


@dataclass
class SQLMeshTestContext:
    """A test context for running SQLMesh"""

    db_path: str
    context_config: SQLMeshContextConfig

    def create_controller(self, enable_debug_console: bool = False):
        console = None
        if enable_debug_console:
            console = get_console()
        return DagsterSQLMeshController.setup_with_config(
            self.context_config, debug_console=console
        )

    def query(self, *args, **kwargs):
        conn = duckdb.connect(self.db_path)
        return conn.sql(*args, **kwargs).fetchall()

    def initialize_test_source(self):
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
        execution_time: t.Optional[TimeLike] = None,
        enable_debug_console: bool = False,
        start: t.Optional[TimeLike] = None,
        end: t.Optional[TimeLike] = None,
        restate_models: t.Optional[t.List[str]] = None,
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
        controller = self.create_controller(enable_debug_console=enable_debug_console)
        recorder = ConsoleRecorder()
        # controller.add_event_handler(ConsoleRecorder())
        plan_options = PlanOptions(
            enable_preview=True,
        )
        run_options = RunOptions()
        if execution_time:
            plan_options["execution_time"] = execution_time
            run_options["execution_time"] = execution_time
        if restate_models:
            plan_options["restate_models"] = restate_models
        if start:
            plan_options["start"] = start
            run_options["start"] = start
        if end:
            plan_options["end"] = end
            run_options["end"] = end

        for event in controller.plan_and_run(
            environment,
            plan_options=plan_options,
            run_options=run_options,
        ):
            recorder(event)


@pytest.fixture
def sample_sqlmesh_test_context(sample_sqlmesh_project: str):
    db_path = os.path.join(sample_sqlmesh_project, "db.db")
    config = SQLMeshConfig(
        gateways={
            "local": GatewayConfig(connection=DuckDBConnectionConfig(database=db_path)),
        },
        default_gateway="local",
        model_defaults=ModelDefaultsConfig(dialect="duckdb"),
    )
    config_as_dict = config.dict()
    context_config = SQLMeshContextConfig(
        path=sample_sqlmesh_project, gateway="local", config_override=config_as_dict
    )
    test_context = SQLMeshTestContext(db_path=db_path, context_config=context_config)
    test_context.initialize_test_source()
    yield test_context
