import logging
import sys
import tempfile
import shutil
import os
from dataclasses import dataclass
from typing import cast, List, Tuple, Optional, Any, Dict

import pytest
import duckdb
import polars
from sqlmesh.utils.date import TimeLike
from sqlmesh.core.plan.builder import PlanBuilder
from sqlmesh.core.console import get_console
from sqlmesh.core.config import (
    Config as SQLMeshConfig,
    GatewayConfig,
    DuckDBConnectionConfig,
    ModelDefaultsConfig,
)

from dagster_sqlmesh.config import SQLMeshContextConfig
from dagster_sqlmesh.events import StatefulConsoleEventHandler, show_plan_summary
from dagster_sqlmesh.asset import setup_sqlmesh_controller

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
    db_path: str
    context_config: SQLMeshContextConfig

    def create_controller(self, enable_debug_console: bool = False):
        console = None
        if enable_debug_console:
            console = get_console()
        return setup_sqlmesh_controller(self.context_config, debug_console=console)

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

    def sqlmesh_plan(
        self,
        *,
        environment: str,
        apply: bool = False,
        execution_time: Optional[TimeLike] = None,
        enable_debug_console: bool = False,
        start: Optional[TimeLike] = None,
        end: Optional[TimeLike] = None,
        restate_models: Optional[List[str]] = None,
    ):
        controller = self.create_controller(enable_debug_console=enable_debug_console)
        controller.add_event_handler(StatefulConsoleEventHandler())
        plan_options: Dict[str, Any] = dict(
            environment=environment,
            enable_preview=True,
        )
        run_options: Dict[str, Any] = dict(
            environment=environment,
        )
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

        builder = cast(
            PlanBuilder,
            controller.context.plan_builder(**plan_options),
        )
        if apply:
            logger.debug("making plan")
            plan = builder.build()
            show_plan_summary(plan, lambda x: x.is_model)
            logger.debug("applying plan")
            controller.context.apply(plan)
            logger.debug("running through the scheduler")
            controller.context.run(**run_options)
        controller.context.close()


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
    context_config = SQLMeshContextConfig(
        path=sample_sqlmesh_project, gateway="local", sqlmesh_config=config
    )
    test_context = SQLMeshTestContext(db_path=db_path, context_config=context_config)
    test_context.initialize_test_source()
    yield test_context
