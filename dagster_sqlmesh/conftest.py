import logging
import os
import shutil
import sys
import tempfile
import typing as t

import pytest
from sqlmesh.core.config import (
    Config as SQLMeshConfig,
    DuckDBConnectionConfig,
    GatewayConfig,
    ModelDefaultsConfig,
)

from dagster_sqlmesh.config import SQLMeshContextConfig
from dagster_sqlmesh.testing import SQLMeshTestContext

logger = logging.getLogger(__name__)


@pytest.fixture(scope="session", autouse=True)
def setup_debug_logging_for_tests() -> None:
    root_logger = logging.getLogger(__name__.split(".")[0])
    root_logger.setLevel(logging.DEBUG)

    logging.basicConfig(stream=sys.stdout, level=logging.DEBUG)


@pytest.fixture
def sample_sqlmesh_project() -> t.Iterator[str]:
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


@pytest.fixture
def sample_sqlmesh_test_context(
    sample_sqlmesh_project: str,
) -> t.Iterator[SQLMeshTestContext]:
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
