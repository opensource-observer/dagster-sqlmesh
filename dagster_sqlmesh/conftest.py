import logging
import os
import shutil
import sys
import tempfile
import typing as t

import pytest

from dagster_sqlmesh.config import SQLMeshContextConfig
from dagster_sqlmesh.testing import (
    SQLMeshTestContext,
    setup_testing_sqlmesh_context_config,
)

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
def sample_sqlmesh_db_path(sample_sqlmesh_project: str) -> t.Iterator[str]:
    db_path = os.path.join(sample_sqlmesh_project, "db.db")
    yield db_path

@pytest.fixture
def sample_sqlmesh_test_context_config(sample_sqlmesh_project: str, sample_sqlmesh_db_path: str) -> t.Iterator[SQLMeshContextConfig]:
    yield setup_testing_sqlmesh_context_config(db_path=sample_sqlmesh_db_path, project_path=sample_sqlmesh_project)

@pytest.fixture
def sample_sqlmesh_test_context(
    sample_sqlmesh_project: str, sample_sqlmesh_test_context_config: SQLMeshContextConfig, sample_sqlmesh_db_path: str
) -> t.Iterator[SQLMeshTestContext]:
    test_context = SQLMeshTestContext(db_path=sample_sqlmesh_db_path, context_config=sample_sqlmesh_test_context_config)
    test_context.initialize_test_source()
    yield test_context
