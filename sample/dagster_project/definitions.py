import os

import time
from dagster import (
    asset,
    AssetExecutionContext,
    Definitions,
)
from dagster_duckdb_polars import DuckDBPolarsIOManager
import polars as pl

from dagster_sqlmesh import sqlmesh_asset, SQLMeshContextConfig, SQLMeshResource
from dagster_sqlmesh.asset import SQLMeshDagsterTranslator

CURR_DIR = os.path.dirname(__file__)
SQLMESH_PROJECT_PATH = os.path.abspath(os.path.join(CURR_DIR, "../sqlmesh_project"))

sqlmesh_config = SQLMeshContextConfig(path=SQLMESH_PROJECT_PATH, gateway="local")


@asset()
def test_source() -> pl.DataFrame:
    return pl.from_dicts(
        [
            {
                "id": time.time() + 1,
                "name": "abc",
            },
            {
                "id": time.time() + 2,
                "name": "def",
            },
        ]
    )


@sqlmesh_asset(config=sqlmesh_config)
def sqlmesh_project(context: AssetExecutionContext, sqlmesh: SQLMeshResource):
    translator = SQLMeshDagsterTranslator()
    yield from sqlmesh.run(context, translator)


defs = Definitions(
    assets=[sqlmesh_project, test_source],
    resources={
        "sqlmesh": SQLMeshResource(config=sqlmesh_config),
        "io_manager": DuckDBPolarsIOManager(
            database=os.path.join(CURR_DIR, "../../db.db"),
            schema="sources",
        ),
    },
)
