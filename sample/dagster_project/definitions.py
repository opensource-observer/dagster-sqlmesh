import os

import time
from dagster import (
    MaterializeResult,
    asset,
    AssetExecutionContext,
    Definitions,
)
from dagster_duckdb_polars import DuckDBPolarsIOManager
import polars as pl

from dagster_sqlmesh import sqlmesh_assets, SQLMeshContextConfig, SQLMeshResource

CURR_DIR = os.path.dirname(__file__)
SQLMESH_PROJECT_PATH = os.path.abspath(os.path.join(CURR_DIR, "../sqlmesh_project"))
DUCKDB_PATH = os.path.join(CURR_DIR, "../../db.db")

sqlmesh_config = SQLMeshContextConfig(path=SQLMESH_PROJECT_PATH, gateway="local")


@asset
def reset_asset() -> MaterializeResult:
    """An asset used for testing this entire workflow. If the duckdb database is
    found, this will delete it. This allows us to continously test this dag if
    this specific asset is materialized
    """
    deleted = False
    if os.path.exists(DUCKDB_PATH):
        os.remove(DUCKDB_PATH)
        deleted = True
    return MaterializeResult(metadata={"deleted": deleted})


@asset(deps=[reset_asset])
def test_source() -> pl.DataFrame:
    """Sets up the `test_source` table in duckdb that one of the sample sqlmesh
    models depends on"""
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


@sqlmesh_assets(config=sqlmesh_config)
def sqlmesh_project(context: AssetExecutionContext, sqlmesh: SQLMeshResource):
    yield from sqlmesh.run(context)


defs = Definitions(
    assets=[sqlmesh_project, test_source, reset_asset],
    resources={
        "sqlmesh": SQLMeshResource(config=sqlmesh_config),
        "io_manager": DuckDBPolarsIOManager(
            database=DUCKDB_PATH,
            schema="sources",
        ),
    },
)
