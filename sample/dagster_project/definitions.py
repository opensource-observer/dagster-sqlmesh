import os
import time
import typing as t

import polars as pl
from dagster import (
    AssetExecutionContext,
    Definitions,
    MaterializeResult,
    asset,
    define_asset_job,
)
from dagster_duckdb_polars import DuckDBPolarsIOManager

from dagster_sqlmesh import SQLMeshContextConfig, SQLMeshResource, sqlmesh_assets

CURR_DIR = os.path.dirname(__file__)
SQLMESH_PROJECT_PATH = os.path.abspath(os.path.join(CURR_DIR, "../sqlmesh_project"))
DUCKDB_PATH = os.path.join(CURR_DIR, "../../db.db")

sqlmesh_config = SQLMeshContextConfig(path=SQLMESH_PROJECT_PATH, gateway="local")


@asset(key=["db", "sources", "reset_asset"])
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


@asset(deps=[reset_asset], key=["db", "sources", "test_source"])
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


@sqlmesh_assets(environment="dev", config=sqlmesh_config, enabled_subsetting=True)
def sqlmesh_project(context: AssetExecutionContext, sqlmesh: SQLMeshResource) -> t.Iterator[MaterializeResult]:
    yield from sqlmesh.run(context)


all_assets_job = define_asset_job(name="all_assets_job")

defs = Definitions(
    assets=[sqlmesh_project, test_source, reset_asset],
    resources={
        "sqlmesh": SQLMeshResource(config=sqlmesh_config),
        "io_manager": DuckDBPolarsIOManager(
            database=DUCKDB_PATH,
            schema="sources",
        ),
    },
    jobs=[all_assets_job],
)
