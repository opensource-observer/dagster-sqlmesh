import os
import time
import typing as t

import polars as pl
from dagster import (
    AssetExecutionContext,
    AssetKey,
    Definitions,
    MaterializeResult,
    ResourceParam,
    asset,
    define_asset_job,
)
from dagster_duckdb_polars import DuckDBPolarsIOManager
from sqlglot import exp

from dagster_sqlmesh import (
    Context,
    SQLMeshContextConfig,
    SQLMeshDagsterTranslator,
    SQLMeshResource,
    sqlmesh_assets,
)

CURR_DIR = os.path.dirname(__file__)
SQLMESH_PROJECT_PATH = os.path.abspath(os.path.join(CURR_DIR, "../sqlmesh_project"))
SQLMESH_CACHE_PATH = os.path.join(SQLMESH_PROJECT_PATH, ".cache")
DUCKDB_PATH = os.path.join(CURR_DIR, "../../db.db")

class CustomSQLMeshContextConfig(SQLMeshContextConfig):
    custom_key: str

    def get_translator(self):
        return RewrittenSQLMeshTranslator(self.custom_key)

sqlmesh_config = CustomSQLMeshContextConfig(
    path=SQLMESH_PROJECT_PATH, 
    gateway="local",
    custom_key="custom_sqlmesh_prefix"
)

class RewrittenSQLMeshTranslator(SQLMeshDagsterTranslator):
    """A contrived SQLMeshDagsterTranslator that flattens the catalog of the
    sqlmesh project and only uses the table db and name

    We include this as a test of the translator functionality.
    """

    def __init__(self, custom_key: str):
        self.custom_key = custom_key

    def get_asset_key(self, context: Context, fqn: str) -> AssetKey:
        table = exp.to_table(fqn)  # Ensure fqn is a valid table expression
        if table.db == "sqlmesh_example":
            # For the sqlmesh_example project, we use a custom key
            return AssetKey([self.custom_key, table.name])
        return AssetKey([table.db, table.name])

    def get_group_name(self, context, model):
        return self.custom_key


@asset(key=["sources", "reset_asset"])
def reset_asset() -> MaterializeResult[t.Any]:
    """An asset used for testing this entire workflow. If the duckdb database is
    found, this will delete it. This allows us to continously test this dag if
    this specific asset is materialized
    """
    deleted = False
    if os.path.exists(DUCKDB_PATH):
        os.remove(DUCKDB_PATH)
        deleted = True
    return MaterializeResult(metadata={"deleted": deleted})


@asset(deps=[reset_asset], key=["sources", "test_source"])
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


@asset(deps=[AssetKey(["custom_sqlmesh_prefix", "full_model"])])
def post_full_model() -> pl.DataFrame:
    """An asset that depends on the `full_model` asset from the sqlmesh project.
    This is used to test that the sqlmesh assets are correctly materialized and
    can be used in other assets.
    """
    import duckdb

    conn = duckdb.connect(DUCKDB_PATH)
    df = conn.query(
        """
        SELECT * FROM sqlmesh_example__dev.full_model
    """
    ).to_df()
    conn.close()
    return pl.from_dataframe(df)


@sqlmesh_assets(
    environment="dev",
    config=sqlmesh_config,
    enabled_subsetting=True,
)
def sqlmesh_project(
    context: AssetExecutionContext, sqlmesh: SQLMeshResource, sqlmesh_config: ResourceParam[SQLMeshContextConfig]
) -> t.Iterator[MaterializeResult[t.Any]]:
    yield from sqlmesh.run(context, config=sqlmesh_config)


all_assets_job = define_asset_job(name="all_assets_job")

defs = Definitions(
    assets=[sqlmesh_project, test_source, reset_asset, post_full_model],
    resources={
        "sqlmesh": SQLMeshResource(),
        "io_manager": DuckDBPolarsIOManager(
            database=DUCKDB_PATH,
            schema="sources",
        ),
        "sqlmesh_config": sqlmesh_config,
    },
    jobs=[all_assets_job],
)
