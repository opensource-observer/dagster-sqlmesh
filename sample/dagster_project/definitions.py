import os

from dagster import (
    load_assets_from_modules,
    Definitions,
)

from dagster_sqlmesh import sqlmesh_asset, SQLMeshResource, SQLMeshContextConfig

CURR_DIR = os.path.dirname(__file__)
SQLMESH_PROJECT_PATH = os.path.abspath(os.path.join(CURR_DIR, "../sqlmesh_project"))


@sqlmesh_asset(config=SQLMeshContextConfig(path=SQLMESH_PROJECT_PATH, gateway="local"))
def sqlmesh_project():
    pass


defs = Definitions(
    assets=[sqlmesh_project],
)
