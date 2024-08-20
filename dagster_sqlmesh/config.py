from typing import Optional

from dagster import Config
from sqlmesh.core.config import Config as MeshConfig
from pydantic import Field


class SQLMeshContextConfig(Config):
    """A very basic sqlmesh configuration. Currently you cannot specify the
    sqlmesh configuration entirely from dagster. It is intended that your
    sqlmesh project define all the configuration in it's own directory which
    also ensures that configuration is consistent if running sqlmesh locally vs
    running via dagster.
    """

    path: str
    gateway: str
    sqlmesh_config: Optional[MeshConfig] = Field(default_factory=lambda: None)
