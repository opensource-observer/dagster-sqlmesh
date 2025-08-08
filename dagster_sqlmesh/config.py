from dataclasses import dataclass
from pathlib import Path
from typing import Any

from dagster import Config
from pydantic import Field
from sqlmesh.core.config import Config as MeshConfig
from sqlmesh.core.config.loader import load_configs


@dataclass
class ConfigOverride:
    config_as_dict: dict[str, Any]

    def dict(self) -> dict[str, Any]:
        return self.config_as_dict


class SQLMeshContextConfig(Config):
    """A very basic sqlmesh configuration. Currently you cannot specify the
    sqlmesh configuration entirely from dagster. It is intended that your
    sqlmesh project define all the configuration in it's own directory which
    also ensures that configuration is consistent if running sqlmesh locally vs
    running via dagster.
    """

    path: str
    gateway: str
    config_override: dict[str, Any] | None = Field(default_factory=lambda: None)

    @property
    def sqlmesh_config(self) -> MeshConfig:
        if self.config_override:
            return MeshConfig.parse_obj(self.config_override)
        sqlmesh_path = Path(self.path)
        configs = load_configs(None, MeshConfig, [sqlmesh_path])
        if sqlmesh_path not in configs:
            raise ValueError(f"SQLMesh configuration not found at {sqlmesh_path}")
        return configs[sqlmesh_path]