from typing import Optional, Dict, Any
from dataclasses import dataclass

from dagster import Config
from sqlmesh.core.config import Config as MeshConfig
from pydantic import Field


@dataclass
class ConfigOverride:
    config_as_dict: Dict

    def dict(self):
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
    config_override: Optional[Dict[str, Any]] = Field(default_factory=lambda: None)

    @property
    def sqlmesh_config(self):
        if self.config_override:
            return MeshConfig.parse_obj(self.config_override)
        return None
