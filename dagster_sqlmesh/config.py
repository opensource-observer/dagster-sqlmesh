import typing as t
from dataclasses import dataclass
from pathlib import Path

from dagster import Config
from pydantic import Field
from sqlmesh.core.config import Config as MeshConfig
from sqlmesh.core.config.loader import load_configs

from dagster_sqlmesh.translator import SQLMeshDagsterTranslator


@dataclass
class ConfigOverride:
    config_as_dict: dict[str, t.Any]

    def dict(self) -> dict[str, t.Any]:
        return self.config_as_dict


class SQLMeshContextConfig(Config):
    """A very basic sqlmesh configuration. Currently you cannot specify the
    sqlmesh configuration entirely from dagster. It is intended that your
    sqlmesh project define all the configuration in it's own directory which
    also ensures that configuration is consistent if running sqlmesh locally vs
    running via dagster.
    
    The config also manages the translator class used for converting SQLMesh
    models to Dagster assets and provides a consistent translator to
    dagster-sqlmesh. The config must always be provided to the SQLMeshResource
    in order for the integration to function correctly. For example, when
    setting up the dagster Definitions, you must provide the
    SQLMeshContextConfig as a resource along with the SQLMeshResource as
    follows:

    ```python 
    sqlmesh_context_config = SQLMeshContextConfig(
        path="/path/to/sqlmesh/project", gateway="local",
    )

    @sqlmesh_assets(
        environment="dev", config=sqlmesh_context_config,
        enabled_subsetting=True,
    ) def sqlmesh_project(
        context: AssetExecutionContext, sqlmesh: SQLMeshResource,
        sqlmesh_context_config: SQLMeshContextConfig
    ) -> t.Iterator[MaterializeResult[t.Any]]:
        yield from sqlmesh.run(context, config=sqlmesh_config)
            

    defs = Definitions(
        assets=[sqlmesh_project], resources={
            "sqlmesh": SQLMeshResource(), "sqlmesh_context_config":
            sqlmesh_context_config,
        },
    )
    ```
    
    In order to provide a custom translator, you will need to subclass this
    class and return a different translator. However, due to the way that
    dagster assets/jobs/ops are run, you will need to ensure that the custom 
    translator is _instantiated_ within the get_translator method rather than
    simply returning an instance variable. This is because dagster will
    serialize/deserialize the config object and any instance variables will
    not be preserved. Therefore, any options you'd like to pass to the translator
    must be serializable within your custom SQLMeshContextConfig subclass.

    This class provides the minimum configuration required to run dagster-sqlmesh.
    """

    path: str
    gateway: str
    config_override: dict[str, t.Any] | None = Field(default_factory=lambda: None)
    
    def get_translator(self) -> SQLMeshDagsterTranslator:
        """Get a translator instance. Override this method to provide a custom translator.
        
        Returns:
            SQLMeshDagsterTranslator: A new instance of the configured translator class
            
        Raises:
            ValueError: If the imported object is not a class or does not inherit 
                       from SQLMeshDagsterTranslator
        """
        return SQLMeshDagsterTranslator()

    @property
    def sqlmesh_config(self) -> MeshConfig:
        if self.config_override:
            return MeshConfig.parse_obj(self.config_override)
        sqlmesh_path = Path(self.path)
        configs = load_configs(None, MeshConfig, [sqlmesh_path])
        if sqlmesh_path not in configs:
            raise ValueError(f"SQLMesh configuration not found at {sqlmesh_path}")
        return configs[sqlmesh_path]