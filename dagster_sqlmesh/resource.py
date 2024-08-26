from dagster import ConfigurableResource, AssetExecutionContext

from .config import SQLMeshContextConfig


class SQLMeshResource(ConfigurableResource):
    config: SQLMeshContextConfig

    def run(self, context: AssetExecutionContext):
        """Execute SQLMesh based on the configuration given"""
        pass

    def plan(self, context: AssetExecutionContext):
        pass
