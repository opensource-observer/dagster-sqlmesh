from dagster import ConfigurableResource, AssetExecutionContext, AssetMaterialization

from .config import SQLMeshContextConfig


class SQLMeshResource(ConfigurableResource):
    config: SQLMeshContextConfig

    def run(self, context: AssetExecutionContext):
        """Execute SQLMesh based on the configuration given"""
        pass

    def plan(self, context: AssetExecutionContext):
        pass
