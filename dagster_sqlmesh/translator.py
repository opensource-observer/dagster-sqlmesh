from collections.abc import Sequence

from dagster import AssetKey
from sqlglot import exp
from sqlmesh.core.context import Context
from sqlmesh.core.model import Model


class SQLMeshDagsterTranslator:
    """Translates sqlmesh objects for dagster"""

    def get_asset_key(self, context: Context, fqn: str) -> AssetKey:
        """Given the sqlmesh context and a model return the asset key"""
        path = self.get_asset_key_name(fqn)
        return AssetKey(path)

    def get_asset_key_name(self, fqn: str) -> Sequence[str]:
        table = exp.to_table(fqn)
        asset_key_name = [table.catalog, table.db, table.name]

        return asset_key_name
    
    def get_group_name(self, context: Context, model: Model) -> str:
        path = self.get_asset_key_name(model.fqn)
        return path[-2]

    def _get_context_dialect(self, context: Context) -> str:
        return context.engine_adapter.dialect

    def get_tags(self, context: Context, model: Model) -> dict[str, str]:
        """Given the sqlmesh context and a model return the tags for that model"""
        return {k: "true" for k in model.tags}
