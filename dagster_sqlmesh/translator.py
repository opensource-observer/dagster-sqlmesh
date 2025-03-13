import typing as t
import sqlglot
from sqlglot import exp
from sqlmesh.core.context import Context
from sqlmesh.core.model import Model
from dagster import AssetKey


class SQLMeshDagsterTranslator:
    """Translates sqlmesh objects for dagster"""

    def get_asset_key_from_model(self, context: Context, model: Model) -> AssetKey:
        """Given the sqlmesh context and a model return the asset key"""
        return AssetKey(model.view_name)

    def get_asset_key_fqn(self, context: Context, fqn: str) -> AssetKey:
        """Given the sqlmesh context and a fqn of a model return an asset key"""
        table = self.get_fqn_to_table(context, fqn)
        return AssetKey(table.name)

    def get_fqn_to_table(self, context: Context, fqn: str) -> exp.Table:
        """Given the sqlmesh context and a fqn return the table"""
        dialect = self._get_context_dialect(context)
        return sqlglot.to_table(fqn, dialect=dialect)

    def _get_context_dialect(self, context: Context) -> str:
        return context.engine_adapter.dialect

    def get_tags(self, context: Context, model: Model) -> t.Dict[str, str]:
        """Given the sqlmesh context and a model return the tags for that model"""
        return {k: "true" for k in model.tags}
