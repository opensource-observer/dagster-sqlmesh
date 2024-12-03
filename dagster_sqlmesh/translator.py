import sqlglot
from sqlglot import exp
from sqlmesh.core.context import Context
from sqlmesh.core.model import Model
from dagster import AssetKey


class SQLMeshDagsterTranslator:
    """Translates sqlmesh objects for dagster"""

    def get_asset_key_from_model(self, context: Context, model: Model) -> AssetKey:
        return AssetKey(model.view_name)

    def get_asset_key_fqn(self, context: Context, fqn: str) -> AssetKey:
        table = self.get_fqn_to_table(context, fqn)
        return AssetKey(table.name)

    def get_fqn_to_table(self, context: Context, fqn: str) -> exp.Table:
        dialect = self.get_context_dialect(context)
        return sqlglot.to_table(fqn, dialect=dialect)

    def get_context_dialect(self, context: Context) -> str:
        return context.engine_adapter.dialect

    # def get_asset_deps(
    #     self, context: Context, model: Model, deps: List[SQLMeshModelDep]
    # ) -> List[AssetKey]:
    #     asset_keys: List[AssetKey] = []
    #     for dep in deps:
    #         if dep.model:
    #             asset_keys.append(AssetKey(dep.model.view_name))
    #         else:
    #             parsed_fqn = dep.parse_fqn()
    #             asset_keys.append(AssetKey([parsed_fqn.view_name]))
    #     return asset_keys
