import logging

from dagster import AssetDep, AssetKey, AssetOut
from dagster._core.definitions.asset_dep import CoercibleToAssetDep

from ..translator import SQLMeshDagsterTranslator
from ..types import SQLMeshModelDep, SQLMeshMultiAssetOptions
from ..utils import sqlmesh_model_name_to_key
from .base import SQLMeshController

logger = logging.getLogger(__name__)


class DagsterSQLMeshController(SQLMeshController):
    """An extension of the sqlmesh controller specifically for dagster use"""

    def to_asset_outs(
        self, environment: str, translator: SQLMeshDagsterTranslator
    ) -> SQLMeshMultiAssetOptions:
        with self.instance(environment, "to_asset_outs") as instance:
            context = instance.context
            output = SQLMeshMultiAssetOptions()
            depsMap: dict[str, CoercibleToAssetDep] = {}

            for model, deps in instance.non_external_models_dag():
                asset_key = translator.get_asset_key_from_model(
                    context,
                    model,
                )
                model_deps = [
                    SQLMeshModelDep(fqn=dep, model=context.get_model(dep))
                    for dep in deps
                ]
                internal_asset_deps: set[AssetKey] = set()
                asset_tags = translator.get_tags(context, model)

                for dep in model_deps:
                    if dep.model:
                        internal_asset_deps.add(
                            translator.get_asset_key_from_model(context, dep.model)
                        )
                    else:
                        table = translator.get_fqn_to_table(context, dep.fqn)
                        key = translator.get_asset_key_fqn(context, dep.fqn)
                        internal_asset_deps.add(key)
                        # create an external dep
                        depsMap[table.name] = AssetDep(key)
                model_key = sqlmesh_model_name_to_key(model.name)
                output.outs[model_key] = AssetOut(
                    key=asset_key, tags=asset_tags, is_required=False
                )
                output.internal_asset_deps[model_key] = internal_asset_deps

            output.deps = list(depsMap.values())
            return output
