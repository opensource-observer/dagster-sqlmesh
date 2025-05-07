# pyright: reportPrivateImportUsage=false
import logging
from inspect import signature

from dagster import AssetDep, AssetKey, AssetOut
from dagster._core.definitions.asset_dep import CoercibleToAssetDep

from dagster_sqlmesh.controller.base import ContextCls, SQLMeshController
from dagster_sqlmesh.translator import SQLMeshDagsterTranslator
from dagster_sqlmesh.types import SQLMeshModelDep, SQLMeshMultiAssetOptions
from dagster_sqlmesh.utils import get_asset_key_str

logger = logging.getLogger(__name__)


class DagsterSQLMeshController(SQLMeshController[ContextCls]):
    """An extension of the sqlmesh controller specifically for dagster use"""

    def to_asset_outs(
        self, environment: str, translator: SQLMeshDagsterTranslator,
    ) -> SQLMeshMultiAssetOptions:
        with self.instance(environment, "to_asset_outs") as instance:
            context = instance.context
            output = SQLMeshMultiAssetOptions()
            depsMap: dict[str, CoercibleToAssetDep] = {}

            for model, deps in instance.non_external_models_dag():
                asset_key = translator.get_asset_key(context=context, fqn=model.fqn)
                model_deps = [
                    SQLMeshModelDep(fqn=dep, model=context.get_model(dep))
                    for dep in deps
                ]
                internal_asset_deps: set[AssetKey] = set()
                asset_tags = translator.get_tags(context, model)

                for dep in model_deps:
                    if dep.model:
                        internal_asset_deps.add(
                            translator.get_asset_key(context, dep.model.fqn)
                        )
                    else:
                        table = get_asset_key_str(dep.fqn)
                        key = translator.get_asset_key(context, dep.fqn)
                        internal_asset_deps.add(key)
                        # create an external dep
                        depsMap[table] = AssetDep(key)
                model_key = get_asset_key_str(model.fqn)
                # If current Dagster supports "kinds", add labels for Dagster UI
                if "kinds" in signature(AssetOut).parameters:
                    output.outs[model_key] = AssetOut(
                        key=asset_key, tags=asset_tags, is_required=False,
                        group_name=translator.get_group_name(context, model),
                        kinds={"sqlmesh", translator._get_context_dialect(context).lower()}
                    )
                else:
                    output.outs[model_key] = AssetOut(
                        key=asset_key, tags=asset_tags, is_required=False,
                        group_name=translator.get_group_name(context, model)
                    )
                output.internal_asset_deps[model_key] = internal_asset_deps

            output.deps = list(depsMap.values())
            return output
