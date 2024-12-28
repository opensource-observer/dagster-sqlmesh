import logging
import typing as t

from dagster._core.definitions.asset_dep import CoercibleToAssetDep
from dagster import (
    AssetDep,
    AssetOut,
    AssetKey,
)

from ..utils import sqlmesh_model_name_to_key
from .base import SQLMeshController
from ..translator import SQLMeshDagsterTranslator
from ..types import SQLMeshMultiAssetOptions, SQLMeshModelDep

logger = logging.getLogger(__name__)


class DagsterSQLMeshController(SQLMeshController):
    """An extension of the sqlmesh controller specifically for dagster use"""

    def to_asset_outs(
        self, environment: str, translator: SQLMeshDagsterTranslator
    ) -> SQLMeshMultiAssetOptions:
        with self.instance(environment, "to_asset_outs") as instance:
            context = instance.context
            dag = context.dag
            output = SQLMeshMultiAssetOptions()
            depsMap: t.Dict[str, CoercibleToAssetDep] = {}

            for model_fqn, deps in dag.graph.items():
                logger.debug(f"model found: {model_fqn}")
                model = context.get_model(model_fqn)
                if not model:
                    # If no model is returned this seems to be an asset dependency
                    continue
                asset_out = translator.get_asset_key_from_model(
                    context,
                    model,
                )
                model_deps = [
                    SQLMeshModelDep(fqn=dep, model=context.get_model(dep))
                    for dep in deps
                ]
                internal_asset_deps: t.Set[AssetKey] = set()
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
                output.outs[model_key] = AssetOut(key=asset_out, is_required=False)
                output.internal_asset_deps[model_key] = internal_asset_deps

            output.deps = list(depsMap.values())
            return output
