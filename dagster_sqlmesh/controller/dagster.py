# pyright: reportPrivateImportUsage=false
import logging

from dagster_sqlmesh.controller.base import (
    ContextCls,
    SQLMeshController,
)
from dagster_sqlmesh.translator import SQLMeshDagsterTranslator
from dagster_sqlmesh.types import (
    ConvertibleToAssetDep,
    ConvertibleToAssetOut,
    SQLMeshModelDep,
    SQLMeshMultiAssetOptions,
)
from dagster_sqlmesh.utils import get_asset_key_str

logger = logging.getLogger(__name__)


class DagsterSQLMeshController(SQLMeshController[ContextCls]):
    """An extension of the sqlmesh controller specifically for dagster use"""

    def to_asset_outs(
        self,
        environment: str,
        translator: SQLMeshDagsterTranslator,
    ) -> SQLMeshMultiAssetOptions:
        """Loads all the asset outs of the current sqlmesh environment. If a
        cache is provided, it will be tried first to load the asset outs."""

        internal_asset_deps_map: dict[str, set[str]] = {}
        deps_map: dict[str, ConvertibleToAssetDep] = {}
        asset_outs: dict[str, ConvertibleToAssetOut] = {}

        with self.instance(environment, "to_asset_outs") as instance:
            context = instance.context

            for model, deps in instance.non_external_models_dag():
                asset_key = translator.get_asset_key(context=context, fqn=model.fqn)
                asset_key_str = asset_key.to_user_string()
                model_deps = [
                    SQLMeshModelDep(fqn=dep, model=context.get_model(dep))
                    for dep in deps
                ]
                internal_asset_deps: set[str] = set()
                asset_tags = translator.get_tags(context, model)

                for dep in model_deps:
                    if dep.model:
                        dep_asset_key_str = translator.get_asset_key(
                            context, dep.model.fqn
                        ).to_user_string()

                        internal_asset_deps.add(dep_asset_key_str)
                    else:
                        table = get_asset_key_str(dep.fqn)
                        key = translator.get_asset_key(
                            context, dep.fqn
                        ).to_user_string()
                        internal_asset_deps.add(key)

                        # create an external dep
                        deps_map[table] = translator.create_asset_dep(key=key)

                model_key = get_asset_key_str(model.fqn)
                asset_outs[model_key] = translator.create_asset_out(
                    model_key=model_key,
                    asset_key=asset_key_str,
                    tags=asset_tags,
                    is_required=False,
                    group_name=translator.get_group_name(context, model),
                    kinds={
                        "sqlmesh",
                        translator.get_context_dialect(context).lower(),
                    },
                )
                internal_asset_deps_map[model_key] = internal_asset_deps

            deps = list(deps_map.values())

            return SQLMeshMultiAssetOptions(
                outs=asset_outs,
                deps=deps,
                internal_asset_deps=internal_asset_deps_map,
            )
