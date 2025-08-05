import typing as t
from collections.abc import Sequence
from inspect import signature

from dagster import AssetDep, AssetKey, AssetOut
from sqlglot import exp
from sqlmesh.core.context import Context
from sqlmesh.core.model import Model

from .types import ConvertibleToAssetDep, ConvertibleToAssetKey, ConvertibleToAssetOut


class _IntermediateAssetOut:
    def __init__(self, *, model_key: str, asset_key: str, **kwargs: t.Any):
        self.model_key = model_key
        self.asset_key = asset_key
        self.kwargs = kwargs

    def to_asset_out(self) -> AssetOut:
        kwargs = self.kwargs.copy()

        asset_key = AssetKey.from_user_string(self.asset_key)

        if "kinds" not in signature(AssetOut).parameters:
            kwargs.pop("kinds", None)

        return AssetOut(asset_key=asset_key, **self.kwargs)


class _IntermediateAssetDep:
    def __init__(self, *, key: str):
        self.key = key

    def to_asset_dep(self) -> AssetDep:
        return AssetDep(AssetKey.from_user_string(self.key))


class _IntermediateAssetKey:
    def __init__(self, *, key: str):
        self.key = key

    def to_asset_key(self) -> AssetKey:
        return AssetKey.from_user_string(self.key)


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

    def get_context_dialect(self, context: Context) -> str:
        return context.engine_adapter.dialect

    def create_asset_dep(self, *, key: str, **kwargs: t.Any) -> ConvertibleToAssetDep:
        """Create an object that resolves to an AssetDep

        Most users of this library will not need to use this method, it is
        primarily the way we enable cacheable assets from dagster-sqlmesh.
        """
        return _IntermediateAssetDep(key=key, **kwargs)

    def create_asset_out(
        self, *, model_key: str, asset_key: str, **kwargs: t.Any
    ) -> ConvertibleToAssetOut:
        """Create an object that resolves to an AssetOut

        Most users of this library will not need to use this method, it is
        primarily the way we enable cacheable assets from dagster-sqlmesh.
        """
        return _IntermediateAssetOut(model_key=model_key, asset_key=asset_key, **kwargs)

    def create_asset_key(self, **kwargs: t.Any) -> ConvertibleToAssetKey:
        """Create an object that resolves to an AssetKey

        Most users of this library will not need to use this method, it is
        primarily the way we enable cacheable assets from dagster-sqlmesh.
        """
        return _IntermediateAssetKey(**kwargs)

    def get_tags(self, context: Context, model: Model) -> dict[str, str]:
        """Given the sqlmesh context and a model return the tags for that model"""
        return {k: "true" for k in model.tags}
