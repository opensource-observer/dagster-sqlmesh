import typing as t
from collections.abc import Sequence
from inspect import signature

from dagster import AssetDep, AssetKey, AssetOut
from pydantic import BaseModel, Field
from sqlglot import exp
from sqlmesh.core.context import Context
from sqlmesh.core.model import Model

from .types import ConvertibleToAssetDep, ConvertibleToAssetOut


class IntermediateAssetOut(BaseModel):
    model_key: str
    asset_key: str
    tags: t.Mapping[str, str] | None = None
    is_required: bool = True
    group_name: str | None = None
    kinds: set[str] | None = None
    kwargs: dict[str, t.Any] = Field(default_factory=dict)

    def to_asset_out(self) -> AssetOut:
        asset_key = AssetKey.from_user_string(self.asset_key)

        if "kinds" not in signature(AssetOut).parameters:
            self.kinds = None

        return AssetOut(
            key=asset_key,
            tags=self.tags,
            is_required=self.is_required,
            group_name=self.group_name,
            kinds=self.kinds,
            **self.kwargs,
        )


class IntermediateAssetDep(BaseModel):
    key: str
    kwargs: dict[str, t.Any] = Field(default_factory=dict)

    def to_asset_dep(self) -> AssetDep:
        return AssetDep(AssetKey.from_user_string(self.key))


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
        return IntermediateAssetDep(key=key, kwargs=kwargs)

    def create_asset_out(
        self, *, model_key: str, asset_key: str, **kwargs: t.Any
    ) -> ConvertibleToAssetOut:
        """Create an object that resolves to an AssetOut

        Most users of this library will not need to use this method, it is
        primarily the way we enable cacheable assets from dagster-sqlmesh.
        """
        return IntermediateAssetOut(
            model_key=model_key,
            asset_key=asset_key,
            kinds=kwargs.pop("kinds", None),
            tags=kwargs.pop("tags", None),
            group_name=kwargs.pop("group_name", None),
            is_required=kwargs.pop("is_required", False),
            kwargs=kwargs,
        )

    def get_tags(self, context: Context, model: Model) -> dict[str, str]:
        """Given the sqlmesh context and a model return the tags for that model"""
        return {k: "true" for k in model.tags}
