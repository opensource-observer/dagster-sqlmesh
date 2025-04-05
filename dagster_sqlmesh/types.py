import typing as t
from dataclasses import dataclass, field

from dagster import AssetCheckResult, AssetKey, AssetMaterialization, AssetOut
from dagster._core.definitions.asset_dep import CoercibleToAssetDep
from sqlmesh.core.model import Model

MultiAssetResponse = t.Iterable[AssetCheckResult | AssetMaterialization]


@dataclass(kw_only=True)
class SQLMeshParsedFQN:
    catalog: str
    schema: str
    view_name: str

    @classmethod
    def parse(cls, fqn: str) -> "SQLMeshParsedFQN":
        split_fqn = fqn.split(".")

        # Remove any quotes
        split_fqn = list(map(lambda a: a.strip("'\""), split_fqn))
        return cls(catalog=split_fqn[0], schema=split_fqn[1], view_name=split_fqn[2])


@dataclass(kw_only=True)
class SQLMeshModelDep:
    fqn: str
    model: Model | None = None

    def parse_fqn(self) -> SQLMeshParsedFQN:
        return SQLMeshParsedFQN.parse(self.fqn)


@dataclass(kw_only=True)
class SQLMeshMultiAssetOptions:
    outs: dict[str, AssetOut] = field(default_factory=lambda: {})
    deps: t.Iterable[CoercibleToAssetDep] = field(default_factory=lambda: {})
    internal_asset_deps: dict[str, set[AssetKey]] = field(default_factory=lambda: {})
