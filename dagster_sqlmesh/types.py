import typing as t
from dataclasses import dataclass, field

from dagster import AssetCheckResult, AssetDep, AssetKey, AssetMaterialization, AssetOut
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
    
class ConvertibleToAssetOut(t.Protocol):
    def to_asset_out(self) -> AssetOut:
        """Convert to an AssetOut object."""
        ...

class ConvertibleToAssetDep(t.Protocol):
    def to_asset_dep(self) -> AssetDep:
        """Convert to an AssetDep object."""
        ...

class ConvertibleToAssetKey(t.Protocol):
    def to_asset_key(self) -> AssetKey:
        ...

@dataclass(kw_only=True)
class SQLMeshMultiAssetOptions:
    """Generic class for returning dagster multi asset options from SQLMesh, the
    types used are intentionally generic so to allow for potentially using an
    intermediate representation of the dagster asset objects. This is most
    useful in caching purposes and is done to allow for users of this library to
    manipulate the dagster asset creation process as they see fit."""

    outs: t.Mapping[str, ConvertibleToAssetOut] = field(default_factory=lambda: {})
    deps: t.Iterable[ConvertibleToAssetDep] = field(default_factory=lambda: [])
    internal_asset_deps: t.Mapping[str, set[str]] = field(default_factory=lambda: {})

    def to_asset_outs(self) -> t.Mapping[str, AssetOut]:
        """Convert to an iterable of AssetOut objects."""
        return {key: out.to_asset_out() for key, out in self.outs.items()}

    def to_asset_deps(self) -> t.Iterable[AssetDep]:
        """Convert to an iterable of AssetDep objects."""
        return [dep.to_asset_dep() for dep in self.deps]
    
    def to_internal_asset_deps(self) -> dict[str, set[AssetKey]]:
        """Convert to a dictionary of internal asset dependencies."""
        return {
            key: {AssetKey.from_user_string(dep) for dep in deps}
            for key, deps in self.internal_asset_deps.items()
        }