import typing as t
from dataclasses import dataclass, field

from dagster import (
    AssetKey,
    AssetOut,
)
from sqlmesh.core.model import Model


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


class ConvertibleToAssetKey(t.Protocol):
    """Protocol for objects that can be lazily converted to AssetKey."""

    def to_asset_key(self) -> AssetKey:
        """Convert to an AssetKey object."""
        ...


@dataclass(kw_only=True)
class SQLMeshMultiAssetOptions:
    """Intermediate representation of Dagster multi-asset options from SQLMesh.

    Uses generic types to allow caching and lazy evaluation during asset loading.
    """

    outs: t.Mapping[str, ConvertibleToAssetOut] = field(default_factory=lambda: {})
    deps: t.Iterable[ConvertibleToAssetKey] = field(default_factory=lambda: [])
    internal_asset_deps: t.Mapping[str, set[str]] = field(default_factory=lambda: {})

    def to_asset_outs(self) -> t.Mapping[str, AssetOut]:
        """Convert to an iterable of AssetOut objects."""
        return {key: out.to_asset_out() for key, out in self.outs.items()}

    def to_asset_deps(self) -> t.Iterable[AssetKey]:
        """Convert dependencies to AssetKey objects."""
        return [dep.to_asset_key() for dep in self.deps]
    
    def to_internal_asset_deps(self) -> dict[str, set[AssetKey]]:
        """Convert to a dictionary of internal asset dependencies."""
        return {
            key: {AssetKey.from_user_string(dep) for dep in deps}
            for key, deps in self.internal_asset_deps.items()
        }
