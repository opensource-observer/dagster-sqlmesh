# pyright: reportPrivateImportUsage=false
import logging
import time
import typing as t
from dataclasses import dataclass
from inspect import signature
from pathlib import Path

from dagster import AssetDep, AssetKey, AssetOut
from dagster._core.definitions.asset_dep import CoercibleToAssetDep
from pydantic import BaseModel, Field, model_validator

from dagster_sqlmesh.controller.base import (
    ContextCls,
    SQLMeshController,
    SQLMeshInstance,
)
from dagster_sqlmesh.translator import SQLMeshDagsterTranslator
from dagster_sqlmesh.types import (
    ConvertibleToAssetDep,
    ConvertibleToAssetKey,
    ConvertibleToAssetOut,
    SQLMeshModelDep,
    SQLMeshMultiAssetOptions,
)
from dagster_sqlmesh.utils import get_asset_key_str

logger = logging.getLogger(__name__)


class CachedMetadata(BaseModel):
    """Metadata for the cache file"""

    type: t.Literal["metadata"] = "metadata"

    creation_ts: int
    environment: str


class CachedAssetOut(BaseModel):
    type: t.Literal["asset_out"] = "asset_out"

    model_key: str
    asset_key: str
    tags: t.Mapping[str, str]
    is_required: bool
    group_name: str
    kinds: set[str] | None

    def to_asset_out(self) -> AssetOut:
        """Convert to a Dagster AssetOut object"""
        if "kinds" in signature(AssetOut).parameters:
            return AssetOut(
                key=AssetKey.from_user_string(self.asset_key),
                tags=self.tags,
                is_required=self.is_required,
                group_name=self.group_name,
                kinds=self.kinds,
            )
        return AssetOut(
            key=AssetKey.from_user_string(self.asset_key),
            tags=self.tags,
            is_required=self.is_required,
            group_name=self.group_name,
        )

    @classmethod
    def from_asset_out(cls, model_key: str, asset_out: AssetOut) -> "CachedAssetOut":
        """Create from a Dagster AssetOut object"""
        assert asset_out.key is not None, "AssetOut key must not be None"

        return cls(
            model_key=model_key,
            asset_key=asset_out.key.to_user_string(),
            tags=asset_out.tags or {},
            is_required=asset_out.is_required,
            group_name=asset_out.group_name or "",
            kinds=asset_out.kinds,
        )


class CachedInternalAssetDeps(BaseModel):
    type: t.Literal["internal_asset_dep"] = "internal_asset_dep"

    internal_asset_deps: dict[str, set[str]]

    def to_internal_asset_deps(self) -> dict[str, set[AssetKey]]:
        """Convert to a Dagster internal asset dependencies dictionary"""
        return {
            k: {AssetKey.from_user_string(dep) for dep in v}
            for k, v in self.internal_asset_deps.items()
        }

    @classmethod
    def from_internal_asset_deps(
        cls, internal_asset_deps: dict[str, set[AssetKey]]
    ) -> "CachedInternalAssetDeps":
        """Create from a Dagster internal asset dependencies dictionary"""
        return cls(
            internal_asset_deps={
                k: {dep.to_user_string() for dep in v}
                for k, v in internal_asset_deps.items()
            }
        )


class CachedDeps(BaseModel):
    type: t.Literal["deps"] = "deps"

    deps: list[dict[str, str]]

    def to_deps(self) -> list[CoercibleToAssetDep]:
        """Convert to a Dagster deps list"""
        return [AssetDep(AssetKey.from_user_string(dep["key"])) for dep in self.deps]

    @classmethod
    def from_deps(cls, deps: list[AssetDep]) -> "CachedDeps":
        """Create from a Dagster deps list"""
        return cls(deps=[{"key": dep.asset_key.to_user_string()} for dep in deps])


CacheEntryPayload = (
    CachedAssetOut | CachedInternalAssetDeps | CachedDeps | CachedMetadata
)


class CacheEntry(BaseModel):
    payload: CacheEntryPayload = Field(discriminator="type")


class DagsterSQLMeshCacheOptions(BaseModel):
    """SQLMesh context cache for Dagster"""

    enabled: bool
    cache_dir: str = ""
    cache_filename: str = "dagster_sqlmesh_cache.jsonl"
    enable_ttl: bool = True
    ttl_seconds: int = 60 * 5  # Defaults to 5 minutes

    @model_validator(mode="after")
    def validate_cache_dir(self):
        """Ensure the cache directory is set if caching is enabled"""
        if not self.enabled:
            return self

        if not self.cache_dir:
            raise ValueError("cache_dir must be set")
        return self

    def cache_path(self, environment: str) -> str:
        """Returns the full path to the cache file"""
        return f"{self.cache_env_dir(environment)}/{self.cache_filename}"

    def cache_env_dir(self, environment: str) -> str:
        """Returns the directory for the cache file"""
        return f"{self.cache_dir}/{environment}"

    def asset_streamer(self, environment: str) -> "DagsterSQLMeshCache":
        """Returns a cache streamer for the given environment"""
        return DagsterSQLMeshCache(environment=environment, options=self)


class CacheableAssetStreamer(t.Protocol):
    """Protocol for streaming SQLMesh assets from a cache"""

    def stream_assets(self) -> t.Iterator[CacheEntry]:
        """Stream the cache file if it exists and is valid"""
        ...


class CacheableAssetCacheWriter(t.Protocol):
    """Protocol for consuming SQLMesh assets from a cache"""

    def append_to_cache(self, data: CacheEntry) -> None:
        """Write data to the cache file"""
        ...

    def start_cache(self) -> None:
        """Clear the cache to prepare for writing"""
        ...


class DummyCacheableAssetWriter(CacheableAssetCacheWriter):
    """A dummy cache writer that does nothing"""

    def append_to_cache(self, data: CacheEntry) -> None:
        """Does nothing"""
        pass

    def start_cache(self) -> None:
        """Does nothing"""
        pass


@dataclass(kw_only=True)
class DagsterSQLMeshCache:
    environment: str
    options: DagsterSQLMeshCacheOptions

    @property
    def is_valid(self) -> bool:
        """Check if the cache file is valid based on the TTL"""
        try:
            iterator = self.stream_assets()
            metadata = next(iterator)  # Get the first entry which should be metadata
            assert isinstance(
                metadata.payload, CachedMetadata
            ), "First entry must be CachedMetadata"
            if not self.options.enable_ttl:
                return True
            if metadata.payload.creation_ts + self.options.ttl_seconds < int(
                time.time()
            ):
                return False
        except FileNotFoundError:
            return False
        except Exception as e:
            logger.error(f"Error reading cache file: {e}")
            return False
        return False

    @property
    def cache_path(self) -> str:
        """Returns the cache file path"""
        return self.options.cache_path(self.environment)

    def stream_assets(self) -> t.Iterator[CacheEntry]:
        """Stream the cache file if it exists and is valid"""
        try:
            with open(self.cache_path) as f:
                for line in f:
                    entry = CacheEntry.model_validate_json(line)
                    yield entry
        except FileNotFoundError:
            logger.warning(f"Cache file {self.cache_path} not found.")
        except Exception as e:
            logger.error(f"Error reading cache file {self.cache_path}: {e}")

    def start_cache(self) -> None:
        """Clear the cache to prepare for writing"""
        # Ensure the cache directory exists
        Path(self.options.cache_env_dir(self.environment)).mkdir(
            parents=True, exist_ok=True
        )
        # Clear the cache file and write the initial metadata
        metadata = CachedMetadata(
            creation_ts=int(time.time()),
            environment=self.environment,
        )
        entry = CacheEntry(payload=metadata)
        with open(self.cache_path, "w") as f:
            f.write(entry.model_dump_json())
            f.write("\n")

    def append_to_cache(self, data: CacheEntry) -> None:
        """Append data to the cache file"""

        with open(self.cache_path, "a") as f:
            f.write(data.model_dump_json())
            f.write("\n")


@dataclass(kw_only=True)
class SQLMeshInstanceAssetStreamer:
    instance: SQLMeshInstance
    translator: SQLMeshDagsterTranslator

    def stream_assets(self) -> t.Iterator[CacheEntry]:
        """Stream the assets from the SQLMesh instance"""
        instance = self.instance
        translator = self.translator

        context = self.instance.context
        internal_asset_deps_map: dict[str, set[AssetKey]] = {}
        deps_map: dict[str, AssetDep] = {}

        for model, deps in instance.non_external_models_dag():
            asset_key = translator.get_asset_key(context=context, fqn=model.fqn)
            model_deps = [
                SQLMeshModelDep(fqn=dep, model=context.get_model(dep)) for dep in deps
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
                    deps_map[table] = AssetDep(key)
            model_key = get_asset_key_str(model.fqn)
            # If current Dagster supports "kinds", add labels for Dagster UI
            if "kinds" in signature(AssetOut).parameters:
                yield CacheEntry(
                    payload=CachedAssetOut(
                        model_key=model_key,
                        asset_key=asset_key.to_user_string(),
                        tags=asset_tags,
                        is_required=False,
                        group_name=translator.get_group_name(context, model),
                        kinds={
                            "sqlmesh",
                            translator.get_context_dialect(context).lower(),
                        },
                    )
                )
            internal_asset_deps_map[model_key] = internal_asset_deps

        yield CacheEntry(
            payload=CachedInternalAssetDeps.from_internal_asset_deps(
                internal_asset_deps=internal_asset_deps_map
            )
        )

        deps = list(deps_map.values())

        yield CacheEntry(payload=CachedDeps.from_deps(deps=deps))


class DagsterSQLMeshController(SQLMeshController[ContextCls]):
    """An extension of the sqlmesh controller specifically for dagster use"""

    def to_asset_outs(
        self,
        environment: str,
        translator: SQLMeshDagsterTranslator,
    ) -> SQLMeshMultiAssetOptions:
        """Loads all the asset outs of the current sqlmesh environment. If a
        cache is provided, it will be tried first to load the asset outs."""

        internal_asset_deps_map: dict[str, set[ConvertibleToAssetKey]] = {}
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
                internal_asset_deps: set[ConvertibleToAssetKey] = set()
                asset_tags = translator.get_tags(context, model)

                for dep in model_deps:
                    if dep.model:
                        dep_asset_key_str = translator.get_asset_key(
                            context, dep.model.fqn
                        ).to_user_string()

                        internal_asset_deps.add(
                            translator.create_asset_key(key=dep_asset_key_str)
                        )
                    else:
                        table = get_asset_key_str(dep.fqn)
                        key = translator.get_asset_key(
                            context, dep.fqn
                        ).to_user_string()
                        internal_asset_deps.add(translator.create_asset_key(key=key))

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
