import typing as t
from collections.abc import Sequence
from dataclasses import dataclass, field
from inspect import signature

from dagster import AssetKey, AssetOut, ConfigurableResource
from sqlglot import exp
from sqlmesh.core.context import Context
from sqlmesh.core.model import Model

from .types import ConvertibleToAssetKey, ConvertibleToAssetOut


@dataclass
class IntermediateAssetOut:
    """Intermediate representation of an AssetOut for lazy evaluation.

    Stores information to create an AssetOut but defers creation until
    `to_asset_out()` is called. Useful for caching during asset loading.
    """

    model_key: str
    asset_key: str
    tags: t.Mapping[str, str] | None = None
    is_required: bool = True
    group_name: str | None = None
    kinds: set[str] | None = None
    kwargs: dict[str, t.Any] = field(default_factory=dict)

    def to_asset_out(self) -> AssetOut:
        """Convert to a Dagster AssetOut."""
        asset_key = AssetKey.from_user_string(self.asset_key)

        kinds = self.kinds
        if "kinds" not in signature(AssetOut).parameters:
            kinds = None

        return AssetOut(
            key=asset_key,
            tags=self.tags,
            is_required=self.is_required,
            group_name=self.group_name,
            kinds=kinds,
            **self.kwargs,
        )


@dataclass
class IntermediateAssetDep:
    """Intermediate representation of an external dependency for lazy evaluation.

    Converts to AssetKey for use in containerized code locations.
    """

    key: str

    def to_asset_key(self) -> AssetKey:
        """Convert to a Dagster AssetKey."""
        return AssetKey.from_user_string(self.key)


class SQLMeshDagsterTranslator(ConfigurableResource):
    """Translates SQLMesh objects for Dagster.

    Converts SQLMesh models and metadata into Dagster-compatible formats.
    Can be subclassed to customize translation behavior such as asset key
    generation or grouping logic.

    Custom attributes must be declared as Pydantic fields (not set in __init__).
    """

    def get_asset_key(self, context: Context, fqn: str) -> AssetKey:
        """Get the Dagster AssetKey for a SQLMesh model.

        Args:
            context: The SQLMesh context
            fqn: Fully qualified name of the SQLMesh model

        Returns:
            The Dagster asset key for this model
        """
        path = self.get_asset_key_name(fqn)
        return AssetKey(path)

    def get_asset_key_name(self, fqn: str) -> Sequence[str]:
        """Parse a fully qualified name into asset key components.

        Args:
            fqn: Fully qualified name (e.g., "catalog.schema.table")

        Returns:
            Asset key components [catalog, schema, table]
        """
        table = exp.to_table(fqn)
        asset_key_name = [table.catalog, table.db, table.name]

        return asset_key_name

    def get_group_name(self, context: Context, model: Model) -> str:
        """Get the Dagster asset group name for a SQLMesh model.

        Args:
            context: The SQLMesh context
            model: The SQLMesh model

        Returns:
            The asset group name (defaults to the schema/database name)
        """
        path = self.get_asset_key_name(model.fqn)
        return path[-2]

    def get_context_dialect(self, context: Context) -> str:
        """Get the SQL dialect used by the SQLMesh context.

        Args:
            context: The SQLMesh context

        Returns:
            The SQL dialect name (e.g., "duckdb", "postgres")
        """
        return context.engine_adapter.dialect

    def create_asset_dep(self, *, key: str) -> ConvertibleToAssetKey:
        """Create an IntermediateAssetDep for an external dependency.

        Args:
            key: The asset key string for the dependency

        Returns:
            An object that can be converted to an AssetKey
        """
        return IntermediateAssetDep(key=key)

    def create_asset_out(
        self, *, model_key: str, asset_key: str, **kwargs: t.Any
    ) -> ConvertibleToAssetOut:
        """Create an IntermediateAssetOut for a model.

        Args:
            model_key: Internal key for the SQLMesh model
            asset_key: The asset key string for the output
            **kwargs: Additional arguments (tags, group_name, kinds, etc.)

        Returns:
            An object that can be converted to an AssetOut
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

    def get_asset_key_str(self, fqn: str) -> str:
        """Get asset key string with sqlmesh prefix for internal mapping.

        Creates an internal identifier used to map outputs and dependencies
        within the dagster-sqlmesh integration. Does not affect the actual
        AssetKeys that users see. The result contains only alphanumeric
        characters and underscores, making it safe for internal usage.

        Args:
            fqn: Fully qualified name of the SQLMesh model

        Returns:
            Internal asset key string with "sqlmesh__" prefix
        """
        table = exp.to_table(fqn)
        asset_key_name = [table.catalog, table.db, table.name]

        return "sqlmesh__" + "_".join(asset_key_name)

    def get_tags(self, context: Context, model: Model) -> dict[str, str]:
        """Get Dagster asset tags for a SQLMesh model.

        Args:
            context: The SQLMesh context
            model: The SQLMesh model

        Returns:
            Dictionary of tags to apply to the Dagster asset. Default
            converts SQLMesh model tags to empty string values, which
            causes the Dagster UI to render them as labels rather than
            key-value pairs.

        Note:
            Tags must contain only strings as keys and values.
        """
        return {k: "" for k in model.tags}
