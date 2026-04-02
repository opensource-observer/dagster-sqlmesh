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
    """Translates SQLMesh objects for Dagster.
    
    This class provides methods to convert SQLMesh models and metadata into
    Dagster-compatible formats. It can be subclassed to customize the translation
    behavior, such as changing asset key generation or grouping logic.
    
    The translator is used throughout the dagster-sqlmesh integration, including
    in the DagsterSQLMeshEventHandler and asset generation process.
    """

    def get_asset_key(self, context: Context, fqn: str) -> AssetKey:
        """Get the Dagster AssetKey for a SQLMesh model.
        
        Args:
            context: The SQLMesh context (unused in default implementation)
            fqn: Fully qualified name of the SQLMesh model
            
        Returns:
            AssetKey: The Dagster asset key for this model
        """
        path = self.get_asset_key_name(fqn)
        return AssetKey(path)

    def get_asset_key_name(self, fqn: str) -> Sequence[str]:
        """Parse a fully qualified name into asset key components.
        
        Args:
            fqn: Fully qualified name of the SQLMesh model (e.g., "catalog.schema.table")
            
        Returns:
            Sequence[str]: Asset key components [catalog, schema, table]
        """
        table = exp.to_table(fqn)
        asset_key_name = [table.catalog, table.db, table.name]

        return asset_key_name

    def get_group_name(self, context: Context, model: Model) -> str:
        """Get the Dagster asset group name for a SQLMesh model.
        
        Args:
            context: The SQLMesh context (unused in default implementation)
            model: The SQLMesh model
            
        Returns:
            str: The asset group name (defaults to the schema/database name)
        """
        path = self.get_asset_key_name(model.fqn)
        return path[-2]

    def get_context_dialect(self, context: Context) -> str:
        """Get the SQL dialect used by the SQLMesh context.
        
        Args:
            context: The SQLMesh context
            
        Returns:
            str: The SQL dialect name (e.g., "duckdb", "postgres", etc.)
        """
        return context.engine_adapter.dialect

    def create_asset_dep(self, *, key: str, **kwargs: t.Any) -> ConvertibleToAssetDep:
        """Create an object that resolves to an AssetDep.

        This creates an intermediate representation that can be converted to a
        Dagster AssetDep. Most users will not need to use this method directly.
        
        Args:
            key: The asset key string for the dependency
            **kwargs: Additional arguments to pass to the AssetDep
            
        Returns:
            ConvertibleToAssetDep: An object that can be converted to an AssetDep
        """
        return IntermediateAssetDep(key=key, kwargs=kwargs)

    def create_asset_out(
        self, *, model_key: str, asset_key: str, **kwargs: t.Any
    ) -> ConvertibleToAssetOut:
        """Create an object that resolves to an AssetOut.

        This creates an intermediate representation that can be converted to a
        Dagster AssetOut. Most users will not need to use this method directly.
        
        Args:
            model_key: Internal key for the SQLMesh model
            asset_key: The asset key string for the output
            **kwargs: Additional arguments including tags, group_name, kinds, etc.
            
        Returns:
            ConvertibleToAssetOut: An object that can be converted to an AssetOut
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
        
        This creates an internal identifier used to map outputs and dependencies
        within the dagster-sqlmesh integration. It will not affect the actual
        AssetKeys that users see. The result contains only alphanumeric characters
        and underscores, making it safe for internal usage.
        
        Args:
            fqn: Fully qualified name of the SQLMesh model
            
        Returns:
            str: Internal asset key string with "sqlmesh__" prefix
        """
        table = exp.to_table(fqn)
        asset_key_name = [table.catalog, table.db, table.name]

        return ("sqlmesh__" + "_".join(asset_key_name)).replace("-", "_")

    def get_tags(self, context: Context, model: Model) -> dict[str, str]:
        """Get Dagster asset tags for a SQLMesh model.
        
        Args:
            context: The SQLMesh context (unused in default implementation)
            model: The SQLMesh model
            
        Returns:
            dict[str, str]: Dictionary of tags to apply to the Dagster asset.
                           Default implementation converts SQLMesh model tags to 
                           empty string values, which causes the Dagster UI to
                           render them as labels rather than key-value pairs.
                           
        Note:
            Tags must contain only strings as keys and values. The Dagster UI
            will render tags with empty string values as "labels" rather than
            key-value pairs.
        """
        return {k: "" for k in model.tags}
