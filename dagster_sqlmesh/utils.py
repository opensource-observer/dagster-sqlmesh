from sqlglot import exp
from sqlmesh.core.snapshot import SnapshotId


def get_asset_key_str(fqn: str) -> str:
    # This is an internal identifier used to map outputs and dependencies
    # it will not affect the existing AssetKeys
    # Only alphanumeric characters and underscores
    table = exp.to_table(fqn)
    asset_key_name = [table.catalog, table.db, table.name]
    
    return "sqlmesh__" + "_".join(asset_key_name)

def snapshot_id_to_model_name(snapshot_id: SnapshotId) -> str:
    """Convert a SnapshotId to its model name.

    Args:
        snapshot_id: The SnapshotId object to extract the model name from

    Returns:
        str: The model name in the format "db"."schema"."name"
    """
    return str(snapshot_id).split("<")[1].split(":")[0]
