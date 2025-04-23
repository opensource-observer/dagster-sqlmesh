from sqlmesh.core.snapshot import SnapshotId


def snapshot_id_to_model_name(snapshot_id: SnapshotId) -> str:
    """Convert a SnapshotId to its model name.

    Args:
        snapshot_id: The SnapshotId object to extract the model name from

    Returns:
        str: The model name in the format "db"."schema"."name"
    """
    return str(snapshot_id).split("<")[1].split(":")[0]
