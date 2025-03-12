def sqlmesh_model_name_to_key(name: str) -> str:
    return name.replace(".", "_dot__")


def key_to_sqlmesh_model_name(key: str) -> str:
    return key.replace("_dot__", ".")


def sqlmesh_model_name_to_asset_key(name: str) -> str:
    return name.replace(".", "/")
