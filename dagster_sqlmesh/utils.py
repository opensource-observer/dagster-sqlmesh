def sqlmesh_model_name_to_key(name: str):
    return name.replace(".", "_dot__")


def key_to_sqlmesh_model_name(key: str):
    return key.replace("_dot__", ".")
