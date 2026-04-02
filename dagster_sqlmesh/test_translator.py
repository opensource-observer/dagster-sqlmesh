from dagster_sqlmesh.translator import SQLMeshDagsterTranslator


def test_get_asset_key_str_sanitizes_hyphens():
    translator = SQLMeshDagsterTranslator()
    result = translator.get_asset_key_str("my-catalog.my-schema.my-model")
    assert result == "sqlmesh__my_catalog_my_schema_my_model"
    assert "-" not in result