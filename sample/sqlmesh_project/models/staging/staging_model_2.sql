MODEL (
  name sqlmesh_example.staging_model_2,
  grain id
);

SELECT
  id,
  item_name
FROM
  sqlmesh_example.seed_model_2
