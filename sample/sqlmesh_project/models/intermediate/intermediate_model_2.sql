MODEL (
  name sqlmesh_example.intermediate_model_2,
  kind FULL
);

SELECT
  SUM(parent.value) as value
FROM sqlmesh_example.staging_model_4 AS parent
