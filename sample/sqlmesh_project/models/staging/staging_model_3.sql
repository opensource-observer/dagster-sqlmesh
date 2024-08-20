MODEL (
  name sqlmesh_example.staging_model_3,
  kind FULL,
  grain id,
  cron '@hourly'
);

SELECT
  id,
  name 
FROM
  sources.test_source
