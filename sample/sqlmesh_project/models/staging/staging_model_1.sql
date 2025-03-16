MODEL (
  name sqlmesh_example.staging_model_1,
  kind INCREMENTAL_BY_TIME_RANGE (
    time_column event_date
  ),
  start '2020-01-01',
  cron '@daily',
  grain (id, event_date),
  tags (
    "staging",
    "incremental"
  )
);

SELECT
  id,
  item_id,
  event_date,
FROM
  sqlmesh_example.seed_model_1
WHERE
  event_date BETWEEN @start_date AND @end_date
