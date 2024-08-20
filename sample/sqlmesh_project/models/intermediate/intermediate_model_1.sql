MODEL (
  name sqlmesh_example.intermediate_model_1,
  kind INCREMENTAL_BY_TIME_RANGE (
    time_column event_date
  ),
  start '2020-01-01',
  cron '@daily',
  grain (id, event_date)
);

SELECT
  main.id,
  main.item_id,
  main.event_date,
  sub.item_name
FROM sqlmesh_example.staging_model_1 AS main
INNER JOIN sqlmesh_example.staging_model_2 as sub
  ON main.id = sub.id
WHERE
  event_date BETWEEN @start_date AND @end_date
