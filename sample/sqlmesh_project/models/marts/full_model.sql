model (
  NAME sqlmesh_example.full_model,
  kind full,
  cron '@daily',
  grain item_id,
  audits (assert_positive_order_ids),
  tags (
    "mart",
    "full",
  )
);
SELECT
  item_id,
  item_name,
  COUNT(
    DISTINCT id
  ) AS num_orders,
  now() AS last_updated_at
FROM
  sqlmesh_example.intermediate_model_1
GROUP BY
  ALL
