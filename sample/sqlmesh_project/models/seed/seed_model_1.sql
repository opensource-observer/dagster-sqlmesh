MODEL (
  name sqlmesh_example.seed_model_1,
  kind SEED (
    path '../../seeds/seed_data_1.csv'
  ),
  columns (
    id INTEGER,
    item_id INTEGER,
    event_date DATE
  ),
  grain (id, event_date)
);
