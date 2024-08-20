MODEL (
  name sqlmesh_example.seed_model_2,
  kind SEED (
    path '../../seeds/seed_data_2.csv'
  ),
  columns (
    id INTEGER,
    item_name VARCHAR
  )
);
