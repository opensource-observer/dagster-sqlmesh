# dagster-sqlmesh

_WARNING: THIS IS A WORK IN PROGRESS_

SQLMesh library for dagster integration.

## Current features

* A `@sqlmesh_assets` decorator akin to `dagster-dbt`'s `@dbt_assets` decorator.
* A `SQLMeshResource` that allows you to call sqlmesh from inside an asset
  (likely one defined by the `@sqlmesh_assets` decorator)
* A `SQLMeshDagsterTranslator` that allows customizing the translation of
  sqlmesh models into dagster assets.

## Basic Usage

This dagster sqlmesh adapter is intended to work in a similar pattern to that of
`dagster-dbt` in the most basic case by using the `@sqlmesh_assets`

Assuming that your sqlmesh project is located in a directory `/home/foo/sqlmesh_project`, this is how you'd setup your dagster assets:

```python
from dagster import (
    AssetExecutionContext,
    Definitions,
)
from dagster_sqlmesh import sqlmesh_assets, SQLMeshContextConfig, SQLMeshResource

sqlmesh_config = SQLMeshContextConfig(path="/home/foo/sqlmesh_project", gateway="name-of-your-gateway")

@sqlmesh_assets(environment="dev", config=sqlmesh_config)
def sqlmesh_project(context: AssetExecutionContext, sqlmesh: SQLMeshResource):
    yield from sqlmesh.run(context)

defs = Definitions(
    assets=[sqlmesh_project],
    resources={
        "sqlmesh": SQLMeshResource(config=sqlmesh_config),
    },
)
```


## Contributing

_We are very open to contributions!_

In order to build the project you'll need the following:

* python 3.11 or 3.12
* node 18+
* pnpm 8+

_Note: this is a python project but some of our dependent tools are in typescript. As such all this is needed_

### Installing

To install everything you need for development just do the following:

```bash
poetry install
pnpm install
```

### Running tests

We have tests that should work entirely locally. You may see a `db.db` file appear in the root of the repository when these tests are run. It can be safely ignored or deleted.

We run tests with `pytest` like so:

```bash
poetry run pytest
```

### Running the "sample" dagster project

In the `sample/dagster_project` directory, is a minimal dagster project with the
accompanying sqlmesh project from `sample/sqlmesh_project` configured as an
asset. To run the sample dagster project deployment with a UI:

```bash
poetry run dagster dev -h 0.0.0.0 -f sample/dagster_project/definitions.py
```

If you'd like to materialize the dagster assets quickly on the CLI:

```bash
dagster asset materialize -f sample/dagster_project/definitions.py --select '*'
```

_Note: The sqlmesh project that is in the sample folder has a dependency on a
table that doesn't exist by default within the defined duckdb database. You'll
notice there's a `test_source` asset in the dagster project. This asset will
automatically populate that table in duckdb so that the sqlmesh project can be
run properly. Before you run any materializations against the sqlmesh related
assets in dagster, ensure that you've run the `test_source` at least once._

## Future Plans

* Create a new "loader" for sqlmesh and dagster definitions to allow for
  automatic creation of administrative jobs for sqlmesh (e.g. migrations).
  Additionally, we may want to have this generate assets outside of the
  `multi_asset` paradigm within dagster such that assets can have independent
  partitions. There is an existing issue for this in [dagster
  itself](https://github.com/dagster-io/dagster/issues/14228).