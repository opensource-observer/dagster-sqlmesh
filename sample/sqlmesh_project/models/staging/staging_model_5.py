import typing as t
from datetime import datetime

import numpy as np
import pandas as pd
from sqlmesh import ExecutionContext, model
from sqlmesh.core.model import ModelKindName


@model(
    name="sqlmesh_example.staging_model_5",
    is_sql=False,
    columns={
        "time": "TIMESTAMP",
        "value": "DOUBLE",
    },
    kind={"name": ModelKindName.INCREMENTAL_BY_TIME_RANGE, "time_column": "time"},
    start="2023-01-01",
)
def staging_model_5(
    context: ExecutionContext,
    start: datetime,
    end: datetime,
    **kwargs: t.Any,
) -> t.Iterator[pd.DataFrame]:
    """A model that randomly fails so that we can test the error handling in the resource."""
    enable_model_failure = context.var("enable_model_failure", default=False)
    if enable_model_failure:
        raise ValueError("This model is designed to fail for testing purposes.")

    # Generates a set of random rows for the model based on the start and end dates
    date_range = pd.date_range(start=start, end=end, freq="D")
    num_days = len(date_range)

    data = {
        "time": date_range,
        "value": np.random.rand(num_days)
        * 100,  # Random double values between 0 and 100
    }

    df = pd.DataFrame(data)
    yield df

    

