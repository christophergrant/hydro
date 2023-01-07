from __future__ import annotations

from typing import Any

import pytest
from delta import configure_spark_with_delta_pip
from delta import DeltaTable
from pyspark.sql import DataFrame
from pyspark.sql import SparkSession


def _df_to_list_of_dict(df: DataFrame | DeltaTable) -> list[dict[Any, Any]]:
    if isinstance(df, DeltaTable):
        df = df.toDF()
    if df.count() > 10:
        raise OverflowError(
            'DataFrame over 10 rows, not materializing. Was this an accident?',
        )
    return [row.asDict(recursive=True) for row in df.collect()]


def test_df_to_dict_exception():
    df = spark.range(11)
    with pytest.raises(OverflowError) as exception:
        _df_to_list_of_dict(df)
    assert (
        exception.value.args[0] ==
        'DataFrame over 10 rows, not materializing. Was this an accident?'
    )


builder = (
    SparkSession.builder.appName('hydro-unit-tests')
    .master('local[*]')
    .config('spark.sql.shuffle.partitions', 1)
    .config('spark.default.parallelism', 1)
    .config('spark.rdd.compress', False)
    .config('spark.shuffle.compress', False)
    .config('spark.shuffle.spill.compress', False)
    .config('spark.dynamicAllocation.enabled', False)
    .config('spark.serializer', 'org.apache.spark.serializer.KryoSerializer')
    .config('spark.sql.extensions', 'io.delta.sql.DeltaSparkSessionExtension')
    .config(
        'spark.sql.catalog.spark_catalog',
        'org.apache.spark.sql.delta.catalog.DeltaCatalog',
    )
)
spark = configure_spark_with_delta_pip(builder).getOrCreate()
