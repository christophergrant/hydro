from __future__ import annotations

import inspect

import pyspark.sql.functions as F
import pytest
from delta import configure_spark_with_delta_pip
from delta import DeltaTable
from pyspark.sql import SparkSession
from pyspark.sql.types import *

import hydro

builder = (
    SparkSession.builder.appName("hydro-unit-tests")
    .master("local[*]")
    .config("spark.sql.shuffle.partitions", 1)
    .config("spark.default.parallelism", 1)
    .config("spark.rdd.compress", False)
    .config("spark.shuffle.compress", False)
    .config("spark.shuffle.spill.compress", False)
    .config("spark.dynamicAllocation.enabled", False)
    .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config(
        "spark.sql.catalog.spark_catalog",
        "org.apache.spark.sql.delta.catalog.DeltaCatalog",
    )
)

spark = configure_spark_with_delta_pip(builder).getOrCreate()


def test_snapshot_allfiles_basic(tmpdir):
    path = f"{tmpdir}/{inspect.stack()[0][3]}"
    spark.range(10).coalesce(1).write.format("delta").save(path)
    delta_table = DeltaTable.forPath(
        spark,
        path,
    )  # didn't catch when i didn't pass spark as first param
    assert hydro._snapshot_allfiles(delta_table).count() == 1


def test_detail_basic(tmpdir):
    path = f"{tmpdir}/{inspect.stack()[0][3]}"
    spark.range(10000).repartition(1000).write.format("delta").save(path)

    delta_table = DeltaTable.forPath(spark, path)
    humanized_details = hydro.detail(delta_table)
    print(humanized_details.collect()[0].asDict())

    raw_details = humanized_details.collect()[0].asDict()
    # presented_details = {k: raw_details[k] for k in ['numFiles', 'size']}
    # expected = {'numFiles': '1,000', 'size': '523.39 KiB'}
    # assert presented_details == expected
    assert raw_details["numFiles"] == "1,000" and raw_details["size"].endswith("KiB")


def test_get_table_zordering_onecol(tmpdir):
    path = f"{tmpdir}/{inspect.stack()[0][3]}"
    spark.range(10).write.format("delta").save(path)
    delta_table = DeltaTable.forPath(spark, path)
    delta_table.optimize().executeZOrderBy("id")
    hydro.get_table_zordering(delta_table).show()


def test_get_table_zordering_twocol(tmpdir):
    path = f"{tmpdir}/{inspect.stack()[0][3]}"
    spark.range(10).withColumn("id2", F.lit("1")).write.format("delta").save(path)
    delta_table = DeltaTable.forPath(spark, path)
    delta_table.optimize().executeZOrderBy("id")
    spark.range(10).withColumn("id2", F.lit("1")).write.mode("append").format(
        "delta",
    ).save(path)
    delta_table.optimize().executeZOrderBy("id", "id2")
    hydro.get_table_zordering(delta_table).show()


def test_fields_nested_basic(tmpdir):
    path = f"{tmpdir}/{inspect.stack()[0][3]}"
    spark.range(1).withColumn("s1", F.struct(F.lit("a").alias("c1"))).write.format(
        "delta",
    ).save(path)
    delta_table = DeltaTable.forPath(spark, path)
    provided = hydro.fields(delta_table, True)
    expected = [("id", LongType()), ("s1.c1", StringType())]
    assert provided == expected


def test_fields_nested_array(tmpdir):
    path = f"{tmpdir}/{inspect.stack()[0][3]}"
    spark.range(1).withColumn(
        "s1",
        F.struct(F.array(F.lit("data")).alias("a")),
    ).write.format("delta").save(path)
    delta_table = DeltaTable.forPath(spark, path)
    provided = hydro.fields(delta_table, True)
    expected = [("id", LongType()), ("s1.a", ArrayType(StringType(), True))]
    assert provided == expected


def test_fields_docs_example(tmpdir):
    path = f"{tmpdir}/{inspect.stack()[0][3]}"
    data = """
    {
       "isbn":"0-942299-79-5",
       "title":"The Society of the Spectacle",
       "author":{
          "first_name":"Guy",
          "last_name":"Debord"
       },
       "published_year":1967,
       "pages":154,
       "language":"French"
    }
    """
    rdd = spark.sparkContext.parallelize([data])
    spark.read.json(rdd).write.format("delta").save(path)
    delta_table = DeltaTable.forPath(spark, path)
    better_fieldnames = hydro.fields(delta_table)
    expected = [
        "author.first_name",
        "author.last_name",
        "isbn",
        "language",
        "pages",
        "published_year",
        "title",
    ]
    assert sorted(better_fieldnames) == sorted(expected)