from pyspark.sql import SparkSession
import inspect
import hydro
from delta import DeltaTable, configure_spark_with_delta_pip
import pyspark.sql.functions as F
from pyspark.sql.types import *


builder = (
    SparkSession.builder.appName("hydro-unit-tests")
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
        spark, path
    )  # didn't catch when i didn't pass spark as first param
    assert hydro._snapshot_allfiles(delta_table).count() == 1


def test_detail_basic(tmpdir):
    path = f"{tmpdir}/{inspect.stack()[0][3]}"
    spark.range(10000).repartition(1000).write.format("delta").save(path)

    delta_table = DeltaTable.forPath(spark, path)
    humanized_details = hydro.detail(delta_table)

    raw_details = humanized_details.collect()[0].asDict()
    presented_details = {k: raw_details[k] for k in ["numFiles", "size"]}
    expected = {"numFiles": "1,000", "size": "522.92 KiB"}
    assert presented_details == expected


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
        "delta"
    ).save(path)
    delta_table.optimize().executeZOrderBy("id", "id2")
    hydro.get_table_zordering(delta_table).show()


def test_schema_leaf_nodes_nested_basic(tmpdir):
    path = f"{tmpdir}/{inspect.stack()[0][3]}"
    spark.range(1).withColumn("s1", F.struct(F.lit("a").alias("c1"))).write.format(
        "delta"
    ).save(path)
    delta_table = DeltaTable.forPath(spark, path)
    provided = hydro.schema_leaf_nodes(delta_table, True)
    expected = [("id", LongType()), ("s1.c1", StringType())]
    assert provided == expected


def test_schema_leaf_nodes_nested_array(tmpdir):
    path = f"{tmpdir}/{inspect.stack()[0][3]}"
    spark.range(1).withColumn(
        "s1", F.struct(F.array(F.lit("data")).alias("a"))
    ).write.format("delta").save(path)
    delta_table = DeltaTable.forPath(spark, path)
    provided = hydro.schema_leaf_nodes(delta_table, True)
    expected = [("id", LongType()), ("s1.a", ArrayType(StringType(), True))]
    assert provided == expected
