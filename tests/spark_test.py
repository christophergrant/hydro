from __future__ import annotations

import inspect

from delta import DeltaTable
from pyspark.sql import functions as F
from pyspark.sql.types import ArrayType
from pyspark.sql.types import LongType
from pyspark.sql.types import StringType

import hydro.spark as hs
from tests import _df_to_list_of_dict
from tests import spark


def test_fields_nested_basic(tmpdir):
    path = f'{tmpdir}/{inspect.stack()[0][3]}'
    spark.range(1).withColumn('s1', F.struct(F.lit('a').alias('c1'))).write.format('delta').save(
        path,
    )
    delta_table = DeltaTable.forPath(spark, path)
    provided = hs.fields(delta_table.toDF())
    expected = ['id', 's1.c1']
    assert provided == expected


def test_fields_nested_array(tmpdir):
    path = f'{tmpdir}/{inspect.stack()[0][3]}'
    spark.range(1).withColumn('s1', F.struct(F.array(F.lit('data')).alias('a'))).write.format(
        'delta',
    ).save(path)
    delta_table = DeltaTable.forPath(spark, path)
    provided = hs.fields_with_types(delta_table.toDF())
    expected = [('id', LongType()), ('s1.a', ArrayType(StringType(), True))]
    assert provided == expected


def test_fields_docs_example(tmpdir):
    path = f'{tmpdir}/{inspect.stack()[0][3]}'
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
    spark.read.json(rdd).write.format('delta').save(path)
    delta_table = DeltaTable.forPath(spark, path)
    better_fieldnames = hs.fields(delta_table.toDF())
    expected = [
        'author.first_name',
        'author.last_name',
        'isbn',
        'language',
        'pages',
        'published_year',
        'title',
    ]
    assert sorted(better_fieldnames) == sorted(expected)


def test_deduplicate_dataframe_keys_notiebreak():
    df = spark.range(1).union(spark.range(1))
    result = hs.deduplicate_dataframe(df, keys=['id'])
    assert _df_to_list_of_dict(result) == [{'id': 0}]


def test_deduplicate_dataframe_nokeys():
    data = [{'id': 0, 'ts': 1}, {'id': 0, 'ts': 1}]
    df = spark.createDataFrame(data)
    result = hs.deduplicate_dataframe(df)
    assert _df_to_list_of_dict(result) == [{'id': 0, 'ts': 1}]


def test_deduplicate_dataframe_negative_nokeys():
    data = [{'id': 0, 'ts': 1}, {'id': 0, 'ts': 2}]
    df = spark.createDataFrame(data)
    result = hs.deduplicate_dataframe(df)
    assert _df_to_list_of_dict(result) == [{'id': 0, 'ts': 1}, {'id': 0, 'ts': 2}]


def test_deduplicate_dataframe_tiebreaker():
    data = [{'id': 0, 'ts': 1}, {'id': 0, 'ts': 2}]
    df = spark.createDataFrame(data)
    result = hs.deduplicate_dataframe(df, keys=['id'], tiebreaking_columns=['ts'])
    assert _df_to_list_of_dict(result) == [{'id': 0, 'ts': 2}]
