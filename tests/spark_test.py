from __future__ import annotations

import inspect

import pytest
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


def test_field_hash_md5():
    df = spark.range(1)
    column = hs.hash_fields(df)
    final = df.withColumn('brown', column)
    assert _df_to_list_of_dict(final) == [{'id': 0, 'brown': 'cfcd208495d565ef66e7dff9f98764da'}]


def test_field_hash_sha1():
    df = spark.range(1)
    column = hs.hash_fields(df, algorithm='sha1')
    final = df.withColumn('brown', column)

    assert _df_to_list_of_dict(final) == [{'id': 0, 'brown': 'b6589fc6ab0dc82cf12099d1c2d40ab994e8410c'}]


def test_field_hash_sha2():  # pragma: no cover
    df = spark.range(1)
    column = hs.hash_fields(df, algorithm='sha2')
    final = df.withColumn('brown', column)
    assert _df_to_list_of_dict(final) == [{'id': 0, 'brown': '5feceb66ffc86f38d952786c6d696c79c2dbc239dd4e91b46729d73a27fb57e9'}]


def test_field_hash_nonsense_algorithm():
    df = spark.range(1)
    with pytest.raises(ValueError) as exception:
        hs.hash_fields(df, algorithm='nonsense')
    assert exception.value.args[0] == """Algorithm nonsense not in supported algorithms ['sha1', 'sha2', 'md5']"""


def test_field_hash_denylist():
    df = spark.range(1).withColumn('drop_this', F.lit(1))
    column = hs.hash_fields(df, algorithm='md5', denylist_fields=['drop_this'])
    final = df.withColumn('brown', column)

    assert _df_to_list_of_dict(final) == [{'id': 0, 'drop_this': 1, 'brown': 'cfcd208495d565ef66e7dff9f98764da'}]
