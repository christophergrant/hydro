from __future__ import annotations

import inspect

import pytest
from delta import DeltaTable
from pyspark.sql import functions as F
from pyspark.sql.types import ArrayType
from pyspark.sql.types import LongType
from pyspark.sql.types import StringType
from pyspark.sql.utils import AnalysisException

import hydro._spark
import hydro.spark as hs
from tests import _df_to_list_of_dict
from tests import spark


def test_trie():
    fields = ['a.b.1', 'a.b.2']
    trie = hydro._spark._field_trie(fields)
    assert trie == {'a.b': ['1', '2']}


def test_trie_single():
    fields = ['a.b.1']
    trie = hydro._spark._field_trie(fields)
    assert trie == {'a.b': ['1']}


def test_trie_toplevel():
    fields = ['a']
    trie = hydro._spark._field_trie(fields)
    assert trie == {'a': [None]}


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


def test_deduplicate_dataframe_keys_notiebreak_str():
    df = spark.range(1).union(spark.range(1))
    result = hs.deduplicate_dataframe(df, keys='id')
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
    column = hs.hash_fields(df, algorithm='md5')
    final = df.withColumn('brown', column)
    assert _df_to_list_of_dict(final) == [{'id': 0, 'brown': 'cfcd208495d565ef66e7dff9f98764da'}]


def test_field_hash_sha1():
    df = spark.range(1)
    column = hs.hash_fields(df, algorithm='sha1')
    final = df.withColumn('brown', column)

    assert _df_to_list_of_dict(final) == [{'id': 0, 'brown': 'b6589fc6ab0dc82cf12099d1c2d40ab994e8410c'}]


def test_field_hash_sha2():
    df = spark.range(1)
    column = hs.hash_fields(df, algorithm='sha2')
    final = df.withColumn('brown', column)
    assert _df_to_list_of_dict(final) == [{'id': 0, 'brown': '5feceb66ffc86f38d952786c6d696c79c2dbc239dd4e91b46729d73a27fb57e9'}]


def test_field_hash_hash():
    df = spark.range(1)
    column = hs.hash_fields(df, algorithm='hash')
    final = df.withColumn('brown', column)
    assert _df_to_list_of_dict(final) == [{'id': 0, 'brown': 735846435}]


def test_field_hash_xxhash64():
    df = spark.range(1)
    column = hs.hash_fields(df, algorithm='xxhash64')
    final = df.withColumn('brown', column)
    assert _df_to_list_of_dict(final) == [{'id': 0, 'brown': -6091425354261976140}]


def test_field_hash_nonsense_algorithm():
    df = spark.range(1)
    with pytest.raises(ValueError) as exception:
        hs.hash_fields(df, algorithm='nonsense')
    assert exception.value.args[0] == """Algorithm nonsense not in supported algorithms ['sha1', 'sha2', 'md5', 'hash', 'xxhash64']"""


def test_field_hash_denylist():
    df = spark.range(1).withColumn('drop_this', F.lit(1))
    column = hs.hash_fields(df, algorithm='md5', denylist_fields=['drop_this'])
    final = df.withColumn('brown', column)

    assert _df_to_list_of_dict(final) == [{'id': 0, 'drop_this': 1, 'brown': 'cfcd208495d565ef66e7dff9f98764da'}]


def test_schema_hash():
    df = spark.range(1)
    column = hs.hash_schema(df)
    final = df.withColumn('schema_hash', column)
    assert _df_to_list_of_dict(final) == [{'id': 0, 'schema_hash': 'b80bb7740288fda1f201890375a60c8f'}]


def test_schema_hash_denylist():
    df = spark.range(1).withColumn('nonsense', F.lit('nonsense'))
    column = hs.hash_schema(df, denylist_fields=['nonsense'])
    final = df.withColumn('schema_hash', column)
    assert _df_to_list_of_dict(final) == [{'id': 0, 'nonsense': 'nonsense', 'schema_hash': 'b80bb7740288fda1f201890375a60c8f'}]


def test_schema_hash_duplicate_column():
    df = spark.range(1).withColumn('nonsense', F.lit('nonsense'))
    joined_df = df.join(df, ['id'])
    with pytest.raises(ValueError) as exception:
        hs.hash_schema(joined_df)
    assert exception.value.args[0] == """Duplicate field(s) detected in df, ['nonsense']"""


def test_map_list():
    df = spark.range(1).withColumn('nonsense', F.lit('nonsense  ')).withColumn('empty', F.lit('  '))
    final = hs.map_fields(df, ['nonsense', 'empty'], F.trim)
    assert _df_to_list_of_dict(final) == [{'id': 0, 'nonsense': 'nonsense', 'empty': ''}]


def test_map_by_type():
    df = spark.range(1).withColumn('nonsense', F.lit('nonsense  ')).withColumn('empty', F.lit('  '))
    final = hs.map_fields_by_type(df, StringType(), F.trim)
    assert _df_to_list_of_dict(final) == [{'id': 0, 'nonsense': 'nonsense', 'empty': ''}]


def test_map_by_type_lambda():
    df = spark.range(1).withColumn('nonsense', F.lit('nonsens  '))
    final = hs.map_fields_by_type(df, StringType(), lambda x: F.concat(F.trim(x), F.lit('ical')))
    assert _df_to_list_of_dict(final) == [{'id': 0, 'nonsense': 'nonsensical'}]


def test_map_by_type_lambda_expr():
    df = spark.range(1).withColumn('null', F.lit('  '))
    final = hs.map_fields_by_type(df, StringType(), lambda x: F.expr(f"nullif(trim({x}), '')"))
    assert _df_to_list_of_dict(final) == [{'id': 0, 'null': None}]


def test_map_by_regex():
    df = spark.range(1).withColumn('nonsense', F.lit('nonsense  '))
    final = hs.map_fields_by_regex(df, r'non.*', F.trim)
    assert _df_to_list_of_dict(final) == [{'id': 0, 'nonsense': 'nonsense'}]


def test_select_by_regex():
    df = spark.range(1).withColumn('nonsense', F.lit('nonsense  '))
    result = hs.select_fields_by_regex(df, 'id.*')
    assert _df_to_list_of_dict(result) == [{'id': 0}]


def test_select_by_type():
    df = spark.range(1).withColumn('nonsense', F.lit('nonsense  '))
    result = hs.select_fields_by_type(df, LongType())
    assert _df_to_list_of_dict(result) == [{'id': 0}]


def test__drop_fields_toplevel_negative():
    with pytest.raises(ValueError) as exception:
        hydro._spark._create_drop_field_column(('a1', [None]))
    assert exception.value.args[0] == 'Cannot drop top-level field `a1` with this function. Use df.drop() instead.'


def test__drop_fields_2():
    top_name, col = hydro._spark._create_drop_field_column(('a1', ['b']))
    assert top_name == 'a1' and str(col) == "Column<'update_fields(a1)'>"


def test__drop_fields_3():
    top_name, col = hydro._spark._create_drop_field_column(('a1.b1', ['a']))
    assert top_name == 'a1' and str(col) == "Column<'update_fields(a1, WithField(update_fields(a1.b1)))'>"


def test_drop_field_nonest():
    data = [{'a1': {'b1': {'a': [1, 2, 3], 'k': 'v'}}}]
    rdd = spark.sparkContext.parallelize(data)
    df = spark.read.json(rdd)
    final = hs.drop_fields(df, ['a1'])
    assert _df_to_list_of_dict(final) == [{}]


def test_drop_field_nonest_str():
    data = [{'a1': {'b1': {'a': [1, 2, 3], 'k': 'v'}}}]
    rdd = spark.sparkContext.parallelize(data)
    df = spark.read.json(rdd)
    final = hs.drop_fields(df, 'a1')
    assert _df_to_list_of_dict(final) == [{}]


def test_drop_field_nest1():
    data = [{'a1': {'a': [1, 2, 3], 'k': 'v'}}]
    rdd = spark.sparkContext.parallelize(data)
    df = spark.read.json(rdd)
    final = hs.drop_fields(df, ['a1.a'])
    assert _df_to_list_of_dict(final) == [{'a1': {'k': 'v'}}]


def test_drop_field_nest2():
    data = [{'a1': {'b1': {'a': [1, 2, 3], 'k': 'v'}}}]
    rdd = spark.sparkContext.parallelize(data)
    df = spark.read.json(rdd)
    final = hs.drop_fields(df, ['a1.b1.a'])
    assert _df_to_list_of_dict(final) == [{'a1': {'b1': {'k': 'v'}}}]


def test_drop_field_nest3():
    data = [{'a1': {'b1': {'c1': {'a': [1, 2, 3], 'k': 'v'}}}}]
    rdd = spark.sparkContext.parallelize(data)
    df = spark.read.json(rdd)
    final = hs.drop_fields(df, ['a1.b1.c1.a'])
    assert _df_to_list_of_dict(final) == [{'a1': {'b1': {'c1': {'k': 'v'}}}}]


def test_drop_field_nest4():
    data = [{'a1': {'b1': {'c1': {'d1': {'a': [1, 2, 3], 'k': 'v'}}}}}]
    rdd = spark.sparkContext.parallelize(data)
    df = spark.read.json(rdd)
    final = hs.drop_fields(df, ['a1.b1.c1.d1.a'])
    assert _df_to_list_of_dict(final) == [{'a1': {'b1': {'c1': {'d1': {'k': 'v'}}}}}]


def test_json_inference():
    data = {'id': 1, 'payload': """{"name": "christopher", "age": 420}"""}
    df = spark.createDataFrame([data])
    schema = hs.infer_json_field(df, 'payload')
    assert str(schema.json()) == """{"fields":[{"metadata":{},"name":"age","nullable":true,"type":"long"},{"metadata":{},"name":"name","nullable":true,"type":"string"}],"type":"struct"}"""


def test_json_inference_with_options():
    data = {'id': 1, 'payload': """{"name": "christopher", "age": 420}"""}
    df = spark.createDataFrame([data])
    options = {'mode': 'FAILFAST'}
    schema = hs.infer_json_field(df, 'payload', options=options)
    assert str(schema.json()) == """{"fields":[{"metadata":{},"name":"age","nullable":true,"type":"long"},{"metadata":{},"name":"name","nullable":true,"type":"string"}],"type":"struct"}"""


def test_csv_inference():
    data = {
        'id': 1, 'payload': """id,data
            1,"data"
            2,"newdata"
            """,
    }
    df = spark.createDataFrame([data])
    schema = hs.infer_csv_field(df, 'payload', {'header': 'True'})
    assert str(schema.json()) == """{"fields":[{"metadata":{},"name":"id","nullable":true,"type":"string"},{"metadata":{},"name":"data","nullable":true,"type":"string"}],"type":"struct"}"""


def test_deconstructed_field_toplevel():
    field = 'a1'
    result = hydro._spark._DeconstructedField(field).__dict__
    assert result == {'levels': ['a1'], 'trunk': 'a1', 'branches': [], 'leaf': None, 'trunk_and_branches': 'a1'}


def test_deconstructed_field_l2():
    field = 'a1.b'
    result = hydro._spark._DeconstructedField(field).__dict__
    assert result == {'levels': ['a1', 'b'], 'trunk': 'a1', 'branches': [], 'leaf': 'b', 'trunk_and_branches': 'a1'}


def test_deconstructed_field_l3():
    field = 'a1.b1.c'
    result = hydro._spark._DeconstructedField(field).__dict__
    assert result == {'levels': ['a1', 'b1', 'c'], 'trunk': 'a1', 'branches': ['b1'], 'leaf': 'c', 'trunk_and_branches': 'a1.b1'}
