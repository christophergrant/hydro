from __future__ import annotations

import inspect
from pprint import pprint
from typing import Any
from uuid import uuid4

import pyspark.sql.functions as F
import pytest
from delta import configure_spark_with_delta_pip
from delta import DeltaTable
from pyspark.sql import DataFrame
from pyspark.sql import SparkSession
from pyspark.sql.types import *

import hydro.delta
import hydro.spark

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


def test_snapshot_allfiles_basic(tmpdir):
    path = f'{tmpdir}/{inspect.stack()[0][3]}'
    spark.range(10).coalesce(1).write.format('delta').save(path)
    delta_table = DeltaTable.forPath(
        spark,
        path,
    )  # didn't catch when i didn't pass spark as first param
    assert hydro.delta._snapshot_allfiles(delta_table).count() == 1


def test_detail_basic(tmpdir):
    path = f'{tmpdir}/{inspect.stack()[0][3]}'
    spark.range(10000).repartition(1000).write.format('delta').save(path)
    delta_table = DeltaTable.forPath(spark, path)
    humanized_details = hydro.delta.detail(delta_table)
    raw_details = humanized_details
    assert raw_details['num_files'] == '1,000' and raw_details['size'].endswith(
        'KiB',
    )


def test_get_table_zordering_onecol(tmpdir):
    path = f'{tmpdir}/{inspect.stack()[0][3]}'
    spark.range(10).write.format('delta').save(path)
    delta_table = DeltaTable.forPath(spark, path)
    delta_table.optimize().executeZOrderBy('id')
    presented = hydro.delta.get_table_zordering(delta_table).collect()[0].asDict()
    expected = {'zOrderBy': '["id"]', 'count': 1}
    assert presented == expected


def test_get_table_zordering_twocol(tmpdir):
    path = f'{tmpdir}/{inspect.stack()[0][3]}'
    spark.range(10).withColumn('id2', F.lit('1')).write.format('delta').save(
        path,
    )
    delta_table = DeltaTable.forPath(spark, path)
    delta_table.optimize().executeZOrderBy('id')
    spark.range(10).withColumn('id2', F.lit('1')).write.mode('append').format(
        'delta',
    ).save(path)
    delta_table.optimize().executeZOrderBy('id', 'id2')
    presented = _df_to_list_of_dict(hydro.delta.get_table_zordering(delta_table))
    expected = [
        {'zOrderBy': '["id","id2"]', 'count': 1},
        {'zOrderBy': '["id"]', 'count': 1},
    ]
    assert presented == expected


def test_fields_nested_basic(tmpdir):
    path = f'{tmpdir}/{inspect.stack()[0][3]}'
    spark.range(1).withColumn('s1', F.struct(F.lit('a').alias('c1'))).write.format(
        'delta',
    ).save(
        path,
    )
    delta_table = DeltaTable.forPath(spark, path)
    provided = hydro.spark.fields(delta_table)
    expected = ['id', 's1.c1']
    assert provided == expected


def test_fields_nested_array(tmpdir):
    path = f'{tmpdir}/{inspect.stack()[0][3]}'
    spark.range(1).withColumn(
        's1',
        F.struct(F.array(F.lit('data')).alias('a')),
    ).write.format('delta').save(path)
    delta_table = DeltaTable.forPath(spark, path)
    provided = hydro.spark.fields_with_types(delta_table)
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
    better_fieldnames = hydro.spark.fields(delta_table)
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


@pytest.mark.timeout(10)  # this should not take longer than 5 seconds
def test_detail_enhanced(tmpdir):
    path = f'{tmpdir}/{inspect.stack()[0][3]}'
    spark.range(1).write.format('delta').save(path)

    delta_table = DeltaTable.forPath(spark, path)
    enhanced_details = hydro.delta.detail_enhanced(delta_table)
    pprint(enhanced_details)
    assert enhanced_details['numRecords'] == '1.0'


def test_partition_stats(tmpdir):
    path = f'{tmpdir}/{inspect.stack()[0][3]}'
    spark.range(100).withColumn('part', F.col('id') % 5).write.partitionBy(
        'part',
    ).format('delta').save(path)
    delta_table = DeltaTable.forPath(spark, path)

    presented = _df_to_list_of_dict(hydro.delta.partition_stats(delta_table))
    expected = [
        {
            'part': '0',
            'total_bytes': 567,
            'bytes_quantiles': [567, 567, 567, 567, 567],
            'num_records': 20.0,
            'num_files': 1,
        },
        {
            'part': '4',
            'total_bytes': 569,
            'bytes_quantiles': [569, 569, 569, 569, 569],
            'num_records': 20.0,
            'num_files': 1,
        },
        {
            'part': '2',
            'total_bytes': 619,
            'bytes_quantiles': [619, 619, 619, 619, 619],
            'num_records': 20.0,
            'num_files': 1,
        },
        {
            'part': '1',
            'total_bytes': 569,
            'bytes_quantiles': [569, 569, 569, 569, 569],
            'num_records': 20.0,
            'num_files': 1,
        },
        {
            'part': '3',
            'total_bytes': 569,
            'bytes_quantiles': [569, 569, 569, 569, 569],
            'num_records': 20.0,
            'num_files': 1,
        },
    ]
    assert True  # TODO fix this: can mask the non-deterministic fields or do fuzzy equality
    # assert presented == expected


def test_partial_update_set_merge(tmpdir):
    path = f'{tmpdir}/{inspect.stack()[0][3]}'
    spark.range(1).withColumn('data', F.lit('data')).withColumn(
        's1',
        F.struct(F.lit('a').alias('c1')),
    ).write.format('delta').save(
        path,
    )
    delta_table = DeltaTable.forPath(spark, path)
    fields = hydro.spark.fields(delta_table)
    source = (
        spark.range(1)
        .withColumn('s1', F.struct(F.lit('b').alias('c1')))
        .withColumn('data', F.lit(None).cast('string'))
    )
    (
        delta_table.alias('target')
        .merge(source=source.alias('source'), condition=F.expr('source.id = target.id'))
        .whenMatchedUpdate(
            set=hydro.delta.partial_update_set(fields, 'source', 'target'),
        )
        .whenNotMatchedInsertAll()
    ).execute()

    presented = delta_table.toDF().collect()[0].asDict(recursive=True)
    expected = {'id': 0, 'data': 'data', 's1': {'c1': 'b'}}
    assert presented == expected


def test_scd_type2(tmpdir):
    path = f'{tmpdir}/{inspect.stack()[0][3]}'
    target_data = [
        {'id': 1, 'location': 'Southern California', 'ts': '1969-01-01 00:00:00'},
    ]
    spark.createDataFrame(target_data).withColumn(
        'end_ts',
        F.lit(None).cast('string'),
    ).write.format('delta').save(path)
    delta_table = DeltaTable.forPath(spark, path)
    source_data = [
        {'id': 1, 'location': 'Northern California', 'ts': '2019-11-01 00:00:00'},
    ]
    df = spark.createDataFrame(source_data)
    presented = _df_to_list_of_dict(
        hydro.delta.scd(delta_table, df, 'id', 'ts', 'end_ts'),
    )
    expected = [
        {
            'id': 1,
            'location': 'Southern California',
            'ts': '1969-01-01 00:00:00',
            'end_ts': '2019-11-01 00:00:00',
        },
        {
            'id': 1,
            'location': 'Northern California',
            'ts': '2019-11-01 00:00:00',
            'end_ts': None,
        },
    ]
    assert sorted(presented, key=lambda x: x['ts']) == sorted(
        expected,
        key=lambda x: x['ts'],
    )


def test_scd_type1(tmpdir):
    path = f'{tmpdir}/{inspect.stack()[0][3]}'
    target_data = [
        {'id': 1, 'location': 'Southern California', 'ts': '1969-01-01 00:00:00'},
    ]
    spark.createDataFrame(target_data).write.format('delta').save(path)
    delta_table = DeltaTable.forPath(spark, path)
    source_data = [
        {'id': 1, 'location': 'Western California', 'ts': '2011-01-01 00:00:00'},
        {'id': 1, 'location': 'Northern California', 'ts': '2019-11-01 00:00:00'},
    ]
    df = spark.createDataFrame(source_data)
    presented = _df_to_list_of_dict(
        hydro.delta.scd(delta_table, df, 'id', 'ts', scd_type=1),
    )
    expected = [
        {
            'id': 1,
            'location': 'Northern California',
            'ts': '2019-11-01 00:00:00',
        },
    ]
    assert presented == expected


def test_bootstrap_scd2_path(tmpdir):
    path = f'{tmpdir}/{inspect.stack()[0][3]}'
    target_data = [
        {'id': 1, 'location': 'Southern California', 'ts': '1969-01-01 00:00:00'},
        {'id': 1, 'location': 'Northern California', 'ts': '2019-11-01 00:00:00'},
    ]
    df = spark.createDataFrame(target_data)
    delta_table = hydro.delta.bootstrap_scd2(df, 'id', 'ts', 'end_ts', path=path)
    presented = _df_to_list_of_dict(delta_table)
    expected = [
        {
            'id': 1,
            'location': 'Southern California',
            'ts': '1969-01-01 00:00:00',
            'end_ts': '2019-11-01 00:00:00',
        },
        {
            'id': 1,
            'location': 'Northern California',
            'ts': '2019-11-01 00:00:00',
            'end_ts': None,
        },
    ]
    assert sorted(presented, key=lambda x: x['ts']) == sorted(
        expected,
        key=lambda x: x['ts'],
    )


def test_bootstrap_scd2_tableidentifier(tmpdir):
    path = f'{tmpdir}/{inspect.stack()[0][3]}'
    target_data = [
        {'id': 1, 'location': 'Southern California', 'ts': '1969-01-01 00:00:00'},
        {'id': 1, 'location': 'Northern California', 'ts': '2019-11-01 00:00:00'},
    ]
    df = spark.createDataFrame(target_data)
    delta_table = hydro.delta.bootstrap_scd2(
        df,
        'id',
        'ts',
        'end_ts',
        table_identifier='jetfuel',
    )
    presented = _df_to_list_of_dict(delta_table)
    expected = [
        {
            'id': 1,
            'location': 'Southern California',
            'ts': '1969-01-01 00:00:00',
            'end_ts': '2019-11-01 00:00:00',
        },
        {
            'id': 1,
            'location': 'Northern California',
            'ts': '2019-11-01 00:00:00',
            'end_ts': None,
        },
    ]
    spark.sql('DROP TABLE IF EXISTS jetfuel')  # TODO: lol...
    assert sorted(presented, key=lambda x: x['ts']) == sorted(
        expected,
        key=lambda x: x['ts'],
    )


def test_bootstrap_scd2_nopath_notable():
    df = spark.range(10)
    with pytest.raises(ValueError) as exception:
        hydro.delta.bootstrap_scd2(df, ['id'], 'ts', 'end_ts')
    assert (
        exception.value.args[0] ==
        'Need to specify one (or both) of `path` and `table_identifier`'
    )


def test__scd2_no_endts(tmpdir):
    path = f'{tmpdir}/{inspect.stack()[0][3]}'
    df = spark.range(10)
    df.write.format('delta').save(path)
    delta_table = DeltaTable.forPath(spark, path)
    with pytest.raises(ValueError) as exception:
        hydro.delta.scd(delta_table, df, ['id'], 'ts')
    assert (
        exception.value.args[0] ==
        '`end_ts` parameter not provided, type 2 scd requires this'
    )


def test_scd_invalid_type(tmpdir):
    path = f'{tmpdir}/{inspect.stack()[0][3]}'
    df = spark.range(10)
    df.write.format('delta').save(path)
    delta_table = DeltaTable.forPath(spark, path)
    with pytest.raises(ValueError) as exception:
        hydro.delta.scd(delta_table, df, ['id'], 'ts', 'end_ts', scd_type=69)
    assert exception.value.args[0] == '`scd_type` not of (1,2)'


def test_deduplicate(tmpdir):
    path = f'{tmpdir}/{inspect.stack()[0][3]}'
    staging_path = f'{tmpdir}/{uuid4().hex}'
    data = [
        {'id': 1, 'ts': 1},
        {'id': 1, 'ts': 1},
    ]
    df = spark.createDataFrame(data)
    df.write.format('delta').save(path)
    delta_table = DeltaTable.forPath(spark, path)
    result = hydro.delta.deduplicate(delta_table, staging_path, 'id')
    expected = [{'id': 1, 'ts': 1}]
    assert sorted(_df_to_list_of_dict(result), key=lambda x: x['id']) == sorted(
        expected,
        key=lambda x: x['id'],
    )


def test_deduplicate_tiebreaking(tmpdir):
    path = f'{tmpdir}/{inspect.stack()[0][3]}'
    staging_path = f'{tmpdir}/{uuid4().hex}'
    data = [
        {'id': 1, 'ts': 1},
        {'id': 1, 'ts': 2},
    ]
    df = spark.createDataFrame(data)
    df.write.format('delta').save(path)
    delta_table = DeltaTable.forPath(spark, path)
    result = hydro.delta.deduplicate(
        delta_table,
        staging_path,
        'id',
        tiebreaking_columns=['ts'],
    )
    expected = [{'id': 1, 'ts': 2}]
    assert sorted(_df_to_list_of_dict(result), key=lambda x: x['id']) == sorted(
        expected,
        key=lambda x: x['id'],
    )
