from __future__ import annotations

import inspect
from uuid import uuid4

import pytest
from delta import DeltaTable
from pyspark.sql import functions as F

import hydro._delta
import hydro.delta as hd
from tests import _df_to_list_of_dict
from tests import spark


def test_snapshot_allfiles_basic(tmpdir):
    path = f'{tmpdir}/{inspect.stack()[0][3]}'
    spark.range(10).coalesce(1).write.format('delta').save(path)
    delta_table = DeltaTable.forPath(
        spark,
        path,
    )  # didn't catch when i didn't pass spark as first param
    assert hydro._delta._snapshot_allfiles(delta_table).count() == 1


def test_get_table_zordering_onecol(tmpdir):
    path = f'{tmpdir}/{inspect.stack()[0][3]}'
    spark.range(10).write.format('delta').save(path)
    delta_table = DeltaTable.forPath(spark, path)
    delta_table.optimize().executeZOrderBy('id')
    presented = hd.zordering_stats(delta_table).collect()[0].asDict()
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
    presented = _df_to_list_of_dict(hd.zordering_stats(delta_table))
    expected = [
        {'zOrderBy': '["id","id2"]', 'count': 1},
        {'zOrderBy': '["id"]', 'count': 1},
    ]
    assert presented == expected


@pytest.mark.timeout(10)  # this should not take longer than 5 seconds
def test_detail(tmpdir):
    path = f'{tmpdir}/{inspect.stack()[0][3]}'
    spark.range(10000).write.format('delta').save(path)

    delta_table = DeltaTable.forPath(spark, path)
    enhanced_details = hd.detail(delta_table)
    assert enhanced_details['numRecords'] == '10,000.0'


def test_file_stats(tmpdir):
    path = f'{tmpdir}/{inspect.stack()[0][3]}'
    spark.range(1).write.format('delta').save(path)
    delta_table = DeltaTable.forPath(spark, path)

    presented = _df_to_list_of_dict(hd.file_stats(delta_table))[0]
    expected = {'path': 'part-00000-23d2fd65-1b58-48e1-a913-59b8c19bce0b-c000.snappy.parquet', 'partitionValues': {}, 'size': 478, 'modificationTime': 1673232534727, 'dataChange': False, 'stats': '{"numRecords":1,"minValues":{"id":0},"maxValues":{"id":0},"nullCount":{"id":0}}', 'tags': None}
    fuzzy_elems = ['path', 'modificationTime']
    for elem in fuzzy_elems:
        del presented[elem]
        del expected[elem]
    assert presented == expected


def test_partition_stats(tmpdir):
    path = f'{tmpdir}/{inspect.stack()[0][3]}'
    spark.range(1).withColumn('part', F.col('id') % 5).write.partitionBy('part').format(
        'delta',
    ).save(path)
    delta_table = DeltaTable.forPath(spark, path)

    presented = _df_to_list_of_dict(hd.partition_stats(delta_table))
    # drop these cuz it's gonna be different every time we test
    del presented[0]['oldest_timestamp']
    del presented[0]['newest_timestamp']
    expected = [{'part': '0', 'total_bytes': 478, 'bytes_quantiles': [478, 478, 478, 478, 478], 'num_records': 1.0, 'num_files': 1}]
    assert presented == expected


def test_partial_update_set_merge(tmpdir):
    path = f'{tmpdir}/{inspect.stack()[0][3]}'
    spark.range(1).withColumn('data', F.lit('data')).withColumn('s1', F.struct(F.lit('a').alias('c1'))).write.format(
        'delta',
    ).save(
        path,
    )
    delta_table = DeltaTable.forPath(spark, path)
    source = (
        spark.range(1).withColumn('s1', F.struct(F.lit('b').alias('c1'))).withColumn('data', F.lit(None).cast('string'))
    )
    (
        delta_table.alias('target')
        .merge(source=source.alias('source'), condition=F.expr('source.id = target.id'))
        .whenMatchedUpdate(
            set=hd.partial_update_set(delta_table, 'source', 'target'),
        )
        .whenNotMatchedInsertAll()
    ).execute()

    presented = delta_table.toDF().collect()[0].asDict(recursive=True)
    expected = {'id': 0, 'data': 'data', 's1': {'c1': 'b'}}
    assert presented == expected


def test_partial_update_set_deltatable(tmpdir):
    path = f'{tmpdir}/{inspect.stack()[0][3]}'
    spark.range(1).write.format('delta').save(path)
    delta_table = DeltaTable.forPath(spark, path)
    update_set = hd.partial_update_set(delta_table, source_alias='source', target_alias='target')
    # you can't equate Columns from pyspark, so we implicitly(?) convert to string and do equality there instead
    assert str(update_set['id']) == str(F.coalesce(F.col('source.id'), F.col('target.id')))


def test_scd_type2(tmpdir):
    path = f'{tmpdir}/{inspect.stack()[0][3]}'
    target_data = [
        {'id': 1, 'location': 'Southern California', 'ts': '1969-01-01 00:00:00'},
    ]
    spark.createDataFrame(target_data).withColumn('end_ts', F.lit(None).cast('string')).write.format(
        'delta',
    ).save(path)
    delta_table = DeltaTable.forPath(spark, path)
    source_data = [
        {'id': 1, 'location': 'Northern California', 'ts': '2019-11-01 00:00:00'},
    ]
    df = spark.createDataFrame(source_data)
    presented = _df_to_list_of_dict(
        hd.scd(delta_table, df, 'id', 'ts', 'end_ts'),
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
        hd.scd(delta_table, df, 'id', 'ts', scd_type=1),
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
    delta_table = hd.bootstrap_scd2(df, 'id', 'ts', 'end_ts', path=path, comment='test', table_properties={'test': 'onetwothree'})
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
    delta_table = hd.bootstrap_scd2(
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
        hd.bootstrap_scd2(df, ['id'], 'ts', 'end_ts')
    assert exception.value.args[0] == 'Need to specify one (or both) of `path` and `table_identifier`'


def test__scd2_no_endts(tmpdir):
    path = f'{tmpdir}/{inspect.stack()[0][3]}'
    df = spark.range(10)
    df.write.format('delta').save(path)
    delta_table = DeltaTable.forPath(spark, path)
    with pytest.raises(ValueError) as exception:
        hd.scd(delta_table, df, ['id'], 'ts')
    assert exception.value.args[0] == '`end_field` parameter not provided, type 2 scd requires this'


def test_scd_invalid_type(tmpdir):
    path = f'{tmpdir}/{inspect.stack()[0][3]}'
    df = spark.range(10)
    df.write.format('delta').save(path)
    delta_table = DeltaTable.forPath(spark, path)
    with pytest.raises(ValueError) as exception:
        hd.scd(delta_table, df, ['id'], 'ts', 'end_ts', scd_type=69)
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
    result = hydro._delta._deduplicate(delta_table, staging_path, 'id')
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
    result = hydro._delta._deduplicate(
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


def test_summarize_allfiles(tmpdir):
    path = f'{tmpdir}/{inspect.stack()[0][3]}'
    spark.range(1).write.format('delta').save(path)
    delta_table = DeltaTable.forPath(spark, path)
    result = hd.summarize_all_files(delta_table)
    del result['oldest_timestamp']
    expected = {'number_of_files': '1', 'total_size': '478.00 bytes'}
    assert result == expected


def test_summarize_allfiles_inhuman(tmpdir):
    path = f'{tmpdir}/{inspect.stack()[0][3]}'
    spark.range(1).write.format('delta').save(path)
    delta_table = DeltaTable.forPath(spark, path)
    result = hd.summarize_all_files(delta_table, False)
    print(result)
    del result['oldest_timestamp']
    expected = {'number_of_files': 1, 'total_size': 478}
    assert result == expected


def test_idempotent_multiwriter(tmpdir):
    path = f'{tmpdir}/{inspect.stack()[0][3]}'
    df = spark.range(1)
    df.write.format('delta').option('txnVersion', 1).option('txnAppId', 'app').save(path)
    delta_table = DeltaTable.forPath(spark, path)
    output = hd.idempotency_markers(delta_table)
    assert str(output) == 'Map(app -> 1)'


def test_late_arriving_scd2_negative(tmpdir):
    path = f'{tmpdir}/{inspect.stack()[0][3]}'
    data = [
        {'id': 1, 'event_date': '2023-01-02'},
        {'id': 1, 'event_date': '2023-01-03'},
    ]
    df = spark.createDataFrame(data)
    delta_table = hd.bootstrap_scd2(df, path=path, keys=['id'], effective_field='event_date', end_field='end_date')
    new_data = [{'id': 1, 'event_date': '2023-01-01'}]
    new_df = spark.createDataFrame(new_data)
    hd.scd(delta_table, new_df, keys=['id'], effective_field='event_date', end_field='end_date')
    result = delta_table.toDF()
    result_dict = _df_to_list_of_dict(result)
    expected = [{'event_date': '2023-01-01', 'id': 1, 'end_date': '2023-01-03'}, {'event_date': '2023-01-02', 'id': 1, 'end_date': '2023-01-03'}, {'event_date': '2023-01-03', 'id': 1, 'end_date': None}]
    assert result_dict == expected


def test_late_arriving_scd2_dups_with_sequence(tmpdir):
    path = f'{tmpdir}/{inspect.stack()[0][3]}'
    data = [
        {'id': 1, 'event_date': '2023-01-02', 'sequence': 1},
        {'id': 1, 'event_date': '2023-01-02', 'sequence': 2},
    ]
    df = spark.createDataFrame(data)
    delta_table = hd.bootstrap_scd2(df, path=path, keys=['id'], effective_field='event_date', end_field='end_date')
    new_data = [{'id': 1, 'event_date': '2023-01-03', 'sequence': 3}]
    new_df = spark.createDataFrame(new_data)
    hd.scd(delta_table, new_df, keys=['id'], effective_field='event_date', end_field='end_date')
    result = delta_table.toDF()
    result.show()
