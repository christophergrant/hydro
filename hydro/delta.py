from __future__ import annotations

import copy
from typing import Any
from uuid import uuid4

from delta import DeltaTable
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql import Window

import hydro.spark as hs
from hydro import _humanize_bytes
from hydro import _humanize_number


def scd(
    delta_table: DeltaTable,
    source: DataFrame,
    keys: list[str] | str,
    effective_ts: str,
    end_ts: str = None,
    scd_type: int = 2,
) -> DeltaTable:
    """
    Slowly Changing Dimensions (SCD) is a data management/engineering technique for handling changes in dimensions.

    Type 1 is a simple overwrite, where old data is overwritten with the new.

    Type 2 lets you track the history of an entity, creating a new row for each change in state.

    :param delta_table: The Delta Lake table that is to be updated. See `hydro.delta.bootstrap_scd2` if you need to create an SCD2 table.
    :param source: The new data that will be used to update `delta_table`
    :param keys: Column(s) that identify unique entities
    :param effective_ts: The start timestamp for a given row
    :param end_ts: The end timestamp for a given row. Used for Type 2 SCD
    :param scd_type: The type of SCD that is to be performed
    :return: The same `delta_table`
    """
    if isinstance(keys, str):  # pragma: no cover
        keys = [keys]

    def _scd2(
        delta_table: DeltaTable,
        source: DataFrame,
        keys: list[str] | str,
        effective_ts: str,
        end_ts: str,
    ):
        if not end_ts:
            raise ValueError(
                '`end_ts` parameter not provided, type 2 scd requires this',
            )

        updated_rows = delta_table.toDF().join(source, keys, 'left_semi').filter(F.col(end_ts).isNull())
        combined_rows = updated_rows.unionByName(source, allowMissingColumns=True)
        window = Window.partitionBy(keys).orderBy(effective_ts)
        final_payload = combined_rows.withColumn(
            end_ts,
            F.lead(effective_ts).over(window),
        )
        merge_keys = keys + [effective_ts]
        merge_key_condition = ' AND '.join(
            [f'source.{key} = target.{key}' for key in merge_keys],
        )
        delta_table.alias('target').merge(
            final_payload.alias('source'),
            merge_key_condition,
        ).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()
        return delta_table

    def _scd1(
        delta_table: DeltaTable,
        source: DataFrame,
        keys: list[str] | str,
        effective_ts: str,
    ):
        window = Window.partitionBy(keys).orderBy(F.col(effective_ts).desc())
        row_number_uuid = uuid4().hex  # avoid column collisions by using uuid
        final_payload = (
            source.withColumn(
                row_number_uuid,
                F.row_number().over(window),
            )
            .filter(F.col(row_number_uuid) == 1)
            .drop(row_number_uuid)
        )
        merge_key_condition = ' AND '.join(
            [f'source.{key} = target.{key}' for key in keys],
        )
        delta_table.alias('target').merge(
            final_payload.alias('source'),
            merge_key_condition,
        ).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()
        return delta_table

    if scd_type == 2:
        return _scd2(delta_table, source, keys, effective_ts, end_ts)
    elif scd_type == 1:
        return _scd1(delta_table, source, keys, effective_ts)
    else:
        raise ValueError('`scd_type` not of (1,2)')


def bootstrap_scd2(
    source_df: DataFrame,
    keys: list[str] | str,
    effective_ts: str,
    end_ts: str,
    table_properties: dict[str, str] = None,
    partition_columns: list[str] = [],
    comment: str = None,
    path: str = None,
    table_identifier: str = None,
) -> DeltaTable:
    """
    Creates an SCD2-ready Delta Lake table.

    :param source_df: Source data that will populate the final Delta Lake table
    :param keys: Column name(s) that identify unique rows. Can be a single column name as a string, or a list of strings, where order of the list does not matter.
    :param effective_ts: The name of the existing column that will be used as the "start" or "effective" timestamp for a given entity.
    :param end_ts: The name of the non-existing column that will be used as the "end" timestamp for a given entity.
    :param table_properties: A set of [Delta Lake table properties](https://docs.delta.io/latest/table-properties.html). Can also be custom properties.
    :param partition_columns: A set of column names that will be used for on-disk partitioning.
    :param comment: Comment that describes the table.
    :param path: Specify the path to the directory where table data is stored, which could be a path on distributed storage.
    :param table_identifier: The table name. Optionally qualified with a database name [catalog_name.] [database_name.] table_name.
    :return: The resulting DeltaTable object
    """
    if not path and not table_identifier:
        raise ValueError(
            'Need to specify one (or both) of `path` and `table_identifier`',
        )
    window = Window.partitionBy(keys).orderBy(effective_ts)
    final_payload = source_df.withColumn(
        end_ts,
        F.lead(effective_ts).over(window),
    )

    builder = DeltaTable.createOrReplace(
        source_df.sparkSession,
    )  # TODO change to createIfNotExists?
    if table_properties:  # pragma: no cover
        for k, v in table_properties.items():
            builder = builder.property(k, v)
    builder = builder.addColumns(source_df.schema)
    builder = builder.partitionedBy(*partition_columns)
    if comment:  # pragma: no cover
        builder = builder.comment(comment)
    if path:
        builder = builder.location(path)
    if table_identifier:
        builder = builder.tableName(table_identifier)
    builder.execute()
    delta_table = None
    if table_identifier:
        final_payload.write.format('delta').option('mergeSchema', 'true').mode(
            'append',
        ).saveAsTable(table_identifier)
        delta_table = DeltaTable.forName(source_df.sparkSession, table_identifier)
    elif path:  # pragma: no cover
        # literally no idea why coverage is failing here
        final_payload.write.format('delta').option('mergeSchema', 'true').mode(
            'append',
        ).save(path)
        delta_table = DeltaTable.forPath(source_df.sparkSession, path)
    return delta_table


def deduplicate(
    delta_table: DeltaTable,
    backup_path: str,
    keys: list[str] | str,
    tiebreaking_columns: list[str] = None,
) -> DeltaTable:
    """
    Removes duplicates from an existing Delta Lake table. This is a destructive operation that occurs over multiple transactions.

    Be careful.

    :param delta_table: The target Delta Lake table that contains duplicates.
    :param backup_path: A temporary location used to stage de-duplicated data. The location that `temp_path` points to needs to be empty.
    :param keys: A list of column names used to distinguish rows. The order of this list does not matter.
    :param tiebreaking_columns: A list of column names used for ordering. The order of this list matters, with earlier elements "weighing" more than lesser ones. The columns will be evaluated in descending order. In the event of a tie, you will get non-deterministic results.
    :return: The same Delta Lake table as **delta_table**.
    """
    if isinstance(keys, str):  # pragma: no cover
        keys = [keys]
    detail_object = detail_enhanced(delta_table)
    target_location = detail_object['location']
    table_version = detail_object['version']
    print(
        f'IF THIS OPERATION FAILS, RUN RESTORE TO {table_version} WITH deltaTable.restoreToVersion({table_version})',
    )
    df = delta_table.toDF()
    spark = df.sparkSession

    deduped = hs.deduplicate_dataframe(df, keys=keys, tiebreaking_columns=tiebreaking_columns)
    deduped.write.format('delta').save(backup_path)

    merge_key_condition = ' AND '.join(
        [f'source.{key} = target.{key}' for key in keys],
    )
    delta_table.alias('target').merge(
        deduped.select(keys).distinct().alias('source'),  # do we need distinct here?
        merge_key_condition,
    ).whenMatchedDelete().execute()
    spark.read.format('delta').load(backup_path).write.format('delta').mode(
        'append',
    ).save(target_location)
    return delta_table


def partial_update_set(
    delta_frame: DataFrame | DeltaTable,
    source_alias: str = 'source',
    target_alias: str = 'target',
) -> F.col:
    """
    Generates an update set for a Delta Lake MERGE operation where the source data provides partial updates.

    Partial updates in this case are when some columns in the data are NULL, but are meant to be non-destructive, or is there no semantic meaning to the NULLs.

    In other words, sometimes we want to keep the original value and not overwrite it with a NULL.

    This is particularly helpful with CDC data.

    :param delta_frame: A Delta Lake table or DataFrame that describes a source dataframe
    :param source_alias: A temporary name given to the source data of the MERGE
    :param target_alias: A temporary name given to the target Delta Table of the MERGE
    :return: A dictionary that describes non-destructive updates for all fields in `delta_frame`
    """
    # why does cov lie?
    if isinstance(delta_frame, DeltaTable):  # pragma: no cover
        delta_frame = delta_frame.toDF()
    fields = hs.fields(delta_frame)
    return {field: F.coalesce(f'{source_alias}.{field}', f'{target_alias}.{field}') for field in fields}


def partition_stats(delta_table: DeltaTable) -> DataFrame:
    """

    This is a utility function that gives detailed information about the partitions of a Delta Lake table.

    It returns a DataFrame that gives per-partition statistics of:
     - total size in bytes
     - byte size quantiles (0, .25, .5, .75, 1.0)
     - total number of records
     - total number of files

    It does this via scanning the table's transaction log, so it is fast, cheap, and scalable.

    :param delta_table: A Delta Lake table that you would like to analyze
    :return: A DataFrame that describes the size of all partitions in the table
    """
    allfiles = _snapshot_allfiles(delta_table)
    detail = DetailOutput(delta_table)
    partition_columns = [f'partitionValues.{col}' for col in detail.partition_columns]
    return allfiles.groupBy(*partition_columns).agg(
        F.sum('size').alias('total_bytes'),
        F.percentile_approx('size', [0, 0.25, 0.5, 0.75, 1.0]).alias('bytes_quantiles'),
        F.sum(F.get_json_object('stats', '$.numRecords')).alias('num_records'),
        F.count('*').alias('num_files'),
    )


def get_table_zordering(delta_table: DeltaTable) -> DataFrame:
    """

    A Delta Lake table can be clustered (Z-Ordered) by different, multiple columns.

    It is good to know what a table is clustered by to improve query performance.

    This function analyzes the Delta Lake table and returns a DataFrame that describes the Z-Ordering that has been applied to the table.

    The resulting DataFrame has schema of `zOrderBy`, `count`.

    :param delta_table: A Delta Lake table that you would like to analyze
    :return: A DataFrame with schema `zOrderBy`, `count`.
    """
    return (
        delta_table.history()
        .filter("operation == 'OPTIMIZE'")
        .filter('operationParameters.zOrderBy IS NOT NULL')
        .select('operationParameters.zOrderBy')
        .groupBy('zOrderBy')
        .count()
    )


def detail(delta_table: DeltaTable) -> dict[Any, Any]:
    """

    Delta Lake tables give details like name, number of files, size, etc.

    These stats are good, but ultimately they are built for machines, not humans, especially when they scale.

    I mean who wants to translate `413241234191 bytes` to `384.9 GiB`? Not me. Not you.

    This function returns a human-friendly version of DeltaTable.describe().

    :param delta_table: A Delta Lake table that you would like to analyze
    :return: A dictionary representing details of a Delta Lake table
    """
    detail_output = DetailOutput(delta_table)
    detail_output.humanize()
    return detail_output.to_dict()


def detail_enhanced(delta_table: DeltaTable) -> dict[Any, Any]:
    """

    See docs for `detail`. This function is similar, but adds more stats to the output including:
    - number of records in the current snapshot
    - percentage of files with collected statistics in the current snapshot
    - number of partitions in the current snapshot
    - current version

    :param delta_table: A Delta Lake table that you would like to analyze
    :return: A dictionary representing enhanced details of a Delta Lake table
    """
    details = detail(delta_table)
    allfiles = _snapshot_allfiles(delta_table)
    partition_columns = [f'partitionValues.{col}' for col in details['partition_columns']]

    num_records = (
        allfiles.select(
            F.get_json_object('stats', '$.numRecords').alias('num_records'),
        )
        .agg(F.sum('num_records').alias('num_records'))
        .collect()[0]['num_records']
    )
    details['numRecords'] = _humanize_number(num_records)

    stats_percentage = (
        allfiles.agg(
            F.avg(
                F.when(F.col('stats').isNotNull(), F.lit(1)).otherwise(F.lit(0)),
            ).alias('stats_percentage'),
        )
    ).collect()[0]['stats_percentage']
    details['stats_percentage'] = stats_percentage * 100

    partition_count = allfiles.select(*partition_columns).distinct().count()
    details['partition_count'] = _humanize_number(partition_count)

    version = delta_table.history().select('version').limit(1).collect()[0].asDict()['version']
    details['version'] = _humanize_number(version)
    return details


def _snapshot_allfiles(delta_table: DeltaTable) -> DataFrame:
    # this is kinda hacky but oh well
    spark = delta_table.toDF().sparkSession
    location = delta_table.detail().collect()[0]['location']

    delta_log = spark._jvm.org.apache.spark.sql.delta.DeltaLog.forTable(
        spark._jsparkSession,
        location,
    )
    return DataFrame(delta_log.snapshot().allFiles(), spark)


class DetailOutput:
    def __init__(self, delta_table: DeltaTable):
        detail_output = delta_table.detail().collect()[0].asDict()
        self.created_at = detail_output['createdAt']
        self.description: str = detail_output['description']
        self.format = detail_output['format']
        self.id = detail_output['id']
        self.last_modified = detail_output['lastModified']
        self.location = detail_output['location']
        self.min_reader_version = detail_output['minReaderVersion']
        self.min_writer_version = detail_output['minWriterVersion']
        self.name: str = detail_output['name']
        self.num_files = detail_output['numFiles']
        self.partition_columns = detail_output['partitionColumns']
        self.properties = detail_output['properties']
        self.size = detail_output['sizeInBytes']

    def humanize(self):
        self.num_files = _humanize_number(self.num_files)
        self.size = _humanize_bytes(self.size)

    def to_dict(self):
        return copy.deepcopy(self.__dict__)
