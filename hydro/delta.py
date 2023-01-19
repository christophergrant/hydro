from __future__ import annotations

from typing import Any
from uuid import uuid4

from delta import DeltaTable
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql import Window

import hydro.spark as hs
from hydro import _humanize_bytes
from hydro import _humanize_number
from hydro import _humanize_timestamp
from hydro._delta import _DetailOutput
from hydro._delta import _snapshot_allfiles
from hydro._delta import _snapshot_transactions
from hydro._delta import _summarize_data_files


def scd(
        delta_table: DeltaTable,
        source: DataFrame,
        keys: list[str] | str,
        effective_ts: str,
        end_ts: str = None,
        scd_type: int = 2,
) -> DeltaTable:
    """

    Executes a slowly changing dimensions transformation and merge.

    Supports Type 1 and Type 2 SCD.

    :param delta_table: The target Delta Lake table that is to be updated. See `hydro.delta.bootstrap_scd2` if you need to create an SCD2 table.
    :param source: The source data that will be used to merge with `delta_table`
    :param keys: Column name(s) that identify unique rows. Can be a single column name as a string, or a list of strings, where order of the list does not matter.
    :param effective_ts: The name of the existing column that will be used as the “start” or “effective” indicator for a given entity. The column be of any orderable type, including timestamp, date, string, and numeric types.
    :param end_ts: Only required for type 2. The name of the non-existing column that will be used as the “end” indicator for a given entity. Its type will match the type of effective_ts.
    :param scd_type: The type of SCD that is to be performed
    :return: The same `delta_table`

    Example
    -----

     For **`SCD Type 2 <https://en.wikipedia.org/wiki/Slowly_changing_dimension#Type_2:_add_new_row>`**,

     Given a Delta Lake table:

    .. code-block:: python

        +---+-----------+----------+----------+
        | id|   location|      date|  end_date|
        +---+-----------+----------+----------+
        |  1|      Kochi|2018-01-01|2019-01-01|
        |  1|Lake Forest|2019-01-01|      null|
        +---+-----------+----------+----------+

    And a source DataFrame:

    .. code-block:: python

        +---+--------+----------+
        | id|location|      date|
        +---+--------+----------+
        |  1|  Irvine|2020-01-01|
        |  2|  Venice|2022-01-01|
        +---+--------+----------+



    .. code-block:: python

        import hydro.delta as hd
        hd.scd(delta_table, df, ["id"], effective_ts="date", end_ts="end_date")

    Results in:

    .. code-block:: python

        +---+-----------+----------+----------+
        | id|   location|      date|  end_date|
        +---+-----------+----------+----------+
        |  1|      Kochi|2018-01-01|2019-01-01|
        |  1|Lake Forest|2019-01-01|2020-01-01|
        |  1|     Irvine|2020-01-01|      null|
        |  2|     Venice|2022-01-01|      null|
        +---+-----------+----------+----------+

    See Also
    -----

    bootstrap_scd2


    """
    if isinstance(keys, str):
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
        partition_columns: list[str] = None,
        comment: str = None,
        path: str = None,
        table_identifier: str = None,
) -> DeltaTable:
    """

    Creates an SCD2-ready Delta Lake table from a source DataFrame.

    :param source_df: Source data that will populate the final Delta Lake table
    :param keys: Column name(s) that identify unique rows. Can be a single column name as a string, or a list of strings, where order of the list does not matter.
    :param effective_ts: The name of the existing column that will be used as the "start" or "effective" timestamp for a given entity. The column be of any orderable type, including timestamp, date, string, and numeric types.
    :param end_ts: The name of the non-existing column that will be used as the "end" timestamp for a given entity. Its type will match the type of `effective_ts`.
    :param table_properties: A set of [Delta Lake table properties](https://docs.delta.io/latest/table-properties.html) or custom properties.
    :param partition_columns: A set of column names that will be used to partition the resulting Delta Lake table.
    :param comment: Comment that describes the table.
    :param path: Specify the path to the directory where table data is stored, which could be a path on distributed storage.
    :param table_identifier: The table name. Optionally qualified with a database name [catalog_name.] [database_name.] table_name.
    :return: The resulting DeltaTable object


    Example
    -----

    Given a DataFrame:

    .. code-block:: python

            +---+-----------+----------+
            | id|   location|      date|
            +---+-----------+----------+
            |  1|      Kochi|2018-01-01|
            |  1|Lake Forest|2019-01-01|
            +---+-----------+----------+

    Run bootstrap_scd2:

    .. code-block:: python

        import hydro.delta as hd
        delta_table = hd.bootstrap_scd2(
            df,
            keys=["id"],
            effective_ts="date",
            end_ts="end_date",
            path="/path/to/delta/table",
        )

    Results in the below data being persisted to the given path, in this case, `/path/to/delta/table`

    .. code-block:: python

            +---+-----------+----------+----------+
            | id|   location|      date|  end_date|
            +---+-----------+----------+----------+
            |  1|      Kochi|2018-01-01|2019-01-01|
            |  1|Lake Forest|2019-01-01|      null|
            +---+-----------+----------+----------+

    See Also
    -----

    scd

    """

    if partition_columns is None:
        partition_columns = []
    if not path and not table_identifier:
        raise ValueError(
            'Need to specify one (or both) of `path` and `table_identifier`',
        )

    # scd2-ify the existing data
    window = Window.partitionBy(keys).orderBy(effective_ts)
    final_payload = source_df.withColumn(
        end_ts,
        F.lead(effective_ts).over(window),
    )

    # build the DeltaTable object
    builder = DeltaTable.createOrReplace(
        source_df.sparkSession,
    )  # TODO change to createIfNotExists?
    if table_properties:
        for k, v in table_properties.items():
            builder = builder.property(k, v)
    builder = builder.addColumns(source_df.schema)
    builder = builder.partitionedBy(*partition_columns)
    if comment:
        builder = builder.comment(comment)
    if path:
        builder = builder.location(path)
    if table_identifier:
        builder = builder.tableName(table_identifier)
    builder.execute()  # save DeltaTable to disk

    # write data to disk
    if table_identifier:
        final_payload.write.format('delta').option('mergeSchema', 'true').mode(
            'append',
        ).saveAsTable(table_identifier)
        delta_table = DeltaTable.forName(source_df.sparkSession, table_identifier)
    else:
        final_payload.write.format('delta').option('mergeSchema', 'true').mode(
            'append',
        ).save(path)
        delta_table = DeltaTable.forPath(source_df.sparkSession, path)
    return delta_table


def partial_update_set(
        delta_frame: DataFrame | DeltaTable,
        source_alias: str = 'source',
        target_alias: str = 'target',
) -> F.col:
    """
    Generates an update set for a Delta Lake MERGE operation where the source data provides partial updates.

    Partial updates in this case are when some columns in the data are NULL, but are meant to be non-destructive, or is there no semantic meaning to the NULLs.

    :param delta_frame: A DeltaTable or DataFrame that describes a source MERGE dataframe
    :param source_alias: A temporary name given to the source data of the MERGE
    :param target_alias: A temporary name given to the target Delta Table of the MERGE
    :return: A dictionary that describes non-destructive updates for all fields in `delta_frame` in {key: coalesce(source.key, target.key)} form

    Example
    -----

    **Example**

    Given a Delta Lake table:

    .. code-block:: python

        +---+----------+------+
        | id|  location|status|
        +---+----------+------+
        |  1|california|  null|
        +---+----------+------+

    And some source data that will partially update the Delta Lake table:

    .. code-block:: python

        +---+--------+------+
        | id|location|status|
        +---+--------+------+
        |  1|    null|active|
        +---+--------+------+

    Perform the following `MERGE`:

    .. code-block:: python

        import hydro.delta as hd
        delta_table.alias("target").merge(
            df.alias("source"), "source.id = target.id"
        ).whenNotMatchedInsertAll().whenMatchedUpdate(set=hd.partial_update_set(df)).execute()

    With the resulting Delta Lake table:

    .. code-block:: python

        +---+----------+------+
        | id|  location|status|
        +---+----------+------+
        |  1|california|active|
        +---+----------+------+


    """
    # why does cov lie?
    if isinstance(delta_frame, DeltaTable):
        delta_frame = delta_frame.toDF()
    fields = hs.fields(delta_frame)
    return {field: F.coalesce(f'{source_alias}.{field}', f'{target_alias}.{field}') for field in fields}


def file_stats(delta_table: DeltaTable) -> DataFrame:
    """

    Returns detailed information about the files of the current snapshot of a Delta Lake table, including (per-file):

    - name of file
    - size of file
    - partition values
    - modification time
    - is data change
    - statistics (min, max, and null counts)
    - tags

    This is done by scanning the table's transaction log, so it is fast, cheap, and scalable.

    :return: A DataFrame that describes the physical files that compose a given Delta Lake table
    """
    return _snapshot_allfiles(delta_table)


def partition_stats(delta_table: DeltaTable) -> DataFrame:
    """

    Returns detailed information about the partitions of the current snapshot of a Delta Lake table including (per-partition):

    - total size in bytes
    - file byte size quantiles (0, .25, .5, .75, 1.0) - where 0 is min and 1.0 is max
    - total number of records
    - total number of files
    - oldest and newest timestamps

    This is done by scanning the table's transaction log, so it is fast, cheap, and scalable.

    :return: A DataFrame that describes the size of all partitions in the table
    """
    allfiles = _snapshot_allfiles(delta_table)
    detail = _DetailOutput(delta_table)
    # the snapshot encodes partition values in the top-level `partitionValues` column
    partition_columns = [f'partitionValues.{col}' for col in detail.partition_columns]
    return allfiles.groupBy(*partition_columns).agg(
        F.sum('size').alias('total_bytes'),
        F.percentile_approx('size', [0, 0.25, 0.5, 0.75, 1.0]).alias('bytes_quantiles'),
        F.sum(F.get_json_object('stats', '$.numRecords')).alias('num_records'),
        F.count('*').alias('num_files'),
        F.min('modificationTime').alias('oldest_timestamp'),
        F.max('modificationTime').alias('newest_timestamp'),
    )


def zordering_stats(delta_table: DeltaTable) -> DataFrame:
    """

    Returns a DataFrame that describes the Z-Ordering that has been applied to the table.

    The resulting DataFrame has schema of `zOrderBy`, `count`.

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


def detail(delta_table: DeltaTable) -> dict[str, Any]:
    """

    Returns details about a Delta Lake table including:

    - table created timestamp
    - description
    - table format
    - table id
    - table last modified
    - location
    - minimum reader version
    - minimum writer version
    - table name
    - total number of records of current snapshot
    - total number of files of current snapshot
    - partition columns
    - total number of partitions of the current snapshot
    - properties
    - total data size of the current snapshot
    - percentage collected stats of the current snapshot
    - snapshot version

    :return: A dictionary representing enhanced details of a Delta Lake table
    """
    detail_output = _DetailOutput(delta_table)
    detail_output.humanize()
    details = detail_output.to_dict()
    allfiles = _snapshot_allfiles(delta_table)
    partition_columns = [f'partitionValues.{col}' for col in details['partition_columns']]

    # compute the number of records in the current snapshot by scanning the Delta log
    num_records = (
        allfiles.select(
            F.get_json_object('stats', '$.numRecords').alias('num_records'),
        )
        .agg(F.sum('num_records').alias('num_records'))
        .collect()[0]['num_records']
    )
    details['numRecords'] = _humanize_number(num_records)

    # compute the percentage of files that have statistics in the current snapshot by scanning the Delta log
    stats_percentage = (
        allfiles.agg(
            F.avg(
                F.when(F.col('stats').isNotNull(), F.lit(1)).otherwise(F.lit(0)),
            ).alias('stats_percentage'),
        )
    ).collect()[0]['stats_percentage']
    details['stats_percentage'] = stats_percentage * 100

    # compute the number of partitions in the current snapshot by scanning the Delta log
    partition_count = allfiles.select(*partition_columns).distinct().count()
    details['partition_count'] = _humanize_number(partition_count)

    # compute the version of the current snapshot by looking at the table's history, which scans the Delta log
    version = delta_table.history().select('version').limit(1).collect()[0].asDict()['version']
    details['version'] = _humanize_number(version)
    return details


def summarize_all_files(delta_table: DeltaTable, humanize: bool = True) -> dict[str, str]:
    """

    Lists and summarizes all of the contents of a Delta Lake table's data directory.

    The directory will contain:

    - data files that are part of the current snapshot
    - data files that are "tombstoned" and not part of the current snapshot

    Returns summary statistics including:

    - the total number of files
    - the total size of the files
    - the oldest timestamp of the files

    :param: humanize: Whether or not the results should be made more easily read by humans. Turn this to False if you're looking to do calculations on the raw metrics.
    :returns: A dictionary containing summary statistics about all of the data files under the Delta Lake table's location
    """
    summary = _summarize_data_files(delta_table)
    if humanize:
        summary['number_of_files'] = _humanize_number(summary['number_of_files'])
        summary['total_size'] = _humanize_bytes(summary['total_size'])
        summary['oldest_timestamp'] = _humanize_timestamp(summary['oldest_timestamp'])
    return summary


def idempotency_markers(delta_table: DeltaTable) -> str:
    """

    Exposes a Delta Lake table's idempotency markers, i.e txnAppId and txnVersion.

    Currently this returns a string representation of a Scala map.

    :returns: A string that represents a Scala map of the idempotency markers. This map is in the form Map(key-> value) where key is a given txnAppId and value is the associated version.
    """
    return _snapshot_transactions(delta_table)
