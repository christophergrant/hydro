from __future__ import annotations

import copy
import math

from delta import DeltaTable
from pyspark.sql import DataFrame
from pyspark.sql import SparkSession

import hydro.spark as hs
from hydro import _humanize_bytes
from hydro import _humanize_number


def _summarize_data_files(delta_table: DeltaTable):
    base_path = delta_table.detail().collect()[0]['location']
    spark = delta_table.toDF().sparkSession
    hadoop_conf = spark._jsparkSession.sessionState().newHadoopConf()
    glob_path = spark._jvm.org.apache.hadoop.fs.Path(base_path)
    driver_fs = glob_path.getFileSystem(hadoop_conf)
    files = driver_fs.listFiles(glob_path, True)
    file_count = 0
    total_size = 0
    min_ts = math.inf
    while files.hasNext():
        file = files.next()
        path = file.getPath()
        if '_delta_log' in path.toUri().toString():  # this ain't great - figure out globbing instead of doing this.
            continue
        file_count += 1
        total_size += file.getLen()
        min_ts = min(min_ts, file.getModificationTime())
    return {'number_of_files': file_count, 'total_size': total_size, 'oldest_timestamp': min_ts}


def _is_running_on_dbr(spark: SparkSession) -> bool:
    flag = True
    try:
        # a canary class, randomly chosen
        spark._jvm.com.DatabricksMain.NONFATAL()
    except TypeError:
        flag = False
    return flag


def _snapshot_transactions(delta_table: DeltaTable):
    delta_log = _delta_log(delta_table)
    return delta_log.snapshot().transactions()


def _delta_log(delta_table: DeltaTable):
    spark = delta_table.toDF().sparkSession
    location = delta_table.detail().collect()[0]['location']
    is_databricks = _is_running_on_dbr(spark)
    if is_databricks:
        delta_log = spark._jvm.org.apache.spark.sql.delta.DeltaLog.forTable(
            spark._jsparkSession,
            location,
        )
    else:
        delta_log = spark._jvm.org.apache.spark.sql.delta.DeltaLog.forTable(
            spark._jsparkSession,
            location,
        )
    return delta_log


def _snapshot_allfiles(delta_table: DeltaTable) -> DataFrame:
    delta_log = _delta_log(delta_table)
    return DataFrame(delta_log.snapshot().allFiles(), delta_table.toDF().sparkSession)


class _DetailOutput:
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


def _deduplicate(
        delta_table: DeltaTable,
        backup_path: str,
        keys: list[str] | str,
        tiebreaking_columns: list[str] = None,
) -> DeltaTable:
    """
    Removes duplicates from a Delta Lake table.

    USE THIS AT YOUR OWN RISK. ANY DATA LOSS PROBLEMS ARE ON YOU, DO NOT OPEN AN ISSUE. DO NOT SEND ME AN EMAIL. DO. NOT.

    The reason that this function is hidden is because of the inherent complexities of doing this kind of thing, especially at scale.

    During this deduplication process, you must turn off any other process that write to the table. This is due to possible concurrency issues, and, in the event of failure and rollback, possible data loss.

    :param delta_table:
    :param backup_path: A temporary location used to stage de-duplicated data. The location that `temp_path` points to needs to be empty.
    :param keys: A list of column names used to distinguish rows. The order of this list does not matter.
    :param tiebreaking_columns: A list of column names used for ordering. The order of this list matters, with earlier elements "weighing" more than lesser ones. The columns will be evaluated in descending order. In the event of a tie, you will get non-deterministic results.
    :return: The same Delta Lake table as **delta_table**.
    """
    if isinstance(keys, str):
        keys = [keys]
    detail_object = _DetailOutput(delta_table)
    target_location = detail_object.location
    table_version = delta_table.history().select('version').limit(1).collect()[0].asDict()['version']
    print(
        f'IF THIS OPERATION FAILS, RUN RESTORE TO {table_version} WITH deltaTable.restoreToVersion({table_version})',
    )
    df = delta_table.toDF()
    spark = df.sparkSession

    # Deduplicate the dataframe using `hydro.spark.deduplicate_dataframe`
    deduped = hs.deduplicate_dataframe(df, keys=keys, tiebreaking_columns=tiebreaking_columns)
    deduped.write.format('delta').save(backup_path)

    # To delete matching rows, we use MERGE. MERGE requires a condition that matches rows between source and target
    merge_key_condition = ' AND '.join(
        [f'source.{key} = target.{key}' for key in keys],
    )

    # Perform deletion of existing duplicates using MERGE
    delta_table.alias('target').merge(
        deduped.select(keys).distinct().alias('source'),  # do we need distinct here?
        merge_key_condition,
    ).whenMatchedDelete().execute()

    # Insert the de-duplicated data into the target Delta table
    spark.read.format('delta').load(backup_path).write.format('delta').mode(
        'append',
    ).save(target_location)
    return delta_table
