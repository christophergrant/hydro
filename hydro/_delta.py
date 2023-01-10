from __future__ import annotations

import copy

from delta import DeltaTable
from pyspark.sql import DataFrame
from pyspark.sql import SparkSession

from hydro import _humanize_bytes
from hydro import _humanize_number


def _is_running_on_dbr(spark: SparkSession) -> bool:
    flag = True
    try:
        # a canary class, randomly chosen
        spark._jvm.com.DatabricksMain.NONFATAL()
    except TypeError:
        flag = False
    return flag


def _snapshot_allfiles(delta_table: DeltaTable) -> DataFrame:
    spark = delta_table.toDF().sparkSession
    location = delta_table.detail().collect()[0]['location']

    is_databricks = _is_running_on_dbr(spark)

    if is_databricks:  # pragma: no cover
        delta_log = spark._jvm.org.apache.spark.sql.delta.DeltaLog.forTable(
            spark._jsparkSession,
            location,
        )
    else:
        delta_log = spark._jvm.org.apache.spark.sql.delta.DeltaLog.forTable(
            spark._jsparkSession,
            location,
        )

    return DataFrame(delta_log.snapshot().allFiles(), spark)


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
