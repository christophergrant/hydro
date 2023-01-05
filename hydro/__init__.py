from __future__ import annotations

import copy
import json
from typing import Any

import pyspark.sql.functions as F
from delta import DeltaTable
from pyspark.sql import DataFrame
from pyspark.sql.types import DataType
from pyspark.sql.types import StructType

# transformed numFiles to string, sizeInBytes -> size with type string
DETAIL_SCHEMA_JSON = '{"fields":[{"metadata":{},"name":"createdAt","nullable":true,"type":"timestamp"},{"metadata":{},"name":"description","nullable":true,"type":"string"},{"metadata":{},"name":"format","nullable":true,"type":"string"},{"metadata":{},"name":"id","nullable":true,"type":"string"},{"metadata":{},"name":"lastModified","nullable":true,"type":"timestamp"},{"metadata":{},"name":"location","nullable":true,"type":"string"},{"metadata":{},"name":"minReaderVersion","nullable":true,"type":"long"},{"metadata":{},"name":"minWriterVersion","nullable":true,"type":"long"},{"metadata":{},"name":"name","nullable":true,"type":"string"},{"metadata":{},"name":"numFiles","nullable":true,"type":"string"},{"metadata":{},"name":"partitionColumns","nullable":true,"type":{"containsNull":true,"elementType":"string","type":"array"}},{"metadata":{},"name":"properties","nullable":true,"type":{"keyType":"string","type":"map","valueContainsNull":true,"valueType":"string"}},{"metadata":{},"name":"size","nullable":true,"type":"string"}],"type":"struct"}'  # noqa: E501


class DetailOutput:
    def __init__(self, delta_table: DeltaTable):
        self.__dict__ = copy.deepcopy(delta_table.detail().collect()[0].asDict())


def _humanize_number(number: int) -> str:
    return f"{number:,}"


def _humanize_bytes(num_bytes: float) -> str:
    # ChatGPT 🤖 prompt: "write a python program that converts bytes to their proper units (kb, mb, gb, tb, pb, etc)" # noqa: E501
    units = ["bytes", "KiB", "MiB", "GiB", "TiB", "PiB", "EiB"]
    i = 0
    while num_bytes >= 1024 and i < len(units) - 1:
        num_bytes /= 1024
        i += 1
    return f"{num_bytes:.2f} {units[i]}"


def _snapshot_allfiles(delta_table: DeltaTable) -> DataFrame:
    # this is kinda hacky but oh well
    spark = delta_table.toDF().sparkSession
    location = delta_table.detail().collect()[0]["location"]

    delta_log = spark._jvm.org.apache.spark.sql.delta.DeltaLog.forTable(
        spark._jsparkSession,
        location,
    )
    return DataFrame(delta_log.snapshot().allFiles(), spark)


def detail(delta_table: DeltaTable) -> DataFrame:
    machine_detail = delta_table.detail().collect()[0].asDict()
    machine_detail["numFiles"] = f"{machine_detail['numFiles']:,}"

    machine_detail["size"] = _humanize_bytes(machine_detail["sizeInBytes"])
    del machine_detail["sizeInBytes"]

    spark = delta_table.toDF().sparkSession
    schema = StructType.fromJson(json.loads(DETAIL_SCHEMA_JSON))
    return spark.createDataFrame([machine_detail], schema)


def detail_enhanced(delta_table: DeltaTable) -> dict[Any, Any]:
    details = detail(delta_table).collect()[0].asDict()
    allfiles = _snapshot_allfiles(delta_table)
    detail_output = DetailOutput(delta_table)
    partition_columns = [
        f"partitionValues.{col}" for col in detail_output.partitionColumns
    ]

    num_records = (
        allfiles.select(
            F.get_json_object("stats", "$.numRecords").alias("num_records"),
        )
        .agg(F.sum("num_records").alias("num_records"))
        .collect()[0]["num_records"]
    )
    details["numRecords"] = _humanize_number(num_records)

    stats_percentage = (
        allfiles.agg(
            F.avg(
                F.when(F.col("stats").isNotNull(), F.lit(1)).otherwise(F.lit(0)),
            ).alias("stats_percentage"),
        )
    ).collect()[0]["stats_percentage"]
    details["stats_percentage"] = stats_percentage * 100

    partition_count = allfiles.select(*partition_columns).distinct().count()
    details["partition_count"] = _humanize_number(partition_count)
    return details


def get_table_zordering(delta_table: DeltaTable) -> DataFrame:
    return (
        delta_table.history()
        .filter("operation == 'OPTIMIZE'")
        .filter("operationParameters.zOrderBy IS NOT NULL")
        .select("operationParameters.zOrderBy")
        .groupBy("zOrderBy")
        .count()
    )


def partition_stats(delta_table: DeltaTable) -> DataFrame:
    allfiles = _snapshot_allfiles(delta_table)
    detail = DetailOutput(delta_table)
    partition_columns = [f"partitionValues.{col}" for col in detail.partitionColumns]
    return allfiles.groupBy(*partition_columns).agg(
        F.sum("size").alias("total_bytes"),
        F.percentile_approx("size", [0, 0.25, 0.5, 0.75, 1.0]).alias("bytes_quantiles"),
        F.sum(F.get_json_object("stats", "$.numRecords")).alias("num_records"),
        F.count("*").alias("num_files"),
    )


def fields(
    delta_table: DeltaTable,
    include_types: bool = False,
) -> list[tuple[str, DataType] | str]:
    # ChatGPT 🤖 prompt:
    # write a program that takes a PySpark StructType and returns the leaf node field names, even the nested ones # noqa: E501
    schema = delta_table.toDF().schema

    def get_leaf_fields(
        struct: StructType,
        include_types: bool,
    ) -> list[tuple[str, DataType] | str]:
        def _get_leaf_fields(
            struct: StructType,
            prefix: str,
        ) -> list[tuple[str, DataType] | str]:
            fields: list[tuple[str, DataType] | str] = []
            for field in struct:
                if isinstance(field.dataType, StructType):
                    fields.extend(
                        _get_leaf_fields(
                            field.dataType,
                            prefix + field.name + ".",
                        ),  # noqa: E501
                    )
                else:
                    if include_types:
                        fields.append((prefix + field.name, field.dataType))
                    else:
                        fields.append(prefix + field.name)
            return fields

        return _get_leaf_fields(struct, "")

    return get_leaf_fields(schema, include_types)


def partial_update_set(
    fields: list[str],
    source_alias: str,
    target_alias: str,
) -> F.col:
    return {
        field: F.coalesce(f"{source_alias}.{field}", f"{target_alias}.{field}")
        for field in fields
    }
