from __future__ import annotations

import json
from typing import Any

import pyspark.sql.functions as F
from delta import DeltaTable
from pyspark.sql import DataFrame
from pyspark.sql.types import DataType
from pyspark.sql.types import StructType

# transformed numFiles to string, sizeInBytes -> size with type string
DETAIL_SCHEMA_JSON = '{"fields":[{"metadata":{},"name":"createdAt","nullable":true,"type":"timestamp"},{"metadata":{},"name":"description","nullable":true,"type":"string"},{"metadata":{},"name":"format","nullable":true,"type":"string"},{"metadata":{},"name":"id","nullable":true,"type":"string"},{"metadata":{},"name":"lastModified","nullable":true,"type":"timestamp"},{"metadata":{},"name":"location","nullable":true,"type":"string"},{"metadata":{},"name":"minReaderVersion","nullable":true,"type":"long"},{"metadata":{},"name":"minWriterVersion","nullable":true,"type":"long"},{"metadata":{},"name":"name","nullable":true,"type":"string"},{"metadata":{},"name":"numFiles","nullable":true,"type":"string"},{"metadata":{},"name":"partitionColumns","nullable":true,"type":{"containsNull":true,"elementType":"string","type":"array"}},{"metadata":{},"name":"properties","nullable":true,"type":{"keyType":"string","type":"map","valueContainsNull":true,"valueType":"string"}},{"metadata":{},"name":"size","nullable":true,"type":"string"}],"type":"struct"}'  # noqa: E501


def _humanize_number(number: int) -> str:
    return f'{number:,}'


def _humanize_bytes(num_bytes: float) -> str:
    # ChatGPT 🤖 prompt: "write a python program that converts bytes to their proper units (kb, mb, gb, tb, pb, etc)" # noqa: E501
    units = ['bytes', 'KiB', 'MiB', 'GiB', 'TiB', 'PiB', 'EiB']
    i = 0
    while num_bytes >= 1024 and i < len(units) - 1:
        num_bytes /= 1024
        i += 1
    return f'{num_bytes:.2f} {units[i]}'


def _snapshot_allfiles(delta_table: DeltaTable) -> DataFrame:
    # this is kinda hacky but oh well
    spark = delta_table.toDF().sparkSession
    location = delta_table.detail().collect()[0]['location']

    delta_log = spark._jvm.org.apache.spark.sql.delta.DeltaLog.forTable(
        spark._jsparkSession,
        location,
    )
    return DataFrame(delta_log.snapshot().allFiles(), spark)


def detail(delta_table: DeltaTable) -> DataFrame:
    machine_detail = delta_table.detail().collect()[0].asDict()
    machine_detail['numFiles'] = f"{machine_detail['numFiles']:,}"

    machine_detail['size'] = _humanize_bytes(machine_detail['sizeInBytes'])
    del machine_detail['sizeInBytes']

    spark = delta_table.toDF().sparkSession
    schema = StructType.fromJson(json.loads(DETAIL_SCHEMA_JSON))
    return spark.createDataFrame([machine_detail], schema)


def detail_enhanced(delta_table: DeltaTable) -> dict[Any, Any]:
    details = detail(delta_table).collect()[0].asDict()
    allfiles = _snapshot_allfiles(delta_table)
    num_records = (
        allfiles.select(
            F.get_json_object('stats', '$.numRecords').alias('num_records'),
        )  # noqa: E501
        .agg(F.sum('num_records').alias('num_records'))
        .collect()[0]['num_records']
    )
    details['numRecords'] = _humanize_number(num_records)
    return details


def get_table_zordering(delta_table: DeltaTable) -> DataFrame:
    return (
        delta_table.history()
        .filter("operation == 'OPTIMIZE'")
        .filter('operationParameters.zOrderBy IS NOT NULL')
        .select('operationParameters.zOrderBy')
        .groupBy('zOrderBy')
        .count()
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
                            prefix + field.name + '.',
                        ),  # noqa: E501
                    )
                else:
                    if include_types:
                        fields.append((prefix + field.name, field.dataType))
                    else:
                        fields.append(prefix + field.name)
            return fields

        return _get_leaf_fields(struct, '')

    return get_leaf_fields(schema, include_types)
