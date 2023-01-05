from __future__ import annotations

import copy
from typing import Any

import pyspark.sql.functions as F
from delta import DeltaTable
from pyspark.sql import DataFrame
from pyspark.sql import Window
from pyspark.sql.types import DataType
from pyspark.sql.types import StructType

# transformed numFiles to string, sizeInBytes -> size with type string
DETAIL_SCHEMA_JSON = '{"fields":[{"metadata":{},"name":"createdAt","nullable":true,"type":"timestamp"},{"metadata":{},"name":"description","nullable":true,"type":"string"},{"metadata":{},"name":"format","nullable":true,"type":"string"},{"metadata":{},"name":"id","nullable":true,"type":"string"},{"metadata":{},"name":"lastModified","nullable":true,"type":"timestamp"},{"metadata":{},"name":"location","nullable":true,"type":"string"},{"metadata":{},"name":"minReaderVersion","nullable":true,"type":"long"},{"metadata":{},"name":"minWriterVersion","nullable":true,"type":"long"},{"metadata":{},"name":"name","nullable":true,"type":"string"},{"metadata":{},"name":"numFiles","nullable":true,"type":"string"},{"metadata":{},"name":"partitionColumns","nullable":true,"type":{"containsNull":true,"elementType":"string","type":"array"}},{"metadata":{},"name":"properties","nullable":true,"type":{"keyType":"string","type":"map","valueContainsNull":true,"valueType":"string"}},{"metadata":{},"name":"size","nullable":true,"type":"string"}],"type":"struct"}'  # noqa: E501


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


def detail(delta_table: DeltaTable) -> dict[Any, Any]:
    detail_output = DetailOutput(delta_table)
    detail_output.humanize()
    return detail_output.to_dict()


def detail_enhanced(delta_table: DeltaTable) -> dict[Any, Any]:
    details = detail(delta_table)
    allfiles = _snapshot_allfiles(delta_table)
    partition_columns = [
        f'partitionValues.{col}' for col in details['partition_columns']
    ]

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


def partition_stats(delta_table: DeltaTable) -> DataFrame:
    allfiles = _snapshot_allfiles(delta_table)
    detail = DetailOutput(delta_table)
    partition_columns = [f'partitionValues.{col}' for col in detail.partition_columns]
    return allfiles.groupBy(*partition_columns).agg(
        F.sum('size').alias('total_bytes'),
        F.percentile_approx('size', [0, 0.25, 0.5, 0.75, 1.0]).alias('bytes_quantiles'),
        F.sum(F.get_json_object('stats', '$.numRecords')).alias('num_records'),
        F.count('*').alias('num_files'),
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


def partial_update_set(
    fields: list[str],
    source_alias: str,
    target_alias: str,
) -> F.col:
    return {
        field: F.coalesce(f'{source_alias}.{field}', f'{target_alias}.{field}')
        for field in fields
    }


def scd(
    delta_table: DeltaTable,
    source: DataFrame,
    keys: list[str] | str,
    effective_ts: str,
    end_ts: str = None,
    scd_type: int = 2,
):
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
            raise ValueError()

        updated_rows = (
            delta_table.toDF()
            .join(source, keys, 'left_semi')
            .filter(F.col(end_ts).isNull())
        )
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
        window = Window.partitionBy(keys).orderBy(effective_ts)
        final_payload = source.withColumn(
            end_ts,
            F.row_number().over(window),
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
        raise ValueError('type not supported')


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
    if not path and not table_identifier:
        raise ValueError()
    window = Window.partitionBy(keys).orderBy(effective_ts)
    final_payload = source_df.withColumn(
        end_ts,
        F.lead(effective_ts).over(window),
    )

    builder = DeltaTable.createOrReplace(source_df.sparkSession)  # TODO change this?
    if table_properties:
        for k, v in table_properties.items():
            builder = builder.property(k, v)
    builder = builder.addColumns(source_df.schema)
    builder = builder.partitionedBy(*partition_columns)
    if comment:
        builder = builder.comment(comment)
    delta_table = None
    if path:
        builder.location(path).execute()
        final_payload.write.format('delta').option('mergeSchema', 'true').mode(
            'append',
        ).save(path)
        delta_table = DeltaTable.forPath(source_df.sparkSession, path)
    elif table_identifier:
        builder.tableName(table_identifier).execute()
        final_payload.write.format('delta').option('mergeSchema', 'true').mode(
            'append',
        ).saveAsTable(table_identifier)
        delta_table = DeltaTable.forName(source_df.sparkSession, table_identifier)
    return delta_table
